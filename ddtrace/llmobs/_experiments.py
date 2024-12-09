import csv
import concurrent.futures
from enum import Enum
from functools import wraps
import hashlib
import inspect
import json
import os
import time
from typing import Any, Callable, Dict, Iterator, List, Optional, Union
from urllib.parse import quote
import uuid

from ._utils import HTTPResponse
from ._utils import http_request

import ddtrace

DD_SITE = os.getenv("DD_SITE", "datadoghq.com")
if DD_SITE == "datadoghq.com":
    BASE_URL = f"https://api.{DD_SITE}"
else:
    BASE_URL = f"https://{DD_SITE}"

class FileType(Enum):
    CSV = 'csv'


class Dataset:
    """A container for LLM experiment data that can be pushed to and retrieved from Datadog.

    This class manages collections of input/output pairs used for LLM experiments,
    with functionality to validate, push to Datadog, and retrieve from Datadog.

    Attributes:
        name (str): Name of the dataset
        description (str): Optional description of the dataset
    """

    def __init__(self, name: str, data: List[Dict[str, Union[str, Dict[str, Any]]]], description: str = "") -> None:
        """
        Args:
            name: Name of the dataset
            data: List of dictionaries where 'input' and 'expected_output' values can be
                 either strings or dictionaries of strings
            description: Optional description of the dataset
        """
        self.name = name
        self.description = description
        self._validate_data(data)
        self._data = data

        # Post-push attributes
        self._datadog_dataset_id = None

    def __iter__(self) -> Iterator[Dict[str, Union[str, Dict[str, Any]]]]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def __getitem__(self, index: int) -> Dict[str, Union[str, Dict[str, Any]]]:
        """Get a dataset record.
        
        Args:
            index: Index of the record to retrieve
            
        Returns:
            Dict containing the record.
        """
        record = self._data[index].copy()
        return record

    def _validate_data(self, data: List[Dict[str, Union[str, Dict[str, Any]]]]) -> None:
        """Validate the format and structure of dataset records.

        Args:
            data: List of dataset records to validate

        Raises:
            ValueError: If data is empty, contains non-dictionary rows,
                       has inconsistent keys, or exceeds 50,000 rows
        """
        if not data:
            raise ValueError("Data cannot be empty.")

        if len(data) > 50000:
            raise ValueError("Dataset cannot exceed 50,000 rows.")

        if not all(isinstance(row, dict) for row in data):
            raise ValueError("All rows must be dictionaries.")

        first_row_keys = set(data[0].keys())
        for row in data:
            if set(row.keys()) != first_row_keys:
                raise ValueError("All rows must have the same keys.")

    @classmethod
    def pull(cls, name: str) -> "Dataset":
        """Create a dataset from a dataset hosted in Datadog.

        Args:
            name: Name of the dataset to retrieve from Datadog

        Returns:
            Dataset: A new Dataset instance populated with the records from Datadog

        Raises:
            ValueError: If the dataset is not found
            Exception: If there are HTTP errors during the request
        """
        # Get dataset ID
        encoded_name = quote(name)
        url = f"/api/unstable/llm-obs/v1/datasets?filter[name]={encoded_name}"
        resp = exp_http_request("GET", url)
        
        response_data = resp.json()
        datasets = response_data.get("data", [])

        if not datasets:
            raise ValueError(f"Dataset '{name}' not found")

        dataset_id = datasets[0]["id"]

        # Get dataset records
        url = f"/api/unstable/llm-obs/v1/datasets/{dataset_id}/records"
        resp = exp_http_request("GET", url)
        records_data = resp.json()

        # Transform records into the expected format
        class_records = []
        for record in records_data.get("data", []):
            attrs = record.get("attributes", {})
            input_data = attrs.get("input")
            expected_output = attrs.get("expected_output")
                
            class_records.append({
                "input": input_data,
                "expected_output": expected_output,
                **attrs.get("metadata", {}),
            })

        # Create new dataset instance
        dataset = cls(name, class_records)
        dataset._datadog_dataset_id = dataset_id
        return dataset

    def push(self) -> None:
        """Push the dataset to Datadog.

        Returns:
            Dict[str, Any]: Dictionary containing dataset information including:
                - dataset_id: The ID of the created/updated dataset
                - dataset_name: The name of the dataset
                - record_count: Number of records uploaded
        """
        # Check if dataset exists
        encoded_name = quote(self.name)
        url = f"/api/unstable/llm-obs/v1/datasets?filter[name]={encoded_name}"
        resp = exp_http_request("GET", url)
        response_data = resp.json()
        datasets = response_data.get("data", [])

        if not datasets:
            # Create new dataset
            dataset_payload = {
                "data": {
                    "type": "datasets",
                    "attributes": {
                        "name": self.name,
                        "description": self.description,
                        "metadata": {"team": "ml-obs"},
                    },
                }
            }
            resp = exp_http_request(
                "POST", "/api/unstable/llm-obs/v1/datasets", body=json.dumps(dataset_payload).encode("utf-8")
            )
            response_data = resp.json()
            dataset_id = response_data["data"]["id"]
            self._datadog_dataset_id = dataset_id
        else:
            # Dataset exists, raise error
            raise ValueError(
                f"Dataset '{self.name}' already exists. Dataset versioning will be supported in a future release. "
                "Please use a different name for your dataset."
            )

        # Add records to the dataset
        records_payload = {"data": {"type": "datasets", "attributes": {"records": self._data}}}
        url = f"/api/unstable/llm-obs/v1/datasets/{dataset_id}/records"
        resp = exp_http_request("POST", url, body=json.dumps(records_payload).encode("utf-8"))
        data = resp.json()

        # Print url to the dataset in Datadog
        print(f"Dataset '{self.name}' created: {BASE_URL}/llm/experiments/datasets/{dataset_id}")

    @classmethod
    def from_csv(
        cls,
        filepath: str,
        name: str,
        description: str = "",
        delimiter: str = ",",
        input_columns: List[str] = None,
        expected_output_columns: List[str] = None,
    ) -> "Dataset":
        """Create a Dataset from a CSV file.

        Args:
            filepath: Path to the CSV file
            name: Name of the dataset
            description: Optional description of the dataset
            delimiter: CSV delimiter character, defaults to comma
            input_columns: List of column names to use as input data
            expected_output_columns: List of column names to use as expected output data

        Returns:
            Dataset: A new Dataset instance containing the CSV data

        Raises:
            ValueError: If input_columns or expected_output_columns are not provided
            Exception: If there are issues reading the CSV file
        """
        if input_columns is None or expected_output_columns is None:
            raise ValueError("`input_columns` and `expected_output_columns` must be provided.")

        data = []
        try:
            with open(filepath, mode='r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile, delimiter=delimiter)
                rows = list(reader)
                if not rows:
                    raise ValueError("CSV file is empty.")

                # Ensure that the specified columns are present
                header_columns = reader.fieldnames
                missing_input_columns = [col for col in input_columns if col not in header_columns]
                missing_output_columns = [col for col in expected_output_columns if col not in header_columns]

                if missing_input_columns:
                    raise ValueError(f"Input columns not found in CSV header: {missing_input_columns}")
                if missing_output_columns:
                    raise ValueError(f"Expected output columns not found in CSV header: {missing_output_columns}")

                # Get metadata columns (all columns not used for input or expected output)
                metadata_columns = [col for col in header_columns if col not in input_columns and col not in expected_output_columns]

                for row in rows:
                    # Handle input data
                    if len(input_columns) == 1:
                        input_data = row[input_columns[0]]
                    else:
                        input_data = {col: row[col] for col in input_columns}

                    # Handle expected output data
                    if len(expected_output_columns) == 1:
                        expected_output_data = row[expected_output_columns[0]]
                    else:
                        expected_output_data = {col: row[col] for col in expected_output_columns}

                    # Handle metadata (all remaining columns)
                    metadata = {col: row[col] for col in metadata_columns}

                    data.append({
                        'input': input_data,
                        'expected_output': expected_output_data,
                        **metadata,
                    })
        except FileNotFoundError as e:
            raise DatasetFileError(f"CSV file not found: {filepath}") from e
        except PermissionError as e:
            raise DatasetFileError(f"Permission denied when reading CSV file: {filepath}") from e
        except csv.Error as e:
            raise DatasetFileError(f"Error parsing CSV file: {e}") from e
        except Exception as e:
            raise DatasetFileError(f"Unexpected error reading CSV file: {e}") from e

        return cls(name=name, data=data, description=description)

    @classmethod
    def load(cls, path: str, filetype: FileType, name: str, description: str = "", input_columns: List[str] = None, expected_output_columns: List[str] = None, metadata_columns: List[str] = None, delimiter: str = ",") -> "Dataset":
        """Import a dataset from a file.

        Args:
            path (str): Path to the input file
            filetype (FileType): Type of file to import (CSV, JSONL, or PARQUET)
            name (str): Name of the dataset
            description (str, optional): Description of the dataset. Defaults to "".
            input_columns (List[str], optional): List of column names to use as input data. Required for CSV and PARQUET files.
            expected_output_columns (List[str], optional): List of column names to use as expected output data. Required for CSV and PARQUET files.
            metadata_columns (List[str], optional): List of column names to include as metadata. Defaults to None.
            delimiter (str, optional): Delimiter character for CSV files. Defaults to ",".

        Returns:
            Dataset: A new Dataset instance containing the imported data

        Raises:
            ValueError: If filetype is not supported or if required columns are missing
        """
        if filetype == FileType.CSV:
            return cls.from_csv(
                filepath=path,
                name=name,
                description=description,
                delimiter=delimiter,
                input_columns=input_columns,
                expected_output_columns=expected_output_columns,
                metadata_columns=metadata_columns,
            )
        else:
            raise ValueError(f"Unsupported file type: {filetype}")

    def as_dataframe(self, multiindex: bool = True) -> "pd.DataFrame":
        """Convert the dataset to a pandas DataFrame.

        Args:
            multiindex (bool): If True, expand 'input' and 'expected_output' dictionaries into columns with MultiIndex.
                            If False, keep 'input' and 'expected_output' as columns containing dictionaries.

        Returns:
            pd.DataFrame: DataFrame representation of the dataset.

        Raises:
            ImportError: If pandas is not installed.
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError(
                "pandas is required to convert dataset to DataFrame. "
                "Please install it with `pip install pandas`"
            )

        if multiindex:
            column_tuples = set()
            data_rows = []
            for record in self._data:
                flat_record = {}

                # Handle 'input' fields
                input_data = record.get('input', {})
                if isinstance(input_data, dict):        
                    for k, v in input_data.items():
                        flat_record[('input', k)] = v
                        column_tuples.add(('input', k))
                else:
                    flat_record[('input', '')] = input_data
                    column_tuples.add(('input', ''))

                # Handle 'expected_output' fields
                expected_output = record.get('expected_output', {})
                if isinstance(expected_output, dict):
                    for k, v in expected_output.items():
                        flat_record[('expected_output', k)] = v
                        column_tuples.add(('expected_output', k))
                else:
                    flat_record[('expected_output', '')] = expected_output
                    column_tuples.add(('expected_output', ''))

                # Handle any other top-level fields
                for k, v in record.items():
                    if k not in ['input', 'expected_output']:
                        flat_record[('metadata', k)] = v
                        column_tuples.add(('metadata', k))
                data_rows.append(flat_record)

            # Convert column_tuples to a sorted list to maintain consistent column order
            column_tuples = sorted(list(column_tuples))

            # Build the DataFrame
            records_list = []
            for flat_record in data_rows:
                row = [flat_record.get(col, None) for col in column_tuples]
                records_list.append(row)

            df = pd.DataFrame(records_list, columns=pd.MultiIndex.from_tuples(column_tuples))

            return df

        else:
            data = []
            for record in self._data:
                new_record = {}
                input_data = record.get('input', {})
                new_record['input'] = input_data
                expected_output = record.get('expected_output', {})
                new_record['expected_output'] = expected_output
                # Copy other fields
                for k, v in record.items():
                    if k not in ['input', 'expected_output']:
                        new_record[k] = v
                data.append(new_record)
            return pd.DataFrame(data)

    def export_to_jsonl(self, file_path):
        """
        Exports the dataset to a JSONL file.

        Args:
            file_path (str): The path to the output JSONL file.
        """
        import json

        with open(file_path, 'w') as f:
            for record in self._data:
                json_line = json.dumps(record)
                f.write(json_line + '\n')


class Experiment:
    """Manages the execution and evaluation of LLM tasks on a dataset.

    This class handles running tasks against datasets, applying evaluators,
    and collecting results for analysis.

    Attributes:
        name (str): Name of the experiment
        task (Callable): Function that processes each dataset record
        dataset (Dataset): Dataset to run the experiment on
        evaluators (List[Callable]): Functions that evaluate task outputs
        tags (List[str]): Tags for organizing experiments
        project_name (str): Name of the project this experiment belongs to
        description (str): Description of the experiment
        metadata (Dict[str, Any]): Additional metadata for the experiment
        config (Optional[Dict[str, Any]]): Configuration for the task
        has_run (bool): Whether the experiment has been executed
        has_evaluated (bool): Whether the evaluations have been performed
        outputs (List[Dict]): Outputs after running the task
        evaluations (List[Dict]): Evaluation results after running evaluators
    """

    def __init__(
        self,
        name: str,
        task: Callable,
        dataset: Dataset,
        evaluators: List[Callable],
        tags: List[str] = [],
        project_name: str = "-",
        description: str = "",
        metadata: Dict[str, Any] = {},
        config: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.name = name
        self.task = task
        self.dataset = dataset
        self.evaluators = evaluators
        self.tags = tags
        self.project_name = project_name
        self.description = description
        self.metadata = metadata
        self.config = config

        # Enforce that the task function has the @task decorator
        if not hasattr(self.task, '_is_task'):
            raise TypeError("Task function must be decorated with @task decorator.")

        # Enforce that all evaluators have the @evaluator decorator
        for evaluator_func in self.evaluators:
            if not hasattr(evaluator_func, '_is_evaluator'):
                raise TypeError(f"Evaluator '{evaluator_func.__name__}' must be decorated with @evaluator decorator.")

        # Post-run attributes
        self.has_run = False
        self.has_evaluated = False
        self.outputs = []
        self.evaluations = []

    def run_task(
        self,
        _jobs: int = 10,
        raise_errors: bool = False,
    ) -> None:
        """Execute the task function on the dataset and store the outputs.

        Args:
            _jobs: Number of concurrent jobs to run (between 1-20). Defaults to 10.
            raise_errors: If True, raises exceptions from failed tasks. If False, stores
                          errors in the output. Defaults to False.

        Raises:
            ValueError: If _jobs is not between 1 and 20
        """
        if not 1 <= _jobs <= 20:
            raise ValueError("Number of jobs must be between 1 and 20")
        
        self.outputs = []
        total_rows = len(self.dataset)
        completed = 0
        error_count = 0 
        error_messages = []  

        def process_row(idx_row):
            idx, row = idx_row
            start_time = time.time()
            try:
                input_data = row['input']
                
                if getattr(self.task, '_accepts_config', False):
                    output = self.task(input_data, self.config)
                else:
                    output = self.task(input_data)

                output_data = {
                    "idx": idx,
                    "output": output,
                    "metadata": {
                        "timestamp": start_time,
                        "duration": time.time() - start_time,
                        "dataset_record_idx": idx,
                        "project_name": self.project_name,
                        "experiment_name": self.name,
                        "dataset_name": self.dataset.name,
                    },
                    "error": {
                        "message": None,
                        "stack": None,
                        "type": None,
                    }
                }
                return output_data

            except Exception as e:
                error_message = str(e)
                error_messages.append(f"Row {idx}: {error_message}")
                return {
                    "idx": idx,
                    "output": None,
                    "metadata": {
                        "timestamp": start_time,
                        "duration": time.time() - start_time,
                        "dataset_record_idx": idx,
                        "project_name": self.project_name,
                        "experiment_name": self.name,
                        "dataset_name": self.dataset.name,
                    },
                    "error": {
                        "message": error_message,
                        "stack": None,
                        "type": type(e).__name__,
                    }
                }

        _print_progress_bar(0, total_rows, prefix='Processing:', suffix='Complete')

        with concurrent.futures.ThreadPoolExecutor(max_workers=_jobs) as executor:
            futures = {executor.submit(process_row, (idx, row)): idx for idx, row in enumerate(self.dataset)}
            outputs_buffer = [None] * total_rows

            try:
                for future in concurrent.futures.as_completed(futures):
                    idx = futures[future]
                    try:
                        output_data = future.result()
                        outputs_buffer[idx] = output_data
                        if raise_errors and output_data['error']['message']:
                            error_message = output_data['error']['message']
                            raise ExperimentTaskError(error_message, idx, output_data['error']['type'])
                        elif output_data['error']['message']:
                            error_count += 1

                    except Exception as e:
                        outputs_buffer[idx] = {
                            "idx": idx,
                            "output": None,
                            "metadata": {
                                "timestamp": time.time(),
                                "duration": 0,
                                "dataset_record_idx": idx,
                                "project_name": self.project_name,
                                "experiment_name": self.name,
                                "dataset_name": self.dataset.name,
                            },
                            "error": {
                                "message": str(e),
                                "stack": None,
                                "type": type(e).__name__,
                            }
                        }
                        if raise_errors:
                            raise e
                        else:
                            error_count += 1
                            error_messages.append(f"Row {idx}: {str(e)}")

                    completed += 1
                    _print_progress_bar(completed, total_rows, prefix='Processing:', suffix='Complete')

            except Exception as e:
                for future in futures:
                    future.cancel()
                executor.shutdown(wait=False)
                raise e

        self.outputs = outputs_buffer
        self.has_run = True

        error_rate = (error_count / total_rows) * 100
        print(f"\nTask completed with {error_count} errors ({error_rate:.2f}% error rate)")

        if error_count > 0:
            print("\nError Summary:")
            for error_msg in error_messages:
                print(f"- {error_msg}")
            print("\nIf you'd like to halt execution on errors and see the full traceback, set `raise_errors=True` when running the experiment.\n")

    def run_evaluations(self, evaluators: Optional[List[Callable]] = None, raise_errors: bool = False) -> "ExperimentResults":
        """Run evaluators on the outputs and return ExperimentResults.
        
        Args:
            evaluators (Optional[List[Callable]]): List of evaluators to use. If None, uses the experiment's evaluators.
            raise_errors (bool): If True, raises exceptions encountered during evaluation.
        
        Returns:
            ExperimentResults: A new ExperimentResults instance with the evaluation results.
        
        Raises:
            ValueError: If task has not been run yet
        """
        if not self.has_run:
            raise ValueError("Task has not been run yet. Please call run_task() before run_evaluations().")

        # Use provided evaluators or fall back to experiment's evaluators
        evaluators_to_use = evaluators if evaluators is not None else self.evaluators

        # Validate that all evaluators have the @evaluator decorator
        for evaluator_func in evaluators_to_use:
            if not hasattr(evaluator_func, '_is_evaluator'):
                raise TypeError(f"Evaluator '{evaluator_func.__name__}' must be decorated with @evaluator decorator.")

        evaluations = []
        total_rows = len(self.outputs)
        completed = 0
        error_count = 0  
        error_messages = [] 

        _print_progress_bar(0, total_rows, prefix='Evaluating:', suffix='Complete')

        for idx, output_data in enumerate(self.outputs):
            try:
                output = output_data["output"]

                dataset_row = self.dataset[idx]
                input_data = dataset_row.get('input', {})
                expected_output = dataset_row.get('expected_output', {})

                evaluations_dict = {}
                for evaluator in evaluators_to_use:
                    try:
                        evaluation_result = evaluator(input_data, output, expected_output)
                        evaluations_dict[evaluator.__name__] = evaluation_result
                    except Exception as e:
                        error_count += 1
                        error_message = f"Row {idx}, Evaluator {evaluator.__name__}: {type(e).__name__}: {e}"
                        error_messages.append(error_message)
                        if raise_errors:
                            raise e

                evaluations.append({
                    "idx": idx,
                    "evaluations": evaluations_dict,
                    "error": None,
                })

            except Exception as e:
                if raise_errors:
                    raise e
                error_count += 1
                error_message = f"Row {idx}: {type(e).__name__}: {e}"
                error_messages.append(error_message)
                evaluations.append({
                    "idx": idx,
                    "evaluations": {},
                    "error": {
                        "message": str(e),
                        "type": type(e).__name__,
                        "stack": None,
                    },
                })

            completed += 1
            _print_progress_bar(completed, total_rows, prefix='Evaluating:', suffix='Complete')

        if len(evaluators_to_use) > 0:  
            error_rate = (error_count / (total_rows * len(evaluators_to_use))) * 100
        else:
            error_rate = 0

        print(f"\nEvaluation completed with {error_count} errors ({error_rate:.2f}% error rate)")

        if error_count > 0:
           
            print("\nError Summary:")
            for error_msg in error_messages:
                print(f"- {error_msg}")
            print("\nIf you'd like to halt execution on errors and see the full traceback, set `raise_errors=True` when running the experiment.\n")
      
        self.has_evaluated = True
        return ExperimentResults(self.dataset, self, self.outputs, evaluations)

    def run(
        self,
        _jobs: int = 10,
        raise_errors: bool = False,
    ) -> "ExperimentResults":
        """Execute the task and evaluations, returning the results.

        Args:
            _jobs (int): Number of worker threads.
            timeout (float, optional): Time limit for the task execution in seconds.
            raise_errors (bool): If True, raises exceptions from failed tasks. If False, stores
                                errors in the output. Defaults to False.

        Returns:
            ExperimentResults: The results of the experiment.
        """
        self.run_task(_jobs=_jobs, raise_errors=raise_errors)
        experiment_results = self.run_evaluations(raise_errors=raise_errors)
        print()  
        return experiment_results


class ExperimentResults:
    """Contains and manages the results of an experiment run.

    Stores the outputs, evaluations, and metadata for each record processed
    in an experiment, with functionality to analyze and push results to Datadog.

    Attributes:
        dataset (Dataset): The dataset used in the experiment
        experiment (Experiment): The experiment that generated these results
        outputs (List[Dict]): Outputs after running the task
        evaluations (List[Dict]): Evaluation results after running evaluators
    """

    def __init__(self, dataset: Dataset, experiment: Experiment, outputs: List[Dict], evaluations: List[Dict]) -> None:
        self.dataset = dataset
        self.experiment = experiment
        self.outputs = outputs  # List of outputs from run_task
        self.evaluations = evaluations  # List of evaluations from run_evaluations
        self.merged_results = self._merge_results()  # Merged outputs and evaluations

    def _merge_results(self) -> List[Dict[str, Any]]:
        """Merge outputs and evaluations into a single list of results."""
        merged_results = []
        for idx in range(len(self.outputs)):
            output_data = self.outputs[idx]
            evaluation_data = self.evaluations[idx]
            dataset_record = self.dataset._data[idx]

            merged_result = {
                "idx": idx,
                "input": dataset_record.get('input', {}),
                "expected_output": dataset_record.get('expected_output', {}),
                "output": output_data.get('output'),
                "evaluations": evaluation_data.get('evaluations', {}),
                "metadata": output_data.get('metadata', {}),
                "error": output_data.get('error'),
                "tags": self.experiment.tags,
            }
            merged_results.append(merged_result)
        return merged_results

    def __iter__(self) -> Iterator[Dict[str, Any]]:
        return iter(self.merged_results)

    def __len__(self) -> int:
        return len(self.merged_results)

    def __getitem__(self, index: int) -> Any:
        """Get a result record.
        
        Args:
            index: Index of the record to retrieve
            
        Returns:
            Dict containing the record.
        """
        result = self.merged_results[index].copy()
        return result

    def as_dataframe(self, multiindex: bool = True) -> "pd.DataFrame":
        """Convert the experiment results to a pandas DataFrame, including the experiment config.

        Args:
            multiindex (bool): If True, expand nested dictionaries into MultiIndex columns.
                            If False, keep the nested dictionaries as they are.

        Returns:
            pd.DataFrame: A DataFrame representation of the experiment results.

        Raises:
            ImportError: If pandas is not installed.
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError(
                "pandas is required to convert experiment results to DataFrame. "
                "Please install it with `pip install pandas`"
            )

        # Define the desired column order
        COLUMN_ORDER = ['input', 'expected_output', 'output', 'evaluations', 'metadata', 'config', 'error']
        
        data_rows = []
        column_tuples = set()

        for result in self.merged_results:
            record = {}

            if multiindex:
                input_data = result.get('input', {})
                if isinstance(input_data, dict):
                    for k, v in input_data.items():
                        record[('input', k)] = v
                        column_tuples.add(('input', k))
                else:
                    record[('input', '')] = input_data
                    column_tuples.add(('input', ''))

                expected_output = result.get('expected_output', {})
                if isinstance(expected_output, dict):
                    for k, v in expected_output.items():
                        record[('expected_output', k)] = v
                        column_tuples.add(('expected_output', k))
                else:
                    record[('expected_output', '')] = expected_output
                    column_tuples.add(('expected_output', ''))

                output = result.get('output', {})
                if isinstance(output, dict):
                    for k, v in output.items():
                        record[('output', k)] = v
                        column_tuples.add(('output', k))
                else:
                    record[('output', '')] = output
                    column_tuples.add(('output', ''))

                # Handle 'evaluations' fields
                evaluations = result.get('evaluations', {})
                for eval_name, eval_result in evaluations.items():
                    if isinstance(eval_result, dict):
                        for k, v in eval_result.items():
                            record[('evaluations', eval_name, k)] = v
                            column_tuples.add(('evaluations', eval_name, k))
                    else:
                        record[('evaluations', eval_name)] = eval_result
                        column_tuples.add(('evaluations', eval_name))

                # Handle 'metadata' fields
                for k, v in result.get('metadata', {}).items():
                    record[('metadata', k)] = v
                    column_tuples.add(('metadata', k))

                # Handle 'config' fields
                if self.experiment.config:
                    for k, v in self.experiment.config.items():
                        record[('config', k)] = v
                        column_tuples.add(('config', k))

                # Handle 'error' fields
                error = result.get('error', {})
                if error:
                    for k, v in error.items():
                        record[('error', k)] = v
                        column_tuples.add(('error', k))

                data_rows.append(record)
            else:
                # Non-multiindex implementation remains the same
                new_record = {}
                input_data = result.get('input', {})
                new_record['input'] = input_data
                expected_output = result.get('expected_output', {})
                new_record['expected_output'] = expected_output
                output = result.get('output', {})
                new_record['output'] = output
                new_record['evaluations'] = result.get('evaluations', {})
                new_record['metadata'] = result.get('metadata', {})
                new_record['config'] = self.experiment.config
                new_record['error'] = result.get('error', {})
                data_rows.append(new_record)

        if multiindex:
            # Sort column_tuples based on the desired order
            column_tuples = sorted(list(column_tuples), 
                                key=lambda x: (COLUMN_ORDER.index(x[0]), x[1:] if len(x) > 1 else ''))

            # Build the DataFrame
            records_list = []
            for record in data_rows:
                row = [record.get(col, None) for col in column_tuples]
                records_list.append(row)

            df = pd.DataFrame(records_list, columns=pd.MultiIndex.from_tuples(column_tuples))
            return df
        else:
            df = pd.DataFrame(data_rows)
            # Reorder columns according to COLUMN_ORDER
            cols = [col for col in COLUMN_ORDER if col in df.columns]
            return df[cols]

    def push(self, overwrite: bool = False) -> None: # TODO: Implement overwrite
        """Push the experiment results to Datadog.

        Raises:
            ValueError: If the dataset hasn't been pushed to Datadog first
        """
        if not self.experiment.dataset._datadog_dataset_id:
            raise ValueError(
                "Dataset has not been pushed to Datadog. "
                "Please call dataset.push() before pushing experiment results."
            )

        # Check if project exists
        url = f"/api/unstable/llm-obs/v1/projects?filter[name]={self.experiment.project_name}"
        resp = exp_http_request("GET", url)
        response_data = resp.json()
        projects = response_data.get("data", [])
        if not projects:
            # Create new project
            project_payload = {
                "data": {
                    "type": "projects",
                    "attributes": {
                        "name": self.experiment.project_name,
                        "description": "",
                        "metadata": {"team": "ml-obs"},
                    },
                }
            }
            resp = exp_http_request(
                "POST",
                "/api/unstable/llm-obs/v1/projects",
                body=json.dumps(project_payload).encode("utf-8"),
            )
            response_data = resp.json()
            project_id = response_data["data"]["id"]
        else:
            project_id = projects[0]["id"]

        # Create new experiment
        experiment_payload = {
            "data": {
                "type": "experiments",
                "attributes": {
                    "name": self.experiment.name,
                    "description": self.experiment.description,
                    "dataset_id": self.experiment.dataset._datadog_dataset_id,
                    "project_id": project_id,
                    "metadata": {
                        "tags": self.experiment.tags,
                        **(self.experiment.metadata or {}),
                        "config": self.experiment.config,
                    },
                    "ensure_unique": True, # Generates a new experiment with a unique name if the experiment name already exists
                },
            }
        }
        resp = exp_http_request(
            "POST", "/api/unstable/llm-obs/v1/experiments", body=json.dumps(experiment_payload).encode("utf-8")
        )
        response_data = resp.json()
        experiment_id = response_data["data"]["id"]
        self.experiment.name = response_data["data"]["attributes"]["name"]

        spans = []
        metrics = []
        for result in self.merged_results:
            idx = result['idx']
            merged_result = result
            output = merged_result.get('output')
            input = merged_result.get('input', {})
            evaluations = merged_result.get('evaluations', {})
            expected_output = merged_result.get('expected_output', {})
            metadata = merged_result.get('metadata', {})
            error = merged_result.get('error', {})

            # When the dataset is not hosted, we use the hash of the input and expected output as the dataset record id
            dataset_record_id = hashlib.md5((str(input) + str(expected_output)).encode('utf-8')).hexdigest()

            span = {
                "span_id": _make_id(),
                "project_id": project_id,
                "experiment_id": experiment_id,
                "dataset_id": self.experiment.dataset._datadog_dataset_id,
                #TODO: Extract the record id from the dataset for hosted datasets
                "dataset_record_id": dataset_record_id,
                "start_ns": int(metadata.get("timestamp", time.time()) * 1e9),
                "duration": float(metadata.get("duration", 0) * 1e9),
                "status": "ok" if not error else "error",
                "metrics": {},  # TODO: Fill in with actual metrics once we have tracing and llm spans
                "meta": {
                    "span": {"kind": "experiment"},
                    "input": merged_result.get('input', {}),
                    "output": output,
                    "expected_output": merged_result.get('expected_output', {}),
                    "error": {
                        "message": error.get("message"),
                        "type": error.get("type"),
                        "stack": error.get("stack"),
                    }
                },
            }
            spans.append(span)

            # Add evaluation metrics
            for metric_name, metric_value in evaluations.items():
                timestamp_ms = int(metadata.get("timestamp", time.time()) * 1000)

                # Check for bool first, since bool is a subclass of int
                if isinstance(metric_value, bool):
                    metric_type = "categorical"
                    metric_value = str(metric_value).lower()
                elif isinstance(metric_value, (int, float)):
                    metric_type = "score"
                else:
                    metric_type = "categorical"
                    metric_value = str(metric_value)

                metric = {
                    "span_id": span["span_id"],
                    "metric_type": metric_type,
                    "timestamp_ms": timestamp_ms,
                    "label": metric_name,
                    "score_value" if metric_type == "score" else "categorical_value": metric_value,
                }

                metrics.append(metric)



        # Prepare payload and send to Datadog
        results_payload = {
            "data": {
                "type": "experiments",
                "tags": self.experiment.tags + ["ddtrace.version:" + ddtrace.__version__],
                "attributes": {"spans": spans, "metrics": metrics},
            }
        }


        url = f"/api/unstable/llm-obs/v1/experiments/{experiment_id}/events"
        exp_http_request("POST", url, body=json.dumps(results_payload).encode("utf-8"))

        # Print URL to the experiment in Datadog
        print(f"Experiment '{self.experiment.name}' created: {BASE_URL}/llm/experiments/experiment-list/{experiment_id}")

    def export_to_jsonl(self, file_path):
        """
        Exports the experiment results to a JSONL file.

        Args:
            file_path (str): The path to the output JSONL file.
        """
        import json

        with open(file_path, 'w') as f:
            for result in self.merged_results:
                json_line = json.dumps(result)
                f.write(json_line + '\n')


def _make_id() -> str:
    """Generate a unique identifier.

    Returns:
        str: A random UUID as a hexadecimal string
    """
    return uuid.uuid4().hex


def exp_http_request(method: str, url: str, body: Optional[bytes] = None) -> HTTPResponse:
    """Make an HTTP request to the Datadog experiments API."""
    missing_keys = []
    for key in ["DD_API_KEY", "DD_APPLICATION_KEY"]:
        if not os.getenv(key):
            missing_keys.append(key)

    if missing_keys:
        raise ValueError(
            f"Missing required Datadog API keys in environment variables: {', '.join(missing_keys)}. "
            "Please set these environment variables before pushing to Datadog."
        )

    headers = {
        "DD-API-KEY": os.getenv("DD_API_KEY"),
        "DD-APPLICATION-KEY": os.getenv("DD_APPLICATION_KEY"),
        "Content-Type": "application/json",
    }
    full_url = BASE_URL + url
    resp = http_request(method, full_url, headers=headers, body=body)
    if resp.status_code == 403:
        if not DD_SITE:
            raise ValueError("DD_SITE may be incorrect. Please check your DD_SITE environment variable.")
        else:
            print(resp.text())
            raise ValueError("DD_API_KEY or DD_APPLICATION_KEY is incorrect.")
    if resp.status_code >= 400:
        try:
            error_details = resp.json()
            error_message = error_details.get('errors', [{}])[0].get('detail', resp.text())
        except Exception:
            error_message = resp.text()
        raise ValueError(f"Request failed with status code {resp.status_code}: {error_message}")
    return resp


def task(func):
    if func.__name__ == "task":
        raise ValueError("Function name 'task' is reserved. Please use a different name for your task function.")
        
    @wraps(func)
    def wrapper(input: Dict[str, Union[str, Dict[str, Any]]], config: Optional[Dict[str, Any]] = None) -> Any:
        # Call the original function with or without config
        if 'config' in inspect.signature(func).parameters:
            return func(input, config)
        return func(input)
    # Enforce signature compliance
    sig = inspect.signature(func)
    params = sig.parameters
    if 'input' not in params:
        raise TypeError("Task function must have an 'input' parameter.")
    # Set attribute to indicate whether the function accepts config
    wrapper._accepts_config = 'config' in params
    wrapper._is_task = True  # Set attribute to indicate decoration
    return wrapper


def evaluator(func):
    @wraps(func)
    def wrapper(input: Union[str, Dict[str, Any]] = None, output: Union[str, Dict[str, Any]] = None, expected_output: Union[str, Dict[str, Any]] = None) -> Any:
        return func(input, output, expected_output)
    # Enforce signature compliance
    sig = inspect.signature(func)
    params = sig.parameters
    required_params = ['input', 'output', 'expected_output']
    if not all(param in params for param in required_params):
        raise TypeError(f"Evaluator function must have parameters {required_params}.")
    wrapper._is_evaluator = True  # Set attribute to indicate decoration
    return wrapper


def _print_progress_bar(iteration, total, prefix='', suffix='', decimals=1, length=50, fill='█'):
    percent = f"{100 * (iteration / float(total)):.{decimals}f}"
    filled_length = int(length * iteration // total)
    bar = fill * filled_length + '-' * (length - filled_length)
    # Use carriage return '\r' to overwrite the line
    print(f'\r{prefix} |{bar}| {percent}% {suffix}', end='\r', flush=True)
    if iteration == total:
        print()  # Move to the next line after completion

class DatasetFileError(Exception):
    """Exception raised when there are errors reading or processing dataset files."""
    pass


class ExperimentTaskError(Exception):
    """Exception raised when a task fails during experiment execution."""
    def __init__(self, message: str, row_idx: int, original_error: Exception = None):
        self.row_idx = row_idx
        self.original_error = original_error
        super().__init__(message)