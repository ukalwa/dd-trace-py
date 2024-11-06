from datetime import datetime
import json
from typing import Any
from typing import Dict
from typing import List
from typing import Tuple

import botocore.client
import botocore.exceptions

from ddtrace import config
from ddtrace.contrib.trace_utils import ext_service
from ddtrace.ext import SpanTypes
from ddtrace.internal import core
from ddtrace.internal.compat import time_ns
from ddtrace.internal.logger import get_logger
from ddtrace.internal.schema import schematize_cloud_messaging_operation
from ddtrace.internal.schema import schematize_service_name
from ddtrace.internal.schema.span_attribute_schema import SpanDirection

from ..utils import extract_DD_json
from ..utils import get_kinesis_data_object


log = get_logger(__name__)


ONE_MB = 1 << 20
MAX_KINESIS_DATA_SIZE = ONE_MB


class TraceInjectionSizeExceed(Exception):
    pass


def update_record(ctx, record: Dict[str, Any], stream: str, inject_trace_context: bool = True) -> None:
    line_break, data_obj = get_kinesis_data_object(record["Data"])
    if data_obj is not None:
        core.dispatch(
            "botocore.kinesis.update_record",
            [ctx, stream, data_obj, record, inject_trace_context],
        )

        try:
            data_json = json.dumps(data_obj)
        except Exception:
            log.warning("Unable to update kinesis record", exc_info=True)

        if line_break is not None:
            data_json += line_break

        data_size = len(data_json)
        if data_size >= MAX_KINESIS_DATA_SIZE:
            log.warning("Data including trace injection (%d) exceeds (%d)", data_size, MAX_KINESIS_DATA_SIZE)

        record["Data"] = data_json


def select_records_for_injection(params: List[Any], inject_trace_context: bool) -> List[Tuple[Any, bool]]:
    records_to_inject_into = []
    if "Records" in params and params["Records"]:
        for i, record in enumerate(params["Records"]):
            if "Data" in record:
                records_to_inject_into.append((record, inject_trace_context and i == 0))
    elif "Data" in params:
        records_to_inject_into.append((params, inject_trace_context))
    return records_to_inject_into


def patched_kinesis_api_call(original_func, instance, args, kwargs, function_vars):
    with core.context_with_data("botocore.patched_kinesis_api_call.propagated") as parent_ctx:
        return _patched_kinesis_api_call(parent_ctx, original_func, instance, args, kwargs, function_vars)


def _patched_kinesis_api_call(parent_ctx, original_func, instance, args, kwargs, function_vars):
    params = function_vars.get("params")
    trace_operation = function_vars.get("trace_operation")
    pin = function_vars.get("pin")
    endpoint_name = function_vars.get("endpoint_name")
    operation = function_vars.get("operation")

    is_getrecords_call = False
    getrecords_error = None
    start_ns = None
    result = None

    if operation == "GetRecords":
        try:
            start_ns = time_ns()
            is_getrecords_call = True
            core.dispatch(f"botocore.{endpoint_name}.{operation}.pre", [params])
            result = original_func(*args, **kwargs)

            records = result["Records"]

            for record in records:
                _, data_obj = get_kinesis_data_object(record["Data"])
                time_estimate = record.get("ApproximateArrivalTimestamp", datetime.now()).timestamp()
                core.dispatch(
                    f"botocore.{endpoint_name}.{operation}.post",
                    [
                        parent_ctx,
                        params,
                        time_estimate,
                        data_obj.get("_datadog") if data_obj else None,
                        record,
                        result,
                        config.botocore.distributed_tracing,
                        extract_DD_json,
                    ],
                )

        except Exception as e:
            getrecords_error = e

    if endpoint_name == "kinesis" and operation in {"PutRecord", "PutRecords"}:
        span_name = schematize_cloud_messaging_operation(
            trace_operation,
            cloud_provider="aws",
            cloud_service="kinesis",
            direction=SpanDirection.OUTBOUND,
        )
    else:
        span_name = trace_operation
    stream_arn = params.get("StreamARN", params.get("StreamName", ""))
    function_is_not_getrecords = not is_getrecords_call
    received_message_when_polling = is_getrecords_call and parent_ctx.get_item("message_received")
    instrument_empty_poll_calls = config.botocore.empty_poll_enabled
    should_instrument = (
        received_message_when_polling or instrument_empty_poll_calls or function_is_not_getrecords or getrecords_error
    )
    is_kinesis_put_operation = endpoint_name == "kinesis" and operation in {"PutRecord", "PutRecords"}

    child_of = parent_ctx.get_item("distributed_context")

    if should_instrument:
        with core.context_with_data(
            "botocore.patched_kinesis_api_call",
            parent=parent_ctx,
            instance=instance,
            args=args,
            params=params,
            endpoint_name=endpoint_name,
            child_of=child_of if child_of is not None else pin.tracer.context_provider.active(),
            operation=operation,
            service=schematize_service_name(
                "{}.{}".format(ext_service(pin, int_config=config.botocore), endpoint_name)
            ),
            call_trace=False,
            pin=pin,
            span_name=span_name,
            span_type=SpanTypes.HTTP,
            activate=True,
            func_run=is_getrecords_call,
            start_ns=start_ns,
        ) as ctx, ctx.span:
            core.dispatch("botocore.patched_kinesis_api_call.started", [ctx])

            if is_kinesis_put_operation:
                records_to_process = select_records_for_injection(params, bool(config.botocore["distributed_tracing"]))
                for record, should_inject_trace_context in records_to_process:
                    update_record(ctx, record, stream_arn, inject_trace_context=should_inject_trace_context)

            try:
                if not is_getrecords_call:
                    core.dispatch(f"botocore.{endpoint_name}.{operation}.pre", [params])
                    result = original_func(*args, **kwargs)
                    core.dispatch(f"botocore.{endpoint_name}.{operation}.post", [params, result])

                if getrecords_error:
                    raise getrecords_error

                core.dispatch("botocore.patched_kinesis_api_call.success", [ctx, result])
                return result

            except botocore.exceptions.ClientError as e:
                core.dispatch(
                    "botocore.patched_kinesis_api_call.exception",
                    [
                        ctx,
                        e.response,
                        botocore.exceptions.ClientError,
                        config.botocore.operations[ctx.span.resource].is_error_code,
                    ],
                )
                raise
    elif is_getrecords_call:
        if getrecords_error:
            raise getrecords_error
        return result
