from typing import Dict
from typing import List

from langchain_core.pydantic_v1 import BaseModel
from langchain_core.pydantic_v1 import Field


"""
This module contains helper pydantic models to validate the inputs
and outputs of LLM calls used for Ragas
"""


class StatementFaithfulnessAnswer(BaseModel):
    statement: str = Field(..., description="the original statement, word-by-word")
    reason: str = Field(..., description="the reason of the verdict")
    verdict: int = Field(..., description="the verdict(0/1) of the faithfulness.")


class StatementFaithfulnessAnswers(BaseModel):
    __root__: List[StatementFaithfulnessAnswer]

    def dicts(self) -> List[Dict]:
        return self.dict()["__root__"]


class Statements(BaseModel):
    sentence_index: int = Field(..., description="Index of the sentence from the statement list")
    simpler_statements: List[str] = Field(..., description="the simpler statements")


class StatementsAnswers(BaseModel):
    __root__: List[Statements]

    def dicts(self) -> List[Dict]:
        return self.dict()["__root__"]
