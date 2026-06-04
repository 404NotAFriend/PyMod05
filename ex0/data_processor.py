#!/usr/bin/env python3
from abc import ABC, abstractmethod
from typing import Any, Union, List


class DataProcessor(ABC):
    def __init__(self):
        self.data_storage = []
        self.next_rank = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        if not self.data_storage:
            raise IndexError("No data available to output")

        oldest_item = self.data_storage.pop(0)
        data, rank = oldest_item
        return (rank, str(data))


class NumericProcessor(DataProcessor):
    def validate(self, data: Union[int,
                                   float, List[Union[int, float]]]) -> bool:
        if isinstance(data, (int, float)):
            return True

        if isinstance(data, list):
            return all(isinstance(item, (int, float)) for item in data)

        return False

    def ingest(self, data: Union[int, float, List[Union[int, float]]]) -> None:
        if not self.validate(data):
            raise ValueError("Invalid data for this processor")

        self.data_storage.append((data, self.next_rank))
        self.next_rank += 1


# class TextProcessor(DataProcessor):


# class LogProcessor(DataProcessor):
