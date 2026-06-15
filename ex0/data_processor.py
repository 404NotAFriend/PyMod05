#!/usr/bin/env python3
from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    def __init__(self) -> None:
        self._storage: list[tuple[int, str]] = []
        self._rank: int = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        if not self._storage:
            raise IndexError("No data available to output")
        return self._storage.pop(0)


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, bool):
            return False
        if isinstance(data, (int, float)):
            return True
        if isinstance(data, list):
            return all(
                isinstance(item, (int, float))
                and not isinstance(item, bool)
                for item in data
            )
        return False

    def ingest(self, data: int | float | list[int | float]) -> None:
        if not self.validate(data):
            raise TypeError("Improper numeric data")
        if isinstance(data, list):
            for item in data:
                self._storage.append((self._rank, str(item)))
                self._rank += 1
        else:
            self._storage.append((self._rank, str(data)))
            self._rank += 1


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True
        if isinstance(data, list):
            return all(isinstance(item, str) for item in data)
        return False

    def ingest(self, data: str | list[str]) -> None:
        if not self.validate(data):
            raise TypeError("Improper text")
        if isinstance(data, list):
            for item in data:
                self._storage.append((self._rank, str(item)))
                self._rank += 1
        else:
            self._storage.append((self._rank, str(data)))
            self._rank += 1


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, dict):
            return (
                "log_level" in data
                and "log_message" in data
                and isinstance(data["log_level"], str)
                and isinstance(data["log_message"], str)
            )
        if isinstance(data, list):
            return all(
                isinstance(item, dict)
                and "log_level" in item
                and "log_message" in item
                and isinstance(item["log_level"], str)
                and isinstance(item["log_message"], str)
                for item in data
            )
        return False

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        if not self.validate(data):
            raise TypeError("Improper log data")
        if isinstance(data, list):
            for item in data:
                self._storage.append((
                    self._rank,
                    f"{item['log_level']}: {item['log_message']}"
                ))
                self._rank += 1
        else:
            self._storage.append((
                self._rank,
                f"{data['log_level']}: {data['log_message']}"
            ))
            self._rank += 1


def main() -> None:
    print("=== Code Nexus - Data Processor ===")

    nproc = NumericProcessor()
    print()
    print("Testing Numeric Processor...")
    print(" Trying to validate input '42':", nproc.validate(42))
    print(" Trying to validate input 'Hello':", nproc.validate('Hello'))
    print(" Test invalid ingestion of string 'foo' without prior validation:")
    try:
        nproc.ingest('foo')  # type: ignore[arg-type]
        print("Something went wrong:[")
    except TypeError as e:
        print(" Got exception:", e)
    datalist_num: list[int | float] = [1, 2, 3, 4, 5]
    print(" Processing data:", datalist_num)
    nproc.ingest(datalist_num)
    print(" Extracting 3 values...")
    for _ in range(3):
        rank, value = nproc.output()
        print(f" Numeric value {rank}: {value}")

    tproc = TextProcessor()
    print()
    print("Testing Text Processor...")
    print(" Trying to validate input '42':", tproc.validate(42))
    datalist_txt: list[str] = ['Hello', 'Nexus', 'World']
    print(" Processing data:", datalist_txt)
    tproc.ingest(datalist_txt)
    print(" Extracting 1 value...")
    print("Text value 0:", tproc.output()[1])
     


if __name__ == "__main__":
    main()
