#!/usr/bin/env python3
from abc import ABC, abstractmethod
from typing import Any
from typing import Protocol


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

    def get_name(self) -> str:
        name = self.__class__.__name__
        return name.replace("Processor", " Processor").strip()

    def get_total(self) -> int:
        return self._rank

    def get_remaining(self) -> int:
        return len(self._storage)


class ExportPlugin(Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        pass


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


class DataStream:
    def __init__(self) -> None:
        self._registered: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self._registered.append(proc)

    def process_stream(self, stream: list[Any]) -> None:
        for element in stream:
            validated: bool = False
            for proc in self._registered:
                if proc.validate(element):
                    proc.ingest(element)
                    validated = True
                    break
            if not validated:
                print("DataStream error",
                      f"- Can't process element in stream: {element}")

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")
        if len(self._registered) == 0:
            print("No processor found, no data")
            return
        for proc in self._registered:
            print(f"{proc.get_name()}:",
                  f"total {proc.get_total()} items processed,",
                  f"remaining {proc.get_remaining()} on processor")

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for proc in self._registered:
            collected: list[tuple[int, str]] = []
            for _ in range(nb):
                try:
                    collected.append(proc.output())
                except IndexError:
                    break
            if collected:
                plugin.process_output(collected)


class CSVPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        print("CSV Output:")
        csv_str: list[str] = []
        for element in data:
            csv_str.append(element[1])
        print(",".join(csv_str))


class JSONPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        print("JSON Output:")
        json_dict: dict[str, str] = {}
        for element in data:
            json_dict[f"item_{element[0]}"] = element[1]
        junction = ", ".join(
            f'"{key}": "{value}"'
            for key, value in json_dict.items()
        )
        print("{" + junction + "}")


BATCH: list[Any] = [
    'Hello world',
    [3.14, -1, 2.71],
    [
        {'log_level': 'WARNING',
         'log_message': 'Telnet access! Use ssh instead'},
        {'log_level': 'INFO', 'log_message': 'User wil is connected'}
    ],
    42,
    ['Hi', 'five']
]


BATCH2: list[Any] = [
    21,
    ['I love AI', 'LLMs are wonderful', 'Stay healthy'],
    [
        {'log_level': 'ERROR', 'log_message': '500 server crash'},
        {'log_level': 'NOTICE',
         'log_message': 'Certificate expires in 10 days'}
    ],
    [32, 42, 64, 84, 128, 168],
    'World hello'
]


def main() -> None:
    print("=== Code Nexus - Data Pipeline ===")
    print()
    print("Initialize Data Stream...")
    dstream = DataStream()
    dstream.print_processors_stats()
    print()
    print("Registering Processors")
    print()
    nproc = NumericProcessor()
    tproc = TextProcessor()
    lproc = LogProcessor()
    dstream.register_processor(nproc)
    dstream.register_processor(tproc)
    dstream.register_processor(lproc)
    print("Send first batch of data on stream:", BATCH)
    dstream.process_stream(BATCH)
    dstream.print_processors_stats()
    print()
    print("Send 3 processed data from each processor to a CSV plugin:")
    csvp = CSVPlugin()
    dstream.output_pipeline(3, csvp)
    print()
    dstream.print_processors_stats()
    print()
    print("Send another batch of data:", BATCH2)
    dstream.process_stream(BATCH2)
    dstream.print_processors_stats()
    print()
    print("Send 5 processed data from each processor to a JSON plugin:")
    jsonp = JSONPlugin()
    dstream.output_pipeline(5, jsonp)
    print()
    dstream.print_processors_stats()
    print()


if __name__ == "__main__":
    main()
