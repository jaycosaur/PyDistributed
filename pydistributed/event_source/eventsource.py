import typing
import os
import logging
from dataclasses import dataclass

from .log_file import (
    LogFile,
    MAX_LOG_SIZE_DEFAULT,
    DEFAULT_INDEX_INTERVAL,
    LOG_FILE_SUFFIX,
)

from .exceptions import CouldNotFindOffset, LogSizeExceeded


@dataclass
class Event:
    offset: int
    timestamp: float
    message_size: int
    data: bytes

    @staticmethod
    def from_tuple(data: typing.Tuple[int, float, int, bytes]):
        return Event(*data)


class EventSource:
    def __init__(
        self,
        log_store_path="logs",
        max_log_size: int = MAX_LOG_SIZE_DEFAULT,
        index_interval: int = DEFAULT_INDEX_INTERVAL,
    ):
        self.max_log_size = max_log_size
        self.index_interval = index_interval
        self.log_store_path = log_store_path
        self.__last_offset: typing.Optional[int] = None
        self._log_files: typing.List[int] = []
        self.logger = logging.getLogger("event_source")

        init_data = self._initialise_logs(log_store_path)

        if init_data is None:
            self.__last_offset = None
            self.__current_log_file = LogFile(
                self.log_store_path,
                0,
                max_log_size=self.max_log_size,
                index_interval=self.index_interval,
            )
            self._log_files.append(0)

    def _initialise_logs(
        self, log_store_path: str
    ) -> typing.Optional[typing.Tuple[int, int]]:
        # scan logs store to get last known offset
        files = self._get_log_initial_indexes()
        if len(files) == 0:
            return None

        log_file = LogFile(
            self.log_store_path,
            files[-1],
            max_log_size=self.max_log_size,
            index_interval=self.index_interval,
        )
        last_offset = log_file.get_last_offset()

        self.__current_log_file = log_file
        self.__last_offset = last_offset

        return files[-1], last_offset

    def _get_log_initial_indexes(self) -> typing.List[int]:
        files = os.listdir(self.log_store_path)
        files_ext = [os.path.splitext(f) for f in files]
        log_files = [int(base) for base, ext in files_ext if ext == LOG_FILE_SUFFIX]
        log_files.sort()
        return log_files

    def _scan_log_files(self, offset: int, end_offset: int) -> typing.List[int]:
        log_files = self._log_files
        if len(log_files) == 0:
            log_files = self._get_log_initial_indexes()
            self._log_files = log_files
        log_indexes: typing.List[int] = []
        for idx, file_index in enumerate(log_files):
            if file_index >= offset:
                if file_index != offset:
                    log_indexes.append(log_files[idx - 1])
                if file_index <= end_offset and idx == len(log_files) - 1:
                    log_indexes.append(file_index)
            if file_index > end_offset:
                break

        return log_indexes

    def write(self, payload: bytes):
        next_offset = 0 if self.__last_offset is None else self.__last_offset + 1
        while True:
            try:
                self.__current_log_file.write(next_offset, payload)
                self.__last_offset = next_offset
                break
            except LogSizeExceeded:
                self.logger.info("Log file size exceeded for offset %s", next_offset)
                self._log_files.append(next_offset)
                self.__current_log_file = LogFile(
                    self.log_store_path,
                    next_offset,
                    max_log_size=self.max_log_size,
                    index_interval=self.index_interval,
                )

    def _get(
        self, offset: int, number_of_results: int
    ) -> typing.List[typing.Tuple[int, float, int, bytes]]:
        final_offset = offset - 1 + number_of_results
        log_file_indexes = self._scan_log_files(offset, final_offset)
        self.logger.info("_get results in a log span of %s", len(log_file_indexes))
        if len(log_file_indexes) == 0:
            raise CouldNotFindOffset()

        if len(log_file_indexes) == 1:
            log_file = LogFile(self.log_store_path, log_file_indexes[0])
            return log_file.get(offset, final_offset)

        results: typing.List[typing.Tuple[int, float, int, bytes]] = []
        for idx, log_file_index in enumerate(log_file_indexes):
            if log_file_index > offset:
                start_offset = log_file_index
            else:
                start_offset = offset
            if idx < len(log_file_indexes) - 1:  # if not last file
                end_offset = -1  # to end of index file
            else:
                end_offset = final_offset
            log_file = LogFile(self.log_store_path, log_file_index)
            log_result = log_file.get(start_offset, end_offset)
            results.extend(log_result)
        return results

    def get(self, offset: int) -> Event:
        return Event.from_tuple(self._get(offset, 1)[0])

    def get_batch(self, offset: int, number_batch: int) -> typing.List[Event]:
        return [Event.from_tuple(data) for data in self._get(offset, number_batch)]
