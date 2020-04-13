import struct
import typing
import os
import time
import logging

from .index_file import IndexFile
from .exceptions import LogSizeExceeded, CouldNotFindOffset

INDEX_FILE_SUFFIX = ".index"
LOG_FILE_SUFFIX = ".log"

MAX_LOG_SIZE_DEFAULT = 1 << 32  # ~ 4GB
DEFAULT_INDEX_INTERVAL = 1 << 12  # 4096B
MAX_MESSAGE_SIZE = 1 << 16  # 1 << 32 # ~ 4GB
META_DATA_SIZE = 20  # B


def filename_formatter(position: int):
    return format(position, "#020d")


class LogFile:
    meta_formatter = struct.Struct(
        "QQI"
    )  # entries are 24 bytes in total [offset, nanosecond timestamp, size of message buffer]

    def __init__(
        self,
        log_store_path: str,
        absolute_offset: int,
        max_log_size: int = MAX_LOG_SIZE_DEFAULT,
        index_interval: int = DEFAULT_INDEX_INTERVAL,
        logger=logging.getLogger("log_file"),
    ):
        self.__max_log_size = max_log_size
        self.__index_interval = index_interval
        self.__log_initial_offset = absolute_offset
        self.__index_file_name = os.path.join(
            log_store_path, filename_formatter(absolute_offset) + INDEX_FILE_SUFFIX
        )
        self.__log_file_name = os.path.join(
            log_store_path, filename_formatter(absolute_offset) + LOG_FILE_SUFFIX
        )
        self.__index_file = IndexFile(self.__index_file_name)
        self.__last_index_size: typing.Optional[int] = None
        self.logger = logger

    def _write(self, offset: int, data: bytes):
        with open(self.__log_file_name, "ab+") as f:
            pre_log_size = f.tell()
            if pre_log_size + len(data) > self.__max_log_size:
                raise LogSizeExceeded

            data_size = f.write(data)
            log_size = pre_log_size + data_size
            if (
                self.__last_index_size is None
                or log_size > self.__last_index_size + self.__index_interval
            ):
                self.__index_file.write(
                    offset - self.__log_initial_offset, pre_log_size,
                )
                self.__last_index_size = log_size
            return log_size

    def write(self, offset: int, payload: bytes) -> int:
        if len(payload) > MAX_MESSAGE_SIZE:
            raise ValueError(f"Data must be at most {MAX_MESSAGE_SIZE} bytes")
        meta_buf = self.meta_formatter.pack(offset, time.time_ns(), len(payload))
        return self._write(offset, meta_buf + payload)

    def _scan_for_message(
        self, start_position: int, offset: int, offset_last: int
    ) -> typing.List[typing.Tuple[int, float, int, bytes]]:
        scan_iterations = 0  # for performance tests
        results_to_return: typing.List[typing.Tuple[int, float, int, bytes]] = []
        with open(self.__log_file_name, "rb+") as f:
            f.seek(start_position, 0)
            while True:
                raw_meta = f.read(META_DATA_SIZE)
                if len(raw_meta) == 0:
                    # EOF
                    break
                message_offset, timestamp, payload_size = self.meta_formatter.unpack(
                    raw_meta
                )
                if message_offset == offset_last:
                    payload = f.read(payload_size)
                    results_to_return.append(
                        (message_offset, timestamp, payload_size, payload)
                    )
                    break
                if message_offset >= offset:
                    payload = f.read(payload_size)
                    results_to_return.append(
                        (message_offset, timestamp, payload_size, payload)
                    )
                else:
                    f.seek(payload_size, 1)
                scan_iterations += 1
        self.logger.info("Number of scan iterations: %s", scan_iterations)
        return results_to_return

    def _read(self, offset: int):
        pass

    def get(
        self, offset: int, offset_end: typing.Optional[int] = None
    ) -> typing.List[typing.Tuple[int, float, int, bytes]]:
        # if offset_end is -1 it means to load until the end of the file
        index_start_file_offset, physical_start_position = self.__index_file.search(
            offset - self.__log_initial_offset
        )

        if offset_end is None:
            offset_end = offset
            index_end_file_offset, physical_end_position = (
                index_start_file_offset,
                physical_start_position,
            )
        elif offset_end == -1:
            index_end_file_offset, physical_end_position = -1, -1
        elif offset_end == offset:
            index_end_file_offset, physical_end_position = (
                index_start_file_offset,
                physical_start_position,
            )
        else:
            index_end_file_offset, physical_end_position = self.__index_file.search(
                offset_end - self.__log_initial_offset
            )

        self.logger.info(
            "Closest match in index file: %d @ position %d to %d @ position %d",
            self.__log_initial_offset + index_start_file_offset,
            physical_start_position,
            self.__log_initial_offset + index_end_file_offset,
            physical_end_position,
        )
        return self._scan_for_message(physical_start_position, offset, offset_end)

    def get_last_offset(self) -> int:
        (
            _index_file_offset,
            physical_position,
        ) = self.__index_file.get_last_relative_offset()
        with open(self.__log_file_name, "rb+") as f:
            last_offset = f.seek(physical_position, 0)
            scan_iterations = 0
            while True:
                raw_meta = f.read(META_DATA_SIZE)
                if len(raw_meta) == 0:
                    self.logger.info("Get last took %s iterations", scan_iterations)
                    f.seek(last_offset, 0)
                    raw_meta = f.read(META_DATA_SIZE)
                    (
                        message_offset,
                        _timestamp,
                        _payload_size,
                    ) = self.meta_formatter.unpack(raw_meta)
                    return message_offset
                message_offset, _, payload_size = self.meta_formatter.unpack(raw_meta)
                last_offset = f.tell() - META_DATA_SIZE
                f.seek(payload_size, 1)
                scan_iterations += 1
