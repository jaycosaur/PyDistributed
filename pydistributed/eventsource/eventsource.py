import struct
import typing
import os
import time
import logging

INDEX_ENTRY_SIZE = 8
INDEX_FILE_SUFFIX = ".index"
LOG_FILE_SUFFIX = ".log"


class OffsetMissingInIndex(Exception):
    pass


class CouldNotFindOffset(Exception):
    pass


class IndexFile:
    formatter = struct.Struct(
        "II"
    )  # entries are 8 bytes in total [file_offset, physical_position]

    def __init__(self, file_name: str, logger=logging.getLogger("index_file")):
        self._file_name = file_name
        self.logger = logger

    def write(self, relative_offset: int, physical_position: int) -> int:
        packed = self.formatter.pack(relative_offset, physical_position)
        with open(self._file_name, "ab+") as f:
            f.write(packed)
            return f.tell()

    def _read(self, index: int) -> bytes:
        with open(self._file_name, "rb") as f:
            f.seek(index * INDEX_ENTRY_SIZE, 0)
            return f.read(INDEX_ENTRY_SIZE)

    def read(self, index: int) -> typing.Tuple[int, int]:
        raw = self._read(index)
        data = self.formatter.unpack(raw)
        return typing.cast(typing.Tuple[int, int], data)

    def get_last_relative_offset(self) -> typing.Tuple[int, int]:
        with open(self._file_name, "rb") as f:
            f.seek(-INDEX_ENTRY_SIZE, 2)
            last_message = f.read(INDEX_ENTRY_SIZE)
            return typing.cast(
                typing.Tuple[int, int], self.formatter.unpack(last_message)
            )

    def search(self, relative_offset: int) -> typing.Tuple[int, int]:
        with open(self._file_name, "rb") as f:
            floor_index = 0
            f.seek(floor_index * INDEX_ENTRY_SIZE, 0)
            floor_buf = f.read(INDEX_ENTRY_SIZE)
            floor_off, floor_pos = self.formatter.unpack(floor_buf)
            if floor_off > relative_offset:
                raise OffsetMissingInIndex()
            ceil_offset = f.seek(-INDEX_ENTRY_SIZE, 2)
            ceil_index = ceil_offset // INDEX_ENTRY_SIZE
            ceil_buf = f.read(INDEX_ENTRY_SIZE)
            _ceil_off, _ceil_pos = self.formatter.unpack(ceil_buf)

            # start binary search
            iters = 0
            while True:
                iters += 1
                current_index = (floor_index + ceil_index) // 2
                f.seek(current_index * INDEX_ENTRY_SIZE)
                current_buf = f.read(INDEX_ENTRY_SIZE)
                current_off, current_pos = self.formatter.unpack(current_buf)
                if current_off == relative_offset:
                    self.logger.info("Search iterations: %d", iters)
                    return (current_off, current_pos)
                if current_off > relative_offset and current_index == floor_index + 1:
                    # must be between the current and the floor
                    self.logger.info("Search iterations: %d", iters)
                    return (floor_off, floor_pos)
                if current_off > relative_offset:
                    # adjust upper bound and retry
                    ceil_index = current_index
                    _ceil_off, _ceil_pos = current_off, current_pos
                    continue
                elif current_off < relative_offset and current_index == ceil_index - 1:
                    # must be between the current and the ceil
                    self.logger.info("Search iterations: %d", iters)
                    return (current_off, current_pos)
                elif current_off < relative_offset:
                    # adjust lower bound and retry
                    floor_index = current_index
                    floor_off, floor_pos = current_off, current_pos
                    continue
                else:
                    raise OffsetMissingInIndex()


MAX_LOG_SIZE_DEFAULT = 1 << 32  # ~ 4GB
DEFAULT_INDEX_INTERVAL = 1 << 12  # 4096B
MAX_MESSAGE_SIZE = 1 << 16  # 1 << 32 # ~ 4GB
META_DATA_SIZE = 20  # B


def filename_formatter(position: int):
    return format(position, "#020d")


class LogSizeExceeded(Exception):
    pass


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

    def get(self, offset: int) -> typing.Tuple[int, float, int, bytes]:
        return self._get(offset, 1)[0]

    def get_batch(
        self, offset: int, number_batch: int
    ) -> typing.List[typing.Tuple[int, float, int, bytes]]:
        return self._get(offset, number_batch)
