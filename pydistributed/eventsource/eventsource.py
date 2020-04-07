import struct
import typing
import os
import time

INDEX_ENTRY_SIZE = 8
INDEX_FILE_SUFFIX = ".index"
LOG_FILE_SUFFIX = ".log"


class IndexFile:
    """[summary]
    """

    formatter = struct.Struct(
        "II"
    )  # entries are 8 bytes in total [file_offset, physical_position]

    def __init__(self, file_name: str):
        self._file_name = file_name

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
                raise Exception("Can't find that here!")
            ceil_offset = f.seek(-INDEX_ENTRY_SIZE, 2)
            ceil_index = ceil_offset // INDEX_ENTRY_SIZE
            ceil_buf = f.read(INDEX_ENTRY_SIZE)
            ceil_off, ceil_pos = self.formatter.unpack(ceil_buf)
            if ceil_off < relative_offset:
                raise Exception("Can't find that here!")

            # start binary search
            iters = 0
            while True:
                iters += 1
                current_index = (floor_index + ceil_index) // 2
                f.seek(current_index * INDEX_ENTRY_SIZE)
                current_buf = f.read(INDEX_ENTRY_SIZE)
                current_off, current_pos = self.formatter.unpack(current_buf)
                if current_off == relative_offset:
                    print("Search iterations: ", iters)
                    return (current_off, current_pos)
                if current_off > relative_offset and current_index == floor_index + 1:
                    # must be between the current and the floor
                    print("Search iterations: ", iters)
                    return (floor_off, floor_pos)
                if current_off > relative_offset:
                    # adjust upper bound and retry
                    ceil_index = current_index
                    ceil_off, ceil_pos = current_off, current_pos
                    continue
                elif current_off < relative_offset and current_index == ceil_index - 1:
                    # must be between the current and the ceil
                    print("Search iterations: ", iters)
                    return (current_off, current_pos)
                elif current_off < relative_offset:
                    # adjust lower bound and retry
                    floor_index = current_index
                    floor_off, floor_pos = current_off, current_pos
                    continue
                else:
                    raise Exception("Could not find!")


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

    def _scan_for_message(self, start_position: int, offset: int):
        # progresses through file linearly
        scan_iterations = 0
        with open(self.__log_file_name, "rb+") as f:
            f.seek(start_position, 0)
            while True:
                raw_meta = f.read(META_DATA_SIZE)
                message_offset, timestamp, payload_size = self.meta_formatter.unpack(
                    raw_meta
                )
                if message_offset == offset:
                    payload = f.read(payload_size)
                    print("Number of scan iterations:", scan_iterations)
                    return (message_offset, timestamp, payload_size, payload)
                f.seek(payload_size, 1)
                scan_iterations += 1

    def _read(self, offset: int):
        pass

    def get(self, offset: int):  # should also take a number of messages!
        _index_file_offset, physical_position = self.__index_file.search(
            offset - self.__log_initial_offset
        )
        print(
            "Closest match in index file:",
            self.__log_initial_offset + _index_file_offset,
            "@ position",
            physical_position,
        )
        return self._scan_for_message(physical_position, offset)

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
                    print(f"Get last took {scan_iterations} iterations")
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

        init_data = self._initialise_logs(log_store_path)

        if init_data is None:
            self.__last_offset = None
            self.__current_log_file = LogFile(
                self.log_store_path,
                0,
                max_log_size=self.max_log_size,
                index_interval=self.index_interval,
            )

    def _initialise_logs(
        self, log_store_path: str
    ) -> typing.Optional[typing.Tuple[int, int]]:
        # scan logs store to get last known offset
        files = self._get_log_initial_indexes()
        if len(files) == 0:
            return None

        self.__current_log_file = LogFile(
            self.log_store_path,
            files[-1],
            max_log_size=self.max_log_size,
            index_interval=self.index_interval,
        )
        self.__last_offset = self.__current_log_file.get_last_offset()

        return files[-1], self.__last_offset

    def _get_log_initial_indexes(self) -> typing.List[int]:
        files = os.listdir(self.log_store_path)
        files_ext = [os.path.splitext(f) for f in files]
        log_files = [int(base) for base, ext in files_ext if ext == LOG_FILE_SUFFIX]
        log_files.sort()
        return log_files

    def _scan_log_files(self, offset: int) -> typing.Optional[int]:
        log_files = self._get_log_initial_indexes()
        print(f"Found {len(log_files)} log files")
        log_index = None
        for file_index in log_files:
            if log_index is not None and file_index > offset:
                return log_index
            log_index = file_index
        return log_index

    def write(self, payload: bytes):
        next_offset = 0 if self.__last_offset is None else self.__last_offset + 1
        while True:
            try:
                self.__current_log_file.write(
                    next_offset, payload
                )  # off by one error here somewhere
                self.__last_offset = next_offset
                break
            except LogSizeExceeded:
                print("Log file size exceeded for offset", next_offset)
                # move onto next log file
                self.__current_log_file = LogFile(
                    self.log_store_path,
                    next_offset,
                    max_log_size=self.max_log_size,
                    index_interval=self.index_interval,
                )

    def get(self, offset: int):
        log_offset = self._scan_log_files(offset)
        if log_offset is None:
            raise Exception("Could not find sir.")
        log_file = LogFile(
            self.log_store_path, log_offset
        )  # this should be cached somewhere
        return log_file.get(offset)


import shutil


def cleanup(folder_path):
    for file_object in os.listdir(folder_path):
        file_object_path = os.path.join(folder_path, file_object)
        if os.path.isfile(file_object_path) or os.path.islink(file_object_path):
            os.unlink(file_object_path)
        else:
            shutil.rmtree(file_object_path)


cleanup("logs")


# event_source = EventSource(max_log_size=1<<20)


# def data_gen(index: int) -> bytes:
#     base_str = f"this is a payload for message {index}"
#     rep = base_str * (index % 50)
#     return rep.encode("utf8")

# # range of positions from 2048 -> 2048000
# for i in range(100000):
#    event_source.write(data_gen(i))


print(event_source.get(0), event_source.get(20382))

