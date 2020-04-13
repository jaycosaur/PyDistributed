import struct
import typing
import logging

from .exceptions import OffsetMissingInIndex

INDEX_ENTRY_SIZE = 8
INDEX_FILE_SUFFIX = ".index"


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
