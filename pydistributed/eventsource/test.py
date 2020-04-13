import unittest
import os
import shutil
from eventsource import EventSource
import logging

# logging.basicConfig(level="DEBUG")


def cleanup(folder_path):
    for file_object in os.listdir(folder_path):
        file_object_path = os.path.join(folder_path, file_object)
        if os.path.isfile(file_object_path) or os.path.islink(file_object_path):
            os.unlink(file_object_path)
        else:
            shutil.rmtree(file_object_path)


def encode_data(index: int) -> bytes:
    return index.to_bytes(256, "little")


def decode_data(data: bytes) -> int:
    return int.from_bytes(data, "little")


LOG_PATH = "logs"


class EventSourceTest(unittest.TestCase):
    def setUp(self):
        self.event_source = EventSource(log_store_path=LOG_PATH, max_log_size=1 << 20)
        for i in range(5000):
            self.event_source.write(encode_data(i))

    def tearDown(self):
        cleanup(LOG_PATH)

    def test_get_any_item(self):
        idx = 1023
        item = self.event_source.get(idx)
        self.assertEqual(item[0], idx)
        self.assertEqual(decode_data(item[3]), idx)

    def test_get(self):
        idx = 0
        item = self.event_source.get(idx)
        self.assertEqual(item[0], idx)
        self.assertEqual(decode_data(item[3]), idx)

    def test_get_last_item(self):
        idx = self.event_source._log_files[1] - 1
        item = self.event_source.get(idx)
        self.assertEqual(item[0], idx)
        self.assertEqual(decode_data(item[3]), idx)

    def test_get_first_item(self):
        idx = self.event_source._log_files[1]
        item = self.event_source.get(idx)
        self.assertEqual(item[0], idx)
        self.assertEqual(decode_data(item[3]), idx)

    def test_batch(self):
        idx_start = self.event_source._log_files[1] - 100
        idx_end = self.event_source._log_files[1] + 100
        number_of_items = idx_end - idx_start
        first_offset = idx_start
        batch = self.event_source.get_batch(first_offset, number_of_items)
        self.assertEqual(len(batch), number_of_items)
        self.assertEqual(batch[0][0], first_offset)
        self.assertEqual(batch[-1][0], first_offset + number_of_items - 1)
