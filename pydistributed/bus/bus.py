import threading
import typing
from collections import defaultdict
from inspect import signature
import gzip
import pickle
import time

from pydistributed.shared import Proxy, Publisher, Subscriber


class MessageBusBroker(Proxy):
    pass


class MessageBusClient(threading.Thread):
    def __init__(self):
        self._publisher = Publisher()
        self._subscriber = Subscriber()
        self._callbacks = defaultdict(lambda: [])
        super().__init__()

    def publish(self, topic, message):
        self._publisher.send(topic, message)

    def publisher(self, topic):
        return lambda message: self._publisher.send(topic, message)

    def receive(self):
        return self._subscriber.receive()

    def subscribe(self, topic):
        return self._subscriber.subscribe(topic)

    def subscribe_all(self):
        return self._subscriber.subscribe_all()

    def unsubscribe_all(self):
        return self._subscriber.unsubscribe_all()

    def _unregister_callback(self, topic: str, callback):
        topic_callbacks = self._callbacks.get(topic)
        topic_callbacks.remove(callback)

    def register_callback(
        self,
        topic: str,
        callback: typing.Callable[
            [typing.Any, typing.Optional[str], typing.Optional["MessageBusClient"]],
            None,
        ],
    ):
        topic_callbacks = self._callbacks[topic]
        topic_callbacks.append(callback)
        self.subscribe(topic)
        return lambda: self._unregister_callback(topic, callback)

    def run(self):
        while True:
            [topic, event] = self.receive()
            topic_callbacks = self._callbacks.get(topic, [])
            callback_arguments = (
                event,
                topic,
                self,
            )
            for callback in topic_callbacks:
                params = signature(callback).parameters
                callback(
                    *callback_arguments[0 : len(params) + 1]
                )  # allow variadic arguments

    def wait(self):
        self.join()

    def shutdown(self):
        self._publisher.shutdown()
        self._subscriber.shutdown()


def stream_to_file(file_path):
    def callback(message, topic: str):
        with gzip.open(file_path) as file:
            pickle.dump(
                dict(topic=topic, message=message, timestamp=time.time()),
                file,
                pickle.HIGHEST_PROTOCOL,
            )

    return callback


def stream_from_file(file_path):
    with gzip.open(file_path, "rb") as f:
        while True:
            try:
                yield pickle.load(f)
            except EOFError:
                return
