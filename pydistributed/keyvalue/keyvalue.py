import typing as _typing
import uuid
import threading as _threading
import pickle
import pickledb
import zmq

from pydistributed.shared import Proxy, Hub, Subscriber, Publisher, HubClient


DEFAULT_PATH = "tmp/keyvalue.db"

# ACTIONS


class GetAction:
    def __init__(self, key: str, subscribe=False):
        self.key = key
        self.subscribe = subscribe


class GetResultAction:
    def __init__(self, key: str, value: _typing.Any):
        self.key = key
        self.value = value


class GetExceptionAction:
    pass


class SetAction:
    def __init__(self, key: str, value: _typing.Any):
        self.key = key
        self.value = value


class SetResultAction:
    def __init__(self, key: str, value: _typing.Any):
        self.key = key
        self.value = value


class SetExceptionAction:
    pass


class SubscribeAction:
    def __init__(self, key: str):
        self.key = key


class UnsubscribeAction:
    def __init__(self, key: str):
        self.key = key


class ExceptionAction:
    pass


# Database


class KeyValueStore(pickledb.PickleDB):
    def __init__(
        self,
        initial: dict = {},
        db_file_path: str = DEFAULT_PATH,
        auto_backup=True,
        keyvalue_host="localhost",
        publisher_port=5559,
        subscriber_port=5560,
        enable_logging=False,
    ):
        super().__init__(db_file_path, auto_backup, False)

        for (key, value) in initial.items():
            self.set(key, value)

        self._context = zmq.Context()

        self._proxy = Hub(context=self._context)

        self._publisher = self._proxy.backend
        self._subscriber = self._proxy.frontend

        self._subscriber.subscribe_all()

        self._subscriber_worker = _threading.Thread(target=self.subscriber_worker)
        self._subscriber_worker.start()
        self.logging = enable_logging

    def subscriber_worker(self):
        try:
            while True:
                [client_id, action] = self._subscriber.receive()
                if isinstance(action, GetAction):
                    if self.logging:
                        print("GET ACTION", client_id, f"{action.key}")
                    value_exists = self.exists(action.key)
                    if value_exists:
                        self._publisher.send(
                            client_id,
                            GetResultAction(key=action.key, value=self.get(action.key)),
                        )
                    else:
                        self._publisher.send(
                            client_id, GetExceptionAction(),
                        )
                elif isinstance(action, SetAction):
                    if self.logging:
                        print("SET ACTION", client_id, f"{action.key}:{action.value}")
                    self.set(action.key, action.value)
                    self._publisher.send(
                        client_id, SetResultAction(key=action.key, value=action.value),
                    )
        except zmq.error.ContextTerminated:
            pass
        print("DB Worker shutting down.")

    def shutdown(self):
        self._proxy.shutdown()
        self._publisher.shutdown()
        self._subscriber.shutdown()
        self._context.term()


class KeyValueClient:
    def __init__(
        self, keyvalue_host="localhost", publisher_port=5559, subscriber_port=5560,
    ):
        self._client_id = uuid.uuid4().hex

        self._hub_client = HubClient(
            keyvalue_host="localhost", publisher_port=5559, subscriber_port=5560,
        )

        self._publisher = self._hub_client.publisher
        self._subscriber = self._hub_client.subscriber

        self._subscriber.subscribe(self._client_id)

    def get(self, key: str) -> _typing.Any:
        self._publisher.send(self._client_id, GetAction(key))
        [_client_id, action] = self._subscriber.receive()
        if isinstance(action, GetResultAction):
            return action.value
        elif isinstance(action, GetExceptionAction):
            raise Exception("GOT EXCEPTION ON GET")

    def set(self, key: str, value: _typing.Any):
        self._publisher.send(self._client_id, SetAction(key, value))
        [_client_id, action] = self._subscriber.receive()
        if isinstance(action, SetResultAction):
            return True
        elif isinstance(action, GetExceptionAction):
            return False

    def shutdown(self):
        self._publisher.shutdown()
        self._subscriber.shutdown()
