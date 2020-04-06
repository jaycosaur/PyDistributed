import zmq
import threading as _threading
import pickle
import time
import random


class Publisher:
    def __init__(
        self, host="localhost", port="5559", context=zmq.Context(1), bind=False
    ):
        self.context = context
        self.socket = context.socket(zmq.PUB)
        if bind:
            self.socket.bind("tcp://*:%s" % (port))
        else:
            self.socket.connect("tcp://%s:%s" % (host, port))

    def send(self, topic: str, messagedata):
        self.socket.send_multipart(
            [topic.encode("utf8"), pickle.dumps(messagedata),]
        )

    def shutdown(self):
        self.socket.close()
        self.context.term()


class Subscriber:
    def __init__(
        self, host="localhost", port="5560", context=zmq.Context(1), bind=False
    ):
        self.context = context
        self.socket = context.socket(zmq.SUB)
        if bind:
            self.socket.bind("tcp://*:%s" % (port))
        else:
            self.socket.connect("tcp://%s:%s" % (host, port))
        self.subscriptions = set()

    def subscribe_all(self):
        for topic in [*self.subscriptions]:
            self.unsubscribe(topic)
        self.subscribe("")

    def unsubscribe_all(self):
        for topic in [*self.subscriptions]:
            self.unsubscribe(topic)

    def subscribe(self, topic: str):
        if topic not in self.subscriptions:
            self.subscriptions.add(topic)
            self.socket.setsockopt_string(zmq.SUBSCRIBE, topic)

    def unsubscribe(self, topic: str):
        if topic in self.subscriptions:
            self.subscriptions.remove(topic)
            self.socket.setsockopt_string(zmq.UNSUBSCRIBE, topic)

    def receive(self,):
        [topic, raw_msg] = self.socket.recv_multipart()
        return [topic.decode("utf8"), pickle.loads(raw_msg)]

    def shutdown(self):
        self.socket.close()
        self.context.term()


class Proxy(_threading.Thread):
    def __init__(self, context=zmq.Context(1), frontend_port=5559, backend_port=5560):
        self.context = context
        self.frontend = Subscriber(
            context=context, host="*", port=frontend_port, bind=True
        )
        self.backend = Publisher(
            context=context, host="*", port=backend_port, bind=True
        )

        self.frontend.subscribe_all()

        super().__init__()

    def run(self):
        zmq.device(zmq.FORWARDER, self.frontend.socket, self.backend.socket)

    def shutdown(self):
        self.frontend.shutdown()
        self.backend.shutdown()
        self.context.term()


class Hub:
    def __init__(self, context=zmq.Context(1), frontend_port=5559, backend_port=5560):
        self.context = context
        self.frontend = Subscriber(
            context=context, host="*", port=frontend_port, bind=True
        )
        self.backend = Publisher(
            context=context, host="*", port=backend_port, bind=True
        )

    def shutdown(self):
        self.frontend.shutdown()
        self.backend.shutdown()
        self.context.term()


class HubClient:
    def __init__(
        self,
        context=zmq.Context(1),
        keyvalue_host="localhost",
        publisher_port=5559,
        subscriber_port=5560,
    ):
        self.context = context
        self.publisher = Publisher(context=context, host=keyvalue_host, port=publisher_port)
        self.subscriber = Subscriber(context=context, host=keyvalue_host, port=subscriber_port)
