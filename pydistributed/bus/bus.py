from pydistributed.shared import Proxy, Publisher, Subscriber


class MessageBusBroker(Proxy):
    pass


class MessageBusClient:
    def __init__(self):
        self._publisher = Publisher()
        self._subscriber = Subscriber()

    def publish(self, topic, message):
        self._publisher.send(topic, message)

    def receive(self):
        return self._subscriber.receive()

    def subscribe(self, topic):
        return self._subscriber.subscribe(topic)

    def subscribe_all(self):
        return self._subscriber.subscribe_all()

    def unsubscribe_all(self):
        return self._subscriber.unsubscribe_all()
