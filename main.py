import zmq
import threading as _threading
import multiprocessing
import pickle
import time
import random
import pydistributed
import gzip


def publish_thread(publisher):
    def run():
        publisher_id = random.randrange(0, 9999)
        while True:
            topic = str(random.randrange(1, 10))
            messagedata = "server#%s" % publisher_id
            print("%s %s" % (topic, messagedata))
            publisher.send(topic, messagedata)
            time.sleep(0.5)

    return _threading.Thread(target=run)


def subscriber_thread(subscriber):
    def run():
        subscriber.subscribe_all()
        time.sleep(2)
        for update_nbr in range(10):
            print("GOT", subscriber.receive())
        subscriber.unsubscribe_all()

    return _threading.Thread(target=run)


def service_ping():
    def callback(message, _topic, publisher):
        print("SERVICE PING", message)
        publisher.publish("pings", "ping")

    client = pydistributed.MessageBusClient()
    client.start()
    unregister = client.register_callback("pongs", callback,)
    time.sleep(0.5)
    client.publish("pings", "ping")

    time.sleep(10)
    unregister()
    client.shutdown()
    print("FINISHING")


def service_pong():
    client = pydistributed.MessageBusClient()
    client.subscribe("pings")
    time.sleep(0.5)
    while True:
        print("SERVICE PONG", client.receive())
        time.sleep(0.5)
        client.publish("pongs", "pong")


def bus_test():
    try:
        broker = pydistributed.MessageBusBroker()
        broker.start()

        ping = multiprocessing.Process(target=service_ping)
        pong = multiprocessing.Process(target=service_pong)
        file = multiprocessing.Process(target=to_file)
        pong.start()
        ping.start()
        file.start()
        broker.join()
    except Exception as e:
        print(e, "bringing down broker")
    finally:
        broker.shutdown()


def kv_client():
    client = pydistributed.KeyValueClient()
    client_id = client.client_id
    count = 0
    print("Client", client_id, "ready...")
    time.sleep(1)
    while count < 1000:
        client.set(client_id, count)
        res = client.get(client_id)
        print(count, "==", res)
        assert count == res
        count += 1
    print("Client", client_id, "finishing...")


def kv_test():
    try:
        store = pydistributed.KeyValueStore()
        time.sleep(2)  # allow time to bootup

        threads = [multiprocessing.Process(target=kv_client) for _ in range(4)]

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

    finally:
        store.shutdown()


if __name__ == "__main__":
    bus_test()
