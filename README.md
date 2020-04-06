# PyDistributed - lean communication infrastructure for distributed systems

**WORK IN PROGRESS**

PyDistributed allows the building of distributed applications easily in pure python!

Example use cases:

- Networking behind realtime distributed projects
- You have a bunch of python modules / scripts but no way to connect them together.
- You need to share global parameters between devices and programs.
- You have a network of devices and need to communicate between them.
- You have a network of Raspberry Pis but want to offload some GPU loads onto a Jetson Nano.
- You want to build an overly complicated ping / pong example.

### TODO

- [x] Message Bus
- [x] Key Value Store
- [ ] Authentication via ZAP https://github.com/zeromq/pyzmq/blob/master/examples/security/ironhouse.py
- [ ] Auto-discovery working with zeroconf networking
- [ ] Coverage Testing

## Message Bus

A pub/sub message bus.

Ping Pong Example

```python
def service_ping():
    client = pydistributed.MessageBusClient()
    client.subscribe("pongs")
    time.sleep(0.5)
    while True:
        client.publish("pings", "ping")
        print("GOT", client.receive())


def service_pong():
    client = pydistributed.MessageBusClient()
    client.subscribe("pings")
    time.sleep(0.5)
    while True:
        print("GOT", client.receive())
        client.publish("pongs", "pong")


try:
    broker = pydistributed.MessageBusBroker()
    broker.start()
    ping = multiprocessing.Process(target=service_ping)
    pong = multiprocessing.Process(target=service_pong)
    ping.start()
    pong.start()
    broker.join()
except Exception as e:
    print(e, "bringing down broker")
finally:
    broker.shutdown()
```

## Key Value Store

- A global key value store backed by zeromq and pickledb, persistent backing to json.

```python

def kv_client():
    client = pydistributed.keyvalue.KeyValueClient()
    client_id = client._client_id
    count = 0
    time.sleep(1)
    print("Client", client_id, "ready...")
    while count < 1000:
        client.set(client_id, count)
        res = client.get(client_id)
        print(count, "==", res)
        assert count == res
        count += 1
    print("Client", client_id, "finishing...")

try:
    store = pydistributed.keyvalue.KeyValueStore()
    time.sleep(2)  # allow time to bootup

    threads = [multiprocessing.Process(target=kv_client) for _ in range(4)]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()
finally:
    store.shutdown()

```
