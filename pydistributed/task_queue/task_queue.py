import threading
import pickle
import zmq
import uuid

""" 
This is a sync queue where clients submit tasks and then block until workers acknowledge messages
"""


class QueueBroker(threading.Thread):
    def __init__(self, context=zmq.Context(1), frontend_port=5559, backend_port=5560):
        self.context = context
        self.frontend = context.socket(zmq.XREP)
        self.frontend.bind("tcp://*:%s" % frontend_port)
        # Socket facing services
        self.backend = context.socket(zmq.XREQ)
        self.backend.bind("tcp://*:%s" % backend_port)
        super().__init__()

    def run(self):
        try:
            zmq.device(zmq.QUEUE, self.frontend, self.backend)
        except Exception as e:
            print("Exception caught in queue broker", e)
        self.shutdown()

    def shutdown(self):
        self.frontend.close()
        self.backend.close()
        self.context.term()


class QueueWorker(threading.Thread):
    def __init__(
        self, callback, host="localhost", worker_port=5560, context=zmq.Context(1),
    ):

        if not callable(callback):
            raise TypeError("callback must be callable")

        self.callback = callback

        self.context = context
        self.socket = context.socket(zmq.REP)
        self.socket.connect("tcp://%s:%s" % (host, worker_port))

        self.worker_id = uuid.uuid4().hex

        super().__init__()

    def run(self):
        try:
            while True:
                message_raw = self.socket.recv()
                message = pickle.loads(message_raw)
                self.callback(message)
                self.socket.send(pickle.dumps(self.worker_id))
        except Exception as e:
            print("Exception caught in QueueWorker", e)

        self.shutdown()

    def shutdown(self):
        self.socket.close()
        self.context.term()


class QueueClient:
    def __init__(
        self, host="localhost", client_port=5559, context=zmq.Context(1),
    ):
        self.context = context
        self.socket = context.socket(zmq.REQ)
        self.socket.connect("tcp://%s:%s" % (host, client_port))
        self.client_id = uuid.uuid4().hex

    def publish(self, messagedata, block=False):
        self.socket.send(pickle.dumps(messagedata))
        reply = self.socket.recv()
        return pickle.loads(reply)

    def shutdown(self):
        self.socket.close()
        self.context.term()
