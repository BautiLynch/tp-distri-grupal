import os
import logging
import socket
import signal
import threading
import multiprocessing
import sys

from common import middleware
from common.heartbeat import HeartbeatSender
from common.message_protocol.internal import deserialize_msg, MsgType as InternalMsgType
from common.message_protocol.external import recv_msg, send_msg, MsgType as ExternalMsgType
from common.message_protocol.queries import QueryId

SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])
MOM_HOST = os.environ["MOM_HOST"]
HEARTBEAT_QUEUE = os.environ["HEARTBEAT_QUEUE"]
HEARTBEAT_INTERVAL = int(os.environ.get("HEARTBEAT_INTERVAL", 5))
INPUT_EXCHANGE = os.environ["INPUT_EXCHANGE"]

QUERY_REPLICAS = {
    query_id: int(os.environ[f"QUERY_{query_id.value}_REPLICAS"])
    for query_id in QueryId
}
if any(amount < 1 for amount in QUERY_REPLICAS.values()):
    raise ValueError("Every QUERY_<id>_REPLICAS must be greater than zero")


def initialize_worker():
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    signal.signal(signal.SIGINT, signal.SIG_IGN)


class ClientHandler:
    def __init__(self, client_socket):
        self.client_socket = client_socket
        self.client_id = None
        self.input_exchange = None
        self.query_end_counts = {query_id: 0 for query_id in QueryId}
        self.completed_queries = set()
        self.finished = False
        self.stopping = False
        self.last_received = {}
        signal.signal(signal.SIGTERM, self.handle_sigterm)

    def run(self):
        if self.stopping:
            return
        message_type, fields = recv_msg(self.client_socket)
        if message_type != ExternalMsgType.CLIENT_ID:
            raise ValueError("Expected a client ID to register the connection")
        self.client_id, = fields
        if self.stopping:
            return
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, INPUT_EXCHANGE, [str(self.client_id)], consume=True
        )
        if self.stopping:
            return
        send_msg(self.client_socket, ExternalMsgType.ACK)
        self.input_exchange.start_consuming(self.handle_result)

    def handle_result(self, message, ack, nack):
        try:
            message_type, fields = deserialize_msg(message)
            client_id, message_id, producer_id, key, *fields = fields
            producer = (client_id, producer_id)
            if self.last_received.get(producer) == key:
                ack()
                return
            if message_type == InternalMsgType.QUERY_END:
                query_id = QueryId(fields[0])
                self.query_end_counts[query_id] += 1
                if self.query_end_counts[query_id] == QUERY_REPLICAS[query_id]:
                    logging.info(
                        "Client %s completed query %s", self.client_id, query_id.name,
                    )
                    self.completed_queries.add(query_id)
                self.finished = len(self.completed_queries) == len(QUERY_REPLICAS)
                if self.finished:
                    send_msg(self.client_socket, ExternalMsgType.END_OF_RECORDS, False)
            elif message_type == InternalMsgType.RESULT:
                send_msg(self.client_socket, ExternalMsgType.RESULT, *fields)
            else:
                raise ValueError(f"Unexpected handler message type: {message_type}")
        except Exception:
            nack()
            raise
        self.last_received[producer] = key
        ack()
        if self.finished:
            self.input_exchange.stop_consuming()

    def handle_sigterm(self, signum, frame):
        if self.stopping:
            return
        self.stopping = True
        self.shutdown_socket()
        if self.input_exchange is not None:
            self.input_exchange.stop_consuming_threadsafe()

    def shutdown_socket(self):
        try:
            self.client_socket.shutdown(socket.SHUT_RDWR)
        except socket.error as error:
            logging.debug("Client already disconnected: %s", error)

    def close(self):
        if self.input_exchange is not None:
            try:
                self.input_exchange.close()
            except Exception:
                logging.exception("Could not close the client's result connection")
        self.shutdown_socket()
        self.client_socket.close()


class Handler:
    def __init__(self):
        self.heartbeat = HeartbeatSender(
            MOM_HOST, HEARTBEAT_QUEUE, socket.gethostname(), "handler", HEARTBEAT_INTERVAL
        )
        self.shutdown_requested = threading.Event()
        signal.signal(signal.SIGTERM, self.handle_sigterm)
        signal.signal(signal.SIGINT, self.handle_sigterm)

    def handle_sigterm(self, signum, frame):
        self.shutdown_requested.set()

    @staticmethod
    def handle_client_response(client_socket, connection_id, client_dict, worker_pids, sigterm_received):
        client_handler = ClientHandler(client_socket)
        try:
            worker_pids[connection_id] = os.getpid()
            if not sigterm_received.value and not client_handler.stopping:
                client_handler.run()
        except (ConnectionError, EOFError):
            logging.info("Client connection %s closed by the client", connection_id)
        except Exception:
            if not client_handler.stopping:
                logging.exception("Client connection %s failed", connection_id)
        finally:
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            client_handler.close()
            client_dict.pop(connection_id, None)
            worker_pids.pop(connection_id, None)

    @staticmethod
    def stop_clients(client_dict, worker_pids, sigterm_received):
        sigterm_received.value = 1
        for connection in list(client_dict.values()):
            try:
                connection.shutdown(socket.SHUT_RDWR)
            except socket.error as error:
                logging.debug("Socket already disconnected during shutdown: %s", error)
        for worker_pid in list(worker_pids.values()):
            try:
                os.kill(worker_pid, signal.SIGTERM)
            except ProcessLookupError:
                logging.debug("Worker %s already stopped", worker_pid)

    def receive_client(self):
        with multiprocessing.Manager() as manager:
            client_dict = manager.dict()
            worker_pids = manager.dict()
            sigterm_received = manager.Value("c_short", 0)
            with multiprocessing.Pool(
                processes=os.process_cpu_count(),
                initializer=initialize_worker,
            ) as processes_pool:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
                    server_socket.bind((SERVER_HOST, SERVER_PORT))
                    server_socket.listen()
                    server_socket.settimeout(0.5)
                    connection_id = 0
                    try:
                        if not self.shutdown_requested.is_set():
                            self.heartbeat.start()
                        while not self.shutdown_requested.is_set():
                            try:
                                client_socket, _ = server_socket.accept()
                                connection_id += 1
                                client_dict[connection_id] = client_socket
                                processes_pool.apply_async(
                                    Handler.handle_client_response,
                                    (client_socket, connection_id, client_dict,
                                     worker_pids, sigterm_received),
                                    callback=lambda _, connection=client_socket: connection.close(),
                                    error_callback=lambda error, connection=client_socket: (
                                        connection.close(),
                                        logging.error("Handler worker failed: %s", error),
                                    ),
                                )
                            except socket.timeout:
                                continue
                            except socket.error:
                                if not self.shutdown_requested.is_set():
                                    logging.exception("The connection with the client was lost")
                                    return 1
                            except Exception:
                                logging.exception("Error accepting a client")
                                return 2
                    finally:
                        self.stop_clients(client_dict, worker_pids, sigterm_received)
                        processes_pool.close()
                        processes_pool.join()
        return 0

    def close(self):
        self.shutdown_requested.set()
        self.heartbeat.stop()

    def start(self):
        try:
            return self.receive_client()
        finally:
            self.close()


def main():
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s.%(msecs)03d | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler = Handler()
    return handler.start()


if __name__ == "__main__":
    sys.exit(main())
