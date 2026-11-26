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
        client_id = None
        input_exchange = None
        query_end_counts = {query_id: 0 for query_id in QueryId}
        completed_queries = set()
        finished = False
        stopping = False
        last_received = {}

        def shutdown_socket():
            try:
                client_socket.shutdown(socket.SHUT_RDWR)
            except socket.error as error:
                logging.debug("Client already disconnected: %s", error)

        def request_shutdown(signum, frame):
            nonlocal stopping
            if stopping:
                return
            stopping = True
            shutdown_socket()
            if input_exchange is not None:
                input_exchange.stop_consuming_threadsafe()

        def handle_result(message, ack, nack):
            nonlocal finished
            try:
                message_type, fields = (
                    deserialize_msg(message)
                )
                client_id, message_id, producer_id, key, *fields = fields
                producer = (client_id, producer_id)
                if last_received.get(producer) == key:
                    ack()
                    return
                if message_type == InternalMsgType.QUERY_END:
                    query_id = QueryId(fields[0])
                    query_end_counts[query_id] += 1
                    if query_end_counts[query_id] == QUERY_REPLICAS[query_id]:
                        logging.info(
                            "Client %s completed query %s",
                            client_id,
                            query_id.name,
                        )
                        completed_queries.add(query_id)
                    finished = len(completed_queries) == len(QUERY_REPLICAS)
                    if finished:
                        send_msg(
                            client_socket,
                            ExternalMsgType.END_OF_RECORDS,
                            False,
                        )
                elif message_type == InternalMsgType.RESULT:
                    send_msg(client_socket, ExternalMsgType.RESULT, *fields)
                else:
                    raise ValueError(f"Unexpected handler message type: {message_type}")
            except Exception:
                nack()
                raise
            last_received[producer] = key
            ack()
            if finished:
                input_exchange.stop_consuming()

        signal.signal(signal.SIGTERM, request_shutdown)
        try:
            worker_pids[connection_id] = os.getpid()
            if sigterm_received.value or stopping:
                return
            message_type, fields = recv_msg(
                client_socket
            )
            if message_type != ExternalMsgType.CLIENT_ID:
                raise ValueError("Expected a client ID to register the connection")
            client_id, = fields
            if stopping:
                return
            input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, INPUT_EXCHANGE, [str(client_id)], consume=True
            )
            if stopping:
                return

            send_msg(
                client_socket,
                ExternalMsgType.ACK,
            )
            input_exchange.start_consuming(handle_result)
        except (ConnectionError, EOFError):
            logging.info("Client connection %s closed by the client", connection_id)
        except Exception:
            if not stopping:
                logging.exception("Client connection %s failed", connection_id)
        finally:
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            if input_exchange is not None:
                try:
                    input_exchange.close()
                except Exception:
                    logging.exception("Could not close the client's result connection")
            shutdown_socket()
            client_socket.close()
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
