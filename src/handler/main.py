import os
import logging
import socket
import signal
import multiprocessing

from common import middleware
from common.message_protocol.internal import deserialize_msg, serialize_payload, MsgType as InternalMsgType
from common.message_protocol.external import recv_msg, send_msg, MsgType as ExternalMsgType

SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_EXCHANGE = os.environ["INPUT_EXCHANGE"]

QUERY_REPLICAS = {
    "q1": int(os.environ["QUERY_1_REPLICAS"]),
    "q2": int(os.environ["QUERY_2_REPLICAS"]),
    "q3": int(os.environ["QUERY_3_REPLICAS"]),
    "q4": int(os.environ["QUERY_4_REPLICAS"]),
    "q5": int(os.environ["QUERY_5_REPLICAS"]),
}
if any(amount < 1 for amount in QUERY_REPLICAS.values()):
    raise ValueError("Every QUERY_<id>_REPLICAS must be greater than zero")


class ClientHandler:
    def __init__(self, client_socket):
        self.client_socket = client_socket
        self.input_exchange = None
        self.query_end_counts = {
            "q1": 0,
            "q2": 0,
            "q3": 0,
            "q4": 0,
            "q5": 0,
        }
        self.completed_queries = set()
        self.finished = False
        self.stopping = False

    def run(self):
        message_type, client_id = recv_msg(
            self.client_socket
        )
        if message_type != ExternalMsgType.CLIENT_ID:
            raise ValueError("Expected a client ID to register the connection")
        if self.stopping:
            return
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, INPUT_EXCHANGE, [str(client_id)], consume=True
        )
        if self.stopping:
            return

        send_msg(
            self.client_socket,
            ExternalMsgType.ACK,
        )
        self.input_exchange.start_consuming(self.handle_result)

    def handle_result(self, message, ack, nack):
        try:
            message_type, _, fields = (
                deserialize_msg(message)
            )
            if message_type == InternalMsgType.QUERY_END:
                query_id, = fields
                query_name = f"q{query_id}"
                self.query_end_counts[query_name] += 1
                if self.query_end_counts[query_name] == QUERY_REPLICAS[query_name]:
                    logging.info(
                        "Client %s completed query %s",
                        self.client_socket.getpeername(),
                        query_name,
                    )
                    self.completed_queries.add(query_name)
                self.finished = len(self.completed_queries) == len(QUERY_REPLICAS)
                if self.finished:
                    send_msg(
                        self.client_socket,
                        ExternalMsgType.END_OF_RECORDS,
                    )
            else:
                payload = serialize_payload(message_type, *fields)
                send_msg(
                    self.client_socket, ExternalMsgType.RESULT, message_type,
                    payload,
                )
        except Exception:
            nack()
            raise
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


def handle_client_response(client_socket, connection_id, client_dict, worker_pids, sigterm_received):
    client_handler = ClientHandler(client_socket)
    signal.signal(signal.SIGTERM, client_handler.handle_sigterm)
    try:
        worker_pids[connection_id] = os.getpid()
        if not sigterm_received.value and not client_handler.stopping:
            client_handler.run()
    except Exception:
        if not client_handler.stopping:
            logging.exception("Client connection %s failed", connection_id)
    finally:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        client_handler.close()
        client_dict.pop(connection_id, None)
        worker_pids.pop(connection_id, None)


def handle_sigterm(server_socket, client_dict, worker_pids, sigterm_received):
    sigterm_received.value = 1
    for connection in [server_socket, *client_dict.values()]:
        try:
            connection.shutdown(socket.SHUT_RDWR)
        except socket.error as error:
            logging.debug("Socket already disconnected during shutdown: %s", error)
    for worker_pid in worker_pids.values():
        try:
            os.kill(worker_pid, signal.SIGTERM)
        except ProcessLookupError:
            logging.debug("Worker %s already stopped", worker_pid)


def initialize_worker():
    logging.basicConfig(
        level=logging.INFO,
        force=True,
        format="%(asctime)s.%(msecs)03d | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    signal.signal(signal.SIGTERM, signal.SIG_IGN)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
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
                signal.signal(
                    signal.SIGTERM,
                    lambda signum, frame: handle_sigterm(
                        server_socket, client_dict, worker_pids, sigterm_received
                    ),
                )
                connection_id = 0
                try:
                    while True:
                        try:
                            client_socket, _ = server_socket.accept()
                            connection_id += 1
                            client_dict[connection_id] = client_socket
                            processes_pool.apply_async(
                                handle_client_response,
                                (client_socket, connection_id, client_dict,
                                 worker_pids, sigterm_received),
                                callback=lambda _, connection=client_socket: connection.close(),
                            )
                        except socket.error:
                            if not sigterm_received.value:
                                logging.exception("The connection with the client was lost")
                                return 1
                            return 0
                        except Exception:
                            logging.exception("Error accepting a client")
                            return 2
                finally:
                    handle_sigterm(server_socket, client_dict, worker_pids, sigterm_received)
                    processes_pool.close()
                    processes_pool.join()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
