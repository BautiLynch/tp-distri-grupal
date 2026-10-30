import os
import logging
import socket
import signal
import multiprocessing

from common import middleware, message_protocol

SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_EXCHANGE = os.environ["INPUT_EXCHANGE"]


class ClientHandler:
    def __init__(self, client_socket):
        self.client_socket = client_socket
        self.input_exchange = None
        self.completed_queries = set()
        self.stopping = False

    def run(self):
        message_type, client_id = message_protocol.external.recv_msg(self.client_socket)
        if message_type != message_protocol.external.MsgType.CLIENT_ID:
            raise ValueError("Expected a client ID to register the connection")
        if self.stopping:
            return
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, INPUT_EXCHANGE, [str(client_id)]
        )
        logging.info("Client %s has result queue %s", client_id, self.input_exchange.queue_name)
        if self.stopping:
            return
        
        message_protocol.external.send_msg(self.client_socket, message_protocol.external.MsgType.ACK)
        self.input_exchange.start_consuming(self.handle_result)

    def handle_result(self, message, ack, nack):
        try:
            message_type, _, query_id, payload = message_protocol.internal.deserialize_result(message)
            if not 1 <= query_id <= message_protocol.internal.QUERY_COUNT:
                raise ValueError("Unknown query ID")
            if message_type == message_protocol.internal.MsgType.RESULT:
                if query_id in self.completed_queries:
                    raise ValueError("Result received after query completion")
                message_protocol.external.send_msg(
                    self.client_socket, message_protocol.external.MsgType.RESULT,
                    query_id, payload,
                )
            elif message_type == message_protocol.internal.MsgType.QUERY_END:
                self.completed_queries.add(query_id)
                if len(self.completed_queries) == message_protocol.internal.QUERY_COUNT:
                    message_protocol.external.send_msg(
                        self.client_socket, message_protocol.external.MsgType.END_OF_RECORDS
                    )
            else:
                raise ValueError("Unknown internal message type")
        except Exception:
            nack()
            raise
        ack()
        if len(self.completed_queries) == message_protocol.internal.QUERY_COUNT:
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


def main():
    logging.basicConfig(level=logging.INFO)
    with multiprocessing.Manager() as manager:
        client_dict = manager.dict()
        worker_pids = manager.dict()
        sigterm_received = manager.Value("c_short", 0)
        with multiprocessing.Pool(
            processes=os.process_cpu_count(),
            initializer=signal.signal,
            initargs=(signal.SIGTERM, signal.SIG_IGN),
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
                logging.info("Listening to connections on %s:%s", *server_socket.getsockname())
                connection_id = 0
                try:
                    while True:
                        try:
                            client_socket, _ = server_socket.accept()
                            logging.info("A new client has connected")
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
