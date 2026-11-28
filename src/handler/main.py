import os
import logging
import socket
import signal
import threading
import multiprocessing
import sys

from common.heartbeat import HeartbeatSender
from client_handler import ClientHandler

SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])
MONITOR_IPS = os.environ["MONITOR_IPS"].split(",")
HEARTBEAT_PORT = int(os.environ["HEARTBEAT_PORT"])
HEARTBEAT_INTERVAL = int(os.environ.get("HEARTBEAT_INTERVAL", 5))


def initialize_worker():
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    signal.signal(signal.SIGINT, signal.SIG_IGN)


class Handler:
    def __init__(self):
        self.heartbeat = HeartbeatSender(
            MONITOR_IPS, HEARTBEAT_PORT, "handler", HEARTBEAT_INTERVAL
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
        except (OSError, EOFError) as error:
            logging.info("Client connection %s disconnected: %s", connection_id, error)
        except Exception:
            if not client_handler.stopping:
                logging.exception("Client connection %s failed", connection_id)
        finally:
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            try:
                client_handler.close()
            except Exception:
                logging.exception("Could not close client connection %s", connection_id)
            finally:
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
