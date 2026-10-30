import logging
import multiprocessing
import os
import signal
import socket

from worker import Worker


SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])


class Gateway:
    def __init__(self):
        self.client_dict = {}
        self.client_id = 0

    def start(self):
        with multiprocessing.Manager() as manager:
            self.sigterm_received = manager.Value("h", 0)
            cpu_count = getattr(os, "process_cpu_count", os.cpu_count) # TODO: Hace falta crear tantos procesos?
            with multiprocessing.Pool(processes=cpu_count()) as self.processes_pool:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as self.server_socket:
                    previous_handler = signal.signal(signal.SIGTERM, self.sigterm_handler)
                    try:
                        self.server_socket.bind((SERVER_HOST, SERVER_PORT))
                        self.server_socket.listen()
                        self.server_socket.settimeout(1)
                        logging.info("Listening to connections")
                        return self.accept_clients()
                    finally: # TODO: Me chifla un agente que acá se puede quedar bloqueado si "recv_msg" de un worker se queda bloqueado"
                        try:
                            self.shutdown()
                            self.processes_pool.close()
                            self.processes_pool.join()
                        finally:
                            signal.signal(signal.SIGTERM, previous_handler)

    def accept_clients(self):
        while not self.sigterm_received.value:
            try:
                client_socket, _ = self.server_socket.accept()
                if self.sigterm_received.value:
                    client_socket.close()
                    break
                logging.info("A new client has connected")
                self.client_id += 1
                self.client_dict[self.client_id] = client_socket
                self.create_worker(client_socket, self.client_id)
            except socket.timeout:
                continue
            except OSError:
                if not self.sigterm_received.value:
                    logging.exception("The connection with the client was lost")
                    return 1
                return 0
            except Exception:
                logging.exception("Error accepting a client")
                return 2
        return 0

    # TODO: Me chifla un agente que deberiamos usar directo "multiprocessing.Process"
    def create_worker(self, client_socket, client_id):
        worker = Worker(client_socket, client_id, self.sigterm_received)

        def release_client(_):
            self.client_dict.pop(client_id, None)
            client_socket.close()

        def worker_failed(error):
            logging.error("Client %s worker failed: %s", client_id, error)
            release_client(None)

        try:
            self.processes_pool.apply_async(
                worker.start,
                callback=release_client,
                error_callback=worker_failed,
            )
        except Exception:
            release_client(None)
            raise

    def sigterm_handler(self, signum, frame):
        self.shutdown() # TODO: Se supone que el shutdown se hace en el "finally" del "start"

    def shutdown(self): # TODO: Creo q no es idempotente 
        if self.sigterm_received is not None:
            self.sigterm_received.value = 1
        connections = list(self.client_dict.values())
        if self.server_socket is not None:
            connections.append(self.server_socket)
        for connection in connections:
            try:
                connection.shutdown(socket.SHUT_RDWR)
            except OSError as error:
                logging.debug("Socket already disconnected during shutdown: %s", error)
        if self.server_socket is not None:
            self.server_socket.close()

