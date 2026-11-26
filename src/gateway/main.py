import logging
import multiprocessing
import os
import signal
import socket
import sys
import threading
from pathlib import Path
from uuid import uuid4

from common.heartbeat import HeartbeatSender
from common import middleware, message_protocol
from common.message_protocol.fields import decode_fields, encode_fields
from common.storage.wal import PROCESSED, WriteAheadLog
from client_gateway import ClientGateway, publish_eof

SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])
HEARTBEAT_QUEUE = os.environ["HEARTBEAT_QUEUE"]
HEARTBEAT_INTERVAL = int(os.environ.get("HEARTBEAT_INTERVAL", 5))
MOM_HOST = os.environ["MOM_HOST"]
FILTER_PREFIX = os.environ["FILTER_PREFIX"]
FILTER_AMOUNT = int(os.environ["FILTER_AMOUNT"])
SPLITTER_PREFIX = os.environ["SPLITTER_PREFIX"]
SPLITTER_AMOUNT = int(os.environ["SPLITTER_AMOUNT"])
STORAGE_PATH = os.environ.get("STORAGE_PATH", "storage")

if FILTER_AMOUNT < 1:
    raise ValueError("FILTER_AMOUNT must be greater than zero")
if SPLITTER_AMOUNT < 1:
    raise ValueError("SPLITTER_AMOUNT must be greater than zero")


def initialize_worker():
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    signal.signal(signal.SIGINT, signal.SIG_IGN)


class Gateway:
    def __init__(self):
        self.heartbeat = HeartbeatSender(
            MOM_HOST, HEARTBEAT_QUEUE, socket.gethostname(), "gateway", HEARTBEAT_INTERVAL
        )
        self.shutdown_requested = threading.Event()
        signal.signal(signal.SIGTERM, self.handle_sigterm)
        signal.signal(signal.SIGINT, self.handle_sigterm)
        self.next_client_id = 1
        self.recover()

    def recover(self):
        pending_clients = []
        for path in Path(STORAGE_PATH).glob("gateway_client_*.wal"):
            client_id = int(path.stem.removeprefix("gateway_client_"))
            self.next_client_id = max(self.next_client_id, client_id + 1)
            wal = WriteAheadLog(path)
            eof_sent = False
            eof_received = False
            pending_eof = None
            try:
                while (record := wal.read()) is not None:
                    status, payload = record
                    fields = decode_fields(payload)
                    if fields == (client_id,):
                        continue
                    if fields and fields[0] == "EOF_R":
                        eof_received = True
                        eof_sent = False
                        pending_eof = fields[1] if len(fields) > 1 else None
                    elif fields == ("EOF_S",):
                        eof_sent = status == PROCESSED
                    else:
                        raise ValueError(f"Invalid gateway WAL record in {path}")
            finally:
                wal.close()
            if not eof_sent:
                pending_clients.append((client_id, path, pending_eof, eof_received))

        if not pending_clients:
            return

        filter_exchange = None
        splitter_exchange = None
        try:
            filter_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, FILTER_PREFIX, [str(index) for index in range(FILTER_AMOUNT)], consume=False
            )
            splitter_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, SPLITTER_PREFIX, [str(index) for index in range(SPLITTER_AMOUNT)], consume=False
            )
            for client_id, path, pending_eof, eof_received in pending_clients:
                if self.shutdown_requested.is_set():
                    return
                if pending_eof is None:
                    message_id = uuid4().bytes
                    pending_eof = message_protocol.internal.serialize_msg(
                        message_protocol.internal.MsgType.END_OF_RECORDS,
                        client_id, message_id, "gateway", message_id, not eof_received,
                    )
                else:
                    message_type, fields = message_protocol.internal.deserialize_msg(pending_eof)
                    if message_type != message_protocol.internal.MsgType.END_OF_RECORDS or fields[0] != client_id:
                        raise ValueError(f"Invalid pending EOF in {path}")
                wal = WriteAheadLog(path)
                try:
                    publish_eof(wal, pending_eof, filter_exchange, splitter_exchange)
                finally:
                    wal.close()
                logging.info("Recovered pending EOF for client %s", client_id)
        finally:
            for exchange in (filter_exchange, splitter_exchange):
                if exchange is not None:
                    try:
                        exchange.close()
                    except Exception:
                        logging.exception("Could not close a gateway recovery connection")

    @staticmethod
    def register_client(client_id):
        wal = WriteAheadLog(f"{STORAGE_PATH}/gateway_client_{client_id}.wal")
        try:
            wal.save(encode_fields(client_id))
            wal.mark_processed()
        finally:
            wal.close()

    def handle_sigterm(self, signum, frame):
        self.shutdown_requested.set()

    @staticmethod
    def stop_clients(client_dict, sigterm_received):
        sigterm_received.value = 1
        for connection in list(client_dict.values()):
            try:
                connection.shutdown(socket.SHUT_RDWR)
            except socket.error as error:
                logging.debug("Client already disconnected during shutdown: %s", error)

    @staticmethod
    def handle_client_request(client_socket, client_id, client_dict, sigterm_received):
        client = ClientGateway(client_socket, client_id, sigterm_received)
        try:
            client.run()
        except (OSError, EOFError) as error:
            if not sigterm_received.value:
                if client.disconnected:
                    logging.info("Client %s disconnected: %s", client_id, error)
                    try:
                        client.send_eof(True)
                    except Exception:
                        logging.exception("Could not send disconnected EOF for client %s", client_id)
                else:
                    logging.error("Gateway failed for client %s: %s", client_id, error)
        except Exception:
            logging.exception("Could not process client %s", client_id)
        finally:
            try:
                client.close()
            finally:
                client_dict.pop(client_id, None)

    def receive_client(self):
        with multiprocessing.Manager() as manager:
            client_dict = manager.dict()
            sigterm_received = manager.Value("c_short", 0)
            with multiprocessing.Pool(
                processes=os.process_cpu_count(), initializer=initialize_worker,
            ) as processes_pool:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
                    server_socket.bind((SERVER_HOST, SERVER_PORT))
                    server_socket.listen()
                    server_socket.settimeout(0.5)
                    try:
                        if not self.shutdown_requested.is_set():
                            self.heartbeat.start()
                        while not self.shutdown_requested.is_set():
                            try:
                                client_socket, _ = server_socket.accept()

                                logging.info("A new client has connected")
                                client_id = self.next_client_id
                                self.next_client_id += 1
                                try:
                                    self.register_client(client_id)
                                except Exception:
                                    client_socket.close()
                                    raise
                                client_dict[client_id] = client_socket
                                processes_pool.apply_async(
                                    Gateway.handle_client_request,
                                    (client_socket, client_id, client_dict, sigterm_received),
                                    callback=lambda _, connection=client_socket: connection.close(),
                                    error_callback=lambda error, connection=client_socket: (
                                        connection.close(),
                                        logging.error("Gateway worker failed: %s", error),
                                    ),
                                )
                            except socket.timeout:
                                continue
                            except socket.error:
                                if not self.shutdown_requested.is_set():
                                    logging.exception("The connection with the client was lost")
                                    return 1
                                else:
                                    return 0
                            except Exception:
                                logging.exception("Error accepting a client")
                                return 2
                    finally:
                        self.stop_clients(client_dict, sigterm_received)
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
    gateway = Gateway()
    return gateway.start()


if __name__ == "__main__":
    sys.exit(main())
