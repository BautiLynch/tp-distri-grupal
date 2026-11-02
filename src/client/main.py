import logging
import math
import os
import queue
import signal
import socket
import threading
import time

from common.message_protocol import external


def expect_message(connection, expected_type):
    message_type, payload = external.recv_msg(connection)
    if message_type != expected_type:
        raise ValueError(f"Expected message {expected_type}, got {message_type}")
    return payload


def write_all(output, data):
    remaining = memoryview(data)
    while remaining:
        written = output.write(remaining)
        if written is None or written <= 0:
            raise OSError("Could not write a complete result")
        remaining = remaining[written:]


class Client:
    def __init__(self, gateway_host, gateway_port, input_path, output_path,
                 connection_timeout=10):
        if not gateway_host or not 1 <= gateway_port <= 65535:
            raise ValueError("Invalid gateway address")
        if not math.isfinite(connection_timeout) or connection_timeout <= 0:
            raise ValueError("CONNECTION_TIMEOUT must be positive and finite")
        self.gateway_address = (gateway_host, gateway_port)
        self.input_path = input_path
        self.output_path = output_path
        self.connection_timeout = connection_timeout
        self.stopping = threading.Event()
        self.receiver_ready = threading.Event()
        self.receiver_errors = queue.SimpleQueue()
        self.connections = []
        self.connections_lock = threading.RLock()

    def stop(self):
        self.stopping.set()
        with self.connections_lock:
            for connection in self.connections:
                try:
                    connection.shutdown(socket.SHUT_RDWR)
                except OSError as error:
                    logging.debug("Socket already disconnected: %s", error)

    def _check_stopping(self):
        self._raise_receiver_error()
        if self.stopping.is_set():
            raise InterruptedError("Client stopped")

    def _connect(self, address, attempts=1):
        for attempt in range(1, attempts + 1):
            self._check_stopping()
            try:
                connection = socket.create_connection(address, self.connection_timeout)
                break
            except OSError as error:
                self._check_stopping()
                if attempt == attempts:
                    raise
                delay = 2 ** (attempt - 1)
                logging.warning(
                    "Connection to %s:%s failed (attempt %s/%s): %s; retrying in %ss",
                    *address, attempt, attempts, error, delay,
                )
                self.stopping.wait(delay)
        connection.settimeout(None)
        with self.connections_lock:
            self.connections.append(connection)
            if self.stopping.is_set():
                connection.close()
                raise InterruptedError("Client stopped")
        return connection

    def _raise_receiver_error(self):
        try:
            error = self.receiver_errors.get_nowait()
        except queue.Empty:
            return
        raise RuntimeError("Could not receive or save results") from error

    def _receive_results(self, connection, client_id, output):
        try:
            external.send_msg(connection, external.MsgType.CLIENT_ID, client_id)
            expect_message(connection, external.MsgType.ACK)
            self.receiver_ready.set()
            while True:
                message_type, payload = external.recv_msg(connection)
                if message_type == external.MsgType.END_OF_RECORDS:
                    return
                if message_type == external.MsgType.RESULT:
                    record = external.serialize_msg(message_type, *payload)
                else:
                    raise ValueError(f"Unexpected handler message {message_type}")
                write_all(output, record)
                output.flush()
        except Exception as error:
            self.receiver_errors.put(error)
            self.stop()
        finally:
            self.receiver_ready.set()

    def _send_records(self, connection, source):
        for line in source:
            self._check_stopping()
            external.send_msg(connection, external.MsgType.ARTICLE, line)
            expect_message(connection, external.MsgType.ACK)
        self._check_stopping()
        external.send_msg(connection, external.MsgType.END_OF_RECORDS)
        expect_message(connection, external.MsgType.ACK)

    def _validate_output(self, source):
        try:
            output_stat = os.stat(self.output_path)
        except FileNotFoundError:
            return
        if os.path.samestat(os.fstat(source.fileno()), output_stat):
            raise ValueError("INPUT_PATH and OUTPUT_PATH must be different files")

    def run(self):
        receiver = None
        with open(self.input_path, "rb") as source:
            self._validate_output(source)
            with open(self.output_path, "wb") as output:
                try:
                    gateway = self._connect(self.gateway_address, attempts=3)
                    client_id = expect_message(gateway, external.MsgType.CLIENT_ID)
                    handler_address = expect_message(gateway, external.MsgType.HANDLER_ADDRESS)
                    handler = self._connect(handler_address)
                    receiver = threading.Thread(
                        target=self._receive_results,
                        args=(handler, client_id, output),
                        name="result-receiver",
                    )
                    receiver.start()
                    self.receiver_ready.wait()
                    self._check_stopping()
                    self._send_records(gateway, source)
                    receiver.join()
                    self._check_stopping()
                    logging.info("Client %s completed; results saved to %s",
                                 client_id, self.output_path)
                except Exception:
                    self._raise_receiver_error()
                    raise
                finally:
                    self.stop()
                    if receiver is not None and receiver.ident is not None:
                        receiver.join()
                    with self.connections_lock:
                        for connection in self.connections:
                            connection.close()
                        self.connections.clear()


def main():
    logging.basicConfig(level=logging.INFO)
    started_at = time.perf_counter()
    logging.info("Client started")
    completed = False
    previous_handlers = {}
    try:
        client = Client(
            os.environ["GATEWAY_HOST"], int(os.environ["GATEWAY_PORT"]),
            os.environ["INPUT_PATH"], os.environ["OUTPUT_PATH"],
            float(os.environ.get("CONNECTION_TIMEOUT", "10")),
        )
        for sig in (signal.SIGTERM, signal.SIGINT):
            previous_handlers[sig] = signal.signal(sig, lambda signum, frame: client.stop())
        client.run()
        completed = True
    except Exception:
        logging.exception("Client failed")
        return 1
    finally:
        for sig, previous in previous_handlers.items():
            signal.signal(sig, previous)
        elapsed = time.perf_counter() - started_at
        logging.info("Client finished; status=%s", "success" if completed else "failed")
        logging.info("Client total duration: %.3f seconds", elapsed)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
