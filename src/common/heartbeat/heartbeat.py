import logging
import socket
import threading

from common.message_protocol.internal import MsgType, serialize_msg


class HeartbeatSender:
    """Manda HEARTBEAT por UDP a todos los monitores."""

    def __init__(self, monitor_ips, port, worker_name, interval):
        self.monitors = [(ip, port) for ip in monitor_ips]
        self.message = serialize_msg(MsgType.HEARTBEAT, worker_name)
        self.interval = interval
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="heartbeat")

    def start(self):
        self._thread.start()

    def stop(self, timeout=None):
        self._stop.set()
        if self._thread.is_alive() and self._thread is not threading.current_thread():
            self._thread.join(timeout)

    def _run(self):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                while not self._stop.is_set():
                    for address in self.monitors:
                        try:
                            sock.sendto(self.message, address)
                        except OSError as e:
                            logging.debug(f"Could not send heartbeat to {address[0]}: {e}")
                    self._stop.wait(self.interval)

        except Exception:
            logging.exception("Heartbeat thread failed")

        finally:
            logging.info("Heartbeat thread stopped")
