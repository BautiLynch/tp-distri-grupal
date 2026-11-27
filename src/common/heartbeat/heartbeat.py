import logging
import socket
import threading

from common.message_protocol.internal import MsgType, serialize_msg


class HeartbeatSender:
    """Manda HEARTBEAT por UDP a todos los monitores."""

    def __init__(self, monitor_amount, port, container_id, worker_name, interval):
        self.monitors = [f"monitor_{index}" for index in range(monitor_amount)]
        self.port = port
        self.message = serialize_msg(MsgType.HEARTBEAT, container_id, worker_name)
        self.interval = interval
        self._ips = {}
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="heartbeat")

    def start(self):
        self._thread.start()

    def stop(self, timeout=None):
        self._stop.set()
        if self._thread.is_alive() and self._thread is not threading.current_thread():
            self._thread.join(timeout)

    def _ip(self, host):
        if host not in self._ips:
            self._ips[host] = socket.gethostbyname(host)
        return self._ips[host]

    def _run(self):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                while not self._stop.is_set():
                    for host in self.monitors:
                        try:
                            sock.sendto(self.message, (self._ip(host), self.port))
                        except OSError as e:
                            logging.debug(f"Could not send heartbeat to {host}: {e}")
                    self._stop.wait(self.interval)

        except Exception:
            logging.exception("Heartbeat thread failed")

        finally:
            logging.info("Heartbeat thread stopped")
