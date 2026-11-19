import logging
import threading

from common import middleware
from common.message_protocol.internal import MsgType, serialize_msg

HEARTBEAT_ROUTING_KEY = "heartbeat"


class HeartbeatSender:

    def __init__(self, host, queue_name, worker_id, worker_name, interval):
        self.host = host
        self.queue_name = queue_name
        self.worker_id = int(worker_id)
        self.worker_name = worker_name
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
        queue = None

        try:
            queue = middleware.MessageMiddlewareExchangeRabbitMQ(
                self.host, self.queue_name
            )
            message = serialize_msg(MsgType.HEARTBEAT, self.worker_id, self.worker_name)

            while not self._stop.is_set():
                queue.send_to(message, routing_key=HEARTBEAT_ROUTING_KEY)
                self._stop.wait(self.interval)

        except Exception:
            logging.exception("Heartbeat thread failed")

        finally:
            if queue is not None:
                try:
                    queue.close()
                except Exception as e:
                    logging.error(f"Error closing heartbeat middleware: {e}")

            logging.info("Heartbeat thread stopped")
