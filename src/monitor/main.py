import docker
import os
import logging
import socket
import threading
import time

from docker.errors import APIError
from common import middleware
from common.message_protocol.internal import deserialize_msg, MsgType

ID = os.environ["ID"]
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
HEARTBEAT_INTERVAL = int(os.environ.get("HEARTBEAT_INTERVAL", 5))
HEARTBEAT_ALLOWED_MISSES = int(os.environ.get("HEARTBEAT_ALLOWED_MISSES", 3))

client = docker.from_env()

class Monitor:
    def __init__(self):
        # TODO: Hilo para heartbeat de monitores y eleccion de lider
    
        self.heartbeats = {}
        self.stopping = False
        self.condition = threading.Condition()
        self.watchdog = threading.Thread(
            target=self._watch_heartbeats,
            name="heartbeat-watchdog",
        )

        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )

    def _watch_heartbeats(self):
        while True:
            if self.stopping:
                return
            
            with self.condition:

                if not self.heartbeats:
                    self.condition.wait()
                    continue

                container = min(
                    self.heartbeats,
                    key=self.heartbeats.get,
                )

                remaining = (
                    self.heartbeats[container] - time.monotonic()
                )

                if remaining > 0:
                    self.condition.wait(timeout=remaining)
                    continue

            threading.Thread(
                target=self._heartbeat_exceeded,
                args=(container,),
                name=f"heartbeat-exceeded-{container}",
            ).start()

    def _heartbeat_exceeded(self, container):
        logging.info(f"{container} heartbeat exceeded")

        try:
            logging.info(f"Restarting container {container}")
            client.containers.get(container).restart()

            with self.condition:
                del self.heartbeats[container]

        except APIError as e:
            logging.error(f"Error restarting {container}: {e}")
        except Exception as e:
            logging.error(f"Unexpected error while restarting {container}: {e}")

    def process_heartbeat(self, message, ack, nack):
        try:
            message_type, fields = deserialize_msg(message)

            if message_type != MsgType.HEARTBEAT:
                logging.warning(f"Received non-heartbeat message: {message_type}")
                nack()
                return
            
            container_id, worker_name = fields

            logging.info(f"Received heartbeat from {worker_name} (container {container_id})")

            with self.condition:
                self.heartbeats[container_id] = (
                    time.monotonic() + HEARTBEAT_INTERVAL * HEARTBEAT_ALLOWED_MISSES
                )
                self.condition.notify()

        except Exception as e:
            logging.error(f"Error processing heartbeat: {e}")
            nack()
            return

        ack()

    def close(self):
        with self.condition:
            self.stopping = True
            self.condition.notify()

        self.watchdog.join()
        self.input_queue.close()

    def start(self):
        self.watchdog.start()
        self.input_queue.start_consuming(self.process_heartbeat)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    monitor = Monitor()
    monitor.start()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
