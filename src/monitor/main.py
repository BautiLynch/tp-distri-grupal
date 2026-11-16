import os
import logging
import threading
from time import time

from common import middleware
from common.message_protocol.internal import deserialize_msg

MOM_HOST = os.environ["MOM_HOST"]
INPUT_EXCHANGE = os.environ["INPUT_EXCHANGE"]
HEARTBEAT_INTERVAL = int(os.environ.get("HEARTBEAT_INTERVAL", 5))


class Monitor:
    def __init__(self):
        # Hilo para heartbeat de monitores y eleccion de lider
    
        self.heartbeats = {}
        self.stopping = False
        self.condition = threading.Condition()
        self.watchdog = threading.Thread(
            target=self._watch_heartbeats,
            name="heartbeat-watchdog",
        )

        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, INPUT_EXCHANGE, ["heartbeat"], consume=True
        )

    def _watch_heartbeats(self):
        while True:
            with self.condition:
                if self.stopping:
                    return

                if not self.heartbeats:
                    self.condition.wait()
                    continue

                container_id = min(
                    self.heartbeats,
                    key=self.heartbeats.get,
                )

                remaining = (
                    self.heartbeats[container_id] - time.monotonic()
                )

                if remaining > 0:
                    self.condition.wait(timeout=remaining)
                    continue

                # TODO: tal vez deberia esperar N heartbeats en vez de uno solo

                del self.heartbeats[container_id]

            self._heartbeat_exceeded(container_id)

    def _heartbeat_exceeded(self, container_id):
        logging.info(f"Container {container_id} heartbeat exceeded")
        # TODO: Aca hay que volver a levantar el container

    def process_heartbeat(self, message, ack, nack):
        try:
            message_type, container_id, _ = deserialize_msg(message)
            logging.info(f"Received heartbeat from container {container_id}")

            with self.condition:
                self.heartbeats[container_id] = (
                    time.monotonic() + HEARTBEAT_INTERVAL
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
        self.input_exchange.close()

    def start(self):
        self.watchdog.start()
        self.input_exchange.start_consuming(self.process_heartbeat)


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