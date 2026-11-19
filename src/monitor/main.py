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
INPUT_EXCHANGE = os.environ["INPUT_EXCHANGE"]
HEARTBEAT_INTERVAL = int(os.environ.get("HEARTBEAT_INTERVAL", 5))
HEARTBEAT_ALLOWED_MISSES = int(os.environ.get("HEARTBEAT_ALLOWED_MISSES", 3))

client = docker.from_env()

# Docker Compose etiqueta cada contenedor con su proyecto y su servicio.
# El proyecto se toma del propio monitor, para no tocar contenedores de otros despliegues.
PROJECT = client.containers.get(socket.gethostname()).labels["com.docker.compose.project"]


def find_containers(service):
    return client.containers.list(all=True, filters={"label": [
        f"com.docker.compose.project={PROJECT}",
        f"com.docker.compose.service={service}",
    ]})

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

        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, INPUT_EXCHANGE, ["heartbeat"], consume=True,
            queue_name=f"monitor_{ID}",
        )

    def _watch_heartbeats(self):
        while True:
            with self.condition:
                if self.stopping:
                    return

                if not self.heartbeats:
                    self.condition.wait()
                    continue

                worker = min(
                    self.heartbeats,
                    key=self.heartbeats.get,
                )

                remaining = (
                    self.heartbeats[worker] - time.monotonic()
                )

                if remaining > 0:
                    self.condition.wait(timeout=remaining)
                    continue

                del self.heartbeats[worker]

            threading.Thread(
                target=self._heartbeat_exceeded,
                args=(worker,),
                name=f"heartbeat-exceeded-{worker}",
            ).start()

    def _heartbeat_exceeded(self, worker):
        logging.info(f"{worker} heartbeat exceeded")

        try:
            containers = find_containers(worker)
            if not containers:
                logging.error(f"No container found for {worker}")
                return

            for container in containers:
                logging.info(f"Restarting {worker} (container {container.name})")
                container.restart()
        except APIError as e:
            logging.error(f"Error restarting {worker}: {e}")
        except Exception as e:
            logging.error(f"Unexpected error while restarting {worker}: {e}")

    def process_heartbeat(self, message, ack, nack):
        try:
            message_type, _, fields = deserialize_msg(message)

            if message_type != MsgType.HEARTBEAT:
                logging.warning(f"Received non-heartbeat message: {message_type}")
                nack()
                return
            
            worker, = fields

            logging.info(f"Received heartbeat from {worker}")

            with self.condition:
                self.heartbeats[worker] = (
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
