import docker
import os
import logging
import socket
import threading
import time

from docker.errors import APIError
from common import middleware
from common.message_protocol.internal import deserialize_msg, serialize_msg, MsgType
from common.shutdown import GracefulShutdown

ID = os.environ["ID"]
MOM_HOST = os.environ["MOM_HOST"]
MONITOR_AMOUNT = int(os.environ.get("MONITOR_AMOUNT", 3))
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
GOSSIP_EXCHANGE = os.environ["GOSSIP_EXCHANGE"]
HEARTBEAT_INTERVAL = int(os.environ.get("HEARTBEAT_INTERVAL", 5))
HEARTBEAT_ALLOWED_MISSES = int(os.environ.get("HEARTBEAT_ALLOWED_MISSES", 3))

client = docker.from_env()

class Monitor:
    def __init__(self):
        self.heartbeats = {}
        self.stopping = False
        self.condition = threading.Condition()
        
        self.election_in_progress = True
        self.gossip_consumer = None

        self.watchdog = threading.Thread(
            target=self._watch_heartbeats,
            name="heartbeat-watchdog",
        )

        self.gossip_publisher = threading.Thread(
            target=self._gossip_publish,
            name="heartbeat-gossip-publisher",
        )
        self.gossip_listener = threading.Thread(
            target=self._gossip_listen,
            name="heartbeat-gossip-listener",
        )

        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )

    def _gossip_publish(self):
        publisher = None
        try:
            publisher = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, GOSSIP_EXCHANGE, [], consume=False
            )

            while True:
                with self.condition:
                    self.condition.wait_for(
                        lambda: self.stopping or not self.election_in_progress
                    )

                    if self.stopping:
                        return

                # TODO: mandar en el gossip del lider los nuevos containers
                message = serialize_msg(MsgType.GOSSIP, int(ID), socket.gethostname())

                for index in range(MONITOR_AMOUNT):
                    if index == int(ID):
                        continue

                    with self.condition:
                        if self.stopping:
                            return
                        if self.election_in_progress:
                            break
                    
                    publisher.send_to(message, f"gossip_monitor_{index}")

                with self.condition:
                    self.condition.wait_for(
                        lambda: self.stopping or self.election_in_progress,
                        timeout=HEARTBEAT_INTERVAL,
                    )

        except Exception:
            logging.error("Gossip publisher failed")

        finally:
            if publisher is not None:
                publisher.close()

    def _gossip_listen(self):
        consumer = None
        try:
            consumer = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, GOSSIP_EXCHANGE, [f"gossip_monitor_{ID}"],
                consume=True, queue_name=f"monitor_{ID}",
            )

            with self.condition:
                if self.stopping:
                    return
                
                self.gossip_consumer = consumer

            consumer.start_consuming(self._process_gossip)

        except Exception:
            logging.error("Gossip listener failed")

        finally:
            with self.condition:
                self.gossip_consumer = None

            if consumer is not None:
                consumer.close()

    def _process_gossip(self, message, ack, nack):
        try:
            message_type, fields = deserialize_msg(message)
            if message_type != MsgType.GOSSIP:
                raise ValueError(f"Unexpected gossip message type: {message_type}")
            
            monitor_id, container_id = fields
            logging.info("Gossip from monitor %s (container %s)", monitor_id, container_id)

            # TODO: actualizar el estado del monitor
            # TODO: procesar mensajes de eleccion 
            # TODO: comenzar la eleccion o levantar el monitor (segun si se cae el lider o uno cualquiera)
        except Exception:
            logging.error("Invalid gossip message")
            # No mando nack xq no quiero reintentar el ms erroreo

        ack()

    def begin_election(self):
        with self.condition:
            self.election_in_progress = True
            self.condition.notify_all()

    def finish_election(self):
        with self.condition:
            self.election_in_progress = False
            self.condition.notify_all()


    def _watch_heartbeats(self):
        while True:
            with self.condition:
                if self.stopping:
                    return

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
                self.condition.notify_all()

        except Exception as e:
            logging.error(f"Error processing heartbeat: {e}")
            nack()
            return

        ack()

    def close(self):
        with self.condition:
            self.stopping = True
            self.condition.notify_all()
            consumer = self.gossip_consumer

        if consumer is not None:
            consumer.stop_consuming_threadsafe()

        for thread in (self.watchdog, self.gossip_publisher, self.gossip_listener):
            if thread.is_alive() and thread is not threading.current_thread():
                thread.join()

        self.input_queue.close()

    def start(self):
        # self.watchdog.start() # TODO: Solo deberia hacer start cuando soy el lider
        self.gossip_listener.start()
        self.gossip_publisher.start()
        # self.input_queue.start_consuming(self.process_heartbeat) # TODO: Idem


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
