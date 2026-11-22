import docker
import os
import logging
import signal
import socket
import threading
import time

from docker.errors import NotFound
from common import middleware
from common.message_protocol.internal import deserialize_msg
from bully import BullyElection
from gossip import Gossip

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
MONITOR_AMOUNT = int(os.environ.get("MONITOR_AMOUNT", 3))
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
GOSSIP_EXCHANGE = os.environ["GOSSIP_EXCHANGE"]
HEARTBEAT_INTERVAL = int(os.environ.get("HEARTBEAT_INTERVAL", 5))
HEARTBEAT_ALLOWED_MISSES = int(os.environ.get("HEARTBEAT_ALLOWED_MISSES", 3))
ELECTION_TIMEOUT = float(os.environ.get("ELECTION_TIMEOUT", HEARTBEAT_INTERVAL))
HEARTBEAT_TIMEOUT = HEARTBEAT_INTERVAL * HEARTBEAT_ALLOWED_MISSES

client = docker.from_env()

class Monitor:
    def __init__(self):
        self.condition = threading.Condition()
        self.stop_event = threading.Event()
        self.stopping = False
        self.failed = False
        self.is_leader = False
        self.leader_term = 0
        self.containers = {}
        self.heartbeat_consumer = None
        self.leader_threads = []

        self.gossip = Gossip(
            ID, MONITOR_AMOUNT, MOM_HOST, GOSSIP_EXCHANGE,
            HEARTBEAT_INTERVAL, HEARTBEAT_TIMEOUT, socket.gethostname(),
            current_containers=self._current_containers,
            on_containers=self._on_containers,
            on_election_message=lambda msg_type, sender: self.bully.handle(msg_type, sender),
            on_peer_timeout=self._on_peer_timeout,
            on_failure=self._fail,
        )
        self.bully = BullyElection(
            ID, MONITOR_AMOUNT, ELECTION_TIMEOUT,
            send=self.gossip.send_election,
            on_leader_change=self._on_leader_change,
        )

    def _current_containers(self):
        with self.condition:
            now = time.monotonic()
            result = []

            for container_id, seen in self.containers.items():
                age_seconds = now - seen
                age_ms = int(age_seconds * 1000)

                if age_ms < 0:
                    age_ms = 0

                result.append((container_id, age_ms))

        return result

    def _on_containers(self, sender, containers):
        from_leader = sender == self.bully.leader_id
        with self.condition:
            now = time.monotonic()
            last_seen = {}
            for container_id, age_ms in containers:
                last_seen[container_id] = now - (age_ms / 1000)

            if self.is_leader:
                for container, seen in last_seen.items():
                    if container not in self.containers:
                        logging.info(f"Learned container {container} from monitor {sender}")
                        self.containers[container] = self._with_grace(seen, now)
                self.condition.notify_all()
            elif from_leader:
                self.containers = last_seen

    def _on_peer_timeout(self, peer, container_id):
        if peer == self.bully.leader_id:
            self.bully.leader_lost(peer)
        elif self.is_leader:
            logging.info(f"Monitor {peer} gossip timeout")
            self._restart(container_id)

    def _on_leader_change(self, leader):
        with self.condition:
            if self.stopping:
                return
            if leader == ID:
                self._become_leader_locked()
            elif self.is_leader:
                self._step_down_locked()


    @staticmethod
    def _with_grace(last_seen, now):
        return max(last_seen, now + HEARTBEAT_INTERVAL - HEARTBEAT_TIMEOUT)

    def _become_leader_locked(self):
        self.is_leader = True
        self.leader_term += 1
        now = time.monotonic()
        self.containers = {c: self._with_grace(seen, now) for c, seen in self.containers.items()}
        logging.info(f"Leader term {self.leader_term}: watching {len(self.containers)} known containers")

        threads = [
            threading.Thread(target=target, args=(self.leader_term,))
            for target in (self._consume_heartbeats, self._watch_heartbeats)
        ]
        self.leader_threads = [t for t in self.leader_threads if t.is_alive()] + threads
        for thread in threads:
            thread.start()

    def _step_down_locked(self):
        logging.info("Stepping down as leader")
        self.is_leader = False
        self.leader_term += 1
        if self.heartbeat_consumer is not None:
            self.heartbeat_consumer.stop_consuming_threadsafe()
        self.condition.notify_all()

    def _consume_heartbeats(self, term):
        consumer = None
        try:
            consumer = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE)

            with self.condition:
                if self.stopping or term != self.leader_term:
                    return
                self.heartbeat_consumer = consumer

            consumer.start_consuming(self._process_heartbeat)

        except Exception:
            if not self.stopping:
                logging.exception("Heartbeat consumer failed")
                self._fail()

        finally:
            with self.condition:
                if self.heartbeat_consumer is consumer:
                    self.heartbeat_consumer = None
            if consumer is not None:
                try:
                    consumer.close()
                except Exception as e:
                    logging.error(f"Error closing heartbeat consumer: {e}")

    def _process_heartbeat(self, message, ack, nack):
        try:
            _, (container_id, worker_name, sent_at) = deserialize_msg(message)
            seen = time.monotonic() - max(0, time.time() - sent_at)
            logging.debug(f"Received heartbeat from {worker_name} (container {container_id})")

            with self.condition:
                if self.is_leader:
                    self.containers[container_id] = max(self.containers.get(container_id, seen), seen)
                    self.condition.notify_all()
        except Exception as e:
            logging.error(f"Invalid heartbeat: {e}")
        ack()

    def _watch_heartbeats(self, term):
        while True:
            with self.condition:
                if self.stopping or term != self.leader_term:
                    return

                if not self.containers:
                    self.condition.wait()
                    continue

                container = min(self.containers, key=self.containers.get)
                remaining = self.containers[container] + HEARTBEAT_TIMEOUT - time.monotonic()

                if remaining > 0:
                    self.condition.wait(timeout=remaining)
                    continue

                self.containers[container] = time.monotonic()

            logging.info(f"{container} heartbeat exceeded")
            self._restart(container)

    def _restart(self, container_id):
        threading.Thread(target=self._restart_container, args=(container_id,)).start()

    def _restart_container(self, container_id):
        try:
            logging.info(f"Restarting container {container_id}")
            client.containers.get(container_id).restart()
        except NotFound:
            logging.warning(f"Container {container_id} no longer exists")
            with self.condition:
                self.containers.pop(container_id, None)
        except Exception as e:
            logging.error(f"Error restarting {container_id}: {e}")

    def _fail(self):
        self.failed = True
        self.stop_event.set()

    def start(self):
        self.gossip.start()
        threading.Thread(target=self._start_election).start()

    def _start_election(self):
        if self.gossip.wait_ready():
            self.bully.start()

    def close(self):
        with self.condition:
            self.stopping = True
            self.condition.notify_all()
            consumer = self.heartbeat_consumer

        if consumer is not None:
            consumer.stop_consuming_threadsafe()
        self.bully.close()
        self.gossip.close()

        for thread in self.leader_threads:
            if thread.is_alive():
                thread.join()


def main():
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s.%(msecs)03d | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    monitor = Monitor()
    signal.signal(signal.SIGTERM, lambda signum, frame: monitor.stop_event.set())
    monitor.start()
    monitor.stop_event.wait()
    monitor.close()
    logging.info("Monitor stopped")
    return 1 if monitor.failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
