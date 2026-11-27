import docker
import os
import logging
import signal
import socket
import sys
import threading
import time

from docker.errors import NotFound
from common.message_protocol.internal import deserialize_msg
from bully import BullyElection
from gossip import Gossip

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
MONITOR_AMOUNT = int(os.environ.get("MONITOR_AMOUNT", 3))
HEARTBEAT_PORT = int(os.environ["HEARTBEAT_PORT"])
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
        self.worker_names = {}
        self.leader_threads = []
        self.heartbeat_receiver = threading.Thread(target=self._receive_heartbeats, name="heartbeat-receiver")

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

                worker_name = self.worker_names.get(container_id, container_id)
                result.append((container_id, worker_name, age_ms))

        return result

    def _on_containers(self, sender, containers):
        # Todos reciben los heartbeats; el gossip solo aporta lo que otro vio mas reciente
        # (containers caidos que este monitor no conoce, o la postergacion de un reinicio).
        with self.condition:
            now = time.monotonic()
            for container_id, worker_name, age_ms in containers:
                seen = now - (age_ms / 1000)
                if container_id not in self.containers:
                    logging.debug(f"Learned {worker_name} (container {container_id}) from monitor {sender}")
                    self.containers[container_id] = seen
                    self.worker_names[container_id] = worker_name
                elif seen > self.containers[container_id]:
                    self.containers[container_id] = seen
            self.condition.notify_all()

    def _on_peer_timeout(self, peer, container_id):
        if peer == self.bully.leader_id:
            self.bully.leader_lost(peer)
        elif self.is_leader:
            logging.info(f"Monitor {peer} gossip timeout")
            self._restart(container_id, f"monitor_{peer}")

    def _on_leader_change(self, leader):
        with self.condition:
            if self.stopping:
                return
            if leader == ID:
                self._become_leader_locked()
            elif self.is_leader:
                self._step_down_locked()


    def _become_leader_locked(self):
        self.is_leader = True
        self.leader_term += 1
        logging.info(f"Leader term {self.leader_term}: watching {len(self.containers)} known containers")

        thread = threading.Thread(target=self._watch_heartbeats, args=(self.leader_term,))
        self.leader_threads = [t for t in self.leader_threads if t.is_alive()] + [thread]
        thread.start()

    def _step_down_locked(self):
        logging.info("Stepping down as leader")
        self.is_leader = False
        self.leader_term += 1
        self.condition.notify_all()

    def _receive_heartbeats(self):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                sock.bind(("", HEARTBEAT_PORT))
                # Sin daemon: el timeout deja revisar stopping para poder joinear el hilo.
                sock.settimeout(1)
                while not self.stopping:
                    try:
                        message, _ = sock.recvfrom(65535)
                    except socket.timeout:
                        continue
                    self._process_heartbeat(message)

        except Exception:
            logging.exception("Heartbeat receiver failed")
            self._fail()

    def _process_heartbeat(self, message):
        _, (container_id, worker_name) = deserialize_msg(message)
        logging.debug(f"Received heartbeat from {worker_name} (container {container_id})")

        with self.condition:
            self.worker_names[container_id] = worker_name
            self.containers[container_id] = time.monotonic()
            self.condition.notify_all()

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
                worker_name = self.worker_names.get(container, container)

            logging.info(f"{worker_name} heartbeat exceeded")
            self._restart(container, worker_name)

    def _restart(self, container_id, name):
        threading.Thread(target=self._restart_container, args=(container_id, name)).start()

    def _restart_container(self, container_id, name):
        try:
            logging.info(f"Restarting {name} (container {container_id})")
            client.containers.get(container_id).restart()
        except NotFound:
            logging.warning(f"{name} (container {container_id}) no longer exists")
            with self.condition:
                self.containers.pop(container_id, None)
                self.worker_names.pop(container_id, None)
        except Exception as e:
            logging.error(f"Error restarting {name} (container {container_id}): {e}")

    def _fail(self):
        self.failed = True
        self.stop_event.set()

    def start(self):
        self.heartbeat_receiver.start()
        self.gossip.start()
        threading.Thread(target=self._start_election).start()

    def _start_election(self):
        if self.gossip.wait_ready():
            self.bully.start()

    def close(self):
        with self.condition:
            self.stopping = True
            self.condition.notify_all()

        self.bully.close()
        self.gossip.close()

        for thread in [self.heartbeat_receiver, *self.leader_threads]:
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
    sys.exit(main())
