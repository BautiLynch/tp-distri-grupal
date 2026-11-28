import docker
import os
import logging
import signal
import socket
import sys
import threading
import time

from docker.errors import NotFound
from common.heartbeat import HeartbeatSender
from common.message_protocol.internal import MsgType, deserialize_msg, serialize_msg
from bully import BullyElection

ID = int(os.environ["ID"])
NAME = f"monitor_{ID}"
MONITOR_IPS = os.environ["MONITOR_IPS"].split(",")
MONITORED = os.environ["MONITORED"].split(",")
HEARTBEAT_PORT = int(os.environ["HEARTBEAT_PORT"])
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
        self.leader = None
        now = time.monotonic()
        self.last_seen = {name: now for name in MONITORED if name != NAME}
        self.sock = None
        other_monitors = [ip for index, ip in enumerate(MONITOR_IPS) if index != ID]
        self.heartbeat = HeartbeatSender(other_monitors, HEARTBEAT_PORT, NAME, HEARTBEAT_INTERVAL)
        self.bully = BullyElection(
            ID, len(MONITOR_IPS), ELECTION_TIMEOUT,
            send=self._send_election,
            on_leader_change=self._on_leader_change,
        )
        self.threads = [
            threading.Thread(target=self._receive, name="receiver"),
            threading.Thread(target=self._watch_heartbeats, name="watcher"),
        ]

    def _send_election(self, msg_type, target):
        try:
            self.sock.sendto(serialize_msg(msg_type, ID), (MONITOR_IPS[target], HEARTBEAT_PORT))
        except OSError as e:
            logging.debug(f"Could not send {msg_type.name} to monitor {target}: {e}")

    def _on_leader_change(self, leader):
        with self.condition:
            self.leader = leader
            self.is_leader = leader == ID
            self.condition.notify_all()

    def _receive(self):
        try:
            while not self.stopping:
                try:
                    message, _ = self.sock.recvfrom(65535)
                except socket.timeout:
                    continue
                msg_type, fields = deserialize_msg(message)
                if msg_type == MsgType.HEARTBEAT:
                    self._process_heartbeat(fields[0])
                else:
                    self.bully.handle(msg_type, fields[0])

        except Exception:
            logging.exception("Receiver failed")
            self._fail()

    def _process_heartbeat(self, name):
        logging.debug(f"Received heartbeat from {name}")

        with self.condition:
            self.last_seen[name] = time.monotonic()
            self.condition.notify_all()

    def _watched_locked(self):
        if self.is_leader:
            return list(self.last_seen)
        leader_name = f"monitor_{self.leader}"
        return [leader_name] if leader_name in self.last_seen else []

    def _watch_heartbeats(self):
        while True:
            with self.condition:
                if self.stopping:
                    return

                watched = self._watched_locked()
                if not watched:
                    self.condition.wait()
                    continue

                name = min(watched, key=self.last_seen.get)
                remaining = self.last_seen[name] + HEARTBEAT_TIMEOUT - time.monotonic()

                if remaining > 0:
                    self.condition.wait(timeout=remaining)
                    continue

                self.last_seen[name] = time.monotonic()
                is_leader, leader = self.is_leader, self.leader

            if is_leader:
                logging.info(f"{name} heartbeat exceeded")
                self._restart(name)
            else:
                self.bully.leader_lost(leader)

    def _restart(self, name):
        threading.Thread(target=self._restart_container, args=(name,)).start()

    def _restart_container(self, name):
        try:
            logging.info(f"Restarting {name}")
            client.containers.get(name).restart()
        except NotFound:
            logging.warning(f"Container {name} does not exist")
            with self.condition:
                self.last_seen.pop(name, None)
        except Exception as e:
            logging.error(f"Error restarting {name}: {e}")

    def _fail(self):
        self.failed = True
        self.stop_event.set()

    def start(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("", HEARTBEAT_PORT))
        self.sock.settimeout(1)
        for thread in self.threads:
            thread.start()
        self.heartbeat.start()
        self.bully.start()

    def close(self):
        with self.condition:
            self.stopping = True
            self.condition.notify_all()

        self.bully.close()
        self.heartbeat.stop()
        for thread in self.threads:
            if thread.is_alive():
                thread.join()
        if self.sock is not None:
            self.sock.close()


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
