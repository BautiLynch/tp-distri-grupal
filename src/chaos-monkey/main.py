"""Chaos monkey: mata contenedores de workers al azar cada cierto tiempo. """

import logging
import os
import random
import signal
import socket
import threading

import docker
from docker.errors import APIError, NotFound

DEFAULT_TARGETS = (
    "filter,filter_language,filter_year,filter_average,image_getter,"
    "splitter,aggregation,average,join"
)

INTERVAL = float(os.environ.get("CHAOS_INTERVAL", 15))
KILLS_PER_ROUND = int(os.environ.get("CHAOS_KILLS_PER_ROUND", 1))
START_DELAY = float(os.environ.get("CHAOS_START_DELAY", 10))
TARGETS = [
    t.strip() for t in os.environ.get("CHAOS_TARGETS", DEFAULT_TARGETS).split(",") if t.strip()
]
SEED = os.environ.get("CHAOS_SEED")

PROJECT_LABEL = "com.docker.compose.project"
SERVICE_LABEL = "com.docker.compose.service"


def is_target(service):
    # filter_year_0 -> filter_year; join -> join
    group, _, replica = service.rpartition("_")
    if not replica.isdigit():
        group = service
    return group in TARGETS or service in TARGETS


class ChaosMonkey:
    def __init__(self):
        self.client = docker.from_env()
        self.stop = threading.Event()
        self.random = random.Random(SEED)
        self.project = self._own_project()

    def _own_project(self):
        """Proyecto de Compose del propio contenedor, para no tocar otros."""
        try:
            me = self.client.containers.get(socket.gethostname())
            return me.labels.get(PROJECT_LABEL)
        except (NotFound, APIError):
            logging.warning("Could not find own container; targeting every Compose project")
            return None

    def _candidates(self):
        filters = {"status": "running", "label": SERVICE_LABEL}
        if self.project:
            filters["label"] = [SERVICE_LABEL, f"{PROJECT_LABEL}={self.project}"]

        return [
            c for c in self.client.containers.list(filters=filters)
            if is_target(c.labels.get(SERVICE_LABEL, ""))
        ]

    def _round(self):
        candidates = self._candidates()
        if not candidates:
            logging.info("No running targets to kill")
            return

        victims = self.random.sample(candidates, min(KILLS_PER_ROUND, len(candidates)))
        for victim in victims:
            service = victim.labels.get(SERVICE_LABEL)
            try:
                victim.kill()
                logging.info(f"Killed {service} (container {victim.short_id})")
            except (NotFound, APIError) as e:
                logging.warning(f"Could not kill {service}: {e}")

    def run(self):
        if INTERVAL <= 0:
            logging.info("CHAOS_INTERVAL <= 0, chaos monkey disabled")
            return

        logging.info(
            f"Chaos monkey started: project={self.project} interval={INTERVAL}s "
            f"kills_per_round={KILLS_PER_ROUND} start_delay={START_DELAY}s targets={TARGETS}"
        )

        if self.stop.wait(START_DELAY):
            return

        while not self.stop.is_set():
            try:
                self._round()
            except Exception:
                logging.exception("Error during chaos round")
            self.stop.wait(INTERVAL)

        logging.info("Chaos monkey stopped")


def main():
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s.%(msecs)03d | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    monkey = ChaosMonkey()
    signal.signal(signal.SIGTERM, lambda *_: monkey.stop.set())
    monkey.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
