"""Chaos monkey: mata servicios de workers al azar cada cierto tiempo.

Uso: make chaos (con el sistema levantado). Se corta con Ctrl+C.
La configuracion se toma de variables de entorno, definidas en el Makefile.
"""

import logging
import os
from pathlib import Path
import random
import subprocess
import sys
import time

DEFAULT_TARGETS = (
    "filter,filter_language,filter_year,filter_average,image_getter,"
    "splitter,aggregation,average,join"
)

INTERVAL = float(os.environ.get("CHAOS_INTERVAL", 15))
KILLS_PER_ROUND = int(os.environ.get("CHAOS_KILLS_PER_ROUND", 1))
START_DELAY = float(os.environ.get("CHAOS_START_DELAY", 0))
TARGETS = [t.strip() for t in os.environ.get("CHAOS_TARGETS", DEFAULT_TARGETS).split(",") if t.strip()]
SEED = os.environ.get("CHAOS_SEED") or None

# docker compose busca el docker-compose.yaml en la raiz del repo.
COMPOSE = ["docker", "compose"]
ROOT = Path(__file__).resolve().parent.parent


def is_target(service):
    # filter_year_0 -> filter_year; join -> join
    group, _, replica = service.rpartition("_")
    if not replica.isdigit():
        group = service
    return group in TARGETS or service in TARGETS


def running_services():
    output = subprocess.check_output(
        COMPOSE + ["ps", "--services", "--status", "running"], cwd=ROOT, text=True
    )
    return output.split()


def kill(services):
    # Sin servicios, docker compose kill mataria todo el proyecto.
    if not services:
        return
    subprocess.run(COMPOSE + ["kill", "-s", "SIGKILL", *services], cwd=ROOT, check=True,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def chaos_round(rng):
    candidates = [s for s in running_services() if is_target(s)]
    if not candidates:
        logging.info("No running targets to kill")
        return

    victims = rng.sample(candidates, min(KILLS_PER_ROUND, len(candidates)))
    kill(victims)
    logging.info(f"Killed {', '.join(victims)}")


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    if INTERVAL <= 0 or KILLS_PER_ROUND <= 0:
        logging.info("CHAOS_INTERVAL or CHAOS_KILLS_PER_ROUND <= 0, chaos monkey disabled")
        return 0

    logging.info(
        f"Chaos monkey started: interval={INTERVAL}s kills_per_round={KILLS_PER_ROUND} "
        f"start_delay={START_DELAY}s targets={TARGETS}"
    )
    rng = random.Random(SEED)

    try:
        time.sleep(START_DELAY)
        while True:
            try:
                chaos_round(rng)
            except subprocess.CalledProcessError as e:
                logging.error(f"docker compose failed: {e}")
            time.sleep(INTERVAL)
    except KeyboardInterrupt:
        logging.info("Chaos monkey stopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
