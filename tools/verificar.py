"""Compara las cinco consultas de cada cliente con la referencia serial."""

import argparse
from collections import Counter
from itertools import islice
import json
from pathlib import Path, PurePosixPath
import subprocess
import sys


FIELDS = {
    "q1": ("url", "date_modified"),
    "q2": ("name", "url"),
    "q3": ("word",),
    "q4": ("word", "amount"),
    "q5": ("url", "amount"),
}


def load_json(path):
    with path.open(encoding="utf-8") as file:
        return json.load(file)


def output_path(service):
    path = PurePosixPath(service["environment"]["OUTPUT_DIR"])
    mounts = [mount for mount in service.get("volumes", [])
              if path.is_relative_to(mount["target"])]
    if not mounts:
        raise ValueError(f"{path} no tiene un montaje local")
    mount = max(mounts, key=lambda entry: len(entry["target"]))
    if mount["type"] != "bind":
        raise ValueError(f"{path} requiere un bind mount")
    return (Path(mount["source"]) / path.relative_to(mount["target"])).resolve()


def compose_outputs(compose_file, wait):
    compose = ["docker", "compose", "-f", str(compose_file)]
    config = json.loads(subprocess.check_output(compose + ["config", "--format", "json"]))
    clients = {name: service for name, service in config["services"].items()
               if {"INPUT_PATH", "OUTPUT_DIR"} <= service.get("environment", {}).keys()}
    outputs = [output_path(service) for service in clients.values()]
    if not outputs or len(outputs) != len(set(outputs)):
        raise ValueError("El Compose debe tener clientes con carpetas de salida distintas")
    if wait:
        containers = subprocess.check_output(
            compose + ["ps", "--all", "--quiet", *clients], text=True,
        ).splitlines()
        if len(containers) != len(clients):
            raise ValueError("Faltan contenedores de clientes")
        print("Esperando que terminen los clientes...", flush=True)
        codes = subprocess.check_output(["docker", "wait", *containers], text=True).splitlines()
        if codes != ["0"] * len(clients):
            raise ValueError(f"Los clientes no terminaron correctamente: {codes}")
    return outputs


def expected_results(reference):
    if reference["format"] != "wikipedia-serial-v2":
        raise ValueError("Referencia incompatible. Regenerala con tools/serial.py")
    return {
        "q1": Counter({(url, date): count for url, date, count in reference["q1"]}),
        "q2": Counter({(name, url): count for name, url, count in reference["q2"]}),
        "q3": Counter((word,) for word in reference["q3"]),
        "q4": Counter(reference["q4"].items()),
        "q5": Counter(reference["q5"].items()),
    }


def received_results(path, query, fields):
    file = path / "q2/q2.json" if query == "q2" else path / f"{query}.json"
    records = load_json(file)
    if not isinstance(records, list):
        raise ValueError(f"{file} debe contener un array")
    with_image = 0
    if query == "q2":
        for record in records:
            image = record["image"]
            size = (file.parent / image).stat().st_size if image is not None else 0
            if size != record["image_size"] or (image is not None and size == 0):
                raise ValueError(f"Imagen inválida para {record['url']}: {image}")
            with_image += image is not None
    return Counter(tuple(record[field] for field in fields) for record in records), with_image


def valid_top_tie(expected, received, candidates):
    if not expected or received.total() != expected.total():
        return False
    cutoff = min(amount for _, amount in expected)
    mandatory = {(word, amount) for word, amount in candidates.items() if amount > cutoff}
    return (mandatory <= received.keys()
            and all(count == 1 and candidates.get(word) == amount
                    for (word, amount), count in received.items()))


def verify(path, expected, candidates):
    print(f"\n== {path}")
    ok = True
    for query, fields in FIELDS.items():
        wanted = expected[query]
        status = "OK"
        try:
            actual, with_image = received_results(path, query, fields)
            if wanted != actual:
                if query == "q4" and valid_top_tie(wanted, actual, candidates):
                    status = "OK (empate en el corte del top)"
                else:
                    ok = False
                    status = "DIFERENTE"
            received = actual.total()
            images = f"  (con imagen: {with_image}, sin imagen: {received - with_image})" if query == "q2" else ""
            print(f"  {query.upper()}  esperado {wanted.total()}  recibido {received}  {status}{images}")
            if status == "DIFERENTE":
                for label, diff in (("faltan", wanted - actual), ("sobran", actual - wanted)):
                    if diff:
                        print(f"      {label}: {list(islice(diff.items(), 5))}")
        except (OSError, ValueError, KeyError, TypeError) as error:
            print(f"  {query.upper()}  ERROR: {error}")
            ok = False
    return ok


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--referencia", required=True, type=Path, help="JSON de tools/serial.py")
    parser.add_argument("--compose", required=True, type=Path, help="Compose del que leer los clientes")
    parser.add_argument("--wait", action="store_true", help="esperar a los clientes de --compose")
    args = parser.parse_args()

    try:
        reference = load_json(args.referencia)
        expected = expected_results(reference)
        candidates = reference["q4_candidates"]
        outputs = compose_outputs(args.compose, args.wait)
    except (OSError, ValueError, KeyError, TypeError, subprocess.CalledProcessError) as error:
        parser.exit(2, f"Error: {error}\n")

    failures = sum(not verify(path, expected, candidates) for path in outputs)
    print("\nRESULTADO:", f"{failures} cliente(s) con diferencias" if failures else "OK")
    return int(failures > 0)


if __name__ == "__main__":
    sys.exit(main())
