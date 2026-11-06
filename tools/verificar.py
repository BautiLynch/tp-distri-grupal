"""Verifica los archivos de salida del cliente contra una referencia serial.

Requiere --referencia con el JSON generado por tools/serial.py.
No abre el dataset ni recalcula las consultas.

Uso:
    python3 tools/verificar.py --referencia test/referencia.json output/cliente_0
    python3 tools/verificar.py --referencia test/referencia.json --compose docker-compose.yaml --wait

Con --compose se toman las salidas de los clientes del Compose vigente.
--wait espera a que esos clientes terminen y comprueba sus codigos de salida.
Sin --compose ni carpetas de salida, se usan todas las output/cliente_*.
Sale con codigo 0 si todo coincide y 1 si hay diferencias.
"""

import argparse
import collections
import glob
import json
import os
from pathlib import Path, PurePosixPath
import struct
import subprocess
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

RESULT = 6
UINT32_SIZE = 4


# --------------------------------------------------------------------------- #
# Referencia y archivos de salida
# --------------------------------------------------------------------------- #
def resolve(path):
    if os.path.isabs(path) or os.path.exists(path):
        return path
    alternative = os.path.join(REPO_ROOT, path)
    return alternative if os.path.exists(alternative) else path


def local_file(service, field):
    path = PurePosixPath(service["environment"][field])
    mounts = [mount for mount in service.get("volumes", [])
              if path.is_relative_to(PurePosixPath(mount["target"]))]
    if not mounts:
        raise ValueError(f"{field}={path} no tiene un montaje local")
    mount = max(mounts, key=lambda item: len(item["target"]))
    if mount["type"] != "bind":
        raise ValueError(f"{field}={path} debe estar en un bind mount")
    return str((Path(mount["source"]) / path.relative_to(mount["target"])).resolve())


def client_outputs(compose_file, wait):
    compose = ["docker", "compose", "-f", compose_file]
    config = json.loads(subprocess.check_output(compose + ["config", "--format", "json"], text=True))
    clients = {name: service for name, service in config["services"].items()
               if {"INPUT_PATH", "OUTPUT_DIR"} <= service.get("environment", {}).keys()}
    if not clients:
        raise ValueError("El Compose no tiene clientes con INPUT_PATH y OUTPUT_DIR")
    outputs = [local_file(service, "OUTPUT_DIR") for service in clients.values()]
    if len(set(outputs)) != len(outputs):
        raise ValueError("Hay clientes que escriben en la misma carpeta de salida")
    for name, output in zip(clients, outputs):
        print(f"cliente: {name} -> {output}")
    if wait:
        containers = subprocess.check_output(
            compose + ["ps", "--all", "--quiet", *clients], text=True,
        ).splitlines()
        if len(containers) != len(clients):
            raise ValueError("Faltan contenedores de clientes; ejecuta docker compose up primero")
        print("Esperando que terminen los clientes...", flush=True)
        codes = subprocess.check_output(["docker", "wait", *containers], text=True).splitlines()
        if len(codes) != len(clients) or any(code != "0" for code in codes):
            raise ValueError(f"Uno o mas clientes terminaron con error: {codes}")
    return outputs


def load_reference(path):
    with open(path, encoding="utf-8") as handle:
        saved = json.load(handle)
    if not isinstance(saved, dict) or saved.get("format") != "wikipedia-serial-v2":
        raise ValueError("referencia incompatible: regenerala con tools/serial.py")
    return {
        1: collections.Counter({(url, date): count for url, date, count in saved["q1"]}),
        2: collections.Counter({(name, url): count for name, url, count in saved["q2"]}),
        3: set(saved["q3"]),
        4: saved["q4"],
        5: saved["q5"],
        "q4_candidates": saved["q4_candidates"],
        "threshold": saved["threshold"],
        "pre_urls": saved["pre_urls"],
        "post_urls": saved["post_urls"],
        "dataset": saved["dataset"],
        "config": saved["config"],
    }


# --------------------------------------------------------------------------- #
# Lectura de las carpetas de salida
# --------------------------------------------------------------------------- #
def read_records(path):
    with open(path, "rb") as handle:
        while True:
            header = handle.read(12)
            if not header:
                return
            if len(header) != 12:
                raise ValueError(f"{path}: registro truncado en el byte {handle.tell() - len(header)}")
            kind, query, size = struct.unpack(">III", header)
            payload = handle.read(size)
            if len(payload) != size:
                raise ValueError(f"{path}: payload truncado en el byte {handle.tell() - len(payload)}")
            yield kind, query, payload


def read_text(payload, offset):
    size, start = read_uint32(payload, offset)
    if start + size > len(payload):
        raise ValueError("texto truncado")
    return payload[start:start + size].decode("utf-8"), start + size


def read_uint32(payload, offset):
    return struct.unpack(">I", payload[offset:offset + 4])[0], offset + 4


def parse_q2_record(payload):
    name, offset = read_text(payload, 0)
    url, offset = read_text(payload, offset)
    size, offset = read_uint32(payload, offset)
    offset += size
    if offset != len(payload):
        raise ValueError("Q2: payload truncado o con bytes sobrantes")
    return name, url, size


def load_json(path):
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


def verify_q2_images(path, records):
    q2_dir = os.path.join(path, "q2")
    binary = collections.Counter()
    for record in records:
        location = record["image"]
        if location is None:
            if record["image_size"] != 0:
                raise ValueError(f"Q2: imagen sin ubicacion: {record['url']}")
        elif location == "resto.bin":
            binary[(record["name"], record["url"], record["image_size"])] += 1
        elif os.path.getsize(os.path.join(q2_dir, location)) != record["image_size"]:
            raise ValueError(f"Q2: tamano incorrecto en {location}")

    stored = collections.Counter()
    for kind, query, payload in read_records(os.path.join(q2_dir, "resto.bin")):
        if kind != RESULT or query != 2:
            raise ValueError(f"registro inesperado en resto.bin: {kind}, {query}")
        stored[parse_q2_record(payload)] += 1
    if stored != binary:
        raise ValueError("Q2: resto.bin no coincide con q2.json")


def received_results(path):
    q1 = collections.Counter(
        (record["url"], record["date_modified"])
        for record in load_json(os.path.join(path, "q1.json"))
    )

    q2_records = load_json(os.path.join(path, "q2", "q2.json"))
    verify_q2_images(path, q2_records)
    q2 = collections.Counter((record["name"], record["url"]) for record in q2_records)
    q2_sizes = {(record["name"], record["url"]): record["image_size"] for record in q2_records}

    q3_records = load_json(os.path.join(path, "q3.json"))
    q3 = {record["word"] for record in q3_records}
    if len(q3) != len(q3_records):
        raise ValueError("Q3: hay palabras duplicadas")

    q4_records = load_json(os.path.join(path, "q4.json"))
    q4 = {record["word"]: record["amount"] for record in q4_records}
    if len(q4) != len(q4_records):
        raise ValueError("Q4: top con palabras repetidas")

    q5_records = load_json(os.path.join(path, "q5.json"))
    q5 = {record["url"]: record["amount"] for record in q5_records}
    if len(q5) != len(q5_records):
        raise ValueError("Q5: hay URLs duplicadas")

    return {1: q1, 2: q2, 3: q3, 4: q4, 5: q5}, q2_sizes


# --------------------------------------------------------------------------- #
# Comparaciones
# --------------------------------------------------------------------------- #
def compare_counter(expected, received):
    missing = expected - received
    extra = received - expected
    return not missing and not extra, missing, extra


def compare_set(expected, received):
    return not (expected - received) and not (received - expected), expected - received, received - expected


def compare_dict(expected, received):
    missing = {key: value for key, value in expected.items() if key not in received}
    extra = {key: value for key, value in received.items() if key not in expected}
    different = {
        key: (expected[key], received[key])
        for key in expected
        if key in received and expected[key] != received[key]
    }
    return not missing and not extra and not different, missing, extra, different


def show(label, values, limit=5):
    if not values:
        return
    items = list(values.items() if isinstance(values, dict) else values)[:limit]
    print(f"      {label}: {items}")


def verify_file(path, expected):
    received, q2_sizes = received_results(path)
    print(f"\n== {path}")

    failures = 0

    # Q1
    ok, missing, extra = compare_counter(expected[1], received[1])
    print(f"  Q1  esperado {len(expected[1]):>6}  recibido {len(received[1]):>6}  {'OK' if ok else 'DIFERENTE'}")
    if not ok:
        failures += 1
        show("faltan", missing)
        show("sobran", extra)

    # Q2
    ok, missing, extra = compare_counter(expected[2], received[2])
    with_image = sum(1 for size in q2_sizes.values() if size > 0)
    empty_image = len(q2_sizes) - with_image
    print(f"  Q2  esperado {len(expected[2]):>6}  recibido {len(received[2]):>6}  {'OK' if ok else 'DIFERENTE'}"
          f"  (imagen >0 bytes: {with_image}, =0 bytes: {empty_image})")
    if not ok:
        failures += 1
        show("faltan", missing)
        show("sobran", extra)

    # Q3
    ok, missing, extra = compare_set(expected[3], received[3])
    print(f"  Q3  esperado {len(expected[3]):>6}  recibido {len(received[3]):>6}  {'OK' if ok else 'DIFERENTE'}")
    if not ok:
        failures += 1
        show("faltan", missing)
        show("sobran", extra)

    # Q4
    ok, missing, extra, different = compare_dict(expected[4], received[4])
    note = ""
    candidates = expected.get("q4_candidates")
    if not ok and candidates is not None and expected[4]:
        cutoff = min(expected[4].values())
        mandatory = {word for word, count in candidates.items() if count > cutoff}
        if (len(received[4]) == len(expected[4]) and mandatory <= received[4].keys()
                and all(candidates.get(word) == count for word, count in received[4].items())):
            ok, note = True, "  (empate en el corte del top)"
    print(f"  Q4  esperado {len(expected[4]):>6}  recibido {len(received[4]):>6}  {'OK' if ok else 'DIFERENTE'}{note}")
    if not ok:
        failures += 1
        show("faltan", missing)
        show("sobran", extra)
        show("distintos", different)

    # Q5
    ok, missing, extra, different = compare_dict(expected[5], received[5])
    print(f"  Q5  esperado {len(expected[5]):>6}  recibido {len(received[5]):>6}  {'OK' if ok else 'DIFERENTE'}"
          f"  (umbral {expected['threshold']:.4f})")
    if not ok:
        failures += 1
        show("faltan", missing)
        show("sobran", extra)
        show("distintos", different)

    return failures


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("outputs", nargs="*", help="carpetas de salida de los clientes")
    parser.add_argument("--compose", help="Compose del que leer los clientes y sus salidas")
    parser.add_argument("--wait", action="store_true", help="esperar a los clientes de --compose")
    parser.add_argument("--referencia", required=True, help="JSON generado por tools/serial.py")
    parser.add_argument("--esperado", action="store_true", help="solo mostrar los valores esperados")
    args = parser.parse_args()
    if args.wait and not args.compose:
        parser.error("--wait requiere --compose")
    if args.compose and args.outputs:
        parser.error("usa --compose o carpetas de salida explicitas, no ambos")

    try:
        reference_path = resolve(args.referencia)
        expected = load_reference(reference_path)
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    print("esperado: " + ", ".join(
        f"Q{query}={len(expected[query])}" for query in range(1, 6)
    ))

    if args.esperado:
        return 0

    try:
        outputs = (client_outputs(resolve(args.compose), args.wait) if args.compose
                   else args.outputs or sorted(glob.glob(resolve("output") + "/cliente_*")))
    except (OSError, ValueError, KeyError, TypeError, subprocess.CalledProcessError) as error:
        print(f"error al obtener salidas de clientes: {error}", file=sys.stderr)
        return 2
    if not outputs:
        print("\nno hay carpetas de salida para comparar")
        return 2

    failures = 0
    for path in outputs:
        path = resolve(path)
        if not os.path.exists(path):
            print(f"\n== {path}\n  error: no existe")
            failures += 1
            continue
        try:
            failures += verify_file(path, expected)
        except (OSError, ValueError, KeyError, TypeError, struct.error) as error:
            print(f"\n== {path}\n  error: {error}")
            failures += 1

    print("\nRESULTADO:", "OK" if failures == 0 else f"{failures} verificacion(es) con diferencias")
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
