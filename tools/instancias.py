import argparse
from copy import deepcopy
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile


WORKERS = (
    "filter", "image_getter", "splitter", "filter_language", "aggregation",
    "filter_year", "average", "filter_average",
)
GROUPS = (*WORKERS, "client")
SPLITTER_CONSUMERS = ("filter_language", "aggregation", "filter_year")
WORD_CONSUMERS = ("filter_language", "aggregation")
SINGLETONS = ("rabbitmq", "gateway", "handler", "join")
COUNTERS = {
    "FILTER_AMOUNT": "filter",
    "FILTER_YEAR_PLUS_AMOUNT": "filter",
    "IMAGE_GETTER_AMOUNT": "image_getter",
    "SPLITTER_AMOUNT": "splitter",
    "FILTER_LANGUAGE_AMOUNT": "filter_language",
    "AGGREGATION_AMOUNT": "aggregation",
    "FILTER_YEAR_AMOUNT": "filter_year",
    "AVERAGE_AMOUNT": "average",
    "FILTER_AVERAGE_AMOUNT": "filter_average",
}


def group_of(name):
    for group in GROUPS:
        if re.fullmatch(re.escape(group) + r"_\d+", name):
            return group
    return None


def scale_config(config, target, amount):
    config = deepcopy(config)
    original = config["services"]
    replicas = {group: {} for group in GROUPS}
    for name, service in original.items():
        group = group_of(name)
        if group:
            replicas[group][int(name.rsplit("_", 1)[1])] = service
    for group in GROUPS:
        if 0 not in replicas[group]:
            raise ValueError(f"Falta el servicio base {group}_0 en el compose")

    counts = {group: len(items) for group, items in replicas.items()}
    targets = WORKERS if target == "todos" else WORD_CONSUMERS if target in WORD_CONSUMERS else (target,)
    for group in targets:
        counts[group] = amount
    if len({counts[group] for group in WORD_CONSUMERS}) != 1:
        raise ValueError("filter_language y aggregation deben tener la misma cantidad de replicas")

    volumes = config.setdefault("volumes", {})
    old_replica_volumes = set()
    services = {}
    emitted = set()
    for name, service in original.items():
        group = group_of(name)
        if not group:
            services[name] = deepcopy(service)
            continue
        for volume in service.get("volumes", []):
            if volume["type"] == "volume":
                old_replica_volumes.add(volume["source"])
        if group in emitted:
            continue
        emitted.add(group)
        for index in range(counts[group]):
            # Conservar ajustes particulares de replicas que ya existian.
            clone = deepcopy(replicas[group].get(index, replicas[group][0]))
            clone.pop("container_name", None)
            clone.pop("scale", None)
            if "deploy" in clone:
                clone["deploy"].pop("replicas", None)
            env = clone.setdefault("environment", {})
            if group != "client":
                env["ID"] = str(index)
            else:
                path = Path(env["OUTPUT_PATH"])
                stem = re.sub(r"_\d+$", "", path.stem)
                env["OUTPUT_PATH"] = str(path.with_name(f"{stem}_{index}{path.suffix}")).replace("\\", "/")
            if index not in replicas[group]:
                for volume in clone.get("volumes", []):
                    if volume["type"] == "volume":
                        source = volume["source"]
                        volume["source"] = f"{source}_{index}"
                        definition = deepcopy(volumes.get(source))
                        if definition and (definition.get("external") or definition.get("name")):
                            raise ValueError(f"El volumen {source} tiene nombre fijo o es externo; no se puede clonar")
                        volumes[volume["source"]] = definition
            services[f"{group}_{index}"] = clone

    for name, service in services.items():
        env = service.get("environment", {})
        if group_of(name) == "splitter":
            env["OUTPUT_EXCHANGE"] = "splitter_output"
            for variable in ("EXCHANGE_QUERY_3", "EXCHANGE_QUERY_4", "EXCHANGE_QUERY_5"):
                env.pop(variable, None)
        elif group_of(name) in SPLITTER_CONSUMERS:
            env["INPUT_EXCHANGE"] = "splitter_output"
        for variable, group in COUNTERS.items():
            if variable in env:
                env[variable] = str(counts[group])
        for query, group in ((1, "filter"), (2, "image_getter"), (3, "filter_language"), (5, "filter_average")):
            variable = f"QUERY_{query}_REPLICAS"
            if variable in env:
                env[variable] = str(counts[group])
        if "QUERY_4_REPLICAS" in env:
            env["QUERY_4_REPLICAS"] = "1"  # Join emite el unico final de Q4.

        # Reconstruir el orden de arranque, incluso si el compose usa el anterior.
        if name.startswith("client_"):
            dependencies = ("gateway", "handler")
        elif name == "gateway":
            dependencies = tuple(
                dependency for dependency in services
                if dependency != "gateway" and not dependency.startswith("client_")
            )
        elif group_of(name) in WORKERS or name in ("handler", "join"):
            dependencies = ("rabbitmq",)
        else:
            continue
        service["depends_on"] = {
            dependency: {
                "condition": "service_healthy" if dependency == "rabbitmq" else "service_started",
                "required": True,
            }
            for dependency in dependencies
        }

    config["services"] = services
    used_volumes = {
        volume["source"] for service in services.values()
        for volume in service.get("volumes", []) if volume["type"] == "volume"
    }
    for source in old_replica_volumes - used_volumes:
        volumes.pop(source, None)
    return config, counts


def compose_config(path, *options):
    result = subprocess.run(
        ["docker", "compose", "--project-directory", str(path.parent), "-f", str(path), "config", *options],
        capture_output=True, text=True, encoding="utf-8",
    )
    if result.returncode:
        raise ValueError(result.stderr.strip() or "Docker Compose no pudo validar la configuracion")
    return result.stdout


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("servicio", help="Nombre del worker, client o todos (excluye clientes y servicios unicos)")
    parser.add_argument("cantidad", type=int)
    parser.add_argument("--compose-file", type=Path, default=Path(__file__).with_name("docker-compose.yaml"))
    args = parser.parse_args()
    target = re.sub(r"_\d+$", "", args.servicio.replace("-", "_"))
    if target in SINGLETONS:
        parser.error(f"{target} usa una sola instancia en la arquitectura actual")
    if target not in (*GROUPS, "todos"):
        parser.error("Servicio desconocido. Opciones: " + ", ".join((*GROUPS, "todos")))
    if args.cantidad < 1:
        parser.error("La cantidad debe ser un entero mayor o igual a 1")

    path = args.compose_file.resolve()
    temporary = None
    try:
        config = json.loads(compose_config(path, "--no-normalize", "--no-path-resolution", "--no-interpolate", "--format", "json"))
        updated, counts = scale_config(config, target, args.cantidad)
        # Compose convierte JSON a YAML y valida antes de reemplazar el archivo.
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent, suffix=".yaml", delete=False) as output:
            temporary = Path(output.name)
            json.dump(updated, output)
        yaml = compose_config(temporary, "--no-normalize", "--no-path-resolution", "--no-interpolate")
        temporary.write_text(yaml, encoding="utf-8", newline="\n")
        compose_config(temporary, "--quiet")
        os.replace(temporary, path)
        print(f"Actualizado {path.name}: " + ", ".join(f"{group}={count}" for group, count in counts.items()))
        print("No se iniciaron contenedores. Para aplicar: docker compose up --build --remove-orphans")
    except (OSError, ValueError) as error:
        parser.exit(1, f"Error: {error}\n")
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
