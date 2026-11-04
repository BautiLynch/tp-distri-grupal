#!/usr/bin/env python3
"""Descarga muestras JSONL de Wikipedia en inglés y francés (Python 3.8+).

Ejemplos:
    python3 tools/download_wikipedia.py --gigabytes 1.5
    python3 tools/download_wikipedia.py --articles 100000 500000 1000000

Usa la versión 1 de Kaggle, compatible con el JSONL original del proyecto.
No requiere paquetes externos ni credenciales. Los registros se conservan
completos. La selección por ventanas no es un muestreo uniforme por artículo.
"""

import argparse
from array import array
from collections import Counter
from contextlib import ExitStack, closing
from decimal import Decimal, InvalidOperation, ROUND_CEILING
import hashlib
import http.client
import json
from pathlib import Path
import random
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request


API = "https://www.kaggle.com/api/v1/datasets"
DATASET = "wikimedia-foundation/wikipedia-structured-contents"
VERSION = 1
TIMEOUT = 90
RETRIES = 5
WINDOWS = 4
BUFFER = 1024 * 1024
DEFAULT_OUTPUT = Path(__file__).resolve().parents[1] / "datasets"


def open_remote(request):
    return urllib.request.urlopen(request, timeout=TIMEOUT)


def retryable(error):
    if isinstance(error, urllib.error.HTTPError):
        return error.code in (408, 429, 500, 502, 503, 504)
    return isinstance(error, (OSError, http.client.HTTPException))


def catalog():
    files, token = [], None
    while True:
        query = {"datasetVersionNumber": VERSION, "pageSize": 1000}
        if token:
            query["pageToken"] = token
        url = f"{API}/list/{DATASET}?{urllib.parse.urlencode(query)}"
        for attempt in range(RETRIES):
            try:
                with open_remote(url) as response:
                    page = json.load(response)
                break
            except (OSError, http.client.HTTPException) as error:
                if not retryable(error) or attempt + 1 == RETRIES:
                    raise
                time.sleep(min(2 ** attempt, 15))
        if page.get("errorMessage"):
            raise RuntimeError(page["errorMessage"])
        files.extend(page.get("datasetFiles", []))
        token = page.get("nextPageToken")
        if not token:
            break
    return [file for file in files
            if file["name"].endswith(".jsonl") and file["totalBytes"] > 0]


def remote_lines(source, start):
    """Descarta el primer registro parcial y reanuda desde la última línea leída."""
    name = urllib.parse.quote(source["name"], safe="")
    url = f"{API}/download/{DATASET}/{name}?datasetVersionNumber={VERSION}"
    position, size = start, source["totalBytes"]
    discard_first, failures = start > 0, 0
    while position < size:
        request = urllib.request.Request(url, headers={"Range": f"bytes={position}-"})
        try:
            with open_remote(request) as response:
                content_range = response.headers.get("Content-Range", "")
                if response.status != 206 or not content_range.startswith(f"bytes {position}-"):
                    raise ValueError("Kaggle no respetó el rango de bytes solicitado.")
                if discard_first:
                    raw = response.readline()
                    if not raw.endswith(b"\n"):
                        if position + len(raw) == size:
                            return
                        raise OSError("Lectura incompleta del primer registro parcial.")
                    position += len(raw)
                    discard_first = False
                while position < size:
                    raw = response.readline()
                    if not raw or (not raw.endswith(b"\n") and position + len(raw) != size):
                        raise OSError("La conexión terminó en medio de un registro.")
                    position += len(raw)
                    if position > size:
                        raise ValueError("La respuesta excedió el tamaño anunciado.")
                    failures = 0
                    yield raw if raw.endswith(b"\n") else raw + b"\n"
        except (OSError, http.client.HTTPException) as error:
            failures += 1
            if not retryable(error) or failures >= RETRIES:
                raise
            print(f"Reintentando {source['name']} desde el byte {position}: {error}", flush=True)
            time.sleep(min(2 ** failures, 15))


class LanguageSample:
    def __init__(self, language, path):
        self.language = language
        self.path = path
        self.offsets = array("Q")
        self.identifiers = set()
        self.urls = set()
        self.sources = Counter()

    def collect(self, destination, source, start, limit):
        if limit <= 0:
            return
        accepted = 0
        with closing(remote_lines(source, start)) as lines:
            for raw in lines:
                record = json.loads(raw)
                language = record.get("in_language", {}).get("identifier")
                identifier, url = record["identifier"], record["url"]
                if language != self.language or not url.startswith(f"https://{language}.wikipedia.org/"):
                    raise ValueError(f"Idioma o URL inesperados en {source['name']}.")
                if identifier in self.identifiers or url in self.urls:
                    continue
                self.offsets.append(destination.tell())
                if destination.write(raw) != len(raw):
                    raise OSError("Escritura incompleta en la muestra temporal.")
                self.identifiers.add(identifier)
                self.urls.add(url)
                self.sources[source["name"]] += 1
                accepted += 1
                if accepted == limit:
                    break

    def release_index_keys(self):
        # Ya se verificó la unicidad; para mezclar sólo hacen falta los offsets.
        self.identifiers.clear()
        self.urls.clear()


def sample_language(language, files, target, work, seed):
    sample = LanguageSample(language, work / f"{language}.jsonl")
    sources = sorted((file for file in files if file["name"].startswith(language + "wiki_")),
                     key=lambda file: file["name"])
    if target and not sources:
        raise ValueError(f"La versión {VERSION} no contiene JSONL para {language}.")
    total_bytes = sum(source["totalBytes"] for source in sources)
    quotas = [target * source["totalBytes"] // total_bytes for source in sources]
    for i in range(target - sum(quotas)):
        quotas[i % len(quotas)] += 1
    with sample.path.open("wb", buffering=BUFFER) as destination:
        for source, quota in zip(sources, quotas):
            salt = int.from_bytes(hashlib.sha256(source["name"].encode()).digest()[:8], "big")
            rng, size = random.Random(seed + salt), source["totalBytes"]
            for window in range(WINDOWS):
                amount = quota // WINDOWS + (window < quota % WINDOWS)
                if amount == 0:
                    continue
                lower, upper = size * window // WINDOWS, size * (window + 1) // WINDOWS
                margin = min(64 * BUFFER, (upper - lower) // 2)
                start = rng.randrange(lower, max(lower + 1, upper - margin))
                sample.collect(destination, source, start, amount)
            if quota:
                print(f"{language}: {len(sample.offsets):,}/{target:,} — {source['name']}", flush=True)
        # Una ventana puede llegar a EOF o contener repeticiones. Completar
        # desde otros registros, sin exigir una cantidad fija a cada fragmento.
        for source in sources:
            missing = target - len(sample.offsets)
            if missing == 0:
                break
            sample.collect(destination, source, 0, missing)
    if len(sample.offsets) != target:
        raise ValueError(f"Sólo se encontraron {len(sample.offsets)} artículos únicos en {language}.")
    sample.release_index_keys()
    random.Random(seed + (language == "fr")).shuffle(sample.offsets)
    return sample


def write_outputs(samples, sizes, work, seed):
    digest, counts, checkpoints = hashlib.sha256(), Counter(), {}
    rng, written = random.Random(seed + 2), 0
    with ExitStack() as stack:
        inputs = {lang: stack.enter_context(sample.path.open("rb", buffering=8192))
                  for lang, sample in samples.items()}
        outputs = {size: stack.enter_context((work / f"wikipedia-{size}.jsonl").open("wb", buffering=BUFFER))
                   for size in sizes}
        for pair in range(len(samples["en"].offsets)):
            order = ("en", "fr") if rng.getrandbits(1) else ("fr", "en")
            if pair >= len(samples["fr"].offsets):
                order = ("en",)
            for language in order:
                inputs[language].seek(samples[language].offsets[pair])
                raw = inputs[language].readline()
                if not raw.endswith(b"\n"):
                    raise ValueError("Registro temporal incompleto.")
                written += 1
                counts[language] += 1
                digest.update(raw)
                for size, output in outputs.items():
                    if written <= size and output.write(raw) != len(raw):
                        raise OSError("Escritura incompleta en un JSONL.")
                if written in outputs:
                    checkpoints[written] = {"sha256": digest.hexdigest(), "languages": dict(counts)}
                if written % 100_000 == 0:
                    print(f"Escritos {written:,} artículos.", flush=True)
    if written != max(sizes):
        raise ValueError("La cantidad escrita no coincide con la solicitada.")
    return checkpoints


def verify_outputs(sizes, checkpoints, work):
    verified = []
    for size in sizes:
        path, digest, lines = work / f"wikipedia-{size}.jsonl", hashlib.sha256(), 0
        with path.open("rb", buffering=4 * BUFFER) as stream:
            while chunk := stream.read(4 * BUFFER):
                digest.update(chunk)
                lines += chunk.count(b"\n")
        checkpoint = checkpoints[size]
        if lines != size or digest.hexdigest() != checkpoint["sha256"]:
            raise ValueError(f"Falló la verificación de {path.name}.")
        languages = {lang: checkpoint["languages"].get(lang, 0) for lang in ("en", "fr")}
        if abs(languages["en"] - languages["fr"]) > 1:
            raise ValueError("La mezcla de idiomas no está balanceada.")
        verified.append({"filename": path.name, "articles": size, "languages": languages,
                         "bytes": path.stat().st_size, "sha256": digest.hexdigest()})
        (work / (path.name + ".sha256")).write_text(f"{digest.hexdigest()}  {path.name}\n", encoding="utf-8")
    return verified


def generate(sizes, output, seed):
    sizes = sorted(set(sizes))
    if not sizes or sizes[0] < 1:
        raise ValueError("La cantidad de artículos debe ser positiva.")
    output.mkdir(parents=True, exist_ok=True)
    manifest_name = f"wikipedia-{max(sizes)}.manifest.json"
    names = [name for size in sizes for name in
             (f"wikipedia-{size}.jsonl", f"wikipedia-{size}.jsonl.sha256")] + [manifest_name]
    for name in names:
        if (output / name).exists():
            raise FileExistsError(f"El archivo ya existe: {output / name}. Elegí otra carpeta o cantidad.")
    files = catalog()
    if not files:
        raise ValueError("Kaggle no devolvió archivos JSONL de la versión 1.")
    print(f"Kaggle versión {VERSION}: {len(files)} fragmentos disponibles.", flush=True)
    # También los temporales de preparación viven dentro de la carpeta elegida.
    with tempfile.TemporaryDirectory(prefix=".wikipedia-build-", dir=output) as temporary:
        work = Path(temporary)
        largest = max(sizes)
        samples = {lang: sample_language(lang, files, target, work, seed)
                   for lang, target in (("en", (largest + 1) // 2), ("fr", largest // 2))}
        checkpoints = write_outputs(samples, sizes, work, seed)
        verified = verify_outputs(sizes, checkpoints, work)
        manifest = {
            "source": f"https://www.kaggle.com/datasets/{DATASET}/versions/{VERSION}",
            "seed": seed, "nested": True, "complete_records": True,
            "unique_by": ["(language, identifier)", "url"],
            "sampling": "Ventanas por fragmento, cuotas iniciales proporcionales a bytes; faltantes desde prefijos. No es una muestra uniforme por artículo.",
            "selected_per_source": {lang: dict(sample.sources) for lang, sample in samples.items()},
            "files": verified,
        }
        (work / manifest_name).write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        for name in names:
            if (output / name).exists():
                raise FileExistsError(f"El destino apareció durante la descarga: {name}")
        for name in names:
            (work / name).rename(output / name)
    for entry in verified:
        print(f"Listo: {output / entry['filename']} — {entry['articles']:,} artículos, {entry['languages']}")


def size_records(language, files, target_bytes, seed):
    """Registros unicos por idioma, desde ventanas y luego prefijos de respaldo."""
    sources = sorted((source for source in files if source["name"].startswith(language + "wiki_")),
                     key=lambda source: source["name"])
    if not sources:
        raise ValueError(f"La version {VERSION} no contiene JSONL para {language}.")
    identifiers, urls = set(), set()
    budget = max(1, target_bytes // (2 * len(sources) * WINDOWS))

    def records(source, start, byte_limit=None):
        accepted_bytes = 0
        with closing(remote_lines(source, start)) as lines:
            for raw in lines:
                record = json.loads(raw)
                actual_language = record.get("in_language", {}).get("identifier")
                identifier, url = record["identifier"], record["url"]
                if actual_language != language or not url.startswith(f"https://{language}.wikipedia.org/"):
                    raise ValueError(f"Idioma o URL inesperados en {source['name']}.")
                if identifier in identifiers or url in urls:
                    continue
                identifiers.add(identifier)
                urls.add(url)
                accepted_bytes += len(raw)
                yield source["name"], raw
                if byte_limit is not None and accepted_bytes >= byte_limit:
                    break

    for source in sources:
        salt = int.from_bytes(hashlib.sha256(source["name"].encode()).digest()[:8], "big")
        rng, size = random.Random(seed + salt), source["totalBytes"]
        for window in range(WINDOWS):
            lower, upper = size * window // WINDOWS, size * (window + 1) // WINDOWS
            margin = min(64 * BUFFER, (upper - lower) // 2)
            start = rng.randrange(lower, max(lower + 1, upper - margin))
            yield from records(source, start, budget)
    for source in sources:
        yield from records(source, 0)


def generate_bytes(target_bytes, output, seed):
    """Escribe hasta alcanzar el objetivo; excedente menor a un registro completo."""
    output.mkdir(parents=True, exist_ok=True)
    names = ("wikipedia.jsonl", "wikipedia.jsonl.sha256", "wikipedia.manifest.json")
    for name in names:
        if (output / name).exists():
            raise FileExistsError(f"El archivo ya existe: {output / name}. Elegi otra carpeta.")
    files = catalog()
    if not files:
        raise ValueError("Kaggle no devolvio archivos JSONL de la version 1.")
    counts, selected = Counter(), {lang: Counter() for lang in ("en", "fr")}
    digest, written, articles = hashlib.sha256(), 0, 0
    next_progress = 64 * BUFFER
    print(f"Objetivo JSONL: {target_bytes:,} bytes ({target_bytes / 1_000_000_000:g} GB).", flush=True)
    with tempfile.TemporaryDirectory(prefix=".wikipedia-build-", dir=output) as temporary:
        work = Path(temporary)
        with ExitStack() as stack:
            streams = {lang: stack.enter_context(closing(size_records(lang, files, target_bytes, seed)))
                       for lang in ("en", "fr")}
            destination = stack.enter_context((work / names[0]).open("wb", buffering=BUFFER))
            rng = random.Random(seed + 2)
            while written < target_bytes:
                order = ("en", "fr") if rng.getrandbits(1) else ("fr", "en")
                for language in order:
                    try:
                        source_name, raw = next(streams[language])
                    except StopIteration:
                        raise ValueError(f"No hay suficientes articulos unicos en {language} para alcanzar el tamano.") from None
                    if destination.write(raw) != len(raw):
                        raise OSError("Escritura incompleta en el JSONL.")
                    digest.update(raw)
                    written += len(raw)
                    articles += 1
                    counts[language] += 1
                    selected[language][source_name] += 1
                    if written >= next_progress:
                        print(f"Descargados {written:,}/{target_bytes:,} bytes; {articles:,} articulos.", flush=True)
                        next_progress = written + 64 * BUFFER
                    if written >= target_bytes:
                        break
        # Verificar bytes, lineas y checksum antes de publicar el resultado.
        actual_digest, actual_lines = hashlib.sha256(), 0
        with (work / names[0]).open("rb", buffering=4 * BUFFER) as source:
            while chunk := source.read(4 * BUFFER):
                actual_digest.update(chunk)
                actual_lines += chunk.count(b"\n")
        if (actual_lines != articles or (work / names[0]).stat().st_size != written
                or actual_digest.hexdigest() != digest.hexdigest()):
            raise ValueError("Fallo la verificacion del JSONL descargado.")
        entry = {"filename": names[0], "articles": articles, "languages": dict(counts),
                 "bytes": written, "sha256": digest.hexdigest()}
        manifest = {"source": f"https://www.kaggle.com/datasets/{DATASET}/versions/{VERSION}",
                    "seed": seed, "target_bytes": target_bytes, "excess_bytes": written - target_bytes,
                    "complete_records": True, "unique_by": ["(language, identifier)", "url"],
                    "sampling": "Ventanas por fragmento, mezcla alternada en/fr; no uniforme por articulo.",
                    "selected_per_source": {lang: dict(values) for lang, values in selected.items()},
                    "files": [entry]}
        (work / names[1]).write_text(f"{digest.hexdigest()}  {names[0]}\n", encoding="utf-8")
        (work / names[2]).write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        for name in names:
            if (output / name).exists():
                raise FileExistsError(f"El destino aparecio durante la descarga: {name}")
        for name in names:
            (work / name).rename(output / name)
    print(f"Listo: {output / names[0]} — {articles:,} articulos descargados; "
          f"{written:,} bytes ({written / 1_000_000_000:.6f} GB); "
          f"excedente: {written - target_bytes:,} bytes; idiomas: {dict(counts)}.", flush=True)
    return entry


def gigabytes_to_bytes(value):
    try:
        number = Decimal(value.replace(",", "."))
    except InvalidOperation:
        raise argparse.ArgumentTypeError("GB debe ser un numero positivo, por ejemplo 1.5.") from None
    if not number.is_finite() or number <= 0:
        raise argparse.ArgumentTypeError("GB debe ser un numero positivo y finito.")
    return int((number * 1_000_000_000).to_integral_value(rounding=ROUND_CEILING))


def positive_integer(value):
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("La cantidad debe ser mayor que cero.")
    return number


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    size = parser.add_mutually_exclusive_group(required=True)
    size.add_argument("--gigabytes", type=gigabytes_to_bytes,
                      help="Tamano JSONL en GB decimales (1 GB = 1000000000 bytes). Completa el ultimo articulo.")
    size.add_argument("--articles", nargs="+", type=positive_integer,
                        help="Uno o varios tamaños. Ejemplo: --articles 100000 500000 1000000")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT,
                        help=f"Carpeta permanente de salida (por defecto: {DEFAULT_OUTPUT}).")
    parser.add_argument("--seed", type=int, default=20261030, help="Semilla para selección y mezcla.")
    args = parser.parse_args()
    try:
        if args.gigabytes is not None:
            generate_bytes(args.gigabytes, args.output.expanduser().resolve(), args.seed)
        else:
            generate(args.articles, args.output.expanduser().resolve(), args.seed)
    except KeyboardInterrupt:
        print("\nDescarga interrumpida; temporales de preparación eliminados.", file=sys.stderr)
        return 130
    except Exception as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
