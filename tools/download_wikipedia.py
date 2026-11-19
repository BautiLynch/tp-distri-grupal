#!/usr/bin/env python3
"""Descarga un JSONL de Wikipedia repartido por tamaño entre inglés y francés."""

import argparse
from contextlib import closing
from decimal import Decimal, InvalidOperation, ROUND_CEILING
from http.client import HTTPException
import json
from pathlib import Path
import tempfile
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen


API = "https://www.kaggle.com/api/v1/datasets"
DATASET = "wikimedia-foundation/wikipedia-structured-contents"
VERSION = 1
TIMEOUT = 90


def catalog():
    params = {"datasetVersionNumber": VERSION, "pageSize": 1000}
    while True:
        with urlopen(f"{API}/list/{DATASET}?{urlencode(params)}", timeout=TIMEOUT) as response:
            page = json.load(response)
        if page.get("errorMessage"):
            raise ValueError(page["errorMessage"])
        yield from page["datasetFiles"]
        if not page.get("nextPageToken"):
            return
        params["pageToken"] = page["nextPageToken"]


def articles(sources):
    for name, size in sources:
        url = f"{API}/download/{DATASET}/{quote(name, safe='')}?datasetVersionNumber={VERSION}"
        request = Request(url, headers={"Range": "bytes=0-"})
        with urlopen(request, timeout=TIMEOUT) as response:
            if response.status != 206 or response.headers.get("Content-Range") != f"bytes 0-{size - 1}/{size}":
                raise ValueError(f"Kaggle devolvió un rango inesperado para {name}.")
            remaining = size
            while remaining:
                raw = response.readline()
                remaining -= len(raw)
                if not raw or remaining < 0 or (remaining and not raw.endswith(b"\n")):
                    raise ValueError(f"Descarga incompleta o inválida: {name}.")
                yield raw if raw.endswith(b"\n") else raw + b"\n"


def download(target, output):
    if output.exists():
        raise FileExistsError(f"Ya existe {output}; elegí otro destino.")
    files = sorted((file["name"], file["totalBytes"]) for file in catalog()
                   if file["name"].endswith(".jsonl") and file["totalBytes"] > 0)
    english = (source for source in files if source[0].startswith("enwiki_"))
    french = (source for source in files if source[0].startswith("frwiki_"))
    output.parent.mkdir(parents=True, exist_ok=True)
    quota = (target + 1) // 2
    written = {"en": 0, "fr": 0}

    with tempfile.TemporaryDirectory(dir=output.parent) as directory:
        partial = Path(directory) / output.name
        with (partial.open("wb") as destination,
              closing(articles(english)) as en, closing(articles(french)) as fr):
            streams = {"en": en, "fr": fr}
            while min(written.values()) < quota:
                # Intercala los idiomas según los bytes escritos, sin mezclar en disco.
                language = min(written, key=written.__getitem__)
                raw = next(streams[language], None)
                if raw is None:
                    raise ValueError(f"No hay suficientes datos en {language} para {target:,} bytes.")
                if destination.write(raw) != len(raw):
                    raise OSError("Escritura incompleta del JSONL.")
                written[language] += len(raw)
        output.hardlink_to(partial)
    print(f"{output}: {sum(written.values()):,} bytes (en {written['en']:,}, fr {written['fr']:,}).")


def gigabytes(value):
    try:
        number = Decimal(value.replace(",", "."))
        if number.is_finite() and number > 0:
            return int((number * 1_000_000_000).to_integral_value(rounding=ROUND_CEILING))
    except InvalidOperation:
        pass
    raise argparse.ArgumentTypeError("GB debe ser un número positivo y finito.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gigabytes", required=True, type=gigabytes, help="GB decimales (1 GB = 10⁹ bytes)")
    parser.add_argument("--output", type=Path, required=True, help="carpeta de destino para wikipedia.jsonl")
    args = parser.parse_args()
    try:
        download(args.gigabytes, args.output.expanduser() / "wikipedia.jsonl")
    except KeyboardInterrupt:
        parser.exit(130, "Descarga interrumpida.\n")
    except (OSError, HTTPException, ValueError, KeyError) as error:
        parser.exit(1, f"Error: {error}\n")


if __name__ == "__main__":
    main()
