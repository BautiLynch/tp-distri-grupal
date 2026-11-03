import argparse
from collections import Counter
from datetime import datetime
import hashlib
import heapq
import json
import os
from pathlib import Path
import re
import signal
import sys
import tempfile
import time


PROJECT = Path(__file__).resolve().parents[1]


def biography_in(parts, accepted):
    for part in parts or ():
        if part.get("type") == "section":
            if (part.get("name") or "").lower() in accepted:
                return True
            if biography_in(part.get("has_parts"), accepted):
                return True
    return False


def analyze(source_path, ignored, cutoff_year, wanted_language, accepted, limit):
    dates, biographies = Counter(), Counter()
    vocabulary = {"en": set(), "fr": set()}
    frequencies = Counter()
    later = []
    seen = set()
    earlier_total = earlier_count = article_count = 0
    digest = hashlib.sha256()

    with source_path.open("rb") as source:
        for line_number, raw in enumerate(source, 1):
            digest.update(raw)
            if not raw.strip():
                continue
            try:
                entry = json.loads(raw)
                address = entry["url"]
                if not isinstance(address, str) or not address:
                    raise ValueError("URL vacia o invalida")
                if address in seen:
                    raise ValueError(f"URL duplicada: {address}")
                seen.add(address)
                locale = entry["in_language"]["identifier"]
                if locale not in vocabulary:
                    raise ValueError(f"idioma no soportado: {locale}")
                modified = entry["date_modified"]
                year = datetime.fromisoformat(modified.replace("Z", "+00:00")).year
                summary = entry.get("abstract")
                if summary is not None and not isinstance(summary, str):
                    raise ValueError("abstract debe ser texto o null")
                occurrences = [token for token in re.findall(r"\w+", (summary or "").lower())
                               if token not in ignored and not token.isdecimal()]
                distinct = set(occurrences)
                vocabulary[locale].update(distinct)
                frequencies.update(distinct)
                amount = len(occurrences)

                if year <= cutoff_year:
                    if locale == wanted_language:
                        dates[(address, modified)] += 1
                    if biography_in(entry.get("sections"), accepted):
                        biographies[(entry["name"], address)] += 1
                    if amount > 0:
                        earlier_total += amount
                        earlier_count += 1
                elif amount > 0:
                    later.append((address, amount))
                article_count += 1
            except (ValueError, KeyError, TypeError, AttributeError) as error:
                raise ValueError(f"{source_path}, linea {line_number}: {error}") from error
            if article_count % 100000 == 0:
                print(f"Procesados {article_count} articulos", file=sys.stderr)

    # Desempate estable; tambien se guardan todas las candidatas empatadas.
    ranking = heapq.nsmallest(limit, frequencies.items(), key=lambda pair: (-pair[1], pair[0]))
    boundary = ranking[-1][1] if ranking else 0
    candidates = {term: count for term, count in frequencies.items() if count >= boundary}
    threshold = earlier_total / earlier_count if earlier_count else 0
    # Comparacion entera: evita errores de redondeo justo en el umbral.
    selected = {address: count for address, count in later
                if count * earlier_count > earlier_total} if earlier_count else {
                    address: count for address, count in later if count > 0}
    return {
        "format": "wikipedia-serial-v2",
        "dataset": {"name": source_path.name, "sha256": digest.hexdigest(), "articles": article_count},
        "config": {"year": cutoff_year, "language": wanted_language,
                   "sections": sorted(accepted), "top": limit,
                   "tokens": "Unicode, lowercase, sin numeros, union stopwords en/fr",
                   "q5": "con repeticiones; solo abstracts con palabras validas, sin null ni ceros"},
        "q1": [[*key, count] for key, count in sorted(dates.items())],
        "q2": [[*key, count] for key, count in sorted(biographies.items())],
        "q3": sorted(vocabulary["en"] & vocabulary["fr"]),
        "q4": dict(ranking),
        "q4_candidates": dict(sorted(candidates.items())),
        "q5": dict(sorted(selected.items())),
        "threshold": threshold,
        "pre_urls": earlier_count,
        "post_urls": len(later),
    }


def save_reference(destination, result):
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=destination.parent,
                                         suffix=".tmp", delete=False) as output:
            temporary = Path(output.name)
            json.dump(result, output, ensure_ascii=False, indent=2)
            output.write("\n")
        os.replace(temporary, destination)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def cancel(signum, frame):
    raise KeyboardInterrupt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dataset", type=Path, help="dataset JSONL enviado al cliente")
    parser.add_argument("--output", required=True, type=Path, help="JSON nuevo para guardar la referencia")
    parser.add_argument("--stopwords", type=Path, default=PROJECT / "src/splitter/stopwords")
    parser.add_argument("--year", type=int, default=2020)
    parser.add_argument("--language", choices=("en", "fr"), default="en")
    parser.add_argument("--sections", default="Biography,Biographie")
    parser.add_argument("--top", type=int, default=20)
    args = parser.parse_args()
    if args.top < 1:
        parser.error("--top debe ser positivo")
    if args.output.exists():
        parser.error(f"ya existe {args.output}; elegi otro archivo para conservar la referencia")

    signal.signal(signal.SIGTERM, cancel)
    try:
        ignored = set()
        stopword_hashes = {}
        for filename in ("english.txt", "french.txt"):
            path = args.stopwords / filename
            raw = path.read_bytes()
            stopword_hashes[filename] = hashlib.sha256(raw).hexdigest()
            ignored.update(word.strip() for word in raw.decode("utf-8").splitlines())
        result = analyze(args.dataset, ignored, args.year, args.language,
                         {name.strip().lower() for name in args.sections.split(",") if name.strip()},
                         args.top)
        result["config"]["stopwords_sha256"] = stopword_hashes
        save_reference(args.output, result)
    except KeyboardInterrupt:
        print("Ejecucion serial interrumpida; no se publico la referencia.", file=sys.stderr)
        return 130
    except (OSError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    print(f"Referencia: {args.output} ({result['dataset']['articles']} articulos)")
    print(", ".join(f"Q{query}={len(result[f'q{query}'])}" for query in range(1, 6)))
    print(f"Promedio Q5: {result['threshold']:.6f}")
    return 0


if __name__ == "__main__":
    started_at = time.perf_counter()
    try:
        raise SystemExit(main())
    finally:
        print(f"Tiempo total: {time.perf_counter() - started_at:.3f} segundos", file=sys.stderr)
