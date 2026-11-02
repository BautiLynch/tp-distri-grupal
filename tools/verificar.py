"""Verifica los archivos de salida del cliente contra los resultados esperados.

Los resultados esperados se calculan desde el dataset, replicando la semantica
real del pipeline (splitter -> filter/filter_year -> average/filter_average).

Uso:
    py -3 tools/verificar.py
    py -3 tools/verificar.py --dataset datasets/wikipedia-1000-v2.jsonl output/output_v2_0.bin
    py -3 tools/verificar.py --dataset datasets/wikipedia-1000-v2.jsonl output/*.bin

Si no se pasan archivos de salida, se usan todos los output/*.bin.
Sale con codigo 0 si todo coincide y 1 si hay diferencias.
"""

import argparse
import collections
import glob
import heapq
import json
import os
import struct
import sys
from datetime import datetime

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

RESULT = 6
UINT32_SIZE = 4

QUERY_NAMES = {
    1: "Q1",
    2: "Q2",
    3: "Q3",
    4: "Q4",
    5: "Q5",
}


# --------------------------------------------------------------------------- #
# Lectura del dataset
# --------------------------------------------------------------------------- #
def resolve(path):
    if os.path.isabs(path) or os.path.exists(path):
        return path
    alternative = os.path.join(REPO_ROOT, path)
    return alternative if os.path.exists(alternative) else path


def load_articles(path):
    articles = []
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                articles.append(json.loads(line))
    return articles


def load_stopwords(directory):
    stopwords = {}
    for name in ("english", "french"):
        path = os.path.join(directory, f"{name}.txt")
        if os.path.exists(path):
            with open(path, encoding="utf-8") as handle:
                stopwords[name[:2]] = {line.strip() for line in handle.readlines()}
        else:
            print(f"aviso: no se encontro {path}", file=sys.stderr)
    return stopwords


def text_of(article, field, default=""):
    value = article.get(field)
    return value if isinstance(value, str) else default


def language_of(article):
    language = article.get("in_language")
    if not isinstance(language, dict):
        return None
    return language.get("identifier")


def word_occurrences(article, stopwords):
    """Palabras no-stopword del articulo, sin deduplicar (el splitter manda el amount)."""
    ignored = stopwords.get(language_of(article), set())
    abstract = text_of(article, "abstract")
    return [word for word in abstract.split(" ") if word not in ignored]


def year_of(article):
    return datetime.fromisoformat(article["date_modified"].replace("Z", "+00:00")).year


def section_names(article):
    names = []

    def walk(sections):
        for section in sections or []:
            if not isinstance(section, dict) or section.get("type") != "section":
                continue
            if section.get("name") is not None:
                names.append(section["name"])
            walk(section.get("has_parts"))

    sections = article.get("sections")
    if isinstance(sections, list):
        walk(sections)
    return names


def expected_results(articles, stopwords, year, language, sections, top_size):
    wanted_sections = {section.lower() for section in sections}

    q1 = collections.Counter()
    q2 = collections.Counter()
    q3_en, q3_fr = set(), set()
    word_articles = collections.Counter()
    pre_words = collections.Counter()   
    post_words = collections.Counter()  

    for article in articles:
        article_language = language_of(article)
        article_year = year_of(article)
        url = article["url"]
        occurrences = word_occurrences(article, stopwords)
        words = set(occurrences)

        if article_year <= year:
            if article_language == language:
                q1[(url, article["date_modified"])] += 1
            names = {name.lower() for name in section_names(article)}
            if wanted_sections.intersection(names):
                q2[(text_of(article, "name"), url)] += 1
            pre_words[url] += len(occurrences)
        else:
            post_words[url] += len(occurrences)

        for word in words:
            word_articles[word] += 1

        if article_language == "en":
            q3_en |= words
        elif article_language == "fr":
            q3_fr |= words

    threshold = 0.0
    if pre_words:
        threshold = sum(pre_words.values()) / len(pre_words)

    q5 = {url: count for url, count in post_words.items() if count > threshold}
    q4 = dict(heapq.nlargest(top_size, word_articles.items(), key=lambda item: item[1]))

    return {
        1: q1,
        2: q2,
        3: q3_en & q3_fr,
        4: q4,
        5: q5,
        "threshold": threshold,
        "pre_urls": len(pre_words),
        "post_urls": len(post_words),
    }


# --------------------------------------------------------------------------- #
# Lectura de los .bin de salida
# --------------------------------------------------------------------------- #
def read_records(path):
    with open(path, "rb") as handle:
        data = handle.read()

    records = []
    offset = 0
    while offset < len(data):
        if offset + 12 > len(data):
            raise ValueError(f"{path}: registro truncado en el byte {offset}")
        kind, query, size = struct.unpack(">III", data[offset:offset + 12])
        offset += 12
        if offset + size > len(data):
            raise ValueError(f"{path}: payload truncado en el byte {offset}")
        records.append((kind, query, data[offset:offset + size]))
        offset += size
    return records


def read_text(payload, offset):
    size = struct.unpack(">I", payload[offset:offset + 4])[0]
    start = offset + 4
    return payload[start:start + size].decode("utf-8"), start + size


def read_uint32(payload, offset):
    return struct.unpack(">I", payload[offset:offset + 4])[0], offset + 4


def parse_payload(query, payload):
    if query == 1:
        url, offset = read_text(payload, 0)
        date_modified, _ = read_text(payload, offset)
        return (url, date_modified)
    if query == 2:
        name, offset = read_text(payload, 0)
        url, offset = read_text(payload, offset)
        size, offset = read_uint32(payload, offset)
        return (name, url), size
    if query == 3:
        word, _ = read_text(payload, 0)
        return word
    if query == 4:
        count, offset = read_uint32(payload, 0)
        items = []
        for _ in range(count):
            word, offset = read_text(payload, offset)
            amount, offset = read_uint32(payload, offset)
            items.append((word, amount))
        return items
    if query == 5:
        url, offset = read_text(payload, 0)
        amount, _ = read_uint32(payload, offset)
        return (url, amount)
    raise ValueError(f"query desconocida: {query}")


def received_results(path):
    q1 = collections.Counter()
    q2 = collections.Counter()
    q2_sizes = {}
    q3 = set()
    q4 = {}
    q5 = {}
    unknown = collections.Counter()

    for kind, query, payload in read_records(path):
        if kind != RESULT:
            continue
        if query not in QUERY_NAMES:
            unknown[query] += 1
            continue
        parsed = parse_payload(query, payload)
        if query == 1:
            q1[parsed] += 1
        elif query == 2:
            key, size = parsed
            q2[key] += 1
            q2_sizes[key] = size
        elif query == 3:
            q3.add(parsed)
        elif query == 4:
            q4 = dict(parsed)
        elif query == 5:
            url, amount = parsed
            q5[url] = amount

    return {1: q1, 2: q2, 3: q3, 4: q4, 5: q5}, q2_sizes, unknown


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
    received, q2_sizes, unknown = received_results(path)
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
    if not ok and sorted(expected[4].values()) == sorted(received[4].values()):
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

    if unknown:
        failures += 1
        print(f"  queries desconocidas en el archivo: {dict(unknown)}")

    return failures


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("outputs", nargs="*", help="archivos .bin del cliente")
    parser.add_argument("--dataset", default="datasets/wikipedia-1000.jsonl")
    parser.add_argument("--stopwords", default="src/splitter/stopwords")
    parser.add_argument("--year", type=int, default=2020, help="YEAR del filtro de anio")
    parser.add_argument("--language", default="en", help="LANGUAGE del filter_0")
    parser.add_argument("--sections", default="Biography,Biographie")
    parser.add_argument("--top", type=int, default=20, help="TOP_SIZE de la Q4")
    parser.add_argument("--esperado", action="store_true", help="solo mostrar los valores esperados")
    args = parser.parse_args()

    dataset_path = resolve(args.dataset)
    if not os.path.exists(dataset_path):
        print(f"error: no existe el dataset {dataset_path}", file=sys.stderr)
        return 2

    articles = load_articles(dataset_path)
    stopwords = load_stopwords(resolve(args.stopwords))
    expected = expected_results(
        articles, stopwords,
        year=args.year,
        language=args.language,
        sections=[section for section in args.sections.split(",") if section],
        top_size=args.top,
    )

    print(f"dataset: {dataset_path}")
    print(f"articulos: {len(articles)}")
    print(f"Q5: umbral {expected['threshold']:.4f} sobre {expected['pre_urls']} urls <= {args.year}"
          f"; {expected['post_urls']} urls > {args.year}")
    print("esperado: " + ", ".join(
        f"Q{query}={len(expected[query])}" for query in range(1, 6)
    ))

    if args.esperado:
        return 0

    outputs = args.outputs or sorted(glob.glob(resolve("output/*.bin")))
    if not outputs:
        print("\nno hay archivos de salida para comparar")
        return 0

    failures = 0
    for path in outputs:
        path = resolve(path)
        if not os.path.exists(path):
            print(f"\n== {path}\n  error: no existe")
            failures += 1
            continue
        failures += verify_file(path, expected)

    print("\nRESULTADO:", "OK" if failures == 0 else f"{failures} verificacion(es) con diferencias")
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
