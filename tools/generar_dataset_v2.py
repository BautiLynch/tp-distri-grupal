"""Genera datasets/wikipedia-1000-v2.jsonl a partir del dataset original.

Uso: py -3 <este script>
"""

import json

SRC = "datasets/wikipedia-1000.jsonl"
DST = "datasets/wikipedia-1000-v2.jsonl"

PRE_2020 = 340
PRE_YEARS = [2012, 2015, 2018, 2019, 2020]
FR_TEMPLATES = [
    "Il est un champion de football et un record mondial dans son club",
    "Le festival de jazz a attire un public nombreux et un succes mondial",
    "La ville a un club de football et un record de visites pour son festival",
    "Un champion de tennis a gagne un titre mondial et un record personnel",
    "Le musee a presente un festival de jazz et un concert pour le public",
]
FR_EXTRA = ["national", "regional", "historique", "moderne", "annuel"]
BROKEN_404 = "https://upload.wikimedia.org/wikipedia/commons/0/00/no-existe-404.jpg"
UNREACHABLE = "http://127.0.0.1:1/imagen.png"
DUP_TARGETS = (400, 450, 500, 550, 600, 650, 700, 750, 800, 850)


def main():
    with open(SRC, encoding="utf-8") as handle:
        originals = [json.loads(line) for line in handle if line.strip()]

    real_images = [
        record["image"]["content_url"]
        for record in originals
        if isinstance(record.get("image"), dict) and record["image"].get("content_url")
    ]

    output = []
    bio_index = 0
    for index, original in enumerate(originals):
        record = json.loads(json.dumps(original))

        # --- fecha ---
        if index == 0:
            date_modified = "2020-12-31T23:59:59Z"
        elif index == PRE_2020:
            date_modified = "2021-01-01T00:00:00Z"
        else:
            if index < PRE_2020:
                year = PRE_YEARS[index % len(PRE_YEARS)]
            else:
                year = 2021 + (index % 4)
            month = 1 + (index * 7) % 12
            day = 1 + (index * 13) % 28
            hour = (index * 5) % 24
            minute = (index * 11) % 60
            second = (index * 17) % 60
            date_modified = f"{year}-{month:02d}-{day:02d}T{hour:02d}:{minute:02d}:{second:02d}Z"
        record["date_modified"] = date_modified

        # --- idioma ---
        french = (index % 10) < 3
        record["in_language"] = {"identifier": "fr" if french else "en"}

        # --- abstract ---
        if french:
            record["abstract"] = (
                FR_TEMPLATES[index % len(FR_TEMPLATES)] + " " + FR_EXTRA[index % len(FR_EXTRA)]
            )
        else:
            abstract = record.get("abstract")
            if not (isinstance(abstract, str) and abstract.strip()):
                if index % 4 == 0:
                    record["abstract"] = ""
                else:
                    record.pop("abstract", None)
            if index % 50 == 0:
                record["abstract"] = (record.get("abstract") or "") + " champion football record"

        # --- biografias (<= 2020) con imagenes variadas ---
        sections = record.get("sections")
        if not isinstance(sections, list):
            sections = []
        if index < PRE_2020 and index % 6 == 0:
            name = "Biographie" if french else "Biography"
            if index % 2 == 0 or not sections or not isinstance(sections[0], dict):
                sections.append({"type": "section", "name": name})
            else:
                sections[0].setdefault("has_parts", []).append(
                    {"type": "section", "name": name}
                )

            category = bio_index % 5
            if category == 0:
                record["image"] = {
                    "content_url": real_images[(bio_index * 7) % len(real_images)]
                }
            elif category == 1:
                record["image"] = {"content_url": BROKEN_404}
            elif category == 2:
                record["image"] = {"content_url": UNREACHABLE}
            elif category == 3:
                record["image"] = None
            else:
                record.pop("image", None)
            bio_index += 1
        record["sections"] = sections

        output.append(record)

    # --- urls repetidas (> 2020), para probar la acumulacion por url ---
    for target in DUP_TARGETS:
        if target + 1 < len(output):
            output[target + 1]["url"] = output[target]["url"]

    with open(DST, "w", encoding="utf-8", newline="\n") as handle:
        for record in output:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")

    print("generado:", DST)
    print("registros:", len(output))
    print("biografias <=2020:", bio_index)


if __name__ == "__main__":
    main()
