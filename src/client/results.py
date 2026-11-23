import glob
import json
import os
from pathlib import Path

from common.message_protocol.queries import QueryId

Q2_DIRECTORY = "q2"
IMAGE_SIGNATURES = (
    (b"\xff\xd8\xff", ".jpg"),
    (b"\x89PNG\r\n\x1a\n", ".png"),
    (b"GIF8", ".gif"),
)


def image_extension(image):
    for signature, extension in IMAGE_SIGNATURES:
        if image.startswith(signature):
            return extension
    if image[:4] == b"RIFF" and image[8:12] == b"WEBP":
        return ".webp"
    if b"<svg" in image[:1024]:
        return ".svg"
    return ".img"


class JsonArrayWriter:
    """Escribe un array JSON registro a registro, sin acumularlo en memoria."""

    def __init__(self, path):
        self.file = open(path, "w", encoding="utf-8")
        self.is_empty = True
        self.file.write("[")

    def write(self, record):
        separator = "\n" if self.is_empty else ",\n"
        self.file.write(f"{separator}  {json.dumps(record, ensure_ascii=False)}")
        self.is_empty = False

    def close(self):
        self.file.write("]\n" if self.is_empty else "\n]\n")
        self.file.close()


class ResultWriter:
    """Guarda cada query en su archivo dentro de la carpeta del cliente."""

    def __init__(self, output_dir):
        self.output_dir = output_dir
        self.q2_dir = os.path.join(output_dir, Q2_DIRECTORY)
        self.saved_images = 0
        self.writers = {}

    def __enter__(self):
        os.makedirs(self.q2_dir, exist_ok=True)
        # Una corrida anterior pudo guardar mas imagenes que esta.
        for image_path in glob.glob(os.path.join(glob.escape(self.q2_dir), "img_*")):
            os.remove(image_path)
        Path(self.q2_dir, "resto.bin").unlink(missing_ok=True)
        paths = {
            QueryId.Q1: os.path.join(self.output_dir, "q1.json"),
            QueryId.Q2: os.path.join(self.q2_dir, "q2.json"),
            QueryId.Q3: os.path.join(self.output_dir, "q3.json"),
            QueryId.Q4: os.path.join(self.output_dir, "q4.json"),
            QueryId.Q5: os.path.join(self.output_dir, "q5.json"),
        }
        try:
            for query_id, path in paths.items():
                self.writers[query_id] = JsonArrayWriter(path)
        except OSError:
            self.__exit__(None, None, None)
            raise
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        for writer in self.writers.values():
            writer.close()

    def write(self, query_id, fields):
        writer = self.writers.get(query_id)
        match query_id:
            case QueryId.Q1:
                url, date_modified = fields
                writer.write({"url": url, "date_modified": date_modified})
            case QueryId.Q2:
                name, url, image = fields
                location = self._store_image(image)
                writer.write({
                    "name": name,
                    "url": url,
                    "image": location,
                    "image_size": len(image),
                })
            case QueryId.Q3:
                word, = fields
                writer.write({"word": word})
            case QueryId.Q4:
                top_list, = fields
                for word, amount in top_list:
                    writer.write({"word": word, "amount": amount})
            case QueryId.Q5:
                url, amount = fields
                writer.write({"url": url, "amount": amount})
            case _:
                raise ValueError(f"Unexpected query result {query_id}")

    def _store_image(self, image):
        if not image:
            return None
        self.saved_images += 1
        file_name = f"img_{self.saved_images:04d}{image_extension(image)}"
        with open(os.path.join(self.q2_dir, file_name), "wb") as image_file:
            image_file.write(image)
        return file_name
