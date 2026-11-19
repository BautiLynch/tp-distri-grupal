import glob
import json
import os

from common.message_protocol import external
from common.message_protocol.queries import QueryId

Q2_DIRECTORY = "q2"
Q2_REMAINING_FILE = "resto.bin"
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

    def __init__(self, output_dir, image_limit):
        self.output_dir = output_dir
        self.q2_dir = os.path.join(output_dir, Q2_DIRECTORY)
        self.image_limit = image_limit
        self.saved_images = 0
        self.writers = {}
        self.remaining_images = None

    def __enter__(self):
        os.makedirs(self.q2_dir, exist_ok=True)
        # Una corrida anterior pudo guardar mas imagenes que esta.
        for image_path in glob.glob(os.path.join(glob.escape(self.q2_dir), "img_*")):
            os.remove(image_path)
        self.remaining_images = open(os.path.join(self.q2_dir, Q2_REMAINING_FILE), "wb")
        try:
            self.writers = {
                QueryId.Q1: JsonArrayWriter(os.path.join(self.output_dir, "q1.json")),
                QueryId.Q2: JsonArrayWriter(os.path.join(self.q2_dir, "q2.json")),
                QueryId.Q3: JsonArrayWriter(os.path.join(self.output_dir, "q3.json")),
                QueryId.Q4: JsonArrayWriter(os.path.join(self.output_dir, "q4.json")),
                QueryId.Q5: JsonArrayWriter(os.path.join(self.output_dir, "q5.json")),
            }
        except OSError:
            self.__exit__(None, None, None)
            raise
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        for writer in self.writers.values():
            writer.close()
        self.remaining_images.close()

    def write(self, query_id, fields):
        writer = self.writers.get(query_id)
        match query_id:
            case QueryId.Q1:
                url, date_modified = fields
                writer.write({"url": url, "date_modified": date_modified})
            case QueryId.Q2:
                name, url, image = fields
                location = self._store_image(query_id, fields, image)
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

    def _store_image(self, query_id, fields, image):
        if not image:
            return None
        if self.saved_images >= self.image_limit:
            self.remaining_images.write(
                external.serialize_msg(external.MsgType.RESULT, query_id, *fields)
            )
            return Q2_REMAINING_FILE
        self.saved_images += 1
        file_name = f"img_{self.saved_images:04d}{image_extension(image)}"
        with open(os.path.join(self.q2_dir, file_name), "wb") as image_file:
            image_file.write(image)
        return file_name
