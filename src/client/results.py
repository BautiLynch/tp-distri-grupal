import glob
import json
import os

from common.message_protocol import external
from common.message_protocol.internal import MsgType, deserialize_payload

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
                MsgType.Q1_RESULT: JsonArrayWriter(os.path.join(self.output_dir, "q1.json")),
                MsgType.Q2_RESULT: JsonArrayWriter(os.path.join(self.q2_dir, "q2.json")),
                MsgType.Q3_RESULT: JsonArrayWriter(os.path.join(self.output_dir, "q3.json")),
                MsgType.Q4_RESULT: JsonArrayWriter(os.path.join(self.output_dir, "q4.json")),
                MsgType.Q5_RESULT: JsonArrayWriter(os.path.join(self.output_dir, "q5.json")),
            }
        except OSError:
            self.__exit__(None, None, None)
            raise
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        for writer in self.writers.values():
            writer.close()
        self.remaining_images.close()

    def write(self, query_id, payload):
        if query_id not in self.writers:
            raise ValueError(f"Unexpected query result {query_id}")
        fields = deserialize_payload(query_id, payload)
        writer = self.writers[query_id]
        if query_id == MsgType.Q1_RESULT:
            url, date_modified = fields
            writer.write({"url": url, "date_modified": date_modified})
        elif query_id == MsgType.Q2_RESULT:
            name, url, image = fields
            location = self._store_image(query_id, payload, image)
            writer.write({
                "name": name,
                "url": url,
                "image": location,
                "image_size": len(image),
            })
        elif query_id == MsgType.Q3_RESULT:
            word, = fields
            writer.write({"word": word})
        elif query_id == MsgType.Q4_RESULT:
            top_list, = fields
            for word, amount in top_list:
                writer.write({"word": word, "amount": amount})
        else:
            url, amount = fields
            writer.write({"url": url, "amount": amount})

    def _store_image(self, query_id, payload, image):
        if not image:
            return None
        if self.saved_images >= self.image_limit:
            self.remaining_images.write(
                external.serialize_msg(external.MsgType.RESULT, query_id, payload)
            )
            return Q2_REMAINING_FILE
        self.saved_images += 1
        file_name = f"img_{self.saved_images:04d}{image_extension(image)}"
        with open(os.path.join(self.q2_dir, file_name), "wb") as image_file:
            image_file.write(image)
        return file_name
