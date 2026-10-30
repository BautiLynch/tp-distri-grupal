import os
import logging
import signal
import threading
import urllib.request

from common import middleware
from common.message_protocol.internal import deserialize_msg, MsgType, serialize_msg

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
IMAGE_GETTER_PREFIX = os.environ["IMAGE_GETTER_PREFIX"]
FILTER_YEAR_PLUS_PREFIX = os.environ["FILTER_YEAR_PLUS_PREFIX"]


class ImageGetter:
    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, f"{FILTER_YEAR_PLUS_PREFIX}_image", [f"{FILTER_YEAR_PLUS_PREFIX}_image_{ID}"]
        )
        self.output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, IMAGE_GETTER_PREFIX
        )

        self.end_event = threading.Event()
        signal.signal(signal.SIGTERM, self.sigterm_handler)

    def _get_image(self, client_id, name, url, image_url):
        with urllib.request.urlopen(image_url) as response: # TODO: Reintentos
            self.output_exchange.send_to(serialize_msg(MsgType.Q2_RESULT, client_id, name, url, response.read()), routing_key=client_id)

    def process_message(self, message, ack, nack):
        logging.info("Process message")

        message_type, client_id, fields = deserialize_msg(message)
        if message_type == MsgType.GET_IMAGE:
            name, url, image_url = fields
            self._get_image(client_id, name, url, image_url)

        elif message_type == MsgType.END_OF_RECORDS:
            logging.info("Received EOF")
            self.output_exchange.send_to(serialize_msg(MsgType.END_OF_RECORDS, client_id, MsgType.Q2_RESULT), routing_key=client_id)

        else:
            nack()
            return
        ack()

    def close(self):
        for mom in [self.input_exchange, self.output_queue]:
            try:
                mom.close()
            except Exception as e:
                logging.error(f"Error closing message middleware: {e}")

    def start(self):
        try:
            self.input_exchange.start_consuming(self.process_message)
        finally:
            self.close()

    def sigterm_handler(self, signum, frame):
        if self.end_event.is_set():
            return
        self.end_event.set()
        self.input_exchange.stop_consuming_threadsafe()

def main():
    logging.basicConfig(level=logging.INFO)
    image_getter = ImageGetter()
    image_getter.start()
    return 0


if __name__ == "__main__":
    main()
