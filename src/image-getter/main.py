import os
import logging
import signal
import threading
import urllib.request

from common import middleware, message_protocol

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
IMAGE_PREFIX = os.environ["IMAGE_PREFIX"]

class ImageGetter:
    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, IMAGE_PREFIX, [f"{IMAGE_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

        self.end_event = threading.Event()
        signal.signal(signal.SIGTERM, self.sigterm_handler)

    def _get_image(self, client_id, title, url, image_url):
        with urllib.request.urlopen(image_url) as response:
            message = [client_id, title, url, response.read()]
            self.output_queue.send(message_protocol.internal.serialize(message))

    def process_message(self, message, ack, nack):
        logging.info("Process message")
        fields = message_protocol.internal.deserialize(message) # TODO: Validar protocolo
        if len(fields) == 4:
            self._get_image(*fields)
        elif len(fields) == 1:
            logging.info("Received EOF")
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
