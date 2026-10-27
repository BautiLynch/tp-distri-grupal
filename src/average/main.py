import heapq
import os
import logging
import signal
import threading

from common import middleware, message_protocol, storage

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
AVERAGE_PREFIX = os.environ["AVERAGE_PREFIX"]
STORAGE_PATH = os.environ.get("STORAGE_PATH", "storage")


class Average:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AVERAGE_PREFIX, [f"{AVERAGE_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ( # TODO: Creo que deberia ser un exchange
            MOM_HOST, OUTPUT_QUEUE
        )

        self.storage = storage.Storage(STORAGE_PATH)
        self.end_event = threading.Event()
        signal.signal(signal.SIGTERM, self.sigterm_handler)

    def _process_eof(self, client_id):
        logging.info("Received EOF")

        urls = {}
        for key in self.storage.load_key(client_id):
            urls[key.key] = urls.get(key.key, 0) + key.amount

        message = [client_id, urls]
        self.output_queue.send(message_protocol.internal.serialize(message))
        

    def process_message(self, message, ack, nack):
        logging.info("Process message")
        fields = message_protocol.internal.deserialize(message) # TODO: Validar protocolo
        if len(fields) == 3:
            self.storage.store_key(*fields)
        elif len(fields) == 1:
            self._process_eof(*fields)
        else:
            nack()
            return
        ack()

    def sigterm_handler(self, signum, frame):
        if self.end_event.is_set():
            return
        self.end_event.set()
        self.input_exchange.stop_consuming_threadsafe()

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

def main():
    logging.basicConfig(level=logging.INFO)
    average = Average()
    average.start()
    return 0


if __name__ == "__main__":
    main()