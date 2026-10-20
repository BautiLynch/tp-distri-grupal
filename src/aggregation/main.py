import heapq
import os
import logging
import bisect
import signal
import threading

from common import middleware, message_protocol, word_item
from aggregation.store import Storage

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])
STORAGE_PATH = os.environ.get("STORAGE_PATH", "storage")


class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

        self.storage = Storage(STORAGE_PATH)
        self.end_event = threading.Event()
        signal.signal(signal.SIGTERM, self.sigterm_handler)

    def _process_eof(self, client):
        logging.info("Received EOF")

        top_words = {}
        for word in self.storage.load_words(client):
            top_words[word.word] += word.amount

        top_parcial = heapq.nlargest(
            TOP_SIZE,
            top_words.items(),
            key=lambda item: item[1],
        )

        message = [client, top_parcial]
        self.output_queue.send(message_protocol.internal.serialize(message))
        

    def process_message(self, message, ack, nack):
        logging.info("Process message")
        fields = message_protocol.internal.deserialize(message) # TODO: Validar protocolo
        if len(fields) == 3:
            self.storage.store_words(*fields)
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
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()