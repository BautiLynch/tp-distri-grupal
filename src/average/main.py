import os
import logging
import signal
import threading

from common import middleware, storage, hashing
from common.message_protocol.internal import deserialize_msg, serialize_msg, MsgType

ID = os.environ["ID"]
MOM_HOST = os.environ["MOM_HOST"]
AVERAGE_PREFIX = os.environ["AVERAGE_PREFIX"]
INPUT_EXCHANGE = os.environ["INPUT_EXCHANGE"]
OUTPUT_EXCHANGE = os.environ["OUTPUT_EXCHANGE"]
FILTER_AVERAGE_AMOUNT = int(os.environ["FILTER_AVERAGE_AMOUNT"])
YEAR = int(os.environ.get("YEAR"))

STORAGE_PATH = os.environ.get("STORAGE_PATH", "storage")


class Average:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, INPUT_EXCHANGE, [ID]
        )
        self.output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, OUTPUT_EXCHANGE, [f"{index}" for index in range(FILTER_AVERAGE_AMOUNT)]
        )

        self.urls_by_client = {}
        self.storage = storage.Storage(STORAGE_PATH)
        self.end_event = threading.Event()
        signal.signal(signal.SIGTERM, self.sigterm_handler)

    def _process_eof(self, client_id):
        logging.info("Received EOF")

        # urls = {}
        # for key in self.storage.load_key(client_id):
        #     urls[key.key] = urls.get(key.key, 0) + key.amount

        for url, amount in self.urls_by_client.get(client_id, {}).items():
            key = hashing.get_hash_key(f"{client_id}_{url}", FILTER_AVERAGE_AMOUNT)
            self.output_exchange.send_to(serialize_msg(MsgType.WORD_YEAR_AMOUNT, client_id, url, YEAR, amount), routing_key=key)

        self.output_exchange.send(serialize_msg(MsgType.END_OF_RECORDS, client_id))
        self.urls_by_client[client_id] = {}
        

    def process_message(self, message, ack, nack):
        logging.info("Process message")
        message_type, client_id, fields = deserialize_msg(message)
        if message_type == MsgType.WORD_YEAR_AMOUNT:
            url, date_modified, amount = fields
            urls = self.urls_by_client.setdefault(client_id, {})
            urls[url] = urls.get(url, 0) + amount
            #self.storage.store_key(*fields)

        elif message_type == MsgType.END_OF_RECORDS:
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
        for mom in [self.input_exchange, self.output_exchange]:
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