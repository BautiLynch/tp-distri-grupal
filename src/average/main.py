import os
import logging
import signal
import threading

from common import middleware, message_protocol, storage, hashing

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
AVERAGE_PREFIX = os.environ["AVERAGE_PREFIX"]
FILTER_AVERAGE_PREFIX = os.environ["FILTER_AVERAGE_PREFIX"]
FILTER_AVERAGE_AMOUNT = int(os.environ["FILTER_AVERAGE_AMOUNT"])
YEAR = int(os.environ.get("YEAR"))

STORAGE_PATH = os.environ.get("STORAGE_PATH", "storage")


class Average:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AVERAGE_PREFIX, [f"{AVERAGE_PREFIX}_{ID}"]
        )
        self.output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, FILTER_AVERAGE_PREFIX
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

        for word, amount in self.urls_by_client.get(client_id, {}).items():
            key = hashing.get_hash_key(f"{client_id}_{word}", FILTER_AVERAGE_AMOUNT)
            self.output_exchange.send_to(message_protocol.internal.serialize([client_id, word, amount, YEAR]), routing_key=key)

        self.output_exchange.send(message_protocol.internal.serialize([client_id])) # TODO: Mandar el EOF
        self.urls_by_client[client_id] = {}
        

    def process_message(self, message, ack, nack):
        logging.info("Process message")
        fields = message_protocol.internal.deserialize(message) # TODO: Validar protocolo
        if len(fields) == 3:
            client_id, key, amount = fields
            self.urls_by_client[client_id] = self.urls_by_client.get(client_id, {})
            self.urls_by_client[client_id][key] = self.urls_by_client[client_id].get(key, 0) + amount
            #self.storage.store_key(*fields)
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