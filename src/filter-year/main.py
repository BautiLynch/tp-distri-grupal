import os
import logging
import signal
import threading

from common import middleware, message_protocol, hashing

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_EXCHAGE = os.environ["OUTPUT_EXCHAGE"]
FILTER_YEAR_PREFIX = os.environ["FILTER_YEAR_PREFIX"]
AVERAGE_AMOUNT = int(os.environ.get("AVERAGE_AMOUNT"))
STORAGE_PATH = os.environ.get("STORAGE_PATH", "storage")
YEAR = os.environ.get("YEAR")


class FilterYear:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, FILTER_YEAR_PREFIX, [f"{FILTER_YEAR_PREFIX}_{ID}"]
        )
        self.output_exchange_older = middleware.MessageMiddlewareExchangeRabbitMQ( 
            MOM_HOST, f"{OUTPUT_EXCHAGE}_older"
        )
        self.output_exchange_newer = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, f"{OUTPUT_EXCHAGE}_newer"
        )

        self.end_event = threading.Event()
        signal.signal(signal.SIGTERM, self.sigterm_handler)

    def _process_eof(self, client_id):
        logging.info("Received EOF")

        amount_urls, amount_words = self.words_by_client.get(client_id, (0, 0))
        self.average_by_client[client_id] = amount_words / amount_urls if amount_urls > 0 else 0


    def process_message(self, message, ack, nack):
        logging.info("Process message")
        fields = message_protocol.internal.deserialize(message) # TODO: Validar protocolo
        if len(fields) == 1:
            self.output_exchange_newer.send(message)
            self.output_exchange_older.send(message)

        elif len(fields) == 5:
            client_id, url, year, amount = fields
            if year >= YEAR: # TODO: validar que este bien el >=
                key = hashing.get_hash_key(f"{client_id}_{url}", AVERAGE_AMOUNT)
                self.output_exchange_newer.send_to(message, routing_key=key) # Este se lo manda solo al hash
            else:
                self.output_exchange_older.send(message) # Este se lo manda a todos

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
    filter_average = FilterYear()
    filter_average.start()
    return 0


if __name__ == "__main__":
    main()