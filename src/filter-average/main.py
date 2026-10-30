import os
import logging
import signal
import threading

from common import middleware, message_protocol

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_EXCHAGE = os.environ["OUTPUT_EXCHAGE"]
FILTER_AVERAGE_PREFIX = os.environ["FILTER_AVERAGE_PREFIX"]
AVERAGE_AMOUNT = int(os.environ["AVERAGE_AMOUNT"])
STORAGE_PATH = os.environ.get("STORAGE_PATH", "storage")
YEAR = os.environ.get("YEAR")


class FilterAverage:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, FILTER_AVERAGE_PREFIX, [f"{FILTER_AVERAGE_PREFIX}_{ID}"]
        )
        self.output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ( 
            MOM_HOST, OUTPUT_EXCHAGE
        )

        self.eof_by_client = {}
        self.words_by_client = {}
        self.average_by_client = {}
        self.pending_messages_by_client = {}

        self.end_event = threading.Event()
        signal.signal(signal.SIGTERM, self.sigterm_handler)

    def _process_eof(self, client_id):
        logging.info("Received EOF")

        self.eof_by_client[client_id] = self.eof_by_client.get(client_id, 0) +1

        if self.eof_by_client[client_id] == 1:
            amount_urls, amount_words = self.words_by_client.get(client_id, (0, 0))
            self.average_by_client[client_id] = amount_words / amount_urls if amount_urls > 0 else 0

            for url, amount_words in self.pending_messages_by_client.get(client_id, []):
                self._process_abstract(client_id, url, amount_words)

        elif self.eof_by_client[client_id] == AVERAGE_AMOUNT + 1:
            self.output_exchange.send_to(message_protocol.internal.serialize([client_id,]), routing_key=client_id) # TODO: Mandar el EOF

    def _process_abstract(self, client_id, url, amount_words):
        logging.info("Received abstract")

        if self.average_by_client.get(client_id) is None:
            logging.warning(f"Average for client {client_id} not calculated yet")
            self.pending_messages_by_client[client_id] = self.pending_messages_by_client.get(client_id, [])
            self.pending_messages_by_client[client_id].append((url, amount_words))
            return
        
        if amount_words > self.average_by_client[client_id]:
            message = [client_id, url, amount_words]
            self.output_exchange.send_to(message_protocol.internal.serialize(message), routing_key=client_id)

    def process_message(self, message, ack, nack):
        logging.info("Process message")
        fields = message_protocol.internal.deserialize(message) # TODO: Validar protocolo
        if len(fields) == 1:
            self._process_eof(*fields)

        elif len(fields) == 4:
            client_id, key, year, amount = fields
            if year >= YEAR: # TODO: validar que este bien el >=
                self.words_by_client[client_id] = self.words_by_client.get(client_id, (0, 0))
                self.words_by_client[client_id] = (self.words_by_client[client_id][0] + 1, self.words_by_client[client_id][1] + amount)
            else:
                self._process_abstract(client_id, key, amount)

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
    filter_average = FilterAverage()
    filter_average.start()
    return 0


if __name__ == "__main__":
    main()