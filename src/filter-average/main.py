import os
import logging
import signal
import threading

from common import middleware
from common.message_protocol.internal import deserialize_msg, serialize_msg, MsgType

ID = os.environ["ID"]
MOM_HOST = os.environ["MOM_HOST"]
INPUT_EXCHANGE = os.environ["INPUT_EXCHANGE"]
OUTPUT_EXCHANGE = os.environ["OUTPUT_EXCHANGE"]

AVERAGE_AMOUNT = int(os.environ["AVERAGE_AMOUNT"])
FILTER_YEAR_AMOUNT = int(os.environ["FILTER_YEAR_AMOUNT"])
STORAGE_PATH = os.environ.get("STORAGE_PATH", "storage")
YEAR = os.environ.get("YEAR")


class FilterAverage:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, INPUT_EXCHANGE, [ID]
        )
        self.output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ( 
            MOM_HOST, OUTPUT_EXCHANGE, []
        )

        self.eof_by_client = {}
        self.words_by_client = {}
        self.average_by_client = {}
        self.pending_messages_by_client = {}

        self.end_event = threading.Event()
        signal.signal(signal.SIGTERM, self.sigterm_handler)

    def _process_eof(self, client_id):
        logging.info("Received EOF")

        self.eof_by_client[client_id] = self.eof_by_client.get(client_id, 0) + 1
        if self.eof_by_client[client_id] == FILTER_YEAR_AMOUNT: 
            amount_urls, amount_words = self.words_by_client.get(client_id, (0, 0))
            self.average_by_client[client_id] = amount_words / amount_urls if amount_urls > 0 else 0

            for url, amount_words in self.pending_messages_by_client.get(client_id, []):
                self._process_abstract(client_id, url, amount_words)

            self.pending_messages_by_client[client_id] = []

        elif self.eof_by_client[client_id] == AVERAGE_AMOUNT + FILTER_YEAR_AMOUNT:
            self.output_exchange.send_to(serialize_msg(MsgType.QUERY_END, client_id, MsgType.Q5_RESULT), routing_key=client_id) 

    def _process_abstract(self, client_id, url, amount_words):
        logging.info("Received abstract")

        if self.average_by_client.get(client_id) is None:
            logging.warning(f"Average for client {client_id} not calculated yet")

            self.pending_messages_by_client.setdefault(client_id, []).append((url, amount_words))
            return
        
        if amount_words > self.average_by_client[client_id]:
            self.output_exchange.send_to(serialize_msg(MsgType.Q5_RESULT, client_id, url, amount_words), routing_key=client_id)

    def process_message(self, message, ack, nack):
        logging.info("Process message")
        message_type, client_id, fields = deserialize_msg(message)
        if message_type == MsgType.WORD_YEAR_AMOUNT:
            url, date_modified, amount = fields

            if date_modified >= YEAR: # TODO: validar que este bien el >=
                url_count, word_count = self.words_by_client.get(client_id, (0, 0))
                self.words_by_client[client_id] = (url_count + 1, word_count + amount)
            else:
                self._process_abstract(client_id, url, amount)

        if message_type == MsgType.END_OF_RECORDS:
            self._process_eof(client_id)

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