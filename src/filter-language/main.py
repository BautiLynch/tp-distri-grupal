import os
import logging
import signal
import threading

from common import middleware
from common.message_protocol.internal import deserialize_msg, serialize_msg, MsgType

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_EXCHANGE = os.environ["INPUT_EXCHANGE"]
OUTPUT_EXCHANGE = os.environ["OUTPUT_EXCHANGE"]
STORAGE_PATH = os.environ.get("STORAGE_PATH", "storage")
YEAR = os.environ.get("YEAR")
SPLITTER_AMOUNT = int(os.environ["SPLITTER_AMOUNT"])


class FilterLanguage:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, INPUT_EXCHANGE, [f"{INPUT_EXCHANGE}_{ID}"]
        )
        self.output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ( 
            MOM_HOST, OUTPUT_EXCHANGE
        )

        self.eof_by_client = {}

        self.words_by_client = {}
        self.end_event = threading.Event()
        signal.signal(signal.SIGTERM, self.sigterm_handler)

    def process_message(self, message, ack, nack):
        logging.info("Process message")

        message_type, client_id, fields = deserialize_msg(message)
        if message_type == MsgType.WORD_LANGUAGE:
            word, language = fields

            word_sent = self.words_by_client.get(client_id, {})
            language_sent = word_sent.get(word, set())

            if len(language_sent) == 2:
                pass # Ya la mandé

            elif language not in language_sent:
                language_sent.add(language)
                word_sent[word] = language_sent
                self.words_by_client[client_id] = word_sent

                if len(language_sent) == 2:
                    logging.info("Received word in both languages")
                    self.output_exchange.send_to(serialize_msg(MsgType.Q3_RESULT, client_id, word), routing_key=client_id) 

        if message_type == MsgType.END_OF_RECORDS:
            logging.info("Received EOF")

            self.eof_by_client[client_id] = self.eof_by_client.get(client_id, 0) + 1
            if self.eof_by_client[client_id] == SPLITTER_AMOUNT:
                self.output_exchange.send_to(serialize_msg(MsgType.END_OF_RECORDS, client_id, MsgType.Q3_RESULT), routing_key=client_id) 

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
    filter_average = FilterLanguage()
    filter_average.start()
    return 0


if __name__ == "__main__":
    main()