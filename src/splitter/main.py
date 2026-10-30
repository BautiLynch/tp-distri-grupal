import os
import logging
import signal
import threading

from common import middleware, hashing
from common.message_protocol.internal import MsgType, serialize_msg, deserialize_msg

ID = os.environ["ID"]
MOM_HOST = os.environ["MOM_HOST"]
INPUT_EXCHANGE = os.environ["INPUT_EXCHANGE"]
EXCHANGE_QUERY_3 = os.environ["EXCHANGE_QUERY_3"]
EXCHANGE_QUERY_4 = os.environ["EXCHANGE_QUERY_4"]
EXCHANGE_QUERY_5 = os.environ["EXCHANGE_QUERY_5"]

STOPWORDS_PATH = os.environ.get("STOPWORDS_PATH", "stopwords")
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
FILTER_YEAR_AMOUNT = int(os.environ["FILTER_YEAR_AMOUNT"])
FILTER_LANGUAGE_AMOUNT = int(os.environ["FILTER_LANGUAGE_AMOUNT"])


class Splitter:
    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, INPUT_EXCHANGE, [ID]
        )
        self.output_exchange_3 = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, EXCHANGE_QUERY_3, [f"{index}" for index in range(FILTER_LANGUAGE_AMOUNT)]
        )
        self.output_exchange_4 = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, EXCHANGE_QUERY_4, [f"{index}" for index in range(AGGREGATION_AMOUNT)]
        )
        self.output_exchange_5 = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, EXCHANGE_QUERY_5, [f"{index}" for index in range(FILTER_YEAR_AMOUNT)]
        )

        self.en_stopwords = self._load_stopwords("english")
        self.fr_stopwords = self._load_stopwords("french")

        self.word_count = {}
        self.end_event = threading.Event()
        signal.signal(signal.SIGTERM, self.sigterm_handler)

    def _load_stopwords(self, language):
        file_path = os.path.join(STOPWORDS_PATH, f"{language}.txt")
        with open(file_path, "r", encoding="utf-8") as file:
            stopwords_list = [line.strip() for line in file.readlines()]

        return set(stopwords_list)

    def _split_and_send(self, client_id, article):
        url, language, date_modified, abstract = article
        words = abstract.split(" ")

        for word in words:
            if self._is_stopword(word, language):
                continue

            self.word_count[word] = self.word_count.get(word, 0) + 1

        for word, amount in self.word_count.items():
            key = hashing.get_hash_key(f"{client_id}_{word}", FILTER_LANGUAGE_AMOUNT)

            self.output_exchange_3.send_to(
                serialize_msg( # TODO: definir un orden fijo (class?)
                    MsgType.WORD_LANGUAGE,
                    client_id, 
                    word, language, 
                ), routing_key=key)

            key = hashing.get_hash_key(f"{client_id}_{word}", AGGREGATION_AMOUNT) 

            self.output_exchange_4.send_to(
                serialize_msg( # TODO: definir un orden fijo (class?)
                    MsgType.WORD_AMOUNT,
                    client_id, 
                    word, 
                ), routing_key=key)

            key = hashing.get_hash_key(f"{client_id}_{word}", FILTER_YEAR_AMOUNT) 
            
            self.output_exchange_5.send_to(
                serialize_msg( # TODO: definir un orden fijo (class?)
                    MsgType.WORD_YEAR_AMOUNT,
                    client_id, 
                    url, date_modified, amount 
                ), routing_key=key)

            # TODO: revisar como evitar repetir tanto codigo

    def _is_stopword(self, word, language):
        if language == "en":
            return word in self.en_stopwords
        elif language == "fr":
            return word in self.fr_stopwords
        else:
            return False

    def process_message(self, message, ack, nack):
        logging.info("Process message")
        try:
            message_type, client_id, fields = deserialize_msg(message)
            if message_type == MsgType.SPLITTER_ARTICLE:
                self._split_and_send(client_id, fields)

            elif message_type == MsgType.END_OF_RECORDS:
                logging.info("Received EOF")
                self.output_exchange_3.send(message)
                self.output_exchange_4.send(message)
                self.output_exchange_5.send(message) 

            else:
                raise ValueError(f"Unexpected splitter message type: {message_type}")
            
        except Exception:
            logging.exception("Could not process splitter input")
            nack()
            return
        ack()

    def close(self):
        for mom in [self.input_queue, self.output_queue]:
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
        self.input_queue.stop_consuming_threadsafe()

def main():
    logging.basicConfig(level=logging.INFO)
    splitter = Splitter()
    splitter.start()
    return 0


if __name__ == "__main__":
    main()
