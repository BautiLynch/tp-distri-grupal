import os
import logging
import re
import signal
import threading

from common import middleware, hashing
from common.message_protocol.internal import MsgType, serialize_msg, deserialize_msg

ID = os.environ["ID"]
MOM_HOST = os.environ["MOM_HOST"]
INPUT_EXCHANGE = os.environ["INPUT_EXCHANGE"]
OUTPUT_EXCHANGE = os.environ["OUTPUT_EXCHANGE"]

STOPWORDS_PATH = os.environ.get("STOPWORDS_PATH", "stopwords")
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
FILTER_YEAR_AMOUNT = int(os.environ["FILTER_YEAR_AMOUNT"])
FILTER_LANGUAGE_AMOUNT = int(os.environ["FILTER_LANGUAGE_AMOUNT"])
if not (AGGREGATION_AMOUNT == FILTER_LANGUAGE_AMOUNT >= 1) or FILTER_YEAR_AMOUNT < 1:
    raise ValueError("aggregation y filter_language deben tener la misma cantidad de replicas; filter_year debe tener al menos una")


class Splitter:
    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, INPUT_EXCHANGE, [ID], consume=True
        )
        self.output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, OUTPUT_EXCHANGE,
            [f"words.{index}" for index in range(FILTER_LANGUAGE_AMOUNT)]
            + [f"urls.{index}" for index in range(FILTER_YEAR_AMOUNT)], consume=False
        )

        self.stopwords = self._load_stopwords("english") | self._load_stopwords("french")

        self.end_event = threading.Event()
        signal.signal(signal.SIGTERM, self.sigterm_handler)

    def _load_stopwords(self, language):
        file_path = os.path.join(STOPWORDS_PATH, f"{language}.txt")
        with open(file_path, "r", encoding="utf-8") as file:
            stopwords_list = [line.strip() for line in file]

        return set(stopwords_list)

    def _split_and_send(self, client_id, article):
        url, language, date_modified, abstract = article
        words = re.findall(r"\w+", abstract.lower())
        word_count = {}

        for word in words:
            if word in self.stopwords or word.isdecimal():
                continue
            word_count[word] = word_count.get(word, 0) + 1

        batches = {}
        for word, amount in word_count.items():
            key = hashing.get_hash_key(f"{client_id}_{word}", FILTER_LANGUAGE_AMOUNT)
            batches.setdefault(key, []).append((word, amount))

        for key, batch in batches.items():
            self.output_exchange.send_to(
                serialize_msg(MsgType.WORD_BATCH, client_id, language, batch),
                routing_key=f"words.{key}",
            )

        # Excluir abstracts sin palabras validas conserva el denominador de Q5.
        if word_count:
            key = hashing.get_hash_key(f"{client_id}_{url}", FILTER_YEAR_AMOUNT)
            self.output_exchange.send_to(
                serialize_msg(MsgType.WORD_YEAR_AMOUNT, client_id,
                              url, date_modified, sum(word_count.values())),
                routing_key=f"urls.{key}",
            )

    def process_message(self, message, ack, nack):
        try:
            message_type, client_id, fields = deserialize_msg(message)
            if message_type == MsgType.SPLITTER_ARTICLE:
                self._split_and_send(client_id, fields)

            elif message_type == MsgType.END_OF_RECORDS:
                self.output_exchange.send(message)

            else:
                raise ValueError(f"Unexpected splitter message type: {message_type}")
            
        except Exception:
            logging.exception("Could not process splitter input")
            nack()
            return
        ack()

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

    def sigterm_handler(self, signum, frame):
        if self.end_event.is_set():
            return
        self.end_event.set()
        self.input_exchange.stop_consuming_threadsafe()

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    splitter = Splitter()
    splitter.start()
    return 0


if __name__ == "__main__":
    main()
