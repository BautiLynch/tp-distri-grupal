import os
import logging

from common import middleware, hashing
from common.shutdown import GracefulShutdown
from common.message_protocol.internal import MsgType, serialize_msg, deserialize_msg

ID = os.environ["ID"]
MOM_HOST = os.environ["MOM_HOST"]
INPUT_EXCHANGE = os.environ["INPUT_EXCHANGE"]
OUTPUT_EXCHANGE = os.environ["OUTPUT_EXCHANGE"]

STOPWORDS_PATH = os.environ.get("STOPWORDS_PATH", "stopwords")
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
FILTER_YEAR_AMOUNT = int(os.environ["FILTER_YEAR_AMOUNT"])
FILTER_LANGUAGE_AMOUNT = int(os.environ["FILTER_LANGUAGE_AMOUNT"])
if not (AGGREGATION_AMOUNT == FILTER_YEAR_AMOUNT == FILTER_LANGUAGE_AMOUNT >= 1):
    raise ValueError("aggregation, filter_year y filter_language deben tener la misma cantidad de replicas")


class Splitter:
    def __init__(self):
        self.shutdown = GracefulShutdown()
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, INPUT_EXCHANGE, [ID], consume=True
        )
        self.output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, OUTPUT_EXCHANGE, [f"{index}" for index in range(FILTER_LANGUAGE_AMOUNT)], consume=False
        )

        self.en_stopwords = self._load_stopwords("english")
        self.fr_stopwords = self._load_stopwords("french")

        self.word_count = {}

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
            if self.shutdown.requested.is_set():
                break

            key = hashing.get_hash_key(f"{client_id}_{word}", FILTER_LANGUAGE_AMOUNT)

            self.output_exchange.send_to(
                serialize_msg(MsgType.SPLIT_WORD, client_id,
                              word, language, url, date_modified, amount),
                routing_key=key,
            )

        self.word_count.clear()

    def _is_stopword(self, word, language):
        if language == "en":
            return word in self.en_stopwords
        elif language == "fr":
            return word in self.fr_stopwords
        else:
            return False

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
        self.shutdown.run(self.input_exchange, self.process_message, self.close)

def main():
    logging.basicConfig(level=logging.INFO)
    splitter = Splitter()
    splitter.start()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
