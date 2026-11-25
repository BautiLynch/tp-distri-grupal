import os
import logging
import sys

from common import middleware, hashing
from common.shutdown import GracefulShutdown
from common.storage.message_publisher import MessagePublisher
from common.message_protocol.internal import MsgType, deserialize_msg

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
        self.shutdown = GracefulShutdown()
        self.last_received = {}
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, INPUT_EXCHANGE, [ID], consume=True
        )
        self.output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, OUTPUT_EXCHANGE,
            [f"words_{index}" for index in range(FILTER_LANGUAGE_AMOUNT)]
            + [f"urls_{index}" for index in range(FILTER_YEAR_AMOUNT)], consume=False
        )

        self.publisher = MessagePublisher(
            self.output_exchange, f"storage/splitter_{ID}.wal", get_routing_key=self._routing_key
        )
        self.stopwords = self._load_stopwords("english") | self._load_stopwords("french")

    def _routing_key(self, message_type, client_id, fields):
        if message_type == MsgType.WORD_BATCH:
            word = fields[1][0]
            key = hashing.get_hash_key(f"{client_id}_{word}", FILTER_LANGUAGE_AMOUNT)
            return f"words_{key}"
        if message_type == MsgType.WORD_YEAR_AMOUNT:
            key = hashing.get_hash_key(f"{client_id}_{fields[0]}", FILTER_YEAR_AMOUNT)
            return f"urls_{key}"
        return None

    def _load_stopwords(self, language):
        file_path = os.path.join(STOPWORDS_PATH, f"{language}.txt")
        with open(file_path, "r", encoding="utf-8") as file:
            stopwords_list = [line.strip() for line in file]

        return set(stopwords_list)

    def _split_and_send(self, client_id, message_id, article, source):
        url, language, date_modified, abstract = article
        text = "".join(char if char.isalnum() or char == "_" else " "
                       for char in abstract.lower())
        words = text.split()
        word_count = {}

        for word in words:
            if word in self.stopwords or word.isdecimal():
                continue
            word_count[word] = word_count.get(word, 0) + 1

        batches = {}
        for word in word_count:
            if self.shutdown.requested.is_set():
                break
            key = hashing.get_hash_key(f"{client_id}_{word}", FILTER_LANGUAGE_AMOUNT)
            batches.setdefault(key, []).append(word)

        for key, batch in batches.items():
            if self.shutdown.requested.is_set():
                break
            self.publisher.send(MsgType.WORD_BATCH, client_id, message_id, (message_id, MsgType.WORD_BATCH, key), source, language, batch)
        if word_count:
            self.publisher.send(
                MsgType.WORD_YEAR_AMOUNT, client_id, message_id, (message_id, MsgType.WORD_YEAR_AMOUNT), source, url, date_modified, sum(word_count.values())
            )

    def process_message(self, message, ack, nack):
        try:
            message_type, client_id, message_id, producer_id, key, fields = deserialize_msg(message)
            producer = (client_id, producer_id)
            if self.last_received.get(producer) == key:
                ack()
                return
            if message_type == MsgType.SPLITTER_ARTICLE:
                self._split_and_send(client_id, message_id, fields, source=(producer_id, key))

            elif message_type == MsgType.END_OF_RECORDS:
                self.publisher.send(message_type, client_id, message_id, (message_id, message_type), (producer_id, key), f"splitter_{ID}")

            else:
                raise ValueError(f"Unexpected splitter message type: {message_type}")
            
        except Exception:
            logging.exception("Could not process splitter input")
            nack()
            raise
        self.last_received[producer] = key
        ack()

    def close(self):
        for mom in [self.input_exchange, self.output_exchange, self.publisher]:
            try:
                mom.close()
            except Exception as e:
                logging.error(f"Error closing message middleware: {e}")

    def start(self):
        try:
            self.publisher.recover()
        except Exception:
            self.close()
            raise
        self.shutdown.run(self.input_exchange, self.process_message, self.close)

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
    sys.exit(main())
