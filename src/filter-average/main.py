from datetime import datetime
import os
import logging

from common import middleware
from common.shutdown import GracefulShutdown
from common.message_protocol.internal import deserialize_msg, serialize_msg, MsgType

ID = os.environ["ID"]
MOM_HOST = os.environ["MOM_HOST"]
INPUT_EXCHANGE = os.environ["INPUT_EXCHANGE"]
OUTPUT_EXCHANGE = os.environ["OUTPUT_EXCHANGE"]

AVERAGE_AMOUNT = int(os.environ["AVERAGE_AMOUNT"])
FILTER_YEAR_AMOUNT = int(os.environ["FILTER_YEAR_AMOUNT"])
STORAGE_PATH = os.environ.get("STORAGE_PATH", "storage")
YEAR = int(os.environ["YEAR"])


class FilterAverage:

    def __init__(self):
        self.shutdown = GracefulShutdown()
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, INPUT_EXCHANGE, [ID], consume=True
        )
        self.output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ( 
            MOM_HOST, OUTPUT_EXCHANGE, [], consume=False
        )

        self.eof_by_client = {}
        self.words_by_client = {}
        self.average_by_client = {}
        self.pending_messages_by_client = {}

    def _process_eof(self, client_id, message_type):
        eof = self.eof_by_client.setdefault(client_id, {"total": 0, "baseline": 0})
        eof["total"] += 1
        if message_type == MsgType.BASELINE_END:
            eof["baseline"] += 1

        if (message_type == MsgType.BASELINE_END and eof["baseline"] == FILTER_YEAR_AMOUNT):
            words_by_url = self.words_by_client.get(client_id, {})
            amount_urls = len(words_by_url)
            amount_words = sum(words_by_url.values())
            self.average_by_client[client_id] = amount_words / amount_urls if amount_urls > 0 else 0

            logging.info(f"Average for client {client_id} calculated: {self.average_by_client[client_id]}")
            logging.info(f"Processing pending messages for client {client_id}: length {len(self.pending_messages_by_client.get(client_id, []))}")
            for url, amount_words in self.pending_messages_by_client.get(client_id, []):
                self._process_abstract(client_id, url, amount_words)

        if eof["total"] == FILTER_YEAR_AMOUNT + AVERAGE_AMOUNT:
            self.output_exchange.send_to(serialize_msg(MsgType.QUERY_END, client_id, MsgType.Q5_RESULT), routing_key=client_id)
            self._release_client(client_id)

    def _release_client(self, client_id):
        self.eof_by_client.pop(client_id, None)
        self.words_by_client.pop(client_id, None)
        self.pending_messages_by_client.pop(client_id, None)
        self.average_by_client.pop(client_id, None)

    def _process_abstract(self, client_id, url, amount_words):
        if self.average_by_client.get(client_id) is None:
            self.pending_messages_by_client.setdefault(client_id, []).append((url, amount_words))
            return
        
        if amount_words > self.average_by_client[client_id]:
            self.output_exchange.send_to(serialize_msg(MsgType.Q5_RESULT, client_id, url, amount_words), routing_key=client_id)

    def process_message(self, message, ack, nack):
        message_type, client_id, fields = deserialize_msg(message)
        if message_type == MsgType.WORD_YEAR_AMOUNT:
            url, date_modified, amount = fields

            if datetime.fromisoformat(date_modified.replace("Z", "+00:00")).year <= YEAR:
                words_by_url = self.words_by_client.setdefault(client_id, {})
                words_by_url[url] = words_by_url.get(url, 0) + amount
            else:
                self._process_abstract(client_id, url, amount)

        elif message_type in (MsgType.BASELINE_END, MsgType.END_OF_RECORDS):
            self._process_eof(client_id, message_type)

        else:
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
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    filter_average = FilterAverage()
    filter_average.start()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
