from datetime import datetime
import os
import logging

from common import middleware, storage, hashing
from common.shutdown import GracefulShutdown
from common.message_protocol.internal import deserialize_msg, serialize_msg, MsgType

ID = os.environ["ID"]
MOM_HOST = os.environ["MOM_HOST"]
AVERAGE_PREFIX = os.environ["AVERAGE_PREFIX"]
INPUT_EXCHANGE = os.environ["INPUT_EXCHANGE"]
OUTPUT_EXCHANGE = os.environ["OUTPUT_EXCHANGE"]
FILTER_AVERAGE_AMOUNT = int(os.environ["FILTER_AVERAGE_AMOUNT"])
FILTER_YEAR_AMOUNT = int(os.environ["FILTER_YEAR_AMOUNT"])
YEAR = int(os.environ.get("YEAR"))

STORAGE_PATH = os.environ.get("STORAGE_PATH", "storage")


class Average:

    def __init__(self):
        self.shutdown = GracefulShutdown()
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, INPUT_EXCHANGE, [ID], consume=True
        )
        self.output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, OUTPUT_EXCHANGE, [f"{index}" for index in range(FILTER_AVERAGE_AMOUNT)], consume=False
        )

        self.urls_by_client = {}
        self.eof_by_client = {}
        self.storage = storage.Storage(STORAGE_PATH)

    def _process_eof(self, client_id):
        self.eof_by_client[client_id] = self.eof_by_client.get(client_id, 0) + 1
        if self.eof_by_client[client_id] != FILTER_YEAR_AMOUNT:
            return 

        date = datetime(YEAR, 1, 1)
        formatted_date = date.isoformat()
        
        for url, amount in self.urls_by_client.get(client_id, {}).items():
            key = hashing.get_hash_key(f"{client_id}_{url}", FILTER_AVERAGE_AMOUNT)
            self.output_exchange.send_to(
                serialize_msg(MsgType.WORD_YEAR_AMOUNT, client_id, url, formatted_date, amount), routing_key=key
            )
        
        self.output_exchange.send(serialize_msg(MsgType.END_OF_RECORDS, client_id))
        self._release_client(client_id)

    def _release_client(self, client_id):
        self.eof_by_client.pop(client_id, None)
        self.urls_by_client.pop(client_id, None)

    def process_message(self, message, ack, nack):
        message_type, client_id, fields = deserialize_msg(message)
        if message_type == MsgType.WORD_YEAR_AMOUNT:
            url, date_modified, amount = fields
            urls = self.urls_by_client.setdefault(client_id, {})
            urls[url] = urls.get(url, 0) + amount

        elif message_type == MsgType.END_OF_RECORDS:
            self._process_eof(client_id)
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
    logging.basicConfig(level=logging.INFO)
    average = Average()
    average.start()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
