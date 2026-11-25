from datetime import datetime
import os
import logging
import sys

from common import middleware, storage, hashing
from common.shutdown import GracefulShutdown
from common.storage.message_publisher import MessagePublisher
from common.storage.eof_counter import EofCounter
from common.message_protocol.internal import deserialize_msg, MsgType

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
        self.last_received = {}
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, INPUT_EXCHANGE, [ID], consume=True
        )
        self.output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, OUTPUT_EXCHANGE, [f"{index}" for index in range(FILTER_AVERAGE_AMOUNT)], consume=False
        )

        self.urls_by_client = {}
        self.eofs = EofCounter(f"{STORAGE_PATH}/average_{ID}_eofs", FILTER_YEAR_AMOUNT)
        self.storage = storage.Storage(STORAGE_PATH)
        self.results = MessagePublisher(
            self.output_exchange, f"{STORAGE_PATH}/average_{ID}.wal",
            get_routing_key=self._routing_key,
        )

    def _routing_key(self, message_type, client_id, fields):
        if message_type == MsgType.WORD_YEAR_AMOUNT:
            return hashing.get_hash_key(f"{client_id}_{fields[0]}", FILTER_AVERAGE_AMOUNT)
        return None

    def _process_eof(self, client_id, message_id, sender_id):
        count = self.eofs.save(client_id, (message_id, sender_id))
        if count != FILTER_YEAR_AMOUNT:
            return

        date = datetime(YEAR, 1, 1)
        formatted_date = date.isoformat()
        
        for url, amount in self.urls_by_client.get(client_id, {}).items():
            self.results.send(MsgType.WORD_YEAR_AMOUNT, client_id, message_id, (url, MsgType.WORD_YEAR_AMOUNT), url, formatted_date, amount)
        
        self.results.send(MsgType.END_OF_RECORDS, client_id, message_id, (message_id, MsgType.END_OF_RECORDS), f"average_{ID}")
        self._release_client(client_id)

    def _release_client(self, client_id):
        self.eofs.clear(client_id)
        self.urls_by_client.pop(client_id, None)

    def process_message(self, message, ack, nack):
        message_type, client_id, message_id, producer_id, key, fields = deserialize_msg(message)
        producer = (client_id, producer_id)
        if self.last_received.get(producer) == key:
            ack()
            return
        if message_type == MsgType.WORD_YEAR_AMOUNT:
            url, date_modified, amount = fields
            urls = self.urls_by_client.setdefault(client_id, {})
            urls[url] = urls.get(url, 0) + amount

        elif message_type == MsgType.END_OF_RECORDS:
            self._process_eof(client_id, message_id, fields[0])
        else:
            nack()
            return
        self.last_received[producer] = key
        ack()

    def close(self):
        for mom in [self.input_exchange, self.output_exchange, self.results, self.eofs]:
            try:
                mom.close()
            except Exception as e:
                logging.error(f"Error closing message middleware: {e}")

    def start(self):
        try:
            self.eofs.recover()
            self.results.recover()
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
    average = Average()
    average.start()
    return 0


if __name__ == "__main__":
    sys.exit(main())
