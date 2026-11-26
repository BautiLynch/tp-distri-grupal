from datetime import datetime
import os
import socket
import logging
import sys

from common import middleware
from common.heartbeat import HeartbeatSender
from common.shutdown import GracefulShutdown
from common.storage.message_publisher import MessagePublisher
from common.storage.eof_counter import EofCounter
from common.storage.state_log import StateLog
from common.message_protocol.internal import deserialize_msg, MsgType
from common.message_protocol.queries import QueryId

ID = os.environ["ID"]
MOM_HOST = os.environ["MOM_HOST"]
HEARTBEAT_QUEUE = os.environ["HEARTBEAT_QUEUE"]
HEARTBEAT_INTERVAL = int(os.environ.get("HEARTBEAT_INTERVAL", 5))
INPUT_EXCHANGE = os.environ["INPUT_EXCHANGE"]
OUTPUT_EXCHANGE = os.environ["OUTPUT_EXCHANGE"]

AVERAGE_AMOUNT = int(os.environ["AVERAGE_AMOUNT"])
FILTER_YEAR_AMOUNT = int(os.environ["FILTER_YEAR_AMOUNT"])
STORAGE_PATH = os.environ.get("STORAGE_PATH", "storage")
YEAR = int(os.environ["YEAR"])


class FilterAverage:

    def __init__(self):
        self.heartbeat = HeartbeatSender(
            MOM_HOST, HEARTBEAT_QUEUE, socket.gethostname(), f"filter_average_{ID}", HEARTBEAT_INTERVAL
        )
        self.shutdown = GracefulShutdown()
        self.last_received = {}
        self.state = StateLog(f"{STORAGE_PATH}/filter_average_{ID}_state")
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, INPUT_EXCHANGE, [ID], consume=True, queue_name=f"filter_average_{ID}"
        )
        self.output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ( 
            MOM_HOST, OUTPUT_EXCHANGE, [], consume=False
        )

        self.eofs = EofCounter(
            f"{STORAGE_PATH}/filter_average_{ID}_eofs", FILTER_YEAR_AMOUNT + AVERAGE_AMOUNT
        )
        self.baseline_eofs = EofCounter(
            f"{STORAGE_PATH}/filter_average_{ID}_baseline_eofs", FILTER_YEAR_AMOUNT
        )
        self.results = MessagePublisher(self.output_exchange, f"{STORAGE_PATH}/filter_average_{ID}.wal")
        self.words_by_client = {}
        self.average_by_client = {}
        self.pending_messages_by_client = {}

    def _process_eof(self, client_id, message_id, message_type, sender_id, source):
        total = self.eofs.save(client_id, (message_id, sender_id))
        if message_type == MsgType.BASELINE_END:
            baseline = self.baseline_eofs.save(client_id, (message_id, sender_id))

        if message_type == MsgType.BASELINE_END and baseline == FILTER_YEAR_AMOUNT:
            words_by_url = self.words_by_client.get(client_id, {})
            amount_urls = len(words_by_url)
            amount_words = sum(words_by_url.values())
            self.average_by_client[client_id] = amount_words / amount_urls if amount_urls > 0 else 0

            logging.info(f"Average for client {client_id} calculated: {self.average_by_client[client_id]}")
            logging.info(f"Processing pending messages for client {client_id}: length {len(self.pending_messages_by_client.get(client_id, []))}")
            for url, amount_words in self.pending_messages_by_client.get(client_id, []):
                self._process_abstract(client_id, message_id, url, amount_words, source=source)

        if total == FILTER_YEAR_AMOUNT + AVERAGE_AMOUNT:
            self.results.send(MsgType.QUERY_END, client_id, message_id, (message_id, MsgType.QUERY_END), source, QueryId.Q5)
            self._release_client(client_id)

    def _release_client(self, client_id):
        self.state.clear(client_id)
        self.eofs.clear(client_id)
        self.baseline_eofs.clear(client_id)
        self.words_by_client.pop(client_id, None)
        self.pending_messages_by_client.pop(client_id, None)
        self.average_by_client.pop(client_id, None)

    def _process_abstract(self, client_id, message_id, url, amount_words, source):
        if self.average_by_client.get(client_id) is None:
            self.pending_messages_by_client.setdefault(client_id, []).append((url, amount_words))
            return
        
        if amount_words > self.average_by_client[client_id]:
            self.results.send(MsgType.RESULT, client_id, message_id, (url, MsgType.RESULT), source, QueryId.Q5, url, amount_words)

    def process_message(self, message, ack, nack):
        message_type, fields = deserialize_msg(message)
        client_id, message_id, producer_id, key, *fields = fields
        producer = (client_id, producer_id)
        if self.last_received.get(producer) == key:
            ack()
            return
        if message_type == MsgType.WORD_YEAR_AMOUNT:
            url, date_modified, amount = fields
            self.state.save(client_id, message, int(client_id in self.average_by_client))

            if datetime.fromisoformat(date_modified.replace("Z", "+00:00")).year <= YEAR:
                words_by_url = self.words_by_client.setdefault(client_id, {})
                words_by_url[url] = words_by_url.get(url, 0) + amount
            else:
                self._process_abstract(client_id, message_id, url, amount, source=(producer_id, key))

        elif message_type in (MsgType.BASELINE_END, MsgType.END_OF_RECORDS):
            self._process_eof(client_id, message_id, message_type, fields[0], source=(producer_id, key))

        else:
            nack()
            return
        if message_type == MsgType.WORD_YEAR_AMOUNT:
            self.state.mark_processed(client_id)
        self.last_received[producer] = key
        ack()

    def _recover_state(self, record):
        message, context = record
        _, fields = deserialize_msg(message)
        client_id, message_id, producer_id, key, url, date_modified, amount = fields
        # El primer dato posterior al promedio fija su valor en ese punto.
        if context and client_id not in self.average_by_client:
            words_by_url = self.words_by_client.get(client_id, {})
            self.average_by_client[client_id] = sum(words_by_url.values()) / len(words_by_url) if words_by_url else 0
        if datetime.fromisoformat(date_modified.replace("Z", "+00:00")).year <= YEAR:
            words_by_url = self.words_by_client.setdefault(client_id, {})
            words_by_url[url] = words_by_url.get(url, 0) + amount
        elif not context:
            self.pending_messages_by_client.setdefault(client_id, []).append((url, amount))
        self.last_received[(client_id, producer_id)] = key

    def close(self):
        self.heartbeat.stop()
        for mom in [self.input_exchange, self.output_exchange, self.results, self.eofs, self.baseline_eofs, self.state]:
            try:
                mom.close()
            except Exception as e:
                logging.error(f"Error closing message middleware: {e}")

    def start(self):
        try:
            self.eofs.recover()
            self.baseline_eofs.recover()
            self.results.recover()
            self.state.recover(self._recover_state)
            if self.results.last is not None:
                (client_id, source), sent = self.results.last
                if any(key[-1] == MsgType.QUERY_END for key in sent):
                    self._release_client(client_id)
                    self.last_received[(client_id, source[0])] = source[1]
            for client_id, count in self.baseline_eofs.counts.items():
                if count == FILTER_YEAR_AMOUNT and client_id not in self.average_by_client:
                    words_by_url = self.words_by_client.get(client_id, {})
                    self.average_by_client[client_id] = sum(words_by_url.values()) / len(words_by_url) if words_by_url else 0
            self.heartbeat.start()
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
    filter_average = FilterAverage()
    filter_average.start()
    return 0


if __name__ == "__main__":
    sys.exit(main())
