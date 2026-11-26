import heapq
import os
import socket
import logging
import sys

from common import middleware, storage
from common.heartbeat import HeartbeatSender
from common.shutdown import GracefulShutdown
from common.storage.message_publisher import MessagePublisher
from common.storage.eof_counter import EofCounter
from common.message_protocol.internal import deserialize_msg, MsgType

ID = os.environ["ID"]
MOM_HOST = os.environ["MOM_HOST"]
HEARTBEAT_QUEUE = os.environ["HEARTBEAT_QUEUE"]
HEARTBEAT_INTERVAL = int(os.environ.get("HEARTBEAT_INTERVAL", 5))
INPUT_EXCHANGE = os.environ["INPUT_EXCHANGE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SPLITTER_AMOUNT = int(os.environ["SPLITTER_AMOUNT"])
TOP_SIZE = int(os.environ["TOP_SIZE"])
STORAGE_PATH = os.environ.get("STORAGE_PATH", "storage")

class AggregationFilter:

    def __init__(self):
        self.heartbeat = HeartbeatSender(
            MOM_HOST, HEARTBEAT_QUEUE, socket.gethostname(), f"aggregation_{ID}", HEARTBEAT_INTERVAL
        )
        self.shutdown = GracefulShutdown()
        self.last_received = {}
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, INPUT_EXCHANGE, [f"words_{ID}"], consume=True, queue_name=f"aggregation_{ID}"
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.eofs = EofCounter(f"{STORAGE_PATH}/aggregation_{ID}_eofs", SPLITTER_AMOUNT)
        self.words_by_client = {}
        self.storage = storage.Storage(STORAGE_PATH)
        self.results = MessagePublisher(
            self.output_queue, f"{STORAGE_PATH}/aggregation_{ID}.wal",
            get_routing_key=lambda message_type, client_id, fields: None,
        )

    def _process_eof(self, client_id, message_id, sender_id, source):
        # for key in self.storage.load_key(client_id):
        #    top_words[key.key] = top_words.get(key.key, 0) + key.amount

        count = self.eofs.save(client_id, (message_id, sender_id))
        if count == SPLITTER_AMOUNT:
            top_parcial = heapq.nlargest(
                TOP_SIZE,
                self.words_by_client.get(client_id, {}).items(),
                key=lambda item: item[1],
            )
            self.results.send(MsgType.PARTIAL_TOP, client_id, message_id, (message_id, MsgType.PARTIAL_TOP), source, top_parcial, f"aggregation_{ID}")
            self._release_client(client_id)

    def _release_client(self, client_id):
        self.eofs.clear(client_id)
        self.words_by_client.pop(client_id, None)

    def process_message(self, message, ack, nack):
        message_type, fields = deserialize_msg(message)
        client_id, message_id, producer_id, key, *fields = fields
        producer = (client_id, producer_id)
        if self.last_received.get(producer) == key:
            ack()
            return
        if message_type == MsgType.WORD_BATCH:
            language, words = fields
            counts = self.words_by_client.setdefault(client_id, {})
            for word in words:
                # Q4 cuenta abstracts que contienen la palabra, no repeticiones.
                counts[word] = counts.get(word, 0) + 1
            
        elif message_type == MsgType.END_OF_RECORDS:
            self._process_eof(client_id, message_id, fields[0], source=(producer_id, key))
        else:
            nack()
            return
        self.last_received[producer] = key
        ack()

    def close(self):
        self.heartbeat.stop()
        for mom in [self.input_exchange, self.output_queue, self.results, self.eofs]:
            try:
                mom.close()
            except Exception as e:
                logging.error(f"Error closing message middleware: {e}")

    def start(self):
        try:
            self.eofs.recover()
            self.results.recover()
            self.heartbeat.start()
        except Exception:
            self.close()
            raise
        self.shutdown.run(self.input_exchange, self.process_message, self.close)

def main():
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s.%(msecs)03d | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    sys.exit(main())
