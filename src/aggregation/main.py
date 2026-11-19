import heapq
import os
import socket
import logging

from common import middleware, storage
from common.heartbeat import HeartbeatSender
from common.shutdown import GracefulShutdown
from common.message_protocol.internal import deserialize_msg, serialize_msg, MsgType

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
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, INPUT_EXCHANGE, [f"words_{ID}"], consume=True, queue_name=f"aggregation_{ID}"
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.eof_by_client = {}
        self.words_by_client = {}
        self.storage = storage.Storage(STORAGE_PATH)

    def _process_eof(self, client_id):
        # for key in self.storage.load_key(client_id):
        #    top_words[key.key] = top_words.get(key.key, 0) + key.amount

        self.eof_by_client[client_id] = self.eof_by_client.get(client_id, 0) + 1
        if self.eof_by_client[client_id] == SPLITTER_AMOUNT:
            top_parcial = heapq.nlargest(
                TOP_SIZE,
                self.words_by_client.get(client_id, {}).items(),
                key=lambda item: item[1],
            )
            self.output_queue.send(serialize_msg(MsgType.PARTIAL_TOP, client_id, top_parcial))
            self._release_client(client_id)

    def _release_client(self, client_id):
        self.eof_by_client.pop(client_id, None)
        self.words_by_client.pop(client_id, None)

    def process_message(self, message, ack, nack):
        message_type, fields = deserialize_msg(message)
        client_id, *fields = fields
        if message_type == MsgType.WORD_BATCH:
            language, words = fields
            counts = self.words_by_client.setdefault(client_id, {})
            for word in words:
                # Q4 cuenta abstracts que contienen la palabra, no repeticiones.
                counts[word] = counts.get(word, 0) + 1
            
        elif message_type == MsgType.END_OF_RECORDS:
            self._process_eof(client_id)
        else:
            nack()
            return
        ack()

    def close(self):
        self.heartbeat.stop()
        for mom in [self.input_exchange, self.output_queue]:
            try:
                mom.close()
            except Exception as e:
                logging.error(f"Error closing message middleware: {e}")

    def start(self):
        self.heartbeat.start()
        self.shutdown.run(self.input_exchange, self.process_message, self.close)

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
