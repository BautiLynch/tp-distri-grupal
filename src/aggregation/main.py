import heapq
import os
import logging
import signal
import threading

from common import middleware, storage
from common.message_protocol.internal import deserialize_msg, serialize_msg, MsgType

ID = os.environ["ID"]
MOM_HOST = os.environ["MOM_HOST"]
INPUT_EXCHANGE = os.environ["INPUT_EXCHANGE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SPLITTER_AMOUNT = int(os.environ["SPLITTER_AMOUNT"])
TOP_SIZE = int(os.environ["TOP_SIZE"])
STORAGE_PATH = os.environ.get("STORAGE_PATH", "storage")

class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, INPUT_EXCHANGE, [ID]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.eof_by_client = {}
        self.words_by_client = {}
        self.storage = storage.Storage(STORAGE_PATH)
        self.end_event = threading.Event()
        signal.signal(signal.SIGTERM, self.sigterm_handler)

    def _process_eof(self, client_id):
        logging.info("Received EOF")

        # for key in self.storage.load_key(client_id):
        #    top_words[key.key] = top_words.get(key.key, 0) + key.amount

        self.eof_by_client[client_id] = self.eof_by_client.get(client_id, 0) + 1
        if self.eof_by_client[client_id] == SPLITTER_AMOUNT:
            top_parcial = heapq.nlargest(
                TOP_SIZE,
                self.words_by_client.get(client_id, {}).items(),
                key=lambda item: item[1],
            )
            self.words_by_client[client_id] = {}
            self.output_queue.send(serialize_msg(MsgType.Q4_RESULT, client_id, top_parcial)) 


    def process_message(self, message, ack, nack):
        logging.info("Process message")
        
        message_type, client_id, fields = deserialize_msg(message)
        if message_type == MsgType.WORD_AMOUNT:
            client_id, word = fields
            self.words_by_client[client_id] = self.words_by_client.get(client_id, {})
            self.words_by_client[client_id][word] = self.words_by_client[client_id].get(word, 0) + 1
            #self.storage.store_key(client_id, key, 1)
            
        elif message_type == MsgType.END_OF_RECORDS:
            self._process_eof(client_id)
        else:
            nack()
            return
        ack()

    def sigterm_handler(self, signum, frame):
        if self.end_event.is_set():
            return
        self.end_event.set()
        self.input_exchange.stop_consuming_threadsafe()

    def close(self):
        for mom in [self.input_exchange, self.output_queue]:
            try:
                mom.close()
            except Exception as e:
                logging.error(f"Error closing message middleware: {e}")

    def start(self):
        try:
            self.input_exchange.start_consuming(self.process_message)
        finally:
            self.close()

def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()