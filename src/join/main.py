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
from common.key_item import KeyItem
from common.message_protocol.internal import deserialize_msg, MsgType
from common.message_protocol.queries import QueryId

MOM_HOST = os.environ["MOM_HOST"]
HEARTBEAT_QUEUE = os.environ["HEARTBEAT_QUEUE"]
HEARTBEAT_INTERVAL = int(os.environ.get("HEARTBEAT_INTERVAL", 5))
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_EXCHANGE = os.environ["OUTPUT_EXCHANGE"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
TOP_SIZE = int(os.environ["TOP_SIZE"])
STORAGE_PATH = os.environ.get("STORAGE_PATH", "storage")

class JoinFilter:

    def __init__(self):
        self.heartbeat = HeartbeatSender(
            MOM_HOST, HEARTBEAT_QUEUE, socket.gethostname(), "join", HEARTBEAT_INTERVAL
        )
        self.shutdown = GracefulShutdown()
        self.last_received = {}
        self.state = StateLog(f"{STORAGE_PATH}/join_state")
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, OUTPUT_EXCHANGE, [], consume=False
        )

        self.eofs = EofCounter(f"{STORAGE_PATH}/join_eofs", AGGREGATION_AMOUNT)
        self.results = MessagePublisher(self.output_exchange, f"{STORAGE_PATH}/join.wal")
        self.top_by_client = {} # Lista de palabras

    def _process_data(self, client_id, top_parcial):
        new_words = []
        for word, amount in top_parcial:
            new_words.append(KeyItem(word, int(amount)))
        
        top_client = self.top_by_client.get(client_id, [])
        top_client.extend(new_words)
        top_client.sort(reverse=True)

        self.top_by_client[client_id] = top_client[:TOP_SIZE]

    def _all_tops_recieved(self, client_id, message_id, sender_id, source):
        count = self.eofs.save(client_id, (message_id, sender_id))
        if count < AGGREGATION_AMOUNT:
            return
        
        final_top = self.top_by_client.get(client_id, [])
        final_top.sort(reverse=True)

        result = [
            (item.key, item.amount)
            for item in final_top
        ]

        self.results.send(MsgType.RESULT, client_id, message_id, (message_id, MsgType.RESULT), source, QueryId.Q4, result)
        self.results.send(MsgType.QUERY_END, client_id, message_id, (message_id, MsgType.QUERY_END), source, QueryId.Q4)

    def _release_client(self, client_id):
        self.state.clear(client_id)
        self.eofs.clear(client_id)
        self.top_by_client.pop(client_id, None)

    def process_message(self, message, ack, nack):
        message_type, fields = deserialize_msg(message)
        client_id, message_id, producer_id, key, *fields = fields
        producer = (client_id, producer_id)
        if self.last_received.get(producer) == key:
            ack()
            return
        if message_type == MsgType.PARTIAL_TOP:
            top_parcial, sender_id = fields
            self.state.save(client_id, message)
            if self.state.pending == message or (message_id, sender_id) not in self.eofs.received.get(client_id, set()):
                self._process_data(client_id, top_parcial)
            self._all_tops_recieved(client_id, message_id, sender_id, source=(producer_id, key))
        else:
            nack()
            return
        self.state.mark_processed(client_id)
        if self.eofs.get(client_id) >= AGGREGATION_AMOUNT:
            self._release_client(client_id)
        self.last_received[producer] = key
        ack()

    def _recover_state(self, record):
        message, context = record
        _, fields = deserialize_msg(message)
        client_id, message_id, producer_id, key, top_parcial, sender_id = fields
        self._process_data(client_id, top_parcial)
        self.last_received[(client_id, producer_id)] = key

    def close(self):
        self.heartbeat.stop()
        for mom in [self.input_queue, self.output_exchange, self.results, self.eofs, self.state]:
            try:
                mom.close()
            except Exception as e:
                logging.error(f"Error closing message middleware: {e}")
    
    def start(self):
        try:
            self.eofs.recover()
            self.results.recover()
            self.state.recover(self._recover_state)
            if self.results.last is not None:
                (client_id, source), sent = self.results.last
                if any(key[-1] == MsgType.QUERY_END for key in sent):
                    self._release_client(client_id)
                    self.last_received[(client_id, source[0])] = source[1]
            self.heartbeat.start()
        except Exception:
            self.close()
            raise
        self.shutdown.run(self.input_queue, self.process_message, self.close)

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    join_filter = JoinFilter()
    join_filter.start()
    return 0


if __name__ == "__main__":
    sys.exit(main())
