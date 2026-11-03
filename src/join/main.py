import os
import logging

from common import middleware
from common.shutdown import GracefulShutdown
from common.key_item import KeyItem
from common.message_protocol.internal import deserialize_msg, serialize_msg, MsgType

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_EXCHANGE = os.environ["OUTPUT_EXCHANGE"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
TOP_SIZE = int(os.environ["TOP_SIZE"])

class JoinFilter:

    def __init__(self):
        self.shutdown = GracefulShutdown()
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, OUTPUT_EXCHANGE, [], consume=False
        )

        self.agg_by_client = {} # Contador
        self.top_by_client = {} # Lista de palabras

    def _process_data(self, client_id, top_parcial):
        new_words = []
        for word, amount in top_parcial:
            new_words.append(KeyItem(word, int(amount)))
        
        top_client = self.top_by_client.get(client_id, [])
        top_client.extend(new_words)
        top_client.sort(reverse=True)

        self.top_by_client[client_id] = top_client[:TOP_SIZE]

    def _all_tops_recieved(self, client_id):
        self.agg_by_client[client_id] = self.agg_by_client.get(client_id, 0) + 1
        if self.agg_by_client[client_id] < AGGREGATION_AMOUNT:
            return
        
        final_top = self.top_by_client.get(client_id, [])
        final_top.sort(reverse=True)

        result = [
            (item.key, item.amount)
            for item in final_top
        ]

        self.output_exchange.send_to(serialize_msg(MsgType.Q4_RESULT, client_id, result), routing_key=client_id)
        self.output_exchange.send_to(serialize_msg(MsgType.QUERY_END, client_id, MsgType.Q4_RESULT), routing_key=client_id)

        self._release_client(client_id)

    def _release_client(self, client_id):
        self.agg_by_client.pop(client_id, None)
        self.top_by_client.pop(client_id, None)

    def process_message(self, message, ack, nack):
        message_type, client_id, fields = deserialize_msg(message)
        if message_type == MsgType.Q4_RESULT:
            self._process_data(client_id, *fields)
            self._all_tops_recieved(client_id)
        else:
            nack()
            return
        ack()

    def close(self):
        for mom in [self.input_queue, self.output_exchange]:
            try:
                mom.close()
            except Exception as e:
                logging.error(f"Error closing message middleware: {e}")
    
    def start(self):
        self.shutdown.run(self.input_queue, self.process_message, self.close)

def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
