import os
import logging
import signal
import threading

from datetime import datetime
from common import middleware, hashing
from common.message_protocol.internal import deserialize_msg, serialize_msg, MsgType

ID = os.environ["ID"]
MOM_HOST = os.environ["MOM_HOST"]
INPUT_EXCHANGE = os.environ["INPUT_EXCHANGE"]
AVERAGE_EXCHANGE = os.environ["AVERAGE_EXCHANGE"]
AVERAGE_AMOUNT = int(os.environ.get("AVERAGE_AMOUNT"))
FILTER_AVERAGE_EXCHANGE = os.environ["FILTER_AVERAGE_EXCHANGE"]
FILTER_AVERAGE_AMOUNT = int(os.environ.get("FILTER_AVERAGE_AMOUNT"))
SPLITTER_AMOUNT = int(os.environ["SPLITTER_AMOUNT"])
STORAGE_PATH = os.environ.get("STORAGE_PATH", "storage")
YEAR = int(os.environ.get("YEAR"))


class FilterYear:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, INPUT_EXCHANGE, [ID]
        )
        self.output_exchange_average = middleware.MessageMiddlewareExchangeRabbitMQ( 
            MOM_HOST, AVERAGE_EXCHANGE, [f"{index}" for index in range(AVERAGE_AMOUNT)]
        )
        self.output_exchange_filter_average = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, FILTER_AVERAGE_EXCHANGE, [f"{index}" for index in range(FILTER_AVERAGE_AMOUNT)]
        )

        self.eof_by_client = {}

        self.end_event = threading.Event()
        signal.signal(signal.SIGTERM, self.sigterm_handler)

    def process_message(self, message, ack, nack):
        message_type, client_id, fields = deserialize_msg(message)
        if message_type == MsgType.WORD_YEAR_AMOUNT:
            url, date_modified, amount = fields
            
            # Los articulos hasta YEAR alimentan el promedio en todos los filtros.
            if datetime.fromisoformat(date_modified.replace("Z", "+00:00")).year <= YEAR:
                self.output_exchange_filter_average.send(message) 
            # Los posteriores se acumulan por URL en un unico Average.
            else:
                key = hashing.get_hash_key(f"{client_id}_{url}", AVERAGE_AMOUNT)
                self.output_exchange_average.send_to(message, routing_key=key)

        elif message_type == MsgType.END_OF_RECORDS:
            self.eof_by_client[client_id] = self.eof_by_client.get(client_id, 0) + 1

            if self.eof_by_client[client_id] == SPLITTER_AMOUNT:
                self.output_exchange_average.send(message)
                self.output_exchange_filter_average.send(message)
                self._release_client(client_id)

        else:
            nack()
            return
        ack()

    def _release_client(self, client_id):
        self.eof_by_client.pop(client_id, None)
        logging.debug("Released state for client %s", client_id)

    def sigterm_handler(self, signum, frame):
        if self.end_event.is_set():
            return
        self.end_event.set()
        self.input_exchange.stop_consuming_threadsafe()

    def close(self):
        for mom in [self.input_exchange, self.output_exchange_average, self.output_exchange_filter_average]:
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
    filter_average = FilterYear()
    filter_average.start()
    return 0


if __name__ == "__main__":
    main()
