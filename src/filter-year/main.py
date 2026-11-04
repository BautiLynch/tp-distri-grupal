import os
import logging

from datetime import datetime
from common import middleware, hashing
from common.shutdown import GracefulShutdown
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
        self.shutdown = GracefulShutdown()
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, INPUT_EXCHANGE, [f"urls.{ID}"], consume=True
        )
        self.output_exchange_average = middleware.MessageMiddlewareExchangeRabbitMQ( 
            MOM_HOST, AVERAGE_EXCHANGE, [f"{index}" for index in range(AVERAGE_AMOUNT)], consume=False
        )
        self.output_exchange_filter_average = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, FILTER_AVERAGE_EXCHANGE, [f"{index}" for index in range(FILTER_AVERAGE_AMOUNT)], consume=False
        )

        self.eof_by_client = {}

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
                self.output_exchange_filter_average.send(
                    serialize_msg(MsgType.BASELINE_END, client_id)
                )
                self.output_exchange_average.send(message)
                self._release_client(client_id)

        else:
            nack()
            return
        ack()

    def _release_client(self, client_id):
        self.eof_by_client.pop(client_id, None)

    def close(self):
        for mom in [self.input_exchange, self.output_exchange_average, self.output_exchange_filter_average]:
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
    filter_average = FilterYear()
    filter_average.start()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
