import os
import logging
import sys

from datetime import datetime
from common import middleware, hashing
from common.shutdown import GracefulShutdown
from common.storage.message_publisher import MessagePublisher
from common.storage.eof_counter import EofCounter
from common.message_protocol.internal import deserialize_msg, MsgType

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
        self.last_received = {}
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, INPUT_EXCHANGE, [f"urls_{ID}"], consume=True
        )
        self.output_exchange_average = middleware.MessageMiddlewareExchangeRabbitMQ( 
            MOM_HOST, AVERAGE_EXCHANGE, [f"{index}" for index in range(AVERAGE_AMOUNT)], consume=False
        )
        self.output_exchange_filter_average = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, FILTER_AVERAGE_EXCHANGE, [f"{index}" for index in range(FILTER_AVERAGE_AMOUNT)], consume=False
        )

        self.averages = MessagePublisher(
            self.output_exchange_average, f"{STORAGE_PATH}/filter_year_{ID}_average.wal",
            get_routing_key=self._average_routing_key,
        )
        self.baselines = MessagePublisher(
            self.output_exchange_filter_average, f"{STORAGE_PATH}/filter_year_{ID}_baseline.wal",
            get_routing_key=lambda message_type, client_id, fields: None,
        )
        self.eofs = EofCounter(f"{STORAGE_PATH}/filter_year_{ID}_eofs", SPLITTER_AMOUNT)

    def _average_routing_key(self, message_type, client_id, fields):
        if message_type == MsgType.WORD_YEAR_AMOUNT:
            return hashing.get_hash_key(f"{client_id}_{fields[0]}", AVERAGE_AMOUNT)
        return None

    def process_message(self, message, ack, nack):
        message_type, client_id, message_id, producer_id, key, fields = deserialize_msg(message)
        producer = (client_id, producer_id)
        if self.last_received.get(producer) == key:
            ack()
            return
        if message_type == MsgType.WORD_YEAR_AMOUNT:
            url, date_modified, amount = fields
            
            # Los articulos hasta YEAR alimentan el promedio en todos los filtros.
            if datetime.fromisoformat(date_modified.replace("Z", "+00:00")).year <= YEAR:
                self.baselines.send(message_type, client_id, message_id, (message_id, message_type), *fields)
            # Los posteriores se acumulan por URL en un unico Average.
            else:
                self.averages.send(message_type, client_id, message_id, (message_id, message_type), *fields)

        elif message_type == MsgType.END_OF_RECORDS:
            count = self.eofs.save(client_id, (message_id, fields[0]))

            if count == SPLITTER_AMOUNT:
                self.baselines.send(MsgType.BASELINE_END, client_id, message_id, (message_id, MsgType.BASELINE_END), f"filter_year_{ID}")
                self.averages.send(message_type, client_id, message_id, (message_id, message_type), f"filter_year_{ID}")
                self.eofs.clear(client_id)

        else:
            nack()
            return
        self.last_received[producer] = key
        ack()

    def close(self):
        for mom in [self.input_exchange, self.output_exchange_average, self.output_exchange_filter_average, self.averages, self.baselines, self.eofs]:
            try:
                mom.close()
            except Exception as e:
                logging.error(f"Error closing message middleware: {e}")

    def start(self):
        try:
            self.eofs.recover()
            self.baselines.recover()
            self.averages.recover()
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
    filter_average = FilterYear()
    filter_average.start()
    return 0


if __name__ == "__main__":
    sys.exit(main())
