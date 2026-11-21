from datetime import datetime
import os
import logging
import sys

from common import middleware, hashing
from common.shutdown import GracefulShutdown
from common.storage.message_publisher import MessagePublisher
from common.message_protocol.internal import deserialize_msg, MsgType
from common.message_protocol.queries import QueryId

ID = os.environ["ID"]
MOM_HOST = os.environ["MOM_HOST"]
INPUT_EXCHANGE = os.environ["INPUT_EXCHANGE"]
OUTPUT_EXCHANGE_1 = os.environ["OUTPUT_EXCHANGE_1"]
OUTPUT_EXCHANGE_2 = os.environ["OUTPUT_EXCHANGE_2"]
IMAGE_GETTER_PREFIX = os.environ["IMAGE_GETTER_PREFIX"]
IMAGE_GETTER_AMOUNT = int(os.environ["IMAGE_GETTER_AMOUNT"])
LANGUAGE = os.environ.get("LANGUAGE")
SECTIONS = os.environ.get("SECTIONS").split(",")
YEAR = int(os.environ["YEAR"])


class FilterYearPlus:
    def __init__(self):
        self.shutdown = GracefulShutdown()
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, INPUT_EXCHANGE, [ID], consume=True
        )
        self.output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ( 
            MOM_HOST, OUTPUT_EXCHANGE_1, [], consume=False
        )
        self.output_exchange_image = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, OUTPUT_EXCHANGE_2, [f"{index}" for index in range(IMAGE_GETTER_AMOUNT)], consume=False
        )

        self.sections = {section.lower() for section in SECTIONS}
        self.results = MessagePublisher(self.output_exchange, f"storage/filter_{ID}.wal")
        self.images = MessagePublisher(
            self.output_exchange_image, f"storage/filter_{ID}_images.wal",
            get_routing_key=self._image_routing_key,
        )

    def _image_routing_key(self, message_type, client_id, fields):
        if message_type == MsgType.GET_IMAGE:
            url = fields[0]
            return hashing.get_hash_key(f"{client_id}_{url}", IMAGE_GETTER_AMOUNT)
        return None

    def process_message(self, message, ack, nack):
        try:
            message_type, client_id, fields = deserialize_msg(message)
            if message_type == MsgType.FILTER_ARTICLE:
                name, url, language, date_modified, image_url, sections = fields

                if datetime.fromisoformat(date_modified.replace("Z", "+00:00")).year <= YEAR:
                    if language.lower() == LANGUAGE.lower():
                        self.results.send(MsgType.RESULT, client_id, QueryId.Q1, url, date_modified)

                    if self.sections.intersection(section.lower() for section in sections):
                        self.images.send(MsgType.GET_IMAGE, client_id, url, name, image_url)

            elif message_type == MsgType.END_OF_RECORDS:
                self.results.send(MsgType.QUERY_END, client_id, QueryId.Q1)
                self.images.send(MsgType.END_OF_RECORDS, client_id)
        except Exception:
            logging.exception("Error processing message")
            nack()
            return

        ack()

    def close(self):
        for resource in (self.input_exchange, self.output_exchange, self.output_exchange_image, self.results, self.images):
            try:
                resource.close()
            except Exception:
                logging.exception("Error closing filter resource")

    def start(self):
        try:
            self.results.recover()
            self.images.recover()
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
    filter_average = FilterYearPlus()
    filter_average.start()
    return 0


if __name__ == "__main__":
    sys.exit(main())
