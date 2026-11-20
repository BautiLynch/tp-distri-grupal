from datetime import datetime
import os
import logging

from common import middleware, hashing
from common.shutdown import GracefulShutdown
from common.storage.wal import WriteAheadLog
from common.message_protocol.internal import deserialize_msg, MsgType, serialize_msg

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
        self.wal = WriteAheadLog(f"storage/filter_{ID}.wal")

    def _recover_q1(self):
        pending = self.wal.load()
        if pending is None:
            return
        try:
            _, client_id, _ = deserialize_msg(pending)
        except ValueError:
            logging.warning("Discarding unreadable Q1 WAL")
            self.wal.discard()
            return
        self.output_exchange.send_to(pending, routing_key=client_id)
        self.wal.mark_sent()

    def _send_q1(self, message_type, client_id, *fields):
        result = serialize_msg(message_type, client_id, *fields)
        self.wal.save(result)
        self.output_exchange.send_to(result, routing_key=client_id)
        self.wal.mark_sent()

    def process_message(self, message, ack, nack):
        try:
            message_type, client_id, fields = deserialize_msg(message)
            if message_type == MsgType.FILTER_ARTICLE:
                name, url, language, date_modified, image_url, sections, *message_id = fields

                if datetime.fromisoformat(date_modified.replace("Z", "+00:00")).year <= YEAR:
                    if language.lower() == LANGUAGE.lower():
                        self._send_q1(MsgType.Q1_RESULT, client_id, url, date_modified, *message_id)

                    if self.sections.intersection(section.lower() for section in sections):
                        key = hashing.get_hash_key(f"{client_id}_{url}", IMAGE_GETTER_AMOUNT)
                        self.output_exchange_image.send_to(
                            serialize_msg(MsgType.GET_IMAGE, client_id, url, name, image_url),
                            routing_key=key,
                        )

            elif message_type == MsgType.END_OF_RECORDS:
                end_id = (fields[0] + ID.encode("ascii"),) if fields else ()
                self._send_q1(MsgType.QUERY_END, client_id, MsgType.Q1_RESULT, *end_id)
                self.output_exchange_image.send(serialize_msg(MsgType.END_OF_RECORDS, client_id))
        except Exception:
            logging.exception("Error processing message")
            nack()
            return

        ack()

    def close(self):
        for resource in (self.input_exchange, self.output_exchange, self.output_exchange_image, self.wal):
            try:
                resource.close()
            except Exception:
                logging.exception("Error closing filter resource")

    def start(self):
        try:
            self._recover_q1()
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
    raise SystemExit(main())
