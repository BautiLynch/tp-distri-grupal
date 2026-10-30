import os
import logging
import signal
import threading

from common import middleware
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
YEAR = os.environ.get("YEAR")


class FilterYearPlus:
    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, INPUT_EXCHANGE, [ID]
        )
        self.output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ( 
            MOM_HOST, OUTPUT_EXCHANGE_1, []
        )
        self.output_exchange_image = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, OUTPUT_EXCHANGE_2, [f"{index}" for index in range(IMAGE_GETTER_AMOUNT)]
        )

        self.sections = set(SECTIONS)

        self.end_event = threading.Event()
        signal.signal(signal.SIGTERM, self.sigterm_handler)

    def process_message(self, message, ack, nack):
        try:
            message_type, client_id, fields = deserialize_msg(message)
            if message_type == MsgType.FILTER_ARTICLE:
                url, language, date_modified, name, sections, image_url = fields

                if date_modified >= YEAR and language == LANGUAGE:
                    self.output_exchange.send_to(serialize_msg(MsgType.Q1_RESULT, client_id, url, date_modified), routing_key=client_id)

                elif date_modified >= YEAR and sections & self.sections:
                    self.output_exchange_image.send_to(serialize_msg(MsgType.GET_IMAGE, client_id, url, name, image_url), routing_key=client_id)
            
            elif message_type == MsgType.END_OF_RECORDS:
                logging.info("Received EOF")
                self.output_exchange.send_to(serialize_msg(MsgType.QUERY_END, client_id, MsgType.Q1_RESULT), routing_key=client_id)
                self.output_exchange_image.send(serialize_msg(MsgType.END_OF_RECORDS, client_id))

        except Exception:
            logging.exception("Error processing message")
            nack()
            return

        ack()

    def sigterm_handler(self, signum, frame):
        if self.end_event.is_set():
            return
        self.end_event.set()
        self.input_exchange.stop_consuming_threadsafe()

    def close(self):
        for mom in [self.input_exchange, self.output_exchange]:
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
    filter_average = FilterYearPlus()
    filter_average.start()
    return 0


if __name__ == "__main__":
    main()
