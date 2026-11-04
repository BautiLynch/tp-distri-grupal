import os
import logging
import urllib.request

from common import middleware
from common.shutdown import GracefulShutdown
from common.message_protocol.internal import deserialize_msg, MsgType, serialize_msg

ID = os.environ["ID"]
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_EXCHANGE = os.environ["OUTPUT_EXCHANGE"]
INPUT_EXCHANGE = os.environ["INPUT_EXCHANGE"]
FILTER_YEAR_PLUS_AMOUNT = int(os.environ["FILTER_YEAR_PLUS_AMOUNT"])
IMAGE_TIMEOUT = float(os.environ.get("IMAGE_TIMEOUT", "10"))
IMAGE_USER_AGENT = os.environ.get("IMAGE_USER_AGENT", "tp-distribuidos")


class ImageGetter:
    def __init__(self):
        self.shutdown = GracefulShutdown()
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, INPUT_EXCHANGE, [ID], consume=True
        )
        self.output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, OUTPUT_EXCHANGE, [], consume=False
        )

        self.eof_by_client = {}

    def _get_image(self, client_id, name, url, image_url):
        if not image_url:
            self.output_exchange.send_to(serialize_msg(MsgType.Q2_RESULT, client_id, name, url, b""), routing_key=client_id)
            return

        request = urllib.request.Request(image_url, headers={"User-Agent": IMAGE_USER_AGENT})
        with urllib.request.urlopen(request, timeout=IMAGE_TIMEOUT) as response:
            self.output_exchange.send_to(serialize_msg(MsgType.Q2_RESULT, client_id, name, url, response.read()), routing_key=client_id)

    def process_message(self, message, ack, nack):
        message_type, client_id, fields = deserialize_msg(message)
        if message_type == MsgType.GET_IMAGE:
            url, name, image_url = fields
            try:
                self._get_image(client_id, name, url, image_url)
            except Exception as error:
                if error.code == 403:
                    nack()
                    return
                else:
                    self.output_exchange.send_to(serialize_msg(MsgType.Q2_RESULT, client_id, name, url, b""), routing_key=client_id)

        elif message_type == MsgType.END_OF_RECORDS:
            self.eof_by_client[client_id] = self.eof_by_client.get(client_id, 0) + 1
            if self.eof_by_client[client_id] == FILTER_YEAR_PLUS_AMOUNT:
                self.output_exchange.send_to(serialize_msg(MsgType.QUERY_END, client_id, MsgType.Q2_RESULT), routing_key=client_id)
                self._release_client(client_id)

        else:
            nack()
            return
        ack()

    def _release_client(self, client_id):
        self.eof_by_client.pop(client_id, None)

    def close(self):
        for mom in [self.input_exchange, self.output_exchange]:
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
    image_getter = ImageGetter()
    image_getter.start()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
