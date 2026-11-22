import os
import logging
import sys
import time
import urllib.error
import urllib.request

from common import middleware
from common.shutdown import GracefulShutdown
from common.storage.eof_counter import EofCounter
from common.storage.message_publisher import MessagePublisher
from common.message_protocol.internal import deserialize_msg, MsgType
from common.message_protocol.queries import QueryId

ID = os.environ["ID"]
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_EXCHANGE = os.environ["OUTPUT_EXCHANGE"]
INPUT_EXCHANGE = os.environ["INPUT_EXCHANGE"]
FILTER_YEAR_PLUS_AMOUNT = int(os.environ["FILTER_YEAR_PLUS_AMOUNT"])
IMAGE_TIMEOUT = float(os.environ.get("IMAGE_TIMEOUT", "10"))
IMAGE_USER_AGENT = os.environ.get(
    "IMAGE_USER_AGENT", "tp-distribuidos/0.0 (https://github.com/BautiLynch/tp-distri-grupal.git)"
)
IMAGE_RATE_LIMIT_DELAY = 60


class ImageGetter:
    def __init__(self):
        self.shutdown = GracefulShutdown()
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, INPUT_EXCHANGE, [ID], consume=True
        )
        self.output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, OUTPUT_EXCHANGE, [], consume=False
        )

        self.results = MessagePublisher(self.output_exchange, f"storage/image_getter_{ID}.wal")
        self.eofs = EofCounter(f"storage/image_getter_{ID}_eofs", FILTER_YEAR_PLUS_AMOUNT)
        self.rate_limited_until = 0

    def _get_image(self, image_url):
        if not image_url or time.time() < self.rate_limited_until:
            return b""

        request = urllib.request.Request(image_url, headers={"User-Agent": IMAGE_USER_AGENT})
        with urllib.request.urlopen(request, timeout=IMAGE_TIMEOUT) as response:
            return response.read()

    def process_message(self, message, ack, nack):
        message_type, client_id, message_id, fields = deserialize_msg(message)
        if message_type == MsgType.GET_IMAGE:
            url, name, image_url = fields
            try:
                image = self._get_image(image_url)
            except Exception as error:
                status_code = getattr(error, "code", None)
                logging.debug(
                    "Could not download image: image_url=%s error_code=%s message=%s",
                    image_url, status_code if status_code is not None else "N/A",
                    getattr(error, "reason", error),
                )
                if status_code == 403:
                    nack()
                    return
                if isinstance(error, urllib.error.HTTPError) and status_code == 429:
                    with error:
                        retry_after = error.headers.get("Retry-After", "")
                    try:
                        delay = int(retry_after)
                    except ValueError:
                        delay = IMAGE_RATE_LIMIT_DELAY
                    self.rate_limited_until = time.time() + delay
                    logging.debug("Wikimedia returned 429; skipping downloads for %s seconds", delay)
                image = b""
            self.results.send(MsgType.RESULT, client_id, message_id, (message_id, MsgType.RESULT), QueryId.Q2, name, url, image)

        elif message_type == MsgType.END_OF_RECORDS:
            count = self.eofs.save(client_id, (message_id, fields[0]))
            if count == FILTER_YEAR_PLUS_AMOUNT:
                self.results.send(MsgType.QUERY_END, client_id, message_id, (message_id, MsgType.QUERY_END), QueryId.Q2)
                self.eofs.clear(client_id)

        else:
            nack()
            return
        ack()

    def close(self):
        for resource in (self.input_exchange, self.output_exchange, self.results, self.eofs):
            try:
                resource.close()
            except Exception:
                logging.exception("Error closing image getter resource")

    def start(self):
        try:
            self.eofs.recover()
            self.results.recover()
        except Exception:
            self.close()
            raise
        self.shutdown.run(self.input_exchange, self.process_message, self.close)


def main():
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s.%(msecs)03d | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    image_getter = ImageGetter()
    image_getter.start()
    return 0


if __name__ == "__main__":
    sys.exit(main())
