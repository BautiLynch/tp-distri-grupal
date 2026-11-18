import os
import logging

from common import middleware
from common.heartbeat import HeartbeatSender
from common.shutdown import GracefulShutdown
from common.message_protocol.internal import deserialize_msg, serialize_msg, MsgType

ID = os.environ["ID"]
MOM_HOST = os.environ["MOM_HOST"]
HEARTBEAT_EXCHANGE = os.environ["HEARTBEAT_EXCHANGE"]
HEARTBEAT_INTERVAL = int(os.environ.get("HEARTBEAT_INTERVAL", 5))
INPUT_EXCHANGE = os.environ["INPUT_EXCHANGE"]
OUTPUT_EXCHANGE = os.environ["OUTPUT_EXCHANGE"]
STORAGE_PATH = os.environ.get("STORAGE_PATH", "storage")
SPLITTER_AMOUNT = int(os.environ["SPLITTER_AMOUNT"])


class FilterLanguage:

    def __init__(self):
        # El nombre es el del servicio en el compose: el monitor lo usa para reiniciarlo.
        self.heartbeat = HeartbeatSender(
            MOM_HOST, HEARTBEAT_EXCHANGE, ID, f"filter_language_{ID}", HEARTBEAT_INTERVAL
        )
        self.shutdown = GracefulShutdown()
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, INPUT_EXCHANGE, [f"words_{ID}"], consume=True, queue_name=f"filter_language_{ID}"
        )
        self.output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ( 
            MOM_HOST, OUTPUT_EXCHANGE, [], consume=False
        )

        self.eof_by_client = {}

        self.words_by_client = {}

    def process_message(self, message, ack, nack):
        message_type, client_id, fields = deserialize_msg(message)
        if message_type == MsgType.WORD_BATCH:
            language, words = fields
            word_sent = self.words_by_client.setdefault(client_id, {})
            for word in words:
                language_sent = word_sent.get(word, set())
                if len(language_sent) == 2:
                    continue
                if language not in language_sent:
                    language_sent.add(language)
                    word_sent[word] = language_sent
                    if len(language_sent) == 2:
                        self.output_exchange.send_to(
                            serialize_msg(MsgType.Q3_RESULT, client_id, word), routing_key=client_id
                        )

        elif message_type == MsgType.END_OF_RECORDS:
            self.eof_by_client[client_id] = self.eof_by_client.get(client_id, 0) + 1
            if self.eof_by_client[client_id] == SPLITTER_AMOUNT:
                self.output_exchange.send_to(serialize_msg(MsgType.QUERY_END, client_id, MsgType.Q3_RESULT), routing_key=client_id)
                self._release_client(client_id)

        else:
            nack()
            return
        ack()

    def _release_client(self, client_id):
        self.eof_by_client.pop(client_id, None)
        self.words_by_client.pop(client_id, None)

    def close(self):
        self.heartbeat.stop()
        for mom in [self.input_exchange, self.output_exchange]:
            try:
                mom.close()
            except Exception as e:
                logging.error(f"Error closing message middleware: {e}")

    def start(self):
        self.heartbeat.start()
        self.shutdown.run(self.input_exchange, self.process_message, self.close)

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    filter_average = FilterLanguage()
    filter_average.start()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
