import os
import socket
import logging
import sys

from common import middleware
from common.heartbeat import HeartbeatSender
from common.shutdown import GracefulShutdown
from common.storage.message_publisher import MessagePublisher
from common.storage.eof_counter import EofCounter
from common.storage.state_log import StateLog
from common.message_protocol.internal import deserialize_msg, MsgType
from common.message_protocol.queries import QueryId

ID = os.environ["ID"]
MOM_HOST = os.environ["MOM_HOST"]
HEARTBEAT_QUEUE = os.environ["HEARTBEAT_QUEUE"]
HEARTBEAT_INTERVAL = int(os.environ.get("HEARTBEAT_INTERVAL", 5))
INPUT_EXCHANGE = os.environ["INPUT_EXCHANGE"]
OUTPUT_EXCHANGE = os.environ["OUTPUT_EXCHANGE"]
STORAGE_PATH = os.environ.get("STORAGE_PATH", "storage")
SPLITTER_AMOUNT = int(os.environ["SPLITTER_AMOUNT"])


class FilterLanguage:

    def __init__(self):
        self.heartbeat = HeartbeatSender(
            MOM_HOST, HEARTBEAT_QUEUE, socket.gethostname(), f"filter_language_{ID}", HEARTBEAT_INTERVAL
        )
        self.shutdown = GracefulShutdown()
        self.last_received = {}
        self.state = StateLog(f"{STORAGE_PATH}/filter_language_{ID}_state")
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, INPUT_EXCHANGE, [f"words_{ID}"], consume=True, queue_name=f"filter_language_{ID}"
        )
        self.output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ( 
            MOM_HOST, OUTPUT_EXCHANGE, [], consume=False
        )

        self.eofs = EofCounter(f"{STORAGE_PATH}/filter_language_{ID}_eofs", SPLITTER_AMOUNT)

        self.words_by_client = {}
        self.results = MessagePublisher(self.output_exchange, f"{STORAGE_PATH}/filter_language_{ID}.wal")

    def process_message(self, message, ack, nack):
        message_type, fields = deserialize_msg(message)
        client_id, message_id, producer_id, key, *fields = fields
        producer = (client_id, producer_id)
        if self.last_received.get(producer) == key:
            ack()
            return
        if message_type == MsgType.WORD_BATCH:
            language, words = fields
            self.state.save(client_id, message)
            word_sent = self.words_by_client.setdefault(client_id, {})
            for word in words:
                language_sent = word_sent.get(word, set())
                if language not in language_sent:
                    language_sent.add(language)
                    word_sent[word] = language_sent
                    if len(language_sent) == 2:
                        self.results.send(MsgType.RESULT, client_id, message_id, (word, MsgType.RESULT), (producer_id, key), QueryId.Q3, word)

        elif message_type == MsgType.END_OF_RECORDS:
            count = self.eofs.save(client_id, (message_id, fields[0]))
            if count == SPLITTER_AMOUNT:
                self.results.send(MsgType.QUERY_END, client_id, message_id, (message_id, MsgType.QUERY_END), (producer_id, key), QueryId.Q3)
                self._release_client(client_id)

        else:
            nack()
            return
        if message_type == MsgType.WORD_BATCH:
            self.state.mark_processed(client_id)
        self.last_received[producer] = key
        ack()

    def _release_client(self, client_id):
        self.state.clear(client_id)
        self.eofs.clear(client_id)
        self.words_by_client.pop(client_id, None)

    def _recover_state(self, record):
        message, context = record
        _, fields = deserialize_msg(message)
        client_id, message_id, producer_id, key, language, words = fields
        word_sent = self.words_by_client.setdefault(client_id, {})
        for word in words:
            word_sent.setdefault(word, set()).add(language)
        self.last_received[(client_id, producer_id)] = key

    def close(self):
        self.heartbeat.stop()
        for mom in [self.input_exchange, self.output_exchange, self.results, self.eofs, self.state]:
            try:
                mom.close()
            except Exception as e:
                logging.error(f"Error closing message middleware: {e}")

    def start(self):
        try:
            self.eofs.recover()
            self.results.recover()
            self.state.recover(self._recover_state)
            if self.results.last is not None:
                (client_id, source), sent = self.results.last
                if any(key[-1] == MsgType.QUERY_END for key in sent):
                    self._release_client(client_id)
                    self.last_received[(client_id, source[0])] = source[1]
            self.heartbeat.start()
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
    filter_average = FilterLanguage()
    filter_average.start()
    return 0


if __name__ == "__main__":
    sys.exit(main())
