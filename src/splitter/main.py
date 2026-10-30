import os
import logging
import signal
import threading

from common import middleware, message_protocol

MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
STOPWORDS_PATH = os.environ.get("STOPWORDS_PATH", "stopwords")

class Splitter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

        self.en_stopwords = self._load_stopwords("english")
        self.fr_stopwords = self._load_stopwords("french")

        self.word_count = {}
        self.end_event = threading.Event()
        signal.signal(signal.SIGTERM, self.sigterm_handler)

    def _load_stopwords(self, language):
        file_path = os.path.join(STOPWORDS_PATH, f"{language}.txt")
        with open(file_path, "r", encoding="utf-8") as file:
            stopwords_list = [line.strip() for line in file.readlines()]

        return set(stopwords_list)

    def _split_and_send(self, client_id, article):
        url, language, date_modified, abstract = article
        words = abstract.split(" ")
        for word in words:
            if self._is_stopword(word, language):
                continue

            self.word_count[word] = self.word_count.get(word, 0) + 1

        for word, amount in self.word_count.items():
            message = message_protocol.internal.serialize_msg(
                message_protocol.internal.MsgType.WORD,
                client_id, message_protocol.internal.INPUT_QUERY_ID,
                url, language, date_modified, word, amount,
            )
            self.output_queue.send(message)

    def _is_stopword(self, word, language):
        if language == "en":
            return word in self.en_stopwords
        elif language == "fr":
            return word in self.fr_stopwords
        else:
            return False

    def process_message(self, message, ack, nack):
        logging.info("Process message")
        try:
            message_type, client_id, _, article = message_protocol.internal.deserialize_msg(message)
            if message_type == message_protocol.internal.MsgType.SPLITTER_ARTICLE:
                self._split_and_send(client_id, article)
            elif message_type == message_protocol.internal.MsgType.END_OF_RECORDS:
                logging.info("Received EOF")
                self.output_queue.send(message_protocol.internal.serialize_msg(
                    message_protocol.internal.MsgType.END_OF_RECORDS,
                    client_id, message_protocol.internal.INPUT_QUERY_ID,
                ))
            else:
                raise ValueError(f"Unexpected splitter message type: {message_type}")
        except Exception:
            logging.exception("Could not process splitter input")
            nack()
            return
        ack()

    def close(self):
        for mom in [self.input_queue, self.output_queue]:
            try:
                mom.close()
            except Exception as e:
                logging.error(f"Error closing message middleware: {e}")

    def start(self):
        try:
            self.input_queue.start_consuming(self.process_message)
        finally:
            self.close()

    def sigterm_handler(self, signum, frame):
        if self.end_event.is_set():
            return
        self.end_event.set()
        self.input_queue.stop_consuming_threadsafe()

def main():
    logging.basicConfig(level=logging.INFO)
    splitter = Splitter()
    splitter.start()
    return 0


if __name__ == "__main__":
    main()
