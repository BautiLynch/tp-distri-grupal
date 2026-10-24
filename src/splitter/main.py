import os
import logging
import signal
import threading

from common import middleware, message_protocol

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SPLITTER_PREFIX = os.environ["SPLITTER_PREFIX"]
STOPWORDS_PATH = os.environ.get("STOPWORDS_PATH", "stopwords")

class Splitter:
    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SPLITTER_PREFIX, [f"{SPLITTER_PREFIX}_{ID}"]
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

    def _split_and_send(self, client_id, title, url, language, date, abstract):
        words = abstract.split(" ")
        for word in words:
            if self._is_stopword(word, language):
                continue

            self.word_count[word] = self.word_count.get(word, 0) + 1

        for word, amount in self.word_count.items():
            message = [client_id, title, url, language, date, word, amount]
            self.output_queue.send(message_protocol.internal.serialize(message))

    def _is_stopword(self, word, language):
        if language == "en":
            return word in self.en_stopwords
        elif language == "fr":
            return word in self.fr_stopwords
        else:
            return False

    def process_message(self, message, ack, nack):
        logging.info("Process message")
        fields = message_protocol.internal.deserialize(message) # TODO: Validar protocolo
        if len(fields) == 6:
            self._split_and_send(*fields)
        elif len(fields) == 1:
            logging.info("Received EOF")
            self.output_queue.send(message)
        else:
            nack()
            return
        ack()

    def close(self):
        for mom in [self.input_exchange, self.output_queue]:
            try:
                mom.close()
            except Exception as e:
                logging.error(f"Error closing message middleware: {e}")

    def start(self):
        try:
            self.input_exchange.start_consuming(self.process_message)
        finally:
            self.close()

    def sigterm_handler(self, signum, frame):
        if self.end_event.is_set():
            return
        self.end_event.set()
        self.input_exchange.stop_consuming_threadsafe()

def main():
    logging.basicConfig(level=logging.INFO)
    splitter = Splitter()
    splitter.start()
    return 0


if __name__ == "__main__":
    main()
