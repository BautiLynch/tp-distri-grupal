import os
import logging
import signal
import threading

from common import middleware, message_protocol

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_EXCHAGE = os.environ["OUTPUT_EXCHAGE"]
FILTER_LANGUAGE_PREFIX = os.environ["FILTER_LANGUAGE_PREFIX"]
STORAGE_PATH = os.environ.get("STORAGE_PATH", "storage")
YEAR = os.environ.get("YEAR")


class FilterLanguage:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, FILTER_LANGUAGE_PREFIX, [f"{FILTER_LANGUAGE_PREFIX}_{ID}"]
        )
        self.output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ( 
            MOM_HOST, OUTPUT_EXCHAGE
        )

        self.words_by_client = {}
        self.end_event = threading.Event()
        signal.signal(signal.SIGTERM, self.sigterm_handler)

    def process_message(self, message, ack, nack):
        logging.info("Process message")
        fields = message_protocol.internal.deserialize(message) # TODO: Validar protocolo
        if len(fields) == 1:
            logging.info("Received EOF")
            self.output_exchange.send_to(message, routing_key=fields[0]) 

        elif len(fields) == 3:
            client_id, word, language = fields

            word_sent = self.words_by_client.get(client_id, {})
            language_sent = word_sent.get(word, set())

            if len(language_sent) == 2:
                pass # Ya la mandé

            elif language not in language_sent:
                language_sent.add(language)
                word_sent[word] = language_sent
                self.words_by_client[client_id] = word_sent

                if len(language_sent) == 2:
                    logging.info("Received word in both languages")
                    self.output_exchange.send_to([client_id, word], routing_key=client_id) 

        else:
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
    filter_average = FilterLanguage()
    filter_average.start()
    return 0


if __name__ == "__main__":
    main()