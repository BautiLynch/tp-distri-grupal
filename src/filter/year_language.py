"""Entidad filter de la query 1: idioma y anio de escritura.

Entrada por articulo (5 campos):
    [client_id, url, creation_date, modification_date, language]

Salida al Handler (3 campos), en cuanto llega cada articulo que cumple:
    [client_id, url, modification_date]

EOF de entrada del cliente:
    [client_id]

EOF de salida (2 campos, incluye la instancia que lo reenvia):
    [client_id, filter_id]
"""

import logging
import os
import signal
import threading

from common import message_protocol, middleware

LEN_Q1 = 5

def _year(value):
    text = str(value).strip()
    if len(text) < 4 or not text[:4].isdigit():
        return None
    if len(text) > 4 and text[4].isdigit():
        return None
    return int(text[:4])


class YearLanguageFilter:
    role = "q1"

    def __init__(
        self,
        env=None,
        input_exchange=None,
        output_queue=None,
        install_signal_handlers=True,
    ):
        env = env or os.environ
        self.mom_host = self._required(env, "MOM_HOST")
        self.prefix = self._required(env, "FILTER_PREFIX")
        self.output_queue_name = self._required(env, "OUTPUT_QUEUE")

        self.amount = int(env.get("FILTER_AMOUNT", "1"))
        self.id = int(env.get("FILTER_ID", "0"))
        if self.amount < 1:
            raise ValueError("FILTER_AMOUNT must be greater than zero")
        if not 0 <= self.id < self.amount:
            raise ValueError(
                f"FILTER_ID must be in [0, {self.amount - 1}], got {self.id}"
            )

        self.languages = {
            language.strip().lower()
            for language in str(env.get("LANGUAGES", "en")).split(",")
            if language.strip()
        }
        if not self.languages:
            raise ValueError("LANGUAGES must contain at least one language")

        self.max_year = int(env.get("MAX_YEAR", "2020"))

        self.end_event = threading.Event()
        self.input_exchange = input_exchange or middleware.MessageMiddlewareExchangeRabbitMQ(
            self.mom_host, self.prefix, [f"{self.prefix}_{self.id}"]
        )
        self.output_queue = output_queue or middleware.MessageMiddlewareQueueRabbitMQ(
            self.mom_host, self.output_queue_name
        )

        if install_signal_handlers:
            signal.signal(signal.SIGTERM, self.sigterm_handler)

    def process_message(self, message, ack, nack):
        try:
            fields = message_protocol.internal.deserialize(message)
            if not isinstance(fields, (list, tuple)) or not fields:
                raise ValueError("message must be a non-empty list")

            if len(fields) == LEN_Q1:
                self._filter(*fields)
            elif len(fields) == 1:
                logging.info("Received EOF")
                self.send([fields[0], self.id])
            else:
                raise ValueError(
                    f"expected {LEN_Q1} article fields or 1 EOF field, "
                    f"got {len(fields)}"
                )
        except Exception:
            logging.exception("Error processing message")
            nack()
            return

        ack()

    def _filter(self, client_id, url, creation_date, modification_date, language):
        if str(language).strip().lower() not in self.languages:
            return

        creation_year = _year(creation_date)
        if creation_year is None:
            logging.warning(
                "Discarding article %s: invalid creation date %r",
                url,
                creation_date,
            )
            return
        if creation_year > self.max_year:
            return

        if _year(modification_date) is None:
            logging.warning(
                "Discarding article %s: invalid modification date %r",
                url,
                modification_date,
            )
            return

        self.send([client_id, url, modification_date])

    def send(self, payload):
        self.output_queue.send(message_protocol.internal.serialize(payload))

    def start(self):
        logging.info(
            "YearLanguageFilter prefix=%s id=%s/%s output=%s languages=%s max_year=%s",
            self.prefix,
            self.id,
            self.amount,
            self.output_queue_name,
            ",".join(sorted(self.languages)),
            self.max_year,
        )
        try:
            self.input_exchange.start_consuming(self.process_message)
        finally:
            self.close()

    def sigterm_handler(self, signum, frame):
        if self.end_event.is_set():
            return
        self.end_event.set()
        self.input_exchange.stop_consuming_threadsafe()

    def close(self):
        for mom in (self.input_exchange, self.output_queue):
            try:
                mom.close()
            except Exception as e:
                logging.error("Error closing message middleware: %s", e)

    @staticmethod
    def _required(env, name):
        value = env.get(name)
        if value is None or not str(value).strip():
            raise ValueError(f"Missing required environment variable: {name}")
        return str(value).strip()
