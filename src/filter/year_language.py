"""Entidad filter de la query 1: idioma y anio de modificacion.

Entrada por RabbitMQ (protocolo interno binario):
    ARTICLE: articulo como diccionario con url, fecha de modificacion e idioma.
    END_OF_RECORDS: fin del stream de un cliente.

Salida al Handler:
    RESULT con un payload external ARTICLE: {"url": ..., "date_modified": ...}
    QUERY_END cuando esta instancia termina de procesar la query para el cliente.
"""

import logging
import os
import signal
import threading

from common import message_protocol, middleware

QUERY_ID = 1


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
        output_exchange=None,
        install_signal_handlers=True,
    ):
        env = env or os.environ
        self.mom_host = self._required(env, "MOM_HOST")
        self.prefix = self._required(env, "FILTER_PREFIX")
        self.output_exchange_name = self._required(env, "OUTPUT_EXCHANGE")

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
        self.input_exchange = (
            input_exchange
            or middleware.MessageMiddlewareExchangeRabbitMQ(
                self.mom_host,
                self.prefix,
                [f"{self.prefix}_{self.id}"],
            )
        )
        self.output_exchange = (
            output_exchange
            or middleware.MessageMiddlewareExchangeRabbitMQ(
                self.mom_host, self.output_exchange_name, []
            )
        )

        if install_signal_handlers:
            signal.signal(signal.SIGTERM, self.sigterm_handler)

    def process_message(self, message, ack, nack):
        try:
            message_type, client_id, article = (
                message_protocol.internal.deserialize_input(message)
            )
            if message_type == message_protocol.internal.MsgType.ARTICLE:
                self._filter(client_id, article)
            elif message_type == message_protocol.internal.MsgType.END_OF_RECORDS:
                logging.info("Received EOF for client %s", client_id)
                self._finish(client_id)
            else:
                raise ValueError(f"Unexpected input message type: {message_type}")
        except Exception:
            logging.exception("Error processing message")
            nack()
            return

        ack()

    def _filter(self, client_id, article):
        if not isinstance(article, dict):
            raise ValueError("ARTICLE payload must be a dictionary")

        url = article.get("url")
        if not url:
            raise ValueError("ARTICLE is missing url")

        language = article.get("language") or article.get("in_language")
        if language is None:
            raise ValueError("ARTICLE is missing language")
        if str(language).strip().lower() not in self.languages:
            return

        modification_date = article.get("modification_date")
        if modification_date is None:
            modification_date = article.get("date_modified")
        if modification_date is None:
            raise ValueError("ARTICLE is missing modification date")

        modification_year = _year(modification_date)
        if modification_year is None:
            logging.warning(
                "Discarding article %s: invalid modification date %r",
                url,
                modification_date,
            )
            return
        if modification_year > self.max_year:
            return

        self._send_result(
            client_id,
            {"url": url, "date_modified": modification_date},
        )

    def _send_result(self, client_id, result):
        message = message_protocol.internal.serialize_result(
            client_id,
            QUERY_ID,
            message_protocol.external.MsgType.ARTICLE,
            result,
        )
        self.output_exchange.send_to(message, str(client_id))

    def _finish(self, client_id):
        message = message_protocol.internal.serialize_query_end(
            client_id, QUERY_ID
        )
        self.output_exchange.send_to(message, str(client_id))

    def start(self):
        logging.info(
            "YearLanguageFilter prefix=%s id=%s/%s output_exchange=%s languages=%s max_year=%s",
            self.prefix,
            self.id,
            self.amount,
            self.output_exchange_name,
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
        for mom in (self.input_exchange, self.output_exchange):
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
