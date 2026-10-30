import json
import logging
import os
import socket

from common import middleware, message_protocol


MOM_HOST = os.environ["MOM_HOST"]
HANDLER_HOST = os.environ["HANDLER_HOST"]
HANDLER_PORT = int(os.environ["HANDLER_PORT"])
FILTER_PREFIX = os.environ["FILTER_PREFIX"]
FILTER_AMOUNT = int(os.environ["FILTER_AMOUNT"])
SPLITTER_PREFIX = os.environ["SPLITTER_PREFIX"]
SPLITTER_AMOUNT = int(os.environ["SPLITTER_AMOUNT"])


class Worker:
    def __init__(self, client_socket, client_id, sigterm_received):
        self.client_socket = client_socket
        self.client_id = client_id
        self.sigterm_received = sigterm_received
        self.article_index = 0

    def start(self):
        try:
            if self.sigterm_received.value:
                return
            self.send_response(message_protocol.external.MsgType.CLIENT_ID, self.client_id)
            self.send_response(
                message_protocol.external.MsgType.HANDLER_ADDRESS,
                HANDLER_HOST, HANDLER_PORT,
            )
            self.filter_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, FILTER_PREFIX, []
            )
            self.splitter_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, SPLITTER_PREFIX, []
            )
            while not self.sigterm_received.value:
                message = message_protocol.external.recv_msg(self.client_socket)
                self.process_message(message)
                self.send_response(message_protocol.external.MsgType.ACK)
                if message[0] == message_protocol.external.MsgType.END_OF_RECORDS:
                    break
        except (OSError, EOFError) as error:
            if not self.sigterm_received.value:
                logging.error("Client %s disconnected: %s", self.client_id, error)
        except Exception:
            logging.exception("Could not process client %s", self.client_id)
        finally:
            self.shutdown()

    def process_message(self, message):
        message_type, payload = message
        if message_type == message_protocol.external.MsgType.ARTICLE:
            self._send_article(payload)
        elif message_type == message_protocol.external.MsgType.END_OF_RECORDS:
            self._send_end_of_records()
        else:
            raise ValueError("Expected an article or end of records")

    def _send_article(self, payload):
        filtered_article, splitter_article = self.preprocess_article(self.parse_article(payload))
        filtered = message_protocol.internal.serialize_msg(
            message_protocol.internal.MsgType.FILTER_ARTICLE,
            self.client_id, *filtered_article,
        )
        split = message_protocol.internal.serialize_msg(
            message_protocol.internal.MsgType.SPLITTER_ARTICLE,
            self.client_id, *splitter_article,
        )
        self.filter_exchange.send_to(
            filtered, f"{FILTER_PREFIX}_{self.article_index % FILTER_AMOUNT}"
        )
        self.splitter_exchange.send_to(
            split, f"{SPLITTER_PREFIX}_{self.article_index % SPLITTER_AMOUNT}"
        )
        self.article_index += 1

    def _send_end_of_records(self):
        end_of_records = message_protocol.internal.serialize_msg(
            message_protocol.internal.MsgType.END_OF_RECORDS, self.client_id,
        )
        for index in range(FILTER_AMOUNT):
            self.filter_exchange.send_to(end_of_records, f"{FILTER_PREFIX}_{index}")
        for index in range(SPLITTER_AMOUNT):
            self.splitter_exchange.send_to(end_of_records, f"{SPLITTER_PREFIX}_{index}")

    def send_response(self, response, *fields):
        message_protocol.external.send_msg(self.client_socket, response, *fields)

    def shutdown(self):
        for attribute in ("filter_exchange", "splitter_exchange"):
            mom = getattr(self, attribute, None)
            if mom is not None:
                try:
                    mom.close()
                except Exception:
                    logging.exception("Could not close a gateway output connection")
                finally:
                    setattr(self, attribute, None)
        try:
            self.client_socket.shutdown(socket.SHUT_RDWR)
        except OSError as error:
            logging.debug("Client %s already disconnected: %s", self.client_id, error)
        finally:
            self.client_socket.close()

    @staticmethod
    def parse_article(payload):
        article = json.loads(payload.decode("utf-8"))
        if not isinstance(article, dict):
            raise ValueError("ARTICLE must contain a JSON object")
        return article

    @staticmethod
    def article_text(article, field, default=None):
        value = article.get(field)
        if value is None:
            value = default
        if not isinstance(value, str):
            raise ValueError(f"Article field {field} must be text")
        return value

    @classmethod
    def section_names(cls, sections):
        if sections is None:
            return ()
        if not isinstance(sections, list):
            raise ValueError("Article sections must be a list")
        names = []
        cls._collect_section_names(sections, names)
        return tuple(names)

    @classmethod
    def _collect_section_names(cls, sections, names):
        for section in sections:
            if not isinstance(section, dict):
                raise ValueError("Article section must be an object")
            if section.get("type") != "section":
                continue
            name = section.get("name")
            if name is not None:
                names.append(cls.article_text(section, "name"))
            parts = section.get("has_parts") or []
            if not isinstance(parts, list):
                raise ValueError("Section has_parts must be a list")
            cls._collect_section_names(parts, names)

    @classmethod
    def preprocess_article(cls, article):
        name = cls.article_text(article, "name")
        url = cls.article_text(article, "url")
        date_modified = cls.article_text(article, "date_modified")
        language = article.get("in_language")
        if not isinstance(language, dict):
            raise ValueError("Article in_language must contain an identifier")
        language = cls.article_text(language, "identifier")
        image = article.get("image")
        if image is not None and not isinstance(image, dict):
            raise ValueError("Article image must be an object")
        image_url = cls.article_text(image or {}, "content_url", "")
        filtered = (
            name, url, language, date_modified, image_url,
            cls.section_names(article.get("sections")),
        )
        split = (
            url, language, date_modified, cls.article_text(article, "abstract", ""),
        )
        return filtered, split
