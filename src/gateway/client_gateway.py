import logging
import os
import socket
from uuid import uuid4

from common import middleware, message_protocol
from common.message_protocol.fields import encode_fields
from common.storage.wal import WriteAheadLog
from helpers import preprocess_article, parse_article

MOM_HOST = os.environ["MOM_HOST"]
FILTER_PREFIX = os.environ["FILTER_PREFIX"]
FILTER_AMOUNT = int(os.environ["FILTER_AMOUNT"])
SPLITTER_PREFIX = os.environ["SPLITTER_PREFIX"]
SPLITTER_AMOUNT = int(os.environ["SPLITTER_AMOUNT"])
HANDLER_HOST = os.environ["HANDLER_HOST"]
HANDLER_PORT = int(os.environ["HANDLER_PORT"])
STORAGE_PATH = os.environ.get("STORAGE_PATH", "storage")


def publish_eof(wal, message, filter_exchange, splitter_exchange):
    wal.save(encode_fields("EOF_R", message))
    filter_exchange.send(message)
    splitter_exchange.send(message)
    wal.mark_processed()
    wal.save(encode_fields("EOF_S"))
    wal.mark_processed()

class ClientGateway:
    def __init__(self, client_socket, client_id, sigterm_received):
        self.client_socket = client_socket
        self.client_id = client_id
        self.sigterm_received = sigterm_received
        self.filter_exchange = None
        self.splitter_exchange = None
        self.wal = None
        self.disconnected = False
        self.article_index = 0

    def run(self):
        if self.sigterm_received.value:
            return
        self.wal = WriteAheadLog(f"{STORAGE_PATH}/gateway_client_{self.client_id}.wal")
        self.filter_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, FILTER_PREFIX, [str(index) for index in range(FILTER_AMOUNT)], consume=False
        )
        self.splitter_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SPLITTER_PREFIX, [str(index) for index in range(SPLITTER_AMOUNT)], consume=False
        )
        self.send_client_info()
        while not self.sigterm_received.value:
            message_type, fields = self.receive_message()
            if message_type == message_protocol.external.MsgType.ARTICLE:
                article, = fields
                self.process_article(article)
            elif message_type == message_protocol.external.MsgType.END_OF_RECORDS:
                self.send_eof(False)
                message_protocol.external.send_msg(self.client_socket, message_protocol.external.MsgType.ACK)
                return
            else:
                raise ValueError("Expected an article or end of records")

    def send_client_info(self):
        try:
            message_protocol.external.send_msg(
                self.client_socket, message_protocol.external.MsgType.CLIENT_ID, self.client_id
            )
            message_protocol.external.send_msg(
                self.client_socket, message_protocol.external.MsgType.HANDLER_ADDRESS,
                HANDLER_HOST, HANDLER_PORT,
            )
        except OSError:
            self.disconnected = True
            raise

    def receive_message(self):
        try:
            return message_protocol.external.recv_msg(self.client_socket)
        except (OSError, EOFError):
            self.disconnected = True
            raise

    def process_article(self, article):
        filtered_article, splitter_article = preprocess_article(parse_article(article))
        message_id = uuid4().bytes
        filtered = message_protocol.internal.serialize_msg(
            message_protocol.internal.MsgType.FILTER_ARTICLE,
            self.client_id, message_id, "gateway", message_id, *filtered_article,
        )
        self.filter_exchange.send_to(filtered, str(self.article_index % FILTER_AMOUNT))
        if splitter_article is not None:
            message_id = uuid4().bytes
            split = message_protocol.internal.serialize_msg(
                message_protocol.internal.MsgType.SPLITTER_ARTICLE,
                self.client_id, message_id, "gateway", message_id, *splitter_article,
            )
            self.splitter_exchange.send_to(split, str(self.article_index % SPLITTER_AMOUNT))
        self.article_index += 1

    def send_eof(self, disconnected):
        message_id = uuid4().bytes
        message = message_protocol.internal.serialize_msg(
            message_protocol.internal.MsgType.END_OF_RECORDS,
            self.client_id, message_id, "gateway", message_id, disconnected,
        )
        publish_eof(self.wal, message, self.filter_exchange, self.splitter_exchange)

    def close(self):
        for resource in (self.filter_exchange, self.splitter_exchange, self.wal):
            if resource is not None:
                try:
                    resource.close()
                except Exception:
                    logging.exception("Could not close a gateway client resource")
        try:
            self.client_socket.shutdown(socket.SHUT_RDWR)
        except OSError as error:
            logging.debug("Client already disconnected: %s", error)
        self.client_socket.close()


