import logging
import os
import signal
import socket

from common import middleware
from common.message_protocol.internal import deserialize_msg, MsgType as InternalMsgType
from common.message_protocol.external import recv_msg, send_msg, MsgType as ExternalMsgType
from common.message_protocol.queries import QueryId

MOM_HOST = os.environ["MOM_HOST"]
INPUT_EXCHANGE = os.environ["INPUT_EXCHANGE"]

QUERY_REPLICAS = {
    query_id: int(os.environ[f"QUERY_{query_id.value}_REPLICAS"])
    for query_id in QueryId
}
if any(amount < 1 for amount in QUERY_REPLICAS.values()):
    raise ValueError("Every QUERY_<id>_REPLICAS must be greater than zero")


class ClientHandler:
    def __init__(self, client_socket):
        self.client_socket = client_socket
        self.client_id = None
        self.input_exchange = None
        self.query_end_counts = {query_id: 0 for query_id in QueryId}
        self.completed_queries = set()
        self.finished = False
        self.stopping = False
        self.last_received = {}
        signal.signal(signal.SIGTERM, self.handle_sigterm)

    def run(self):
        if self.stopping:
            return
        message_type, fields = recv_msg(self.client_socket)
        if message_type != ExternalMsgType.CLIENT_ID:
            raise ValueError("Expected a client ID to register the connection")
        self.client_id, = fields
        if self.stopping:
            return
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, INPUT_EXCHANGE, [str(self.client_id)], consume=True
        )
        if self.stopping:
            return
        self.send_message(ExternalMsgType.ACK)
        self.input_exchange.start_consuming(self.handle_result)

    def send_message(self, message_type, *fields):
        try:
            send_msg(self.client_socket, message_type, *fields)
        except OSError:
            self.stopping = True
            raise

    def handle_result(self, message, ack, nack):
        try:
            message_type, fields = deserialize_msg(message)
            client_id, message_id, producer_id, key, *body = fields
            producer = (client_id, producer_id)
            if self.last_received.get(producer) == key:
                ack()
                return
            if message_type == InternalMsgType.QUERY_END:
                query_id = QueryId(body[0])
                self.query_end_counts[query_id] += 1
                if self.query_end_counts[query_id] == QUERY_REPLICAS[query_id]:
                    logging.info(
                        "Client %s completed query %s", self.client_id, query_id.name,
                    )
                    self.completed_queries.add(query_id)
                self.finished = len(self.completed_queries) == len(QUERY_REPLICAS)
                if self.finished:
                    self.send_message(ExternalMsgType.END_OF_RECORDS, False)
            elif message_type == InternalMsgType.RESULT:
                self.send_message(ExternalMsgType.RESULT, *body)
            else:
                raise ValueError(f"Unexpected handler message type: {message_type}")
        except Exception:
            nack()
            raise
        self.last_received[producer] = key
        ack()
        if self.finished:
            self.input_exchange.stop_consuming()

    def handle_sigterm(self, signum, frame):
        if self.stopping:
            return
        self.stopping = True
        self.shutdown_socket()
        if self.input_exchange is not None:
            self.input_exchange.stop_consuming_threadsafe()

    def shutdown_socket(self):
        try:
            self.client_socket.shutdown(socket.SHUT_RDWR)
        except socket.error as error:
            logging.debug("Client already disconnected: %s", error)

    def close(self):
        self.stopping = True
        if self.input_exchange is not None:
            try:
                self.input_exchange.close()
            except Exception:
                logging.exception("Could not close the client's result connection")
        self.shutdown_socket()
        self.client_socket.close()


