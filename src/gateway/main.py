import json
import logging
import multiprocessing
import os
import signal
import socket

from common import middleware, message_protocol

SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])
MOM_HOST = os.environ["MOM_HOST"]
FILTER_PREFIX = os.environ["FILTER_PREFIX"]
FILTER_AMOUNT = int(os.environ["FILTER_AMOUNT"])
SPLITTER_QUEUE = os.environ["SPLITTER_QUEUE"]
HANDLER_HOST = os.environ["HANDLER_HOST"]
HANDLER_PORT = int(os.environ["HANDLER_PORT"])

if FILTER_AMOUNT < 1:
    raise ValueError("FILTER_AMOUNT must be greater than zero")

FILTER_FIELDS = (
    "date_modified",
    "image",
    "in_language",
    "language",
    "modification_date",
    "name",
    "sections",
    "url",
)
SPLITTER_FIELDS = ("abstract", "date_modified", "in_language", "name", "sections", "url")


def filter_article_fields(article, fields):
    filtered_article = {}
    for name, value in article.items():
        if name in fields:
            filtered_article[name] = value
    return filtered_article


def parse_article_line(line):
    article = json.loads(line.decode("utf-8"))
    if not isinstance(article, dict):
        raise ValueError("Each dataset line must contain a JSON object")
    return article


def filter_routing_key(index):
    return f"{FILTER_PREFIX}_{index % FILTER_AMOUNT}"


def filter_routing_keys():
    return [f"{FILTER_PREFIX}_{index}" for index in range(FILTER_AMOUNT)]


def handle_client_request(client_socket, client_id, client_dict, sigterm_received):
    filter_exchange = None
    splitter_queue = None
    article_index = 0
    try:
        message_protocol.external.send_msg(
            client_socket, message_protocol.external.MsgType.CLIENT_ID, client_id
        )
        message_protocol.external.send_msg(
            client_socket, message_protocol.external.MsgType.HANDLER_ADDRESS,
            HANDLER_HOST, HANDLER_PORT,
        )
        filter_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, FILTER_PREFIX, []
        )
        splitter_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, SPLITTER_QUEUE
        )
        while True:
            message_type, article = message_protocol.external.recv_msg(client_socket)
            if message_type not in (
                message_protocol.external.MsgType.ARTICLE,
                message_protocol.external.MsgType.ARTICLE_LINE,
                message_protocol.external.MsgType.END_OF_RECORDS,
            ):
                raise ValueError("Expected an article or end of records")
            if message_type == message_protocol.external.MsgType.ARTICLE_LINE:
                article = parse_article_line(article)
            if message_type != message_protocol.external.MsgType.END_OF_RECORDS:
                filtered = message_protocol.internal.serialize_article(
                    client_id, filter_article_fields(article, FILTER_FIELDS)
                )
                split = message_protocol.internal.serialize_article(
                    client_id, filter_article_fields(article, SPLITTER_FIELDS)
                )
                filter_exchange.send_rk(filtered, filter_routing_key(article_index))
                article_index += 1
                splitter_queue.send(split)
            else:
                end_of_records = message_protocol.internal.serialize_end_of_records(
                    client_id
                )
                for routing_key in filter_routing_keys():
                    filter_exchange.send_rk(end_of_records, routing_key)
                splitter_queue.send(end_of_records)
            message_protocol.external.send_msg(
                client_socket, message_protocol.external.MsgType.ACK
            )
            if message_type == message_protocol.external.MsgType.END_OF_RECORDS:
                break
    except (socket.error, EOFError) as e:
        if not sigterm_received.value:
            logging.error("Client %s disconnected: %s", client_id, e)
    except Exception as e:
        logging.error("Client %s: %s", client_id, e)
    finally:
        for mom in (filter_exchange, splitter_queue):
            if mom is not None:
                try:
                    mom.close()
                except Exception:
                    logging.exception("Could not close a gateway output connection")
        try:
            client_socket.shutdown(socket.SHUT_RDWR)
        except socket.error as e:
            logging.debug("Client %s already disconnected: %s", client_id, e)
        client_socket.close()
        client_dict.pop(client_id, None)


def handle_sigterm(server_socket, client_dict, sigterm_received):
    sigterm_received.value = 1
    for connection in [server_socket, *client_dict.values()]:
        try:
            connection.shutdown(socket.SHUT_RDWR)
        except socket.error as e:
            logging.debug("Socket already disconnected during shutdown: %s", e)


def main():
    logging.basicConfig(level=logging.INFO)

    with multiprocessing.Manager() as manager:
        client_dict = manager.dict()
        sigterm_received = manager.Value("c_short", 0)
        with multiprocessing.Pool(processes=os.process_cpu_count()) as processes_pool:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
                server_socket.bind((SERVER_HOST, SERVER_PORT))
                server_socket.listen()
                signal.signal(
                    signal.SIGTERM,
                    lambda signum, frame: handle_sigterm(
                        server_socket, client_dict, sigterm_received
                    ),
                )
                logging.info("Listening to connections")
                client_id = 0
                try:
                    while True:
                        try:
                            client_socket, _ = server_socket.accept()

                            logging.info("A new client has connected")
                            client_id += 1
                            client_dict[client_id] = client_socket
                            processes_pool.apply_async(
                                handle_client_request,
                                (client_socket, client_id, client_dict, sigterm_received),
                                callback=lambda _, connection=client_socket: connection.close(),
                            )
                        except socket.error:
                            if sigterm_received.value == 0:
                                logging.exception("The connection with the client was lost")
                                return 1
                            else:
                                return 0
                        except Exception:
                            logging.exception("Error accepting a client")
                            return 2
                finally:
                    handle_sigterm(server_socket, client_dict, sigterm_received)
                    processes_pool.close()
                    processes_pool.join()
    return 0


if __name__ == "__main__":
    main()
