import os
import logging
import socket
import signal
import multiprocessing
from common import middleware, message_protocol

SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])
MOM_HOST = os.environ["MOM_HOST"]
FILTER_QUEUE = os.environ["FILTER_QUEUE"]
SPLITTER_QUEUE = os.environ["SPLITTER_QUEUE"]

FITLER_FIELDS = ("date_modified", "image", "in_language", "name", "sections", "url")
SPLITTER_FIELDS = ("abstract", "date_modified", "in_language", "name", "sections", "url")


def filter_article_fields(article, fields):
    filtered_article = {}
    for name, value in article.items():
        if name in fields:
            filtered_article[name] = value
    return filtered_article


def handle_client_request(client_socket, client_id, client_dict, sigterm_received):
    filter_queue = None
    splitter_queue = None
    try:
        message_protocol.external.send_msg(
            client_socket, message_protocol.external.MsgType.CLIENT_ID, client_id
        )
        filter_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, FILTER_QUEUE)
        splitter_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, SPLITTER_QUEUE)
        while True:
            message_type, article = message_protocol.external.recv_msg(client_socket)
            if message_type not in (
                message_protocol.external.MsgType.ARTICLE,
                message_protocol.external.MsgType.END_OF_RECORDS,
            ):
                raise ValueError("Expected an article or end of records")
            if message_type == message_protocol.external.MsgType.ARTICLE:
                filter_queue.send(
                    message_protocol.internal.serialize_article(client_id, filter_article_fields(article, FITLER_FIELDS))
                )
                splitter_queue.send(
                    message_protocol.internal.serialize_article(client_id, filter_article_fields(article, SPLITTER_FIELDS))
                )
            else:
                end_of_records = message_protocol.internal.serialize_end_of_records(client_id)
                filter_queue.send(end_of_records)
                splitter_queue.send(end_of_records)
            message_protocol.external.send_msg(client_socket, message_protocol.external.MsgType.ACK)
            if message_type == message_protocol.external.MsgType.END_OF_RECORDS:
                break
    except (socket.error, EOFError) as e:
        if not sigterm_received.value:
            logging.error("Client %s disconnected: %s", client_id, e)
    except Exception as e:
        logging.error("Client %s: %s", client_id, e)
    finally:
        for queue in (filter_queue, splitter_queue):
            if queue is not None:
                try:
                    queue.close()
                except Exception:
                    logging.exception("Could not close a gateway output queue")
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
