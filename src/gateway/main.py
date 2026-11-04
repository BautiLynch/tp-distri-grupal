import logging
import multiprocessing
import os
import signal
import socket

from common import middleware, message_protocol

from helpers import preprocess_article, parse_article

SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])
MOM_HOST = os.environ["MOM_HOST"]
FILTER_PREFIX = os.environ["FILTER_PREFIX"]
FILTER_AMOUNT = int(os.environ["FILTER_AMOUNT"])
SPLITTER_PREFIX = os.environ["SPLITTER_PREFIX"]
SPLITTER_AMOUNT = int(os.environ["SPLITTER_AMOUNT"])
HANDLER_HOST = os.environ["HANDLER_HOST"]
HANDLER_PORT = int(os.environ["HANDLER_PORT"])

if FILTER_AMOUNT < 1:
    raise ValueError("FILTER_AMOUNT must be greater than zero")
if SPLITTER_AMOUNT < 1:
    raise ValueError("SPLITTER_AMOUNT must be greater than zero")

def handle_client_request(client_socket, client_id, client_dict, sigterm_received):
    filter_exchange = None
    splitter_exchange = None
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
            MOM_HOST, FILTER_PREFIX, [f"{index}" for index in range(FILTER_AMOUNT)], consume=False
        )
        splitter_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SPLITTER_PREFIX, [f"{index}" for index in range(SPLITTER_AMOUNT)], consume=False
        )
        while True:
            message_type, article = message_protocol.external.recv_msg(client_socket)
            if message_type not in (
                message_protocol.external.MsgType.ARTICLE,
                message_protocol.external.MsgType.END_OF_RECORDS,
            ):
                raise ValueError("Expected an article or end of records")
            if message_type == message_protocol.external.MsgType.ARTICLE:
                filtered_article, splitter_article = preprocess_article(parse_article(article))
                filtered = message_protocol.internal.serialize_msg(
                    message_protocol.internal.MsgType.FILTER_ARTICLE,
                    client_id, *filtered_article,
                )
                filter_exchange.send_to(filtered, str(article_index % FILTER_AMOUNT))
                if splitter_article is not None:
                    split = message_protocol.internal.serialize_msg(
                        message_protocol.internal.MsgType.SPLITTER_ARTICLE,
                        client_id, *splitter_article,
                    )
                    splitter_exchange.send_to(split, str(article_index % SPLITTER_AMOUNT))
                article_index += 1
            else:
                end_of_records = message_protocol.internal.serialize_msg(
                    message_protocol.internal.MsgType.END_OF_RECORDS,
                    client_id,
                )
                filter_exchange.send(end_of_records)
                splitter_exchange.send(end_of_records)
                    
            # message_protocol.external.send_msg(
            #     client_socket, message_protocol.external.MsgType.ACK # TODO: Esta bien que el cliente necesite un ACK? 
            # )
            if message_type == message_protocol.external.MsgType.END_OF_RECORDS:
                break

    except (socket.error, EOFError) as e:
        if not sigterm_received.value:
            logging.error("Client %s disconnected: %s", client_id, e)
    except Exception as e:
        logging.error("Client %s: %s", client_id, e)
    finally:
        for mom in (filter_exchange, splitter_exchange):
            if mom is not None:
                try:
                    mom.close()
                except Exception:
                    logging.exception("Could not close a gateway output connection")
        try:
            client_socket.shutdown(socket.SHUT_RDWR)
        except socket.error as e:
            pass
        client_socket.close()
        client_dict.pop(client_id, None)


def handle_sigterm(server_socket, client_dict, sigterm_received): # TODO: Se supone que el shutdown se hace en el "finally" del "start"
    sigterm_received.value = 1
    for connection in [server_socket, *client_dict.values()]:
        try:
            connection.shutdown(socket.SHUT_RDWR)
        except socket.error as e:
            pass


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with multiprocessing.Manager() as manager:
        client_dict = manager.dict()
        sigterm_received = manager.Value("c_short", 0)
        with multiprocessing.Pool(processes=os.process_cpu_count()) as processes_pool: # TODO: Hace falta crear tantos procesos?
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
                server_socket.bind((SERVER_HOST, SERVER_PORT))
                server_socket.listen()
                signal.signal(
                    signal.SIGTERM,
                    lambda signum, frame: handle_sigterm(
                        server_socket, client_dict, sigterm_received
                    ),
                )
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
