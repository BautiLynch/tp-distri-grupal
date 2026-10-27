import os
import sys
import unittest
from unittest import mock

sys.path.insert(
    0,
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"),
)

os.environ.update(
    {
        "SERVER_HOST": "127.0.0.1",
        "SERVER_PORT": "12346",
        "MOM_HOST": "rabbitmq",
        "INPUT_EXCHANGE": "handler_exchange",
    }
)

from common import message_protocol
from handler import main as handler_main


class BytesReader:
    def __init__(self, data):
        self.data = data
        self.offset = 0

    def recv(self, size):
        chunk = self.data[self.offset:self.offset + size]
        self.offset += len(chunk)
        return chunk


class FakeSocket:
    def __init__(self):
        self.sent = []

    def send(self, data):
        self.sent.append(bytes(data))
        return len(data)

    def shutdown(self, *args):
        pass

    def close(self):
        pass


class FakeExchange:
    def __init__(self):
        self.stopped = False

    def stop_consuming(self):
        self.stopped = True


def build_handler(replicas=2):
    with mock.patch.dict(os.environ, {"QUERY_REPLICAS": str(replicas)}):
        handler = handler_main.ClientHandler(FakeSocket())
    handler.input_exchange = FakeExchange()
    return handler


def process(handler, message):
    acked = []
    nacked = []
    handler.handle_result(
        message,
        lambda: acked.append(True),
        lambda: nacked.append(True),
    )
    return acked, nacked


def query_end(client_id, query_id=1):
    return message_protocol.internal.serialize_query_end(client_id, query_id)


class HandlerTests(unittest.TestCase):
    def test_waits_for_all_q1_replicas(self):
        handler = build_handler(replicas=2)

        process(handler, query_end(1))
        self.assertEqual(handler.query_end_count, 1)
        self.assertFalse(handler.finished)
        self.assertEqual(handler.client_socket.sent, [])

        process(handler, query_end(1))
        self.assertEqual(handler.query_end_count, 2)
        self.assertTrue(handler.finished)
        self.assertEqual(len(handler.client_socket.sent), 1)

    def test_sends_end_of_records_after_q1_replicas(self):
        handler = build_handler(replicas=2)

        process(handler, query_end(1))
        process(handler, query_end(1))

        self.assertTrue(handler.finished)
        self.assertEqual(len(handler.client_socket.sent), 1)
        message_type, payload = message_protocol.external.recv_msg(
            BytesReader(handler.client_socket.sent[0])
        )
        self.assertEqual(
            message_type, message_protocol.external.MsgType.END_OF_RECORDS
        )
        self.assertIsNone(payload)
        self.assertTrue(handler.input_exchange.stopped)

    def test_duplicate_query_end_does_not_send_end_twice(self):
        handler = build_handler(replicas=2)

        process(handler, query_end(1))
        process(handler, query_end(1))
        process(handler, query_end(1))

        self.assertEqual(len(handler.client_socket.sent), 1)

    def test_rejects_other_query_id(self):
        handler = build_handler(replicas=2)
        nacked = []

        with self.assertRaisesRegex(ValueError, "Unexpected query id"):
            handler.handle_result(
                query_end(1, query_id=2),
                lambda: None,
                lambda: nacked.append(True),
            )

        self.assertTrue(nacked)
        self.assertFalse(handler.finished)

    def test_result_is_forwarded_raw_to_the_client(self):
        handler = build_handler(replicas=2)
        article = {
            "url": "http://example.com/article",
            "date_modified": "2020-01-01",
        }
        message = message_protocol.internal.serialize_result(
            1,
            1,
            message_protocol.external.MsgType.ARTICLE,
            article,
        )

        acked, nacked = process(handler, message)

        self.assertTrue(acked)
        self.assertFalse(nacked)
        self.assertEqual(len(handler.client_socket.sent), 1)
        external_type, decoded = message_protocol.external.recv_msg(
            BytesReader(handler.client_socket.sent[0])
        )
        self.assertEqual(external_type, message_protocol.external.MsgType.ARTICLE)
        self.assertEqual(decoded, article)

    def test_invalid_query_replicas_is_rejected(self):
        with mock.patch.dict(os.environ, {"QUERY_REPLICAS": "0"}):
            with self.assertRaisesRegex(ValueError, "QUERY_REPLICAS"):
                handler_main.ClientHandler(FakeSocket())


if __name__ == "__main__":
    unittest.main()
