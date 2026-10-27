import os
import socket
import sys
import unittest

sys.path.insert(
    0,
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"),
)

from common import message_protocol


class BytesReader:
    def __init__(self, data):
        self.data = data
        self.offset = 0

    def recv(self, size):
        chunk = self.data[self.offset:self.offset + size]
        self.offset += len(chunk)
        return chunk


class ProtocolTests(unittest.TestCase):
    def test_internal_article_round_trip(self):
        article = {
            "url": "http://example.com/article",
            "date_modified": "2020-01-01",
            "in_language": "en",
        }
        body = message_protocol.internal.serialize_article(7, article)

        self.assertEqual(
            message_protocol.internal.deserialize_input(body),
            (message_protocol.internal.MsgType.ARTICLE, 7, article),
        )

    def test_internal_end_of_records_round_trip(self):
        body = message_protocol.internal.serialize_end_of_records(7)

        self.assertEqual(
            message_protocol.internal.deserialize_input(body),
            (message_protocol.internal.MsgType.END_OF_RECORDS, 7, None),
        )

    def test_internal_result_round_trip(self):
        article = {
            "url": "http://example.com/article",
            "date_modified": "2020-01-01",
        }
        body = message_protocol.internal.serialize_result(
            7,
            1,
            message_protocol.external.MsgType.ARTICLE,
            article,
        )

        message_type, client_id, query_id, payload = (
            message_protocol.internal.deserialize_result(body)
        )
        external_type, decoded = message_protocol.external.recv_msg(
            BytesReader(payload)
        )

        self.assertEqual(message_type, message_protocol.internal.MsgType.RESULT)
        self.assertEqual(client_id, 7)
        self.assertEqual(query_id, 1)
        self.assertEqual(external_type, message_protocol.external.MsgType.ARTICLE)
        self.assertEqual(decoded, article)

    def test_internal_query_end_round_trip(self):
        body = message_protocol.internal.serialize_query_end(7, 1)

        self.assertEqual(
            message_protocol.internal.deserialize_result(body),
            (message_protocol.internal.MsgType.QUERY_END, 7, 1, b""),
        )

    def test_external_round_trip(self):
        left, right = socket.socketpair()
        try:
            message_protocol.external.send_msg(
                left,
                message_protocol.external.MsgType.CLIENT_ID,
                7,
            )
            self.assertEqual(
                message_protocol.external.recv_msg(right),
                (message_protocol.external.MsgType.CLIENT_ID, 7),
            )

            article = {
                "url": "http://example.com/article",
                "date_modified": "2020-01-01",
            }
            message_protocol.external.send_msg(
                left,
                message_protocol.external.MsgType.ARTICLE,
                article,
            )
            self.assertEqual(
                message_protocol.external.recv_msg(right),
                (message_protocol.external.MsgType.ARTICLE, article),
            )

            message_protocol.external.send_msg(
                left, message_protocol.external.MsgType.ACK
            )
            self.assertEqual(
                message_protocol.external.recv_msg(right),
                (message_protocol.external.MsgType.ACK, None),
            )

            message_protocol.external.send_msg(
                left, message_protocol.external.MsgType.END_OF_RECORDS
            )
            self.assertEqual(
                message_protocol.external.recv_msg(right),
                (message_protocol.external.MsgType.END_OF_RECORDS, None),
            )
        finally:
            left.close()
            right.close()


if __name__ == "__main__":
    unittest.main()
