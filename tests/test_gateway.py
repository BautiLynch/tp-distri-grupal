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
        "SERVER_PORT": "12345",
        "MOM_HOST": "rabbitmq",
        "FILTER_PREFIX": "filter_q1",
        "FILTER_AMOUNT": "3",
        "SPLITTER_QUEUE": "splitter_queue",
    }
)

from gateway import main as gateway_main


class FakeFilterExchange:
    def __init__(self):
        self.sent = []
        self.closed = False

    def send_rk(self, message, routing_key):
        self.sent.append((message, routing_key))

    def close(self):
        self.closed = True


class FakeSplitterQueue:
    def __init__(self):
        self.sent = []
        self.closed = False

    def send(self, message):
        self.sent.append(message)

    def close(self):
        self.closed = True


class FakeSocket:
    def shutdown(self, *args):
        pass

    def close(self):
        pass


class FakeSignal:
    value = 0


class GatewayTests(unittest.TestCase):
    def test_filter_article_fields_keeps_q1_fields(self):
        article = {
            "abstract": "not for q1",
            "creation_date": "2019-01-01",
            "date_modified": "2024-01-01",
            "image": "image-ref",
            "in_language": "en",
            "name": "Article",
            "sections": "History",
            "url": "http://example.com/article",
        }

        filtered = gateway_main.filter_article_fields(
            article, gateway_main.FILTER_FIELDS
        )

        self.assertEqual(
            filtered,
            {
                "creation_date": "2019-01-01",
                "date_modified": "2024-01-01",
                "image": "image-ref",
                "in_language": "en",
                "name": "Article",
                "sections": "History",
                "url": "http://example.com/article",
            },
        )

    def test_filter_routing_key_round_robins_per_article(self):
        self.assertEqual(gateway_main.filter_routing_key(0), "filter_q1_0")
        self.assertEqual(gateway_main.filter_routing_key(1), "filter_q1_1")
        self.assertEqual(gateway_main.filter_routing_key(2), "filter_q1_2")
        self.assertEqual(gateway_main.filter_routing_key(3), "filter_q1_0")

    def test_filter_routing_keys_covers_every_instance(self):
        self.assertEqual(
            gateway_main.filter_routing_keys(),
            ["filter_q1_0", "filter_q1_1", "filter_q1_2"],
        )

    def test_round_robins_articles_and_broadcasts_eof(self):
        articles = [
            {
                "url": f"http://example.com/{index}",
                "creation_date": "2019-01-01",
                "date_modified": "2024-01-01",
                "in_language": "en",
            }
            for index in range(3)
        ]
        responses = [
            (gateway_main.message_protocol.external.MsgType.ARTICLE, article)
            for article in articles
        ]
        responses.append(
            (gateway_main.message_protocol.external.MsgType.END_OF_RECORDS, None)
        )
        filter_exchange = FakeFilterExchange()
        splitter_queue = FakeSplitterQueue()
        external_sent = []

        def recv_msg(_):
            return responses.pop(0)

        def send_msg(_, message_type, *args):
            external_sent.append((message_type, args))

        with mock.patch.object(
            gateway_main.middleware,
            "MessageMiddlewareExchangeRabbitMQ",
            return_value=filter_exchange,
        ), mock.patch.object(
            gateway_main.middleware,
            "MessageMiddlewareQueueRabbitMQ",
            return_value=splitter_queue,
        ), mock.patch.object(
            gateway_main.message_protocol.external,
            "recv_msg",
            side_effect=recv_msg,
        ), mock.patch.object(
            gateway_main.message_protocol.external,
            "send_msg",
            side_effect=send_msg,
        ):
            gateway_main.handle_client_request(FakeSocket(), 7, {}, FakeSignal())

        external = gateway_main.message_protocol.external
        internal = gateway_main.message_protocol.internal
        self.assertEqual(
            external_sent[0], (external.MsgType.CLIENT_ID, (7,))
        )
        self.assertEqual(
            [message_type for message_type, _ in external_sent[1:]],
            [external.MsgType.ACK] * 4,
        )

        filter_articles = filter_exchange.sent[:3]
        self.assertEqual(
            [routing_key for _, routing_key in filter_articles],
            ["filter_q1_0", "filter_q1_1", "filter_q1_2"],
        )
        for (message, _), article in zip(filter_articles, articles):
            message_type, client_id, decoded = internal.deserialize_input(message)
            self.assertEqual(message_type, internal.MsgType.ARTICLE)
            self.assertEqual(client_id, 7)
            self.assertEqual(decoded["url"], article["url"])

        filter_eof = filter_exchange.sent[3:]
        self.assertEqual(
            [routing_key for _, routing_key in filter_eof],
            ["filter_q1_0", "filter_q1_1", "filter_q1_2"],
        )
        for message, _ in filter_eof:
            message_type, client_id, article = internal.deserialize_input(message)
            self.assertEqual(message_type, internal.MsgType.END_OF_RECORDS)
            self.assertEqual(client_id, 7)
            self.assertIsNone(article)

        self.assertEqual(len(splitter_queue.sent), 4)
        self.assertTrue(filter_exchange.closed)
        self.assertTrue(splitter_queue.closed)


if __name__ == "__main__":
    unittest.main()
