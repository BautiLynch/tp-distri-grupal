import os
import sys
import unittest
from unittest import mock

sys.path.insert(
    0,
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"),
)

from common import message_protocol
from filter import main as filter_main
from filter.year_language import YearLanguageFilter

BASE_ENV = {
    "MOM_HOST": "rabbitmq",
    "FILTER_PREFIX": "filter_q1",
    "OUTPUT_EXCHANGE": "handler_exchange",
    "FILTER_ID": "0",
    "FILTER_AMOUNT": "1",
    "LANGUAGES": "en",
    "MAX_YEAR": "2020",
}


class BytesReader:
    def __init__(self, data):
        self.data = data
        self.offset = 0

    def recv(self, size):
        chunk = self.data[self.offset:self.offset + size]
        self.offset += len(chunk)
        return chunk


class FakeInputExchange:
    def __init__(self):
        self.stopped = False

    def stop_consuming_threadsafe(self):
        self.stopped = True

    def close(self):
        pass


class FakeOutputExchange:
    def __init__(self):
        self.messages = []

    def send_rk(self, message, routing_key):
        self.messages.append((message, routing_key))

    def close(self):
        pass


def make_article(
    language="en",
    creation_date="2015-03-10",
    modification_date="2020-12-31",
    url="http://example.com/article",
):
    return {
        "url": url,
        "creation_date": creation_date,
        "modification_date": modification_date,
        "language": language,
    }


def build_filter(**overrides):
    env = {**BASE_ENV, **overrides}
    output = FakeOutputExchange()
    input_exchange = FakeInputExchange()
    instance = YearLanguageFilter(
        env,
        input_exchange=input_exchange,
        output_exchange=output,
        install_signal_handlers=False,
    )
    return instance, output, input_exchange


def process(instance, message):
    acked = []
    nacked = []
    instance.process_message(
        message,
        lambda: acked.append(True),
        lambda: nacked.append(True),
    )
    return acked, nacked


def decode_result(message):
    message_type, client_id, query_id, payload = (
        message_protocol.internal.deserialize_result(message)
    )
    if message_type != message_protocol.internal.MsgType.RESULT:
        return message_type, client_id, query_id, None, None
    external_type, article = message_protocol.external.recv_msg(
        BytesReader(payload)
    )
    return message_type, client_id, query_id, external_type, article


class YearLanguageFilterTests(unittest.TestCase):
    def test_emits_url_and_modification_date(self):
        instance, output, _ = build_filter()
        message = message_protocol.internal.serialize_article(
            1,
            make_article(
                creation_date="2015-03-10",
                modification_date="2024-06-01",
            ),
        )

        acked, nacked = process(instance, message)

        self.assertTrue(acked)
        self.assertFalse(nacked)
        self.assertEqual(len(output.messages), 1)
        body, routing_key = output.messages[0]
        self.assertEqual(routing_key, "1")
        self.assertEqual(
            decode_result(body),
            (
                message_protocol.internal.MsgType.RESULT,
                1,
                1,
                message_protocol.external.MsgType.ARTICLE,
                {
                    "url": "http://example.com/article",
                    "date_modified": "2024-06-01",
                },
            ),
        )

    def test_filters_by_creation_date_not_modification_date(self):
        instance, output, _ = build_filter()
        process(
            instance,
            message_protocol.internal.serialize_article(
                1,
                make_article(
                    creation_date="2021-01-01",
                    modification_date="2019-01-01",
                ),
            ),
        )

        self.assertEqual(output.messages, [])

    def test_max_year_is_inclusive(self):
        instance, output, _ = build_filter(MAX_YEAR="2020")
        process(
            instance,
            message_protocol.internal.serialize_article(
                1, make_article(creation_date="2020-12-31")
            ),
        )
        self.assertEqual(len(output.messages), 1)

    def test_discards_article_written_after_max_year(self):
        instance, output, _ = build_filter()
        acked, nacked = process(
            instance,
            message_protocol.internal.serialize_article(
                1, make_article(creation_date="2021-01-01")
            ),
        )

        self.assertTrue(acked)
        self.assertFalse(nacked)
        self.assertEqual(output.messages, [])

    def test_non_matching_article_does_not_need_modification_date(self):
        instance, output, _ = build_filter()
        article = {
            "url": "http://example.com/after-max-year",
            "creation_date": "2021-01-01",
            "language": "en",
        }

        acked, nacked = process(
            instance,
            message_protocol.internal.serialize_article(1, article),
        )

        self.assertTrue(acked)
        self.assertFalse(nacked)
        self.assertEqual(output.messages, [])

    def test_empty_language_falls_back_to_in_language(self):
        instance, output, _ = build_filter()
        article = {
            "url": "http://example.com/empty-language",
            "creation_date": "2019-02-03",
            "modification_date": "2024-05-06",
            "language": "",
            "in_language": "en",
        }

        acked, nacked = process(
            instance,
            message_protocol.internal.serialize_article(1, article),
        )

        self.assertTrue(acked)
        self.assertFalse(nacked)
        self.assertEqual(len(output.messages), 1)

    def test_year_parsing(self):
        accepted = ("2020-05-01T10:00:00Z", "2020")
        rejected = ("12020", "20201231", "1577836800")

        for value in accepted:
            with self.subTest(creation_date=value):
                instance, output, _ = build_filter()
                process(
                    instance,
                    message_protocol.internal.serialize_article(
                        1, make_article(creation_date=value)
                    ),
                )
                self.assertEqual(len(output.messages), 1)

        for value in rejected:
            with self.subTest(creation_date=value):
                instance, output, _ = build_filter()
                with self.assertLogs(level="WARNING"):
                    process(
                        instance,
                        message_protocol.internal.serialize_article(
                            1, make_article(creation_date=value)
                        ),
                    )
                self.assertEqual(output.messages, [])

    def test_language_filtering(self):
        cases = (
            ({}, "fr", False),
            ({"LANGUAGES": "en, fr"}, "fr", True),
            ({}, " EN ", True),
        )

        for overrides, language, emitted in cases:
            with self.subTest(language=language, overrides=overrides):
                instance, output, _ = build_filter(**overrides)
                acked, _ = process(
                    instance,
                    message_protocol.internal.serialize_article(
                        1, make_article(language=language)
                    ),
                )
                self.assertTrue(acked)
                self.assertEqual(len(output.messages), 1 if emitted else 0)

    def test_discards_invalid_dates(self):
        cases = (
            ("creation_date", "unknown", "invalid creation date"),
            ("modification_date", "n/a", "invalid modification date"),
            ("modification_date", "", "invalid modification date"),
        )

        for field, value, fragment in cases:
            with self.subTest(field=field, value=value):
                instance, output, _ = build_filter()
                with self.assertLogs(level="WARNING") as captured:
                    acked, nacked = process(
                        instance,
                        message_protocol.internal.serialize_article(
                            1, make_article(**{field: value})
                        ),
                    )

                self.assertTrue(acked)
                self.assertFalse(nacked)
                self.assertEqual(output.messages, [])
                self.assertIn(fragment, captured.output[0])

    def test_streams_each_match_without_waiting_for_eof(self):
        instance, output, _ = build_filter()

        process(
            instance,
            message_protocol.internal.serialize_article(
                1, make_article(url="http://example.com/one")
            ),
        )
        self.assertEqual(len(output.messages), 1)

        process(
            instance,
            message_protocol.internal.serialize_article(
                1, make_article(url="http://example.com/two")
            ),
        )
        self.assertEqual(len(output.messages), 2)
        self.assertEqual(
            decode_result(output.messages[1][0])[4]["url"],
            "http://example.com/two",
        )

    def test_eof_is_forwarded_as_query_end(self):
        instance, output, _ = build_filter()
        with self.assertLogs(level="INFO"):
            acked, nacked = process(
                instance,
                message_protocol.internal.serialize_end_of_records(7),
            )

        self.assertTrue(acked)
        self.assertFalse(nacked)
        self.assertEqual(len(output.messages), 1)
        body, routing_key = output.messages[0]
        self.assertEqual(routing_key, "7")
        self.assertEqual(
            decode_result(body),
            (
                message_protocol.internal.MsgType.QUERY_END,
                7,
                1,
                None,
                None,
            ),
        )

    def test_accepts_gateway_field_aliases(self):
        instance, output, _ = build_filter()
        article = {
            "url": "http://example.com/aliases",
            "date_created": "2019-02-03",
            "date_modified": "2024-05-06",
            "in_language": "en",
        }

        acked, nacked = process(
            instance,
            message_protocol.internal.serialize_article(1, article),
        )

        self.assertTrue(acked)
        self.assertFalse(nacked)
        self.assertEqual(
            decode_result(output.messages[0][0])[4],
            {
                "url": "http://example.com/aliases",
                "date_modified": "2024-05-06",
            },
        )

    def test_falls_back_to_date_modified_for_the_year(self):
        instance, output, _ = build_filter()
        article = {
            "url": "http://example.com/no-creation",
            "date_modified": "2019-02-03",
            "in_language": "en",
        }

        with self.assertLogs(level="WARNING"):
            acked, nacked = process(
                instance,
                message_protocol.internal.serialize_article(1, article),
            )

        self.assertTrue(acked)
        self.assertFalse(nacked)
        self.assertEqual(len(output.messages), 1)

    def test_malformed_payloads_are_nacked(self):
        payloads = (
            b"",
            message_protocol.internal.serialize_result(
                1,
                1,
                message_protocol.external.MsgType.ARTICLE,
                {"url": "http://example.com"},
            ),
            message_protocol.internal.serialize_article(
                1, {"url": "http://example.com", "language": "en"}
            ),
            message_protocol.internal.serialize_article(
                1, {"date_modified": "2020-01-01", "language": "en"}
            ),
            message_protocol.internal.serialize_article(
                1, {"url": "http://example.com", "date_modified": "2020-01-01"}
            ),
        )

        for payload in payloads:
            with self.subTest(payload=payload):
                instance, output, _ = build_filter()
                with self.assertLogs(level="ERROR"):
                    acked, nacked = process(instance, payload)

                self.assertFalse(acked)
                self.assertTrue(nacked)
                self.assertEqual(output.messages, [])


class ConfigTests(unittest.TestCase):
    def test_defaults(self):
        instance, _, _ = build_filter(FILTER_AMOUNT="1", FILTER_ID="0")
        self.assertEqual(instance.amount, 1)
        self.assertEqual(instance.id, 0)
        self.assertEqual(instance.languages, frozenset({"en"}))
        self.assertEqual(instance.max_year, 2020)

    def test_missing_required_variable(self):
        for name in ("MOM_HOST", "FILTER_PREFIX", "OUTPUT_EXCHANGE"):
            env = {k: v for k, v in BASE_ENV.items() if k != name}
            with self.assertRaisesRegex(ValueError, name):
                YearLanguageFilter(env, install_signal_handlers=False)

    def test_supports_multiple_instances(self):
        instance, _, _ = build_filter(FILTER_AMOUNT="2", FILTER_ID="1")
        self.assertEqual(instance.amount, 2)
        self.assertEqual(instance.id, 1)

    def test_invalid_config(self):
        cases = (
            ({"FILTER_AMOUNT": "0"}, "FILTER_AMOUNT"),
            ({"FILTER_AMOUNT": "1", "FILTER_ID": "1"}, "FILTER_ID"),
            ({"FILTER_AMOUNT": "2", "FILTER_ID": "2"}, "FILTER_ID"),
            ({"LANGUAGES": " , "}, "LANGUAGES"),
        )

        for overrides, name in cases:
            with self.subTest(overrides=overrides):
                with self.assertRaisesRegex(ValueError, name):
                    YearLanguageFilter(
                        {**BASE_ENV, **overrides},
                        install_signal_handlers=False,
                    )


class RoleSelectionTests(unittest.TestCase):
    def test_q1_maps_to_year_language_filter(self):
        self.assertIs(filter_main.FILTERS["q1"], YearLanguageFilter)

    def test_invalid_role_exits(self):
        for env in ({}, {"FILTER_ROLE": "q9"}):
            with self.subTest(env=env):
                with mock.patch.dict(os.environ, env, clear=True):
                    with self.assertRaises(SystemExit):
                        filter_main.main()


if __name__ == "__main__":
    unittest.main()
