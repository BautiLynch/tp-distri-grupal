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
    "FILTER_PREFIX": "filter_year_language",
    "OUTPUT_QUEUE": "handler_q1",
    "FILTER_ID": "0",
    "FILTER_AMOUNT": "1",
    "LANGUAGES": "en",
    "MAX_YEAR": "2020",
}


class FakeQueue:
    def __init__(self):
        self.messages = []

    def send(self, message):
        self.messages.append(message_protocol.internal.deserialize(message))

    def close(self):
        pass


class FakeExchange:
    def __init__(self):
        self.stopped = False

    def stop_consuming_threadsafe(self):
        self.stopped = True

    def close(self):
        pass


def make_article(
    client="c1",
    language="en",
    creation_date="2015-03-10",
    modification_date="2020-12-31",
    url="http://example.com/article",
):
    return [client, url, creation_date, modification_date, language]


def build_filter(**overrides):
    env = {**BASE_ENV, **overrides}
    output = FakeQueue()
    exchange = FakeExchange()
    instance = YearLanguageFilter(
        env,
        input_exchange=exchange,
        output_queue=output,
        install_signal_handlers=False,
    )
    return instance, output, exchange


def process(instance, payload):
    acked = []
    nacked = []
    instance.process_message(
        message_protocol.internal.serialize(payload),
        lambda: acked.append(True),
        lambda: nacked.append(True),
    )
    return acked, nacked


class YearLanguageFilterTests(unittest.TestCase):
    def test_emits_url_and_modification_date(self):
        instance, output, _ = build_filter()
        acked, nacked = process(
            instance,
            make_article(creation_date="2015-03-10", modification_date="2024-06-01"),
        )

        self.assertTrue(acked)
        self.assertFalse(nacked)
        self.assertEqual(
            output.messages,
            [["c1", "http://example.com/article", "2024-06-01"]],
        )

    def test_filters_by_creation_date_not_modification_date(self):
        instance, output, _ = build_filter()
        process(
            instance,
            make_article(creation_date="2021-01-01", modification_date="2019-01-01"),
        )

        self.assertEqual(output.messages, [])

    def test_max_year_is_inclusive(self):
        instance, output, _ = build_filter(MAX_YEAR="2020")
        process(instance, make_article(creation_date="2020-12-31"))
        self.assertEqual(len(output.messages), 1)

    def test_discards_article_written_after_max_year(self):
        instance, output, _ = build_filter()
        acked, nacked = process(instance, make_article(creation_date="2021-01-01"))

        self.assertTrue(acked)
        self.assertFalse(nacked)
        self.assertEqual(output.messages, [])

    def test_year_parsing(self):
        accepted = ("2020-05-01T10:00:00Z", "2020")
        rejected = ("12020", "20201231", "1577836800")

        for value in accepted:
            with self.subTest(creation_date=value):
                instance, output, _ = build_filter()
                process(instance, make_article(creation_date=value))
                self.assertEqual(len(output.messages), 1)

        for value in rejected:
            with self.subTest(creation_date=value):
                instance, output, _ = build_filter()
                with self.assertLogs(level="WARNING"):
                    process(instance, make_article(creation_date=value))
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
                acked, _ = process(instance, make_article(language=language))
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
                    acked, nacked = process(instance, make_article(**{field: value}))

                self.assertTrue(acked)
                self.assertFalse(nacked)
                self.assertEqual(output.messages, [])
                self.assertIn(fragment, captured.output[0])

    def test_streams_each_match_without_waiting_for_eof(self):
        instance, output, _ = build_filter()

        process(instance, make_article(url="http://example.com/one"))
        self.assertEqual(len(output.messages), 1)

        process(instance, make_article(url="http://example.com/two"))
        self.assertEqual(len(output.messages), 2)
        self.assertEqual(output.messages[1][1], "http://example.com/two")

    def test_eof_is_forwarded_with_filter_id(self):
        instance, output, _ = build_filter(FILTER_ID="2", FILTER_AMOUNT="3")
        with self.assertLogs(level="INFO"):
            acked, nacked = process(instance, ["c1"])

        self.assertTrue(acked)
        self.assertFalse(nacked)
        self.assertEqual(output.messages, [["c1", 2]])

    def test_malformed_payloads_are_nacked(self):
        payloads = (
            ["c1", "http://example.com/article", "2020-01-01", "en"],
            ["c1", "Article", "http://example.com", ["Bio"], "2020-01-01", "en", "", "img"],
            ["c1", 0],
            42,
            [],
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
        for name in ("MOM_HOST", "FILTER_PREFIX", "OUTPUT_QUEUE"):
            env = {k: v for k, v in BASE_ENV.items() if k != name}
            with self.assertRaisesRegex(ValueError, name):
                YearLanguageFilter(env, install_signal_handlers=False)

    def test_invalid_config(self):
        cases = (
            ({"FILTER_AMOUNT": "0"}, "FILTER_AMOUNT"),
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
