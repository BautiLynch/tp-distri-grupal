import os
import socket
import sys
import unittest

sys.path.insert(
    0,
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"),
)

from common import message_protocol


class ProtocolTests(unittest.TestCase):
    def test_internal_round_trip(self):
        payload = ["c1", "word", 3]
        body = message_protocol.internal.serialize(payload)
        self.assertEqual(message_protocol.internal.deserialize(body), payload)

    def test_external_round_trip(self):
        left, right = socket.socketpair()
        try:
            message_protocol.external.send_msg(left, ["c1", "word", 3])
            self.assertEqual(
                message_protocol.external.recv_msg(right), ["c1", "word", 3]
            )
        finally:
            left.close()
            right.close()


if __name__ == "__main__":
    unittest.main()
