import logging

from common.message_protocol.internal import deserialize_msg, serialize_msg
from common.storage.wal import WriteAheadLog


class MessagePublisher:
    def __init__(self, exchange, path, get_routing_key=None):
        self.exchange = exchange
        self.get_routing_key = get_routing_key
        self.wal = WriteAheadLog(path)

    def send(self, message_type, client_id, *fields):
        message = serialize_msg(message_type, client_id, *fields)
        self.wal.save(message)
        self._publish(message, message_type, client_id, fields)

    def recover(self):
        pending = self.wal.load()
        if pending is None:
            return
        try:
            message_type, client_id, fields = deserialize_msg(pending)
        except ValueError:
            logging.warning("Discarding unreadable message WAL")
            self.wal.discard()
            return

        self._publish(pending, message_type, client_id, fields)
        return message_type, client_id

    def _publish(self, message, message_type, client_id, fields):
        routing_key = client_id
        if self.get_routing_key is not None:
            routing_key = self.get_routing_key(message_type, client_id, fields)
        if routing_key is None:
            self.exchange.send(message)
        else:
            self.exchange.send_to(message, routing_key=routing_key)
        self.wal.mark_processed()

    def close(self):
        self.wal.close()
