from common.message_protocol.internal import serialize_msg
from common.message_protocol.fields import encode_fields
from common.storage.wal import WriteAheadLog, PROCESSED


class MessagePublisher:
    def __init__(self, exchange, path, get_routing_key=None):
        self.exchange = exchange
        self.get_routing_key = get_routing_key
        self.wal = WriteAheadLog(path)
        self.sent = set()

    def send(self, message_type, client_id, message_id, key, *fields):
        record = encode_fields(client_id, key)
        if record in self.sent:
            return
        message = serialize_msg(
            message_type, client_id, message_id, *fields,
            producer_id=str(self.wal.path), key=key,
        )
        self.wal.save(record)
        self._publish(message, message_type, client_id, fields)
        self.sent.add(record)

    def recover(self):
        self.wal.file.seek(0)
        entry = self.wal.read()
        while entry is not None:
            status, record = entry
            if status == PROCESSED:
                self.sent.add(record)
            entry = self.wal.read()

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
