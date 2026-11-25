from common.message_protocol.internal import serialize_msg
from common.message_protocol.fields import encode_fields, decode_fields
from common.storage.wal import WriteAheadLog, PROCESSED


class MessagePublisher:
    def __init__(self, exchange, path, get_routing_key=None):
        self.exchange = exchange
        self.get_routing_key = get_routing_key
        self.wal = WriteAheadLog(path)
        self.last = None

    def send(self, message_type, client_id, message_id, key, source, *fields):
        if self.last is None or self.last[0] != (client_id, source):
            self.last = ((client_id, source), set())
        if key in self.last[1]:
            return
        message = serialize_msg(
            message_type, client_id, message_id, str(self.wal.path), key, *fields,
        )
        record = encode_fields(client_id, source, key)
        self.wal.save(record)
        self._publish(message, message_type, client_id, fields)
        self.last[1].add(key)

    def recover(self):
        self.wal.file.seek(0)
        entry = self.wal.read()
        while entry is not None:
            status, record = entry
            client_id, source, key = decode_fields(record)
            if self.last is None or self.last[0] != (client_id, source):
                self.last = ((client_id, source), set())
            if status == PROCESSED:
                self.last[1].add(key)
            else:
                self.last[1].discard(key)
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
