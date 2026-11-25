import logging
import os

from common.storage.wal import WriteAheadLog
from common.message_protocol.fields import encode_fields, decode_fields


class EofCounter:
    def __init__(self, directory, expected):
        self.directory = directory
        self.expected = expected
        os.makedirs(directory, exist_ok=True)
        self.wals = {}
        self.counts = {}
        self.received = {}

    def get(self, client_id):
        return self.counts.get(client_id, 0)

    def save(self, client_id, key):
        received = self.received.setdefault(client_id, set())
        if key in received:
            return self.get(client_id)
        count = self.get(client_id) + 1
        if client_id not in self.wals:
            path = os.path.join(self.directory, f"{client_id}.wal")
            self.wals[client_id] = WriteAheadLog(path)
        self.wals[client_id].save(encode_fields(count, key))
        self.counts[client_id] = count
        received.add(key)
        return count

    def clear(self, client_id):
        wal = self.wals.pop(client_id, None)
        if wal is not None:
            wal.delete()
        self.counts.pop(client_id, None)
        self.received.pop(client_id, None)

    def recover(self):
        for filename in os.listdir(self.directory):
            if not filename.endswith(".wal"):
                continue
            client_id = int(filename[:-4])
            wal = WriteAheadLog(os.path.join(self.directory, filename))
            self.wals[client_id] = wal
            record = wal.read()
            while record is not None:
                status, pending = record
                try:
                    count, key = decode_fields(pending)
                    valid = 0 < count <= self.expected
                except (ValueError, TypeError):
                    valid = False
                if not valid:
                    logging.warning("Discarding unreadable EOF count for client %s", client_id)
                    wal.discard()
                    self.counts.pop(client_id, None)
                    self.received.pop(client_id, None)
                    break
                self.counts[client_id] = count
                self.received.setdefault(client_id, set()).add(key)
                record = wal.read()

    def close(self):
        for wal in self.wals.values():
            wal.close()
