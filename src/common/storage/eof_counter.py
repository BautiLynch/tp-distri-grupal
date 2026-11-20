import logging
import os

from common.storage.wal import WriteAheadLog


class EofCounter:
    def __init__(self, directory, expected):
        self.directory = directory
        self.expected = expected
        os.makedirs(directory, exist_ok=True)
        self.wals = {}
        self.counts = {}

    def get(self, client_id):
        return self.counts.get(client_id, 0)

    def save(self, client_id, count):
        if client_id not in self.wals:
            path = os.path.join(self.directory, f"{client_id}.wal")
            self.wals[client_id] = WriteAheadLog(path)
        self.wals[client_id].save(count.to_bytes(4, "big"))
        self.counts[client_id] = count

    def clear(self, client_id):
        wal = self.wals.pop(client_id, None)
        if wal is not None:
            wal.delete()
        self.counts.pop(client_id, None)

    def recover(self):
        for filename in os.listdir(self.directory):
            if not filename.endswith(".wal"):
                continue
            client_id = int(filename[:-4])
            wal = WriteAheadLog(os.path.join(self.directory, filename))
            self.wals[client_id] = wal
            pending = wal.load()
            if pending is None:
                continue
            count = int.from_bytes(pending, "big")
            if len(pending) != 4 or not 0 < count < self.expected:
                logging.warning("Discarding unreadable EOF count for client %s", client_id)
                wal.discard()
                continue
            self.counts[client_id] = count

    def close(self):
        for wal in self.wals.values():
            wal.close()
