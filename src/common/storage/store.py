import csv
from collections.abc import Iterator
import os
from common import key_item

class Storage:
    def __init__(self, storage_path) -> None:
        self.storage_path = storage_path

    def store_key(self, client_id, key, amount) -> None:
        os.makedirs(self.storage_path, exist_ok=True)

        file_path = os.path.join(
            self.storage_path,
            f"client_{client_id}.csv"
        )

        with open(file_path, "a", newline="", encoding="utf-8") as file:
            writer = csv.writer(file, quoting=csv.QUOTE_MINIMAL)
            writer.writerow([key, amount])

    def load_key(self, client_id) -> Iterator[key_item.KeyItem]:
        os.makedirs(self.storage_path, exist_ok=True)
        
        file_path = os.path.join(
            self.storage_path,
            f"client_{client_id}.csv"
        )

        with open(file_path, "r") as file:
            reader = csv.reader(file, quoting=csv.QUOTE_MINIMAL)
            for row in reader:
                [key, amount] = row
                yield key_item.KeyItem(
                    key,
                    int(amount)
                )