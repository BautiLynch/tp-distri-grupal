import csv
from collections.abc import Iterator
import os
from common import word_item

class Storage:
    def __init__(self, storage_path) -> None:
        self.storage_path = storage_path

    def store_words(self, clientId, word, amount) -> None:
        os.makedirs(self.storage_path, exist_ok=True)

        file_path = os.path.join(
            self.storage_path,
            f"client_{clientId}.csv"
        )

        with open(file_path, "a", newline="", encoding="utf-8") as file:
            writer = csv.writer(file, quoting=csv.QUOTE_MINIMAL)
            writer.writerow([word, 1])

    def load_words(self, clientId) -> Iterator[word_item.WordItem]:
        os.makedirs(self.storage_path, exist_ok=True)
        
        file_path = os.path.join(
            self.storage_path,
            f"client_{clientId}.csv"
        )

        with open(file_path, "r") as file:
            reader = csv.reader(file, quoting=csv.QUOTE_MINIMAL)
            for row in reader:
                [word, amount] = row
                yield word_item.WordItem(
                    word,
                    int(amount)
                )