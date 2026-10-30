import functools


@functools.total_ordering
class KeyItem:

    def __init__(self, key, amount):
        self.key = key
        self.amount = amount

    def __add__(self, other):
        return KeyItem(self.key, self.amount + other.amount)

    def __eq__(self, other):
        return self.key == other.key and self.amount == other.amount

    def __lt__(self, other):
        if self.amount == other.amount:
            return self.key < other.key
        return self.amount < other.amount

    def __str__(self):
        return f"{self.key:16} {self.amount:5d}"
