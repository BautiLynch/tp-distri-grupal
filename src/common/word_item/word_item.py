import functools


@functools.total_ordering
class WordItem:

    def __init__(self, word, amount):
        self.word = word
        self.amount = amount

    def __add__(self, other):
        return WordItem(self.word, self.amount + other.amount)

    def __eq__(self, other):
        return self.word == other.word and self.amount == other.amount

    def __lt__(self, other):
        if self.amount == other.amount:
            return self.word < other.word
        return self.amount < other.amount

    def __str__(self):
        return f"{self.word:16} {self.amount:5d}"
