import csv
import os


class StaticCounter:
    def __init__(self, filename="counter.csv"):
        self.filename = filename
        self.counter = 0
        self._load()

    def _load(self):
        """Load the counter value from a file."""
        if os.path.exists(self.filename):
            with open(self.filename, mode="r", newline="") as file:
                reader = csv.reader(file)
                for row in reader:
                    self.counter = int(row[0])
                    break  # Expecting only one row with one value.

    def increment(self, value):
        """Increment the counter and save the new value."""
        self.counter += value
        self._save()

    def _save(self):
        """Save the counter value to a file."""
        with open(self.filename, mode="w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow([self.counter])

    def get_value(self):
        """Get the current value of the counter."""
        return self.counter
