from mrjob.job import MRJob
from mrjob.step import MRStep
import re


# word_re = re.compile(r"[\w']+")


class AgeSum(MRJob):
    def mapper(self, _, line):
        data = line.split(',')  # each line is CSV
        if len(data) >= 2:
            age = data[0].strip()
            yield None, int(age)

    def reducer(self, _, values):
        age = set(values)
        avg = sum(age) / len(age)
        yield "average:", avg


if __name__ == "__main__":
    AgeSum.run()
