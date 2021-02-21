from mrjob.job import MRJob
from mrjob.step import MRStep


class Task2(MRJob):
    def mapper(self, _, line):  # Empty key and a line as value
        data = line.split(",")  # split every line by using ','
        age = data[0].strip()  # the 1st data is age and remove whitespace
        yield age, 1  # emits (key, value) pair as (age, 1)

    def reducer(self, key, values):  # receives pairs (key, values) grouped by key
        yield None, (sum(values), key)  # for each key, sum its values to compute its count

    def top10_reducer(self, _, values):
        for count, age in sorted(values, reverse=True):  # output the 10 most frequent values in age attribute in descending order
            yield int(count), str(age)

    def steps(self):  # define the order of running process
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(reducer=self.top10_reducer)
        ]


if __name__ == "__main__":
    Task2.run()
