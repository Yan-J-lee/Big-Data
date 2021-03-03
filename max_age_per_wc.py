# find maximum age and its frequency per work-class value


from mrjob.job import MRJob
from mrjob.job import MRStep


class MRmyjob(MRJob):
    def mapper(self, _, line):
        data = line.split(', ')
        if len(data) >= 2:
            age = data[0].strip()
            job = data[1].strip()
            yield (job, age), 1  # emits ((job, age), 1) as (key, value) pairs

    # receives ((job, age), 1) pairs and groups by (job, age)
    def reducer_frequency(self, work_age, count):
        yield work_age[0], (work_age[1], sum(count))

    def reducer_max_age(self, job_age, list_of_values):
        yield job_age, max(list_of_values)

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer_frequency),
            MRStep(reducer=self.reducer_max_age)
        ]


if __name__ == "__main__":
    MRmyjob.run()
