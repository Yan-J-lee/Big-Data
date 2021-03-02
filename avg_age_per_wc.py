# compute the average age per work-class value

from mrjob.job import MRJob


class MRmyjob(MRJob):
    def mapper(self, _, line):
        data = line.split(', ')  # each line is CSV
        if len(data) >= 2:
            age = data[0].strip()  # extract the 1st attribute, age, and remove whitespace
            occupation = data[1].strip()  # extract the 2nd attribute, work class
            yield (occupation, age)  # emits (key, value) pairs as (occupation, age)

    # receives (occupation, age) pairs from mapper and groups by key: occupation
    def reducer(self, job, list_of_age):
        total = 0.0  # the sum of all age in a certain job
        count = 0  # the total count of age
        for age in list_of_age:
            total += int(age)
            count += 1
        avg = total / count
        yield (job, avg)  # return the average age of each occupation


if __name__ == "__main__":
    MRmyjob.run()
