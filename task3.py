from mrjob.job import MRJob
from mrjob.step import MRStep


class Task3(MRJob):
    # Parses each line of the input and emits (key, value) pair as (sym, line_id)
    def mapper(self, _, line):  # empty key and a line as value
        data = line.strip().split(' ')  # split line using whitespace and then remove whitespace
        line_id = data[0]  # extract line id
        symbols = data[1:len(data)]  # obtain symbols
        symbols = set(symbols)  # remove duplicates in the symbols
        for sym in symbols:  # output each symbol and its line id
            yield sym, line_id

    # receives pairs (key, values) grouped by key
    def reducer(self, key, values):
        line_ids = list(map(int, values))  # convert line id to list containing int type 
        yield key, line_ids  # output each symbol and its line id list 

    # define the order of running process
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]


if __name__ == "__main__":
    Task3.run()
