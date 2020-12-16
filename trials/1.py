from mrjob.job import MRJob
from mrjob.step import MRStep
import csv
cols = 'iso_code,continent,location,date,total_cases,new_cases,total_deaths,new_deaths'.split(',')


class MostReported(MRJob):

    def mapper(self, _, line):
        # Convert each line into a dictionary
        row = dict(zip(cols, [a.strip()
                              for a in csv.reader([line]).next()]))

        if  row['product'] and row['reports']:
            yield row['product'],  int(row['report'])
			

    def reducer_count_reports(self, key, values):
        # send all (num_occurrences, word) pairs to the same reducer.
        # num_occurrences is so we can easily use Python's max() function.
        yield None, (max(values), key)

    def reducer_max_reports(self, _, key_values_pairs):
        # each item of word_count_pairs is (count, word),
        # so yielding one results in key=counts, value=word
        try:
            yield max(key_values_pairs)
        except ValueError:
            pass

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer_count_reports),
            MRStep(reducer=self.reducer_max_reports)
        ]


if __name__ == '__main__':
    MostReported.run()
