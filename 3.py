from mrjob.job import MRJob
from mrjob.step import MRStep
import csv
cols = 'iso_code,continent,location,date,total_cases,new_cases,total_deaths,new_deaths'.split(',')
class TopChemicals(MRJob):
   def mapper(self, _, line):
       # Convert each line into a dictionary
       row = dict(zip(cols, [a.strip()
                             for a in csv.reader([line]).__next__()]))
       
           yield row['product'], int(row['reports'])

   def reducer_count_reports(self, location, new_cases):
           # send all (num_occurrences, word) pairs to the same reducer.
           # num_occurrences is so we can easily use Python's max() function.
       yield None, (int(max(reports)), product)

   def secondreducer(self,key,max_cases):
       self.max_list = []
       for value in max_cases:
           self.max_list.append(value)

       for index in range(10):
           yield max(self.max_list)
           self.max_list.remove(max(self.max_list))

   def steps(self):
       return [
           MRStep(mapper=self.mapper,
                   reducer=self.reducer_count_reports),
           MRStep(reducer=self.secondreducer)
           ]
if __name__ == '__main__':
   TopChemicals.run()