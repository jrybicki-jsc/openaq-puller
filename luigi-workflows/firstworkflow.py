import random
import luigi

from collections import defaultdict
from datetime import timedelta, date


class ObjectList(luigi.Task):
    date = luigi.DateParameter()

    def run(self):
        with self.output().open('w') as output:
            for _ in range(100):
                output.write('{}\n'.format(random.randint(0, 100)))

    def output(self):
        return luigi.LocalTarget(self.date.strftime('data/output_faked_%Y_%m_%d.dat'))


class Aggregate(luigi.Task):
    date_interval = luigi.DateIntervalParameter()

    def output(self):
        return luigi.LocalTarget('data/aggregatet%r.dat'.format(self.date_interval))

    def requires(self):
        return [ObjectList(date) for date in self.date_interval]

    def run(self):

        number_count = defaultdict(int)

        for t in self.input():
            with t.open('r') as in_file:
                for line in in_file:
                    number_count[int(line)] += 1

        with self.output().open('w') as out_file:
            for ac, c in number_count.items():
                out_file.write('{} ==> {}\n'.format(ac, c))
