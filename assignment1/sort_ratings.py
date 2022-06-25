from mrjob.job import MRJob
from mrjob.step import MRStep

class RatingsBreakdown (MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   combiner=self.combine_ratings_count,
                   reducer=self.reducer_count_ratings
            ),
            MRStep(
                  reducer=self.reducer_sort_counts
           )
        ]

    def mapper_get_ratings(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1

    def combine_ratings_count (self, key, values):
        yield key, sum(values)

    def reducer_count_ratings(self, key, values):
        yield None, (sum(values), key)

    def reducer_sort_counts(self, _, ratings_counts):
        for count, key in sorted(ratings_counts, reverse=True):
           yield key, count


if __name__ == '__main__':
    RatingsBreakdown.run()
