from pyspark.sql import SparkSession
from operator import add

spark = SparkSession.builder \
    .appName("ScalableMovieAggregation") \
    .getOrCreate()

sc = spark.sparkContext

ratings_rdd = sc.textFile("ratings_10GB_generated.csv")

header = ratings_rdd.first()
data_rdd = ratings_rdd.filter(lambda line: line != header)

mapped_rdd = data_rdd.map(lambda line: line.split(',')) \
                     .map(lambda parts: (int(parts[1]), (float(parts[2]), 1)))

rating_counts_rdd = mapped_rdd.reduceByKey(
    lambda v1, v2: (v1[0] + v2[0], v1[1] + v2[1])
)

average_ratings_rdd = rating_counts_rdd.map(
    lambda x: (x[0], (x[1][0] / x[1][1], x[1][1]))
).filter(lambda x: x[1][1] >= 50)

top_10 = average_ratings_rdd.map(lambda x: (x[1][0], x[0])) \
                            .sortByKey(ascending=False) \
                            .take(10)

output_lines = [
    "-------------------------------------------------------",
    "MovieID\t\tAverage Rating",
    "--------------------------------------------------------"
]
for avg_rating, movie_id in top_10:
    output_lines.append(f"{movie_id}\t\t{avg_rating:.4f}")

with open("top_10.txt", "w") as f:
    f.write('\n'.join(output_lines))

print('\n'.join(output_lines))

spark.stop()
