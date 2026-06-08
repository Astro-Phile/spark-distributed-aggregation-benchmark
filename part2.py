from pyspark.sql import SparkSession
from operator import add

spark = SparkSession.builder \
    .appName("InefficientMovieAggregation") \
    .getOrCreate()

sc = spark.sparkContext

ratings_rdd = sc.textFile("ratings_10GB_generated.csv")

header = ratings_rdd.first()
data_rdd = ratings_rdd.filter(lambda line: line != header)

mapped_rdd = data_rdd.map(lambda line: line.split(',')) \
                     .map(lambda parts: (int(parts[1]), float(parts[2])))

grouped_rdd = mapped_rdd.groupByKey()

def calculate_metrics(ratings):
    ratings_list = list(ratings)
    count = len(ratings_list)
    total_rating = sum(ratings_list)
    return (total_rating / count, count)

try:
    average_ratings_rdd = grouped_rdd.mapValues(calculate_metrics) \
                                     .filter(lambda x: x[1][1] >= 50)

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

    print("Job finished successfully (unlikely on large, skewed data):")
    print('\n'.join(output_lines))

except Exception as e:
    print(f"The groupByKey job failed as expected. Error: {e}")

spark.stop()
