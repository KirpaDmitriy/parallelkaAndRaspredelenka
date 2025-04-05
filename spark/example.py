from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Word Count Example").getOrCreate()

text_data = ["Hello world", "apache spark", "hello spark"]

rdd = spark.sparkContext.parallelize(text_data)

words = rdd.flatMap(lambda line: line.split(" "))
word_pairs = words.map(lambda word: (word, 1))
word_counts = word_pairs.reduceByKey(lambda a, b: a + b)

for word, count in word_counts.collect():
    print(f"{word}: {count}")

spark.stop()
