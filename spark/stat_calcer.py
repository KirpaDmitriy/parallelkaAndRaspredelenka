import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import os

spark = SparkSession.builder \
    .appName("KafkaCountryStatistics") \
    .getOrCreate()

schema = StructType([
    StructField("country", StringType(), True),
])

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "country-stats-topic") \
    .option("startingOffsets", "earliest") \
    .load()

parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

results = {}
lock = threading.Lock()

def process_batch(batch_df, batch_id):
    with lock:
        batch_counts = batch_df.groupBy("country").count().collect()
        
        for row in batch_counts:
            country = row["country"]
            count = row["count"]
            results[country] = results.get(country, 0) + count
        
        draw_ascii_graph(results)

def draw_ascii_graph(data, max_width=80, top_countries=13):
    os.system('cls' if os.name == 'nt' else 'clear')
    print("\n=== Real-Time Top 13 Countries ===\n")
    max_count = max(data.values()) if data else 1
    
    sorted_data = sorted(data.items(), key=lambda x: x[1], reverse=True)[:top_countries]
    
    for country, count in sorted_data:
        bar_length = int((count / max_count) * max_width)
        bar = "#" * bar_length
        print(f"{country:<25} | {bar} ({count})")
    
    print("\n" + "=" * 50)

query = parsed_df.writeStream \
    .outputMode("update") \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", "/tmp/spark-checkpoint") \
    .start()

query.awaitTermination()

spark.stop()
