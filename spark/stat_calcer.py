import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import matplotlib.pyplot as plt

spark = SparkSession.builder.appName("Kafka Spark Streaming").master("local[*]").getOrCreate()

schema = StructType([
    StructField("country", StringType(), True),
    StructField("amount", IntegerType(), True)
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "your-topic-name") \
    .option("startingOffsets", "earliest") \
    .load()

df = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

agg_df = df.groupBy("country").agg(spark_sum("amount").alias("total_amount"))

def plot_update(df_pandas):
    ax.clear()
    df_pandas.plot(kind='bar', x='country', y='total_amount', ax=ax, color='skyblue')
    plt.title('Total Amount by Country')
    plt.ylabel('Amount')
    plt.xlabel('Country')
    plt.draw()
    plt.pause(0.1)

plt.ion()
fig, ax = plt.subplots()

def process_row(batch_df, batch_id):
    batch_pandas = batch_df.toPandas()
    print(batch_pandas)  # Для отладки
    plot_update(batch_pandas)

query = agg_df.writeStream \
    .outputMode("complete") \
    .foreachBatch(process_row) \
    .start()

query.awaitTermination()
