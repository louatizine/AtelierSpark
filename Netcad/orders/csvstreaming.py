from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

spark = SparkSession.builder.appName('Spark Structured Streaming').getOrCreate()
spark.sparkContext.setLogLevel("WARN")
df = spark \
    .readStream \
    .schema("num int,client int,montant int")\
    .csv("orders/", header=True)

count_orders = df.groupby("client").count()
# Start running the query that prints the running counts to the console
query =count_orders \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()
query.awaitTermination()