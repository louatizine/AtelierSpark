# ...existing code...
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
import os
import json

spark = SparkSession.builder.appName('Spark Structured Streaming').getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ...existing code...
# read streaming CSVs from orders/ (drop files there to simulate stream)
df = spark \
    .readStream \
    .schema("num int,client int,montant int")\
    .option("header", "true") \
    .csv("orders/")

count_orders = df.groupby("client").count()
# ...existing code...
# Start running the query that prints the running counts to the console
query =count_orders \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()
# ...existing code...

# New: also persist to CSV files and optionally publish to Kafka using foreachBatch
OUTPUT_CSV_DIR = "output/csv_counts"
CHECKPOINT_DIR = "checkpoint/csv_counts"
os.makedirs(OUTPUT_CSV_DIR, exist_ok=True)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")  # e.g. "localhost:9092" or "kafka:29092"
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "orders-counts")

def foreach_batch_write(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return
    # write a snapshot CSV (overwrite each micro-batch for simple realtime view)
    tmp_dir = os.path.join(OUTPUT_CSV_DIR, f"batch-{batch_id}")
    batch_df.coalesce(1).write.mode("overwrite").option("header","true").csv(tmp_dir)
    # optionally publish to Kafka as JSON messages (one message per row)
    if KAFKA_BOOTSTRAP:
        # prepare DataFrame with 'value' column as JSON string
        from pyspark.sql.functions import to_json, struct
        kafka_df = batch_df.selectExpr("CAST(client AS STRING) AS key").select(to_json(struct("*")).alias("value"))
        kafka_df.selectExpr("CAST(value AS STRING) as value") \
            .write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
            .option("topic", KAFKA_TOPIC) \
            .save()

# start an additional stream using foreachBatch to persist and optionally push to Kafka
svc = count_orders.writeStream \
    .outputMode("complete") \
    .foreachBatch(foreach_batch_write) \
    .option("checkpointLocation", CHECKPOINT_DIR) \
    .start()

try:
    svc.awaitTermination()
finally:
    svc.stop()
    query.stop()