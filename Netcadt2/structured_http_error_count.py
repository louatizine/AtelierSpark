from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col
import os

OUTPUT_DIR = "output/errors_json"
os.makedirs(OUTPUT_DIR, exist_ok=True)

spark = SparkSession.builder.appName("HTTP Error Counter").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# read from socket (the Python log generator listens on 9999)
lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

# extract HTTP status code (pattern like ... "GET /... HTTP/1.1" 404 0)
status = regexp_extract(col("value"), r'\" (\d{3}) ', 1).alias("status")
statuses = lines.select(status).filter(col("status") != "").withColumn("status", col("status").cast("int"))

# keep only errors (>= 400)
errors = statuses.filter(col("status") >= 400)
counts = errors.groupBy("status").count()

def write_snapshot(batch_df, batch_id):
    # overwrite snapshot for visualization
    batch_df.coalesce(1).write.mode("overwrite").json(OUTPUT_DIR)

query = counts.writeStream.outputMode("complete").foreachBatch(write_snapshot).start()
console_q = counts.writeStream.format("console").outputMode("complete").start()

try:
    query.awaitTermination()
finally:
    console_q.stop()
    query.stop()