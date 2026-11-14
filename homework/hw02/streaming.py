
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, StructField

BASE_DIR = Path(__file__).resolve().parent

# Switch to False for the 24 Hours
TEST_MODE = True

WINDOW_DURATION = "1 minute" if TEST_MODE else "24 hours"
SLIDE_DURATION = "10 seconds" if TEST_MODE else "5 minutes"
TRIGGER_INTERVAL = "5 seconds" if TEST_MODE else "5 minutes"

# This is a separate late-event tolerance setting.
WATERMARK_DELAY = "30 seconds" if TEST_MODE else "5 minutes"

checkpoint_dir = BASE_DIR / "checkpoints" 

# Initialize Spark
spark = (
    SparkSession.builder
    .appName("QueryFrequencyStreaming")
    .master("local[*]")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Define the input schema
schema = StructType([
    StructField("timestamp", StringType()),
    StructField("query", StringType()),
])

# Read the stream
queries = (
    spark.readStream
    .schema(schema)
    .json(str(BASE_DIR / "input"))
)

# Normalize queries and parse timestamps
normalized = (
    queries
    .withColumn("event_time", F.to_timestamp("timestamp"))
    .withColumn(
        "normalized_query",
        F.concat_ws(
            " ",
            F.array_sort(
                F.split(F.trim(F.lower(F.col("query"))), r"\s+")
            )
        )
    )
    .filter(F.col("event_time").isNotNull())
    .filter(F.col("normalized_query") != "")
)

# Count occurrences per query and time window
counts = (
    normalized
    .withWatermark("event_time", WATERMARK_DELAY)
    .groupBy(
        F.window("event_time", WINDOW_DURATION, SLIDE_DURATION),
        F.col("normalized_query")
    )
    .count()
)

# Prepare readable output
result = counts.select(
    F.col("window.start").alias("window_start"),
    F.col("window.end").alias("window_end"),
    F.col("normalized_query").alias("query"),
    F.col("count")
)

# Start streaming
stream = (
    result.writeStream
    .format("console")
    .outputMode("complete")
    .option("truncate", False)
    .option("numRows", 100)
    .option("checkpointLocation", str(checkpoint_dir))
    .trigger(processingTime=TRIGGER_INTERVAL)
    .start()
)

try:
    stream.awaitTermination()
finally:
    stream.stop()
    spark.stop()
