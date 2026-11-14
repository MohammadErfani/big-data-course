
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, StructField

BASE_DIR = Path(__file__).resolve().parent

# Switch to False for the 24 Hours
TEST_MODE = True

WINDOW_DURATION = "1 minute" if TEST_MODE else "24 hours"
SLIDE_DURATION = "30 seconds" if TEST_MODE else "5 minutes"
TRIGGER_INTERVAL = "10 seconds" if TEST_MODE else "5 minutes"

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

# Count queries in sliding windows
counts = (
    normalized
    .withWatermark("event_time", WATERMARK_DELAY)
    .groupBy(
        F.window(
            "event_time",
            WINDOW_DURATION,
            SLIDE_DURATION
        ),
        F.col("normalized_query")
    )
    .count()
)

# Prepare the results
result = counts.select(
    F.col("window.start").alias("window_start"),
    F.col("window.end").alias("window_end"),
    F.col("normalized_query").alias("query"),
    F.col("count")
)

# Write finalized windows to CSV
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def write_batch(batch_df, batch_id):
    # Find windows finalized in this micro-batch
    windows = (
        batch_df
        .select("window_start", "window_end")
        .distinct()
        .collect()
    )

    for window in windows:
        start = window["window_start"]
        end = window["window_end"]

        # Select all query counts for this window
        window_result = (
            batch_df
            .filter(
                (F.col("window_start") == start) &
                (F.col("window_end") == end)
            )
            .select("query", "count")
        )

        # Create a readable filename
        filename = (
            f"{start.strftime('%Y%m%d_%H%M%S')}_"
            f"{end.strftime('%Y%m%d_%H%M%S')}.csv"
        )

        # Spark writes CSV directories, so use a temporary directory
        temp_dir = OUTPUT_DIR / f"_temp_{batch_id}"

        (
            window_result
            .coalesce(1)
            .write
            .mode("overwrite")
            .option("header", True)
            .csv(str(temp_dir))
        )

        # Move the generated CSV into the output directory
        part_file = next(temp_dir.glob("part-*.csv"))
        final_file = OUTPUT_DIR / filename

        part_file.replace(final_file)

        # Remove Spark's temporary directory
        import shutil
        shutil.rmtree(temp_dir)

        print(f"Finalized window: {start} → {end}")


# Start streaming
stream = (
    result.writeStream
    .foreachBatch(write_batch)
    .outputMode("append")
    .option("checkpointLocation", str(checkpoint_dir))
    .trigger(processingTime=TRIGGER_INTERVAL)
    .start()
)

try:
    stream.awaitTermination()
finally:
    stream.stop()
    spark.stop()