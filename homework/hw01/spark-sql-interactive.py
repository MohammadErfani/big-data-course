
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType
import argparse
import csv
import shutil
from pathlib import Path
spark = (
    SparkSession.builder
    .appName("FriendsOfFriends")
    .master("local[*]")
    .getOrCreate()
)

schema = StructType([
    StructField("person", LongType(), False),
    StructField("friend", LongType(), False)
])

parser = argparse.ArgumentParser(
    description="Find friends of friends using Spark SQL"
)

parser.add_argument(
    "-i", "--input",
    default="input/friends.csv",
    help="Input CSV path"
)

parser.add_argument(
    "-o", "--output",
    default="output/friends_of_friends.csv",
    help="Output CSV path"
)

args = parser.parse_args()

edges = spark.read.csv(
    args.input,
    header=True,
    schema=schema
)

edges.createOrReplaceTempView("edges")

result = spark.sql("""
    WITH candidates AS (

        -- Case 1: Shared friend column
        SELECT
            a.person AS x,
            b.person AS y
        FROM edges a
        JOIN edges b
            ON a.friend = b.friend

        UNION ALL

        -- Case 2: Friend matches person
        SELECT
            a.person AS x,
            b.friend AS y
        FROM edges a
        JOIN edges b
            ON a.friend = b.person

        UNION ALL

        -- Case 3: Shared person column
        SELECT
            a.friend AS x,
            b.friend AS y
        FROM edges a
        JOIN edges b
            ON a.person = b.person
    ),

    normalized AS (
        SELECT DISTINCT
            LEAST(x, y) AS person,
            GREATEST(x, y) AS friend
        FROM candidates
        WHERE x <> y
    )

    SELECT
        n.person,
        n.friend AS friend_of_friend
    FROM normalized n
    LEFT ANTI JOIN edges e
        ON n.person = e.person
       AND n.friend = e.friend
""")

result.show()


# Write using Spark's normal parallel output
output_path = Path(args.output)
temp_dir = output_path.parent / f"{output_path.stem}_parts"

result.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(str(temp_dir))

# Merge the CSV files into one
output_path.parent.mkdir(parents=True, exist_ok=True)

with output_path.open("w", newline="") as output_file:
    writer = csv.writer(output_file)
    writer.writerow(["person", "friend_of_friend"])

    for part_file in sorted(temp_dir.glob("part-*.csv")):
        with part_file.open("r", newline="") as input_file:
            reader = csv.reader(input_file)
            next(reader, None)  # Skip this part's header
            writer.writerows(reader)

# Remove temporary Spark output
shutil.rmtree(temp_dir)

print(f"Results saved to: {output_path}")


spark.stop()
