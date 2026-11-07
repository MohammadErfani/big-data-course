
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType

# 1. Initialize Spark
spark = (
    SparkSession.builder
    .appName("FriendsOfFriends")
    .master("local[*]")
    .getOrCreate()
)

# 2. Define the input schema
schema = StructType([
    StructField("person", LongType(), False),
    StructField("friend", LongType(), False)
])

# 3. Read the CSV
edges = spark.read.csv(
    "input/friends.csv",
    header=True,
    schema=schema
)

# 4. Register the DataFrame as a SQL view
edges.createOrReplaceTempView("edges")

# 5. Find friends of friends using three self-joins
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

# 6. Display results
result.show()

# 7. Write output
(
    result.write
    .mode("overwrite")
    .option("header", True)
    .csv("output/friends_of_friends")
)

spark.stop()
