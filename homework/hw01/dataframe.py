from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. Initialize Spark
spark = (
    SparkSession.builder
    .appName("FriendsOfFriends")
    .master("local[*]")
    .getOrCreate()
)

# 2. Read CSV
edges = spark.read.csv(
    "input/friends-small.csv",
    header=True,
    inferSchema=True
)

# 3. Create aliases for self-joins
a = edges.alias("a")
b = edges.alias("b")

# 4. First join: friend = friend
case1 = (
    a.join(b, F.col("a.friend") == F.col("b.friend"))
    .select(
        F.col("a.person").alias("x"),
        F.col("b.person").alias("y")
    )
)

# 5. Second join: friend = person
case2 = (
    a.join(b, F.col("a.friend") == F.col("b.person"))
    .select(
        F.col("a.person").alias("x"),
        F.col("b.friend").alias("y")
    )
)

# 6. Third join: person = person
case3 = (
    a.join(b, F.col("a.person") == F.col("b.person"))
    .select(
        F.col("a.friend").alias("x"),
        F.col("b.friend").alias("y")
    )
)

# 7. Combine all candidates
candidates = case1.union(case2).union(case3)

# 8. Remove self-pairs and normalize
normalized = (
    candidates
    .filter(F.col("x") != F.col("y"))
    .select(
        F.least("x", "y").alias("person"),
        F.greatest("x", "y").alias("friend")
    )
    .distinct()
)

# 9. Remove direct friendships
result = normalized.join(
    edges,
    on=["person", "friend"],
    how="left_anti"
)

# 10. Display and save
result.show()

result.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("output/friends_of_friends")

spark.stop()