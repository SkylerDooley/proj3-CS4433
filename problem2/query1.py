"""
Problem 2 - Query 1
Filter out purchases with TransTotal > 100
Store the result as Q1.csv
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("P2Q1_FilterPurchases").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Load Purchases.csv
purchases = (
    spark.read.csv("../datasets/Purchases.csv", header=True, inferSchema=True)
)

# Query 1: Keep only rows where TransTotal <= 100
T1 = purchases.filter(purchases.TransTotal <= 100)

# Save output
T1.write.csv("problem2/T1.csv", header=True, mode="overwrite")

print("\nQuery 1 complete.")
print(f"Rows in T1: {T1.count():,}")

spark.stop()
