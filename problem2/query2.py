"""
Problem 2, Query 2:
Group purchases in T1 by TransNumItems.
For each group, calculate median, min, and max of TransTotal.
Report results to client side.
"""

import os, sys
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["HADOOP_HOME"] = "C:\\tools\\hadoop-3.2.2"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, max, percentile_approx

spark = SparkSession.builder \
    .appName("P2_Query2_GroupByItems") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Load T1 
# T1 = Purchases filtered to TransTotal <= 100 (from Query 1)
T1 = spark.read.csv(
    "../datasets/T1.csv",
    header=True,
    inferSchema=True
)

print(f"T1 row count: {T1.count():,}")
T1.printSchema()

# Register as SQL view 
T1.createOrReplaceTempView("T1")

# Query 2: Group by TransNumItems, compute median/min/max of TransTotal 
# percentile_approx(col, 0.5) computes the median (50th percentile)
result = spark.sql("""
    SELECT
        TransNumItems,
        percentile_approx(TransTotal, 0.5)  AS median_amount,
        MIN(TransTotal)                      AS min_amount,
        MAX(TransTotal)                      AS max_amount,
        COUNT(*)                             AS num_purchases
    FROM T1
    GROUP BY TransNumItems
    ORDER BY TransNumItems
""")

# Report results to client 
print("\nQuery 2 Results - Grouped by Number of Items Purchased:")
print("=" * 65)
result.show(20, truncate=False)

# Collect and print in a formatted table
rows = result.collect()
print(f"\n{'Items':<8} {'Median $':>10} {'Min $':>10} {'Max $':>10} {'Count':>10}")
print("-" * 55)
for r in rows:
    print(f"  {r.TransNumItems:<6} {r.median_amount:>10.2f} {r.min_amount:>10.2f} {r.max_amount:>10.2f} {r.num_purchases:>10,}")

spark.stop()