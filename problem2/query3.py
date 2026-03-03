"""
Problem 2 - Query 3
GenZ customers (18–21):
Group Q1 by CustID and compute:
  - total number of items purchased
  - total amount spent
Store result as T3.csv
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum

spark = SparkSession.builder.appName("P2Q3_GenZTotals").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Load Customers and T1
customers = spark.read.csv("../datasets/Customers.csv", header=True, inferSchema=True)
T1 = spark.read.csv("../datasets/T1.csv", header=True, inferSchema=True)

# Join T1 with Customers
joined = T1.join(customers, on="CustID")

# Filter for GenZ (18–21)
genz = joined.filter((joined.Age >= 18) & (joined.Age <= 21))

# Group by customer and compute totals
T3 = (
    genz.groupBy("CustID", "Age")
        .agg(
            _sum("TransNumItems").alias("total_items"),
            _sum("TransTotal").alias("total_spent")
        )
)

# Save output
T3.write.csv("problem2/T3.csv", header=True, mode="overwrite")

print("\nQuery 3 complete.")
print(f"Rows in T3: {T3.count():,}")

spark.stop()
