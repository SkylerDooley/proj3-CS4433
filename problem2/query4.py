"""
Problem 2, Query 4:
Join T1 with Customers on CustID.
Group by customer, compute total costs.
Return customers where salary < total_expenses (cannot cover expenses).
"""

import os, sys
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["HADOOP_HOME"] = "C:\\tools\\hadoop-3.2.2"

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("P2_Query4_CannotCoverExpenses") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Load Customers 
customers = spark.read.csv(
    "../datasets/Customers.csv",
    header=True,
    inferSchema=True
)
print(f"Customers loaded: {customers.count():,} rows")

# Load T1
T1 = spark.read.csv(
    "../datasets/T1.csv",
    header=True,
    inferSchema=True
)
print(f"T1 loaded: {T1.count():,} rows")

# Register views
customers.createOrReplaceTempView("Customers")
T1.createOrReplaceTempView("T1")

# Query 4
result = spark.sql("""
    SELECT
        c.CustID,
        c.Salary,
        c.Address,
        SUM(t.TransTotal)   AS total_expenses
    FROM T1 t
    JOIN Customers c ON t.CustID = c.CustID
    GROUP BY c.CustID, c.Salary, c.Address
    HAVING SUM(t.TransTotal) > c.Salary
    ORDER BY total_expenses DESC
""")

# Results 
count = result.count()
print(f"\nCustomers who cannot cover their expenses: {count:,}")
print("=" * 75)
result.show(20, truncate=False)

# Formatted print
rows = result.collect()
print(f"\n{'CustID':<10} {'Salary':>10} {'Total Expenses':>16} {'Shortfall':>12}  Address")
print("-" * 75)
for r in rows[:20]:
    shortfall = r.total_expenses - r.Salary
    print(f"  {r.CustID:<8} {r.Salary:>10.2f} {r.total_expenses:>16.2f} {shortfall:>12.2f}  {r.Address}")

spark.stop()