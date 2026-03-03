"""
Query 3:
Given Mega-Event, return all HEALTHY people who were sitting at the same table
as at least one SICK person. Do not return a healthy person twice.
"""

import os, sys
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["HADOOP_HOME"] = "C:\\tools\\hadoop-3.2.2"

from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Query3CloseContact").setMaster("local[*]")
sc   = SparkContext(conf=conf)
sc.setLogLevel("WARN")

# Load data
# Each line: pi_id, pi_name, pi_table, pi_test
raw_rdd = sc.textFile("../datasets/MetaEvent.csv")

# Parse
header = raw_rdd.first()

parsed_rdd = (
    raw_rdd
    .filter(lambda line: line.strip() != header.strip())          # skip header
    .map(lambda line: line.split(","))            # split CSV columns
    .map(lambda f: (f[0].strip(), f[1].strip(), f[2].strip(), f[3].strip()))  # id, name, table, test
)

# Get tables that have at least one sick person
sick_tables_rdd = (
    parsed_rdd
    .filter(lambda p: p[3] == "sick")
    .map(lambda p: p[2])   # just the table ID
    .distinct()
)

# Broadcast sick tables set for efficient lookup across workers
sick_tables = sc.broadcast(set(sick_tables_rdd.collect()))

# Get all HEALTHY people on a sick table, no duplicates
result_rdd = (
    parsed_rdd
    .filter(lambda p: p[3] == "not-sick")             # healthy only
    .filter(lambda p: p[2] in sick_tables.value)      # on a sick table
    .distinct()                                        # no duplicates
)

# Output
results = result_rdd.collect()
print(f"\nHealthy people at risk (sitting with sick person): {len(results):,}\n")
print("First 10:")
for p in results[:10]:
    print(f"  id={p[0]:>8}  name={p[1]:<20}  table={p[2]}  test={p[3]}")

sc.stop()