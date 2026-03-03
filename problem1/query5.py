"""
Query 5:
Given Mega-Event-No-Disclosure and Reported-Illnesses, return all healthy people
(not in Reported-Illnesses) who were sitting at the same table as a sick person
(from Reported-Illnesses). Do not return a healthy person twice.
"""

import os, sys
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["HADOOP_HOME"] = "C:\\tools\\hadoop-3.2.2"

from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Query5CloseContacts").setMaster("local[*]")
conf.set("spark.driver.memory", "4g")
conf.set("spark.executor.memory", "4g")
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")

# Load Mega-Event-No-Disclosure 
# Fields: pi_id, pi_name, pi_table
raw_nd = sc.textFile("../datasets/MetaEventNoDisclosure.csv")
header_nd = raw_nd.first().strip()

# (id, name, table)
no_disclosure_rdd = (
    raw_nd
    .filter(lambda line: line.strip() != header_nd)
    .map(lambda line: line.strip().split(","))
    .map(lambda f: (f[0].strip(), f[1].strip(), f[2].strip()))
)

# Load Reported-Illnesses
# Fields: pi_id, pi_test=sick
raw_ri = sc.textFile("../datasets/ReportedIllnesses.csv")
header_ri = raw_ri.first().strip()

# Set of sick person IDs - broadcast for efficient lookup
sick_ids_rdd = (
    raw_ri
    .filter(lambda line: line.strip() != header_ri)
    .map(lambda line: line.strip().split(","))
    .map(lambda f: f[0].strip())
)
sick_ids = sc.broadcast(set(sick_ids_rdd.collect()))

# Find tables that contain at least one sick person
# Join no_disclosure with sick_ids to find which tables have sick people
sick_tables = sc.broadcast(
    set(
        no_disclosure_rdd
        .filter(lambda p: p[0] in sick_ids.value)   # person is in Reported-Illnesses
        .map(lambda p: p[2])                          # get their table
        .distinct()
        .collect()
    )
)

# Find healthy people on sick tables 
# Healthy = NOT in Reported-Illnesses
result_rdd = (
    no_disclosure_rdd
    .filter(lambda p: p[0] not in sick_ids.value)    # not sick
    .filter(lambda p: p[2] in sick_tables.value)     # on a sick table
    .distinct()                                       # no duplicates
)

# Output
results = result_rdd.collect()
print(f"\nHealthy people at risk (close contact with sick person): {len(results):,}\n")
print("First 10:")
for p in results[:10]:
    print(f"  id={p[0]:>8}  name={p[1]:<20}  table={p[2]}")

sc.stop()