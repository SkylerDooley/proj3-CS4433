"""
Query 4:
For each table in MetaEvent, return:
  - number of people sitting at the table
  - flag = "healthy" if all are not-sick
           "concern" if at least one is sick
"""

from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Query4TableStatus").setMaster("local[*]")
sc   = SparkContext(conf=conf)
sc.setLogLevel("WARN")

# Load MetaEvent
raw_rdd = sc.textFile("datasets/MetaEvent.csv")

header = raw_rdd.first()

parsed = (
    raw_rdd
    .filter(lambda line: line != header)
    .map(lambda line: line.split(","))
    .map(lambda f: (f[2], f[3].strip()))   # (table, test)
)

# Group by table
grouped = parsed.groupByKey()

# Compute (count, flag)
table_status = grouped.mapValues(
    lambda tests: (
        len(list(tests)),
        "healthy" if all(t == "not-sick" for t in tests) else "concern"
    )
)

# Collect
results = table_status.collect()

print(f"\nTotal tables: {len(results):,}\n")
print("First 10:")
for table, (count, flag) in results[:10]:
    print(f"  table={table}  count={count}  flag={flag}")

sc.stop()
