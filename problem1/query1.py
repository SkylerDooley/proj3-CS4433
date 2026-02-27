"""
Query 1: Return all people pi in MetaEvent where pi.test = "sick"
Using PySpark
"""

from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Query1SickPeople").setMaster("local[*]")
sc   = SparkContext(conf=conf)
sc.setLogLevel("WARN")

# Load data
# Each line: pi_id, pi_name, pi_table, pi_test
raw_rdd = sc.textFile("datasets/MetaEvent.csv")

# Parse
header = raw_rdd.first()                          

parsed_rdd = (
    raw_rdd
    .filter(lambda line: line != header)          # header
    .map(lambda line: line.split(","))            # split CSV columns
    .map(lambda f: (f[0], f[1], f[2], f[3].strip()))  # id, name, table, test
)

# Query 1
#   Each record is a tuple: (pi_id, pi_name, pi_table, pi_test)
#   We keep only those where the 4th field == "sick"
sick_rdd = parsed_rdd.filter(lambda p: p[3] == "sick")

# Output
results = sick_rdd.collect()

print(f"\nTotal sick people: {len(results):,}\n")
print("First 10:")
for p in results[:10]:
    print(f"  id={p[0]:>8}  name={p[1]:<20}  table={p[2]}  test={p[3]}")

sc.stop()