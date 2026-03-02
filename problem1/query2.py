"""
Query 2:
Given MetaEvent-No-Disclosure and Reported-Illnesses,
return all people in MetaEvent-No-Disclosure who appear
in Reported-Illnesses (i.e., they were sick).
"""

from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Query2ReportedSick").setMaster("local[*]")
sc   = SparkContext(conf=conf)
sc.setLogLevel("WARN")

# Load files
no_disc_raw = sc.textFile("datasets/MetaEventNoDisclosure.csv")
reported_raw = sc.textFile("datasets/ReportedIllnesses.csv")

# Parse No-Disclosure
no_disc_header = no_disc_raw.first()
no_disc = (
    no_disc_raw
    .filter(lambda line: line != no_disc_header)
    .map(lambda line: line.split(","))
    .map(lambda f: (f[0], (f[1], f[2])))   # (id, (name, table))
)

# Parse Reported-Illnesses
reported_header = reported_raw.first()
reported = (
    reported_raw
    .filter(lambda line: line != reported_header)
    .map(lambda line: line.split(","))
    .map(lambda f: (f[0], f[1]))           # (id, "sick")
)

# Query 2: join on id
joined = no_disc.join(reported)

# Collect results
results = joined.collect()

print(f"\nTotal reported sick found in No-Disclosure: {len(results):,}\n")
print("First 10:")
for r in results[:10]:
    pid = r[0]
    name, table = r[1][0]
    test = r[1][1]
    print(f"  id={pid}  name={name}  table={table}  test={test}")

sc.stop()
