import csv
import os
import random
import string

# ─────────────────────────────────────────────
# Parameters
# ─────────────────────────────────────────────
NUM_CUSTOMERS  = 50_000
NUM_PURCHASES  = 5_000_000
OUTPUT_DIR     = "datasets"
SEED           = 42

os.makedirs(OUTPUT_DIR, exist_ok=True)
random.seed(SEED)

customers_path = os.path.join(OUTPUT_DIR, "Customers.csv")
purchases_path = os.path.join(OUTPUT_DIR, "Purchases.csv")

print("Generating Customers and Purchases datasets …")

# Generate Customers.csv
with open(customers_path, "w", newline="") as f_cust:
    w = csv.writer(f_cust)
    w.writerow(["CustID", "Name", "Age", "Address", "Salary"])

    for cid in range(1, NUM_CUSTOMERS + 1):
        name = ''.join(random.choices(string.ascii_letters, k=random.randint(10, 20)))
        age = random.randint(18, 100)
        address = ''.join(random.choices(string.ascii_letters + " ", k=30))
        salary = round(random.uniform(1000, 10000), 2)

        w.writerow([cid, name, age, address, salary])

        if cid % 10_000 == 0:
            print(f"  Customers: {cid:,} / {NUM_CUSTOMERS:,}")

# Generate Purchases.csv
with open(purchases_path, "w", newline="") as f_pur:
    w = csv.writer(f_pur)
    w.writerow(["TransID", "CustID", "TransTotal", "TransNumItems", "TransDesc"])

    for tid in range(1, NUM_PURCHASES + 1):
        cust_id = random.randint(1, NUM_CUSTOMERS)
        total = round(random.uniform(10, 2000), 2)
        num_items = random.randint(1, 15)
        desc = ''.join(random.choices(string.ascii_letters + " ", k=random.randint(20, 50)))

        w.writerow([tid, cust_id, total, num_items, desc])

        if tid % 500_000 == 0:
            print(f"  Purchases: {tid:,} / {NUM_PURCHASES:,}")

print("\n─── Done ──────────────────────────────────────────────")
print(f"Customers  : {customers_path}  ({NUM_CUSTOMERS:,} rows)")
print(f"Purchases  : {purchases_path}  ({NUM_PURCHASES:,} rows)")
