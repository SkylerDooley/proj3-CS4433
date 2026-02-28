import csv
import os
import random

# ─────────────────────────────────────────────
# Parameters
# ─────────────────────────────────────────────
NUM_PEOPLE    = 500_000
NUM_TABLES    = 1_000
SICK_FRACTION = 0.05
OUTPUT_DIR    = "datasets"
SEED          = 42

os.makedirs(OUTPUT_DIR, exist_ok=True)
random.seed(SEED)

meta_event_path       = os.path.join(OUTPUT_DIR, "MetaEvent.csv")
no_disclosure_path    = os.path.join(OUTPUT_DIR, "MetaEventNoDisclosure.csv")
reported_path         = os.path.join(OUTPUT_DIR, "ReportedIllnesses.csv")

print("Generating datasets …")

with open(meta_event_path,    "w", newline="") as f_meta, \
        open(no_disclosure_path, "w", newline="") as f_nod, \
        open(reported_path,      "w", newline="") as f_rep:

    w_meta = csv.writer(f_meta)
    w_nod  = csv.writer(f_nod)
    w_rep  = csv.writer(f_rep)

    # Headers
    w_meta.writerow(["pi_id", "pi_name", "pi_table", "pi_test"])
    w_nod .writerow(["pi_id", "pi_name", "pi_table"])
    w_rep .writerow(["pi_id", "pi_test"])

    sick_count = 0

    for i in range(1, NUM_PEOPLE + 1):
        pid     = i
        name    = f"Person_{pid:07d}"
        table   = f"Table_{random.randint(1, NUM_TABLES):04d}"
        test    = "sick" if random.random() < SICK_FRACTION else "not-sick"

        w_meta.writerow([pid, name, table, test])
        w_nod .writerow([pid, name, table])

        if test == "sick":
            w_rep.writerow([pid, test])
            sick_count += 1

        if i % 100_000 == 0:
            print(f"  {i:,} / {NUM_PEOPLE:,} rows written …")

print("\n─── Done ──────────────────────────────────────────────")
print(f"Meta-Event             : {meta_event_path}  ({NUM_PEOPLE:,} rows)")
print(f"Meta-Event-No-Disc.    : {no_disclosure_path}  ({NUM_PEOPLE:,} rows)")
print(f"Reported-Illnesses     : {reported_path}  ({sick_count:,} rows, "
      f"{sick_count/NUM_PEOPLE*100:.1f}% sick)")