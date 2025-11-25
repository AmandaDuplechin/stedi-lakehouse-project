# STEDI Human Balance Analytics — Data Lakehouse Project

This project builds a complete data lakehouse for the STEDI Step Trainer using:
- AWS S3
- AWS Glue
- AWS Athena
- PySpark

The ETL pipeline progresses data through Landing → Trusted → Curated zones and produces a final dataset used to train a machine learning model that predicts human balance.

---

## Table Row Counts (Verification)

| Table | Row Count |
|-------|-----------|
| customer_landing | 956 |
| accelerometer_landing | 81273 |
| step_trainer_landing | 28680 |
| customer_trusted | 482 |
| accelerometer_trusted | 40981 |
| step_trainer_trusted | 14460 |
| customer_curated | 482 |
| machine_learning_curated | 43681 |

All values match the project rubric.

---

## Repository Contents

.
├── scripts/ # AWS Glue ETL scripts (PySpark)
│ ├── customer_landing_to_trusted.py
│ ├── accelerometer_landing_to_trusted.py
│ ├── customer_trusted_to_curated.py
│ ├── step_trainer_landing_to_trusted.py
│ └── machine_learning_curated.py
│
└── screenshots/ # Athena SQL validation screenshots
---

## ETL Workflow Summary

1. **Landing Zone**
   - JSON data loaded to S3
   - Tables created in Glue Catalog
   - Counts validated through Athena

2. **Trusted Zone**
   - `customer_landing_to_trusted.py` filters customers with research consent
   - `accelerometer_landing_to_trusted.py` keeps only accelerometer readings for consenting customers
   - `step_trainer_landing_to_trusted.py` keeps only step trainer readings for consenting customers with accelerometer data

3. **Curated Zone**
   - `customer_trusted_to_curated.py` identifies only customers who have accelerometer data
   - `machine_learning_curated.py` joins accelerometer + step trainer tables on timestamp to build the ML dataset

---

## Completion

All ETL jobs executed successfully, wrote Parquet to S3, and updated the Glue Data Catalog.
Athena validated correct row counts for every table.

# stedi-lakehouse-project
