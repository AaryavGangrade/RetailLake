# RetailLake Gold stage

Run from the project folder with the environment activated:

```powershell
python scripts\build_gold.py
```

## What it creates

| Table | One row represents | Purpose |
| --- | --- | --- |
| product_dim | One product | Product attributes and reporting category |
| item_facts | One order item | Join item prices and shipping with order status and purchase time |
| category_sales | One category | Delivered-item sales, item counts, and distinct order counts |
| monthly_sales | One purchase month | Delivered-item sales, item counts, and distinct order counts |

All order statuses are retained in item_facts. Only delivered orders contribute to sales summaries. Each order-item record counts as one item; the dataset has no separate quantity column. Sales sum item prices and exclude freight. Distinct order counts cannot be summed across categories because one order may include several categories.

Missing categories become Unknown in product_dim. Gold corrects the source spelling product_name_lenght and product_description_lenght to product_name_length and product_description_length. Silver retains the original column names. Timestamp NTZ preserves the source's timezone-free values.

## Output and verification

The pipeline reads outputs/silver_latest.json and requires the source run's completion manifest. It validates Silver counts, preserves 112,650 item fact rows, checks joined keys/references, and reconciles category/monthly sales with delivered item prices and the prototype total 13,221,498.11.

Each run writes four Delta tables in data/gold/runs/<run_id>/. Each table is read back; summary rows and sums are compared with their calculated results. Small CSV exports are sorted by category sales descending and purchase month ascending. These CSVs are views for convenient inspection; the Delta tables remain the curated outputs.

A completion_manifest.json and outputs/gold_latest.json appear only after the full run succeeds. Failed run folders are not complete snapshots. No AWS resources are created, and output publication to S3 is a later milestone.
