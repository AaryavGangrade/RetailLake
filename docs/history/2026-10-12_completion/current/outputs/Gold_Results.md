# Local Gold results — 12 October 2026

Status: completed and verified on Windows.

| Table | Rows | Use |
| --- | ---: | --- |
| product_dim | 32,951 | Product attributes and reporting categories |
| item_facts | 112,650 | One row per order item, joined with its order and category |
| category_sales | 74 | Delivered-item sales by category |
| monthly_sales | 23 | Delivered-item sales by purchase month |

Delivered summaries include 110,197 item rows from 96,478 distinct orders with items. All three sales calculations reconcile to 13,221,498.11, matching the Databricks prototype. Sales exclude freight. Category distinct-order counts must not be summed across categories.

Run: 20261011T185542Z_b894d4ac.
Report: gold_latest.json.
Tables and sorted CSV summaries: ../data/gold/runs/20261011T185542Z_b894d4ac/.
Code and definitions: ../scripts/build_gold.py and ../scripts/GOLD_README.md.

All four tables were read back from Delta. Summary data matched the calculation, and the fact join preserved all 112,650 item rows with no duplicate fact keys or unmatched joins. No output has been uploaded to S3 yet.

Runtime note: Windows Spark emitted temporary-directory cleanup errors after verification completed. The successful completion manifest and output tables are preserved. The pipeline returned exit code 0.
