# RetailLake local dashboard

Open index.html in a modern browser. Keep index.html, styles.css, app.js, and data.js together. The dashboard works offline: no CDN, package installation, web hosting, or AWS requests are needed to view/filter it.

From the RetailLake folder in PowerShell:

```powershell
Start-Process .\dashboard\index.html
```

## Included

- Delivered-item sales in BRL, distinct delivered orders with items, item counts, and sales per order.
- Purchase-month range and single-category filters.
- Trend switch: sales, orders, or items; point details on hover/keyboard focus.
- Top 5/10/20 category rankings and hover details.
- Searchable, sortable, paginated category table.
- CSV export of category totals for the selected period/category (table search affects the table view only).
- Data notes, metric exclusions, snapshot provenance, and dataset limitations.

The monthly category cube enables the filters. All-category order indicators use monthly distinct orders, not the sum of overlapping category order counts. Within a single category, monthly order counts can be summed because one order has one purchase month. Unknown categories are retained. Missing calendar months are not silently filled with zero; the trend breaks across missing months.

Amounts use integer cents in the dashboard data. The displayed sales definition is delivered item prices excluding freight. The incremental simulation is not included.

## Rebuild after a new verified publication

```powershell
.\.venv\Scripts\python.exe scripts\build_dashboard.py
```

The exporter requires a complete current Gold run whose publication status is verified. It checks local Gold artifacts against the published SHA256 inventory, reads real item_facts to produce a category-month cube, and reconciles all monthly/category sums and counts before replacing data.js/data.json. It does not contact AWS or upload anything.

Verified source: Gold run 20261011T185542Z_b894d4ac. Full totals: R$13,221,498.11; 96,478 delivered orders with items; 110,197 item rows. Export: 23 observed months, 74 categories, 1,273 category-month rows.

Validation scope: exporter data checks passed, and a temporary local preview served HTTP 200. Browser interaction/visual tests were not run. The Codex browser opening request was queued rather than confirmed visible; use the local HTML file directly. Windows Spark shutdown cleanup messages occurred after successful data export. No cloud deployment was performed.
