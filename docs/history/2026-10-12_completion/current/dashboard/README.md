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

Latest verified source and exact run ID: `data.json` meta and the page's Data notes. Full totals: R$13,221,498.11; 96,478 delivered orders with items; 110,197 items. Export: 23 observed months, 74 categories, 1,273 category-month rows.

Browser verification completed on 12 October: default/reset period, metric switches and matching legends, single-month view, category filtering, empty search, pagination, Top 20 rankings, item-count descending sort, notes and CSV download passed. Results are in `outputs/dashboard_browser_checks.json`; screenshot in `docs/history/2026-10-12_completion/dashboard_verified.png`. Fresh dashboard assets contain a snapshot version in their URL to avoid stale cached data. No cloud hosting was deployed. The local HTML file works without the temporary preview server.

Dashboard default updated on 12 October 2026: initial load and Reset filters now select January 2017 through the latest available month (currently August 2018). Earlier 2016 months remain selectable; stored data and full-history baseline reports are unchanged. If January 2017 is absent in a future dataset, the default falls back to its first available month. Dashboard indicators now initially represent the selected 2017-onward period, not the full-history baseline.
