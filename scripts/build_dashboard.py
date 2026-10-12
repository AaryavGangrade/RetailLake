"""Export a local dashboard dataset from the verified, published Gold snapshot."""
import csv
import hashlib
import json
import re
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from build_silver import ROOT, SparkSession, F, configure_spark_with_delta_pip


def main():
    report = json.loads((ROOT / 'outputs' / 'gold_latest.json').read_text(encoding='utf-8-sig'))
    publication = json.loads((ROOT / 'outputs' / 'publication_latest.json').read_text(encoding='utf-8-sig'))
    published_gold = next(entry for entry in publication['publications'] if entry['layer'] == 'gold')
    if report['status'] != 'complete' or publication['status'] != 'verified' or published_gold['run_id'] != report['run_id']:
        raise ValueError('Dashboard requires the currently verified published Gold run')
    source = ROOT / 'data' / 'gold' / 'runs' / report['run_id']
    manifest = json.loads((ROOT / 'outputs' / 'gold_publication_manifest.json').read_text(encoding='utf-8-sig'))
    if manifest['run_id'] != report['run_id']:
        raise ValueError('Gold publication manifest mismatch')
    # Confirm every current local Gold artifact still matches its published checksum.
    for entry in manifest['files']:
        path = source / entry['relative_path']
        with path.open('rb') as stream:
            checksum = hashlib.file_digest(stream, 'sha256').hexdigest()
        if path.stat().st_size != entry['size_bytes'] or checksum != entry['sha256']:
            raise ValueError(f"Gold artifact changed: {entry['relative_path']}")
    monthly = []
    with (source / 'monthly_sales.csv').open(encoding='utf-8', newline='') as stream:
        for row in csv.DictReader(stream):
            monthly.append({'month': row['purchase_month'], 'orders': int(row['orders']),
                'items': int(row['items_sold']), 'salesCents': int(Decimal(row['item_sales']) * 100)})
    monthly.sort(key=lambda row: row['month'])
    builder = (SparkSession.builder.appName('RetailLake_Dashboard_Export').master('local[2]')
        .config('spark.driver.host', '127.0.0.1').config('spark.driver.bindAddress', '127.0.0.1')
        .config('spark.sql.shuffle.partitions', '2').config('spark.sql.timestampType', 'TIMESTAMP_NTZ')
        .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
        .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
        .config('spark.jars.ivy', str(ROOT / '.cache' / 'ivy'))
        .config('spark.sql.warehouse.dir', (ROOT / '.cache' / 'warehouse').as_uri()))
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    try:
        facts = spark.read.format('delta').load((source / 'item_facts').as_uri()).filter(F.col('is_delivered'))
        grouped = facts.groupBy('purchase_month', 'category').agg(F.count('*').alias('items'),
            F.countDistinct('order_id').alias('orders'), F.sum('price').alias('sales'))
        cube = [{'month': row['purchase_month'], 'category': row['category'], 'items': row['items'],
                 'orders': row['orders'], 'salesCents': int(row['sales'] * 100)}
                for row in grouped.orderBy('purchase_month', 'category').collect()]
        baseline = int(Decimal(report['validation']['item_level_sales']) * 100)
        if sum(row['salesCents'] for row in monthly) != baseline or sum(row['salesCents'] for row in cube) != baseline:
            raise ValueError('Dashboard sales export differs from verified Gold')
        if sum(row['items'] for row in cube) != report['validation']['delivered_items'] or sum(row['orders'] for row in monthly) != report['validation']['delivered_orders_with_items']:
            raise ValueError('Dashboard count export differs from verified Gold')
        categories = {}
        for row in cube:
            totals = categories.setdefault(row['category'], {'items': 0, 'orders': 0, 'salesCents': 0})
            for key in totals:
                totals[key] += row[key]
        with (source / 'category_sales.csv').open(encoding='utf-8', newline='') as stream:
            for row in csv.DictReader(stream):
                expected = {'items': int(row['items_sold']), 'orders': int(row['orders']), 'salesCents': int(Decimal(row['item_sales']) * 100)}
                if categories[row['category']] != expected:
                    raise ValueError('Dashboard category cube differs from verified summary')
        payload = {'meta': {'dataset': 'Olist Brazilian E-Commerce', 'currency': 'BRL',
            'goldRunId': report['run_id'], 'verifiedAt': publication['verified_at_utc'],
            'exportedAt': datetime.now(timezone.utc).isoformat(), 'source': published_gold['s3_uri'],
            'simulation': False, 'salesDefinition': 'Delivered item prices, excluding freight',
            'missingCategoryProducts': 610, 'ordersWithoutItems': 775},
            'monthly': monthly, 'categoryMonthly': cube,
            'baseline': {'salesCents': baseline, 'orders': report['validation']['delivered_orders_with_items'],
                         'items': report['validation']['delivered_items']}}
        destination = ROOT / 'dashboard'
        destination.mkdir(exist_ok=True)
        serialized = json.dumps(payload, ensure_ascii=False).replace('<', '\\u003c').replace('>', '\\u003e')
        (destination / 'data.js').write_text('window.RETAIL_DATA = ' + serialized + ';\n', encoding='utf-8')
        (destination / 'data.json').write_text(json.dumps(payload, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
        index_path = destination / 'index.html'
        if index_path.exists():
            index = index_path.read_text(encoding='utf-8')
            for asset in ['data.js', 'app.js']:
                index = re.sub(r'src="' + re.escape(asset) + r'[^\"]*"',
                               'src="' + asset + '?v=' + report['run_id'] + '"', index)
            index_path.write_text(index, encoding='utf-8')
        print(f'Dashboard data exported: {len(monthly)} months, {len(categories)} categories, {len(cube)} category-month rows.', flush=True)
        print('Sales/count exports reconcile with verified Gold. Simulation data excluded.', flush=True)
    finally:
        spark.stop()

if __name__ == '__main__':
    main()
