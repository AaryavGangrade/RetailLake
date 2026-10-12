"""Build verified Gold Delta snapshots from the last complete Silver run."""
import csv
import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

# Reuse Windows Hadoop, Python worker, and localhost settings from the Silver stage.
from build_silver import ROOT, SparkSession, F, configure_spark_with_delta_pip

BASELINE_SALES = Decimal('13221498.11')

def total_sales(frame):
    value = frame.agg(F.sum('item_sales').alias('total')).first()['total']
    return value if value is not None else Decimal('0.00')

def export_summary(frame, path, sort_column, descending=False):
    ordering = F.col(sort_column).desc() if descending else F.col(sort_column).asc()
    with path.open('w', newline='', encoding='utf-8') as stream:
        writer = csv.writer(stream)
        writer.writerow(frame.columns)
        for row in frame.orderBy(ordering).collect():
            writer.writerow(list(row))

def main():
    silver = json.loads((ROOT / 'outputs' / 'silver_latest.json').read_text(encoding='utf-8-sig'))
    if silver.get('status') != 'complete':
        raise ValueError('Gold requires a completed Silver snapshot')
    silver_paths = {}
    for name in ['orders', 'order_items', 'products']:
        path = Path(silver['tables'][name]['path']).resolve()
        if not path.is_relative_to((ROOT / 'data' / 'silver' / 'runs').resolve()):
            raise ValueError('Silver path is outside the project snapshot directory')
        completion = json.loads((path.parent / 'completion_manifest.json').read_text(encoding='utf-8'))
        if completion.get('status') != 'complete' or completion.get('run_id') != silver['run_id']:
            raise ValueError('Silver completion manifest mismatch')
        silver_paths[name] = path
    run_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '_' + uuid4().hex[:8]
    run_root = ROOT / 'data' / 'gold' / 'runs' / run_id
    run_root.mkdir(parents=True)
    report = {'run_id': run_id, 'silver_run_id': silver['run_id'], 'status': 'in_progress', 'tables': {},
        'metrics': {'item_sales': 'Sum of item price for delivered orders; excludes freight; BRL',
                    'items_sold': 'Number of order-item rows belonging to delivered orders',
                    'orders': 'Distinct order IDs within each group; category counts are not additive',
                    'purchase_month': 'Month of purchase in source wall-clock time, not delivery month'}}
    spark = None
    try:
        builder = (SparkSession.builder.appName('RetailLake_Gold').master('local[2]')
            .config('spark.driver.host', '127.0.0.1')
            .config('spark.driver.bindAddress', '127.0.0.1')
            .config('spark.sql.shuffle.partitions', '2')
            .config('spark.sql.timestampType', 'TIMESTAMP_NTZ')
            .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
            .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
            .config('spark.jars.ivy', str(ROOT / '.cache' / 'ivy'))
            .config('spark.sql.warehouse.dir', (ROOT / '.cache' / 'warehouse').as_uri()))
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        spark.sparkContext.setLogLevel('WARN')
        frames = {name: spark.read.format('delta').load(path.as_uri()) for name, path in silver_paths.items()}
        for name, frame in frames.items():
            if frame.count() != silver['tables'][name]['rows']:
                raise ValueError(f'{name}: Silver count differs from manifest')
        orders, items, products = frames['orders'], frames['order_items'], frames['products']
        product_dim = (products
            .withColumn('category', F.coalesce(F.col('product_category_name'), F.lit('Unknown')))
            .withColumnRenamed('product_name_lenght', 'product_name_length')
            .withColumnRenamed('product_description_lenght', 'product_description_length')).cache()
        # One fact row per (order_id, order_item_id); retain all statuses for future analysis.
        facts = (items.join(orders, 'order_id', 'left')
            .join(product_dim.select('product_id', 'category'), 'product_id', 'left')
            .withColumn('purchase_month', F.date_format('order_purchase_timestamp', 'yyyy-MM'))
            .withColumn('is_delivered', F.col('order_status') == 'delivered')).cache()
        count = facts.count()
        duplicates = facts.groupBy('order_id', 'order_item_id').count().filter(F.col('count') > 1).count()
        missing_joins = facts.filter(F.col('order_status').isNull() | F.col('category').isNull() | F.col('purchase_month').isNull()).count()
        if count != 112650 or duplicates or missing_joins:
            raise ValueError('Fact join row count, keys, or references failed')
        delivered = facts.filter(F.col('is_delivered')).cache()
        aggregations = [F.count('*').alias('items_sold'), F.countDistinct('order_id').alias('orders'), F.sum('price').alias('item_sales')]
        category_sales = delivered.groupBy('category').agg(*aggregations).cache()
        monthly_sales = delivered.groupBy('purchase_month').agg(*aggregations).select('purchase_month', 'orders', 'items_sold', 'item_sales').cache()
        item_total = delivered.agg(F.sum('price').alias('total')).first()['total']
        category_total, monthly_total = total_sales(category_sales), total_sales(monthly_sales)
        delivered_count = delivered.count()
        delivered_orders = delivered.select('order_id').distinct().count()
        category_items = category_sales.agg(F.sum('items_sold').alias('n')).first()['n']
        monthly_items = monthly_sales.agg(F.sum('items_sold').alias('n')).first()['n']
        monthly_orders = monthly_sales.agg(F.sum('orders').alias('n')).first()['n']
        if not (item_total == category_total == monthly_total == BASELINE_SALES):
            raise ValueError('Sales reconciliation differs from Databricks baseline')
        if category_items != delivered_count or monthly_items != delivered_count or monthly_orders != delivered_orders:
            raise ValueError('Summary item or monthly order counts do not reconcile')
        report['validation'] = {'fact_rows': count, 'duplicate_fact_keys': duplicates, 'unmatched_fact_rows': missing_joins,
            'delivered_items': delivered_count, 'delivered_orders_with_items': delivered_orders,
            'item_level_sales': str(item_total), 'category_sales_total': str(category_total), 'monthly_sales_total': str(monthly_total),
            'databricks_baseline_sales': str(BASELINE_SALES), 'totals_match': True,
            'summary_item_counts_match': True, 'monthly_order_counts_match': True}
        print(f'Joins preserved {count:,} item rows. Delivered-item sales: {item_total}', flush=True)
        output_frames = {'product_dim': product_dim, 'item_facts': facts, 'category_sales': category_sales, 'monthly_sales': monthly_sales}
        for name, frame in output_frames.items():
            target = run_root / name
            rows = frame.count()
            frame.write.format('delta').mode('errorifexists').save(target.as_uri())
            saved = spark.read.format('delta').load(target.as_uri())
            if saved.count() != rows or saved.dtypes != frame.dtypes:
                raise ValueError(f'{name}: Delta row count or type mismatch')
            if name in ['category_sales', 'monthly_sales']:
                if total_sales(saved) != BASELINE_SALES or saved.exceptAll(frame).limit(1).count() or frame.exceptAll(saved).limit(1).count():
                    raise ValueError(f'{name}: Delta summary readback differs')
            elif name == 'item_facts':
                saved_total = saved.filter(F.col('is_delivered')).agg(F.sum('price').alias('total')).first()['total']
                if saved_total != BASELINE_SALES:
                    raise ValueError('Saved fact sales differ from baseline')
            report['tables'][name] = {'rows': rows, 'path': str(target), 'readback_passed': True, 'schema': saved.schema.jsonValue()}
            print(f'{name}: {rows:,} rows saved and verified', flush=True)
        # Export the small verified summaries for convenient viewing in Antigravity/Excel.
        export_summary(spark.read.format('delta').load((run_root / 'category_sales').as_uri()), run_root / 'category_sales.csv', 'item_sales', True)
        export_summary(spark.read.format('delta').load((run_root / 'monthly_sales').as_uri()), run_root / 'monthly_sales.csv', 'purchase_month')
        report['status'] = 'complete'
        report['completed_at_utc'] = datetime.now(timezone.utc).isoformat()
        (run_root / 'completion_manifest.json').write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
        (ROOT / 'outputs' / 'gold_latest.json').write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
        print(f'Completed Gold snapshot: {run_root}', flush=True)
    except Exception as error:
        report['status'] = 'failed'
        report['error'] = str(error)
        (run_root / 'failure_report.json').write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
        raise
    finally:
        if spark is not None:
            spark.stop()

if __name__ == '__main__':
    main()
