"""Build a complete local Silver snapshot from verified S3 Bronze inputs."""
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
local_temp = ROOT / '.cache' / 'tmp'
local_temp.mkdir(parents=True, exist_ok=True)
os.environ['TEMP'] = str(local_temp)
os.environ['TMP'] = str(local_temp)
import tempfile
tempfile.tempdir = str(local_temp)
hadoop_home = ROOT / 'tools' / 'hadoop-3.4.1'
if os.name == 'nt':
    os.environ['HADOOP_HOME'] = str(hadoop_home)
    os.environ['PATH'] = str(hadoop_home / 'bin') + os.pathsep + os.environ.get('PATH', '')
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F, types as T

DEFINITIONS = {
    'orders': {
        'file': 'olist_orders_dataset.csv',
        'columns': ['order_id', 'customer_id', 'order_status', 'order_purchase_timestamp', 'order_approved_at', 'order_delivered_carrier_date', 'order_delivered_customer_date', 'order_estimated_delivery_date'],
        'timestamps': ['order_purchase_timestamp', 'order_approved_at', 'order_delivered_carrier_date', 'order_delivered_customer_date', 'order_estimated_delivery_date'],
        'integers': [], 'decimals': [],
        'keys': ['order_id'], 'required': ['order_id', 'customer_id', 'order_status', 'order_purchase_timestamp'],
    },
    'order_items': {
        'file': 'olist_order_items_dataset.csv',
        'columns': ['order_id', 'order_item_id', 'product_id', 'seller_id', 'shipping_limit_date', 'price', 'freight_value'],
        'timestamps': ['shipping_limit_date'], 'integers': ['order_item_id'], 'decimals': ['price', 'freight_value'],
        'keys': ['order_id', 'order_item_id'], 'required': ['order_id', 'order_item_id', 'product_id', 'seller_id', 'shipping_limit_date', 'price', 'freight_value'],
    },
    'products': {
        'file': 'olist_products_dataset.csv',
        'columns': ['product_id', 'product_category_name', 'product_name_lenght', 'product_description_lenght', 'product_photos_qty', 'product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm'],
        'timestamps': [], 'decimals': [],
        'integers': ['product_name_lenght', 'product_description_lenght', 'product_photos_qty', 'product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm'],
        'keys': ['product_id'], 'required': ['product_id'],
    },
}

def main():
    subprocess.run([sys.executable, str(ROOT / 'scripts' / 'verify_bronze.py')], check=True)
    run_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '_' + uuid4().hex[:8]
    run_root = ROOT / 'data' / 'silver' / 'runs' / run_id
    run_root.mkdir(parents=True)
    builder = (SparkSession.builder.appName('RetailLake_Silver').master('local[2]')
        .config('spark.driver.host', '127.0.0.1')
        .config('spark.driver.bindAddress', '127.0.0.1')
        .config('spark.sql.shuffle.partitions', '2')
        # Source timestamps have no timezone. Timestamp NTZ preserves source wall-clock values.
        .config('spark.sql.timestampType', 'TIMESTAMP_NTZ')
        .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
        .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
        .config('spark.jars.ivy', str(ROOT / '.cache' / 'ivy'))
        .config('spark.sql.warehouse.dir', (ROOT / '.cache' / 'warehouse').as_uri()))
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    batch = json.loads((ROOT / 'outputs' / 'input_batch.json').read_text(encoding='utf-8-sig'))
    report = {'batch_id': batch['batch_id'], 'input_files': batch['files'], 'run_id': run_id, 'status': 'in_progress', 'source': 'verified S3 Bronze download', 'tables': {}}
    frames = {}
    try:
        manifest = json.loads((ROOT / 'data' / 'bronze' / 'olist' / 'ingestion_manifest.json').read_text(encoding='utf-8-sig'))
        expected = {entry['name']: entry['data_rows'] for entry in manifest['files']}
        for name, definition in DEFINITIONS.items():
            schema = T.StructType([T.StructField(column, T.StringType(), True) for column in definition['columns']])
            raw = (spark.read.schema(schema).option('header', True).option('enforceSchema', False)
                .option('mode', 'FAILFAST').csv((ROOT / 'data' / 'bronze' / 'olist' / definition['file']).as_uri()))
            normalized = raw.select(*[F.when(F.trim(F.col(column)) == '', F.lit(None)).otherwise(F.trim(F.col(column))).alias(column) for column in raw.columns])
            expressions = []
            invalid = []
            for column in raw.columns:
                value = F.col(column)
                if column in definition['timestamps']:
                    converted = F.expr(f"try_cast(`{column}` AS timestamp_ntz)")
                elif column in definition['integers']:
                    converted = F.expr(f"try_cast(`{column}` AS int)")
                elif column in definition['decimals']:
                    converted = F.expr(f"try_cast(`{column}` AS decimal(12,2))")
                else:
                    converted = value
                expressions.append(converted.alias(column))
                if column in definition['timestamps'] + definition['integers'] + definition['decimals']:
                    invalid.append(F.sum(F.when(value.isNotNull() & converted.isNull(), 1).otherwise(0)).alias(column))
            conversion_failures = normalized.agg(*invalid).first().asDict() if invalid else {}
            if any(conversion_failures.values()):
                raise ValueError(f'{name}: conversion failures {conversion_failures}')
            frame = normalized.select(*expressions).cache()
            count = frame.count()
            missing = frame.agg(*[F.sum(F.when(F.col(column).isNull(), 1).otherwise(0)).alias(column) for column in definition['required']]).first().asDict()
            duplicates = frame.groupBy(*definition['keys']).count().filter(F.col('count') > 1).count()
            if count != expected[definition['file']] or any(missing.values()) or duplicates:
                raise ValueError(f'{name}: invalid count, required fields, or duplicate keys')
            report['tables'][name] = {'rows': count, 'conversion_failures': conversion_failures, 'missing_required': missing, 'duplicate_keys': duplicates, 'schema': frame.schema.jsonValue(), 'path': str(run_root / name)}
            frames[name] = frame
            print(f'{name}: {count:,} rows; conversions and keys passed', flush=True)
        orphan_orders = frames['order_items'].join(frames['orders'].select('order_id'), 'order_id', 'left_anti').count()
        orphan_products = frames['order_items'].join(frames['products'].select('product_id'), 'product_id', 'left_anti').count()
        report['unmatched_item_orders'] = orphan_orders
        report['unmatched_item_products'] = orphan_products
        if orphan_orders or orphan_products:
            raise ValueError('Order items reference missing orders or products')
        report['orders_without_items'] = frames['orders'].join(frames['order_items'].select('order_id').distinct(), 'order_id', 'left_anti').count()
        report['products_without_category'] = frames['products'].filter(F.col('product_category_name').isNull()).count()
        # Keep category nulls in Silver; reporting will label them Unknown in Gold.
        for name, frame in frames.items():
            frame.write.format('delta').mode('errorifexists').save((run_root / name).as_uri())
            readback = spark.read.format('delta').load((run_root / name).as_uri())
            if readback.count() != report['tables'][name]['rows'] or readback.schema != frame.schema:
                raise ValueError(f'{name}: Delta readback mismatch')
            print(f'{name}: Delta saved and read back successfully', flush=True)
        report['status'] = 'complete'
        report['completed_at_utc'] = datetime.now(timezone.utc).isoformat()
        (run_root / 'completion_manifest.json').write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
        (ROOT / 'outputs' / 'silver_latest.json').write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
        print(f'Completed Silver snapshot: {run_root}', flush=True)
    except Exception as error:
        report['status'] = 'failed'
        report['error'] = str(error)
        (run_root / 'failure_report.json').write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
        raise
    finally:
        spark.stop()

if __name__ == '__main__':
    main()


