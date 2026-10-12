"""SIMULATION ONLY: incremental item inserts, synthetic updates, and batch replay."""
import hashlib
import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from time import perf_counter
from uuid import uuid4
from build_silver import ROOT, SparkSession, F, configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import Window


def price_sum(frame):
    return frame.agg(F.sum('price').alias('total')).first()['total']

def protected_inventory():
    folders = [ROOT / 'data' / name for name in ['bronze', 'silver', 'gold', 'published_verification']]
    folders += [ROOT / 'state', ROOT / 'outputs']
    inventory = {}
    for folder in folders:
        if folder.exists():
            for path in sorted(folder.rglob('*')):
                if path.is_file() and path.name != 'incremental_simulation_latest.json':
                    with path.open('rb') as stream:
                        inventory[str(path.relative_to(ROOT))] = hashlib.file_digest(stream, 'sha256').hexdigest()
    return inventory


def main():
    started = perf_counter()
    protected_before = protected_inventory()
    silver = json.loads((ROOT / 'outputs' / 'silver_latest.json').read_text(encoding='utf-8-sig'))
    if silver['status'] != 'complete':
        raise ValueError('Simulation requires a complete Silver snapshot')
    source = Path(silver['tables']['order_items']['path']).resolve()
    if not source.is_relative_to((ROOT / 'data' / 'silver' / 'runs').resolve()):
        raise ValueError('Unexpected source location')
    run_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '_' + uuid4().hex[:8]
    run_root = ROOT / 'data' / 'simulations' / 'incremental' / run_id
    run_root.mkdir(parents=True)
    report = {'simulation': True, 'run_id': run_id, 'status': 'running', 'source_silver_run_id': silver['run_id'],
        'started_at_utc': datetime.now(timezone.utc).isoformat(), 'synthetic_change': 'Add 1.00 to each of ten existing item prices; not real source corrections',
        'merge_key': ['order_id', 'order_item_id'], 'target_path': str(run_root / 'order_items'), 'aws_operations': 0}
    builder = (SparkSession.builder.appName('RetailLake_Incremental_SIMULATION').master('local[2]')
        .config('spark.driver.host', '127.0.0.1').config('spark.driver.bindAddress', '127.0.0.1')
        .config('spark.sql.shuffle.partitions', '2').config('spark.sql.timestampType', 'TIMESTAMP_NTZ')
        .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
        .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
        .config('spark.jars.ivy', str(ROOT / '.cache' / 'ivy'))
        .config('spark.sql.warehouse.dir', (ROOT / '.cache' / 'warehouse').as_uri()))
    spark = None
    try:
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        spark.sparkContext.setLogLevel('WARN')
        original = spark.read.format('delta').load(source.as_uri())
        if original.count() != 112650:
            raise ValueError('Unexpected source row count')
        ranked = original.withColumn('_simulation_row', F.row_number().over(Window.orderBy('order_id', 'order_item_id'))).cache()
        initial = ranked.filter(F.col('_simulation_row') <= 110650).drop('_simulation_row')
        new_rows = ranked.filter(F.col('_simulation_row') > 110650).drop('_simulation_row')
        updated = ranked.filter(F.col('_simulation_row') <= 10).drop('_simulation_row').withColumn('price', (F.col('price') + F.lit(Decimal('1.00'))).cast('decimal(12,2)'))
        replayed = ranked.filter(F.col('_simulation_row').between(11, 15)).drop('_simulation_row')
        batch = new_rows.unionByName(updated).unionByName(replayed).cache()
        if batch.count() != 2015 or batch.groupBy('order_id', 'order_item_id').count().filter(F.col('count') > 1).count():
            raise ValueError('Simulation batch count or key uniqueness failed')
        target = run_root / 'order_items'
        batch.write.format('delta').mode('errorifexists').save((run_root / 'batch_001').as_uri())
        initial.write.format('delta').mode('errorifexists').save(target.as_uri())
        report['initial_rows'] = initial.count()
        report['batch'] = {'new_rows': 2000, 'synthetic_updates': 10, 'unchanged_replays': 5, 'rows': 2015}
        report['source_all_statuses_price_sum'] = str(price_sum(original))
        expected_total = price_sum(original) + Decimal('10.00')
        condition = 't.order_id = s.order_id AND t.order_item_id = s.order_item_id'
        changed = ' OR '.join(f'NOT (t.`{column}` <=> s.`{column}`)' for column in original.columns if column not in ['order_id', 'order_item_id'])
        def merge_once():
            table = DeltaTable.forPath(spark, target.as_uri())
            (table.alias('t').merge(batch.alias('s'), condition)
                .whenMatchedUpdateAll(condition=changed).whenNotMatchedInsertAll().execute())
            return table.history(1).select('operation', 'operationMetrics', 'version').first().asDict()
        first = merge_once()
        merged = spark.read.format('delta').load(target.as_uri())
        merged_count, merged_total = merged.count(), price_sum(merged)
        duplicates = merged.groupBy('order_id', 'order_item_id').count().filter(F.col('count') > 1).count()
        expected = ranked.withColumn('price', F.when(F.col('_simulation_row') <= 10,
            (F.col('price') + F.lit(Decimal('1.00'))).cast('decimal(12,2)')).otherwise(F.col('price'))).drop('_simulation_row')
        if merged_count != 112650 or merged_total != expected_total or duplicates or merged.exceptAll(expected).limit(1).count() or expected.exceptAll(merged).limit(1).count():
            raise ValueError('First MERGE result differs from the expected simulated dataset')
        metrics = first['operationMetrics']
        if int(metrics.get('numTargetRowsInserted', -1)) != 2000 or int(metrics.get('numTargetRowsUpdated', -1)) != 10:
            raise ValueError('First MERGE insert/update metrics differ')
        first_version = first['version']
        print('First MERGE: 2,000 inserted; 10 updated; 112,650 rows; no duplicate keys.', flush=True)
        second = merge_once()
        after_replay = spark.read.format('delta').load(target.as_uri())
        before_replay = spark.read.format('delta').option('versionAsOf', first_version).load(target.as_uri())
        if after_replay.count() != merged_count or price_sum(after_replay) != merged_total or after_replay.exceptAll(before_replay).limit(1).count() or before_replay.exceptAll(after_replay).limit(1).count():
            raise ValueError('Same-batch replay changed table contents')
        replay_metrics = second['operationMetrics']
        if int(replay_metrics.get('numTargetRowsInserted', -1)) or int(replay_metrics.get('numTargetRowsUpdated', -1)):
            raise ValueError('Replay unexpectedly inserted or updated rows')
        if protected_inventory() != protected_before:
            raise ValueError('Verified project files or original ledger changed')
        report.update({'status': 'complete', 'first_merge': first, 'replay_merge': second,
            'final_rows': merged_count, 'duplicate_keys': duplicates, 'final_all_statuses_price_sum': str(merged_total),
            'synthetic_price_increase': '10.00', 'full_expected_rows_match': True, 'replay_content_unchanged': True,
            'protected_project_files_unchanged': True, 'protected_files_checked': len(protected_before),
            'duration_seconds': round(perf_counter() - started, 2), 'finished_at_utc': datetime.now(timezone.utc).isoformat()})
        (run_root / 'simulation_report.json').write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
        (ROOT / 'outputs' / 'incremental_simulation_latest.json').write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
        print('Replay MERGE: 0 inserted, 0 updated; full table content and totals unchanged.', flush=True)
        print('Verified project files and ledger unchanged. SIMULATION report:', run_root / 'simulation_report.json', flush=True)
    except Exception as error:
        report.update({'status': 'failed', 'error': str(error), 'duration_seconds': round(perf_counter() - started, 2)})
        (run_root / 'simulation_failure.json').write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
        raise
    finally:
        if spark is not None:
            spark.stop()

if __name__ == '__main__':
    main()
