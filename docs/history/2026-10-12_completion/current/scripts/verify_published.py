"""Read all seven Delta tables downloaded from published S3 snapshots."""
import json
from decimal import Decimal
from pathlib import Path
from datetime import datetime, timezone
from build_silver import ROOT, SparkSession, F, T, configure_spark_with_delta_pip


def main():
    publication = json.loads((ROOT / 'outputs' / 'publication_latest.json').read_text(encoding='utf-8'))
    if publication['status'] not in ['files_verified', 'verified']:
        raise ValueError('Published files must pass checksum verification first')
    builder = (SparkSession.builder.appName('RetailLake_Published_Readback').master('local[2]')
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
    checked = {}
    manifests = {}
    try:
        gold = {}
        for snapshot in publication['publications']:
            download = Path(snapshot['verification_path']).resolve()
            if not download.is_relative_to((ROOT / 'data' / 'published_verification').resolve()):
                raise ValueError('Verification path outside project')
            manifest = json.loads((download / 'completion_manifest.json').read_text(encoding='utf-8'))
            if manifest['status'] != 'complete' or manifest['run_id'] != snapshot['run_id']:
                raise ValueError('Published completion mismatch')
            manifests[snapshot['layer']] = manifest
            for name, table in manifest['tables'].items():
                frame = spark.read.format('delta').load((download / name).as_uri())
                rows = frame.count()
                expected_schema = T.StructType.fromJson(table['schema'])
                if rows != table['rows'] or frame.schema != expected_schema:
                    raise ValueError(f"Published {snapshot['layer']}/{name} count or schema mismatch")
                checked[f"{snapshot['layer']}/{name}"] = {'rows': rows, 'schema_matches': True}
                if snapshot['layer'] == 'gold':
                    gold[name] = frame
                print(f"Published {snapshot['layer']}/{name}: {rows:,} rows verified", flush=True)
        if manifests['gold']['source_silver_run_id'] != manifests['silver']['run_id']:
            raise ValueError('Published Gold does not reference the published Silver run')
        baseline = Decimal('13221498.11')
        facts_total = gold['item_facts'].filter(F.col('is_delivered')).agg(F.sum('price').alias('total')).first()['total']
        category_total = gold['category_sales'].agg(F.sum('item_sales').alias('total')).first()['total']
        monthly_total = gold['monthly_sales'].agg(F.sum('item_sales').alias('total')).first()['total']
        if facts_total != baseline or category_total != baseline or monthly_total != baseline:
            raise ValueError('Published sales totals differ from baseline')
        publication['status'] = 'verified'
        publication['verified_at_utc'] = datetime.now(timezone.utc).isoformat()
        publication['delta_readback'] = checked
        publication['sales_totals'] = {'item_level': str(facts_total), 'category': str(category_total), 'monthly': str(monthly_total), 'totals_match': True}
        (ROOT / 'outputs' / 'publication_latest.json').write_text(json.dumps(publication, indent=2) + '\n', encoding='utf-8')
        print(f'All seven published Delta tables verified; sales totals match {baseline}.', flush=True)
    finally:
        spark.stop()

if __name__ == '__main__':
    main()
