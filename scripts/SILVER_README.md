# Running the local Silver stage

From the RetailLake folder in Antigravity:

```powershell
.\.venv\Scripts\Activate.ps1
python scripts\build_silver.py
```

The script verifies S3-downloaded Bronze files before processing. It sets the local driver address to 127.0.0.1 and, on Windows, uses tools/hadoop-3.4.1 for Hadoop native components. These settings apply to this Python process and its children; system environment variables are not changed.

The Hadoop binaries are a community Windows build, not an Apache-distributed binary release. Release source and SHA256 digest are recorded in tools/hadoop_dependency.json. They match Hadoop 3.4.1 bundled with Spark 4.0.1 and were built with Temurin Java 17.

Silver tables retain all source rows and IDs. Timestamp columns use timestamp_ntz to preserve the source's timezone-free values. Item numbers and product measurements become integers; money becomes decimal(12,2). Product column spelling `lenght` is preserved. Missing category values remain null in Silver and will be labeled Unknown in Gold.

Each run writes a separate directory in data/silver/runs/. It checks conversions, required fields, duplicate keys, reference integrity, and Delta readback. A completion manifest and outputs/silver_latest.json appear only after every table succeeds. Partial run directories must not be published.

This command does not upload output or create AWS compute.
