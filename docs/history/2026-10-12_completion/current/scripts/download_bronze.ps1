$ErrorActionPreference = 'Stop'
$awsExecutable = (Get-Command aws -ErrorAction SilentlyContinue).Source
if (-not $awsExecutable) {
    $awsExecutable = Join-Path $env:LOCALAPPDATA 'Programs\Amazon\AWSCLIV2\aws.exe'
}
$pipelineProfile = if ($env:RETAILLAKE_AWS_PROFILE) { $env:RETAILLAKE_AWS_PROFILE } else { 'retaillake-pipeline' }
$projectRoot = Split-Path -Parent $PSScriptRoot
$inputDestination = Join-Path $projectRoot 'data\bronze\olist'
$account = & $awsExecutable sts get-caller-identity --profile $pipelineProfile --region ap-south-1 --query Account --output text
if ($LASTEXITCODE -ne 0 -or $account.Trim() -ne '037315459859') { throw 'Authenticate to the RetailLake project account before downloading.' }
& $awsExecutable s3 cp 's3://retaillake-dev-037315459859-mumbai/bronze/olist/' $inputDestination --recursive --exclude '*' --include '*.csv' --include 'ingestion_manifest.json' --include 'data_quality_report.txt' --profile $pipelineProfile --region ap-south-1 --no-progress
if ($LASTEXITCODE -ne 0) { throw 'S3 download failed.' }
& (Join-Path $projectRoot '.venv\Scripts\python.exe') (Join-Path $PSScriptRoot 'verify_bronze.py')
if ($LASTEXITCODE -ne 0) { throw 'Verification failed; stop processing.' }
