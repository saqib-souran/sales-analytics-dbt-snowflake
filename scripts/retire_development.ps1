# Delete obsolete dev code AFTER extracting the final package.
# Git history retains old versions if previously committed.
$ErrorActionPreference = 'Stop'
$repo = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
if (-not (Test-Path (Join-Path $repo 'airflow/dags/icommerce_ingestion_dag.py'))) {
  throw 'Final DAG is missing; extract the package first.'
}
$oldPaths = @(
 'airflow/dags/generic_ingestion_dev_dag.py',
 'airflow/dags/category_load_dev_dag.py',
 'airflow/src/icommerce_ingestion/incremental_merge.py',
 'airflow/src/icommerce_ingestion/bronze_append.py',
 'airflow/tests/test_incremental_merge.py',
 'airflow/tests/test_bronze_append.py'
)
foreach ($relative in $oldPaths) {
 $path = Join-Path $repo $relative
 if (Test-Path $path) { Remove-Item -LiteralPath $path -Force; Write-Host "Retired $relative" }
}
Write-Host 'Only the final event-driven ingestion DAG remains from the generic pipeline.'
