# Run at repo root once all seven initial Airflow loads have succeeded.
# Also usable after each verified incremental batch (or a group of batches).
$ErrorActionPreference = 'Stop'
$repo = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
Push-Location $repo
try {
  & dbt deps
  if ($LASTEXITCODE -ne 0) { throw 'dbt deps failed' }
  & dbt seed
  if ($LASTEXITCODE -ne 0) { throw 'dbt seed failed' }
  & dbt run --select path:models/staging
  if ($LASTEXITCODE -ne 0) { throw 'dbt staging run failed' }
  & dbt snapshot
  if ($LASTEXITCODE -ne 0) { throw 'dbt snapshot failed' }
  & dbt build --select path:models/silver path:models/gold
  if ($LASTEXITCODE -ne 0) { throw 'dbt silver/gold build failed' }
  & dbt test
  if ($LASTEXITCODE -ne 0) { throw 'dbt test failed' }
  Write-Host 'dbt staging, snapshots, Silver, Gold and tests finished.'
} finally {
  Pop-Location
}
