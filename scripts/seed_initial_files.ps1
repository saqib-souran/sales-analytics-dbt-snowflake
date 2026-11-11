# Run at repo root AFTER old watcher is stopped and originals are truncated.
# Copies your ORIGINAL unmodified seven CSV files to incoming; they get auto-classified
# as initial by the watcher. Rejected if the source SHA differs from originals.
param([Parameter(Mandatory=$true)][string]$SourceDir)
$ErrorActionPreference = 'Stop'
$repo = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$incoming = Join-Path $repo 'airflow/data/incoming'
$archive = Join-Path $repo ('airflow/data/dev_archive_' + (Get-Date -Format 'yyyyMMdd_HHmmss'))
$hashes = [ordered]@{
  'CATEGORY' = 'ede458dfe4906b5b1fd33e45e1d0b48a59499f9ff32d7604c2ee5d5940cf7156'
  'SUB_CATEGORY' = '8f6ad2425b05ce66eb397422089612fe389556dac855834fdacefa95802aeeef'
  'TYPE_CUSTOMER' = 'ad9614e225720789688f338655b5d014b2ba55301689f6d69b1ee46dafaea0b0'
  'CUSTOMER' = '263382ca2a192352756c6f23268772bb7d735b49a78050493aba6ed723dec0c7'
  'PRODUCT' = '7f8f813c07db82af7b5fca6d54907e9d52bfc541a673370e3dc4626e2266052f'
  'SALE' = 'b32f51ef07c2f8fc346d6089027fd8ca41bca4dd6ebdc3c9d1f03b6288fcb894'
  'DETAIL_SALE' = '81b66becf9a051fb2d4398b10020009af95bb58cd54cede65559a1b69572535f'
}
# Validate ALL files before touching incoming.
$source = (Resolve-Path $SourceDir).Path
foreach ($name in $hashes.Keys) {
  $path = Join-Path $source ($name + '.csv')
  if (-not (Test-Path $path)) { throw "Missing $path" }
  $sha = (Get-FileHash -Algorithm SHA256 -Path $path).Hash.ToLowerInvariant()
  if ($sha -ne $hashes[$name]) { throw "$name hash differs from verified baseline. Do NOT continue." }
}
New-Item -ItemType Directory -Force -Path $incoming | Out-Null
# Prevent previous DEMO/test files from entering original Bronze on startup.
$old = Get-ChildItem -Path $incoming -File -Filter '*.csv'
if ($old.Count -gt 0) {
  New-Item -ItemType Directory -Force -Path $archive | Out-Null
  foreach ($f in $old) { Move-Item -LiteralPath $f.FullName -Destination $archive }
  Write-Host "Archived $($old.Count) old incoming CSV(s) to $archive"
}
foreach ($name in $hashes.Keys) {
  $from = Join-Path $source ($name + '.csv')
  $partial = Join-Path $incoming ($name + '.part')
  $to = Join-Path $incoming ($name + '.csv')
  Copy-Item -LiteralPath $from -Destination $partial -Force
  Move-Item -LiteralPath $partial -Destination $to -Force
}
Write-Host 'Seven verified original CSVs staged. Now start the FINAL watcher:'
Write-Host 'python airflow/watch_folder_generic.py'
