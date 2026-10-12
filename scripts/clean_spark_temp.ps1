param([switch]$Apply)
$ErrorActionPreference = 'Stop'
$projectRoot = [IO.Path]::GetFullPath((Split-Path -Parent $PSScriptRoot))
$tempRoot = [IO.Path]::GetFullPath((Join-Path $projectRoot '.cache\tmp'))
if (-not $tempRoot.StartsWith($projectRoot + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)) { throw 'Temporary path escaped the project.' }
$stateRoot = Join-Path $projectRoot 'state'
New-Item -ItemType Directory -Path $stateRoot -Force | Out-Null
$lockPath = Join-Path $stateRoot 'pipeline.lock'
try { $cleanupLock = [IO.File]::Open($lockPath, [IO.FileMode]::OpenOrCreate, [IO.FileAccess]::ReadWrite, [IO.FileShare]::None) }
catch { throw 'Pipeline state is in use. Wait for the runner to finish before cleanup.' }
try {
if (Get-Process -Name java,javaw -ErrorAction SilentlyContinue) { throw 'A Java process is running. Close Spark before cleaning temporary files.' }
$candidates = @()
if (Test-Path -LiteralPath $tempRoot) {
    if ((Get-Item -LiteralPath $tempRoot).Attributes -band [IO.FileAttributes]::ReparsePoint) { throw 'Temporary root is a reparse point.' }
    foreach ($directory in Get-ChildItem -LiteralPath $tempRoot -Directory -Filter 'spark-*') {
        $resolved = [IO.Path]::GetFullPath($directory.FullName)
        if (-not $resolved.StartsWith($tempRoot + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)) { throw 'Candidate escaped temporary root.' }
        $children = @(Get-ChildItem -LiteralPath $resolved -Recurse -Force)
        if ($directory.Attributes -band [IO.FileAttributes]::ReparsePoint -or ($children | Where-Object { $_.Attributes -band [IO.FileAttributes]::ReparsePoint })) { throw 'Candidate contains a reparse point; inspect manually.' }
        $bytes = ($children | Where-Object { -not $_.PSIsContainer } | Measure-Object Length -Sum).Sum
        $candidates += [PSCustomObject]@{path=$resolved; bytes=[long]$bytes}
    }
}
foreach ($entry in $candidates) {
    if ($Apply) { Remove-Item -LiteralPath $entry.path -Recurse -Force }
}
$report = @{mode=$(if ($Apply) {'applied'} else {'dry_run'}); timestamp_utc=[DateTime]::UtcNow.ToString('o'); directories=$candidates; count=$candidates.Count; total_bytes=[long](($candidates | Measure-Object bytes -Sum).Sum)}
($report | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath (Join-Path $projectRoot 'outputs\spark_temp_cleanup.json') -Encoding UTF8
$report | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $projectRoot ("logs\spark_temp_cleanup_" + [DateTime]::UtcNow.ToString('yyyyMMddTHHmmssffff') + '.json')) -Encoding UTF8
Write-Output "$($report.mode): $($report.count) Spark temporary directories, $($report.total_bytes) bytes."

} finally { $cleanupLock.Dispose() }
