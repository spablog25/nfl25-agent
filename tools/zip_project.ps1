param(
  [string]$ProjectRoot = (Resolve-Path ".").Path,
  [string]$ZipOut = "nfl25-agent-clean.zip",
  [switch]$IncludeOddsCache
)

# --- Safety checks ---
if (-not (Test-Path $ProjectRoot)) { throw "ProjectRoot not found: $ProjectRoot" }
$ProjectRoot = (Resolve-Path $ProjectRoot).Path

# --- Paths ---
$stamp = (Get-Date).ToString("yyyyMMdd_HHmmss")
$TempRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("nfl25_zip_" + $stamp)
$StageDir = Join-Path $TempRoot "stage"
New-Item -ItemType Directory -Force -Path $StageDir | Out-Null

# --- EXCLUDES ---
$xd = @(
  ".git", ".idea", "venv", ".venv", "env",
  "__pycache__", ".pytest_cache", ".mypy_cache",
  ".ipynb_checkpoints", ".vscode", ".history",
  "dist", "build", "logs",
  "picks\millions\exports", "picks\millions\diagnostics",
  "picks\survivor\exports"
)
if (-not $IncludeOddsCache) { $xd += "data\odds_cache" }

$xf = @(
  "*.pyc", "*.pyo", "*.pyd", "*.pem", "*.key",
  ".env", ".env.*", "*.secret*", "*.token*",
  ".DS_Store", "Thumbs.db", ".coverage", "coverage.xml"
)

# --- Stage using robocopy (fast, honors excludes) ---
# /MIR mirrors files except for excluded dirs/files; avoids copying large junk.
$rcLog = Join-Path $TempRoot "robocopy.log"
$xdArgs = $xd | ForEach-Object { @("/XD", (Join-Path $ProjectRoot $_)) } | ForEach-Object { $_ }
$xfArgs = $xf | ForEach-Object { @("/XF", $_) } | ForEach-Object { $_ }

# robocopy requires trailing backslash on source to mirror contents
$src = ($ProjectRoot.TrimEnd('\') + '\')
$cmd = @(
  "robocopy", $src, $StageDir,
  "/MIR", "/R:1", "/W:1", "/NFL", "/NDL", "/NP"
) + $xdArgs + $xfArgs

Write-Host "Staging files..." $cmd -ForegroundColor Cyan
$process = Start-Process -FilePath $cmd[0] -ArgumentList $cmd[1..($cmd.Length-1)] -NoNewWindow -PassThru -Wait

if ($process.ExitCode -ge 8) {
  throw "Robocopy failed with exit code $($process.ExitCode). See $rcLog"
}

# --- Create zip ---
$zipFull = if ([System.IO.Path]::IsPathRooted($ZipOut)) { $ZipOut } else { Join-Path $ProjectRoot $ZipOut }
if (Test-Path $zipFull) { Remove-Item $zipFull -Force }
Add-Type -AssemblyName System.IO.Compression.FileSystem
[System.IO.Compression.ZipFile]::CreateFromDirectory($StageDir, $zipFull, [System.IO.Compression.CompressionLevel]::Optimal, $false)

Write-Host "Created zip:" $zipFull -ForegroundColor Green

# --- Cleanup ---
Remove-Item $TempRoot -Recurse -Force
