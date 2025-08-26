<#
Publish the Broken Glass Likes Matrix to GitHub Pages by copying the HTML + data
into the `docs/` folder (GitHub Pages source). Optionally commit & push.

USAGE
-----
(venv) PS> .\tools\publish_pages.ps1                           # copy only
(venv) PS> .\tools\publish_pages.ps1 -GitPush                  # copy, then git add/commit/push
(venv) PS> .\tools\publish_pages.ps1 -HtmlName "public\broken_glass_likes_matrix.html" -GitPush

NOTES
-----
• If -HtmlName is provided, the script uses that relative to the repo root.
• Otherwise it auto-detects, preferring these (in order):
  - broken_glass_likes_matrix.html (repo root)
  - broken_glass_likes_matrix_v3.html (repo root)
  - broken_glass_likes_matrix_v2.html (repo root)
  - public\broken_glass_likes_matrix.html
• After running with -GitPush, wait ~1–2 minutes for Pages to update.
#>

[CmdletBinding()] param(
  [switch]$GitPush,
  [string]$HtmlName = '',
  [string]$DocsDirName = 'docs'
)

$ErrorActionPreference = 'Stop'

# repoRoot = parent of the tools/ folder
$repoRoot = Split-Path -Parent $PSScriptRoot
$docsDir  = Join-Path $repoRoot $DocsDirName
$dataSrc  = Join-Path $repoRoot 'data'
$dataDst  = Join-Path $docsDir  'data'

function Short([string]$path){
  if(-not $path){ return $path }
  if($path.StartsWith($repoRoot, [System.StringComparison]::OrdinalIgnoreCase)){
    return ($path.Substring($repoRoot.Length) -replace '^[\\/]+','')
  }
  return $path
}

# Pick HTML file
$htmlSrc = $null
if ($HtmlName) {
  $candidate = Join-Path $repoRoot $HtmlName
  if (-not (Test-Path $candidate)) { throw "HTML not found: $HtmlName (looked for: $candidate)" }
  $htmlSrc = $candidate
} else {
  $candidates = @(
    'broken_glass_likes_matrix.html',
    'broken_glass_likes_matrix_v3.html',
    'broken_glass_likes_matrix_v2.html',
    'public\broken_glass_likes_matrix.html'
  )
  foreach ($c in $candidates) {
    $try = Join-Path $repoRoot $c
    if (Test-Path $try) { $htmlSrc = $try; break }
  }
  if (-not $htmlSrc) {
    throw 'No HTML matrix file found. Pass -HtmlName or place the file at the repo root.'
  }
}

# Verify CSVs
$likesCsv = Join-Path $dataSrc 'survivor_bg_likes_YN.csv'
$schedCsv = Join-Path $dataSrc '2025_nfl_schedule_cleaned.csv'
foreach ($p in @($likesCsv,$schedCsv)) {
  if (-not (Test-Path $p)) { throw "Missing required file: $p" }
}

# Create docs/ structure
New-Item -ItemType Directory -Path $docsDir -Force | Out-Null
New-Item -ItemType Directory -Path $dataDst -Force | Out-Null

# Copy files (HTML becomes docs/index.html)
$indexHtml = Join-Path $docsDir 'index.html'
Copy-Item $htmlSrc $indexHtml -Force
Copy-Item $likesCsv $dataDst -Force
Copy-Item $schedCsv $dataDst -Force

Write-Host "`n✅ Copied:" -ForegroundColor Green
Write-Host ("  HTML : {0} -> {1}" -f ($htmlSrc | Split-Path -Leaf), (Short $indexHtml))
Write-Host ("  CSV  : {0} -> {1}" -f (Split-Path -Leaf $likesCsv), (Short (Join-Path $dataDst (Split-Path -Leaf $likesCsv))))
Write-Host ("  CSV  : {0} -> {1}" -f (Split-Path -Leaf $schedCsv), (Short (Join-Path $dataDst (Split-Path -Leaf $schedCsv))))

if ($GitPush) {
  if (-not (Get-Command git -ErrorAction SilentlyContinue)) {
    throw 'Git is not available on PATH. Install Git or run without -GitPush.'
  }
  Push-Location $repoRoot
  try {
    git add -A | Out-Null
    $stamp = (Get-Date).ToString('yyyy-MM-dd HH:mm')
    git commit -m "Publish BG matrix to Pages ($stamp)" | Out-Null
    git push | Out-Null
    Write-Host "`n🚀 Committed & pushed. GitHub Pages will refresh shortly." -ForegroundColor Cyan
  } catch { throw }
  finally { Pop-Location }
}

Write-Host "Done.`n" -ForegroundColor Green
