#Requires -Version 5.1
<#
.SYNOPSIS
  Incremental pipeline in conda env qlib_zhengshi: CSV (by_date merge) then DumpDataUpdate.

.DESCRIPTION
  Universe Full    -> tushare_to_qlib_async.py (full market list).
  Universe Csi500  -> update_by_csi500.py (instrument pool file).
  Default fetch_mode=by_date, dump_mode=dump_update.

.PARAMETER PauseBetweenSteps
  When Stage=all: run download, press Enter, then convert.

.EXAMPLE
  .\incremental_update.ps1 -Universe Csi500 -StartDate 20250501 -CsvDir D:\qlib_data\csv_data -QlibDir D:\qlib_data\qlib_data

.EXAMPLE
  .\incremental_update.ps1 -VerifyEnvOnly

  Manual verification one-liners: see incremental_update_verify.md in this repo.
#>

param(
    [Parameter(Mandatory = $false)]
    [ValidateSet('Full', 'Csi500')]
    [string] $Universe = 'Csi500',

    [Parameter(Mandatory = $false)]
    [string] $StartDate = '',

    [Parameter(Mandatory = $false)]
    [string] $EndDate = (Get-Date -Format 'yyyyMMdd'),

    [Parameter(Mandatory = $false)]
    [string] $CsvDir = 'D:\qlib_data\csv_data',

    [Parameter(Mandatory = $false)]
    [string] $QlibDir = 'D:\qlib_data\qlib_data',

    [Parameter(Mandatory = $false)]
    [string] $Csi500Input = '',

    [Parameter(Mandatory = $false)]
    [ValidateSet('all', 'download', 'convert')]
    [string] $Stage = 'all',

    [Parameter(Mandatory = $false)]
    [string] $CondaEnv = 'qlib_zhengshi',

    [Parameter(Mandatory = $false)]
    [string] $ExtraPythonPath = '',

    [Parameter(Mandatory = $false)]
    [switch] $VerifyEnvOnly,

    [Parameter(Mandatory = $false)]
    [switch] $PauseBetweenSteps,

    [Parameter(Mandatory = $false)]
    [int] $MaxConcurrent = 16
)

$ErrorActionPreference = 'Stop'
$RepoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path

function Invoke-CondaPython {
    param(
        [string[]] $PythonArgs
    )
    $condaCmd = Get-Command conda -ErrorAction SilentlyContinue
    if (-not $condaCmd) {
        throw 'conda not found in PATH. Install Miniconda/Anaconda and init the shell.'
    }
    $oldPy = $env:PYTHONPATH
    try {
        if ($ExtraPythonPath) {
            if ($oldPy) {
                $sep = [System.IO.Path]::PathSeparator
                $env:PYTHONPATH = $ExtraPythonPath + $sep + $oldPy
            }
            else {
                $env:PYTHONPATH = $ExtraPythonPath
            }
        }
        & conda run -n $CondaEnv --no-capture-output python @PythonArgs
        if ($LASTEXITCODE -ne 0) {
            throw "python exited with code $LASTEXITCODE"
        }
    }
    finally {
        $env:PYTHONPATH = $oldPy
    }
}

if ($VerifyEnvOnly) {
    Write-Host ('== Verify: dump_bin in conda env ' + $CondaEnv + ' ==') -ForegroundColor Cyan
    Invoke-CondaPython @('-c', "from dump_bin import DumpDataUpdate; print('dump_bin OK')")
    exit 0
}

if (-not $StartDate -or $StartDate.Trim().Length -eq 0) {
    $StartDate = $env:QLIB_UPDATE_START_DATE
}
if (-not $StartDate -or $StartDate.Trim().Length -eq 0) {
    throw 'Set -StartDate YYYYMMDD or environment variable QLIB_UPDATE_START_DATE.'
}

function Invoke-Full {
    param([string] $OneStage)
    $scriptPath = Join-Path $RepoRoot 'tushare_to_qlib_async.py'
    $argList = @(
        $scriptPath,
        '--csv_dir', $CsvDir,
        '--output_dir', $QlibDir,
        '--start_date', $StartDate,
        '--end_date', $EndDate,
        '--fetch_mode', 'by_date',
        '--dump_mode', 'dump_update',
        '--max_concurrent', "$MaxConcurrent",
        '--stage', $OneStage
    )
    Write-Host ('conda run -n ' + $CondaEnv + ' python ' + ($argList -join ' ')) -ForegroundColor DarkGray
    Invoke-CondaPython $argList
}

function Invoke-Csi500 {
    param([string] $OneStage)
    $scriptPath = Join-Path $RepoRoot 'update_by_csi500.py'
    $argList = @(
        $scriptPath,
        '--start_date', $StartDate,
        '--end_date', $EndDate,
        '--csv_dir', $CsvDir,
        '--output_dir', $QlibDir,
        '--max_concurrent', "$MaxConcurrent",
        '--fetch_mode', 'by_date',
        '--dump_mode', 'dump_update',
        '--stage', $OneStage
    )
    if ($Csi500Input) {
        $argList += @('--input_file', $Csi500Input)
    }
    Write-Host ('conda run -n ' + $CondaEnv + ' python ' + ($argList -join ' ')) -ForegroundColor DarkGray
    Invoke-CondaPython $argList
}

Write-Host ('RepoRoot=' + $RepoRoot + ' Universe=' + $Universe + ' Stage=' + $Stage + ' ' + $StartDate + '..' + $EndDate) -ForegroundColor Cyan
Write-Host ('CsvDir=' + $CsvDir + ' QlibDir=' + $QlibDir) -ForegroundColor Cyan

if ($Stage -eq 'all' -and $PauseBetweenSteps) {
    if ($Universe -eq 'Full') {
        Invoke-Full 'download'
    }
    else {
        Invoke-Csi500 'download'
    }
    Write-Host ''
    Write-Host '[pause] Inspect CSV, then press Enter to run convert...' -ForegroundColor Yellow
    Read-Host | Out-Null
    if ($Universe -eq 'Full') {
        Invoke-Full 'convert'
    }
    else {
        Invoke-Csi500 'convert'
    }
}
else {
    if ($Universe -eq 'Full') {
        Invoke-Full $Stage
    }
    else {
        Invoke-Csi500 $Stage
    }
}

Write-Host ''
Write-Host 'Done. Optional: -VerifyEnvOnly; manual checks in incremental_update_verify.md' -ForegroundColor Green
