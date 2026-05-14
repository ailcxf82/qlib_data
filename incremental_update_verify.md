# Incremental update: manual verification (qlib_zhengshi)

Replace paths with your `CsvDir` / `QlibDir`. Run from any shell where `conda` is on PATH.

## 0) dump_bin import

```powershell
conda run -n qlib_zhengshi python -c "from dump_bin import DumpDataUpdate; print('dump_bin OK')"
```

If this fails, add Qlib `scripts` to `PYTHONPATH`, or pass `-ExtraPythonPath` to `incremental_update.ps1`.

## 1) CSV max `datetime` (spot check)

```powershell
conda run -n qlib_zhengshi python -c "import pandas as pd; from pathlib import Path; p=Path(r'D:\qlib_data\csv_data'); f=next(p.glob('*.csv')); d=pd.read_csv(f); print(f, d['datetime'].max())"
```

## 2) Qlib calendar first / last line

```powershell
conda run -n qlib_zhengshi python -c "from pathlib import Path; t=(Path(r'D:\qlib_data\qlib_data')/'calendars'/'day.txt').read_text(encoding='utf-8').strip().splitlines(); print(t[0], t[-1])"
```

## 3) Qlib `D.features` (PowerShell: escape `$` in field names)

```powershell
conda run -n qlib_zhengshi python -c "import qlib; from qlib.data import D; qlib.init(provider_uri=r'D:\qlib_data\qlib_data', region='cn'); print(D.features(['000001.SZ'], ['`$close'], '2025-05-01', '2025-05-13').head())"
```

In **cmd.exe**, use a single `$` before `close` inside the Python string (no backtick).
