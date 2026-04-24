#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
从 Tushare index_weight 拉取指数成分快照，合并为 Qlib instruments 格式（与 all.txt 一致）。

每行：标的代码 \\t 开始日期 \\t 结束日期（YYYY-MM-DD，制表符分隔）

用法：
    python index_weight_to_instruments.py
    python index_weight_to_instruments.py --start_date 20260301 --end_date 20260331
    python index_weight_to_instruments.py --output_dir D:/qlib_data/qlib_data/instruments
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

from tushare_to_qlib import get_tushare_token

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# 指数代码 -> 输出文件名（不含路径）
# 说明：Tushare index_weight 中「中证500」标准代码为 000905.SH；399905.SZ 可能无返回数据，
# 此时 fetch_index_weight 会自动回退为 000905.SH。
DEFAULT_INDEX_FILES: Dict[str, str] = {
    "399300.SZ": "csi300.txt",
    "399905.SZ": "csi500.txt",
}

CSI500_FALLBACK_CODE = "000905.SH"

# index_weight 单次可能行数较多，按年分段请求；段间保守等待避免超频
_CHUNK_SLEEP_SEC = 2.1


def _resolve_token(cli_token: Optional[str]) -> Optional[str]:
    if cli_token:
        return cli_token.strip()
    t = os.environ.get("TUSHARE_API_KEY", "").strip()
    if t:
        logger.info("从环境变量 TUSHARE_API_KEY 获取 Token")
        return t
    t = os.environ.get("TUSHARE_TOKEN", "").strip()
    if t:
        logger.info("从环境变量 TUSHARE_TOKEN 获取 Token")
        return t
    return get_tushare_token()


def _ym(ts: pd.Timestamp) -> int:
    return ts.year * 12 + ts.month


def _parse_trade_date(s: str) -> pd.Timestamp:
    s = str(s).strip()
    return pd.to_datetime(s, format="%Y%m%d")


def _to_yyyy_mm_dd(ts: pd.Timestamp) -> str:
    return ts.strftime("%Y-%m-%d")


def _month_chunks(start_yyyymmdd: str, end_yyyymmdd: str) -> List[Tuple[str, str]]:
    """按月切分请求窗口，避免单次返回超行数上限（如 CSI500 年线）。"""
    start = pd.to_datetime(start_yyyymmdd, format="%Y%m%d")
    end = pd.to_datetime(end_yyyymmdd, format="%Y%m%d")
    chunks: List[Tuple[str, str]] = []
    cur = pd.Timestamp(year=start.year, month=start.month, day=1)
    end_month_first = pd.Timestamp(year=end.year, month=end.month, day=1)
    while cur <= end_month_first:
        month_end = (cur + pd.offsets.MonthEnd(0)).normalize()
        chunk_start = max(start, cur)
        chunk_end = min(end, month_end)
        if chunk_start <= chunk_end:
            chunks.append(
                (chunk_start.strftime("%Y%m%d"), chunk_end.strftime("%Y%m%d"))
            )
        cur = cur + pd.DateOffset(months=1)
    return chunks


def fetch_index_weight(
    pro,
    index_code: str,
    start_yyyymmdd: str,
    end_yyyymmdd: str,
) -> pd.DataFrame:
    """按月分段拉取 index_weight，合并为一张表。"""

    def _pull(code: str) -> pd.DataFrame:
        frames: List[pd.DataFrame] = []
        for sd, ed in _month_chunks(start_yyyymmdd, end_yyyymmdd):
            logger.info("index_weight %s %s ~ %s", code, sd, ed)
            df = pro.index_weight(index_code=code, start_date=sd, end_date=ed)
            time.sleep(_CHUNK_SLEEP_SEC)
            if df is not None and len(df) > 0:
                frames.append(df)
        if not frames:
            return pd.DataFrame(
                columns=["index_code", "con_code", "trade_date", "weight"]
            )
        out = pd.concat(frames, ignore_index=True)
        return out.drop_duplicates(subset=["con_code", "trade_date"], keep="first")

    out = _pull(index_code)
    if (
        out.empty
        and index_code == "399905.SZ"
        and CSI500_FALLBACK_CODE != index_code
    ):
        logger.warning(
            "%s 无数据，改用 %s（Tushare 中证500 成分常用代码）",
            index_code,
            CSI500_FALLBACK_CODE,
        )
        out = _pull(CSI500_FALLBACK_CODE)
    return out


def snapshots_to_segments(df: pd.DataFrame) -> List[Tuple[str, str, str]]:
    """
    将 (con_code, trade_date) 快照合并为存续区间。
    相邻两个月（历月差<=1）视为同一段；更大间隔视为退出后再入。
    """
    if df.empty:
        return []
    need = {"con_code", "trade_date"}
    if not need.issubset(df.columns):
        raise ValueError(f"DataFrame 需包含列: {need}")

    work = df[list(need)].copy()
    work["trade_date"] = work["trade_date"].astype(str).str.strip()
    work["_ts"] = work["trade_date"].map(_parse_trade_date)
    work = work.sort_values(["con_code", "_ts"])

    rows: List[Tuple[str, str, str]] = []
    for con_code, g in work.groupby("con_code", sort=False):
        dates = g["_ts"].drop_duplicates().sort_values()
        if dates.empty:
            continue
        seg_start = dates.iloc[0]
        seg_end = dates.iloc[0]
        prev = dates.iloc[0]
        for i in range(1, len(dates)):
            cur = dates.iloc[i]
            if _ym(cur) - _ym(prev) <= 1:
                seg_end = cur
                prev = cur
            else:
                rows.append(
                    (con_code, _to_yyyy_mm_dd(seg_start), _to_yyyy_mm_dd(seg_end))
                )
                seg_start = cur
                seg_end = cur
                prev = cur
        rows.append((con_code, _to_yyyy_mm_dd(seg_start), _to_yyyy_mm_dd(seg_end)))

    rows.sort(key=lambda x: (x[0], x[1]))
    return rows


def write_instruments(path: Path, segments: List[Tuple[str, str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for sym, s, e in segments:
            f.write(f"{sym}\t{s}\t{e}\n")
    logger.info("已写入 %s，共 %d 行", path, len(segments))


def validate_instruments_file(path: Path) -> None:
    """校验三列制表符格式。"""
    with open(path, "r", encoding="utf-8") as f:
        for i, line in enumerate(f, 1):
            line = line.rstrip("\n")
            if not line.strip():
                raise ValueError(f"{path} 第 {i} 行为空")
            parts = line.split("\t")
            if len(parts) != 3:
                raise ValueError(f"{path} 第 {i} 行列数应为 3，实际 {len(parts)}")
            pd.to_datetime(parts[1])
            pd.to_datetime(parts[2])


def try_qlib_load(instruments_dir: Path, market: str) -> bool:
    """若已安装 qlib，则尝试加载股票池名称（不含 .txt）。"""
    try:
        import qlib
        from qlib.config import REG_CN
        from qlib.data import D

        qlib.init(provider_uri=str(instruments_dir.parent), region=REG_CN)
        inst = D.instruments(market)
        _ = D.list_instruments(
            instruments=inst,
            start_time="2020-01-01",
            end_time="2020-01-10",
            as_list=True,
        )
        logger.info("Qlib 加载 instruments(%r) 抽样成功", market)
        return True
    except Exception as ex:
        logger.warning("跳过 Qlib 验证（未安装或初始化失败）: %s", ex)
        return False


def main() -> None:
    default_out = Path(r"D:\qlib_data\qlib_data\instruments")
    today = datetime.now().strftime("%Y%m%d")

    parser = argparse.ArgumentParser(description="Tushare 指数成分 -> Qlib instruments")
    parser.add_argument("--token", type=str, default=None, help="Tushare Token")
    parser.add_argument(
        "--output_dir",
        type=str,
        default=str(default_out),
        help=f"instruments 目录，默认: {default_out}",
    )
    parser.add_argument(
        "--start_date",
        type=str,
        default="20200102",
        help="开始日期 YYYYMMDD，默认 20200102",
    )
    parser.add_argument(
        "--end_date",
        type=str,
        default=today,
        help=f"结束日期 YYYYMMDD，默认今天 {today}",
    )
    args = parser.parse_args()

    token = _resolve_token(args.token)
    if not token:
        logger.error("未找到 Tushare Token，请设置 TUSHARE_API_KEY 或 --token")
        sys.exit(1)

    try:
        pd.to_datetime(args.start_date, format="%Y%m%d")
        pd.to_datetime(args.end_date, format="%Y%m%d")
    except ValueError:
        logger.error("日期须为 YYYYMMDD")
        sys.exit(1)

    import tushare as ts

    ts.set_token(token)
    pro = ts.pro_api()

    out_dir = Path(args.output_dir)

    for index_code, fname in DEFAULT_INDEX_FILES.items():
        raw = fetch_index_weight(pro, index_code, args.start_date, args.end_date)
        if raw.empty:
            logger.warning("未取到任何数据: %s，跳过写出", index_code)
            continue
        segments = snapshots_to_segments(raw)
        out_path = out_dir / fname
        write_instruments(out_path, segments)
        validate_instruments_file(out_path)

    # Qlib 抽样：market 名 = 文件名去掉 .txt
    for _, fname in DEFAULT_INDEX_FILES.items():
        p = out_dir / fname
        if p.exists():
            try_qlib_load(out_dir, fname.replace(".txt", ""))
            break


if __name__ == "__main__":
    main()
