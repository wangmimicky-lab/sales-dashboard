"""
数据清洗核心
- 去重（基于订单号）
- 日期格式标准化
- 金额/数量格式清理（去除符号、逗号等）
- 空值处理
- 异常值标记
"""
from __future__ import annotations

import re
from datetime import datetime
from typing import Optional

import polars as pl

from .config import AnalysisConfig


class DataCleaner:
    """基于 Polars 的数据清洗管道"""

    def __init__(self, config: Optional[AnalysisConfig] = None):
        self.config = config or AnalysisConfig()
        self.cleaning_log: list[str] = []      # 清洗操作记录
        self.stats: dict = {}                   # 清洗前后统计

    # ─── 核心清洗管道 ─────────────────────────────────────────

    def clean(self, df: pl.DataFrame, field_mapping: dict[str, str]) -> pl.DataFrame:
        """
        执行完整清洗流程。
        df: 原始 Polars DataFrame
        field_mapping: {原始列名: 标准字段名}
        返回: 清洗后的 DataFrame
        """
        self.cleaning_log = []
        initial_rows = len(df)

        # 1. 重命名列（按映射表）
        df = self._rename_columns(df, field_mapping)

        # 2. 去除完全空行
        df = self._drop_empty_rows(df)

        # 3. 去重（基于订单号）
        df = self._deduplicate(df)

        # 4. 日期标准化
        df = self._standardize_dates(df)

        # 5. 数值格式清理
        df = self._clean_numeric_columns(df)

        # 6. 空值处理
        df = self._handle_nulls(df)

        # 7. 记录统计
        final_rows = len(df)
        self.stats = {
            "initial_rows": initial_rows,
            "final_rows": final_rows,
            "removed_rows": initial_rows - final_rows,
            "removal_rate": f"{(initial_rows - final_rows) / max(initial_rows, 1) * 100:.1f}%",
        }

        return df

    # ─── 清洗步骤实现 ─────────────────────────────────────────

    def _rename_columns(self, df: pl.DataFrame, mapping: dict[str, str]) -> pl.DataFrame:
        """按映射表重命名列"""
        rename_map = {raw: standard for raw, standard in mapping.items() if raw in df.columns}
        df = df.rename(rename_map)
        self.cleaning_log.append(f"列重命名: {len(rename_map)} 列 → 标准字段")
        return df

    def _drop_empty_rows(self, df: pl.DataFrame) -> pl.DataFrame:
        """去除所有列都为空的行"""
        before = len(df)
        # 判断全空：所有列都为 null 或空字符串
        if "order_id" in df.columns:
            df = df.filter(
                pl.col("order_id").is_not_null()
                & (pl.col("order_id").cast(pl.Utf8).str.strip_chars() != "")
            )
        else:
            # 没有订单号时，按所有非空列过滤
            non_null_cols = [c for c in df.columns if df[c].null_count() < len(df)]
            if non_null_cols:
                filters = pl.col(non_null_cols[0]).is_not_null()
                for c in non_null_cols[1:4]:  # 最多检查前4列，避免性能问题
                    filters = filters & pl.col(c).is_not_null()
                df = df.filter(filters)

        removed = before - len(df)
        if removed > 0:
            self.cleaning_log.append(f"去除空行: {removed} 行")
        return df

    def _deduplicate(self, df: pl.DataFrame) -> pl.DataFrame:
        """基于订单号去重（保留第一条）"""
        if "order_id" not in df.columns:
            return df

        before = len(df)
        df = df.unique(subset=["order_id"], keep="first")
        removed = before - len(df)

        if removed > 0:
            self.cleaning_log.append(f"去重: 移除 {removed} 条重复订单")
        return df

    def _standardize_dates(self, df: pl.DataFrame) -> pl.DataFrame:
        """日期格式标准化为 YYYY-MM-DD"""
        if "order_date" not in df.columns:
            return df

        df = df.with_columns(
            pl.col("order_date").cast(pl.Utf8, strict=False)
        )

        # 清理常见日期格式问题
        df = df.with_columns(
            pl.col("order_date").str.replace_all("[年月]", "-")
        )
        df = df.with_columns(
            pl.col("order_date").str.replace_all("日$", "")
        )
        # 合并连续横杠：2024--3--5 → 2024-3-5
        df = df.with_columns(
            pl.col("order_date").str.replace_all("-{2,}", "-")
        )
        df = df.with_columns(
            pl.col("order_date").str.replace_all(r"[/\s]+", "")
        )
        # 插入分隔符：YYYYMMDD → YYYY-MM-DD（仅纯数字 8 位）
        df = df.with_columns(
            pl.when(
                (pl.col("order_date").str.len_chars() == 8)
                & pl.col("order_date").str.contains(r"^\d{8}$")
            )
            .then(
                pl.col("order_date").str.slice(0, 4) + "-" +
                pl.col("order_date").str.slice(4, 2) + "-" +
                pl.col("order_date").str.slice(6, 2)
            )
            .otherwise(pl.col("order_date"))
        )

        # 尝试解析为日期
        df = df.with_columns(
            pl.col("order_date").str.to_date(format="%Y-%m-%d", strict=False)
        )

        # 记录转换情况
        null_count = df["order_date"].null_count()
        if null_count > 0:
            self.cleaning_log.append(f"日期转换: {null_count} 条无法解析为标准日期格式")
        else:
            self.cleaning_log.append("日期转换: 全部成功")

        return df

    def _clean_numeric_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        """清理数值列：去除货币符号、逗号、百分号等"""
        numeric_cols = ["sales_amount", "quantity", "unit_price", "discount_rate"]
        cleaned_count = 0

        for col in numeric_cols:
            if col not in df.columns:
                continue

            # 先转字符串
            df = df.with_columns(pl.col(col).cast(pl.Utf8, strict=False))

            # 清理符号
            df = df.with_columns(
                pl.col(col).str.replace_all("[¥$€£,]", "")
            )
            df = df.with_columns(
                pl.col(col).str.replace_all("%", "")
            )
            df = df.with_columns(
                pl.col(col).str.strip_chars()
            )

            # 折扣率特殊处理：如果是 "15%" 或 "0.15" 格式
            if col == "discount_rate":
                df = df.with_columns(
                    pl.when(pl.col("discount_rate").str.contains("%$"))
                    .then(
                        pl.col("discount_rate")
                        .str.replace_all("%", "")
                        .cast(pl.Float64, strict=False)
                        / 100.0
                    )
                    .otherwise(
                        pl.col("discount_rate")
                        .cast(pl.Float64, strict=False)
                    )
                    .alias("discount_rate")
                )
            else:
                df = df.with_columns(
                    pl.col(col).cast(pl.Float64, strict=False)
                )

            cleaned_count += 1

        if cleaned_count > 0:
            self.cleaning_log.append(f"数值清理: {cleaned_count} 个数值列已标准化")
        return df

    def _handle_nulls(self, df: pl.DataFrame) -> pl.DataFrame:
        """空值处理：数值列填 0，文本列填 "未知"，日期列过滤"""
        null_summary = {}

        for col in df.columns:
            null_count = df[col].null_count()
            if null_count == 0:
                continue

            null_summary[col] = null_count

            if df[col].dtype in (pl.Float64, pl.Int64, pl.Int32):
                df = df.with_columns(pl.col(col).fill_null(0.0))
            elif df[col].dtype == pl.Utf8:
                df = df.with_columns(pl.col(col).fill_null("未知"))
            # 日期列的空值暂不填充，后续分析时跳过

        if null_summary:
            parts = [f"{k}: {v}" for k, v in null_summary.items()]
            self.cleaning_log.append(f"空值处理: {', '.join(parts)}")

        return df

    # ─── 报告 ─────────────────────────────────────────────────

    def get_report(self) -> dict:
        """返回清洗报告"""
        return {
            "stats": self.stats,
            "log": self.cleaning_log,
        }
