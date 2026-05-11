"""
动态数据清洗管道 (Data Cleaner)
根据 mapping_config 动态生成 Polars 处理管道，将原始数据转换为标准格式。
"""
from __future__ import annotations

import re
from datetime import datetime
from typing import Optional

import polars as pl

from .schema import STANDARD_SCHEMA, FieldType


class DataCleaner:
    """动态数据清洗引擎"""

    def __init__(self):
        self._report: dict = {
            "total_rows": 0,
            "rows_after_clean": 0,
            "duplicates_removed": 0,
            "null_values_filled": 0,
            "type_conversions": [],
            "date_formats_detected": [],
        }

    def process_data(
        self,
        df: pl.DataFrame,
        mapping: dict[str, str],
    ) -> pl.DataFrame:
        """
        processData 核心函数：
        根据 mapping 将原始 DataFrame 转换为标准格式。

        Args:
            df: 原始 Polars DataFrame
            mapping: {标准字段名: 原始列名} 的映射

        Returns:
            清洗后的标准格式 DataFrame
        """
        self._report["total_rows"] = len(df)

        # 步骤 1: 重命名列 → 标准名
        rename_map = {raw: std for std, raw in mapping.items()}
        df = df.rename(rename_map)

        # 只保留标准字段列
        available_cols = [c for c in STANDARD_SCHEMA.keys() if c in df.columns]
        df = df.select(available_cols)

        # 步骤 2: 类型转换
        for col_name in available_cols:
            field_def = STANDARD_SCHEMA[col_name]
            df, conversions = self._convert_type(df, col_name, field_def)
            self._report["type_conversions"].extend(conversions)

        # 步骤 3: 去重（基于 order_id）
        if "order_id" in df.columns:
            before = len(df)
            df = df.drop_nulls(subset=["order_id"])
            df = df.unique(subset=["order_id"], keep="first")
            self._report["duplicates_removed"] = before - len(df)

        # 步骤 4: 空值处理
        null_count = 0
        if "revenue" in df.columns:
            null_count += df["revenue"].null_count()
            df = df.with_columns(pl.col("revenue").fill_null(0))
        if "quantity" in df.columns:
            null_count += df["quantity"].null_count()
            df = df.with_columns(pl.col("quantity").fill_null(0))
        self._report["null_values_filled"] = null_count

        # 步骤 5: 过滤无效行
        df = df.filter(pl.col("revenue") >= 0)

        # 步骤 6: 计算衍生指标
        df = self._compute_derived_metrics(df)

        # 步骤 7: 提取时间维度
        if "sales_date" in df.columns:
            df = self._extract_time_dimensions(df)

        self._report["rows_after_clean"] = len(df)

        return df

    def _convert_type(
        self, df: pl.DataFrame, col_name: str, field_def
    ) -> tuple[pl.DataFrame, list[str]]:
        """根据字段定义进行类型转换"""
        conversions = []

        if field_def.field_type == FieldType.DATE:
            original_nulls = df[col_name].null_count()
            df = df.with_columns(
                pl.col(col_name).map_elements(
                    self._parse_date, return_dtype=pl.String
                ).str.to_datetime(time_zone=None).alias(col_name)
            )
            new_nulls = df[col_name].null_count()
            if original_nulls != new_nulls or original_nulls > 0:
                conversions.append(f"{col_name}: string → datetime (解析失败: {new_nulls} 行)")

        elif field_def.field_type == FieldType.NUMBER:
            original_nulls = df[col_name].null_count()
            df = df.with_columns(
                pl.col(col_name)
                .map_elements(self._parse_number, return_dtype=pl.String)
                .cast(pl.Float64, strict=False)
                .alias(col_name)
            )
            conversions.append(f"{col_name}: string → float64")

        elif field_def.field_type == FieldType.INTEGER:
            df = df.with_columns(
                pl.col(col_name)
                .cast(pl.Float64, strict=False)
                .cast(pl.Int64, strict=False)
                .alias(col_name)
            )
            conversions.append(f"{col_name}: → int64")

        elif field_def.field_type == FieldType.STRING:
            df = df.with_columns(pl.col(col_name).cast(pl.String).alias(col_name))
            conversions.append(f"{col_name}: → string")

        return df, conversions

    @staticmethod
    def _parse_date(value) -> Optional[str]:
        """智能解析各种日期格式"""
        if value is None:
            return None
        s = str(value).strip()
        if not s or s.lower() in ("nan", "none", "null", ""):
            return None

        # 尝试常见格式
        formats = [
            "%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d", "%Y%m%d",
            "%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S",
            "%m/%d/%Y", "%d-%m-%Y", "%d/%m/%Y",
            "%Y年%m月%d日", "%Y年%m月",
        ]
        for fmt in formats:
            try:
                dt = datetime.strptime(s, fmt)
                return dt.strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                continue

        # 尝试从字符串中提取日期
        match = re.search(r'(\d{4}[-/\.]\d{1,2}[-/\.]\d{1,2})', s)
        if match:
            return match.group(1).replace("/", "-").replace(".", "-")

        return None

    @staticmethod
    def _parse_number(value) -> Optional[str]:
        """智能解析各种金额格式"""
        if value is None:
            return None
        s = str(value).strip()
        if not s or s.lower() in ("nan", "none", "null", ""):
            return None

        # 移除货币符号、千分位分隔符、空格
        s = re.sub(r'[¥$€£,\s]', '', s)
        # 处理负号在末尾的情况 (如 "1000-")
        if s.endswith('-'):
            s = '-' + s[:-1]
        # 处理括号表示负数 (如 "(1000)")
        if s.startswith('(') and s.endswith(')'):
            s = '-' + s[1:-1]

        try:
            float(s)
            return s
        except (ValueError, TypeError):
            return None

    def _compute_derived_metrics(self, df: pl.DataFrame) -> pl.DataFrame:
        """计算衍生指标"""
        # 客单价 = 销售额 / 销量
        if "revenue" in df.columns and "quantity" in df.columns:
            df = df.with_columns(
                (pl.col("revenue") / pl.col("quantity").cast(pl.Float64, strict=False)
                 .replace(0, None))
                .round(2)
                .alias("avg_order_value")
            )

        # 达成率 = 销售额 / 销售目标
        # 业绩缺口 = 销售目标 - 销售额
        if "revenue" in df.columns and "sales_target" in df.columns:
            df = df.with_columns(
                (pl.col("revenue") / pl.col("sales_target").replace(0, None) * 100)
                .round(1)
                .alias("achievement_rate"),
                (pl.col("sales_target") - pl.col("revenue"))
                .round(2)
                .alias("revenue_gap"),
            )

        return df

    def _extract_time_dimensions(self, df: pl.DataFrame) -> pl.DataFrame:
        """从日期字段提取时间维度"""
        df = df.with_columns([
            pl.col("sales_date").dt.year().alias("year"),
            pl.col("sales_date").dt.month().alias("month"),
            pl.col("sales_date").dt.quarter().alias("quarter"),
            pl.col("sales_date").dt.week().alias("week"),
            pl.col("sales_date").dt.weekday().alias("day_of_week"),
        ])
        return df

    def get_report(self) -> dict:
        """返回清洗报告"""
        return self._report
