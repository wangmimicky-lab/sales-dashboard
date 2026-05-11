"""
分析引擎 + 规则引擎 (Analyzer + Rule Engine)
根据可用字段自动推荐图表类型，执行动态分析。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import polars as pl


# ─── 图表推荐规则 ─────────────────────────────────────────────

@dataclass
class ChartRule:
    """图表推荐规则"""
    rule_id: str
    title: str
    chart_type: str          # echarts 图表类型
    required_fields: list[str]
    optional_fields: list[str] = field(default_factory=list)
    description: str = ""
    config: dict = field(default_factory=dict)


CHART_RULES: list[ChartRule] = [
    # RULE 1: 销售趋势图
    ChartRule(
        rule_id="trend",
        title="销售趋势分析",
        chart_type="line_area",
        required_fields=["sales_date", "revenue"],
        optional_fields=["order_id", "quantity", "sales_target"],
        description="按时间粒度展示销售额和订单量趋势",
        config={"x_field": "sales_date", "y_fields": ["revenue", "orders"]},
    ),
    # RULE 2: 区域分布图
    ChartRule(
        rule_id="region",
        title="区域业绩分布",
        chart_type="bar_horizontal",
        required_fields=["region", "revenue"],
        description="按区域展示销售额排名",
        config={"x_field": "region", "y_field": "revenue"},
    ),
    # RULE 3: KPI 达成进度
    ChartRule(
        rule_id="kpi",
        title="KPI 达成进度",
        chart_type="gauge",
        required_fields=["revenue", "sales_target"],
        description="展示目标完成率和业绩缺口",
        config={"value_field": "revenue", "target_field": "sales_target"},
    ),
    # RULE 4: 销售排行榜
    ChartRule(
        rule_id="ranking",
        title="销售排行榜",
        chart_type="bar_horizontal",
        required_fields=["sales_rep", "revenue"],
        optional_fields=["quantity", "order_id"],
        description="按销售人员展示业绩排名",
        config={"x_field": "sales_rep", "y_field": "revenue", "top_n": 10},
    ),
    # RULE 5: 客户贡献度
    ChartRule(
        rule_id="dealer",
        title="客户/经销商贡献度",
        chart_type="bar_horizontal",
        required_fields=["customer_name", "revenue"],
        optional_fields=["quantity"],
        description="帕累托分析 + TOP 客户排名",
        config={"x_field": "customer_name", "y_field": "revenue", "top_n": 15},
    ),
    # RULE 6: 品类表现
    ChartRule(
        rule_id="product",
        title="产品/品类表现",
        chart_type="pie",
        required_fields=["category", "revenue"],
        optional_fields=["quantity"],
        description="品类销售额占比 + ABC 分类",
        config={"name_field": "category", "value_field": "revenue"},
    ),
    # RULE 7: 客单价分布
    ChartRule(
        rule_id="order_value",
        title="客单价分布",
        chart_type="histogram",
        required_fields=["revenue", "order_id"],
        optional_fields=["quantity"],
        description="订单金额区间分布 + 异常单标注",
        config={"value_field": "revenue"},
    ),
    # RULE 8: 月度热力图
    ChartRule(
        rule_id="heatmap",
        title="月度销售热力图",
        chart_type="heatmap",
        required_fields=["sales_date", "revenue"],
        optional_fields=["month", "year"],
        description="年 × 月 热力图展示销售节奏",
        config={"x_field": "year", "y_field": "month", "value_field": "revenue"},
    ),
]


class RuleEngine:
    """图表推荐规则引擎"""

    def __init__(self, available_fields: set[str]):
        self.available_fields = available_fields

    def recommend_charts(self) -> list[dict]:
        """根据可用字段推荐图表"""
        results = []
        for rule in CHART_RULES:
            # 检查必需字段是否全部可用
            if all(f in self.available_fields for f in rule.required_fields):
                # 检查可选字段
                available_optional = [
                    f for f in rule.optional_fields if f in self.available_fields
                ]
                results.append({
                    "rule_id": rule.rule_id,
                    "title": rule.title,
                    "chart_type": rule.chart_type,
                    "description": rule.description,
                    "config": rule.config,
                    "available_optional_fields": available_optional,
                })
        return results


class Analyzer:
    """销售数据分析引擎"""

    def analyze_all(
        self,
        df: pl.DataFrame,
        granularity: str = "auto",
        filters: Optional[dict] = None,
    ) -> dict:
        """
        执行完整分析，根据可用字段自动推荐并执行分析。
        """
        # 应用筛选
        if filters:
            df = self._apply_filters(df, filters)

        available_fields = set(df.columns)

        # 规则引擎推荐
        engine = RuleEngine(available_fields)
        recommended = engine.recommend_charts()

        # 生成摘要
        summary = self._generate_summary(df)

        # 执行各模块分析
        result = {"summary": summary, "recommended_charts": recommended}

        for chart in recommended:
            rule_id = chart["rule_id"]
            if rule_id == "trend":
                result["trend"] = self.analyze_trend(df, granularity)
            elif rule_id == "region":
                result["region"] = self.analyze_by_field(df, "region")
            elif rule_id == "kpi":
                result["kpi"] = self.analyze_kpi(df)
            elif rule_id == "ranking":
                result["ranking"] = self.analyze_by_field(
                    df, "sales_rep", top_n=chart["config"].get("top_n", 10)
                )
            elif rule_id == "dealer":
                result["dealer"] = self.analyze_dealer(df)
            elif rule_id == "product":
                result["product"] = self.analyze_product(df)
            elif rule_id == "order_value":
                result["order_value"] = self.analyze_order_value(df)
            elif rule_id == "heatmap":
                result["heatmap"] = self.analyze_heatmap(df)

        return result

    def _apply_filters(self, df: pl.DataFrame, filters: dict) -> pl.DataFrame:
        """应用筛选条件"""
        for field_name, value in filters.items():
            if field_name in df.columns and value and value != "全部":
                df = df.filter(pl.col(field_name) == value)
        return df

    def _generate_summary(self, df: pl.DataFrame) -> dict:
        """生成数据摘要"""
        summary = {"total_rows": len(df)}

        if "revenue" in df.columns:
            summary["total_revenue"] = float(df["revenue"].sum())
            summary["avg_revenue"] = float(df["revenue"].mean())
            summary["max_revenue"] = float(df["revenue"].max())
            summary["min_revenue"] = float(df["revenue"].min())

        if "order_id" in df.columns:
            summary["total_orders"] = int(df["order_id"].n_unique())

        if "quantity" in df.columns:
            summary["total_quantity"] = int(df["quantity"].sum())

        if "sales_date" in df.columns:
            dates = df["sales_date"].drop_nulls()
            if len(dates) > 0:
                summary["date_range"] = {
                    "start": str(dates.min()),
                    "end": str(dates.max()),
                }

        if "sales_target" in df.columns:
            total_target = float(df["sales_target"].sum())
            total_revenue = float(df["revenue"].sum()) if "revenue" in df.columns else 0
            summary["sales_target"] = total_target
            if total_target > 0:
                summary["achievement_rate"] = round(total_revenue / total_target * 100, 1)
                summary["revenue_gap"] = round(total_target - total_revenue, 2)

        return summary

    def analyze_trend(self, df: pl.DataFrame, granularity: str = "auto") -> dict:
        """销售趋势分析"""
        if "sales_date" not in df.columns or "revenue" not in df.columns:
            return {"error": "缺少日期或金额字段"}

        # 自动推荐粒度
        if granularity == "auto":
            granularity = self._recommend_granularity(df)

        # 按粒度聚合
        if granularity == "week":
            df_agg = df.with_columns(
                pl.col("sales_date").dt.truncate("1w").alias("period")
            )
        elif granularity == "month":
            df_agg = df.with_columns(
                pl.col("sales_date").dt.truncate("1mo").alias("period")
            )
        else:
            df_agg = df.with_columns(pl.col("sales_date").alias("period"))

        agg_exprs = [
            pl.col("revenue").sum().alias("revenue"),
        ]
        if "order_id" in df.columns:
            agg_exprs.append(pl.col("order_id").n_unique().alias("orders"))
        if "quantity" in df.columns:
            agg_exprs.append(pl.col("quantity").sum().alias("quantity"))

        df_agg = df_agg.group_by("period").agg(agg_exprs).sort("period")

        # 计算环比
        df_agg = df_agg.with_columns(
            (
                (pl.col("revenue") - pl.col("revenue").shift(1))
                / pl.col("revenue").shift(1)
            )
            .round(4)
            .alias("mom_rate")
        )

        return {
            "granularity": granularity,
            "data": [
                {
                    "period": str(row["period"]),
                    "revenue": float(row["revenue"]),
                    "orders": int(row["orders"]) if "orders" in row else None,
                    "quantity": float(row["quantity"]) if "quantity" in row else None,
                    "mom_rate": float(row["mom_rate"]) if row["mom_rate"] is not None else None,
                }
                for row in df_agg.iter_rows(named=True)
            ],
        }

    def analyze_by_field(self, df: pl.DataFrame, field_name: str, top_n: int = 10) -> dict:
        """按指定维度字段分析"""
        if field_name not in df.columns or "revenue" not in df.columns:
            return {"error": f"缺少 {field_name} 或 revenue 字段"}

        agg_exprs = [
            pl.col("revenue").sum().alias("total_revenue"),
            pl.col("revenue").mean().alias("avg_revenue"),
        ]
        if "order_id" in df.columns:
            agg_exprs.append(pl.col("order_id").n_unique().alias("order_count"))
        if "quantity" in df.columns:
            agg_exprs.append(pl.col("quantity").sum().alias("total_quantity"))

        df_agg = df.group_by(field_name).agg(agg_exprs).sort(
            "total_revenue", descending=True
        ).head(top_n)

        total_revenue = float(df_agg["total_revenue"].sum())

        return {
            "field": field_name,
            "top_n": top_n,
            "data": [
                {
                    "name": str(row[field_name]),
                    "total_revenue": float(row["total_revenue"]),
                    "avg_revenue": float(row["avg_revenue"]),
                    "order_count": int(row["order_count"]) if "order_count" in row else None,
                    "total_quantity": float(row["total_quantity"]) if "total_quantity" in row else None,
                    "share": round(float(row["total_revenue"]) / total_revenue * 100, 1) if total_revenue > 0 else 0,
                }
                for row in df_agg.iter_rows(named=True)
            ],
        }

    def analyze_kpi(self, df: pl.DataFrame) -> dict:
        """KPI 达成分析"""
        if "revenue" not in df.columns or "sales_target" not in df.columns:
            return {"error": "缺少 revenue 或 sales_target 字段"}

        total_revenue = float(df["revenue"].sum())
        total_target = float(df["sales_target"].sum())
        achievement_rate = round(total_revenue / total_target * 100, 1) if total_target > 0 else 0
        revenue_gap = round(total_target - total_revenue, 2)

        # 按销售人员分析 KPI
        rep_kpi = None
        if "sales_rep" in df.columns:
            df_kpi = df.group_by("sales_rep").agg([
                pl.col("revenue").sum().alias("total_revenue"),
                pl.col("sales_target").sum().alias("total_target"),
            ]).with_columns([
                (pl.col("total_revenue") / pl.col("total_target").replace(0, None) * 100)
                .round(1).alias("achievement_rate"),
                (pl.col("total_target") - pl.col("total_revenue"))
                .round(2).alias("revenue_gap"),
            ]).sort("total_revenue", descending=True)

            rep_kpi = [
                {
                    "name": str(row["sales_rep"]),
                    "revenue": float(row["total_revenue"]),
                    "target": float(row["total_target"]),
                    "achievement_rate": float(row["achievement_rate"]) if row["achievement_rate"] is not None else None,
                    "revenue_gap": float(row["revenue_gap"]) if row["revenue_gap"] is not None else None,
                }
                for row in df_kpi.iter_rows(named=True)
            ]

        return {
            "total_revenue": total_revenue,
            "total_target": total_target,
            "achievement_rate": achievement_rate,
            "revenue_gap": revenue_gap,
            "rep_kpi": rep_kpi,
        }

    def analyze_dealer(self, df: pl.DataFrame) -> dict:
        """客户/经销商贡献度分析（含帕累托）"""
        if "customer_name" not in df.columns or "revenue" not in df.columns:
            return {"error": "缺少 customer_name 或 revenue 字段"}

        result = self.analyze_by_field(df, "customer_name", top_n=15)

        # 帕累托分析
        data = result["data"]
        total = sum(d["total_revenue"] for d in data)
        cumulative = 0
        for d in data:
            cumulative += d["total_revenue"]
            d["cumulative_share"] = round(cumulative / total * 100, 1) if total > 0 else 0

        # 找出贡献前 80% 的客户数
        cumulative = 0
        top_80_count = 0
        for d in data:
            cumulative += d["total_revenue"]
            top_80_count += 1
            if cumulative / total >= 0.8:
                break

        result["pareto"] = {
            "top_n_count": top_80_count,
            "top_n_share": 80,
            "total_customers": len(data),
            "concentration": round(top_80_count / len(data) * 100, 1) if data else 0,
        }

        return result

    def analyze_product(self, df: pl.DataFrame) -> dict:
        """产品/品类表现分析（含 ABC 分类）"""
        if "category" not in df.columns or "revenue" not in df.columns:
            return {"error": "缺少 category 或 revenue 字段"}

        result = self.analyze_by_field(df, "category", top_n=20)

        # ABC 分类
        data = result["data"]
        total = sum(d["total_revenue"] for d in data)
        cumulative = 0
        for d in data:
            cumulative += d["total_revenue"]
            pct = cumulative / total * 100 if total > 0 else 0
            if pct <= 80:
                d["abc_class"] = "A"
            elif pct <= 95:
                d["abc_class"] = "B"
            else:
                d["abc_class"] = "C"

        # 统计 ABC 数量
        abc_count = {"A": 0, "B": 0, "C": 0}
        for d in data:
            abc_count[d["abc_class"]] += 1
        result["abc_classification"] = abc_count

        return result

    def analyze_order_value(self, df: pl.DataFrame) -> dict:
        """客单价分布分析"""
        if "revenue" not in df.columns or "order_id" not in df.columns:
            return {"error": "缺少 revenue 或 order_id 字段"}

        # 计算客单价
        df_ov = df.with_columns(
            (pl.col("revenue") / pl.col("quantity").cast(pl.Float64, strict=False)
             .replace(0, None))
            .round(2)
            .alias("avg_order_value")
        )

        # 按区间分组
        bins = [0, 100, 500, 1000, 5000, 10000, 50000, float("inf")]
        labels = ["0-100", "100-500", "500-1K", "1K-5K", "5K-10K", "10K-50K", "50K+"]

        # 使用 when/otherwise 手动分箱
        df_ov = df_ov.with_columns(
            pl.when(pl.col("avg_order_value").is_between(0, 100, closed="both")).then(pl.lit("0-100"))
             .when(pl.col("avg_order_value").is_between(100, 500, closed="both")).then(pl.lit("100-500"))
             .when(pl.col("avg_order_value").is_between(500, 1000, closed="both")).then(pl.lit("500-1K"))
             .when(pl.col("avg_order_value").is_between(1000, 5000, closed="both")).then(pl.lit("1K-5K"))
             .when(pl.col("avg_order_value").is_between(5000, 10000, closed="both")).then(pl.lit("5K-10K"))
             .when(pl.col("avg_order_value").is_between(10000, 50000, closed="both")).then(pl.lit("10K-50K"))
             .otherwise(pl.lit("50K+"))
             .alias("price_range")
        )

        df_dist = df_ov.group_by("price_range").agg([
            pl.col("order_id").n_unique().alias("order_count"),
            pl.col("revenue").sum().alias("total_revenue"),
            pl.col("avg_order_value").mean().alias("avg_value"),
        ]).sort("price_range")

        return {
            "data": [
                {
                    "range": str(row["price_range"]),
                    "order_count": int(row["order_count"]),
                    "total_revenue": float(row["total_revenue"]),
                    "avg_value": float(row["avg_value"]),
                }
                for row in df_dist.iter_rows(named=True)
            ],
            "statistics": {
                "mean": float(df_ov["avg_order_value"].mean()),
                "median": float(df_ov["avg_order_value"].median()),
                "std": float(df_ov["avg_order_value"].std()),
                "min": float(df_ov["avg_order_value"].min()),
                "max": float(df_ov["avg_order_value"].max()),
            },
        }

    def analyze_heatmap(self, df: pl.DataFrame) -> dict:
        """月度热力图分析"""
        if "sales_date" not in df.columns or "revenue" not in df.columns:
            return {"error": "缺少 sales_date 或 revenue 字段"}

        # 确保有时间维度
        if "year" not in df.columns:
            df = df.with_columns([
                pl.col("sales_date").dt.year().alias("year"),
                pl.col("sales_date").dt.month().alias("month"),
            ])

        df_hm = df.group_by(["year", "month"]).agg([
            pl.col("revenue").sum().alias("revenue"),
        ]).sort(["year", "month"])

        # 转为矩阵格式
        years = sorted(df_hm["year"].unique().to_list())
        months = list(range(1, 13))

        matrix = []
        for year in years:
            row = {"year": int(year)}
            for month in months:
                val = df_hm.filter(
                    (pl.col("year") == year) & (pl.col("month") == month)
                )
                row[str(month)] = float(val["revenue"].sum()) if len(val) > 0 else 0
            matrix.append(row)

        return {
            "years": [int(y) for y in years],
            "months": months,
            "matrix": matrix,
        }

    def _recommend_granularity(self, df: pl.DataFrame) -> str:
        """根据数据时间跨度推荐粒度"""
        if "sales_date" not in df.columns:
            return "day"
        dates = df["sales_date"].drop_nulls()
        if len(dates) == 0:
            return "day"
        date_range = (dates.max() - dates.min()).dt.total_days()
        days = float(date_range) if date_range is not None else 0

        if days <= 31:
            return "day"
        elif days <= 180:
            return "week"
        else:
            return "month"
