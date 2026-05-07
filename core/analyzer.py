"""
分析引擎 - 4 大分析模块 + 描述型文字结论
所有分析基于 Polars DataFrame，返回结构化 JSON 供前端渲染
"""
from __future__ import annotations

import math
from datetime import datetime
from typing import Optional

import polars as pl

from .config import AnalysisConfig


class Analyzer:
    """销售数据分析引擎"""

    def __init__(self, config: Optional[AnalysisConfig] = None):
        self.config = config or AnalysisConfig()

    # ─── 核心方法：一次性分析全部模块 ─────────────────────────

    def analyze_all(self, df: pl.DataFrame, granularity: str = "auto") -> dict:
        """
        执行完整分析，返回 4 大模块结果
        """
        result = {
            "summary": self._generate_summary(df),
            "trend": self.analyze_trend(df, granularity),
            "dealer": self.analyze_dealer(df),
            "product": self.analyze_product(df),
            "order": self.analyze_order(df),
        }

        # 区域分析
        if "region" in df.columns:
            result["region"] = self.analyze_region(df)

        # 经销商活跃度
        if "customer_name" in df.columns and "order_date" in df.columns:
            result["activity"] = self.analyze_activity(df)

        return result

    # ─── 模块 1: 销售趋势与节奏 ──────────────────────────────

    def analyze_trend(self, df: pl.DataFrame, granularity: str = "auto") -> dict:
        """分析销售趋势：日/周/月走势 + 环比 + 异常波动"""
        if "order_date" not in df.columns or "sales_amount" not in df.columns:
            return {"error": "缺少日期或金额字段"}

        # 自动推荐粒度（除非用户手动指定）
        if granularity == "auto":
            granularity = self._recommend_granularity(df)

        # 按粒度聚合
        if granularity == "week":
            df_agg = df.with_columns(
                pl.col("order_date").dt.truncate("1w").alias("period")
            )
        elif granularity == "month":
            df_agg = df.with_columns(
                pl.col("order_date").dt.truncate("1mo").alias("period")
            )
        else:
            df_agg = df.with_columns(
                pl.col("order_date").alias("period")
            )

        df_agg = df_agg.group_by("period").agg(
            pl.col("sales_amount").sum().alias("sales"),
            pl.col("order_id").n_unique().alias("orders"),
        ).sort("period")

        # 计算环比
        df_agg = df_agg.with_columns(
            (
                (pl.col("sales") - pl.col("sales").shift(1))
                / pl.col("sales").shift(1)
            ).alias("mom_rate")
        )

        # 计算移动平均（3 期）
        df_agg = df_agg.with_columns(
            pl.col("sales").rolling_mean(window_size=3).alias("ma3")
        )

        # 异常波动标注
        df_agg = df_agg.with_columns(
            (pl.col("mom_rate").abs() > self.config.VOLATILITY_THRESHOLD).alias("is_anomaly")
        )

        # 提取数据
        periods = df_agg["period"].dt.to_string("%Y-%m-%d").to_list()
        sales = df_agg["sales"].to_list()
        orders = df_agg["orders"].to_list()
        mom = df_agg["mom_rate"].to_list()
        anomalies = []
        for i, row in enumerate(df_agg.iter_rows(named=True)):
            if row["is_anomaly"] and row["mom_rate"] is not None:
                anomalies.append({
                    "date": periods[i],
                    "sales": round(row["sales"], 0),
                    "rate": round(row["mom_rate"] * 100, 1),
                    "direction": "up" if row["mom_rate"] > 0 else "down",
                })

        # 整体环比
        total_sales = df["sales_amount"].sum()
        total_orders = df["order_id"].n_unique()
        avg_order = total_sales / max(total_orders, 1)

        # 最近一期环比
        latest_mom = mom[-1] if mom[-1] is not None else 0
        prev_mom = mom[-2] if mom[-2] is not None else 0

        insights = self._trend_insights(
            periods, sales, mom, anomalies,
            total_sales, total_orders, avg_order,
            granularity
        )

        return {
            "granularity": granularity,
            "chart": {
                "periods": periods,
                "sales": [round(s, 0) for s in sales],
                "orders": orders,
                "mom": [round(m * 100, 1) if m is not None else None for m in mom],
            },
            "anomalies": anomalies,
            "summary": {
                "total_sales": round(total_sales, 0),
                "total_orders": total_orders,
                "avg_order": round(avg_order, 0),
                "latest_mom": round(latest_mom * 100, 1),
                "period_count": len(periods),
            },
            "insights": insights,
        }

    # ─── 模块 2: 经销商贡献与集中度 ──────────────────────────

    def analyze_dealer(self, df: pl.DataFrame) -> dict:
        """分析经销商贡献：TOP 排名 + 帕累托 + CR10 + 静默客户"""
        if "customer_name" not in df.columns or "sales_amount" not in df.columns:
            return {"error": "缺少经销商或金额字段"}

        # 按经销商聚合
        dealer_agg = df.group_by("customer_name").agg(
            pl.col("sales_amount").sum().alias("sales"),
            pl.col("order_id").n_unique().alias("orders"),
            pl.col("sales_amount").mean().alias("avg_order"),
        ).sort("sales", descending=True).with_row_count("rank")

        total_sales = dealer_agg["sales"].sum()
        total_dealers = len(dealer_agg)

        # 计算累计占比
        dealer_agg = dealer_agg.with_columns(
            (pl.col("sales").cum_sum() / total_sales * 100).alias("cum_pct")
        )

        # 帕累托分层
        head_count = max(1, int(total_dealers * self.config.PARETO_HEAD_RATIO))
        dealer_agg = dealer_agg.with_columns(
            pl.when(pl.col("rank") <= head_count)
            .then(pl.lit("头部"))
            .when(pl.col("cum_pct") <= 80)
            .then(pl.lit("腰部"))
            .otherwise(pl.lit("尾部"))
            .alias("tier")
        )

        # CR10 集中度
        cr_n = self.config.CR_TOP_N
        cr_sales = dealer_agg.head(cr_n)["sales"].sum()
        cr_rate = cr_sales / total_sales * 100

        # TOP 数据
        top_data = []
        for row in dealer_agg.head(20).iter_rows(named=True):
            top_data.append({
                "rank": row["rank"] + 1,
                "name": row["customer_name"],
                "sales": round(row["sales"], 0),
                "orders": row["orders"],
                "avg_order": round(row["avg_order"], 0),
                "pct": round(row["sales"] / total_sales * 100, 1),
                "cum_pct": round(row["cum_pct"], 1),
                "tier": row["tier"],
            })

        # 静默客户（近 30/60 天无订单）
        silent_short = 0
        silent_long = 0
        if "order_date" in df.columns:
            max_date = df["order_date"].max()
            for row in dealer_agg.iter_rows(named=True):
                name = row["customer_name"]
                dealer_df = df.filter(pl.col("customer_name") == name)
                last_order = dealer_df["order_date"].max()
                if last_order:
                    days_since = (max_date - last_order).days
                    if days_since >= self.config.SILENT_DAYS_LONG:
                        silent_long += 1
                    elif days_since >= self.config.SILENT_DAYS_SHORT:
                        silent_short += 1

        insights = self._dealer_insights(
            total_sales, total_dealers, cr_rate, cr_n,
            head_count, silent_short, silent_long,
            top_data[:5]
        )

        return {
            "chart": {
                "names": [d["name"] for d in top_data],
                "sales": [d["sales"] for d in top_data],
                "cum_pct": [d["cum_pct"] for d in top_data],
            },
            "summary": {
                "total_sales": round(total_sales, 0),
                "total_dealers": total_dealers,
                "cr_n": cr_n,
                "cr_rate": round(cr_rate, 1),
                "head_count": head_count,
                "silent_short": silent_short,
                "silent_long": silent_long,
            },
            "top": top_data,
            "insights": insights,
        }

    # ─── 模块 3: 产品/品类表现 ──────────────────────────────

    def analyze_product(self, df: pl.DataFrame) -> dict:
        """分析产品表现：ABC 分类 + TOP 单品 + 价格带分布"""
        if "sales_amount" not in df.columns:
            return {"error": "缺少金额字段"}

        results = {}

        # 品类分析
        if "product_category" in df.columns:
            cat_agg = df.group_by("product_category").agg(
                pl.col("sales_amount").sum().alias("sales"),
                pl.col("quantity").sum().alias("qty"),
                pl.col("order_id").n_unique().alias("orders"),
                pl.col("product_name").n_unique().alias("sku_count"),
            ).sort("sales", descending=True)

            total_sales = cat_agg["sales"].sum()
            cat_agg = cat_agg.with_columns(
                (pl.col("sales") / total_sales * 100).alias("pct")
            )

            # ABC 分类
            cat_agg = cat_agg.with_columns(
                (pl.col("sales").cum_sum() / total_sales * 100).alias("cum_pct")
            )
            cat_agg = cat_agg.with_columns(
                pl.when(pl.col("cum_pct") <= self.config.ABC_A_THRESHOLD * 100)
                .then(pl.lit("A"))
                .when(pl.col("cum_pct") <= self.config.ABC_B_THRESHOLD * 100)
                .then(pl.lit("B"))
                .otherwise(pl.lit("C"))
                .alias("abc_class")
            )

            cat_data = []
            for row in cat_agg.iter_rows(named=True):
                cat_data.append({
                    "name": row["product_category"],
                    "sales": round(row["sales"], 0),
                    "qty": row["qty"],
                    "orders": row["orders"],
                    "sku_count": row["sku_count"],
                    "pct": round(row["pct"], 1),
                    "abc_class": row["abc_class"],
                })

            results["category"] = {
                "chart": {
                    "names": [c["name"] for c in cat_data],
                    "sales": [c["sales"] for c in cat_data],
                    "pct": [c["pct"] for c in cat_data],
                },
                "data": cat_data,
            }

        # 单品分析
        if "product_name" in df.columns:
            prod_agg = df.group_by("product_name").agg(
                pl.col("sales_amount").sum().alias("sales"),
                pl.col("quantity").sum().alias("qty"),
                pl.col("sales_amount").mean().alias("avg_price"),
            ).sort("sales", descending=True)

            total_sales = prod_agg["sales"].sum()
            prod_agg = prod_agg.with_columns(
                (pl.col("sales") / total_sales * 100).alias("pct")
            )

            prod_data = []
            for row in prod_agg.head(20).iter_rows(named=True):
                prod_data.append({
                    "name": row["product_name"],
                    "sales": round(row["sales"], 0),
                    "qty": row["qty"],
                    "avg_price": round(row["avg_price"], 0),
                    "pct": round(row["pct"], 1),
                })

            results["product"] = {
                "chart": {
                    "names": [p["name"] for p in prod_data],
                    "sales": [p["sales"] for p in prod_data],
                },
                "data": prod_data,
            }

        # 价格带分布
        if "unit_price" in df.columns:
            prices = df["unit_price"].drop_nulls()
            if len(prices) > 0:
                bins = [0, 100, 500, 1000, 5000, 10000, float("inf")]
                labels = ["0-100", "100-500", "500-1k", "1k-5k", "5k-10k", "10k+"]
                price_hist = []
                for i in range(len(bins) - 1):
                    count = ((prices >= bins[i]) & (prices < bins[i+1])).sum()
                    price_hist.append({
                        "range": labels[i],
                        "count": int(count),
                    })

                results["price_distribution"] = {
                    "chart": {
                        "ranges": [p["range"] for p in price_hist],
                        "counts": [p["count"] for p in price_hist],
                    }
                }

        # 文字洞察
        insights = []
        if "category" in results:
            top_cat = cat_data[0] if cat_data else None
            if top_cat:
                a_count = sum(1 for c in cat_data if c["abc_class"] == "A")
                insights.append(
                    f"共 {len(cat_data)} 个品类，"
                    f"{top_cat['name']} 占比最高（{top_cat['pct']}%），"
                    f"A 类品类 {a_count} 个，贡献 {self.config.ABC_A_THRESHOLD*100:.0f}% 销售额"
                )

        if "product" in results and prod_data:
            insights.append(
                f"TOP 单品 {prod_data[0]['name']} 销售额 {prod_data[0]['sales']:.0f}，"
                f"占整体 {prod_data[0]['pct']}%"
            )

        return {
            **results,
            "insights": insights,
        }

    # ─── 模块 4: 订单特征 ───────────────────────────────────

    def analyze_order(self, df: pl.DataFrame) -> dict:
        """分析订单特征：客单价分布 + 单均件数 + 大额单标注"""
        if "sales_amount" not in df.columns or "order_id" not in df.columns:
            return {"error": "缺少订单号或金额字段"}

        # 按订单聚合（计算单笔金额和件数）
        order_agg = df.group_by("order_id").agg(
            pl.col("sales_amount").sum().alias("order_amount"),
            pl.col("quantity").sum().alias("order_qty"),
        )

        amounts = order_agg["order_amount"]
        qty = order_agg["order_qty"]

        # 分位数
        p25 = float(amounts.quantile(self.config.QUANTILE_LOW))
        p50 = float(amounts.quantile(0.50))
        p75 = float(amounts.quantile(self.config.QUANTILE_HIGH))
        p95 = float(amounts.quantile(self.config.QUANTILE_EXTREME))

        # 客单价分布（等宽分箱）
        max_amt = float(amounts.max())
        bin_edges = [0, p25, p50, p75, p95, max_amt * 1.1]
        bin_labels = [
            f"0-{int(p25/100)*100}",
            f"{int(p25/100)*100}-{int(p50/100)*100}",
            f"{int(p50/100)*100}-{int(p75/100)*100}",
            f"{int(p75/100)*100}-{int(p95/100)*100}",
            f"{int(p95/100)*100}+",
        ]

        dist_data = []
        for i in range(len(bin_edges) - 1):
            count = int(((order_agg["order_amount"] >= bin_edges[i]) &
                        (order_agg["order_amount"] < bin_edges[i+1])).sum())
            dist_data.append({
                "range": bin_labels[i],
                "count": count,
            })

        # 大额单标注
        extreme_threshold = p95
        extreme_orders = order_agg.filter(pl.col("order_amount") >= extreme_threshold)
        extreme_count = len(extreme_orders)
        extreme_sales = float(extreme_orders["order_amount"].sum())
        total_sales = float(amounts.sum())
        extreme_pct = extreme_sales / max(total_sales, 1) * 100

        # 剔除大额单后的常规订单
        normal_orders = order_agg.filter(pl.col("order_amount") < extreme_threshold)
        normal_avg = float(normal_orders["order_amount"].mean()) if len(normal_orders) > 0 else 0

        insights = self._order_insights(
            p25, p50, p75, p95,
            extreme_count, extreme_pct,
            normal_avg, float(amounts.mean()),
            float(qty.mean())
        )

        return {
            "chart": {
                "ranges": [d["range"] for d in dist_data],
                "counts": [d["count"] for d in dist_data],
            },
            "summary": {
                "total_orders": len(order_agg),
                "p25": round(p25, 0),
                "p50": round(p50, 0),
                "p75": round(p75, 0),
                "p95": round(p95, 0),
                "mean": round(float(amounts.mean()), 0),
                "avg_qty": round(float(qty.mean()), 1),
                "extreme_count": extreme_count,
                "extreme_pct": round(extreme_pct, 1),
                "normal_avg": round(normal_avg, 0),
            },
            "insights": insights,
        }

    # ─── 模块 5: 区域分析 ──────────────────────────────────

    def analyze_region(self, df: pl.DataFrame) -> dict:
        """分析区域销售分布"""
        if "region" not in df.columns or "sales_amount" not in df.columns:
            return {"error": "缺少区域或金额字段"}

        region_agg = df.group_by("region").agg(
            pl.col("sales_amount").sum().alias("sales"),
            pl.col("order_id").n_unique().alias("orders"),
            pl.col("customer_name").n_unique().alias("dealers"),
        ).sort("sales", descending=True)

        total_sales = region_agg["sales"].sum()
        region_agg = region_agg.with_columns(
            (pl.col("sales") / total_sales * 100).alias("pct")
        )

        region_data = []
        for row in region_agg.iter_rows(named=True):
            region_data.append({
                "name": row["region"],
                "sales": round(row["sales"], 0),
                "orders": row["orders"],
                "dealers": row["dealers"],
                "pct": round(row["pct"], 1),
            })

        insights = [f"共 {len(region_data)} 个区域，{region_data[0]['name']} 占比最高（{region_data[0]['pct']}%）"]

        return {
            "chart": {
                "names": [r["name"] for r in region_data],
                "sales": [r["sales"] for r in region_data],
                "pct": [r["pct"] for r in region_data],
            },
            "data": region_data,
            "insights": insights,
        }

    # ─── 模块 6: 经销商活跃度 ──────────────────────────────

    def analyze_activity(self, df: pl.DataFrame) -> dict:
        """分析经销商活跃度：横轴=销售额，纵轴=距上次进货天数"""
        if "customer_name" not in df.columns or "order_date" not in df.columns:
            return {"error": "缺少必要字段"}

        max_date = df["order_date"].max()
        if max_date is None:
            return {"error": "无有效日期数据"}

        # 按经销商聚合
        dealer_agg = df.group_by("customer_name").agg(
            pl.col("sales_amount").sum().alias("sales"),
            pl.col("order_id").n_unique().alias("orders"),
            pl.col("order_date").max().alias("last_order"),
        )

        # 计算距上次进货天数
        dealer_agg = dealer_agg.with_columns(
            (pl.lit(max_date) - pl.col("last_order")).dt.total_days().alias("days_since")
        )

        points = []
        risk_count = 0
        warn_count = 0

        for row in dealer_agg.iter_rows(named=True):
            days = int(row["days_since"])
            sales = round(row["sales"], 0)
            points.append({
                "name": row["customer_name"],
                "sales": sales,
                "days": days,
                "orders": row["orders"],
            })
            if days >= self.config.SILENT_DAYS_LONG:
                risk_count += 1
            elif days >= self.config.SILENT_DAYS_SHORT:
                warn_count += 1

        insights = [
            f"经销商活跃度分析：高风险 {risk_count} 家（>{self.config.SILENT_DAYS_LONG}天未进货），"
            f"关注 {warn_count} 家（>{self.config.SILENT_DAYS_SHORT}天未进货）"
        ]

        return {
            "chart": {
                "points": points,
                "riskCount": risk_count,
                "warnCount": warn_count,
            },
            "insights": insights,
        }

    # ─── 辅助方法 ────────────────────────────────────────────

    def _recommend_granularity(self, df: pl.DataFrame) -> str:
        """根据数据跨度推荐时间粒度"""
        if "order_date" not in df.columns:
            return "day"

        dates = df["order_date"].drop_nulls()
        if len(dates) < 2:
            return "day"

        min_date = dates.min()
        max_date = dates.max()
        span_days = (max_date - min_date).days

        if span_days <= self.config.GRANULARITY_DAILY_MAX:
            return "day"
        elif span_days <= self.config.GRANULARITY_WEEKLY_MAX:
            return "week"
        else:
            return "month"

    def _generate_summary(self, df: pl.DataFrame) -> dict:
        """生成数据概览"""
        summary = {
            "total_rows": len(df),
            "columns": list(df.columns),
        }

        if "order_date" in df.columns:
            dates = df["order_date"].drop_nulls()
            if len(dates) > 0:
                summary["date_range"] = {
                    "start": str(dates.min()),
                    "end": str(dates.max()),
                    "days": (dates.max() - dates.min()).days,
                }

        if "sales_amount" in df.columns:
            summary["total_sales"] = round(float(df["sales_amount"].sum()), 0)

        if "order_id" in df.columns:
            summary["total_orders"] = df["order_id"].n_unique()

        if "customer_name" in df.columns:
            summary["total_dealers"] = df["customer_name"].n_unique()

        return summary

    # ─── 描述型文字结论生成 ─────────────────────────────────

    def _trend_insights(self, periods, sales, mom, anomalies, total, orders, avg, granularity):
        insights = []

        # 整体概况
        insights.append(
            f"数据覆盖 {len(periods)} 个{granularity}周期，"
            f"总销售额 {total:,.0f}，共 {orders} 笔订单，"
            f"平均单笔 {avg:,.0f}"
        )

        # 最新环比
        if mom and mom[-1] is not None:
            direction = "上升" if mom[-1] > 0 else "下降"
            insights.append(
                f"最近一期销售额环比{direction} {abs(mom[-1]*100):.1f}%"
            )

        # 异常波动
        if anomalies:
            ups = [a for a in anomalies if a["direction"] == "up"]
            downs = [a for a in anomalies if a["direction"] == "down"]
            if ups:
                insights.append(
                    f"检测到 {len(ups)} 次显著上升波动"
                )
            if downs:
                insights.append(
                    f"检测到 {len(downs)} 次显著下降波动"
                )

        # 趋势方向
        if len(sales) >= 3:
            first_half = sum(sales[:len(sales)//2]) / max(len(sales)//2, 1)
            second_half = sum(sales[len(sales)//2:]) / max(len(sales) - len(sales)//2, 1)
            if second_half > first_half * 1.1:
                insights.append("整体呈上升趋势")
            elif second_half < first_half * 0.9:
                insights.append("整体呈下降趋势")
            else:
                insights.append("整体趋于平稳")

        return insights

    def _dealer_insights(self, total, count, cr_rate, cr_n, head, silent_s, silent_l, top5):
        insights = []

        insights.append(
            f"共 {count} 家经销商，总销售额 {total:,.0f}，"
            f"CR{cr_n} 为 {cr_rate:.1f}%"
        )

        if cr_rate > 70:
            insights.append(f"头部集中度较高，前 {cr_n} 家贡献超过 {cr_rate:.0f}% 销售额")
        elif cr_rate < 40:
            insights.append(f"头部集中度较低，前 {cr_n} 家贡献 {cr_rate:.0f}%，分布较为分散")

        if top5:
            top_name = top5[0]["name"]
            top_pct = top5[0]["pct"]
            insights.append(
                f"TOP 1 经销商 {top_name} 贡献 {top_pct:.1f}%，"
                f"销售额 {top5[0]['sales']:,.0f}"
            )

        if silent_l > 0:
            insights.append(
                f"近 {self.config.SILENT_DAYS_LONG} 天无订单的静默经销商 {silent_l} 家"
            )
        if silent_s > 0:
            insights.append(
                f"近 {self.config.SILENT_DAYS_SHORT} 天无订单的经销商 {silent_s} 家"
            )

        return insights

    def _order_insights(self, p25, p50, p75, p95, extreme_count, extreme_pct, normal_avg, mean, avg_qty):
        insights = []

        insights.append(
            f"客单价中位数 {p50:,.0f}，P25 为 {p25:,.0f}，P75 为 {p75:,.0f}，"
            f"均值 {mean:,.0f}"
        )

        if abs(mean - p50) / max(p50, 1) > 0.3:
            insights.append(
                "均值与中位数差异较大，存在极端值拉高整体水平"
            )

        if extreme_count > 0:
            insights.append(
                f"大额订单（>{p95:,.0f}）共 {extreme_count} 笔，"
                f"占总额 {extreme_pct:.1f}%"
            )
            insights.append(
                f"剔除大额订单后，常规订单平均 {normal_avg:,.0f}"
            )

        insights.append(f"平均单笔 {avg_qty:.1f} 件")

        return insights
