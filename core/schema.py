"""
标准字段集定义 (The Standard Schema)
作为字段匹配器的目标，所有原始数据最终都转换为此格式。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class FieldType(Enum):
    """字段类型枚举"""
    DATE = "date"
    NUMBER = "number"
    STRING = "string"
    INTEGER = "integer"


class FieldCategory(Enum):
    """字段分类"""
    MEASURE = "measure"       # 核心度量
    DIMENSION = "dimension"   # 维度


@dataclass
class StandardField:
    """标准字段定义"""
    name: str                     # 标准字段名 (如 revenue)
    display_name: str             # 显示名 (如 "销售额")
    field_type: FieldType         # 类型
    category: FieldCategory       # 分类
    required: bool = False        # 是否必需
    aliases: list[str] = field(default_factory=list)  # 同义词/别名
    description: str = ""         # 字段说明


# ─── 标准字段集定义 ───────────────────────────────────────────

STANDARD_SCHEMA: dict[str, StandardField] = {
    # ── 核心度量 (Measures) ──
    "sales_date": StandardField(
        name="sales_date",
        display_name="销售日期",
        field_type=FieldType.DATE,
        category=FieldCategory.MEASURE,
        required=True,
        aliases=[
            # 中文
            "日期", "销售日期", "下单日期", "订单日期", "交易日期", "成交日期",
            "开票日期", "发货日期", "日期/时间", "date", "时间",
            # 英文
            "date", "order_date", "sale_date", "transaction_date", "deal_date",
            "invoice_date", "ship_date", "datetime", "created_at", "timestamp",
        ],
        description="订单发生的日期，用于趋势分析",
    ),
    "revenue": StandardField(
        name="revenue",
        display_name="销售额",
        field_type=FieldType.NUMBER,
        category=FieldCategory.MEASURE,
        required=True,
        aliases=[
            # 中文
            "销售额", "金额", "收入", "流水", "营业额", "成交金额",
            "订单金额", "销售收入", "营收", "实收金额", "应收金额",
            "sale_amount", "revenue", "amount",
            # 英文
            "amount", "revenue", "sales_amount", "total_amount", "order_amount",
            "deal_amount", "transaction_amount", "price", "value", "income",
            "gross_sales", "net_sales",
        ],
        description="订单的销售金额，用于计算总额、趋势、排名等核心指标",
    ),
    "order_id": StandardField(
        name="order_id",
        display_name="订单号",
        field_type=FieldType.STRING,
        category=FieldCategory.MEASURE,
        required=True,
        aliases=[
            # 中文
            "订单号", "单号", "编号", "流水号", "交易号", "订单编号",
            "order_no", "order_id",
            # 英文
            "order_id", "order_no", "order_number", "transaction_id",
            "deal_id", "invoice_no", "bill_no", "ref_no", "reference",
        ],
        description="唯一订单标识，用于去重和计数",
    ),
    "quantity": StandardField(
        name="quantity",
        display_name="销量",
        field_type=FieldType.INTEGER,
        category=FieldCategory.MEASURE,
        required=False,
        aliases=[
            # 中文
            "销量", "数量", "件数", "订货数量", "销售数量", "出库数量",
            "qty", "quantity",
            # 英文
            "quantity", "qty", "units", "count", "pieces", "volume",
            "sales_qty", "order_qty",
        ],
        description="销售数量，用于计算客单价等衍生指标",
    ),
    "sales_target": StandardField(
        name="sales_target",
        display_name="销售目标",
        field_type=FieldType.NUMBER,
        category=FieldCategory.MEASURE,
        required=False,
        aliases=[
            # 中文
            "销售目标", "目标", "指标", "任务", "定额", "计划",
            "target", "goal", "quota",
            # 英文
            "target", "sales_target", "goal", "quota", "plan", "budget",
            "KPI", "target_amount", "monthly_target",
        ],
        description="销售目标值，用于计算达成率和业绩缺口",
    ),
    # ── 维度 (Dimensions) ──
    "region": StandardField(
        name="region",
        display_name="区域",
        field_type=FieldType.STRING,
        category=FieldCategory.DIMENSION,
        required=False,
        aliases=[
            # 中文
            "区域", "地区", "省份", "城市", "大区", "区域/省份",
            "region", "province", "city", "area",
            # 英文
            "region", "area", "province", "state", "city", "location",
            "territory", "district", "zone", "market",
        ],
        description="销售区域，用于区域分布分析",
    ),
    "category": StandardField(
        name="category",
        display_name="产品类别",
        field_type=FieldType.STRING,
        category=FieldCategory.DIMENSION,
        required=False,
        aliases=[
            # 中文
            "产品类别", "类别", "品类", "产品分类", "商品类别", "品类名称",
            "category", "product_category", "type",
            # 英文
            "category", "product_category", "product_type", "type",
            "product_line", "segment", "class", "group", "product_name",
            "item_name", "sku_name",
        ],
        description="产品/服务类别，用于品类表现分析",
    ),
    "sales_rep": StandardField(
        name="sales_rep",
        display_name="销售人员",
        field_type=FieldType.STRING,
        category=FieldCategory.DIMENSION,
        required=False,
        aliases=[
            # 中文
            "销售人员", "业务员", "销售", "客户经理", "代表", "负责人",
            "sales_rep", "salesperson", "rep",
            # 英文
            "sales_rep", "salesperson", "rep", "salesman", "account_manager",
            "sales_executive", "consultant", "agent", "employee",
            "sales_name", "person",
        ],
        description="负责销售的人员，用于排行榜和绩效分析",
    ),
    "customer_name": StandardField(
        name="customer_name",
        display_name="客户名称",
        field_type=FieldType.STRING,
        category=FieldCategory.DIMENSION,
        required=False,
        aliases=[
            # 中文
            "客户名称", "客户", "经销商", "买家", "购买方", "采购商",
            "customer", "dealer", "buyer",
            # 英文
            "customer_name", "customer", "dealer", "buyer", "client",
            "account", "purchaser", "store_name", "shop_name",
            "customer_id", "client_name",
        ],
        description="客户/经销商名称，用于贡献度分析",
    ),
}

# 必需字段列表
REQUIRED_FIELDS = {"sales_date", "revenue", "order_id"}

# 可选字段列表
OPTIONAL_FIELDS = {"quantity", "sales_target", "region", "category", "sales_rep", "customer_name"}


def get_schema_info() -> dict:
    """返回标准字段集信息（用于前端展示）"""
    return {
        "measures": [
            {
                "name": f.name,
                "display_name": f.display_name,
                "field_type": f.field_type.value,
                "required": f.required,
                "aliases": f.aliases[:10],  # 只返回部分别名用于展示
            }
            for f in STANDARD_SCHEMA.values()
            if f.category == FieldCategory.MEASURE
        ],
        "dimensions": [
            {
                "name": f.name,
                "display_name": f.display_name,
                "field_type": f.field_type.value,
                "required": f.required,
                "aliases": f.aliases[:10],
            }
            for f in STANDARD_SCHEMA.values()
            if f.category == FieldCategory.DIMENSION
        ],
    }
