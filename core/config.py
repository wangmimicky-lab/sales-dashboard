"""
标准字段映射表 & 全局配置
所有导出格式的最终对齐目标，规则表覆盖 80% 常见表头。
"""

# ─── 标准字段定义 ─────────────────────────────────────────────
# 系统内部统一使用的字段名
STANDARD_FIELDS = [
    "order_id",
    "order_date",
    "customer_name",
    "region",
    "product_name",
    "product_category",
    "quantity",
    "unit_price",
    "sales_amount",
    "discount_rate",
]

# ─── 字段映射表（中文/英文别名 → 标准字段）────────────────────
FIELD_ALIASES = {
    # 订单号
    "order_id": [
        "订单号", "订单编号", "单号", "交易单号", "单据编号",
        "order id", "order_no", "orderno", "order no", "order_no_",
        "order_number", "order_num",
    ],
    # 下单日期
    "order_date": [
        "下单日期", "订单日期", "交易日期", "日期", "下单时间",
        "订单时间", "成交日期", "创建日期", "下单日",
        "order date", "transaction date", "date", "created_at",
        "create_date", "order_time", "trade_date",
    ],
    # 经销商名称
    "customer_name": [
        "客户名称", "经销商", "门店名称", "客户", "经销商名称",
        "门店", "买家名称", "买家", "客户名",
        "customer name", "dealer", "store name", "client",
        "buyer_name", "buyer", "customer", "dealer_name",
    ],
    # 区域
    "region": [
        "区域", "地区", "省份", "大区", "城市", "所属区域",
        "所在地区", "销售区域", "省份/城市",
        "region", "area", "province", "territory", "city",
        "district", "location",
    ],
    # 产品名称
    "product_name": [
        "商品名称", "产品名称", "品名", "货品", "商品",
        "商品名", "货品名称", "商品全称",
        "product name", "item name", "sku name", "goods_name",
        "product", "item", "sku_name", "goods",
    ],
    # 产品品类
    "product_category": [
        "品类", "分类", "类别", "大类", "商品分类", "产品分类",
        "品类名称", "商品类别", "二级分类",
        "category", "class", "type", "category_name",
        "product_category", "product_type", "goods_category",
    ],
    # 数量
    "quantity": [
        "数量", "件数", "销量", "销售数量", "成交数量",
        "购买数量", "订货数量", "出库数量",
        "quantity", "qty", "amount", "sales_volume",
        "num", "count", "volume",
    ],
    # 单价
    "unit_price": [
        "单价", "零售价", "成交价", "销售价格", "商品单价",
        "产品单价", "成交单价",
        "unit price", "price", "retail price", "sale_price",
        "unitcost", "cost_price",
    ],
    # 销售金额
    "sales_amount": [
        "销售额", "金额", "实收", "营业额", "gmv",
        "销售金额", "实收金额", "成交金额", "实际金额",
        "应收金额", "订单金额", "合计",
        "sales amount", "revenue", "total", "gm_v",
        "actual_amount", "order_amount", "amount_total",
        "sale_amount", "paid_amount",
    ],
    # 折扣率
    "discount_rate": [
        "折扣", "折扣率", "优惠率", "折扣比例",
        "discount", "discount rate", "discount_rate",
        "discount_pct", "preferential_rate",
    ],
}

# 构建反向查找表：别名 → 标准字段（全小写匹配）
ALIAS_TO_STANDARD = {}
for standard, aliases in FIELD_ALIASES.items():
    for alias in aliases:
        ALIAS_TO_STANDARD[alias.strip().lower()] = standard

# ─── 分析阈值配置 ─────────────────────────────────────────────
class AnalysisConfig:
    """所有阈值可在此调整，无需改代码"""

    # 波动带阈值（偏离滚动均值超过此比例标注为异常）
    VOLATILITY_THRESHOLD = 0.15  # 15%

    # 帕累托分层比例
    PARETO_HEAD_RATIO = 0.20     # 头部 20%
    PARETO_TAIL_RATIO = 0.80     # 尾部 80%

    # CR 集中度阈值
    CR_TOP_N = 10                # 计算 CR10

    # ABC 分类阈值（按销售额累计占比）
    ABC_A_THRESHOLD = 0.70       # A 类：累计 70%
    ABC_B_THRESHOLD = 0.90       # B 类：累计 90%，余下为 C

    # 静默客户判定（天数）
    SILENT_DAYS_SHORT = 30
    SILENT_DAYS_LONG = 60

    # 客单价分位数
    QUANTILE_LOW = 0.25
    QUANTILE_HIGH = 0.75
    QUANTILE_EXTREME = 0.95

    # 时间粒度推荐阈值（数据跨度天数）
    GRANULARITY_DAILY_MAX = 30
    GRANULARITY_WEEKLY_MAX = 180  # 6 个月

    # 数据量分级
    SMALL_DATA_THRESHOLD = 50_000
    MEDIUM_DATA_THRESHOLD = 300_000

    # 环比计算最小间隔（天）
    MOM_MIN_DAYS = 25
    WOW_MIN_DAYS = 5
