#!/usr/bin/env python3
"""
生成示例销售数据（CSV）
用于测试仪表板功能
"""
import csv
import random
from datetime import datetime, timedelta

random.seed(42)

dealers = [f"经销商{chr(64+i)}" for i in range(1, 21)]  # A-T
regions = ["华东", "华南", "华北", "西南", "华中", "东北", "西北"]
products = [
    ("产品A", "品类X"), ("产品B", "品类X"), ("产品C", "品类Y"),
    ("产品D", "品类Y"), ("产品E", "品类Z"), ("产品F", "品类Z"),
    ("产品G", "品类W"), ("产品H", "品类W"),
]

start_date = datetime(2024, 1, 1)
rows = []

for i in range(500):
    order_id = f"ORD{str(i+1).zfill(5)}"
    days_offset = random.randint(0, 179)  # 6 个月
    order_date = start_date + timedelta(days=days_offset)

    # 随机日期格式
    fmt = random.choice(["iso", "slash", "cn"])
    if fmt == "iso":
        date_str = order_date.strftime("%Y-%m-%d")
    elif fmt == "slash":
        date_str = order_date.strftime("%Y/%m/%d")
    else:
        date_str = order_date.strftime("%Y年%m月%d日")

    dealer = random.choice(dealers)
    region = random.choice(regions)
    product, category = random.choice(products)

    quantity = random.randint(1, 50)
    unit_price = round(random.uniform(50, 500), 2)
    sales_amount = round(quantity * unit_price, 2)

    # 随机加一些格式噪声
    if random.random() < 0.3:
        sales_amount_str = f"¥{sales_amount:,.2f}"
    else:
        sales_amount_str = f"{sales_amount:,.2f}"

    rows.append([order_id, date_str, dealer, region, product, category, quantity, unit_price, sales_amount_str])

# 加入一些重复行和空行
for _ in range(10):
    rows.append(rows[random.randint(0, len(rows)-1)].copy())
rows.append(["", "", "", "", "", "", "", "", ""])

with open("sample_data.csv", "w", encoding="utf-8", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["订单号", "下单日期", "客户名称", "区域", "商品名称", "品类", "数量", "单价", "销售额"])
    writer.writerows(rows)

print(f"✅ 已生成 sample_data.csv（{len(rows)} 行）")
