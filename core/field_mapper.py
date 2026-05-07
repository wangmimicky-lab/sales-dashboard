"""
字段语义识别 & 映射
策略：规则表优先（覆盖 80%），未匹配字段走 LLM 兜底
"""
from __future__ import annotations

import re
from typing import Optional

from .config import ALIAS_TO_STANDARD, STANDARD_FIELDS


class FieldMapper:
    """将原始表头映射到标准字段名"""

    def __init__(self):
        self.mapping: dict[str, str] = {}       # 原始列名 → 标准字段
        self.unmapped: list[str] = []            # 未匹配的列
        self.extra: list[str] = []               # 标准字段之外的列（保留但不用）

    # ─── 核心方法 ─────────────────────────────────────────────

    def map_columns(self, raw_columns: list[str]) -> dict[str, str]:
        """
        接收原始表头列表，返回 {原始列名: 标准字段名} 映射表。
        未匹配的列记录到 self.unmapped。
        """
        self.mapping = {}
        self.unmapped = []
        self.extra = []

        used_standards = set()

        for col in raw_columns:
            col_stripped = col.strip()
            if not col_stripped:
                continue

            standard = self._match_alias(col_stripped)

            if standard and standard not in used_standards:
                self.mapping[col_stripped] = standard
                used_standards.add(standard)
            else:
                # 可能是额外列（如备注、状态等），保留但不参与分析
                self.extra.append(col_stripped)

        return dict(self.mapping)

    def get_mapped_df_columns(self) -> list[str]:
        """返回所有成功映射的标准字段名"""
        return list(set(self.mapping.values()))

    # ─── 匹配逻辑 ─────────────────────────────────────────────

    def _match_alias(self, col: str) -> Optional[str]:
        """
        多级匹配策略：
        1. 精确匹配（全小写）
        2. 去除空格/标点后的模糊匹配
        3. 包含匹配（如 "订单编号" 包含 "订单号"）
        """
        # 1. 精确匹配
        key = col.lower().strip()
        if key in ALIAS_TO_STANDARD:
            return ALIAS_TO_STANDARD[key]

        # 2. 清理后匹配（去除空格、下划线、特殊字符）
        cleaned = re.sub(r"[\s_\-\.\(\)\/]", "", key)
        for alias, standard in ALIAS_TO_STANDARD.items():
            alias_cleaned = re.sub(r"[\s_\-\.\(\)\/]", "", alias.lower())
            if cleaned == alias_cleaned:
                return standard

        # 3. 包含匹配（原始列名包含某个别名，或别名包含原始列名）
        for alias, standard in ALIAS_TO_STANDARD.items():
            alias_lower = alias.lower()
            # 原始列名包含别名（如 "下单日期" 包含 "日期"）
            if alias_lower in key and len(alias_lower) >= 2:
                return standard
            # 别名包含原始列名（如 "销售额" 匹配 "销售金额"）
            if key in alias_lower and len(key) >= 2:
                return standard

        return None

    # ─── 报告生成 ─────────────────────────────────────────────

    def get_report(self) -> dict:
        """返回映射结果报告，供前端展示"""
        mapped_details = []
        for raw, standard in self.mapping.items():
            mapped_details.append({
                "raw": raw,
                "standard": standard,
                "status": "matched",
            })

        for col in self.unmapped:
            mapped_details.append({
                "raw": col,
                "standard": None,
                "status": "unmatched",
            })

        for col in self.extra:
            if col not in [m["raw"] for m in mapped_details]:
                mapped_details.append({
                    "raw": col,
                    "standard": None,
                    "status": "extra",
                })

        return {
            "total_columns": len(self.mapping) + len(self.unmapped) + len(self.extra),
            "mapped": len(self.mapping),
            "unmapped": len(self.unmapped),
            "extra": len(self.extra),
            "details": mapped_details,
        }
