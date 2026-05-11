"""
模糊匹配引擎 (Field Matcher)
根据原始表头自动推荐标准字段映射，支持精确匹配、部分匹配、关键词评分。
"""
from __future__ import annotations

import re
from collections import defaultdict
from typing import Optional

from .schema import STANDARD_SCHEMA, REQUIRED_FIELDS, OPTIONAL_FIELDS


class FieldMapper:
    """字段模糊匹配引擎"""

    def __init__(self):
        # 预编译所有别名为小写索引
        self._alias_index: dict[str, list[str]] = defaultdict(list)
        for field_name, field_def in STANDARD_SCHEMA.items():
            for alias in field_def.aliases:
                self._alias_index[alias.lower().strip()].append(field_name)

        # 关键词权重表（用于模糊匹配评分）
        self._keyword_scores: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
        for field_name, field_def in STANDARD_SCHEMA.items():
            for alias in field_def.aliases:
                # 拆分别名为关键词
                words = re.split(r'[\s_\-/]+', alias.lower())
                for word in words:
                    if len(word) >= 2:  # 忽略太短的词
                        self._keyword_scores[word][field_name] += 1.0

    def match_column(self, raw_column: str) -> Optional[str]:
        """
        对单个原始列名进行匹配，返回最可能的标准字段名。
        返回 None 表示无法匹配。
        """
        cleaned = raw_column.strip().lower()
        if not cleaned:
            return None

        # 优先级 1: 精确匹配（别名完全一致）
        if cleaned in self._alias_index:
            candidates = self._alias_index[cleaned]
            # 优先返回必需字段
            for c in candidates:
                if c in REQUIRED_FIELDS:
                    return c
            return candidates[0]

        # 优先级 2: 部分匹配（原始列名包含某个别名）
        best_score = 0.0
        best_match = None

        for alias, field_names in self._alias_index.items():
            if alias in cleaned or cleaned in alias:
                score = min(len(alias), len(cleaned)) / max(len(alias), len(cleaned))
                # 必需字段加分
                for fn in field_names:
                    if fn in REQUIRED_FIELDS:
                        score *= 1.5
                if score > best_score:
                    best_score = score
                    best_match = field_names[0]

        if best_score >= 0.5:
            return best_match

        # 优先级 3: 关键词评分匹配
        words = re.split(r'[\s_\-/]+', cleaned)
        field_scores: dict[str, float] = defaultdict(float)

        for word in words:
            if len(word) < 2:
                continue
            for field_name, weight in self._keyword_scores.get(word, {}).items():
                field_scores[field_name] += weight

        if field_scores:
            best_field = max(field_scores, key=field_scores.get)
            if field_scores[best_field] >= 1.5:  # 至少有一个关键词匹配
                return best_field

        return None

    def map_columns(self, raw_columns: list[str]) -> dict[str, str]:
        """
        批量匹配原始列名 → 标准字段名。
        返回 {标准字段名: 原始列名} 的映射字典。
        """
        mapping: dict[str, str] = {}
        used_raw: set[str] = set()

        # 第一轮：精确匹配（必需字段优先）
        for col in raw_columns:
            matched = self.match_column(col)
            if matched and matched not in mapping and col not in used_raw:
                if matched in REQUIRED_FIELDS:
                    mapping[matched] = col
                    used_raw.add(col)

        # 第二轮：匹配可选字段
        for col in raw_columns:
            if col in used_raw:
                continue
            matched = self.match_column(col)
            if matched and matched not in mapping:
                mapping[matched] = col
                used_raw.add(col)

        return mapping

    def get_match_suggestions(self, raw_column: str, top_n: int = 3) -> list[dict]:
        """
        为单个原始列名返回 Top-N 匹配建议（含评分）。
        用于前端展示可选映射。
        """
        cleaned = raw_column.strip().lower()
        if not cleaned:
            return []

        suggestions: list[dict] = []

        # 收集所有可能的匹配
        field_scores: dict[str, float] = defaultdict(float)

        # 精确匹配
        if cleaned in self._alias_index:
            for fn in self._alias_index[cleaned]:
                field_scores[fn] = max(field_scores[fn], 1.0)

        # 部分匹配
        for alias, field_names in self._alias_index.items():
            if alias in cleaned or cleaned in alias:
                score = min(len(alias), len(cleaned)) / max(len(alias), len(cleaned))
                for fn in field_names:
                    field_scores[fn] = max(field_scores[fn], score * 0.8)

        # 关键词匹配
        words = re.split(r'[\s_\-/]+', cleaned)
        for word in words:
            if len(word) < 2:
                continue
            for field_name, weight in self._keyword_scores.get(word, {}).items():
                field_scores[field_name] = max(field_scores[field_name], weight * 0.5)

        # 排序并返回 Top-N
        sorted_fields = sorted(field_scores.items(), key=lambda x: -x[1])
        for field_name, score in sorted_fields[:top_n]:
            field_def = STANDARD_SCHEMA[field_name]
            suggestions.append({
                "standard_name": field_name,
                "display_name": field_def.display_name,
                "category": field_def.category.value,
                "required": field_def.required,
                "score": round(score, 3),
                "field_type": field_def.field_type.value,
            })

        return suggestions

    def get_report(self) -> dict:
        """返回匹配报告（用于 API 响应）"""
        return {
            "standard_schema": {
                "required": list(REQUIRED_FIELDS),
                "optional": list(OPTIONAL_FIELDS),
            }
        }
