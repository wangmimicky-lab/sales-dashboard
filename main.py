"""
销售数据仪表板 - FastAPI 入口
"""
from __future__ import annotations

import os
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Optional

import polars as pl
from fastapi import Depends, FastAPI, File, Form, HTTPException, UploadFile, Response
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

from core import FieldMapper, DataCleaner, Analyzer, get_schema_info

# ─── 认证模块 ──────────────────────────────────────────────────
import auth

# ─── FastAPI 应用 ──────────────────────────────────────────────
app = FastAPI(title="销售数据仪表板", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 上传目录
UPLOAD_DIR = Path(__file__).parent / "uploads"
UPLOAD_DIR.mkdir(exist_ok=True)

# 当前会话数据
_session_data: dict = {}


# ─── 认证路由 ──────────────────────────────────────────────────

@app.post("/api/auth/login")
async def login(username: str = Form(...), password: str = Form(...)):
    """用户登录"""
    user = auth.verify_user(username, password)
    if not user:
        raise HTTPException(status_code=401, detail="用户名或密码错误")

    token = auth.create_token(username)
    response = {
        "success": True,
        "username": username,
        "expire_date": user["expire_date"],
    }
    resp = JSONResponse(content=response)
    resp.set_cookie(
        key="access_token",
        value=token,
        httponly=True,
        max_age=auth.TOKEN_MAX_AGE,
        samesite="lax",
    )
    return resp


@app.post("/api/auth/logout")
async def logout():
    """登出"""
    resp = JSONResponse(content={"success": True})
    resp.delete_cookie(key="access_token")
    return resp


@app.get("/api/auth/me")
async def get_me(current_user: dict = Depends(auth.get_current_user)):
    """获取当前用户信息"""
    return current_user


# ─── 新 API: 标准字段集 ────────────────────────────────────────

@app.get("/api/schema")
async def get_schema(current_user: dict = Depends(auth.get_current_user)):
    """获取标准字段集定义（供前端展示）"""
    return {"success": True, "schema": get_schema_info()}


# ─── 新 API: 字段匹配建议 ──────────────────────────────────────

@app.post("/api/match-suggestions")
async def get_match_suggestions(
    raw_column: str = Form(...),
    current_user: dict = Depends(auth.get_current_user),
):
    """为单个原始列名返回匹配建议"""
    mapper = FieldMapper()
    suggestions = mapper.get_match_suggestions(raw_column)
    return {"success": True, "raw_column": raw_column, "suggestions": suggestions}


# ─── 数据上传 & 处理 ──────────────────────────────────────────

@app.post("/api/upload")
async def upload_file(
    file: UploadFile = File(...),
    current_user: dict = Depends(auth.get_current_user),
):
    """
    上传数据文件（CSV/Excel）
    执行：字段识别 → 返回原始列名 + 自动匹配建议
    注意：此时不执行清洗，等用户确认映射后再处理
    """
    file_ext = Path(file.filename).suffix.lower()
    if file_ext not in (".csv", ".xlsx", ".xls"):
        raise HTTPException(status_code=400, detail="仅支持 CSV / Excel 文件")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    saved_path = UPLOAD_DIR / f"{timestamp}_{file.filename}"
    content = await file.read()
    saved_path.write_bytes(content)

    # 读取数据
    try:
        if file_ext == ".csv":
            df = pl.read_csv(saved_path, try_parse_dates=True)
        else:
            df = pl.read_excel(saved_path)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"文件解析失败: {str(e)}")

    if len(df) == 0:
        raise HTTPException(status_code=400, detail="文件为空，无数据行")

    # 字段匹配
    mapper = FieldMapper()
    raw_columns = df.columns
    auto_mapping = mapper.map_columns(raw_columns)

    # 为每个原始列生成匹配建议
    column_suggestions = {}
    for col in raw_columns:
        suggestions = mapper.get_match_suggestions(col)
        column_suggestions[col] = suggestions

    return {
        "success": True,
        "filename": file.filename,
        "rows": len(df),
        "columns": raw_columns,
        "auto_mapping": auto_mapping,  # {标准字段: 原始列名}
        "column_suggestions": column_suggestions,  # {原始列名: [建议列表]}
        "preview": df.head(5).to_dicts(),  # 前5行预览
    }


# ─── 新 API: 应用映射并执行分析 ───────────────────────────────

@app.post("/api/process")
async def process_data(
    mapping_config: str = Form(...),  # JSON: {标准字段: 原始列名}
    granularity: str = Form("auto"),
    filters: str = Form("{}"),
    current_user: dict = Depends(auth.get_current_user),
):
    """
    根据用户确认的映射配置执行数据处理和分析。
    mapping_config: JSON 字符串 {标准字段名: 原始列名}
    """
    import json

    if "df" not in _session_data:
        raise HTTPException(status_code=400, detail="请先上传数据文件")

    # 解析映射配置
    try:
        mapping = json.loads(mapping_config)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="映射配置格式错误")

    if len(mapping) < 3:
        raise HTTPException(status_code=400, detail="至少需要映射 3 个字段（日期、金额、订单号）")

    # 检查必需字段
    from core import REQUIRED_FIELDS
    missing = REQUIRED_FIELDS - set(mapping.keys())
    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"缺少必需字段映射: {', '.join(missing)}",
        )

    # 执行数据处理
    cleaner = DataCleaner()
    df_raw = _session_data["df"]
    df_clean = cleaner.process_data(df_raw, mapping)

    if len(df_clean) == 0:
        raise HTTPException(status_code=400, detail="处理后可用数据为空")

    # 更新会话
    _session_data["df_clean"] = df_clean
    _session_data["mapping"] = mapping
    _session_data["cleaner_report"] = cleaner.get_report()

    # 执行分析
    try:
        filter_dict = json.loads(filters) if filters else {}
    except json.JSONDecodeError:
        filter_dict = {}

    analyzer = Analyzer()
    analysis_result = analyzer.analyze_all(df_clean, granularity=granularity, filters=filter_dict)

    return {
        "success": True,
        "rows": len(df_clean),
        "columns": list(df_clean.columns),
        "mapping": mapping,
        "cleaning": cleaner.get_report(),
        "analysis": analysis_result,
    }


# ─── 筛选器选项 ───────────────────────────────────────────────

@app.get("/api/filters")
async def get_filters(
    current_user: dict = Depends(auth.get_current_user),
):
    """获取筛选器选项"""
    if "df_clean" not in _session_data:
        raise HTTPException(status_code=400, detail="请先上传并处理数据")

    df = _session_data["df_clean"]
    filters = {}

    for dim_field in ["customer_name", "region", "category", "sales_rep"]:
        if dim_field in df.columns:
            filters[dim_field] = sorted(
                df[dim_field].drop_nulls().unique().to_list()
            )

    return {"success": True, "filters": filters}


# ─── 前端静态文件 ─────────────────────────────────────────────

FRONTEND_DIR = Path(__file__).parent / "frontend"

@app.get("/", response_class=HTMLResponse)
async def index():
    """首页 → 登录页"""
    return FRONTEND_DIR.joinpath("index.html").read_text(encoding="utf-8")

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    """仪表板页"""
    return FRONTEND_DIR.joinpath("dashboard.html").read_text(encoding="utf-8")

app.mount("/static", StaticFiles(directory=str(FRONTEND_DIR)), name="static")


# ─── 启动 ──────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
