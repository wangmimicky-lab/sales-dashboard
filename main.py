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
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

from core import FieldMapper, DataCleaner, Analyzer, AnalysisConfig

# ─── 认证模块 ──────────────────────────────────────────────────
import auth

# ─── FastAPI 应用 ──────────────────────────────────────────────
app = FastAPI(title="销售数据仪表板", version="0.1.0")

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

# 当前会话数据（MVP 单期覆盖）
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

    # 返回 Token 给前端存储（Cookie 模式）
    from fastapi.responses import JSONResponse
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
    from fastapi.responses import JSONResponse
    resp = JSONResponse(content={"success": True})
    resp.delete_cookie(key="access_token")
    return resp


@app.get("/api/auth/me")
async def get_me(current_user: dict = Depends(auth.get_current_user)):
    """获取当前用户信息"""
    return current_user


# ─── 数据上传 & 处理 ──────────────────────────────────────────

@app.post("/api/upload")
async def upload_file(
    file: UploadFile = File(...),
    current_user: dict = Depends(auth.get_current_user),
):
    """
    上传数据文件（CSV/Excel）
    执行：字段识别 → 数据清洗 → 返回结果摘要
    """
    # 保存文件
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

    # 字段映射
    mapper = FieldMapper()
    raw_columns = df.columns
    mapping = mapper.map_columns(raw_columns)

    if len(mapping) < 3:
        raise HTTPException(
            status_code=400,
            detail=f"字段识别不足：仅匹配 {len(mapping)} 个字段，至少需要 3 个（订单号、日期、金额）",
        )

    # 数据清洗
    cleaner = DataCleaner()
    df_clean = cleaner.clean(df, mapping)

    if len(df_clean) == 0:
        raise HTTPException(status_code=400, detail="清洗后无有效数据")

    # 存储到会话
    _session_data.clear()
    _session_data["df"] = df_clean
    _session_data["mapping"] = mapping
    _session_data["mapper_report"] = mapper.get_report()
    _session_data["cleaner_report"] = cleaner.get_report()
    _session_data["filename"] = file.filename
    _session_data["uploaded_at"] = datetime.now().isoformat()

    return {
        "success": True,
        "filename": file.filename,
        "rows": len(df_clean),
        "columns": len(df_clean.columns),
        "mapping": mapper.get_report(),
        "cleaning": cleaner.get_report(),
    }


# ─── 分析数据接口 ─────────────────────────────────────────────

@app.get("/api/filters")
async def get_filters(
    current_user: dict = Depends(auth.get_current_user),
):
    """获取筛选器选项（经销商列表、区域列表）"""
    if "df" not in _session_data:
        raise HTTPException(status_code=400, detail="请先上传数据文件")

    df = _session_data["df"]
    filters = {}

    if "customer_name" in df.columns:
        filters["dealers"] = sorted(df["customer_name"].drop_nulls().unique().to_list())

    if "region" in df.columns:
        filters["regions"] = sorted(df["region"].drop_nulls().unique().to_list())

    return {"success": True, "filters": filters}


@app.get("/api/analysis")
async def get_analysis(
    granularity: str = "auto",
    dealer: Optional[str] = None,
    region: Optional[str] = None,
    current_user: dict = Depends(auth.get_current_user),
):
    """
    获取分析数据（4 大模块）
    支持按经销商、区域筛选
    """
    if "df" not in _session_data:
        raise HTTPException(status_code=400, detail="请先上传数据文件")

    df = _session_data["df"].clone()

    # 应用筛选
    if dealer and dealer != "全部":
        if "customer_name" in df.columns:
            df = df.filter(pl.col("customer_name") == dealer)

    if region and region != "全部":
        if "region" in df.columns:
            df = df.filter(pl.col("region") == region)

    if len(df) == 0:
        return {"success": True, "message": "筛选后无数据"}

    # 执行分析
    analyzer = Analyzer()
    result = analyzer.analyze_all(df, granularity=granularity)

    return {"success": True, "data": result}


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
