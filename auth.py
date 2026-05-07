"""
认证模块
- bcrypt 密码哈希
- HttpOnly Cookie 会话管理
- SQLite 用户存储
"""
from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import bcrypt
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from itsdangerous import URLSafeTimedSerializer, SignatureExpired

# ─── 配置 ──────────────────────────────────────────────────────
HERMES_HOME = Path(os.environ.get("HERMES_HOME", Path.home() / ".sales-dashboard"))
DB_PATH = HERMES_HOME / "users.db"
SECRET_KEY = os.environ.get("DASHBOARD_SECRET_KEY", "dev-secret-change-in-production")

serializer = URLSafeTimedSerializer(SECRET_KEY)
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/login", auto_error=False)

TOKEN_MAX_AGE = 86400 * 7  # 7 天


# ─── 数据库操作 ────────────────────────────────────────────────

def init_db():
    """初始化用户表"""
    HERMES_HOME.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                status TEXT DEFAULT 'active',
                expire_date TEXT,
                created_at TEXT NOT NULL
            )
        """)
        conn.commit()


def create_user(username: str, password: str, days: int = 365) -> bool:
    """创建用户（密码自动 bcrypt 哈希）"""
    password_hash = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
    expire_date = (datetime.now() + timedelta(days=days)).strftime("%Y-%m-%d")
    created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute(
                "INSERT INTO users (username, password_hash, status, expire_date, created_at) VALUES (?, ?, 'active', ?, ?)",
                (username, password_hash, expire_date, created_at),
            )
            conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False  # 用户名已存在


def verify_user(username: str, password: str, skip_password: bool = False) -> Optional[dict]:
    """验证用户名密码，返回用户信息或 None"""
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM users WHERE username = ?", (username,)
        ).fetchone()

    if not row:
        return None

    user = dict(row)

    # 验证密码（可选跳过）
    if not skip_password:
        if not bcrypt.checkpw(password.encode("utf-8"), user["password_hash"].encode("utf-8")):
            return None

    # 检查状态
    if user["status"] != "active":
        return None

    # 检查有效期
    if user["expire_date"]:
        expire = datetime.strptime(user["expire_date"], "%Y-%m-%d")
        if datetime.now() > expire:
            return None

    return {
        "username": user["username"],
        "status": user["status"],
        "expire_date": user["expire_date"],
        "created_at": user["created_at"],
    }


def create_token(username: str) -> str:
    """生成签名 Token"""
    return serializer.dumps(username, salt="sales-dashboard")


def verify_token(token: str) -> Optional[str]:
    """验证 Token，返回用户名或 None"""
    try:
        username = serializer.loads(token, salt="sales-dashboard", max_age=TOKEN_MAX_AGE)
        return username
    except SignatureExpired:
        return None
    except Exception:
        return None


# ─── FastAPI 依赖注入 ──────────────────────────────────────────

from fastapi import Cookie

async def get_current_user(
    token: Optional[str] = Depends(oauth2_scheme),
    access_token: Optional[str] = Cookie(default=None),
) -> dict:
    """
    获取当前用户（用于路由保护）
    支持从 Cookie 或 Authorization header 中提取 Token
    """
    # 优先使用 header 中的 token，其次使用 cookie
    actual_token = token or access_token

    if not actual_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="未登录或会话已过期",
            headers={"WWW-Authenticate": "Bearer"},
        )

    username = verify_token(actual_token)
    if not username:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="会话无效或已过期",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # 再次验证用户状态（防止被封禁后仍可用旧 Token）
    user = verify_user(username, "", skip_password=True)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="账号已被禁用或已过期",
        )

    return user


# ─── 初始化 ────────────────────────────────────────────────────
init_db()
