#!/usr/bin/env python3
"""
管理员工具 - 用户管理
用法:
    python admin.py add-user --name 张三 --days 30
    python admin.py list-users
    python admin.py disable-user --name 张三
    python admin.py enable-user --name 张三
    python admin.py reset-password --name 张三 --password newpass
"""
import argparse
import sqlite3
from datetime import datetime, timedelta

import bcrypt

from auth import DB_PATH


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def add_user(name: str, password: str, days: int = 365):
    password_hash = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
    expire_date = (datetime.now() + timedelta(days=days)).strftime("%Y-%m-%d")
    created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with get_conn() as conn:
        try:
            conn.execute(
                "INSERT INTO users (username, password_hash, status, expire_date, created_at) VALUES (?, ?, 'active', ?, ?)",
                (name, password_hash, expire_date, created_at),
            )
            conn.commit()
            print(f"✅ 用户 '{name}' 已创建，有效期 {days} 天，到期: {expire_date}")
        except sqlite3.IntegrityError:
            print(f"❌ 用户 '{name}' 已存在")


def list_users():
    with get_conn() as conn:
        rows = conn.execute("SELECT * FROM users ORDER BY created_at DESC").fetchall()

    if not rows:
        print("暂无用户")
        return

    print(f"{'用户名':<15} {'状态':<8} {'到期日期':<12} {'创建时间':<20}")
    print("-" * 60)
    for row in rows:
        print(f"{row['username']:<15} {row['status']:<8} {row['expire_date']:<12} {row['created_at']:<20}")


def disable_user(name: str):
    with get_conn() as conn:
        result = conn.execute(
            "UPDATE users SET status = 'disabled' WHERE username = ?", (name,)
        ).rowcount
        conn.commit()
        if result:
            print(f"✅ 用户 '{name}' 已禁用")
        else:
            print(f"❌ 用户 '{name}' 不存在")


def enable_user(name: str):
    with get_conn() as conn:
        result = conn.execute(
            "UPDATE users SET status = 'active' WHERE username = ?", (name,)
        ).rowcount
        conn.commit()
        if result:
            print(f"✅ 用户 '{name}' 已启用")
        else:
            print(f"❌ 用户 '{name}' 不存在")


def reset_password(name: str, password: str):
    password_hash = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
    with get_conn() as conn:
        result = conn.execute(
            "UPDATE users SET password_hash = ? WHERE username = ?", (password_hash, name)
        ).rowcount
        conn.commit()
        if result:
            print(f"✅ 用户 '{name}' 密码已重置")
        else:
            print(f"❌ 用户 '{name}' 不存在")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="销售数据仪表板 - 用户管理")
    subparsers = parser.add_subparsers(dest="command")

    # add-user
    p_add = subparsers.add_parser("add-user", help="创建新用户")
    p_add.add_argument("--name", required=True, help="用户名")
    p_add.add_argument("--password", default="123456", help="初始密码（默认 123456）")
    p_add.add_argument("--days", type=int, default=365, help="有效期天数（默认 365）")

    # list-users
    subparsers.add_parser("list-users", help="列出所有用户")

    # disable-user
    p_dis = subparsers.add_parser("disable-user", help="禁用用户")
    p_dis.add_argument("--name", required=True, help="用户名")

    # enable-user
    p_en = subparsers.add_parser("enable-user", help="启用用户")
    p_en.add_argument("--name", required=True, help="用户名")

    # reset-password
    p_reset = subparsers.add_parser("reset-password", help="重置密码")
    p_reset.add_argument("--name", required=True, help="用户名")
    p_reset.add_argument("--password", required=True, help="新密码")

    args = parser.parse_args()

    if args.command == "add-user":
        add_user(args.name, args.password, args.days)
    elif args.command == "list-users":
        list_users()
    elif args.command == "disable-user":
        disable_user(args.name)
    elif args.command == "enable-user":
        enable_user(args.name)
    elif args.command == "reset-password":
        reset_password(args.name, args.password)
    else:
        parser.print_help()
