import os
import psycopg2
from sqlalchemy import create_engine
from datetime import datetime, timedelta, timezone

DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)
BEIJING_TZ = timezone(timedelta(hours=8))

def get_conn():
    return psycopg2.connect(DATABASE_URL)

def get_db():
    return get_conn()

def init_db():
    """初始化数据库结构"""
    with get_conn() as conn:
        with conn.cursor() as cur:
            # ✅ 创建 users 表，username 作为主键
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    username TEXT PRIMARY KEY,
                    name TEXT NOT NULL
                );
            """)

            # ✅ 创建 messages 表
            cur.execute("""
                CREATE TABLE IF NOT EXISTS messages (
                    id SERIAL PRIMARY KEY,
                    username TEXT,
                    content TEXT,
                    timestamp TIMESTAMPTZ NOT NULL,
                    keyword TEXT,
                    name TEXT,
                    shift TEXT
                );
            """)
            conn.commit()
            print("✅ 数据库初始化完成（基于 username）。")

def migrate_username(old_username: str, new_username: str):
    """
    用户改用户名后迁移数据：
    - 更新 users 表
    - 更新 messages 表
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT name FROM users WHERE username=%s", (old_username,))
            row = cur.fetchone()

            if row:
                cur.execute("UPDATE users SET username=%s WHERE username=%s", (new_username, old_username))
                cur.execute("UPDATE messages SET username=%s WHERE username=%s", (new_username, old_username))
                conn.commit()
                print(f"🔄 用户名迁移完成：{old_username} → {new_username}")
            else:
                print(f"⚠️ 未找到旧用户名 {old_username}，无需迁移。")

def sync_username(username: str):
    """
    确保用户存在，如果检测到改名则迁移数据
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            # 检查是否已存在该用户名
            cur.execute("SELECT username FROM users WHERE username=%s", (username,))
            if cur.fetchone():
                return  # 用户已存在，无需处理

            # 查找最近的消息记录中的旧用户名
            cur.execute("SELECT username FROM messages ORDER BY timestamp DESC LIMIT 1;")
            last = cur.fetchone()

            if last and last[0] != username:
                migrate_username(last[0], username)
            else:
                # 新用户直接注册
                cur.execute("INSERT INTO users (username, name) VALUES (%s, %s)", (username, ""))
                print(f"✅ 新用户 {username} 已注册。")
            conn.commit()

def save_message(username, name, content, timestamp, keyword, shift=None):
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=BEIJING_TZ)
    else:
        timestamp = timestamp.astimezone(BEIJING_TZ)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO messages (username, name, content, timestamp, keyword, shift)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (username, name, content, timestamp, keyword, shift))
            conn.commit()

def get_user_logs(username, start, end):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT timestamp, keyword, shift FROM messages
                WHERE username=%s AND timestamp >= %s AND timestamp < %s
                ORDER BY timestamp ASC
            """, (username, start, end))
            return cur.fetchall()

def get_user_month_logs(username):
    now = datetime.now(BEIJING_TZ)
    start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    end = (start.replace(day=28) + timedelta(days=4)).replace(day=1)
    return get_user_logs(username, start, end)

def get_user_name(username):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT name FROM users WHERE username=%s", (username,))
            row = cur.fetchone()
            return row[0] if row else None

def set_user_name(username, name):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT username FROM users WHERE name=%s AND username!=%s", (name, username))
            if cur.fetchone():
                raise ValueError(f"姓名 {name} 已被使用，请换一个。")
            cur.execute("""
                INSERT INTO users (username, name)
                VALUES (%s, %s)
                ON CONFLICT (username) DO UPDATE SET name=EXCLUDED.name
            """, (username, name))
            conn.commit()
