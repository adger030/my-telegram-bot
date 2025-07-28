import os
import psycopg2
from sqlalchemy import create_engine
from datetime import datetime, timedelta, timezone

DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)
BEIJING_TZ = timezone(timedelta(hours=8))

def get_conn():
    return psycopg2.connect(DATABASE_URL)

def init_db():
    with get_conn() as conn:
        with conn.cursor() as cur:
            # 创建 messages 表
            cur.execute("""
                CREATE TABLE IF NOT EXISTS messages (
                    id SERIAL PRIMARY KEY,
                    username TEXT,
                    content TEXT,
                    timestamp TIMESTAMPTZ NOT NULL,
                    keyword TEXT
                );
            """)

            # 检查并补充 name 和 shift 列
            cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='messages'")
            columns = [row[0] for row in cur.fetchall()]

            if "name" not in columns:
                cur.execute("ALTER TABLE messages ADD COLUMN name TEXT;")
                print("✅ 已为 messages 表添加 name 字段")

            if "shift" not in columns:
                cur.execute("ALTER TABLE messages ADD COLUMN shift TEXT;")
                print("✅ 已为 messages 表添加 shift 字段")

            # 创建 users 表（name 唯一）
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    username TEXT PRIMARY KEY,
                    name TEXT UNIQUE NOT NULL
                );
            """)
            conn.commit()


def has_user_checked_keyword_today(username, keyword):
    today = datetime.now(BEIJING_TZ).date()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM messages
                WHERE username = %s AND keyword = %s 
                  AND DATE(timestamp AT TIME ZONE 'Asia/Shanghai') = %s
            """, (username, keyword, today))
            return cur.fetchone()[0] > 0

# ✅ 支持 name 字段的 save_message
def save_message(username, name, content, timestamp, keyword, shift=None):
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=BEIJING_TZ)
    else:
        timestamp = timestamp.astimezone(BEIJING_TZ)

    print(f"[DB] Saving: {username}, {name}, {content}, {timestamp}, {keyword}, shift={shift}")
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
                WHERE username = %s AND timestamp >= %s AND timestamp < %s
                ORDER BY timestamp ASC
            """, (username, start, end))
            return cur.fetchall()

def get_user_month_logs(username):
    now = datetime.now(BEIJING_TZ)
    start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    end = (start.replace(day=28) + timedelta(days=4)).replace(day=1)
    return get_user_logs(username, start, end)

def delete_old_data(days=30):
    cutoff = datetime.now() - timedelta(days=days)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT content FROM messages
                WHERE timestamp < %s AND content LIKE '%%.jpg'
            """, (cutoff,))
            photos = [row[0] for row in cur.fetchall()]
            cur.execute("DELETE FROM messages WHERE timestamp < %s", (cutoff,))
            conn.commit()
    return photos

def save_shift(username, shift):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE messages 
                SET shift = %s 
                WHERE username = %s 
                AND timestamp = (
                    SELECT MAX(timestamp) FROM messages WHERE username = %s
                )
            """, (shift, username, username))
            conn.commit()

def get_today_shift(username):
    today = datetime.now(BEIJING_TZ).date()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT shift FROM messages
                WHERE username = %s 
                AND keyword = '#上班打卡'
                AND DATE(timestamp AT TIME ZONE 'Asia/Shanghai') = %s
                ORDER BY timestamp DESC
                LIMIT 1
            """, (username, today))
            row = cur.fetchone()
            return row[0] if row else None

# ✅ 用户姓名管理
def get_user_name(username):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT name FROM users WHERE username = %s", (username,))
            row = cur.fetchone()
            return row[0] if row else None

def set_user_name(username, name):
    with get_conn() as conn:
        with conn.cursor() as cur:
            # 检查是否已有相同姓名（排除自己）
            cur.execute("SELECT username FROM users WHERE name = %s AND username != %s", (name, username))
            if cur.fetchone():
                raise ValueError(f"姓名 {name} 已被使用，请换一个。")

            # 插入或更新
            cur.execute("""
                INSERT INTO users (username, name)
                VALUES (%s, %s)
                ON CONFLICT (username) DO UPDATE SET name = EXCLUDED.name
            """, (username, name))
            conn.commit()

