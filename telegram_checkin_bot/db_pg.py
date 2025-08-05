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
    with get_conn() as conn:
        with conn.cursor() as cur:
            # ✅ 创建 messages 表，增加 user_id
            cur.execute("""
                CREATE TABLE IF NOT EXISTS messages (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT,
                    username TEXT,
                    content TEXT,
                    timestamp TIMESTAMPTZ NOT NULL,
                    keyword TEXT,
                    name TEXT,
                    shift TEXT
                );
            """)

            # ✅ 创建 users 表，使用 user_id 为主键
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    username TEXT,
                    name TEXT UNIQUE NOT NULL
                );
            """)

            # 检查 messages 表是否有 user_id 列（兼容旧版本）
            cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='messages'")
            columns = [row[0] for row in cur.fetchall()]
            if "user_id" not in columns:
                cur.execute("ALTER TABLE messages ADD COLUMN user_id BIGINT;")

            # 检查 users 表是否有 user_id 列（兼容旧版本）
            cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='users'")
            u_columns = [row[0] for row in cur.fetchall()]
            if "user_id" not in u_columns:
                cur.execute("ALTER TABLE users ADD COLUMN user_id BIGINT;")
            conn.commit()

def sync_username(user_id, username):
    """同步用户的最新 Telegram username"""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO users (user_id, username, name)
                VALUES (%s, %s, COALESCE((SELECT name FROM users WHERE user_id=%s), ''))
                ON CONFLICT (user_id) DO UPDATE SET username=EXCLUDED.username
            """, (user_id, username, user_id))
            conn.commit()

def has_user_checked_keyword_today(user_id, keyword, day_offset=0):
    target_date = (datetime.now(BEIJING_TZ) + timedelta(days=day_offset)).date()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM messages
                WHERE user_id=%s AND keyword=%s
                  AND DATE(timestamp AT TIME ZONE 'Asia/Shanghai')=%s
            """, (user_id, keyword, target_date))
            return cur.fetchone()[0] > 0

def save_message(user_id, username, name, content, timestamp, keyword, shift=None):
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=BEIJING_TZ)
    else:
        timestamp = timestamp.astimezone(BEIJING_TZ)
    print(f"[DB] Saving: {user_id}, {username}, {name}, {content}, {timestamp}, {keyword}, shift={shift}")
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO messages (user_id, username, name, content, timestamp, keyword, shift)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (user_id, username, name, content, timestamp, keyword, shift))
            conn.commit()

def get_user_logs(user_id, start, end):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT timestamp, keyword, shift FROM messages
                WHERE user_id=%s AND timestamp >= %s AND timestamp < %s
                ORDER BY timestamp ASC
            """, (user_id, start, end))
            return cur.fetchall()

def get_user_month_logs(user_id):
    now = datetime.now(BEIJING_TZ)
    start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    end = (start.replace(day=28) + timedelta(days=4)).replace(day=1)
    return get_user_logs(user_id, start, end)

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

def save_shift(user_id, shift):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE messages 
                SET shift=%s 
                WHERE user_id=%s 
                AND timestamp=(SELECT MAX(timestamp) FROM messages WHERE user_id=%s)
            """, (shift, user_id, user_id))
            conn.commit()

def get_today_shift(user_id):
    today = datetime.now(BEIJING_TZ).date()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT shift FROM messages
                WHERE user_id=%s AND keyword='#上班打卡'
                AND DATE(timestamp AT TIME ZONE 'Asia/Shanghai')=%s
                ORDER BY timestamp DESC
                LIMIT 1
            """, (user_id, today))
            row = cur.fetchone()
            return row[0] if row else None

def get_user_name(user_id):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT name FROM users WHERE user_id=%s", (user_id,))
            row = cur.fetchone()
            return row[0] if row else None

def set_user_name(user_id, username, name):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT user_id FROM users WHERE name=%s AND user_id!=%s", (name, user_id))
            if cur.fetchone():
                raise ValueError(f"姓名 {name} 已被使用，请换一个。")
            cur.execute("""
                INSERT INTO users (user_id, username, name)
                VALUES (%s, %s, %s)
                ON CONFLICT (user_id) DO UPDATE SET username=EXCLUDED.username, name=EXCLUDED.name
            """, (user_id, username, name))
            conn.commit()
