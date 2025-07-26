import os
import psycopg2
from sqlalchemy import create_engine
from datetime import datetime, timedelta

# 从环境变量中获取数据库连接 URL
DATABASE_URL = os.getenv("DATABASE_URL")

# 创建 SQLAlchemy 数据库引擎
engine = create_engine(DATABASE_URL)

def get_conn():
    return psycopg2.connect(os.environ["DATABASE_URL"])

def init_db():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS messages (
                    id SERIAL PRIMARY KEY,
                    username TEXT,
                    content TEXT,
                    timestamp TIMESTAMPTZ NOT NULL,  -- ✅ 北京时间存入这里
                    keyword TEXT
                );
            """)
            conn.commit()

def has_user_checked_keyword_today(username, keyword):
    today = datetime.now().date()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM messages
                WHERE username = %s AND keyword = %s AND DATE(timestamp) = %s
            """, (username, keyword, today))
            count = cur.fetchone()[0]
    return count > 0

def save_message(username, content, timestamp, keyword):
    print(f"[DB] Saving: {username}, {content}, {timestamp}, {keyword}")  # 加日志
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO messages (username, content, timestamp, keyword)
                VALUES (%s, %s, %s, %s)
            """, (username, content, timestamp, keyword))
            conn.commit()

def get_user_month_logs(username):
    now = datetime.now()
    start = now.replace(day=1).date()
    end = now.date()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT timestamp, keyword FROM messages
                WHERE username = %s AND DATE(timestamp) BETWEEN %s AND %s
                ORDER BY timestamp
            """, (username, start, end))
            return cur.fetchall()

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
    return photos  # 留给图片清理函数处理
