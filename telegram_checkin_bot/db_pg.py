import os
import psycopg2
from sqlalchemy import create_engine
from datetime import datetime, timedelta, timezone

# 从环境变量中获取数据库连接 URL
DATABASE_URL = os.getenv("DATABASE_URL")

# 创建 SQLAlchemy 数据库引擎
engine = create_engine(DATABASE_URL)

def get_conn():
    return psycopg2.connect(os.environ["DATABASE_URL"])

def init_db():
    with get_conn() as conn:
        with conn.cursor() as cur:
            # 创建表
            cur.execute("""
                CREATE TABLE IF NOT EXISTS messages (
                    id SERIAL PRIMARY KEY,
                    username TEXT,
                    content TEXT,
                    timestamp TIMESTAMPTZ NOT NULL,
                    keyword TEXT
                );
            """)
            conn.commit()

            # 检查是否有 shift 列
            cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='messages'")
            columns = [row[0] for row in cur.fetchall()]
            if "shift" not in columns:
                cur.execute("ALTER TABLE messages ADD COLUMN shift TEXT;")
                conn.commit()
                print("✅ 已为 messages 表添加 shift 字段")


BEIJING_TZ = timezone(timedelta(hours=8))

def has_user_checked_keyword_today(username, keyword):
    today = datetime.now(BEIJING_TZ).date()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM messages
                WHERE username = %s AND keyword = %s 
                  AND DATE(timestamp AT TIME ZONE 'Asia/Shanghai') = %s
            """, (username, keyword, today))
            count = cur.fetchone()[0]
    return count > 0

def save_message(username, content, timestamp, keyword):
    # 强制确保时间是 Asia/Shanghai
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=BEIJING_TZ)
    else:
        timestamp = timestamp.astimezone(BEIJING_TZ)

    print(f"[DB] Saving: {username}, {content}, {timestamp}, {keyword}")
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO messages (username, content, timestamp, keyword)
                VALUES (%s, %s, %s, %s)
            """, (username, content, timestamp, keyword))
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
    """
    获取用户当月打卡记录（基于 get_user_logs 封装）
    """
    now = datetime.now(BEIJING_TZ)
    start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    end = (start.replace(day=28) + timedelta(days=4)).replace(day=1)  # 下月1号
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
    return photos  # 留给图片清理函数处理

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
            """, (shift.strip(), username, username))  # 确保去除空格并完整保存
            conn.commit()
