import sqlite3
import os
from datetime import datetime, timedelta
from config import DATA_DIR

DB_PATH = os.path.join(DATA_DIR, "messages.db")

def init_db():
    os.makedirs(DATA_DIR, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT,
                content TEXT,
                timestamp TEXT,
                keyword TEXT
            )
        """)

def has_user_checked_keyword_today(username, keyword):
    today_str = datetime.now().strftime("%Y-%m-%d")
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT COUNT(*) FROM messages
            WHERE username = ? AND keyword = ?
            AND DATE(timestamp) = ?
        """, (username, keyword, today_str))
        count = cursor.fetchone()[0]
    return count > 0

def save_message(username, content, timestamp, keyword):
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO messages (username, content, timestamp, keyword)
            VALUES (?, ?, ?, ?)
        """, (username, content, timestamp, keyword))
        conn.commit()
        
def get_user_month_logs(username):
    conn = sqlite3.connect(os.path.join(DATA_DIR, "messages.db"))
    cursor = conn.cursor()
    now = datetime.now()
    start_month = now.replace(day=1).strftime("%Y-%m-%d")
    end_month = now.strftime("%Y-%m-%d")

    cursor.execute("""
        SELECT timestamp, keyword FROM messages
        WHERE username = ? AND DATE(timestamp) BETWEEN ? AND ?
        ORDER BY timestamp
    """, (username, start_month, end_month))

    logs = cursor.fetchall()
    conn.close()
    return logs

def delete_old_data(days=30):
    cutoff = (datetime.now() - timedelta(days=days)).isoformat()
    with sqlite3.connect(DB_PATH) as conn:
        # 删除记录
        cursor = conn.cursor()
        cursor.execute("""
            SELECT content FROM messages WHERE timestamp < ? AND content LIKE '%.jpg'
        """, (cutoff,))
        photos = [row[0] for row in cursor.fetchall()]
        conn.execute("DELETE FROM messages WHERE timestamp < ?", (cutoff,))

    # 删除旧图片
    for path in photos:
        if os.path.exists(path):
            try:
                os.remove(path)
            except Exception as e:
                print(f"删除图片失败 {path}: {e}")
