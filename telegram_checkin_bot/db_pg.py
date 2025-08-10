import os
import psycopg2
from sqlalchemy import create_engine
from datetime import datetime, timedelta, timezone
from config import BEIJING_TZ, DATABASE_URL

# ===========================
# 数据库配置
# ===========================
engine = create_engine(DATABASE_URL)


# ===========================
# 数据库连接封装
# ===========================
def get_conn():
    return psycopg2.connect(DATABASE_URL)

def get_db():
    return get_conn()


# ===========================
# 初始化数据库结构
# ===========================
def init_db():
    with get_conn() as conn:
        with conn.cursor() as cur:
            
            # 创建 messages 表
            cur.execute("""
                CREATE TABLE IF NOT EXISTS messages (
                    id SERIAL PRIMARY KEY,
                    username TEXT,
                    name TEXT,
                    keyword TEXT,
                    timestamp TIMESTAMPTZ NOT NULL,
                    shift TEXT,
                    content TEXT
                );
            """)

            # 创建 users 表
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    username TEXT PRIMARY KEY,
                    name TEXT UNIQUE NOT NULL
                );
            """)

            # 创建 shifts 表
            cur.execute("""
                CREATE TABLE IF NOT EXISTS shifts (
                    code TEXT PRIMARY KEY,
                    label TEXT NOT NULL,
                    start TEXT NOT NULL,
                    "end" TEXT NOT NULL
                );
            """)
            conn.commit()
    print("✅ 数据库已重建")


# ===========================
# 初始化默认班次
# ===========================
def init_shifts():
    from shift_manager import reload_shift_globals  # 避免循环导入

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM shifts;")
            if cur.fetchone()[0] == 0:
                defaults = [
                    ("F", "F班（12:00-21:00）", "12:00", "21:00"),
                    ("G", "G班（13:00-22:00）", "13:00", "22:00"),
                    ("H", "H班（14:00-23:00）", "14:00", "23:00"),
                    ("I", "I班（15:00-00:00）", "15:00", "00:00")
                ]
                cur.executemany("""
                    INSERT INTO shifts (code, label, start, "end")
                    VALUES (%s, %s, %s, %s)
                """, defaults)
                conn.commit()
                print("✅ 默认班次已初始化")

    reload_shift_globals()
    
init_shifts()

# ===========================
# 用户打卡检查（指定关键词）
# ===========================
def has_user_checked_keyword_today(username, keyword, day_offset=0):
    """
    检查用户是否在当天（或偏移日期）打过指定关键词卡
    :param username: 用户名
    :param keyword: 关键词（如 #上班打卡）
    :param day_offset: 日期偏移（用于凌晨跨天处理）
    """
    target_date = (datetime.now(BEIJING_TZ) + timedelta(days=day_offset)).date()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM messages
                WHERE username = %s AND keyword = %s 
                  AND DATE(timestamp AT TIME ZONE 'Asia/Shanghai') = %s
            """, (username, keyword, target_date))
            return cur.fetchone()[0] > 0


# ===========================
# 保存打卡记录
# ===========================
def save_message(username, name, content, timestamp, keyword, shift=None):
    # 统一时区：若无 tzinfo，则加上北京时间
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


# ===========================
# 查询用户打卡日志
# ===========================
def get_user_logs(username, start, end):
    """查询用户指定时间段的所有打卡记录"""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT timestamp, keyword, shift FROM messages
                WHERE username = %s AND timestamp >= %s AND timestamp < %s
                ORDER BY timestamp ASC
            """, (username, start, end))
            return cur.fetchall()

def get_user_month_logs(username):
    """查询用户本月打卡记录"""
    now = datetime.now(BEIJING_TZ)
    start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    end = (start.replace(day=28) + timedelta(days=4)).replace(day=1)  # 下月 1 号
    return get_user_logs(username, start, end)


def get_user_logs_by_name(name: str, start: datetime, end: datetime):
    """根据 name 字段查询用户打卡记录"""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT timestamp, keyword, shift
                FROM messages
                WHERE name = %s AND timestamp >= %s AND timestamp < %s
                ORDER BY timestamp ASC
            """, (name, start, end))
            return cur.fetchall()
            
# ===========================
# 删除旧数据（含过期图片）
# ===========================
def delete_old_data(days=30):
    """删除指定天数前的数据，并返回被删除的图片路径列表"""
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


# ===========================
# 更新用户班次
# ===========================
def save_shift(username, shift):
    """更新用户最后一条打卡记录的班次"""
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


# ===========================
# 获取用户当天班次
# ===========================
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


# ===========================
# 用户姓名相关操作
# ===========================
def get_user_name(username):
    """查询用户名对应的姓名"""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT name FROM users WHERE username = %s", (username,))
            row = cur.fetchone()
            return row[0] if row else None

def set_user_name(username, name):
    """设置/更新用户名与姓名映射（姓名唯一性检查）"""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT username FROM users WHERE name = %s AND username != %s", (name, username))
            if cur.fetchone():
                raise ValueError(f"姓名 {name} 已被使用，请换一个。")

            cur.execute("""
                INSERT INTO users (username, name)
                VALUES (%s, %s)
                ON CONFLICT (username) DO UPDATE SET name = EXCLUDED.name
            """, (username, name))
            conn.commit()


# ===========================
# 迁移用户数据（合并账号）
# ===========================
def transfer_user_data(user_a, user_b):
    """
    将用户 A 的所有数据迁移到用户 B：
    1. 合并 messages 表（修改 username & name）
    2. 如果 B 没有姓名且 A 有姓名，则迁移姓名
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            # 检查 A 是否存在
            cur.execute("SELECT username, name FROM users WHERE username=%s", (user_a,))
            user_a_data = cur.fetchone()
            if not user_a_data:
                raise ValueError(f"用户 {user_a} 不存在。")

            # 检查 B 是否存在
            cur.execute("SELECT username, name FROM users WHERE username=%s", (user_b,))
            user_b_data = cur.fetchone()
            if not user_b_data:
                raise ValueError(f"用户 {user_b} 不存在。")

            # 若 B 无姓名但 A 有姓名，则迁移姓名
            if user_a_data[1] and not user_b_data[1]:
                cur.execute("UPDATE users SET name=%s WHERE username=%s", (user_a_data[1], user_b))

            # 更新 messages：将 A 的记录归属迁移到 B
            cur.execute("""
                UPDATE messages
                SET username=%s, name=(SELECT name FROM users WHERE username=%s)
                WHERE username=%s
            """, (user_b, user_b, user_a))

            conn.commit()
            print(f"✅ 数据已从 {user_a} 转移至 {user_b}")
