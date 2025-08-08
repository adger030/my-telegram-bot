import os
import psycopg2
from datetime import datetime
from config import ADMIN_IDS
from sqlalchemy import create_engine

# ===========================
# 数据库初始化配置
# ===========================
DATABASE_URL = os.getenv("DATABASE_URL")  # 从环境变量读取数据库连接 URL
engine = create_engine(DATABASE_URL)  # SQLAlchemy 引擎（用于 SQL 操作）

# ===========================
# 数据库连接
# ===========================
def get_conn():
    return psycopg2.connect(DATABASE_URL)

# ===========================
# 初始化数据库表
# ===========================
def init_shift_table():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS shifts (
                    code TEXT PRIMARY KEY,
                    label TEXT NOT NULL,
                    start TEXT NOT NULL,
                    "end" TEXT NOT NULL
                );
            """)
            conn.commit()

# ===========================
# 从数据库加载班次到内存
# ===========================
def reload_shift_globals():
    global SHIFT_OPTIONS, SHIFT_TIMES, SHIFT_SHORT_TIMES
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT code, label, start, \"end\" FROM shifts ORDER BY code;")
            rows = cur.fetchall()

    SHIFT_OPTIONS = {code: label for code, label, start, end in rows}

    SHIFT_TIMES = {
        label: (
            datetime.strptime(start, "%H:%M").time(),
            datetime.strptime(end, "%H:%M").time()
        )
        for code, label, start, end in rows
    }

    SHIFT_SHORT_TIMES = {
        label.split("（")[0]: (
            datetime.strptime(start, "%H:%M").time(),
            datetime.strptime(end, "%H:%M").time()
        )
        for code, label, start, end in rows
    }

# ===========================
# CRUD 操作
# ===========================
def get_shift_options():
    return SHIFT_OPTIONS

def get_shift_times():
    return SHIFT_TIMES

def get_shift_times_short():
    return SHIFT_SHORT_TIMES

def save_shift(code, name, start, end):
    """新增或更新班次"""
    label = f"{name}（{start}-{end}）"
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO shifts (code, label, start, "end")
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (code) DO UPDATE
                SET label = EXCLUDED.label,
                    start = EXCLUDED.start,
                    "end" = EXCLUDED.end
            """, (code, label, start, end))
            conn.commit()
    reload_shift_globals()

def delete_shift(code):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM shifts WHERE code = %s", (code,))
            conn.commit()
    reload_shift_globals()

# ===========================
# Telegram 命令
# ===========================
async def list_shifts_cmd(update, context):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT code, label FROM shifts ORDER BY code;")
            rows = cur.fetchall()
    lines = ["📅 当前班次配置："] + [label for code, label in rows]
    await update.message.reply_text("\n".join(lines))

async def edit_shift_cmd(update, context):
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("❌ 你不是管理员，没有权限修改班次。")
        return

    if len(context.args) < 4:
        await update.message.reply_text("⚠️ 用法：/edit_shift 班次代码 班次名 开始时间 结束时间\n"
                                        "例如：/edit_shift F F班 10:00 19:00")
        return

    code = context.args[0].upper()
    name = context.args[1]
    start = context.args[2]
    end = context.args[3]

    save_shift(code, name, start, end)
    await update.message.reply_text(f"✅ 班次 {code} 已修改为：{name}（{start}-{end}）")

async def delete_shift_cmd(update, context):
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("❌ 你不是管理员，没有权限删除班次。")
        return

    if len(context.args) != 1:
        await update.message.reply_text("⚠️ 用法：/delete_shift 班次代码\n例如：/delete_shift F")
        return

    code = context.args[0].upper()
    delete_shift(code)
    await update.message.reply_text(f"✅ 已删除班次 {code}")

# ===========================
# 初始化时创建表 & 加载数据
# ===========================
init_shift_table()

# 如果是第一次运行且表为空，就插入默认班次
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

reload_shift_globals()
