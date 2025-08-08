import json
import os
import time
from threading import Lock
from datetime import datetime, time as dt_time
from contextlib import contextmanager
from config import ADMIN_IDS

SHIFT_FILE = "shift_config.json"
LOCK_FILE = SHIFT_FILE + ".lock"

# ===========================
# 工具函数
# ===========================
def load_shift_config():
    if not os.path.exists(SHIFT_FILE):
        return {}
    with open(SHIFT_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

def save_shift_config(data):
    with _lock:  # 防并发
        tmp_file = SHIFT_FILE + ".tmp"
        with open(tmp_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp_file, SHIFT_FILE)
    reload_shift_globals()

def reload_shift_globals():
    """重新加载班次到全局变量（热更新）"""
    global SHIFT_OPTIONS, SHIFT_TIMES
    cfg = load_shift_config()
    SHIFT_OPTIONS = {k: v["label"] for k, v in cfg.items()}
    SHIFT_TIMES = {
        v["label"]: (
            datetime.strptime(v["start"], "%H:%M").time(),
            datetime.strptime(v["end"], "%H:%M").time()
        )
        for v in cfg.values()
    }

# 启动时先加载一次
reload_shift_globals()

@contextmanager
def file_lock(lock_file, timeout=5):
    start_time = time.time()
    while True:
        try:
            fd = os.open(lock_file, os.O_CREAT | os.O_EXCL | os.O_RDWR)
            break
        except FileExistsError:
            if time.time() - start_time > timeout:
                raise TimeoutError(f"无法获取文件锁: {lock_file}")
            time.sleep(0.05)
    try:
        yield
    finally:
        os.close(fd)
        os.remove(lock_file)

def load_shift_config():
    if not os.path.exists(SHIFT_FILE):
        return {}
    with open(SHIFT_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

def save_shift_config(data):
    with file_lock(LOCK_FILE):
        tmp_file = SHIFT_FILE + ".tmp"
        with open(tmp_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp_file, SHIFT_FILE)

if not os.path.exists(SHIFT_FILE):
    default = {
        "F": {"label": "F班（12:00-21:00）", "start": "12:00", "end": "21:00"},
        "G": {"label": "G班（13:00-22:00）", "start": "13:00", "end": "22:00"},
        "H": {"label": "H班（14:00-23:00）", "start": "14:00", "end": "23:00"},
        "I": {"label": "I班（15:00-00:00）", "start": "15:00", "end": "00:00"}
    }
    save_shift_config(default)

def get_shift_options():
    """按钮显示用"""
    cfg = load_shift_config()
    return {k: v["label"] for k, v in cfg.items()}

def get_shift_times():
    """上下班时间范围"""
    cfg = load_shift_config()
    return {v["label"]: (datetime.strptime(v["start"], "%H:%M").time(),
                         datetime.strptime(v["end"], "%H:%M").time())
            for v in cfg.values()}

# ========== Telegram 命令 ==========

async def list_shifts_cmd(update, context):
    cfg = load_shift_config()
    # 按班次代码排序
    sorted_cfg = dict(sorted(cfg.items(), key=lambda x: x[0]))
    lines = ["📅 当前班次配置："]
    for code, info in sorted_cfg.items():
        lines.append(f"{code}: {info['label']}")
    text = "\n".join(lines)
    await update.message.reply_text(text)
    
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

    cfg = load_shift_config()
    cfg[code] = {
        "label": f"{name}（{start}-{end}）",
        "start": start,
        "end": end
    }
    save_shift_config(cfg)

    await update.message.reply_text(f"✅ 班次 {code} 已修改为：{cfg[code]['label']}")

async def delete_shift_cmd(update, context):
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("❌ 你不是管理员，没有权限删除班次。")
        return

    if len(context.args) != 1:
        await update.message.reply_text("⚠️ 用法：/delete_shift 班次代码\n例如：/delete_shift F")
        return

    code = context.args[0].upper()
    cfg = load_shift_config()

    if code not in cfg:
        await update.message.reply_text(f"⚠️ 班次 {code} 不存在。")
        return

    deleted_label = cfg[code]["label"]
    del cfg[code]
    save_shift_config(cfg)

    await update.message.reply_text(f"✅ 已删除班次 {code}：{deleted_label}")
