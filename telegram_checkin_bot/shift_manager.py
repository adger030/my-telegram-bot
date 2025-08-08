import json
import os
from datetime import datetime, time
from threading import Lock
from config import ADMIN_IDS

SHIFT_FILE = os.path.join("data", "shift_config.json")
_lock = Lock()

# ===========================
# 全局变量（热更新用）
# ===========================
SHIFT_OPTIONS = {}
SHIFT_TIMES = {}

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

# ===========================
# 命令：编辑/添加班次
# ===========================
async def edit_shift_cmd(update, context):
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("❌ 你不是管理员，没有权限编辑班次。")
        return

    if len(context.args) != 4:
        await update.message.reply_text("⚠️ 用法：/edit_shift 班次代码 班次名称 上班时间 下班时间\n例如：/edit_shift F F班 12:00 21:00")
        return

    code = context.args[0].upper()
    label = context.args[1]
    start_str = context.args[2]
    end_str = context.args[3]

    try:
        start_time = datetime.strptime(start_str, "%H:%M").time()
        end_time = datetime.strptime(end_str, "%H:%M").time()
    except ValueError:
        await update.message.reply_text("⚠️ 时间格式错误，请用 HH:MM（24小时制）")
        return

    cfg = load_shift_config()
    cfg[code] = {
        "label": f"{label}（{start_str}-{end_str}）",
        "start": start_str,
        "end": end_str
    }
    save_shift_config(cfg)

    await update.message.reply_text(f"✅ 已更新/添加班次 {code}：{label}（{start_str}-{end_str}）")

# ===========================
# 命令：删除班次
# ===========================
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

# ===========================
# 命令：列出班次
# ===========================
async def list_shifts_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """列出当前班次配置（无重复时间）"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("❌ 无权限，仅管理员可查看班次配置。")
        return

    # 读取 JSON 班次配置
    shift_options = load_shift_options()

    if not shift_options:
        await update.message.reply_text("⚠️ 当前没有配置任何班次。")
        return

    lines = ["📅 当前班次配置："]
    for code, desc in shift_options.items():
        lines.append(f"{code}: {desc}")  # 直接显示描述，不额外加时间段

    await update.message.reply_text("\n".join(lines))
