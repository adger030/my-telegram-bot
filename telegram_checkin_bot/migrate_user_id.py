import psycopg2
import os

DATABASE_URL = os.getenv("DATABASE_URL")

def get_conn():
    return psycopg2.connect(DATABASE_URL)

def migrate_user_ids(user_map):
    """
    user_map: dict[username -> user_id]
    需要传入当前机器人缓存的用户名-固定ID映射（可从 Telegram API 获取 update.effective_user）
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            # 1️⃣ 补全 users 表
            for username, user_id in user_map.items():
                cur.execute("""
                    UPDATE users SET user_id = %s WHERE username = %s AND (user_id IS NULL OR user_id != %s)
                """, (user_id, username, user_id))
            print("✅ 已更新 users 表的 user_id")

            # 2️⃣ 补全 messages 表
            for username, user_id in user_map.items():
                cur.execute("""
                    UPDATE messages SET user_id = %s 
                    WHERE username = %s AND (user_id IS NULL OR user_id != %s)
                """, (user_id, username, user_id))
            print("✅ 已更新 messages 表的 user_id")

            conn.commit()


def find_missing_user_ids():
    """检查哪些记录缺少 user_id"""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT username FROM users WHERE user_id IS NULL")
            missing_users = [row[0] for row in cur.fetchall()]
            cur.execute("SELECT DISTINCT username FROM messages WHERE user_id IS NULL")
            missing_msgs = [row[0] for row in cur.fetchall()]
    return set(missing_users) | set(missing_msgs)


if __name__ == "__main__":
    print("🔍 正在检查缺少 user_id 的用户...")
    missing = find_missing_user_ids()
    if missing:
        print("以下用户名缺少 user_id，请提供对应的 Telegram ID:")
        print(missing)
        # 示例映射（手动或从机器人缓存/Telegram API获取）
        user_map = {
            "old_username1": 123456789,
            "old_username2": 987654321,
        }
        migrate_user_ids(user_map)
    else:
        print("✅ 所有用户均已补全 user_id，无需迁移")
