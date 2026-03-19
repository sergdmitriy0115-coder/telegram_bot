import os
import logging
from datetime import datetime
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

# --- НАСТРОЙКИ ---
BOT_TOKEN = os.environ.get('TELEGRAM_TOKEN')
ADMIN_ID = 7675037573
LOG_FILE = "logs.txt"

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

def save_message(user_id, username, first_name, text):
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            f.write(f"[{now}] | ID: {user_id} | @{username} | {first_name}: {text}\n")
    except Exception as e:
        logger.error(f"Log error: {e}")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    logger.info(f"🚀 /start от {user.id}")
    save_message(user.id, user.username or "нет", user.first_name or "нет", "/start")
    await update.message.reply_text(f"Привет, {user.first_name}! 👋 Это тестовый режим.")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.message
    
    # 🔥 ЯВНАЯ ДИАГНОСТИКА
    logger.info(f"🔥🔥🔥 ПОЛУЧЕНО СООБЩЕНИЕ от {user.id}: {message.text}")
    
    save_message(user.id, user.username or "нет", user.first_name or "нет", message.text)
    await update.message.reply_text(f"Я получил твоё сообщение: '{message.text}'. Всё работает!")

async def check_sheets(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await update.message.reply_text("✅ Тестовый бот работает!")

def main():
    logger.info("🚀 Запуск ТЕСТОВОГО бота...")
    
    app = Application.builder().token(BOT_TOKEN).build()
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("check_sheets", check_sheets))
    
    # 👇 Простейший обработчик всех текстовых сообщений
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    
    logger.info("✅ Обработчики добавлены")
    app.run_polling()

if __name__ == "__main__":
    main()
