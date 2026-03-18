import os
import logging
import threading
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

# --- БЕРЁМ ТОКЕН ИЗ ПЕРЕМЕННЫХ ОКРУЖЕНИЯ ---
BOT_TOKEN = os.environ.get('8603711589:AAEqA6rQU-m5BxNAcdvMGnIrW4jtrxB2Avg')
ADMIN_ID = 1121954610  # ЗАМЕНИ НА СВОЙ ID!
LOG_FILE = "logs.txt"

# --- Логирование ---
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# === ВЕБ-СЕРВЕР ДЛЯ RENDER (только ОДИН раз!) ===
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")
    
    def log_message(self, format, *args):
        pass  # Отключаем логирование запросов

def run_health_server():
    try:
        server = HTTPServer(('0.0.0.0', 10000), HealthCheckHandler)
        logger.info("✅ Health check server started on port 10000")
        server.serve_forever()
    except Exception as e:
        logger.error(f"Health server error: {e}")

# === ФУНКЦИЯ ДЛЯ СОХРАНЕНИЯ СООБЩЕНИЙ ===
def save_message(user_id, username, first_name, text):
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            f.write(f"[{now}] | ID: {user_id} | @{username} | {first_name}: {text}\n")
    except Exception as e:
        logger.error(f"Ошибка при сохранении лога: {e}")

# === КОМАНДА /START ===
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    await update.message.reply_text(
        f"Здарова браток, {user.first_name}! Я типа твой бот-консультант, но я не хочу тебе отвечать."
    )

# === АВТООТВЕТЧИК ===
async def auto_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_message = update.message.text
    
    save_message(
        user_id=user.id,
        username=user.username or "нет_username",
        first_name=user.first_name or "нет_имени",
        text=user_message
    )
    
    logger.info(f"Сообщение от {user.id} (@{user.username}): {user_message}")
    
    reply_text = "Ну я же просил, мне не писать. Лааадно, передам всё челобеку."
    await update.message.reply_text(reply_text)

# === КОМАНДА ДЛЯ АДМИНА ===
async def admin_logs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    if user_id != ADMIN_ID:
        await update.message.reply_text("⛔ Нет прав.")
        return
    
    try:
        if os.path.exists(LOG_FILE) and os.path.getsize(LOG_FILE) > 0:
            with open(LOG_FILE, "rb") as f:
                await update.message.reply_document(
                    document=f,
                    filename=f"logs_{datetime.now().strftime('%Y%m%d')}.txt",
                    caption="📋 Логи сообщений"
                )
        else:
            await update.message.reply_text("📭 Логов пока нет.")
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")

# === ГЛАВНАЯ ФУНКЦИЯ ===
def main():
    application = Application.builder().token(BOT_TOKEN).build()
    
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("admin_logs", admin_logs))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, auto_reply))
    
    logger.info("🚀 Бот запущен и готов к работе!")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

# === ТОЧКА ВХОДА ===
if __name__ == "__main__":
    # Запускаем health check сервер в отдельном потоке
    health_thread = threading.Thread(target=run_health_server, daemon=True)
    health_thread.start()
    
    # Запускаем бота
    main()