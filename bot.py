import logging
import os
from datetime import datetime
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

# --- НАСТРОЙКИ (ЗАМЕНИ НА СВОИ) ---
BOT_TOKEN = "8603711589:AAEqA6rQU-m5BxNAcdvMGnIrW4jtrxB2Avg"
ADMIN_ID = 1121954610  # Твой Telegram ID (число)
LOG_FILE = "logs.txt"      # Файл для хранения сообщений

# --- Настройка логирования в консоль (для Render) ---
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# === ФУНКЦИЯ ДЛЯ СОХРАНЕНИЯ СООБЩЕНИЙ ===
def save_message(user_id, username, first_name, text):
    """Сохраняет сообщение в файл с датой и временем"""
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
        f"Привет, {user.first_name}! Я бот-консультант. Напиши свой вопрос, и я отвечу автоматически, а твоё сообщение сохранится."
    )

# === АВТООТВЕТЧИК + СОХРАНЕНИЕ ===
async def auto_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отвечает на сообщение и сохраняет его в лог"""
    user = update.effective_user
    user_message = update.message.text
    
    # 1. Сохраняем сообщение в файл
    save_message(
        user_id=user.id,
        username=user.username or "нет_username",
        first_name=user.first_name or "нет_имени",
        text=user_message
    )
    
    # 2. Логируем в консоль (чтобы видеть в Render)
    logger.info(f"Сообщение от {user.id} (@{user.username}): {user_message}")
    
    # 3. Отправляем автоответ
    reply_text = "✅ Спасибо за обращение! Ваша заявка принята. Мы свяжемся с вами в ближайшее время."
    await update.message.reply_text(reply_text)

# === КОМАНДА ДЛЯ ТЕБЯ (просмотр логов) ===
async def admin_logs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отправляет админу файл с логами"""
    user_id = update.effective_user.id
    
    # Проверяем, что команду вызвал админ
    if user_id != ADMIN_ID:
        await update.message.reply_text("⛔ У тебя нет прав для этой команды.")
        return
    
    try:
        # Проверяем, существует ли файл и не пустой ли он
        if os.path.exists(LOG_FILE) and os.path.getsize(LOG_FILE) > 0:
            with open(LOG_FILE, "rb") as f:
                await update.message.reply_document(
                    document=f,
                    filename=f"logs_{datetime.now().strftime('%Y%m%d')}.txt",
                    caption="📋 Вот все сообщения за всё время"
                )
        else:
            await update.message.reply_text("📭 Логов пока нет.")
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка при отправке логов: {e}")
        logger.error(f"Ошибка отправки логов: {e}")

# === ГЛАВНАЯ ФУНКЦИЯ ===
def main():
    """Запуск бота"""
    # Создаем приложение
    application = Application.builder().token(BOT_TOKEN).build()
    
    # Добавляем обработчики
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("admin_logs", admin_logs))  # Новая команда
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, auto_reply))
    
    # Запускаем
    logger.info("🚀 Бот запущен и готов к работе!")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()