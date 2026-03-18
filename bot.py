import os
import logging
import threading
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

# --- НАСТРОЙКИ ---
BOT_TOKEN = os.environ.get('TELEGRAM_TOKEN')  # Токен из переменных окружения
ADMIN_ID = 1121954610  # Твой Telegram ID (замени, если нужно)
LOG_FILE = "logs.txt"

# Хранилище связи: {admin_message_id: user_chat_id}
# Нужно, чтобы помнить, кому отвечать, когда ты реплаишь сообщение
user_reply_map = {}

# --- Логирование ---
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# === ВЕБ-СЕРВЕР ДЛЯ RENDER ===
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")
    
    def log_message(self, format, *args):
        pass

def run_health_server():
    try:
        server = HTTPServer(('0.0.0.0', 10000), HealthCheckHandler)
        logger.info("✅ Health check server started on port 10000")
        server.serve_forever()
    except Exception as e:
        logger.error(f"Health server error: {e}")

# === СОХРАНЕНИЕ В ЛОГ ===
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
        f"Добрый день, {user.first_name}! Меня зовут ADD bot. Я ваш личный помощник. Подскажите, какой у вас вопрос?"
    )

# === ОБРАБОТКА СООБЩЕНИЙ ОТ КЛИЕНТОВ ===
async def handle_client_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает сообщения от обычных пользователей и пересылает их админу"""
    user = update.effective_user
    message = update.message
    
    # Сохраняем в лог
    save_message(
        user_id=user.id,
        username=user.username or "нет_username",
        first_name=user.first_name or "нет_имени",
        text=message.text or "[не текст]"  # Для фото/видео нужна отдельная обработка
    )
    
    # Создаём информационную подпись для админа
    user_info = (
        f"📨 Сообщение от пользователя:\n"
        f"ID: {user.id}\n"
        f"Username: @{user.username if user.username else 'нет'}\n"
        f"Имя: {user.first_name}\n\n"
        f"Текст: {message.text}"
    )
    
    # Пересылаем сообщение админу
    sent_message = await context.bot.send_message(
        chat_id=ADMIN_ID,
        text=user_info
    )
    
    # Запоминаем связь: ID сообщения у админа → ID чата пользователя
    user_reply_map[sent_message.message_id] = user.id
    
    # Отправляем подтверждение клиенту
    await message.reply_text("Cпасибо за твой ответ. Скоро с тобой свяжется команда")

# === ОБРАБОТКА ОТВЕТОВ ОТ АДМИНА ===
async def handle_admin_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает ответы админа (когда он отвечает на пересланное сообщение)"""
    user = update.effective_user
    message = update.message
    
    # Проверяем, что это админ
    if user.id != ADMIN_ID:
        return
    
    # Проверяем, что это ответ на какое-то сообщение
    if not message.reply_to_message:
        await message.reply_text("ℹ️ Чтобы ответить клиенту, используй 'Reply' на его сообщение.")
        return
    
    # Получаем ID сообщения, на которое ответили
    replied_msg_id = message.reply_to_message.message_id
    
    # Ищем, какому клиенту это сообщение принадлежит
    if replied_msg_id in user_reply_map:
        client_chat_id = user_reply_map[replied_msg_id]
        
        # Отправляем ответ клиенту
        await context.bot.send_message(
            chat_id=client_chat_id,
            text=f"✏️ Ответ от администратора:\n\n{message.text}"
        )
        
        # Уведомляем админа
        await message.reply_text("✅ Ответ отправлен клиенту!")
        
        # Логируем
        logger.info(f"Админ ответил клиенту {client_chat_id}")
    else:
        await message.reply_text("❌ Не могу найти, кому отправить ответ. Возможно, бот перезапускался и потерял связь.")

# === ОБРАБОТКА ФОТО/ВИДЕО (базовая) ===
async def handle_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Просто пересылает медиафайлы админу как есть"""
    user = update.effective_user
    
    # Пересылаем оригинальное сообщение админу
    await update.message.forward(chat_id=ADMIN_ID)
    
    # Отправляем информацию о пользователе отдельно
    await context.bot.send_message(
        chat_id=ADMIN_ID,
        text=f"📎 Медиафайл от @{user.username or 'нет'} (ID: {user.id})"
    )
    
    await update.message.reply_text("✅ Файл передан администратору!")

# === КОМАНДА ДЛЯ АДМИНА (просмотр логов) ===
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
    
    # Обработчики для админа
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("admin_logs", admin_logs))
    
    # Обработчик ответов админа (должен быть перед общим обработчиком)
    application.add_handler(MessageHandler(
        filters.Chat(ADMIN_ID) & filters.TEXT & ~filters.COMMAND,
        handle_admin_reply
    ))
    
    # Обработчик медиа от клиентов
    application.add_handler(MessageHandler(
        ~filters.Chat(ADMIN_ID) & (filters.PHOTO | filters.VIDEO | filters.Document.ALL),
        handle_media
    ))
    
    # Обработчик текстовых сообщений от клиентов
    application.add_handler(MessageHandler(
        ~filters.Chat(ADMIN_ID) & filters.TEXT & ~filters.COMMAND,
        handle_client_message
    ))
    
    logger.info("🚀 Бот-консультант запущен!")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    # Запускаем health check сервер
    health_thread = threading.Thread(target=run_health_server, daemon=True)
    health_thread.start()
    
    # Запускаем бота
    main()
