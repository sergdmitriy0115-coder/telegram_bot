import os
import logging
import threading
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

# --- НАСТРОЙКИ ---
BOT_TOKEN = os.environ.get('TELEGRAM_TOKEN')
ADMIN_ID = 7675037573  # ТВОЙ ID
GROUP_ID = -1003743707530  # 🔥 ЗДЕСЬ БУДЕТ ID ТВОЕЙ ГРУППЫ (ОТРИЦАТЕЛЬНОЕ ЧИСЛО)
LOG_FILE = "logs.txt"

# Хранилище: {user_id: topic_id}
user_topics = {}
# Хранилище этапов диалога
user_stage = {}

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
    def log_message(self, format, *args): pass

def run_health_server():
    try:
        server = HTTPServer(('0.0.0.0', 10000), HealthCheckHandler)
        logger.info("✅ Health check server started")
        server.serve_forever()
    except Exception as e:
        logger.error(f"Health server error: {e}")

# === СОХРАНЕНИЕ В ЛОГ ===
def save_message(user_id, username, first_name, text, is_from_admin=False):
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            sender = "АДМИН" if is_from_admin else "КЛИЕНТ"
            f.write(f"[{now}] {sender} | ID: {user_id} | @{username} | {first_name}: {text}\n")
    except Exception as e:
        logger.error(f"Log error: {e}")

# === ПОЛУЧИТЬ ИЛИ СОЗДАТЬ ТЕМУ ДЛЯ КЛИЕНТА ===
async def get_or_create_topic(context: ContextTypes.DEFAULT_TYPE, user_id: int, username: str, first_name: str):
    """Возвращает ID темы для клиента, создаёт новую если нужно"""
    global user_topics
    
    if user_id in user_topics:
        return user_topics[user_id]
    
    # Создаём название темы
    topic_name = f"{first_name} (@{username if username else 'no_username'})"
    
    try:
        # Создаём тему в группе
        result = await context.bot.create_forum_topic(
            chat_id=GROUP_ID,
            name=topic_name[:128]  # Ограничение длины
        )
        topic_id = result.message_thread_id
        user_topics[user_id] = topic_id
        
        # Приветственное сообщение в теме для админа
        await context.bot.send_message(
            chat_id=GROUP_ID,
            message_thread_id=topic_id,
            text=f"🆕 **Новый клиент!**\n"
                 f"Имя: {first_name}\n"
                 f"Username: @{username if username else 'нет'}\n"
                 f"ID: `{user_id}`\n\n"
                 f"📝 Все сообщения клиента будут приходить сюда.\n"
                 f"💬 Чтобы ответить, просто напиши в эту тему."
        )
        
        logger.info(f"✅ Создана тема {topic_id} для пользователя {user_id}")
        return topic_id
    except Exception as e:
        logger.error(f"❌ Ошибка создания темы: {e}")
        return None

# === КОМАНДА /START ===
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    save_message(user.id, user.username or "нет", user.first_name or "нет", "/start")
    
    # Создаём или получаем тему
    topic_id = await get_or_create_topic(context, user.id, user.username, user.first_name)
    
    if not topic_id:
        await update.message.reply_text("❌ Техническая ошибка. Попробуйте позже.")
        return
    
    # Устанавливаем этап 1
    user_stage[user.id] = 1
    
    # Отправляем приветствие клиенту
    await update.message.reply_text(
        f"Здравствуйте, {user.first_name}! 👋\n\n"
        f"Подскажите, по какому вопросу хотели бы к нам обратиться?"
    )
    
    # Уведомление в тему для админа
    await context.bot.send_message(
        chat_id=GROUP_ID,
        message_thread_id=topic_id,
        text=f"👤 Клиент начал диалог. Ожидается ответ на вопрос."
    )

# === ОБРАБОТКА СООБЩЕНИЙ ОТ КЛИЕНТОВ ===
async def handle_client_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.message
    user_id = user.id
    
    # Сохраняем в лог
    save_message(user_id, user.username or "нет", user.first_name or "нет", message.text)
    
    # Получаем или создаём тему
    topic_id = await get_or_create_topic(context, user_id, user.username, user.first_name)
    if not topic_id:
        await message.reply_text("❌ Ошибка")
        return
    
    # Пересылаем сообщение клиента в тему
    await context.bot.send_message(
        chat_id=GROUP_ID,
        message_thread_id=topic_id,
        text=f"👤 **Клиент:**\n{message.text}"
    )
    
    # Обработка этапов диалога
    stage = user_stage.get(user_id, 0)
    
    if stage == 1:
        # Клиент ответил на вопрос
        await message.reply_text(
            "Отлично, спасибо за ваш ответ! 👍\n\n"
            "В ближайшее время с вами свяжется наша команда."
        )
        user_stage[user_id] = 2
        
        await context.bot.send_message(
            chat_id=GROUP_ID,
            message_thread_id=topic_id,
            text=f"✅ Клиенту отправлено подтверждение. Ожидание дальнейших сообщений."
        )
        
    elif stage == 2:
        # Клиент пишет после подтверждения
        await message.reply_text(
            "Остались ли у вас какие-либо вопросы? 🤔"
        )
        user_stage[user_id] = 3
        
        await context.bot.send_message(
            chat_id=GROUP_ID,
            message_thread_id=topic_id,
            text=f"❓ Клиенту отправлен вопрос об остальных вопросах."
        )
        
    elif stage == 3:
        # Клиент ответил на вопрос
        await message.reply_text(
            "Если что-то непонятно, обратитесь пожалуйста сюда: @serg.dmitriy 📱\n\n"
            "Всегда рады помочь!"
        )
        user_stage[user_id] = 4
        
        await context.bot.send_message(
            chat_id=GROUP_ID,
            message_thread_id=topic_id,
            text=f"📱 Клиенту отправлен контакт для связи. Диалог завершён."
        )
        
    else:
        # После завершения диалога
        await message.reply_text(
            "Если у вас появились новые вопросы, просто напишите нам! Мы обязательно поможем. 🙌"
        )
        
        await context.bot.send_message(
            chat_id=GROUP_ID,
            message_thread_id=topic_id,
            text=f"📨 Новое сообщение после диалога:\n{message.text}"
        )

# === ОБРАБОТКА ОТВЕТОВ ИЗ ТЕМЫ (АДМИН ПИШЕТ В ТЕМЕ) ===
async def handle_admin_reply_in_topic(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Админ отвечает в теме → отправляем клиенту"""
    
    # Проверяем, что сообщение из нашей группы
    if update.effective_chat.id != GROUP_ID:
        return
    
    # Проверяем, что это сообщение в теме (не общий чат)
    if not update.effective_message.message_thread_id:
        return
    
    message = update.effective_message
    topic_id = message.message_thread_id
    
    # Ищем клиента по ID темы
    client_id = None
    for uid, tid in user_topics.items():
        if tid == topic_id:
            client_id = uid
            break
    
    if not client_id:
        await message.reply_text("❌ Не могу найти клиента для этой темы.")
        return
    
    # Отправляем ответ клиенту (от имени бота)
    await context.bot.send_message(
        chat_id=client_id,
        text=f"{message.text}"
    )
    
    # Сохраняем в лог
    save_message(
        user_id=ADMIN_ID,
        username="admin",
        first_name="Админ",
        text=message.text,
        is_from_admin=True
    )
    
    # Подтверждение в тему
    await message.reply_text("✅ Ответ отправлен клиенту!")
    logger.info(f"Админ ответил клиенту {client_id} через тему {topic_id}")

# === ОБРАБОТКА МЕДИА ===
async def handle_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.message
    
    # Получаем тему
    topic_id = await get_or_create_topic(context, user.id, user.username, user.first_name)
    if not topic_id:
        await message.reply_text("❌ Ошибка")
        return
    
    # Определяем тип медиа
    media_type = "фото"
    if message.video:
        media_type = "видео"
    elif message.document:
        media_type = "документ"
    elif message.voice:
        media_type = "голосовое"
    
    # Пересылаем медиа в тему
    await message.forward(chat_id=GROUP_ID, message_thread_id=topic_id)
    
    # Добавляем информацию
    await context.bot.send_message(
        chat_id=GROUP_ID,
        message_thread_id=topic_id,
        text=f"📎 {media_type} от клиента"
    )
    
    # Сохраняем в лог
    save_message(user.id, user.username or "нет", user.first_name or "нет", f"[{media_type}]")
    
    # Отвечаем клиенту
    await message.reply_text("✅ Файл получен!")

# === КОМАНДА ДЛЯ ЛОГОВ ===
async def admin_logs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    
    try:
        if os.path.exists(LOG_FILE):
            with open(LOG_FILE, "rb") as f:
                await update.message.reply_document(f)
        else:
            await update.message.reply_text("Логов нет")
    except Exception as e:
        await update.message.reply_text(f"Ошибка: {e}")

# === ГЛАВНАЯ ===
def main():
    app = Application.builder().token(BOT_TOKEN).build()
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("admin_logs", admin_logs))
    
    # Обработка ответов админа из темы
    app.add_handler(MessageHandler(
        filters.Chat(GROUP_ID) & filters.TEXT & ~filters.COMMAND,
        handle_admin_reply_in_topic
    ))
    
    # Медиа от клиентов
    app.add_handler(MessageHandler(
        ~filters.Chat(ADMIN_ID) & (filters.PHOTO | filters.VIDEO | filters.Document.ALL | filters.VOICE),
        handle_media
    ))
    
    # Текст от клиентов
    app.add_handler(MessageHandler(
        ~filters.Chat(ADMIN_ID) & filters.TEXT & ~filters.COMMAND,
        handle_client_message
    ))
    
    logger.info("🚀 Бот с темами запущен")
    app.run_polling()

if __name__ == "__main__":
    health_thread = threading.Thread(target=run_health_server, daemon=True)
    health_thread.start()
    main()
