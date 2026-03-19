import os
import json
import logging
import threading
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# --- НАСТРОЙКИ (УЖЕ С ТВОИМИ ДАННЫМИ) ---
BOT_TOKEN = os.environ.get('TELEGRAM_TOKEN')
ADMIN_ID = 1121954610  # ТВОЙ ID
GROUP_ID = -1003701333730  # ID ТВОЕЙ ГРУППЫ С ТЕМАМИ
LOG_FILE = "logs.txt"

# --- НАСТРОЙКИ GOOGLE SHEETS (УЖЕ ВСТАВЛЕНЫ!) ---
# ID твоей таблицы из ссылки: https://docs.google.com/spreadsheets/d/1gXNDETCzIhF-NFJ0iKHfJ6W1fglQ8J1iTCNyRWroXKg/
SPREADSHEET_ID = "1gXNDETCzIhF-NFJ0iKHfJ6W1fglQ8J1iTCNyRWroXKg"
WORKSHEET_NAME = "Логи клиентов"  # Название листа в таблице

# Хранилища
user_topics = {}
user_stage = {}

# --- Логирование ---
logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# === ИНИЦИАЛИЗАЦИЯ GOOGLE SHEETS ===
def init_google_sheets():
    """Подключается к Google Sheets и возвращает объект листа"""
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        
        creds_json = os.environ.get('GOOGLE_CREDS_JSON')
        if not creds_json:
            logger.error("❌ GOOGLE_CREDS_JSON не найден в переменных окружения")
            return None
        
        creds_dict = json.loads(creds_json)
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_key(SPREADSHEET_ID)
        
        try:
            worksheet = spreadsheet.worksheet(WORKSHEET_NAME)
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=WORKSHEET_NAME, rows=1000, cols=20)
            headers = ["Timestamp", "User ID", "Username", "First Name", "Message", "Stage", "Topic ID"]
            worksheet.append_row(headers)
            logger.info("✅ Создан новый лист с заголовками")
        
        logger.info("✅ Успешное подключение к Google Sheets")
        return worksheet
    except Exception as e:
        logger.error(f"❌ Ошибка подключения к Google Sheets: {e}")
        return None

worksheet = init_google_sheets()

# === ЗАПИСЬ В ТАБЛИЦУ ===
def log_to_sheets(user_id, username, first_name, message_text, stage=0, topic_id=None):
    if not worksheet:
        return
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        row = [timestamp, str(user_id), username or "нет", first_name or "нет", message_text, str(stage), str(topic_id) if topic_id else ""]
        worksheet.append_row(row)
        logger.info(f"✅ Записано в таблицу: {user_id}")
    except Exception as e:
        logger.error(f"❌ Ошибка записи: {e}")

# === ВЕБ-СЕРВЕР ===
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

# === СОХРАНЕНИЕ В ФАЙЛ ===
def save_message(user_id, username, first_name, text, is_from_admin=False):
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            sender = "АДМИН" if is_from_admin else "КЛИЕНТ"
            f.write(f"[{now}] {sender} | ID: {user_id} | @{username} | {first_name}: {text}\n")
    except Exception as e:
        logger.error(f"Log error: {e}")

# === СОЗДАНИЕ ТЕМЫ ===
async def get_or_create_topic(context, user_id, username, first_name):
    if user_id in user_topics:
        return user_topics[user_id]
    
    topic_name = f"{first_name} (@{username if username else 'no_username'})"
    try:
        result = await context.bot.create_forum_topic(chat_id=GROUP_ID, name=topic_name[:128])
        topic_id = result.message_thread_id
        user_topics[user_id] = topic_id
        await context.bot.send_message(
            chat_id=GROUP_ID, message_thread_id=topic_id,
            text=f"🆕 **Новый клиент!**\nИмя: {first_name}\nUsername: @{username if username else 'нет'}\nID: `{user_id}`"
        )
        logger.info(f"✅ Создана тема {topic_id}")
        return topic_id
    except Exception as e:
        logger.error(f"❌ Ошибка создания темы: {e}")
        return None

# === /START ===
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    save_message(user.id, user.username or "нет", user.first_name or "нет", "/start")
    
    topic_id = await get_or_create_topic(context, user.id, user.username, user.first_name)
    if not topic_id:
        await update.message.reply_text("❌ Ошибка. Попробуйте позже.")
        return
    
    log_to_sheets(user.id, user.username, user.first_name, "/start", stage=1, topic_id=topic_id)
    user_stage[user.id] = 1
    
    await update.message.reply_text(
        f"Здравствуйте, {user.first_name}! 👋\n\nПодскажите, по какому вопросу хотели бы к нам обратиться?"
    )
    await context.bot.send_message(chat_id=GROUP_ID, message_thread_id=topic_id, text="👤 Клиент начал диалог.")

# === СООБЩЕНИЯ КЛИЕНТОВ ===
async def handle_client_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.message
    user_id = user.id
    
    save_message(user_id, user.username or "нет", user.first_name or "нет", message.text)
    
    topic_id = await get_or_create_topic(context, user_id, user.username, user.first_name)
    if not topic_id:
        await message.reply_text("❌ Ошибка")
        return
    
    stage = user_stage.get(user_id, 0)
    log_to_sheets(user_id, user.username, user.first_name, message.text, stage=stage, topic_id=topic_id)
    
    await context.bot.send_message(chat_id=GROUP_ID, message_thread_id=topic_id, text=f"👤 **Клиент:**\n{message.text}")
    
    if stage == 1:
        await message.reply_text("Отлично, спасибо за ваш ответ! 👍\n\nВ ближайшее время с вами свяжется наша команда.")
        user_stage[user_id] = 2
        await context.bot.send_message(chat_id=GROUP_ID, message_thread_id=topic_id, text="✅ Подтверждение отправлено.")
    elif stage == 2:
        await message.reply_text("Остались ли у вас какие-либо вопросы? 🤔")
        user_stage[user_id] = 3
        await context.bot.send_message(chat_id=GROUP_ID, message_thread_id=topic_id, text="❓ Вопрос отправлен.")
    elif stage == 3:
        await message.reply_text("Если что-то непонятно, обратитесь пожалуйста сюда: @serg.dmitriy 📱\n\nВсегда рады помочь!")
        user_stage[user_id] = 4
        await context.bot.send_message(chat_id=GROUP_ID, message_thread_id=topic_id, text="📱 Диалог завершён.")
    else:
        await message.reply_text("Если у вас появились новые вопросы, просто напишите нам! 🙌")
        await context.bot.send_message(chat_id=GROUP_ID, message_thread_id=topic_id, text=f"📨 Новое сообщение:\n{message.text}")

# === ОТВЕТЫ АДМИНА ===
async def handle_admin_reply_in_topic(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != GROUP_ID or not update.effective_message.message_thread_id:
        return
    
    message = update.effective_message
    topic_id = message.message_thread_id
    client_id = next((uid for uid, tid in user_topics.items() if tid == topic_id), None)
    
    if not client_id:
        await message.reply_text("❌ Не могу найти клиента.")
        return
    
    await context.bot.send_message(chat_id=client_id, text=f"{message.text}")
    save_message(ADMIN_ID, "admin", "Админ", message.text, is_from_admin=True)
    await message.reply_text("✅ Ответ отправлен клиенту!")

# === МЕДИА ===
async def handle_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.message
    
    topic_id = await get_or_create_topic(context, user.id, user.username, user.first_name)
    if not topic_id:
        await message.reply_text("❌ Ошибка")
        return
    
    media_type = "фото"
    if message.video: media_type = "видео"
    elif message.document: media_type = "документ"
    elif message.voice: media_type = "голосовое"
    
    stage = user_stage.get(user.id, 0)
    log_to_sheets(user.id, user.username, user.first_name, f"[{media_type}]", stage=stage, topic_id=topic_id)
    
    await message.forward(chat_id=GROUP_ID, message_thread_id=topic_id)
    await context.bot.send_message(chat_id=GROUP_ID, message_thread_id=topic_id, text=f"📎 {media_type} от клиента")
    save_message(user.id, user.username or "нет", user.first_name or "нет", f"[{media_type}]")
    await message.reply_text("✅ Файл получен!")

# === ЛОГИ ===
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

# === ПРОВЕРКА SHEETS ===
async def check_sheets(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if worksheet:
        await update.message.reply_text("✅ Google Sheets подключен и работает!")
    else:
        await update.message.reply_text("❌ Google Sheets не подключен. Проверь GOOGLE_CREDS_JSON.")

# === ГЛАВНАЯ ===
def main():
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("admin_logs", admin_logs))
    app.add_handler(CommandHandler("check_sheets", check_sheets))
    app.add_handler(MessageHandler(filters.Chat(GROUP_ID) & filters.TEXT & ~filters.COMMAND, handle_admin_reply_in_topic))
    app.add_handler(MessageHandler(~filters.Chat(ADMIN_ID) & (filters.PHOTO | filters.VIDEO | filters.Document.ALL | filters.VOICE), handle_media))
    app.add_handler(MessageHandler(~filters.Chat(ADMIN_ID) & filters.TEXT & ~filters.COMMAND, handle_client_message))
    logger.info("🚀 Бот с Google Sheets запущен")
    app.run_polling()

if __name__ == "__main__":
    health_thread = threading.Thread(target=run_health_server, daemon=True)
    health_thread.start()
    main()
