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

# --- НАСТРОЙКИ ---
BOT_TOKEN = os.environ.get('TELEGRAM_TOKEN')
ADMIN_ID = 1121954610  # ТВОЙ ID
GROUP_ID = -1003701333730  # ID ТВОЕЙ ГРУППЫ С ТЕМАМИ
LOG_FILE = "logs.txt"

# --- НАСТРОЙКИ GOOGLE SHEETS ---
SPREADSHEET_ID = "15vlEZ0Q6OmQh51DsA9B_fgiLwed12ekroz1aeWsgXVI"
WORKSHEET_NAME = "Логи клиентов"

# Хранилища
user_topics = {}
user_stage = {}

# --- Логирование ---
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

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

# === ИНИЦИАЛИЗАЦИЯ GOOGLE SHEETS С ПОДРОБНЫМ ЛОГИРОВАНИЕМ ===
def init_google_sheets():
    """Подключается к Google Sheets и возвращает объект листа"""
    logger.info("🔄 Начинаем подключение к Google Sheets...")
    
    try:
        # Проверяем наличие переменной окружения
        creds_json = os.environ.get('GOOGLE_CREDS_JSON')
        if not creds_json:
            logger.error("❌ GOOGLE_CREDS_JSON не найден в переменных окружения")
            logger.error("👉 Добавь переменную GOOGLE_CREDS_JSON на Render с полным JSON-ключом")
            return None
        
        logger.info("✅ GOOGLE_CREDS_JSON найден, пробуем распарсить JSON...")
        
        # Пробуем распарсить JSON
        try:
            creds_dict = json.loads(creds_json)
            logger.info(f"✅ JSON распаршен успешно. client_email: {creds_dict.get('client_email', 'не найден')}")
        except json.JSONDecodeError as e:
            logger.error(f"❌ Ошибка парсинга JSON: {e}")
            logger.error("👉 Проверь, что GOOGLE_CREDS_JSON содержит валидный JSON")
            return None
        
        # Настраиваем scope
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        logger.info(f"🔄 Scope настроен: {scope}")
        
        # Создаем credentials
        try:
            creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
            logger.info("✅ Credentials созданы успешно")
        except Exception as e:
            logger.error(f"❌ Ошибка создания credentials: {e}")
            return None
        
        # Авторизуемся
        try:
            client = gspread.authorize(creds)
            logger.info("✅ Авторизация в gspread успешна")
        except Exception as e:
            logger.error(f"❌ Ошибка авторизации в gspread: {e}")
            return None
        
        # Открываем таблицу по ID
        logger.info(f"🔄 Пробуем открыть таблицу с ID: {SPREADSHEET_ID}")
        try:
            spreadsheet = client.open_by_key(SPREADSHEET_ID)
            logger.info(f"✅ Таблица открыта успешно. Название: {spreadsheet.title}")
        except gspread.exceptions.SpreadsheetNotFound:
            logger.error(f"❌ Таблица с ID {SPREADSHEET_ID} не найдена")
            logger.error("👉 Проверь правильность SPREADSHEET_ID и дай доступ сервисному аккаунту к таблице")
            return None
        except gspread.exceptions.APIError as e:
            logger.error(f"❌ Ошибка API при открытии таблицы: {e}")
            if hasattr(e, 'response'):
                try:
                    error_json = e.response.json()
                    logger.error(f"Детали ошибки: {error_json}")
                except:
                    pass
            return None
        
        # Работаем с листом
        logger.info(f"🔄 Пробуем получить лист с названием '{WORKSHEET_NAME}'")
        try:
            worksheet = spreadsheet.worksheet(WORKSHEET_NAME)
            logger.info(f"✅ Лист '{WORKSHEET_NAME}' найден")
        except gspread.exceptions.WorksheetNotFound:
            logger.info(f"🔄 Лист '{WORKSHEET_NAME}' не найден, создаем новый...")
            try:
                worksheet = spreadsheet.add_worksheet(title=WORKSHEET_NAME, rows=1000, cols=20)
                headers = ["Timestamp", "User ID", "Username", "First Name", "Message", "Stage", "Topic ID"]
                worksheet.append_row(headers)
                logger.info(f"✅ Создан новый лист с заголовками")
            except Exception as e:
                logger.error(f"❌ Ошибка создания листа: {e}")
                return None
        
        logger.info("🎉 Успешное подключение к Google Sheets!")
        return worksheet
        
    except Exception as e:
        logger.error(f"❌ Неожиданная ошибка в init_google_sheets: {e}", exc_info=True)
        return None

# Инициализируем Google Sheets
logger.info("🔄 Запуск init_google_sheets()...")
worksheet = init_google_sheets()
if worksheet:
    logger.info("✅ Google Sheets инициализирован успешно")
else:
    logger.error("❌ Google Sheets НЕ инициализирован")

# === ЗАПИСЬ В ТАБЛИЦУ ===
def log_to_sheets(user_id, username, first_name, message_text, stage=0, topic_id=None):
    if not worksheet:
        logger.warning(f"⚠️ Пропускаем запись в таблицу для user {user_id}: worksheet не инициализирован")
        return
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        row = [timestamp, str(user_id), username or "нет", first_name or "нет", message_text, str(stage), str(topic_id) if topic_id else ""]
        worksheet.append_row(row)
        logger.info(f"✅ Записано в таблицу: {user_id}")
    except Exception as e:
        logger.error(f"❌ Ошибка записи в таблицу для user {user_id}: {e}")

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
        logger.info(f"✅ Создана тема {topic_id} для пользователя {user_id}")
        return topic_id
    except Exception as e:
        logger.error(f"❌ Ошибка создания темы для {user_id}: {e}")
        return None

# === /START ===
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    logger.info(f"Команда /start от пользователя {user.id} (@{user.username})")
    
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
    
    logger.info(f"Сообщение от клиента {user_id}: {message.text[:50]}...")
    
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
    
    logger.info(f"Медиа от клиента {user.id}")
    
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

# === ПРОВЕРКА SHEETS ===
async def check_sheets(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    logger.info(f"Команда /check_sheets от пользователя {user_id}")
    
    if user_id != ADMIN_ID:
        logger.warning(f"Пользователь {user_id} не админ, доступ запрещен")
        return
    
    if worksheet:
        await update.message.reply_text("✅ Google Sheets подключен и работает!")
        logger.info("✅ Ответ на /check_sheets: подключен")
    else:
        error_msg = "❌ Google Sheets не подключен. Проверь логи на Render для деталей."
        await update.message.reply_text(error_msg)
        logger.error(f"Ответ на /check_sheets: не подключен. worksheet = {worksheet}")

# === ГЛАВНАЯ ===
def main():
    logger.info("🚀 Запуск бота...")
    
    app = Application.builder().token(BOT_TOKEN).build()
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("admin_logs", admin_logs))
    app.add_handler(CommandHandler("check_sheets", check_sheets))
    
    app.add_handler(MessageHandler(filters.Chat(GROUP_ID) & filters.TEXT & ~filters.COMMAND, handle_admin_reply_in_topic))
    app.add_handler(MessageHandler(~filters.Chat(ADMIN_ID) & (filters.PHOTO | filters.VIDEO | filters.Document.ALL | filters.VOICE), handle_media))
    app.add_handler(MessageHandler(~filters.Chat(ADMIN_ID) & filters.TEXT & ~filters.COMMAND, handle_client_message))
    
    logger.info("✅ Обработчики добавлены, запускаем polling...")
    app.run_polling()

if __name__ == "__main__":
    logger.info("🔄 Запуск health check сервера в отдельном потоке")
    health_thread = threading.Thread(target=run_health_server, daemon=True)
    health_thread.start()
    
    logger.info("🔄 Запуск main()")
    main()
