import os
import json
import logging
import threading
import re
import asyncio
import traceback
import random
import qrcode
from io import BytesIO
from datetime import datetime, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, CallbackQueryHandler
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# --- НАСТРОЙКИ ---
BOT_TOKEN = os.environ.get('TELEGRAM_TOKEN')
ADMIN_ID = 7675037573
GROUP_ID = -1003743707530
LOG_FILE = "logs.txt"

# --- GOOGLE SHEETS ---
SPREADSHEET_ID = "15vlEZ0Q6OmQh51DsA9B_fgiLwed12ekroz1aeWsgXVI"
WORKSHEET_NAME = "Логи клиентов"

# --- СТАТУСЫ ---
CLIENT_STATUSES = [
    "Новый",
    "Уточняем нишу",
    "Уточняем оборот",
    "Уточняем проблему",
    "Уточняем источник",
    "Готов к передаче",
    "Передан руководителю",
    "Нецелевой",
    "Отказ"
]

# --- КНОПКИ ДЛЯ ВОПРОСОВ ---
BUTTONS = {
    "niche": [
        {"text": "📚 Онлайн-курс", "value": "Онлайн-курс"},
        {"text": "📘 Инфопродукт", "value": "Инфопродукт"},
        {"text": "🎓 Наставничество", "value": "Наставничество"},
        {"text": "✍️ Другое (напишу сам)", "value": "other"}
    ],
    "turnover": [
        {"text": "💰 До 500 тыс", "value": "До 500 тыс ₽"},
        {"text": "💰 500 тыс – 1 млн", "value": "500 тыс – 1 млн ₽"},
        {"text": "💰 1 млн – 3 млн", "value": "1 млн – 3 млн ₽"},
        {"text": "💰 Более 3 млн", "value": "Более 3 млн ₽"},
        {"text": "✍️ Напишу сам", "value": "other"}
    ],
    "problem": [
        {"text": "📉 Мало лидов", "value": "Мало лидов"},
        {"text": "🔄 Низкая конверсия", "value": "Низкая конверсия"},
        {"text": "💸 Дорогое привлечение", "value": "Дорогое привлечение"},
        {"text": "👥 Сложно нанять менеджеров", "value": "Сложно нанять менеджеров"},
        {"text": "✍️ Другое (напишу сам)", "value": "other"}
    ],
    "source": [
        {"text": "📸 Instagram", "value": "Instagram"},
        {"text": "💬 Telegram", "value": "Telegram"},
        {"text": "🎥 YouTube", "value": "YouTube"},
        {"text": "🗣️ Сарафан", "value": "Сарафан"},
        {"text": "📢 Реклама", "value": "Реклама"},
        {"text": "✍️ Другое (напишу сам)", "value": "other"},
        {"text": "✅ ГОТОВО", "value": "done"}
    ]
}

# Для хранения временных ответов на множественный выбор
user_temp_sources = {}

# Уникальные коды клиентов
client_codes = {}

def generate_client_code(user_id):
    if str(user_id) not in client_codes:
        year = datetime.now().year
        num = len(client_codes) + 1
        client_codes[str(user_id)] = f"ADD-{year}-{num:04d}"
    return client_codes[str(user_id)]

# GIF для приветствия
GREETING_GIFS = [
    "https://media.giphy.com/media/3o7abB06u9bNzA8LC8/giphy.gif",
    "https://media.giphy.com/media/JIX9t2j0ZTN9S/giphy.gif",
]

COMPLETE_EFFECTS = ["🎉", "✨", "🎊", "🌟", "💫", "⭐", "🎈", "🎆", "🎇", "🔥"]

def get_random_complete_effect():
    return " ".join(random.sample(COMPLETE_EFFECTS, 5))

STATUS_EMOJI = {
    "Новый": "🟢",
    "Уточняем нишу": "🟡",
    "Уточняем оборот": "🟡",
    "Уточняем проблему": "🟡",
    "Уточняем источник": "🟡",
    "Готов к передаче": "🟢",
    "Передан руководителю": "🔵",
    "Нецелевой": "⚪",
    "Отказ": "🔴"
}

MANAGER_CONTACTS = ["@Darya_Pril06", "@anny_nizh"]

def get_greeting():
    hour = datetime.now().hour
    if 5 <= hour < 12:
        return "Доброе утро"
    elif 12 <= hour < 18:
        return "Добрый день"
    elif 18 <= hour < 23:
        return "Добрый вечер"
    else:
        return "Доброй ночи"

# Вопросы
QUESTIONS = {
    1: "Расскажите, над каким проектом работаете? Какая у вас ниша?",
    2: "Понял! А какой сейчас примерный ежемесячный оборот?",
    3: "Спасибо. А в чём сейчас основная сложность с продажами?",
    4: "Благодарю! И последний вопрос — откуда узнали о нас? (можно выбрать несколько вариантов)"
}

# Хранилища
user_topics = {}
blacklist = set()
user_stage = {}
user_answers = {}
user_timers = {}
admin_actions_log = []

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
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

# === ОТПРАВКА ОШИБОК ===
async def send_error_notification(context, error_title, error_details, user_info=None):
    try:
        error_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        message = f"🚨 ОШИБКА В БОТЕ\n\n⏰ Время: {error_time}\n📌 Тип: {error_title}\n\n📋 Детали:\n{error_details[:1500]}\n"
        if user_info:
            message += f"\n👤 Пользователь: {user_info}"
        await context.bot.send_message(chat_id=ADMIN_ID, text=message)
        logger.info(f"✅ Уведомление отправлено")
    except Exception as e:
        logger.error(f"❌ Ошибка отправки уведомления: {e}")

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        error_details = traceback.format_exc()
        user_info = None
        if update and update.effective_user:
            user = update.effective_user
            user_info = f"{user.first_name} (@{user.username}) ID: {user.id}"
        logger.error(f"❌ Ошибка: {error_details}")
        await send_error_notification(context, "Исключение в обработчике", error_details, user_info)
    except Exception as e:
        logger.critical(f"❌ Критическая ошибка: {e}")

def catch_errors(func):
    async def wrapper(update, context, *args, **kwargs):
        try:
            return await func(update, context, *args, **kwargs)
        except Exception as e:
            error_details = traceback.format_exc()
            user_info = None
            if update and update.effective_user:
                user = update.effective_user
                user_info = f"{user.first_name} (@{user.username}) ID: {user.id}"
            logger.error(f"❌ Ошибка в {func.__name__}: {error_details}")
            await send_error_notification(context, f"Ошибка в {func.__name__}", error_details, user_info)
            try:
                await update.message.reply_text("❌ Произошла внутренняя ошибка. Мы уже работаем над её исправлением.")
            except:
                pass
    return wrapper

# === QR-КОД ===
async def generate_qr_code(data):
    qr = qrcode.QRCode(version=1, box_size=10, border=5)
    qr.add_data(data)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    bio = BytesIO()
    bio.name = 'qr.png'
    img.save(bio, 'PNG')
    bio.seek(0)
    return bio

# === GOOGLE SHEETS ФУНКЦИИ ===
def format_worksheet(worksheet):
    try:
        sheet_id = worksheet.id
        requests = [
            {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": worksheet.row_count},
                            "cell": {"userEnteredFormat": {"textFormat": {"fontFamily": "Arial", "fontSize": 11}}},
                            "fields": "userEnteredFormat.textFormat"}},
            {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1},
                            "cell": {"userEnteredFormat": {"textFormat": {"bold": True},
                                                           "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9}}},
                            "fields": "userEnteredFormat.textFormat,userEnteredFormat.backgroundColor"}},
            {"autoResizeDimensions": {"dimensions": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 8}}},
            {"updateSheetProperties": {"properties": {"sheetId": sheet_id, "gridProperties": {"frozenRowCount": 1}},
                                       "fields": "gridProperties.frozenRowCount"}}
        ]
        status_colors = [
            {"red": 0.7, "green": 1.0, "blue": 0.7}, {"red": 1.0, "green": 1.0, "blue": 0.7},
            {"red": 1.0, "green": 0.9, "blue": 0.6}, {"red": 1.0, "green": 0.8, "blue": 0.5},
            {"red": 1.0, "green": 0.7, "blue": 0.4}, {"red": 0.5, "green": 1.0, "blue": 0.5},
            {"red": 0.7, "green": 0.8, "blue": 1.0}, {"red": 0.6, "green": 0.6, "blue": 0.6},
            {"red": 1.0, "green": 0.6, "blue": 0.6}
        ]
        for i, status in enumerate(CLIENT_STATUSES):
            requests.append({"addConditionalFormatRule": {"rule": {"ranges": [{"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": 5, "endColumnIndex": 6}],
                            "booleanRule": {"condition": {"type": "TEXT_EQ", "values": [{"userEnteredValue": status}]},
                                            "format": {"backgroundColor": status_colors[i], "textFormat": {"bold": True}}}}}})
        worksheet.spreadsheet.batch_update({"requests": requests})
        logger.info("✅ Таблица отформатирована")
    except Exception as e:
        logger.error(f"❌ Ошибка форматирования: {e}")

def is_first_message(user_id):
    if not worksheet: return True
    try:
        user_ids = worksheet.col_values(2)[1:]
        return str(user_id) not in user_ids
    except: return True

def update_client_status(user_id, new_status):
    if not worksheet: return False
    try:
        all_data = worksheet.get_all_values()
        if len(all_data) < 2: return False
        last_row_index = None
        for i in range(len(all_data)-1, 0, -1):
            if len(all_data[i]) > 1 and all_data[i][1] == str(user_id):
                last_row_index = i + 1
                break
        if last_row_index:
            worksheet.update_cell(last_row_index, 6, new_status)
            logger.info(f"✅ Статус {user_id} → {new_status}")
            return True
        return False
    except Exception as e:
        logger.error(f"❌ Ошибка обновления статуса: {e}")
        return False

def add_note_to_client(user_id, note):
    if not worksheet: return False
    try:
        all_data = worksheet.get_all_values()
        if len(all_data) < 2: return False
        last_row_index = None
        for i in range(len(all_data)-1, 0, -1):
            if len(all_data[i]) > 1 and all_data[i][1] == str(user_id):
                last_row_index = i + 1
                break
        if last_row_index:
            current_note = ""
            if len(all_data[last_row_index-1]) >= 7:
                current_note = all_data[last_row_index-1][6]
            timestamp = datetime.now().strftime("%d.%m %H:%M")
            new_note = f"{current_note}\n[{timestamp}] {note}".strip()
            worksheet.update_cell(last_row_index, 7, new_note)
            logger.info(f"✅ Заметка добавлена для {user_id}")
            return True
        return False
    except Exception as e:
        logger.error(f"❌ Ошибка добавления заметки: {e}")
        return False

def get_client_note(user_id):
    if not worksheet: return ""
    try:
        all_data = worksheet.get_all_values()
        for i in range(len(all_data)-1, 0, -1):
            if len(all_data[i]) > 1 and all_data[i][1] == str(user_id):
                if len(all_data[i]) >= 7:
                    return all_data[i][6]
                return ""
        return ""
    except: return ""

def get_client_info(user_id):
    if not worksheet: return None
    try:
        all_data = worksheet.get_all_values()
        for i in range(len(all_data)-1, 0, -1):
            if len(all_data[i]) > 1 and all_data[i][1] == str(user_id):
                return {
                    "timestamp": all_data[i][0] if len(all_data[i]) > 0 else "",
                    "user_id": all_data[i][1] if len(all_data[i]) > 1 else "",
                    "username": all_data[i][2] if len(all_data[i]) > 2 else "",
                    "name": all_data[i][3] if len(all_data[i]) > 3 else "",
                    "message": all_data[i][4] if len(all_data[i]) > 4 else "",
                    "status": all_data[i][5] if len(all_data[i]) > 5 else "",
                    "note": all_data[i][6] if len(all_data[i]) > 6 else "",
                    "source": all_data[i][7] if len(all_data[i]) > 7 else ""
                }
        return None
    except Exception as e:
        logger.error(f"❌ Ошибка получения информации: {e}")
        return None

def update_client_source(user_id, source):
    if not worksheet: return False
    try:
        all_data = worksheet.get_all_values()
        if len(all_data) < 2: return False
        last_row_index = None
        for i in range(len(all_data)-1, 0, -1):
            if len(all_data[i]) > 1 and all_data[i][1] == str(user_id):
                last_row_index = i + 1
                break
        if last_row_index:
            worksheet.update_cell(last_row_index, 8, source)
            logger.info(f"✅ Источник {user_id} → {source}")
            return True
        return False
    except Exception as e:
        logger.error(f"❌ Ошибка обновления источника: {e}")
        return False

def log_to_sheets(user_id, username, first_name, message_text, status="Новый", source=""):
    if not worksheet: return
    try:
        if is_first_message(user_id):
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            row = [timestamp, str(user_id), f"@{username}" if username else "нет", first_name or "нет", message_text, status, "", source]
            worksheet.append_row(row)
            logger.info(f"✅ Новая запись для {user_id}")
            try:
                props = worksheet.spreadsheet.fetch_sheet_metadata(fields="sheets.properties.gridProperties.frozenRowCount")
                frozen = props['sheets'][0]['properties']['gridProperties'].get('frozenRowCount', 0)
                if frozen == 0:
                    format_worksheet(worksheet)
            except: pass
        else:
            update_client_status(user_id, status)
            if source:
                update_client_source(user_id, source)
    except Exception as e:
        logger.error(f"❌ Ошибка записи: {e}")

def get_all_users_from_sheets():
    if not worksheet: return []
    try:
        all_data = worksheet.get_all_values()
        if len(all_data) < 2: return []
        users = set()
        for row in all_data[1:]:
            if len(row) > 1 and row[1]:
                try:
                    users.add(int(row[1]))
                except: continue
        return list(users)
    except: return []

def init_google_sheets():
    logger.info("🔄 Подключение к Google Sheets...")
    try:
        creds_json = os.environ.get('GOOGLE_CREDS_JSON')
        if not creds_json:
            logger.error("❌ GOOGLE_CREDS_JSON не найден")
            return None
        creds_dict = json.loads(creds_json)
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_key(SPREADSHEET_ID)
        try:
            worksheet = spreadsheet.worksheet(WORKSHEET_NAME)
            logger.info("✅ Лист найден")
            headers = worksheet.row_values(1)
            new_headers = ["Timestamp", "User ID", "Ник клиента", "Имя", "Сообщение", "Статус", "Заметки", "Источник"]
            if headers != new_headers:
                logger.info("🔄 Обновляем заголовки")
                worksheet.clear()
                worksheet.append_row(new_headers)
                format_worksheet(worksheet)
        except gspread.exceptions.WorksheetNotFound:
            logger.info("🔄 Создаём новый лист")
            worksheet = spreadsheet.add_worksheet(title=WORKSHEET_NAME, rows=1000, cols=8)
            worksheet.append_row(["Timestamp", "User ID", "Ник клиента", "Имя", "Сообщение", "Статус", "Заметки", "Источник"])
            format_worksheet(worksheet)
        logger.info("🎉 Google Sheets подключен")
        return worksheet
    except Exception as e:
        logger.error(f"❌ Ошибка: {e}")
        return None

worksheet = init_google_sheets()

def save_message(user_id, username, first_name, text, is_from_admin=False):
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            sender = "АДМИН" if is_from_admin else "КЛИЕНТ"
            f.write(f"[{now}] {sender} | ID: {user_id} | @{username} | {first_name}: {text}\n")
    except Exception as e:
        logger.error(f"Log error: {e}")

def log_admin_action(admin_id, action, target_id=None, details=""):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] Админ {admin_id}: {action}"
    if target_id:
        log_entry += f" (клиент {target_id})"
    if details:
        log_entry += f" - {details}"
    admin_actions_log.append(log_entry)
    if len(admin_actions_log) > 100:
        admin_actions_log.pop(0)
    logger.info(log_entry)

# === КНОПКИ ДЛЯ ВОПРОСОВ ===
def get_buttons_for_question(question_type):
    keyboard = []
    for btn in BUTTONS[question_type]:
        keyboard.append([InlineKeyboardButton(btn["text"], callback_data=f"{question_type}_{btn['value']}")])
    return InlineKeyboardMarkup(keyboard)

def get_source_buttons_with_selected(selected):
    """Генерирует кнопки для множественного выбора с отметкой выбранных"""
    keyboard = []
    for btn in BUTTONS["source"]:
        if btn["value"] == "done":
            # Кнопка "ГОТОВО"
            keyboard.append([InlineKeyboardButton("✅ ГОТОВО", callback_data="source_done")])
        else:
            # Добавляем галочку, если вариант выбран
            text = btn["text"]
            if btn["value"] in selected:
                text = "✅ " + text
            keyboard.append([InlineKeyboardButton(text, callback_data=f"source_select_{btn['value']}")])
    return InlineKeyboardMarkup(keyboard)

# === ОТЛОЖЕННЫЕ ОТВЕТЫ ===
async def reminder_callback(context: ContextTypes.DEFAULT_TYPE, user_id: int, attempt: int = 1):
    if user_id in blacklist:
        return
    stage = user_stage.get(user_id, 0)
    if stage == 0 or stage > 4:
        return
    if attempt == 1:
        await context.bot.send_message(
            chat_id=user_id,
            text="⏰ Я вижу, вы задумались. Если хотите, можете просто написать 'да' или 'нет', я пойму. Или сразу свяжитесь с руководителем."
        )
        context.job_queue.run_once(
            lambda ctx: reminder_callback(ctx, user_id, 2),
            600,
            name=f"reminder_{user_id}_2"
        )
    elif attempt == 2:
        await context.bot.send_message(
            chat_id=user_id,
            text=f"⏰ Если вам удобнее, можете сразу связаться с руководителем: @Darya_Pril06 или @anny_nizh.\n\nНапишите 'да', если хотите продолжить."
        )

async def schedule_reminder(context: ContextTypes.DEFAULT_TYPE, user_id: int):
    for job in context.job_queue.jobs():
        if job.name and job.name.startswith(f"reminder_{user_id}"):
            job.schedule_removal()
    context.job_queue.run_once(
        lambda ctx: reminder_callback(ctx, user_id, 1),
        300,
        name=f"reminder_{user_id}_1"
    )

# === СОЗДАНИЕ ТЕМЫ ===
async def get_or_create_topic(context, user_id, username, first_name):
    if user_id in user_topics:
        return user_topics[user_id]
    
    client_code = generate_client_code(user_id)
    topic_name = f"{client_code} | {first_name}"
    
    try:
        result = await context.bot.create_forum_topic(chat_id=GROUP_ID, name=topic_name[:128])
        topic_id = result.message_thread_id
        user_topics[user_id] = topic_id
        
        current_status = get_client_info(user_id).get("status", "Новый") if get_client_info(user_id) else "Новый"
        current_note = get_client_note(user_id)
        
        welcome_text = f"👤 **Новый клиент**\n"
        welcome_text += f"🔑 Код: {client_code}\n"
        welcome_text += f"Имя: {first_name}\n"
        welcome_text += f"Username: @{username if username else 'нет'}\n"
        welcome_text += f"Статус: {current_status}\n"
        
        if user_id in blacklist:
            welcome_text += f"\n🚫 Клиент в ЧЕРНОМ СПИСКЕ!\n"
        if current_note:
            welcome_text += f"\n📝 Заметки: {current_note}\n"
        
        await context.bot.send_message(
            chat_id=GROUP_ID,
            message_thread_id=topic_id,
            text=welcome_text
        )
        return topic_id
    except Exception as e:
        logger.error(f"❌ Ошибка создания темы: {e}")
        return None

# === УПРОЩЁННОЕ СООБЩЕНИЕ В ТЕМУ ===
async def send_simple_message_to_topic(context, topic_id, client_text, bot_response, status):
    status_emoji = STATUS_EMOJI.get(status, "🟡")
    
    # Находим код клиента по topic_id
    client_code = "?"
    for uid, tid in user_topics.items():
        if tid == topic_id:
            client_code = generate_client_code(uid)
            break
    
    message = f"👤 Клиент (код {client_code}):\n{client_text}\n\n"
    message += f"🤖 Бот:\n{bot_response}\n\n"
    message += f"{status_emoji} Статус: {status}"
    
    await context.bot.send_message(
        chat_id=GROUP_ID,
        message_thread_id=topic_id,
        text=message
    )

# === ОСНОВНОЙ ОБРАБОТЧИК ===
@catch_errors
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    logger.info(f"🚀 Команда /start от {user.id}")
    
    if user.id in blacklist:
        await update.message.reply_text("⛔ Вы заблокированы в этом боте.")
        return
    
    client_info = get_client_info(user.id)
    if client_info and client_info.get("status") in ["Передан руководителю", "Отказ"]:
        await update.message.reply_text(f"Ваш запрос уже передан руководителю. Если у вас остались вопросы, можете написать напрямую: {', '.join(MANAGER_CONTACTS)}")
        return
    
    save_message(user.id, user.username or "нет", user.first_name or "нет", "/start")
    
    topic_id = await get_or_create_topic(context, user.id, user.username, user.first_name)
    if not topic_id:
        await update.message.reply_text("❌ Ошибка")
        return
    
    user_stage[user.id] = 1
    user_answers[user.id] = {"niche": "", "turnover": "", "problem": "", "source": []}
    user_temp_sources[user.id] = []
    log_to_sheets(user.id, user.username, user.first_name, "/start", status="Новый")
    
    greeting_gif = random.choice(GREETING_GIFS)
    await context.bot.send_animation(chat_id=user.id, animation=greeting_gif)
    
    greeting = get_greeting()
    welcome_msg = f"{greeting}, {user.first_name}! 👋\n\nЯ виртуальный помощник ADD production. Мы помогаем выстраивать отделы продаж для онлайн-курсов.\n\nДавайте познакомимся, чтобы я мог передать ваш запрос руководителю."
    
    await update.message.reply_text(welcome_msg)
    
    client_code = generate_client_code(user.id)
    await context.bot.send_message(chat_id=GROUP_ID, message_thread_id=topic_id, text=f"👤 **Новый клиент!**\n🔑 Код: {client_code}\n📝 Начинаем диалог...")
    
    await update.message.reply_text(
        QUESTIONS[1],
        reply_markup=get_buttons_for_question("niche")
    )
    log_to_sheets(user.id, user.username, user.first_name, "Вопрос: ниша", status="Уточняем нишу")
    await schedule_reminder(context, user.id)

# === ОБРАБОТКА КНОПОК ===
@catch_errors
async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    user = query.from_user
    user_id = user.id
    data = query.data
    
    for job in context.job_queue.jobs():
        if job.name and job.name.startswith(f"reminder_{user_id}"):
            job.schedule_removal()
    
    # Обработка множественного выбора источников
    if data.startswith("source_select_"):
        value = data.replace("source_select_", "")
        selected = user_temp_sources.get(user_id, [])
        
        if value in selected:
            selected.remove(value)
        else:
            selected.append(value)
        
        user_temp_sources[user_id] = selected
        
        # Обновляем сообщение с кнопками
        await query.edit_message_text(
            text=QUESTIONS[4],
            reply_markup=get_source_buttons_with_selected(selected)
        )
        return
    
    if data == "source_done":
        selected = user_temp_sources.get(user_id, [])
        if not selected:
            await query.edit_message_text("❌ Выберите хотя бы один вариант или нажмите 'Другое'")
            return
        
        answer = ", ".join(selected)
        
        answers = user_answers.get(user_id, {"niche": "", "turnover": "", "problem": "", "source": []})
        answers["source"] = answer
        user_answers[user_id] = answers
        
        is_vip = False
        if any(x in answers.get("turnover", "") for x in ["Более 3 млн", "3 млн", "3 000 000"]):
            is_vip = True
        
        summary = f"📊 **ВЫЖИМКА**\n\n"
        summary += f"🔹 **Ниша:** {answers['niche']}\n"
        summary += f"🔹 **Оборот:** {answers['turnover']}\n"
        summary += f"🔹 **Проблема:** {answers['problem']}\n"
        summary += f"🔹 **Источник:** {answer}"
        
        if is_vip:
            summary += f"\n\n🔥 **VIP-КЛИЕНТ!**"
        
        add_note_to_client(user_id, summary)
        log_to_sheets(user_id, user.username or "нет", user.first_name or "нет", f"Источник: {answer}", status="Передан руководителю", source=answer)
        
        farewell = f"{get_random_complete_effect()}\n\n"
        farewell += f"🎉 **СПАСИБО, {user.first_name}!** 🎉\n\n"
        farewell += f"Мы уже в курсе вашего запроса и скоро свяжемся.\n\n"
        farewell += f"{get_random_complete_effect()}"
        
        await query.edit_message_text(farewell)
        
        qr_bio = await generate_qr_code(f"ADD production\nКонтакты: {', '.join(MANAGER_CONTACTS)}")
        await context.bot.send_photo(chat_id=user_id, photo=qr_bio, caption="📱 Отсканируйте QR-код, чтобы сохранить контакты руководителя")
        
        user_stage[user_id] = 5
        update_client_status(user_id, "Передан руководителю")
        
        topic_id = user_topics.get(user_id)
        if topic_id:
            await send_simple_message_to_topic(
                context, topic_id,
                f"Ответы на все вопросы", 
                f"Ниша: {answers['niche']}\nОборот: {answers['turnover']}\nПроблема: {answers['problem']}\nИсточник: {answer}",
                "Передан руководителю"
            )
        
        if user_id in user_temp_sources:
            del user_temp_sources[user_id]
        return
    
    # Обработка обычных кнопок
    parts = data.split("_", 1)
    if len(parts) < 2:
        await query.edit_message_text("❌ Ошибка")
        return
    
    question_type = parts[0]
    answer = parts[1]
    
    stage = user_stage.get(user_id, 1)
    answers = user_answers.get(user_id, {"niche": "", "turnover": "", "problem": "", "source": []})
    
    if answer == "other":
        await query.edit_message_text(f"✍️ Напишите свой вариант ответа в чат.")
        return
    
    if question_type == "niche":
        answers["niche"] = answer
        user_answers[user_id] = answers
        await query.edit_message_text(f"✅ Принято: {answer}\n\n{QUESTIONS[2]}")
        await context.bot.send_message(
            chat_id=user_id,
            text=QUESTIONS[2],
            reply_markup=get_buttons_for_question("turnover")
        )
        user_stage[user_id] = 2
        update_client_status(user_id, "Уточняем оборот")
        await schedule_reminder(context, user_id)
        
    elif question_type == "turnover":
        answers["turnover"] = answer
        user_answers[user_id] = answers
        await query.edit_message_text(f"✅ Принято: {answer}\n\n{QUESTIONS[3]}")
        await context.bot.send_message(
            chat_id=user_id,
            text=QUESTIONS[3],
            reply_markup=get_buttons_for_question("problem")
        )
        user_stage[user_id] = 3
        update_client_status(user_id, "Уточняем проблему")
        await schedule_reminder(context, user_id)
        
    elif question_type == "problem":
        answers["problem"] = answer
        user_answers[user_id] = answers
        user_temp_sources[user_id] = []
        await query.edit_message_text(
            text=QUESTIONS[4],
            reply_markup=get_source_buttons_with_selected([])
        )
        user_stage[user_id] = 4
        update_client_status(user_id, "Уточняем источник")
        await schedule_reminder(context, user_id)

# === ОБРАБОТКА ТЕКСТОВЫХ СООБЩЕНИЙ (СВОБОДНЫЙ ВВОД) ===
@catch_errors
async def handle_client_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.message
    user_id = user.id
    
    logger.info(f"🔥 ПОЛУЧЕНО СООБЩЕНИЕ от {user_id}: {message.text}")
    
    if user_id in blacklist:
        await message.reply_text("⛔ Вы заблокированы в этом боте.")
        return
    
    save_message(user_id, user.username or "нет", user.first_name or "нет", message.text)
    
    topic_id = await get_or_create_topic(context, user_id, user.username, user.first_name)
    if not topic_id:
        await message.reply_text("❌ Ошибка")
        return
    
    for job in context.job_queue.jobs():
        if job.name and job.name.startswith(f"reminder_{user_id}"):
            job.schedule_removal()
    
    stage = user_stage.get(user_id, 1)
    answers = user_answers.get(user_id, {"niche": "", "turnover": "", "problem": "", "source": []})
    
    client_info = get_client_info(user_id)
    if client_info and client_info.get("status") in ["Передан руководителю", "Отказ"]:
        await message.reply_text(f"Ваш запрос уже передан руководителю. Если у вас остались вопросы, можете написать напрямую: {', '.join(MANAGER_CONTACTS)}")
        return
    
    msg_lower = message.text.lower()
    if any(word in msg_lower for word in ["позвоните", "свяжитесь", "напишите", "человек", "менеджер"]):
        await message.reply_text(f"Я передаю ваш запрос руководителю. Он свяжется с вами в ближайшее время.\n\nКонтакты: @Darya_Pril06 или @anny_nizh.")
        update_client_status(user_id, "Передан руководителю")
        user_stage[user_id] = 5
        return
    
    if stage == 1:
        answers["niche"] = message.text[:200]
        user_answers[user_id] = answers
        await message.reply_text(
            QUESTIONS[2],
            reply_markup=get_buttons_for_question("turnover")
        )
        user_stage[user_id] = 2
        update_client_status(user_id, "Уточняем оборот")
        await schedule_reminder(context, user_id)
        
    elif stage == 2:
        answers["turnover"] = message.text[:100]
        user_answers[user_id] = answers
        
        turnover_text = message.text.lower()
        if any(x in turnover_text for x in ["3 млн", "3 000 000", "3000000", "более 3", "больше 3"]):
            add_note_to_client(user_id, "🔥 VIP-КЛИЕНТ (оборот более 3 млн)")
        
        await message.reply_text(
            QUESTIONS[3],
            reply_markup=get_buttons_for_question("problem")
        )
        user_stage[user_id] = 3
        update_client_status(user_id, "Уточняем проблему")
        await schedule_reminder(context, user_id)
        
    elif stage == 3:
        answers["problem"] = message.text[:300]
        user_answers[user_id] = answers
        user_temp_sources[user_id] = []
        await message.reply_text(
            text=QUESTIONS[4],
            reply_markup=get_source_buttons_with_selected([])
        )
        user_stage[user_id] = 4
        update_client_status(user_id, "Уточняем источник")
        await schedule_reminder(context, user_id)
        
    elif stage == 4:
        # Если клиент пишет текстом вместо кнопок
        answers["source"] = message.text[:100]
        user_answers[user_id] = answers
        
        is_vip = False
        if any(x in answers.get("turnover", "") for x in ["Более 3 млн", "3 млн", "3 000 000"]):
            is_vip = True
        
        summary = f"📊 **ВЫЖИМКА**\n\n"
        summary += f"🔹 **Ниша:** {answers['niche']}\n"
        summary += f"🔹 **Оборот:** {answers['turnover']}\n"
        summary += f"🔹 **Проблема:** {answers['problem']}\n"
        summary += f"🔹 **Источник:** {answers['source']}"
        
        if is_vip:
            summary += f"\n\n🔥 **VIP-КЛИЕНТ!**"
        
        add_note_to_client(user_id, summary)
        log_to_sheets(user_id, user.username or "нет", user.first_name or "нет", f"Источник: {answers['source']}", status="Передан руководителю", source=answers['source'])
        
        farewell = f"{get_random_complete_effect()}\n\n"
        farewell += f"🎉 **СПАСИБО, {user.first_name}!** 🎉\n\n"
        farewell += f"Мы уже в курсе вашего запроса и скоро свяжемся.\n\n"
        farewell += f"{get_random_complete_effect()}"
        
        await message.reply_text(farewell)
        
        qr_bio = await generate_qr_code(f"ADD production\nКонтакты: {', '.join(MANAGER_CONTACTS)}")
        await context.bot.send_photo(chat_id=user_id, photo=qr_bio, caption="📱 Отсканируйте QR-код, чтобы сохранить контакты руководителя")
        
        user_stage[user_id] = 5
        update_client_status(user_id, "Передан руководителю")
        
        await send_simple_message_to_topic(
            context, topic_id,
            f"Ответы на все вопросы", 
            f"Ниша: {answers['niche']}\nОборот: {answers['turnover']}\nПроблема: {answers['problem']}\nИсточник: {answers['source']}",
            "Передан руководителю"
        )
        
    else:
        await message.reply_text(f"Ваш запрос уже передан руководителю. Если у вас остались вопросы, напишите напрямую: {', '.join(MANAGER_CONTACTS)}")

# === АНАЛИТИКА ПО ИСТОЧНИКАМ ===
@catch_errors
async def sources_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    
    if not worksheet:
        await update.message.reply_text("❌ Таблица не подключена")
        return
    
    try:
        all_data = worksheet.get_all_values()
        if len(all_data) < 2:
            await update.message.reply_text("📭 Нет данных")
            return
        
        source_counts = {}
        total = 0
        
        for row in all_data[1:]:
            if len(row) >= 8 and row[7]:
                sources = row[7].split(", ")
                for source in sources:
                    source_counts[source] = source_counts.get(source, 0) + 1
                    total += 1
        
        if total == 0:
            await update.message.reply_text("📭 Нет данных об источниках")
            return
        
        sorted_sources = sorted(source_counts.items(), key=lambda x: x[1], reverse=True)
        
        text = f"📊 **ИСТОЧНИКИ КЛИЕНТОВ**\n\n"
        
        for source, count in sorted_sources:
            percentage = int(count / total * 20)
            bar = "█" * percentage + "░" * (20 - percentage)
            text += f"{source}\n{bar} {count} ({int(count/total*100)}%)\n\n"
        
        text += f"👥 **Всего ответов:** {total}"
        
        await update.message.reply_text(text)
        
    except Exception as e:
        logger.error(f"❌ Ошибка аналитики источников: {e}")
        await update.message.reply_text("❌ Ошибка при получении статистики")

# === ОСТАЛЬНЫЕ КОМАНДЫ ===
@catch_errors
async def admin_logs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    try:
        if os.path.exists(LOG_FILE):
            with open(LOG_FILE, "rb") as f:
                await update.message.reply_document(f)
        else:
            await update.message.reply_text("Логов нет")
    except Exception as e:
        await update.message.reply_text(f"Ошибка: {e}")

@catch_errors
async def check_sheets(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    if worksheet:
        users_count = len(get_all_users_from_sheets())
        await update.message.reply_text(f"✅ Google Sheets подключен\n📊 Всего клиентов: {users_count}")
    else:
        await update.message.reply_text("❌ Google Sheets не подключен")

@catch_errors
async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    await update.message.reply_text("📊 Статусы обновляются автоматически. Ручная смена не требуется.")

@catch_errors
async def handle_admin_reply_in_topic(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != GROUP_ID or not update.effective_message.message_thread_id:
        return
    message = update.effective_message
    topic_id = message.message_thread_id
    client_id = next((uid for uid, tid in user_topics.items() if tid == topic_id), None)
    if not client_id:
        await message.reply_text("❌ Не могу найти клиента")
        return
    await context.bot.send_message(chat_id=client_id, text=f"{message.text}")
    save_message(ADMIN_ID, "admin", "Админ", message.text, is_from_admin=True)
    await message.reply_text("✅ Ответ отправлен")

# === МЕДИА ===
@catch_errors
async def handle_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.message
    
    if user.id in blacklist:
        await message.reply_text("⛔ Вы заблокированы в этом боте.")
        return
    
    topic_id = await get_or_create_topic(context, user.id, user.username, user.first_name)
    if not topic_id:
        await message.reply_text("❌ Ошибка")
        return
    
    if message.photo:
        media_type = "🖼️ Фото"
        await context.bot.send_photo(
            chat_id=GROUP_ID,
            message_thread_id=topic_id,
            photo=message.photo[-1].file_id,
            caption=f"📎 {media_type} от {user.first_name}"
        )
    elif message.video:
        media_type = "🎥 Видео"
        await context.bot.send_video(
            chat_id=GROUP_ID,
            message_thread_id=topic_id,
            video=message.video.file_id,
            caption=f"📎 {media_type} от {user.first_name}"
        )
    elif message.document:
        media_type = "📄 Документ"
        await context.bot.send_document(
            chat_id=GROUP_ID,
            message_thread_id=topic_id,
            document=message.document.file_id,
            caption=f"📎 {media_type} от {user.first_name}"
        )
    elif message.voice:
        media_type = "🎙️ Голосовое"
        await context.bot.send_voice(
            chat_id=GROUP_ID,
            message_thread_id=topic_id,
            voice=message.voice.file_id,
            caption=f"📎 {media_type} от {user.first_name}"
        )
    else:
        return
    
    current_status = get_client_info(user.id).get("status", "Новый") if get_client_info(user.id) else "Новый"
    log_to_sheets(user.id, user.username, user.first_name, f"[{media_type}]", status=current_status)
    await message.reply_text("✅ Файл получен!")

# === СТАТИСТИКА ===
@catch_errors
async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    if not worksheet:
        await update.message.reply_text("❌ Таблица не подключена")
        return
    try:
        all_data = worksheet.get_all_values()
        if len(all_data) < 2:
            await update.message.reply_text("📭 Нет данных")
            return
        total_clients = len(set(row[1] for row in all_data[1:] if len(row) > 1))
        status_counts = {status: 0 for status in CLIENT_STATUSES}
        last_statuses = {}
        for row in reversed(all_data[1:]):
            if len(row) >= 6 and row[1] not in last_statuses:
                last_statuses[row[1]] = row[5]
        for status in last_statuses.values():
            if status in status_counts:
                status_counts[status] += 1
        
        text = f"📊 **СТАТИСТИКА**\n\n"
        text += f"👥 **Всего клиентов:** {total_clients}\n\n"
        text += f"**По статусам:**\n"
        for status in CLIENT_STATUSES:
            count = status_counts.get(status, 0)
            bar_length = int(count / max(1, total_clients) * 20)
            bar = "█" * bar_length + "░" * (20 - bar_length)
            text += f"{STATUS_EMOJI.get(status, '🟢')} {status}: {count} {bar}\n"
        
        await update.message.reply_text(text)
    except Exception as e:
        logger.error(f"❌ Ошибка статистики: {e}")
        await update.message.reply_text("❌ Ошибка при получении статистики")

# === VIP-КЛИЕНТЫ ===
@catch_errors
async def vip_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    if not worksheet:
        await update.message.reply_text("❌ Таблица не подключена")
        return
    try:
        all_data = worksheet.get_all_values()
        if len(all_data) < 2:
            await update.message.reply_text("📭 Нет данных")
            return
        
        vip_clients = []
        for row in all_data[1:]:
            if len(row) >= 7 and "VIP" in row[6]:
                name = row[3] if len(row) > 3 else "?"
                username = row[2] if len(row) > 2 else "?"
                status = row[5] if len(row) > 5 else "?"
                vip_clients.append(f"• {name} (@{username}) — {status}")
        
        if not vip_clients:
            await update.message.reply_text("📭 Нет VIP-клиентов")
            return
        
        text = f"🔥 **VIP-КЛИЕНТЫ ({len(vip_clients)})**\n\n"
        text += "\n".join(vip_clients[:30])
        if len(vip_clients) > 30:
            text += f"\n\n... и ещё {len(vip_clients) - 30}"
        
        await update.message.reply_text(text)
    except Exception as e:
        logger.error(f"❌ Ошибка VIP: {e}")
        await update.message.reply_text("❌ Ошибка при получении списка")

# === ЗАМЕТКИ, ПОИСК, БЛОКИРОВКА ===
@catch_errors
async def note_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    message = update.message
    if message.reply_to_message:
        original_text = message.reply_to_message.text or ""
        match = re.search(r'ID: `?(\d+)`?', original_text)
        if not match:
            await message.reply_text("❌ Не могу найти ID клиента")
            return
        client_id = int(match.group(1))
        note_text = " ".join(context.args) if context.args else "Без текста"
        if add_note_to_client(client_id, note_text):
            await message.reply_text(f"✅ Заметка добавлена к клиенту {client_id}")
            if client_id in user_topics:
                topic_id = user_topics[client_id]
                await context.bot.send_message(chat_id=GROUP_ID, message_thread_id=topic_id, text=f"📝 Новая заметка: {note_text}")
        else:
            await message.reply_text("❌ Ошибка при добавлении заметки")
    elif len(context.args) >= 2:
        try:
            client_id = int(context.args[0])
            note_text = " ".join(context.args[1:])
            if add_note_to_client(client_id, note_text):
                await message.reply_text(f"✅ Заметка добавлена к клиенту {client_id}")
                if client_id in user_topics:
                    topic_id = user_topics[client_id]
                    await context.bot.send_message(chat_id=GROUP_ID, message_thread_id=topic_id, text=f"📝 Новая заметка: {note_text}")
            else:
                await message.reply_text("❌ Ошибка при добавлении заметки")
        except ValueError:
            await message.reply_text("❌ Неверный формат. Используй: /note ID текст")
    else:
        await message.reply_text("📝 Как добавить заметку:\n1. Ответь на сообщение клиента: /note текст\n2. Или напиши: /note ID текст")

@catch_errors
async def search_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    if not context.args:
        await update.message.reply_text("🔍 Используй: /search текст")
        return
    search_text = " ".join(context.args).lower()
    if not worksheet:
        await update.message.reply_text("❌ Таблица не подключена")
        return
    try:
        all_data = worksheet.get_all_values()
        if len(all_data) < 2:
            await update.message.reply_text("📭 Нет данных")
            return
        results = []
        for row in all_data[1:]:
            if len(row) >= 5 and search_text in row[4].lower():
                timestamp = row[0] if len(row) > 0 else "?"
                username = row[2] if len(row) > 2 else "?"
                message = row[4][:100] + "..." if len(row[4]) > 100 else row[4]
                results.append(f"• {timestamp} | {username}: {message}")
        if not results:
            await update.message.reply_text("🔍 Ничего не найдено")
            return
        text = f"🔍 Найдено {len(results)} совпадений:\n\n"
        if len(results) > 20:
            results = results[:20]
            text += "(показаны первые 20)\n\n"
        for i, res in enumerate(results, 1):
            text += f"{i}. {res}\n"
        await update.message.reply_text(text[:4000])
    except Exception as e:
        logger.error(f"❌ Ошибка поиска: {e}")
        await update.message.reply_text("❌ Ошибка при поиске")

@catch_errors
async def ban_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    if not context.args:
        await update.message.reply_text("❌ Используй: /ban ID [причина]")
        return
    try:
        user_id = int(context.args[0])
        reason = " ".join(context.args[1:]) if len(context.args) > 1 else "Без причины"
        blacklist.add(user_id)
        add_note_to_client(user_id, f"🚫 ЗАБЛОКИРОВАН. Причина: {reason}")
        update_client_status(user_id, "Отказ")
        if user_id in user_topics:
            topic_id = user_topics[user_id]
            await context.bot.send_message(chat_id=GROUP_ID, message_thread_id=topic_id, text=f"🚫 Клиент заблокирован\nПричина: {reason}")
        await update.message.reply_text(f"✅ Клиент {user_id} заблокирован")
    except ValueError:
        await update.message.reply_text("❌ Неверный ID")

@catch_errors
async def unban_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    if not context.args:
        await update.message.reply_text("❌ Используй: /unban ID")
        return
    try:
        user_id = int(context.args[0])
        if user_id in blacklist:
            blacklist.remove(user_id)
            add_note_to_client(user_id, "✅ РАЗБЛОКИРОВАН")
            if user_id in user_topics:
                topic_id = user_topics[user_id]
                await context.bot.send_message(chat_id=GROUP_ID, message_thread_id=topic_id, text="✅ Клиент разблокирован")
            await update.message.reply_text(f"✅ Клиент {user_id} разблокирован")
        else:
            await update.message.reply_text(f"ℹ️ Клиент {user_id} не в черном списке")
    except ValueError:
        await update.message.reply_text("❌ Неверный ID")

@catch_errors
async def blacklist_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    if not blacklist:
        await update.message.reply_text("📭 Черный список пуст")
        return
    text = "🚫 **ЧЕРНЫЙ СПИСОК**\n\n"
    for user_id in blacklist:
        info = get_client_info(user_id)
        name = info.get("name", "Неизвестно") if info else "Неизвестно"
        text += f"• `{user_id}` — {name}\n"
    await update.message.reply_text(text)

@catch_errors
async def active_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    if not worksheet:
        await update.message.reply_text("❌ Таблица не подключена")
        return
    try:
        all_data = worksheet.get_all_values()
        if len(all_data) < 2:
            await update.message.reply_text("📭 Нет данных")
            return
        active_statuses = ["Новый", "Уточняем нишу", "Уточняем оборот", "Уточняем проблему", "Уточняем источник", "Готов к передаче"]
        active_clients = []
        for row in all_data[1:]:
            if len(row) >= 6 and row[5] in active_statuses:
                user_id = row[1] if len(row) > 1 else "?"
                name = row[3] if len(row) > 3 else "?"
                username = row[2] if len(row) > 2 else "?"
                status = row[5]
                active_clients.append(f"• {name} (@{username}) — {status}")
        if not active_clients:
            await update.message.reply_text("📭 Нет активных диалогов")
            return
        text = f"🔥 **Активные диалоги ({len(active_clients)})**\n\n"
        text += "\n".join(active_clients[:20])
        if len(active_clients) > 20:
            text += f"\n\n... и ещё {len(active_clients) - 20}"
        await update.message.reply_text(text)
    except Exception as e:
        logger.error(f"❌ Ошибка active: {e}")
        await update.message.reply_text("❌ Ошибка при получении списка")

@catch_errors
async def info_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    if not context.args:
        await update.message.reply_text("❌ Используй: /info ID")
        return
    try:
        user_id = int(context.args[0])
        info = get_client_info(user_id)
        if not info:
            await update.message.reply_text(f"❌ Клиент с ID {user_id} не найден")
            return
        answers = user_answers.get(user_id, {})
        text = f"📋 **Информация о клиенте**\n\n"
        text += f"🔑 **Код:** {generate_client_code(user_id)}\n"
        text += f"🆔 ID: {info['user_id']}\n"
        text += f"👤 Имя: {info['name']}\n"
        text += f"📱 Username: {info['username']}\n"
        text += f"📅 Дата: {info['timestamp']}\n"
        text += f"📊 Статус: {info['status']}\n"
        if answers:
            text += f"\n📌 **Ответы клиента:**\n"
            text += f"• Ниша: {answers.get('niche', '—')}\n"
            text += f"• Оборот: {answers.get('turnover', '—')}\n"
            text += f"• Проблема: {answers.get('problem', '—')}\n"
            text += f"• Источник: {answers.get('source', '—')}\n"
        await update.message.reply_text(text)
    except ValueError:
        await update.message.reply_text("❌ Неверный ID")

@catch_errors
async def timeline_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    if not context.args:
        await update.message.reply_text("❌ Используй: /timeline ID")
        return
    try:
        user_id = int(context.args[0])
        info = get_client_info(user_id)
        if not info:
            await update.message.reply_text(f"❌ Клиент с ID {user_id} не найден")
            return
        answers = user_answers.get(user_id, {})
        text = f"📜 **ХРОНИКА ДИАЛОГА**\n\n"
        text += f"🔑 Код: {generate_client_code(user_id)}\n"
        text += f"👤 Клиент: {info['name']}\n"
        text += f"📅 Начало: {info['timestamp']}\n"
        text += f"📊 Статус: {info['status']}\n\n"
        if answers:
            text += f"📌 **Ответы:**\n"
            text += f"┌─────────────────────────┐\n"
            text += f"│ 📍 Ниша: {answers.get('niche', '—')}\n"
            text += f"│ 💰 Оборот: {answers.get('turnover', '—')}\n"
            text += f"│ ⚠️ Проблема: {answers.get('problem', '—')}\n"
            text += f"│ 🔍 Источник: {answers.get('source', '—')}\n"
            text += f"└─────────────────────────┘\n"
        await update.message.reply_text(text)
    except ValueError:
        await update.message.reply_text("❌ Неверный ID")

@catch_errors
async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    message = update.message
    broadcast_text = None
    if context.args:
        broadcast_text = " ".join(context.args)
    elif message.reply_to_message:
        broadcast_text = message.reply_to_message.text
    if not broadcast_text:
        await message.reply_text("❌ Укажи текст рассылки: /broadcast текст")
        return
    users = get_all_users_from_sheets()
    users = [u for u in users if u not in blacklist]
    if not users:
        await message.reply_text("📭 Нет пользователей для рассылки")
        return
    await message.reply_text(
        f"📊 Подтверждение рассылки\n\nСообщение: {broadcast_text[:100]}{'...' if len(broadcast_text) > 100 else ''}\nПолучателей: {len(users)}\n\nНачать рассылку?",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✅ Да", callback_data="broadcast_confirm"), InlineKeyboardButton("❌ Нет", callback_data="broadcast_cancel")]])
    )
    context.user_data['broadcast_data'] = {'text': broadcast_text, 'users': users}

@catch_errors
async def broadcast_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != ADMIN_ID: return
    if query.data == "broadcast_cancel":
        await query.edit_message_text("❌ Отменено")
        return
    if query.data == "broadcast_confirm":
        data = context.user_data.get('broadcast_data')
        if not data:
            await query.edit_message_text("❌ Ошибка")
            return
        await query.edit_message_text("📤 Начинаю рассылку...")
        text = data['text']
        users = data['users']
        success = 0
        failed = 0
        for i, uid in enumerate(users):
            try:
                await context.bot.send_message(chat_id=uid, text=text)
                success += 1
            except Exception as e:
                failed += 1
            if (i + 1) % 10 == 0:
                await query.message.edit_text(f"📤 Рассылка в процессе...\nОбработано: {i + 1}/{len(users)}\n✅ Успешно: {success}\n❌ Ошибок: {failed}")
            await asyncio.sleep(0.05)
        result = f"✅ Рассылка завершена!\n\n✅ Успешно: {success}\n❌ Ошибок: {failed}"
        await query.message.edit_text(result)
        context.user_data.pop('broadcast_data', None)

async def check_inactive_clients(context: ContextTypes.DEFAULT_TYPE):
    if not worksheet: return
    try:
        all_data = worksheet.get_all_values()
        if len(all_data) < 2: return
        client_data = {}
        for row in all_data[1:]:
            if len(row) >= 6 and row[1]:
                user_id = row[1]
                client_data[user_id] = {'status': row[5] if len(row) > 5 else "Новый", 'date': row[0] if len(row) > 0 else None}
        today = datetime.now().date()
        for user_id, data in client_data.items():
            if data['status'] in ["Передан руководителю", "Отказ", "Нецелевой"]:
                continue
            if data['date']:
                try:
                    last_date = datetime.strptime(data['date'].split()[0], "%Y-%m-%d").date()
                    if (today - last_date).days >= 3:
                        update_client_status(int(user_id), "Нецелевой")
                except: continue
    except Exception as e:
        logger.error(f"❌ Ошибка проверки неактивных: {e}")

async def daily_stats(context: ContextTypes.DEFAULT_TYPE):
    if not worksheet: return
    try:
        all_data = worksheet.get_all_values()
        if len(all_data) < 2: return
        yesterday = (datetime.now() - timedelta(days=1)).date()
        new_clients_yesterday = set()
        for row in all_data[1:]:
            if len(row) >= 2 and row[0] and row[1]:
                try:
                    msg_date = datetime.strptime(row[0].split()[0], "%Y-%m-%d").date()
                    if msg_date == yesterday:
                        if is_first_message(int(row[1])):
                            new_clients_yesterday.add(row[1])
                except: continue
        last_statuses = {}
        for row in reversed(all_data[1:]):
            if len(row) >= 6 and row[1] and row[1] not in last_statuses:
                last_statuses[row[1]] = row[5]
        status_counts = {status: 0 for status in CLIENT_STATUSES}
        for status in last_statuses.values():
            if status in status_counts:
                status_counts[status] += 1
        text = f"📊 **ЕЖЕДНЕВНАЯ СТАТИСТИКА**\n\n"
        text += f"📅 За вчера ({yesterday.strftime('%d.%m.%Y')}):\n"
        text += f"• Новых клиентов: {len(new_clients_yesterday)}\n\n"
        text += f"**Текущие статусы:**\n"
        for status in CLIENT_STATUSES:
            count = status_counts.get(status, 0)
            bar_length = int(count / max(1, len(last_statuses)) * 20)
            bar = "█" * bar_length + "░" * (20 - bar_length)
            text += f"{STATUS_EMOJI.get(status, '🟢')} {status}: {count} {bar}\n"
        text += f"\n👥 **Всего клиентов:** {len(last_statuses)}"
        await context.bot.send_message(chat_id=ADMIN_ID, text=text)
        logger.info("✅ Ежедневная статистика отправлена")
    except Exception as e:
        logger.error(f"❌ Ошибка отправки статистики: {e}")

async def daily_backup(context: ContextTypes.DEFAULT_TYPE):
    if not worksheet: return
    try:
        all_data = worksheet.get_all_values()
        if len(all_data) < 2: return
        backup_text = f"📊 **БЭКАП ТАБЛИЦЫ** {datetime.now().strftime('%Y-%m-%d')}\n\n"
        for i, row in enumerate(all_data[:20]):
            backup_text += " | ".join(row) + "\n"
        if len(all_data) > 20:
            backup_text += f"\n... и ещё {len(all_data) - 20} строк"
        await context.bot.send_message(chat_id=ADMIN_ID, text=backup_text[:4000])
        logger.info("✅ Ежедневный бэкап отправлен")
    except Exception as e:
        logger.error(f"❌ Ошибка бэкапа: {e}")

# === ГЛАВНАЯ ===
def main():
    app = Application.builder().token(BOT_TOKEN).build()
    
    app.add_error_handler(error_handler)
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("admin_logs", admin_logs))
    app.add_handler(CommandHandler("check_sheets", check_sheets))
    app.add_handler(CommandHandler("status", status_command))
    app.add_handler(CommandHandler("broadcast", broadcast_command))
    app.add_handler(CommandHandler("stats", stats_command))
    app.add_handler(CommandHandler("sources", sources_command))
    app.add_handler(CommandHandler("vip", vip_command))
    app.add_handler(CommandHandler("note", note_command))
    app.add_handler(CommandHandler("search", search_command))
    app.add_handler(CommandHandler("ban", ban_command))
    app.add_handler(CommandHandler("unban", unban_command))
    app.add_handler(CommandHandler("blacklist", blacklist_command))
    app.add_handler(CommandHandler("info", info_command))
    app.add_handler(CommandHandler("active", active_command))
    app.add_handler(CommandHandler("timeline", timeline_command))
    
    app.add_handler(CallbackQueryHandler(button_callback, pattern="^(niche|turnover|problem|source_select|source_done)_"))
    app.add_handler(CallbackQueryHandler(broadcast_callback, pattern="^broadcast_"))
    
    app.add_handler(MessageHandler(filters.Chat(GROUP_ID) & filters.TEXT & ~filters.COMMAND, handle_admin_reply_in_topic))
    app.add_handler(MessageHandler(~filters.Chat(ADMIN_ID) & (filters.PHOTO | filters.VIDEO | filters.Document.ALL | filters.VOICE), handle_media))
    app.add_handler(MessageHandler(~filters.Chat(ADMIN_ID) & filters.TEXT & ~filters.COMMAND, handle_client_message))
    
    job_queue = app.job_queue
    if job_queue:
        job_queue.run_daily(daily_stats, time=datetime.strptime("09:00", "%H:%M").time())
        job_queue.run_daily(daily_backup, time=datetime.strptime("10:00", "%H:%M").time())
        job_queue.run_repeating(check_inactive_clients, interval=3600, first=10)
        logger.info("⏰ Планировщик задач запущен")
    
    app.run_polling()

if __name__ == "__main__":
    logger.info("🔄 Запуск health check сервера")
    health_thread = threading.Thread(target=run_health_server, daemon=True)
    health_thread.start()
    
    logger.info("🔄 Запуск main()")
    main()
