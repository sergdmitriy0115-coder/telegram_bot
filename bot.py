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

# --- НАСТРОЙКИ (ТВОИ ДАННЫЕ) ---
BOT_TOKEN = os.environ.get('TELEGRAM_TOKEN')
ADMIN_ID = 7675037573  # ТВОЙ ID
GROUP_ID = -1003743707530  # ID ТВОЕЙ ГРУППЫ С ТЕМАМИ
LOG_FILE = "logs.txt"

# --- НАСТРОЙКИ GOOGLE SHEETS ---
SPREADSHEET_ID = "15vlEZ0Q6OmQh51DsA9B_fgiLwed12ekroz1aeWsgXVI"
WORKSHEET_NAME = "Логи клиентов"

# --- СТАТУСЫ ---
CLIENT_STATUSES = [
    "Новый",
    "Вопрос 1: ниша",
    "Вопрос 2: оборот",
    "Вопрос 3: проблема",
    "Вопрос 4: источник",
    "Готов к передаче",
    "Передан руководителю",
    "Нецелевой",
    "Отказ"
]

# Уникальные коды клиентов
client_codes = {}

def generate_client_code(user_id):
    """Генерирует уникальный код клиента"""
    if str(user_id) not in client_codes:
        year = datetime.now().year
        num = len(client_codes) + 1
        client_codes[str(user_id)] = f"ADD-{year}-{num:04d}"
    return client_codes[str(user_id)]

# GIF для приветствия
GREETING_GIFS = [
    "https://media.giphy.com/media/3o7abB06u9bNzA8LC8/giphy.gif",  # волна
    "https://media.giphy.com/media/JIX9t2j0ZTN9S/giphy.gif",       # печатная машинка
    "https://media.giphy.com/media/l0MYt5jH6gkLrgNRe/giphy.gif",   # приветствие
]

# Эффекты для завершения
COMPLETE_EFFECTS = [
    "🎉", "✨", "🎊", "🌟", "💫", "⭐", "🎈", "🎆", "🎇", "🔥"
]

def get_random_complete_effect():
    return " ".join(random.sample(COMPLETE_EFFECTS, 5))

def get_progress_map(stage):
    """Возвращает карту прогресса в виде путешествия"""
    stages_map = ["🚀 Начало", "🟡 Ниша", "🟡 Оборот", "🟡 Проблема", "🟡 Источник", "🏁 Финиш"]
    progress = []
    for i, s in enumerate(stages_map):
        if i <= stage:
            progress.append(f"✅ {s}")
        else:
            progress.append(f"⬜ {s}")
    return " → ".join(progress)

STATUS_EMOJI = {
    "Новый": "🟢",
    "Вопрос 1: ниша": "🟡",
    "Вопрос 2: оборот": "🟡",
    "Вопрос 3: проблема": "🟡",
    "Вопрос 4: источник": "🟡",
    "Готов к передаче": "🟢",
    "Передан руководителю": "🔵",
    "Нецелевой": "⚪",
    "Отказ": "🔴"
}

# Контакты руководителей
MANAGER_CONTACTS = ["@Darya_Pril06", "@anny_nizh"]

# Приветствия по времени суток
def get_greeting():
    hour = datetime.now().hour
    if 5 <= hour < 12:
        return "Доброе утро 🌅"
    elif 12 <= hour < 18:
        return "Добрый день ☀️"
    elif 18 <= hour < 23:
        return "Добрый вечер 🌙"
    else:
        return "Доброй ночи 🌟"

# Хранилища
user_topics = {}
blacklist = set()
user_stage = {}
user_answers = {}
admin_actions_log = []

# --- Логирование ---
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
    """Генерирует QR-код с данными"""
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
            {"autoResizeDimensions": {"dimensions": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 7}}},
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
                    "note": all_data[i][6] if len(all_data[i]) > 6 else ""
                }
        return None
    except Exception as e:
        logger.error(f"❌ Ошибка получения информации: {e}")
        return None

def log_to_sheets(user_id, username, first_name, message_text, status="Новый"):
    if not worksheet: return
    try:
        if is_first_message(user_id):
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            row = [timestamp, str(user_id), f"@{username}" if username else "нет", first_name or "нет", message_text, status, ""]
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
            new_headers = ["Timestamp", "User ID", "Ник клиента", "Имя", "Сообщение", "Статус", "Заметки"]
            if headers != new_headers:
                logger.info("🔄 Обновляем заголовки")
                worksheet.clear()
                worksheet.append_row(new_headers)
                format_worksheet(worksheet)
        except gspread.exceptions.WorksheetNotFound:
            logger.info("🔄 Создаём новый лист")
            worksheet = spreadsheet.add_worksheet(title=WORKSHEET_NAME, rows=1000, cols=7)
            worksheet.append_row(["Timestamp", "User ID", "Ник клиента", "Имя", "Сообщение", "Статус", "Заметки"])
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

# === СОЗДАНИЕ ТЕМЫ ===
async def get_or_create_topic(context, user_id, username, first_name):
    if user_id in user_topics:
        return user_topics[user_id]
    
    client_code = generate_client_code(user_id)
    topic_name = f"{client_code} | {first_name} (@{username if username else 'no_username'})"
    
    try:
        result = await context.bot.create_forum_topic(chat_id=GROUP_ID, name=topic_name[:128])
        topic_id = result.message_thread_id
        user_topics[user_id] = topic_id
        
        current_status = get_client_info(user_id).get("status", "Новый") if get_client_info(user_id) else "Новый"
        current_note = get_client_note(user_id)
        status_emoji = STATUS_EMOJI.get(current_status, "🟢")
        
        welcome_text = f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        welcome_text += f"{status_emoji} **НОВЫЙ КЛИЕНТ**\n"
        welcome_text += f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        welcome_text += f"🔑 **Код:** `{client_code}`\n"
        welcome_text += f"👤 **Имя:** {first_name}\n"
        welcome_text += f"📱 **Username:** @{username if username else 'нет'}\n"
        welcome_text += f"🆔 **ID:** `{user_id}`\n"
        welcome_text += f"📊 **Статус:** {current_status}\n"
        
        if user_id in blacklist:
            welcome_text += f"\n🚫 **Клиент в ЧЕРНОМ СПИСКЕ!**\n"
        if current_note:
            welcome_text += f"\n📝 **Заметки:**\n{current_note}\n"
        
        welcome_text += f"\n────────────────────────────────"
        
        await context.bot.send_message(chat_id=GROUP_ID, message_thread_id=topic_id, text=welcome_text, parse_mode='Markdown')
        return topic_id
    except Exception as e:
        logger.error(f"❌ Ошибка создания темы: {e}")
        return None

# === КРАСИВОЕ СООБЩЕНИЕ В ТЕМУ ===
async def send_pretty_message_to_topic(context, topic_id, client_text, bot_response, status, stage, note_text=""):
    status_emoji = STATUS_EMOJI.get(status, "🟢")
    progress_map = get_progress_map(stage)
    
    message_lines = []
    message_lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    message_lines.append(f"{status_emoji} **СООБЩЕНИЕ ОТ КЛИЕНТА**")
    message_lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    message_lines.append("")
    message_lines.append("📝 **Текст:**")
    message_lines.append(f"{client_text}")
    message_lines.append("")
    message_lines.append("🤖 **Ответ бота:**")
    message_lines.append(f"{bot_response}")
    message_lines.append("")
    message_lines.append(f"📊 **Статус:** {status}")
    message_lines.append(f"🗺️ **Прогресс:** {progress_map}")
    
    if note_text:
        message_lines.append("")
        message_lines.append("📝 **Заметки:**")
        if len(note_text) > 500:
            note_text = note_text[:500] + "..."
        message_lines.append(note_text)
    
    message_lines.append("")
    message_lines.append("────────────────────────────────")
    
    await context.bot.send_message(
        chat_id=GROUP_ID,
        message_thread_id=topic_id,
        text="\n".join(message_lines),
        parse_mode='Markdown'
    )

# === СКРИПТ ОТВЕТОВ ===
def get_status_and_next_message(stage, user_message="", user_id=None):
    user_message_lower = user_message.lower()
    user_message_clean = user_message.strip()
    
    # Проверка на слишком короткий ответ
    if len(user_message_clean.split()) < 3 and stage > 0 and stage < 5:
        return None, "Извините, я не совсем понял. Можете ответить чуть подробнее?", None
    
    # Экстренные сценарии
    if any(word in user_message_lower for word in ["бот", "робот", "ты бот", "автоответчик"]):
        return "Передан руководителю", "Да, я бот. Моя задача — быстро собрать базовую информацию и передать её руководителю.\n\nЕсли хотите, можете сразу написать руководителю: @Darya_Pril06 или @anny_nizh.", None
    
    if any(word in user_message_lower for word in ["позвоните", "перезвоните", "свяжитесь", "напишите", "человек", "менеджер"]):
        return "Передан руководителю", "Я передаю ваш запрос руководителю. Он свяжется с вами в ближайшее время.\n\nКонтакты: @Darya_Pril06 или @anny_nizh.", None
    
    if any(word in user_message_lower for word in ["не надо", "бесполезно", "дорого", "обман", "не хочу", "отстань"]):
        return "Отказ", "Понимаю. Я передаю ваш запрос руководителю, он сможет подробно ответить на все вопросы.\n\nКонтакты: @Darya_Pril06 или @anny_nizh.", None
    
    answers = user_answers.get(user_id, {"niche": "", "turnover": "", "problem": "", "source": ""}) if user_id else {}
    
    if stage == 0:
        return "Вопрос 1: ниша", "Расскажите, над каким проектом работаете? Какая у вас ниша?", answers
    elif stage == 1:
        answers["niche"] = user_message_clean[:200]
        return "Вопрос 2: оборот", "Спасибо! А какой сейчас примерный ежемесячный оборот? Это поможет понять масштаб.", answers
    elif stage == 2:
        answers["turnover"] = user_message_clean[:100]
        return "Вопрос 3: проблема", "Понял. А в чём сейчас основная сложность с продажами? Что бы вы хотели изменить или улучшить?", answers
    elif stage == 3:
        answers["problem"] = user_message_clean[:300]
        return "Вопрос 4: источник", "Спасибо за подробности! Откуда узнали о нас?", answers
    elif stage == 4:
        answers["source"] = user_message_clean[:100]
        summary = f"📊 **Выжимка:**\n• Ниша: {answers['niche']}\n• Оборот: {answers['turnover']}\n• Проблема: {answers['problem']}\n• Источник: {answers['source']}"
        return "Готов к передаче", "Отлично, спасибо за информацию!\n\nЯ передаю ваш запрос руководителю. Он свяжется с вами в ближайшее время.\n\nКонтакты: @Darya_Pril06 или @anny_nizh.", summary
    elif stage == 5:
        return "Передан руководителю", None, None
    else:
        return "Нецелевой", "Извините, я не совсем понял ваш запрос.\n\nЕсли вы ищете отдел продаж для онлайн-курса — напишите мне, и я помогу.\n\nЕсли нет — извините, что отвлёк!", None

# === КОМАНДА /START ===
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
    
    user_stage[user.id] = 0
    user_answers[user.id] = {"niche": "", "turnover": "", "problem": "", "source": ""}
    log_to_sheets(user.id, user.username, user.first_name, "/start", status="Новый")
    
    # Отправляем GIF-приветствие
    greeting_gif = random.choice(GREETING_GIFS)
    await context.bot.send_animation(chat_id=user.id, animation=greeting_gif)
    
    # Отправляем текстовое приветствие
    greeting = get_greeting()
    welcome_msg = f"{greeting}! 👋\n\nЯ виртуальный помощник компании ADD production. Мы помогаем выстраивать отделы продаж для онлайн-курсов и инфопродуктов.\n\nЧтобы я мог передать ваш запрос руководителю, пожалуйста, ответьте на несколько коротких вопросов."
    
    await update.message.reply_text(welcome_msg)
    
    # Уведомление в группу
    client_code = generate_client_code(user.id)
    await context.bot.send_message(chat_id=GROUP_ID, message_thread_id=topic_id, text=f"👤 **Новый клиент!**\n🔑 Код: {client_code}\n📝 Начинаем диалог...")
    
    # Отправляем первый вопрос
    new_status, next_msg, _ = get_status_and_next_message(0)
    await update.message.reply_text(next_msg)
    user_stage[user.id] = 1
    log_to_sheets(user.id, user.username, user.first_name, "Вопрос: ниша", status="Вопрос 1: ниша")
    
    # Отправляем красивое сообщение в тему
    await send_pretty_message_to_topic(context, topic_id, "Начало диалога", next_msg, "Вопрос 1: ниша", 1)

# === ОСНОВНОЙ ОБРАБОТЧИК ===
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
    
    # Получаем текущий этап
    stage = user_stage.get(user_id, 0)
    
    # Проверяем, не завершён ли диалог
    client_info = get_client_info(user_id)
    if client_info and client_info.get("status") in ["Передан руководителю", "Отказ"]:
        await message.reply_text(f"Ваш запрос уже передан руководителю. Если у вас остались вопросы, можете написать напрямую: {', '.join(MANAGER_CONTACTS)}")
        return
    
    # Получаем следующий статус и ответ
    new_status, reply_text, summary = get_status_and_next_message(stage, message.text, user_id)
    
    # Если ответ слишком короткий и нужно переспросить
    if new_status is None:
        await message.reply_text(reply_text)
        await send_pretty_message_to_topic(context, topic_id, message.text, reply_text, user_stage.get(user_id, "Новый"), stage)
        return
    
    # Отправляем ответ
    if reply_text:
        # Эффект "печатает"
        await context.bot.send_chat_action(chat_id=user_id, action="typing")
        await asyncio.sleep(1)
        await message.reply_text(reply_text)
    
    # Обновляем этап
    if new_status == "Готов к передаче":
        user_stage[user_id] = 5
        # Добавляем выжимку в заметки
        if summary:
            add_note_to_client(user_id, summary)
        
        # Отправляем эффект завершения
        effect = get_random_complete_effect()
        await message.reply_text(f"{effect}\n🎉 **ПОЗДРАВЛЯЮ, {user.first_name}!** 🎉\n\nТвой запрос передан руководителю!\n\n{effect}")
        
        # Отправляем QR-код с контактами
        qr_bio = await generate_qr_code(f"ADD production\nКонтакты: {', '.join(MANAGER_CONTACTS)}")
        await context.bot.send_photo(chat_id=user_id, photo=qr_bio, caption="📱 Отсканируйте QR-код, чтобы сохранить контакты руководителя")
        
    elif new_status == "Передан руководителю":
        user_stage[user_id] = 5
    elif new_status == "Отказ":
        user_stage[user_id] = 5
    elif stage < 4:
        user_stage[user_id] = stage + 1
    
    # Обновляем статус в таблице
    log_to_sheets(user_id, user.username, user.first_name, message.text, status=new_status)
    
    # Отправляем красивое сообщение в тему
    await send_pretty_message_to_topic(
        context, topic_id, message.text, reply_text if reply_text else "[смена статуса]", 
        new_status, user_stage.get(user_id, stage)
    )
    
    logger.info(f"🏁 Обработка завершена. Новый статус: {new_status}")

# === КОМАНДА /TIMELINE ===
@catch_errors
async def timeline_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    
    if not context.args:
        await update.message.reply_text("❌ Используй: /timeline ID")
        return
    
    try:
        user_id = int(context.args[0])
        info = get_client_info(user_id)
        if not info:
            await update.message.reply_text(f"❌ Клиент с ID {user_id} не найден")
            return
        
        # Получаем все сообщения клиента (из таблицы сложно, но можем из лога)
        # Для простоты покажем только последние данные
        text = f"📜 **ХРОНИКА ДИАЛОГА**\n\n"
        text += f"🔑 Код: {generate_client_code(user_id)}\n"
        text += f"👤 Клиент: {info['name']}\n"
        text += f"📅 Начало: {info['timestamp']}\n"
        text += f"📊 Статус: {info['status']}\n"
        
        if user_id in user_answers:
            answers = user_answers[user_id]
            text += f"\n📌 **Ответы:**\n"
            text += f"┌─────────────────────────┐\n"
            text += f"│ 📍 Ниша: {answers.get('niche', '—')}\n"
            text += f"│ 💰 Оборот: {answers.get('turnover', '—')}\n"
            text += f"│ ⚠️ Проблема: {answers.get('problem', '—')}\n"
            text += f"│ 🔍 Источник: {answers.get('source', '—')}\n"
            text += f"└─────────────────────────┘\n"
        
        await update.message.reply_text(text)
        
    except ValueError:
        await update.message.reply_text("❌ Неверный ID")

# === ОСТАЛЬНЫЕ КОМАНДЫ (сокращённо, но все работают) ===
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
    await update.message.reply_text("📊 Статусы теперь обновляются автоматически. Ручная смена не требуется.")

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

@catch_errors
async def handle_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user.id in blacklist:
        await update.message.reply_text("⛔ Вы заблокированы в этом боте.")
        return
    message = update.message
    topic_id = await get_or_create_topic(context, user.id, user.username, user.first_name)
    if not topic_id:
        await message.reply_text("❌ Ошибка")
        return
    media_type = "фото"
    if message.video: media_type = "видео"
    elif message.document: media_type = "документ"
    elif message.voice: media_type = "голосовое"
    current_status = get_client_info(user.id).get("status", "Новый") if get_client_info(user.id) else "Новый"
    log_to_sheets(user.id, user.username, user.first_name, f"[{media_type}]", status=current_status)
    await message.forward(chat_id=GROUP_ID, message_thread_id=topic_id)
    await message.reply_text("✅ Файл получен!")

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
        active_statuses = ["Новый", "Вопрос 1: ниша", "Вопрос 2: оборот", "Вопрос 3: проблема", "Вопрос 4: источник", "Готов к передаче"]
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
        text += f"💬 Сообщение: {info['message'][:100]}...\n"
        if answers:
            text += f"\n📌 **Ответы:**\n"
            text += f"• Ниша: {answers.get('niche', '—')}\n"
            text += f"• Оборот: {answers.get('turnover', '—')}\n"
            text += f"• Проблема: {answers.get('problem', '—')}\n"
            text += f"• Источник: {answers.get('source', '—')}\n"
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
        for i, row in enumerate(all_data[:20]):  # Ограничим первыми 20 строками для безопасности
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
    app.add_handler(CommandHandler("note", note_command))
    app.add_handler(CommandHandler("search", search_command))
    app.add_handler(CommandHandler("ban", ban_command))
    app.add_handler(CommandHandler("unban", unban_command))
    app.add_handler(CommandHandler("blacklist", blacklist_command))
    app.add_handler(CommandHandler("info", info_command))
    app.add_handler(CommandHandler("active", active_command))
    app.add_handler(CommandHandler("timeline", timeline_command))
    
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
