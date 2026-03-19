import os
import json
import logging
import threading
import re
import asyncio
import traceback
from datetime import datetime, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, CallbackQueryHandler
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from openai import OpenAI

# --- НАСТРОЙКИ (ТВОИ ДАННЫЕ) ---
BOT_TOKEN = os.environ.get('TELEGRAM_TOKEN')
ADMIN_ID = 7675037573  # ТВОЙ ID
GROUP_ID = -1003743707530  # ID ТВОЕЙ ГРУППЫ С ТЕМАМИ
LOG_FILE = "logs.txt"

# --- НАСТРОЙКИ ДЛЯ ОТПРАВКИ ОШИБОК ---
ERROR_LOG_CHAT = 7675037573  # ТВОЙ ID (числом!)

# --- НАСТРОЙКИ CLOUD.RU (ЭВОЛЮШН ФАУНДЭЙШН МОДЕЛС) ---
CLOUD_API_KEY = os.environ.get('CLOUD_API_KEY')
AI_MODEL = "GigaChat-2-Max"  # Бесплатная модель до октября 2025

# --- НАСТРОЙКИ GOOGLE SHEETS (ТВОЯ ТАБЛИЦА) ---
SPREADSHEET_ID = "15vlEZ0Q6OmQh51DsA9B_fgiLwed12ekroz1aeWsgXVI"
WORKSHEET_NAME = "Логи клиентов"

# Статусы для клиентов
CLIENT_STATUSES = [
    "Новый",
    "В процессе квалификации", 
    "Готов к передаче",
    "Передан руководителю 👤",
    "Негатив/Отказ",
    "Нецелевой"
]

# Контакты руководителей
MANAGER_CONTACTS = [
    "@Darya_Pril06",
    "@anny_nizh"
]

# Информация о компании (с сайта)
COMPANY_INFO = """
ADD production - компания, которая предоставляет отдел продаж для онлайн-курсов и инфопродуктов.

Что мы делаем:
- Разрабатываем скрипты и стратегии продаж
- Предоставляем топ-менеджеров с опытом в инфопродуктах
- Делаем анализ продаж и ежедневную отчётность
- Работаем за процент от прибыли
- Есть опыт работы с разными нишами

Сайт: https://add.production.tilda.ws
"""

# Хранилища
user_topics = {}
blacklist = set()
user_conversation_history = {}

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

# === ФУНКЦИЯ ДЛЯ ОТПРАВКИ ОШИБОК ===
async def send_error_notification(context, error_title, error_details, user_info=None):
    try:
        error_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        message = f"🚨 ОШИБКА В БОТЕ\n\n"
        message += f"⏰ Время: {error_time}\n"
        message += f"📌 Тип: {error_title}\n\n"
        message += f"📋 Детали:\n{error_details[:1500]}\n"
        
        if user_info:
            message += f"\n👤 Пользователь: {user_info}"
        
        await context.bot.send_message(
            chat_id=ERROR_LOG_CHAT,
            text=message
        )
        logger.info(f"✅ Уведомление об ошибке отправлено в {ERROR_LOG_CHAT}")
    except Exception as e:
        logger.error(f"❌ Не удалось отправить уведомление об ошибке: {e}")

# === ГЛОБАЛЬНЫЙ ОБРАБОТЧИК ОШИБОК ===
async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        error_title = "Исключение в обработчике"
        error_details = traceback.format_exc()
        
        user_info = None
        if update and update.effective_user:
            user = update.effective_user
            user_info = f"{user.first_name} (@{user.username}) ID: {user.id}"
        
        logger.error(f"❌ Ошибка: {error_details}")
        await send_error_notification(context, error_title, error_details, user_info)
    except Exception as e:
        logger.critical(f"❌ Критическая ошибка в error_handler: {e}")

# === ДЕКОРАТОР ДЛЯ ОТЛОВА ОШИБОК ===
def catch_errors(func):
    async def wrapper(update, context, *args, **kwargs):
        try:
            logger.info(f"🔄 Вызов функции {func.__name__}")
            return await func(update, context, *args, **kwargs)
        except Exception as e:
            error_title = f"Ошибка в {func.__name__}"
            error_details = traceback.format_exc()
            
            user_info = None
            if update and update.effective_user:
                user = update.effective_user
                user_info = f"{user.first_name} (@{user.username}) ID: {user.id}"
            
            logger.error(f"❌ Ошибка в {func.__name__}: {error_details}")
            await send_error_notification(context, error_title, error_details, user_info)
            
            try:
                await update.message.reply_text("❌ Произошла внутренняя ошибка. Мы уже работаем над её исправлением.")
            except:
                pass
    return wrapper

# === ИНИЦИАЛИЗАЦИЯ CLOUD.RU (ЭВОЛЮШН ФАУНДЭЙШН МОДЕЛС) ===
def init_ai_client():
    cloud_api_key = os.environ.get('CLOUD_API_KEY')
    if not cloud_api_key:
        logger.warning("⚠️ CLOUD_API_KEY не задан. Бот будет работать без AI.")
        return None

    try:
        client = OpenAI(
            base_url="https://foundation-models.api.cloud.ru/v1",
            api_key=cloud_api_key,
            default_headers={}
        )
        logger.info(f"✅ AI клиент (Cloud.ru) инициализирован")
        return client
    except Exception as e:
        logger.error(f"❌ Ошибка инициализации AI клиента Cloud.ru: {e}")
        return None

ai_client = init_ai_client()

# === ФУНКЦИЯ ДЛЯ ГЕНЕРАЦИИ ОТВЕТА ИИ ===
async def generate_ai_response(user_id, user_message, user_name):
    logger.info(f"🤖 Генерация AI ответа для {user_id}")
    
    if not ai_client:
        logger.warning("⚠️ AI клиент не инициализирован, возвращаем заглушку")
        return "Спасибо за обращение! С вами свяжется наш руководитель."
    
    history = user_conversation_history.get(user_id, [])
    if len(history) > 10:
        history = history[-10:]
    
    contacts_str = ', '.join(MANAGER_CONTACTS)
    
    system_prompt = f"""Ты — виртуальный ассистент компании ADD production. Твоя задача — квалифицировать потенциальных клиентов (продюсеров, экспертов, владельцев онлайн-школ), которые ищут отдел продаж для своих проектов.

**ВАЖНО: ТЫ НИЧЕГО НЕ ПРОДАЁШЬ!** Твоя цель — только собрать информацию и передать клиента руководителю.

**ИНФОРМАЦИЯ О КОМПАНИИ:**
{COMPANY_INFO}

**ЧТО НУЖНО УЗНАТЬ У КЛИЕНТА:**
1. Ниша — какой у него продукт/курс/услуга
2. Оборот — текущий или ожидаемый (примерно)
3. Проблема — почему ищет отдел продаж
4. Откуда узнал о нас
5. Готовность к сотрудничеству

**СТИЛЬ ОБЩЕНИЯ:**
- Общайся на "ты", дружелюбно, но профессионально
- Используй эмодзи 😊, но умеренно
- Отвечай кратко (2-3 предложения)

**ПРАВИЛА:**
- НИКОГДА не предлагай цены, услуги, коммерческие предложения
- Если клиент агрессивен или недоволен → сразу передавай руководителю
- Если клиент чётко описал потребность → предложи передать руководителю
- НИКОГДА не признавайся, что ты ИИ

Имя клиента: {user_name}
"""
    
    messages = [{"role": "system", "content": system_prompt}]
    for msg in history:
        messages.append(msg)
    messages.append({"role": "user", "content": user_message})
    
    try:
        logger.info(f"📤 Отправка запроса к Cloud.ru")
        response = ai_client.chat.completions.create(
            model=AI_MODEL,
            messages=messages,
            temperature=0.7,
            max_tokens=500
        )
        
        ai_answer = response.choices[0].message.content.strip()
        logger.info(f"✅ AI ответ получен")
        
        user_conversation_history.setdefault(user_id, []).append(
            {"role": "user", "content": user_message}
        )
        user_conversation_history.setdefault(user_id, []).append(
            {"role": "assistant", "content": ai_answer}
        )
        
        return ai_answer
        
    except Exception as e:
        logger.error(f"❌ Ошибка генерации AI ответа: {e}", exc_info=True)
        return "Извини, техническая заминка. Я передам твой запрос руководителю, он свяжется с тобой."

# === ФОРМАТИРОВАНИЕ ТАБЛИЦЫ ===
def format_worksheet(worksheet):
    try:
        sheet_id = worksheet.id
        
        requests = [
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 0,
                        "endRowIndex": worksheet.row_count
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "textFormat": {"fontFamily": "Arial", "fontSize": 11}
                        }
                    },
                    "fields": "userEnteredFormat.textFormat"
                }
            },
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 0,
                        "endRowIndex": 1
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "textFormat": {"bold": True},
                            "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9}
                        }
                    },
                    "fields": "userEnteredFormat.textFormat,userEnteredFormat.backgroundColor"
                }
            },
            {
                "autoResizeDimensions": {
                    "dimensions": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": 0,
                        "endIndex": 7
                    }
                }
            },
            {
                "updateBorders": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 0,
                        "endRowIndex": worksheet.row_count,
                        "startColumnIndex": 0,
                        "endColumnIndex": 7
                    },
                    "top": {"style": "SOLID", "color": {"red": 0, "green": 0, "blue": 0}},
                    "bottom": {"style": "SOLID", "color": {"red": 0, "green": 0, "blue": 0}},
                    "left": {"style": "SOLID", "color": {"red": 0, "green": 0, "blue": 0}},
                    "right": {"style": "SOLID", "color": {"red": 0, "green": 0, "blue": 0}},
                    "innerHorizontal": {"style": "SOLID", "color": {"red": 0, "green": 0, "blue": 0}},
                    "innerVertical": {"style": "SOLID", "color": {"red": 0, "green": 0, "blue": 0}}
                }
            },
            {
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": sheet_id,
                        "gridProperties": {"frozenRowCount": 1}
                    },
                    "fields": "gridProperties.frozenRowCount"
                }
            }
        ]
        
        status_colors = [
            {"red": 0.8, "green": 0.9, "blue": 1.0},
            {"red": 1.0, "green": 1.0, "blue": 0.7},
            {"red": 0.7, "green": 1.0, "blue": 0.7},
            {"red": 1.0, "green": 0.9, "blue": 0.4},
            {"red": 1.0, "green": 0.7, "blue": 0.7},
            {"red": 0.6, "green": 0.6, "blue": 0.6}
        ]
        
        for i, status in enumerate(CLIENT_STATUSES):
            requests.append({
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": [{
                            "sheetId": sheet_id,
                            "startRowIndex": 1,
                            "startColumnIndex": 5,
                            "endColumnIndex": 6
                        }],
                        "booleanRule": {
                            "condition": {
                                "type": "TEXT_EQ",
                                "values": [{"userEnteredValue": status}]
                            },
                            "format": {
                                "backgroundColor": status_colors[i],
                                "textFormat": {"bold": True}
                            }
                        }
                    }
                }
            })
        
        worksheet.spreadsheet.batch_update({"requests": requests})
        logger.info("✅ Таблица отформатирована")
    except Exception as e:
        logger.error(f"❌ Ошибка форматирования: {e}")

# === ПРОВЕРКА НА ДУБЛИКАТЫ ===
def is_first_message(user_id):
    if not worksheet:
        return True
    try:
        user_ids = worksheet.col_values(2)[1:]
        return str(user_id) not in user_ids
    except Exception as e:
        logger.error(f"❌ Ошибка проверки дубликатов: {e}")
        return True

# === ОБНОВЛЕНИЕ СТАТУСА ===
def update_client_status(user_id, new_status):
    if not worksheet:
        return False
    
    try:
        all_data = worksheet.get_all_values()
        if len(all_data) < 2:
            return False
        
        last_row_index = None
        for i in range(len(all_data)-1, 0, -1):
            if len(all_data[i]) > 1 and all_data[i][1] == str(user_id):
                last_row_index = i + 1
                break
        
        if last_row_index:
            worksheet.update_cell(last_row_index, 6, new_status)
            logger.info(f"✅ Статус для user {user_id} обновлён на {new_status}")
            return True
        return False
    except Exception as e:
        logger.error(f"❌ Ошибка обновления статуса: {e}")
        return False

# === ДОБАВЛЕНИЕ ЗАМЕТКИ ===
def add_note_to_client(user_id, note):
    if not worksheet:
        return False
    
    try:
        all_data = worksheet.get_all_values()
        if len(all_data) < 2:
            return False
        
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
            logger.info(f"✅ Заметка добавлена для user {user_id}")
            return True
        return False
    except Exception as e:
        logger.error(f"❌ Ошибка добавления заметки: {e}")
        return False

# === ПОЛУЧИТЬ ЗАМЕТКУ ===
def get_client_note(user_id):
    if not worksheet:
        return ""
    try:
        all_data = worksheet.get_all_values()
        for i in range(len(all_data)-1, 0, -1):
            if len(all_data[i]) > 1 and all_data[i][1] == str(user_id):
                if len(all_data[i]) >= 7:
                    return all_data[i][6]
                return ""
        return ""
    except Exception as e:
        logger.error(f"❌ Ошибка получения заметки: {e}")
        return ""

# === ЗАПИСЬ В ТАБЛИЦУ ===
def log_to_sheets(user_id, username, first_name, message_text, status="Новый"):
    if not worksheet:
        return
    
    try:
        if is_first_message(user_id):
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            row = [
                timestamp,
                str(user_id),
                f"@{username}" if username else "нет",
                first_name or "нет",
                message_text,
                status,
                ""
            ]
            worksheet.append_row(row)
            logger.info(f"✅ Новая запись для user {user_id}")
            
            try:
                props = worksheet.spreadsheet.fetch_sheet_metadata(
                    fields="sheets.properties.gridProperties.frozenRowCount"
                )
                frozen = props['sheets'][0]['properties']['gridProperties'].get('frozenRowCount', 0)
                if frozen == 0:
                    format_worksheet(worksheet)
            except:
                pass
        else:
            update_client_status(user_id, status)
    except Exception as e:
        logger.error(f"❌ Ошибка записи: {e}")

# === ПОЛУЧИТЬ СТАТУС ===
def get_client_status(user_id):
    if not worksheet:
        return "Новый"
    
    try:
        all_data = worksheet.get_all_values()
        if len(all_data) < 2:
            return "Новый"
        
        for i in range(len(all_data)-1, 0, -1):
            if len(all_data[i]) > 1 and all_data[i][1] == str(user_id):
                if len(all_data[i]) >= 6:
                    return all_data[i][5]
                return "Новый"
        return "Новый"
    except Exception as e:
        logger.error(f"❌ Ошибка получения статуса: {e}")
        return "Новый"

# === ПОЛУЧЕНИЕ СПИСКА ПОЛЬЗОВАТЕЛЕЙ ===
def get_all_users_from_sheets():
    if not worksheet:
        return []
    try:
        all_data = worksheet.get_all_values()
        if len(all_data) < 2:
            return []
        
        users = set()
        for row in all_data[1:]:
            if len(row) > 1 and row[1]:
                try:
                    users.add(int(row[1]))
                except:
                    continue
        return list(users)
    except Exception as e:
        logger.error(f"❌ Ошибка получения списка: {e}")
        return []

# === ИНИЦИАЛИЗАЦИЯ GOOGLE SHEETS ===
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
            headers = ["Timestamp", "User ID", "Ник клиента", "Имя", "Сообщение", "Статус", "Заметки"]
            worksheet.append_row(headers)
            format_worksheet(worksheet)
        
        logger.info("🎉 Google Sheets подключен")
        return worksheet
        
    except Exception as e:
        logger.error(f"❌ Ошибка: {e}")
        return None

worksheet = init_google_sheets()

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
        
        current_status = get_client_status(user_id)
        current_note = get_client_note(user_id)
        
        welcome_text = f"🆕 **Новый клиент!**\n"
        welcome_text += f"Имя: {first_name}\n"
        welcome_text += f"Username: @{username if username else 'нет'}\n"
        welcome_text += f"ID: `{user_id}`\n"
        welcome_text += f"Статус: {current_status}\n"
        
        if user_id in blacklist:
            welcome_text += f"\n🚫 **Клиент в ЧЕРНОМ СПИСКЕ!**\n"
        
        if current_note:
            welcome_text += f"\n📝 **Заметки:**\n{current_note}"
        
        await context.bot.send_message(
            chat_id=GROUP_ID, message_thread_id=topic_id,
            text=welcome_text
        )
        return topic_id
        
    except Exception as e:
        logger.error(f"❌ Ошибка создания темы: {e}")
        return None

# === КОМАНДА /START ===
@catch_errors
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    logger.info(f"🚀 Команда /start от пользователя {user.id}")
    
    if user.id in blacklist:
        await update.message.reply_text("⛔ Вы заблокированы в этом боте.")
        return
    
    save_message(user.id, user.username or "нет", user.first_name or "нет", "/start")
    
    topic_id = await get_or_create_topic(context, user.id, user.username, user.first_name)
    if not topic_id:
        await update.message.reply_text("❌ Ошибка")
        return
    
    log_to_sheets(user.id, user.username, user.first_name, "/start", status="Новый")
    
    welcome_msg = f"Привет, {user.first_name}! 👋\n\nЯ помощник компании ADD production. Расскажи, какой у тебя проект и что именно ищешь? Мы помогаем с отделами продаж для онлайн-курсов."
    
    await update.message.reply_text(welcome_msg)
    
    await context.bot.send_message(
        chat_id=GROUP_ID, 
        message_thread_id=topic_id, 
        text="👤 Клиент начал диалог. ИИ-помощник активен."
    )
    
    if user.id not in user_conversation_history:
        user_conversation_history[user.id] = []

# === ОСНОВНОЙ ОБРАБОТЧИК СООБЩЕНИЙ ===
@catch_errors
async def handle_client_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    logger.info(f"🔥🔥🔥 ПОЛУЧЕНО СООБЩЕНИЕ от {user.id}")
    logger.info(f"Текст: {update.message.text}")
    
    if user.id in blacklist:
        await update.message.reply_text("⛔ Вы заблокированы в этом боте.")
        return
    
    message = update.message
    user_id = user.id
    
    save_message(user_id, user.username or "нет", user.first_name or "нет", message.text)
    
    topic_id = await get_or_create_topic(context, user_id, user.username, user.first_name)
    if not topic_id:
        await message.reply_text("❌ Ошибка")
        return
    
    current_status = get_client_status(user_id)
    logger.info(f"📊 Текущий статус: {current_status}")
    
    # Генерируем ответ
    ai_response = await generate_ai_response(user_id, message.text, user.first_name)
    
    # Проверяем, нужно ли передать руководителю
    transfer_keywords = ["передаю руководителю", "свяжется руководитель", "передам твой запрос", "контакты"]
    should_transfer = any(keyword in ai_response.lower() for keyword in transfer_keywords)
    
    if should_transfer:
        update_client_status(user_id, "Готов к передаче")
        current_status = "Готов к передаче"
        
        await context.bot.send_message(
            chat_id=GROUP_ID,
            message_thread_id=topic_id,
            text=f"🤖 **Клиент готов к передаче!**\nКонтакты: {', '.join(MANAGER_CONTACTS)}"
        )
    
    # Отправляем ответ
    await message.reply_text(ai_response)
    
    # Логируем
    log_to_sheets(user_id, user.username, user.first_name, message.text, status=current_status)
    
    # Отправляем в тему
    current_note = get_client_note(user_id)
    note_text = f"\n\n📝 Заметка: {current_note}" if current_note else ""
    
    await context.bot.send_message(
        chat_id=GROUP_ID, 
        message_thread_id=topic_id, 
        text=f"👤 Клиент: {message.text}\n\n🤖 Ответ: {ai_response}\n\nСтатус: {current_status}{note_text}"
    )
    
    logger.info(f"🏁 Обработка сообщения от {user_id} завершена")

# === КОМАНДА СТАТУС ===
@catch_errors
async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    
    keyboard = []
    row = []
    for i, status in enumerate(CLIENT_STATUSES):
        row.append(InlineKeyboardButton(status, callback_data=f"status_{i+1}"))
        if (i + 1) % 2 == 0:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    
    keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="status_cancel")])
    
    await update.message.reply_text(
        "📊 Выбери новый статус для клиента:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

# === ОБРАБОТКА КНОПОК ===
@catch_errors
async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    if query.from_user.id != ADMIN_ID:
        await query.edit_message_text("⛔ Нет прав")
        return
    
    data = query.data
    
    if data == "status_cancel":
        await query.edit_message_text("❌ Отменено")
        return
    
    if data.startswith("status_"):
        status_num = int(data.replace("status_", "")) - 1
        selected_status = CLIENT_STATUSES[status_num]
        
        if query.message.reply_to_message:
            original_text = query.message.reply_to_message.text or ""
            match = re.search(r'ID: `?(\d+)`?', original_text)
            
            if match:
                client_id = int(match.group(1))
                if update_client_status(client_id, selected_status):
                    await query.edit_message_text(f"✅ Статус обновлён на: {selected_status}")
                    
                    if client_id in user_topics:
                        topic_id = user_topics[client_id]
                        await context.bot.send_message(
                            chat_id=GROUP_ID,
                            message_thread_id=topic_id,
                            text=f"📊 Статус обновлён: {selected_status}"
                        )
                else:
                    await query.edit_message_text("❌ Ошибка обновления статуса")
            else:
                await query.edit_message_text("❌ Не могу найти ID клиента")
        else:
            await query.edit_message_text("❌ Используй Reply на сообщение клиента")

# === РАССЫЛКА ===
@catch_errors
async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    
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
        f"📊 Подтверждение рассылки\n\n"
        f"Сообщение: {broadcast_text[:100]}{'...' if len(broadcast_text) > 100 else ''}\n"
        f"Получателей: {len(users)}\n\n"
        f"Начать рассылку?",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Да", callback_data="broadcast_confirm"),
             InlineKeyboardButton("❌ Нет", callback_data="broadcast_cancel")]
        ])
    )
    
    context.user_data['broadcast_data'] = {'text': broadcast_text, 'users': users}

# === ОБРАБОТКА РАССЫЛКИ ===
@catch_errors
async def broadcast_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    if query.from_user.id != ADMIN_ID:
        return
    
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
                logger.warning(f"❌ Не удалось отправить {uid}: {e}")
            
            if (i + 1) % 10 == 0:
                await query.message.edit_text(
                    f"📤 Рассылка в процессе...\n"
                    f"Обработано: {i + 1}/{len(users)}\n"
                    f"✅ Успешно: {success}\n"
                    f"❌ Ошибок: {failed}"
                )
            
            await asyncio.sleep(0.05)
        
        result = f"✅ Рассылка завершена!\n\n✅ Успешно: {success}\n❌ Ошибок: {failed}"
        await query.message.edit_text(result)
        context.user_data.pop('broadcast_data', None)

# === КОМАНДА ЛОГОВ ===
@catch_errors
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
@catch_errors
async def check_sheets(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    
    if worksheet:
        users_count = len(get_all_users_from_sheets())
        blacklist_count = len(blacklist)
        ai_status = "✅ Работает (Cloud.ru)" if ai_client else "❌ Не подключен"
        
        await update.message.reply_text(
            f"✅ Google Sheets подключен\n"
            f"📊 Всего клиентов: {users_count}\n"
            f"🚫 В черном списке: {blacklist_count}\n"
            f"🤖 AI статус: {ai_status}\n"
            f"📍 Модель: {AI_MODEL}"
        )
    else:
        await update.message.reply_text("❌ Google Sheets не подключен")

# === ОТВЕТЫ АДМИНА ===
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
    
    current_status = get_client_status(user.id)
    log_to_sheets(user.id, user.username, user.first_name, f"[{media_type}]", status=current_status)
    
    await message.forward(chat_id=GROUP_ID, message_thread_id=topic_id)
    await message.reply_text("✅ Файл получен!")

# === СТАТИСТИКА ===
@catch_errors
async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
        
        total_clients = len(set(row[1] for row in all_data[1:] if len(row) > 1))
        
        status_counts = {status: 0 for status in CLIENT_STATUSES}
        last_statuses = {}
        
        for row in reversed(all_data[1:]):
            if len(row) >= 6 and row[1] not in last_statuses:
                last_statuses[row[1]] = row[5]
        
        for status in last_statuses.values():
            if status in status_counts:
                status_counts[status] += 1
        
        today = datetime.now().date()
        week_ago = today - timedelta(days=7)
        month_ago = today - timedelta(days=30)
        
        today_count = 0
        week_count = 0
        month_count = 0
        
        for row in all_data[1:]:
            if len(row) >= 1 and row[0]:
                try:
                    msg_date = datetime.strptime(row[0].split()[0], "%Y-%m-%d").date()
                    if msg_date == today:
                        today_count += 1
                    if msg_date >= week_ago:
                        week_count += 1
                    if msg_date >= month_ago:
                        month_count += 1
                except:
                    continue
        
        text = f"📊 СТАТИСТИКА\n\n"
        text += f"👥 Всего клиентов: {total_clients}\n\n"
        text += "По статусам:\n"
        for status in CLIENT_STATUSES:
            count = status_counts.get(status, 0)
            text += f"{status}: {count}\n"
        
        text += f"\n📅 Активность:\n"
        text += f"• За сегодня: {today_count} сообщ.\n"
        text += f"• За неделю: {week_count} сообщ.\n"
        text += f"• За месяц: {month_count} сообщ.\n"
        
        active_today = set()
        for row in all_data[1:]:
            if len(row) >= 2 and row[0]:
                try:
                    msg_date = datetime.strptime(row[0].split()[0], "%Y-%m-%d").date()
                    if msg_date == today:
                        active_today.add(row[1])
                except:
                    continue
        
        text += f"\n🔥 Активных сегодня: {len(active_today)} клиентов"
        
        await update.message.reply_text(text)
        
    except Exception as e:
        logger.error(f"❌ Ошибка статистики: {e}")
        await update.message.reply_text("❌ Ошибка при получении статистики")

# === ЗАМЕТКИ ===
@catch_errors
async def note_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    
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
                await context.bot.send_message(
                    chat_id=GROUP_ID,
                    message_thread_id=topic_id,
                    text=f"📝 Новая заметка: {note_text}"
                )
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
                    await context.bot.send_message(
                        chat_id=GROUP_ID,
                        message_thread_id=topic_id,
                        text=f"📝 Новая заметка: {note_text}"
                    )
            else:
                await message.reply_text("❌ Ошибка при добавлении заметки")
        except ValueError:
            await message.reply_text("❌ Неверный формат. Используй: /note ID текст")
    else:
        await message.reply_text(
            "📝 Как добавить заметку:\n"
            "1. Ответь на сообщение клиента: /note текст\n"
            "2. Или напиши: /note ID текст"
        )

# === ПОИСК ===
@catch_errors
async def search_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    
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

# === БЛОКИРОВКА ===
@catch_errors
async def ban_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    
    if not context.args:
        await update.message.reply_text("❌ Используй: /ban ID [причина]")
        return
    
    try:
        user_id = int(context.args[0])
        reason = " ".join(context.args[1:]) if len(context.args) > 1 else "Без причины"
        
        blacklist.add(user_id)
        add_note_to_client(user_id, f"🚫 ЗАБЛОКИРОВАН. Причина: {reason}")
        update_client_status(user_id, "Негатив/Отказ")
        
        if user_id in user_topics:
            topic_id = user_topics[user_id]
            await context.bot.send_message(
                chat_id=GROUP_ID,
                message_thread_id=topic_id,
                text=f"🚫 Клиент заблокирован\nПричина: {reason}"
            )
        
        await update.message.reply_text(f"✅ Клиент {user_id} заблокирован")
        
    except ValueError:
        await update.message.reply_text("❌ Неверный ID")

@catch_errors
async def unban_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    
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
                await context.bot.send_message(
                    chat_id=GROUP_ID,
                    message_thread_id=topic_id,
                    text="✅ Клиент разблокирован"
                )
            
            await update.message.reply_text(f"✅ Клиент {user_id} разблокирован")
        else:
            await update.message.reply_text(f"ℹ️ Клиент {user_id} не в черном списке")
        
    except ValueError:
        await update.message.reply_text("❌ Неверный ID")

@catch_errors
async def blacklist_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    
    if not blacklist:
        await update.message.reply_text("📭 Черный список пуст")
        return
    
    text = "🚫 ЧЕРНЫЙ СПИСОК\n\n"
    
    for user_id in blacklist:
        name = "Неизвестно"
        if worksheet:
            try:
                all_data = worksheet.get_all_values()
                for row in reversed(all_data[1:]):
                    if len(row) > 3 and row[1] == str(user_id):
                        name = row[3] if row[3] else row[2] if row[2] else "Неизвестно"
                        break
            except:
                pass
        
        text += f"• {user_id} - {name}\n"
    
    await update.message.reply_text(text)

# === ПРОВЕРКА НЕАКТИВНЫХ КЛИЕНТОВ ===
async def check_inactive_clients(context: ContextTypes.DEFAULT_TYPE):
    if not worksheet:
        return
    
    try:
        all_data = worksheet.get_all_values()
        if len(all_data) < 2:
            return
        
        client_data = {}
        for row in all_data[1:]:
            if len(row) >= 6 and row[1]:
                user_id = row[1]
                client_data[user_id] = {
                    'status': row[5] if len(row) > 5 else "Новый",
                    'date': row[0] if len(row) > 0 else None
                }
        
        today = datetime.now().date()
        changed_count = 0
        
        for user_id, data in client_data.items():
            if data['status'] in ["Передан руководителю 👤", "Негатив/Отказ", "Нецелевой"]:
                continue
            
            if data['date']:
                try:
                    last_date = datetime.strptime(data['date'].split()[0], "%Y-%m-%d").date()
                    days_inactive = (today - last_date).days
                    
                    if days_inactive >= 3:
                        if update_client_status(int(user_id), "Нецелевой"):
                            changed_count += 1
                except:
                    continue
        
        if changed_count > 0:
            logger.info(f"✅ Автостатус: обновлено {changed_count} клиентов")
            
    except Exception as e:
        logger.error(f"❌ Ошибка проверки неактивных клиентов: {e}")

# === ЕЖЕДНЕВНАЯ СТАТИСТИКА ===
async def daily_stats(context: ContextTypes.DEFAULT_TYPE):
    if not worksheet:
        return
    
    try:
        all_data = worksheet.get_all_values()
        if len(all_data) < 2:
            return
        
        yesterday = (datetime.now() - timedelta(days=1)).date()
        
        new_clients_yesterday = set()
        messages_yesterday = 0
        active_clients_yesterday = set()
        
        for row in all_data[1:]:
            if len(row) >= 2 and row[0] and row[1]:
                try:
                    msg_date = datetime.strptime(row[0].split()[0], "%Y-%m-%d").date()
                    if msg_date == yesterday:
                        messages_yesterday += 1
                        active_clients_yesterday.add(row[1])
                        
                        if is_first_message(int(row[1])):
                            new_clients_yesterday.add(row[1])
                except:
                    continue
        
        last_statuses = {}
        for row in reversed(all_data[1:]):
            if len(row) >= 6 and row[1] and row[1] not in last_statuses:
                last_statuses[row[1]] = row[5]
        
        status_counts = {status: 0 for status in CLIENT_STATUSES}
        for status in last_statuses.values():
            if status in status_counts:
                status_counts[status] += 1
        
        text = f"📊 ЕЖЕДНЕВНАЯ СТАТИСТИКА\n\n"
        text += f"📅 За вчера ({yesterday.strftime('%d.%m.%Y')}):\n"
        text += f"• Новых клиентов: {len(new_clients_yesterday)}\n"
        text += f"• Всего сообщений: {messages_yesterday}\n"
        text += f"• Активных клиентов: {len(active_clients_yesterday)}\n\n"
        
        text += "Текущие статусы:\n"
        for status in CLIENT_STATUSES:
            count = status_counts.get(status, 0)
            text += f"{status}: {count}\n"
        
        text += f"\n👥 Всего клиентов: {len(last_statuses)}"
        
        await context.bot.send_message(chat_id=ADMIN_ID, text=text)
        logger.info("✅ Ежедневная статистика отправлена")
        
    except Exception as e:
        logger.error(f"❌ Ошибка отправки статистики: {e}")

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
    
    app.add_handler(CallbackQueryHandler(button_callback, pattern="^status_"))
    app.add_handler(CallbackQueryHandler(broadcast_callback, pattern="^broadcast_"))
    
    app.add_handler(MessageHandler(
        filters.Chat(GROUP_ID) & filters.TEXT & ~filters.COMMAND, 
        handle_admin_reply_in_topic
    ))
    
    app.add_handler(MessageHandler(
        ~filters.Chat(ADMIN_ID) & (filters.PHOTO | filters.VIDEO | filters.Document.ALL | filters.VOICE), 
        handle_media
    ))
    
    app.add_handler(MessageHandler(
        ~filters.Chat(ADMIN_ID) & filters.TEXT & ~filters.COMMAND, 
        handle_client_message
    ))
    
    job_queue = app.job_queue
    if job_queue:
        job_queue.run_daily(daily_stats, time=datetime.strptime("09:00", "%H:%M").time())
        job_queue.run_repeating(check_inactive_clients, interval=3600, first=10)
        logger.info("⏰ Планировщик задач запущен")
    
    app.run_polling()

if __name__ == "__main__":
    logger.info("🔄 Запуск health check сервера")
    health_thread = threading.Thread(target=run_health_server, daemon=True)
    health_thread.start()
    
    logger.info("🔄 Запуск main()")
    main()
