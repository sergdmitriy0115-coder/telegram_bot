import os
import json
import logging
import threading
import re
import asyncio
from datetime import datetime, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, CallbackQueryHandler
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# --- НАСТРОЙКИ ---
BOT_TOKEN = os.environ.get('TELEGRAM_TOKEN')
ADMIN_ID = 7675037573  # ТВОЙ ID
GROUP_ID = -1003743707530  # ID ТВОЕЙ ГРУППЫ С ТЕМАМИ
LOG_FILE = "logs.txt"

# --- НАСТРОЙКИ GOOGLE SHEETS ---
SPREADSHEET_ID = "15vlEZ0Q6OmQh51DsA9B_fgiLwed12ekroz1aeWsgXVI"
WORKSHEET_NAME = "Логи клиентов"

# Статусы для клиентов
CLIENT_STATUSES = [
    "1️⃣ Новый",
    "2️⃣ Думает", 
    "3️⃣ В работе",
    "4️⃣ Завершён",
    "5️⃣ Отказ",
    "6️⃣ Удалил переписку"
]

# Хранилища
user_topics = {}

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

# === ФОРМАТИРОВАНИЕ ТАБЛИЦЫ ===
def format_worksheet(worksheet):
    """Делает таблицу красивой: Arial 11, жирные границы, авторасширение"""
    try:
        sheet_id = worksheet.id
        
        requests = [
            # Шрифт Arial 11 для всей таблицы
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 0,
                        "endRowIndex": worksheet.row_count
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "textFormat": {
                                "fontFamily": "Arial",
                                "fontSize": 11
                            }
                        }
                    },
                    "fields": "userEnteredFormat.textFormat(fontFamily,fontSize)"
                }
            },
            # Жирный шрифт и заливка для заголовков
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 0,
                        "endRowIndex": 1
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "textFormat": {
                                "bold": True,
                                "fontFamily": "Arial",
                                "fontSize": 11
                            },
                            "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9},
                            "horizontalAlignment": "CENTER"
                        }
                    },
                    "fields": "userEnteredFormat(textFormat,backgroundColor,horizontalAlignment)"
                }
            },
            # Авто-ширина для всех колонок
            {
                "autoResizeDimensions": {
                    "dimensions": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": 0,
                        "endIndex": 7  # 7 колонок (добавили Заметки)
                    }
                }
            },
            # Жирные границы
            {
                "updateBorders": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 0,
                        "endRowIndex": worksheet.row_count,
                        "startColumnIndex": 0,
                        "endColumnIndex": 7
                    },
                    "top": {"style": "SOLID_MEDIUM", "color": {"red": 0, "green": 0, "blue": 0}},
                    "bottom": {"style": "SOLID_MEDIUM", "color": {"red": 0, "green": 0, "blue": 0}},
                    "left": {"style": "SOLID_MEDIUM", "color": {"red": 0, "green": 0, "blue": 0}},
                    "right": {"style": "SOLID_MEDIUM", "color": {"red": 0, "green": 0, "blue": 0}},
                    "innerHorizontal": {"style": "SOLID_MEDIUM", "color": {"red": 0, "green": 0, "blue": 0}},
                    "innerVertical": {"style": "SOLID_MEDIUM", "color": {"red": 0, "green": 0, "blue": 0}}
                }
            },
            # Фиксация заголовков
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
        
        # Цветные плашки для статусов
        status_colors = [
            {"red": 0.8, "green": 0.9, "blue": 1.0},   # 1️⃣ Новый
            {"red": 1.0, "green": 1.0, "blue": 0.7},   # 2️⃣ Думает
            {"red": 0.7, "green": 1.0, "blue": 0.7},   # 3️⃣ В работе
            {"red": 0.9, "green": 0.9, "blue": 0.9},   # 4️⃣ Завершён
            {"red": 1.0, "green": 0.7, "blue": 0.7},   # 5️⃣ Отказ
            {"red": 0.6, "green": 0.6, "blue": 0.6}    # 6️⃣ Удалил
        ]
        
        for i, status in enumerate(CLIENT_STATUSES):
            requests.append({
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": [{
                            "sheetId": sheet_id,
                            "startRowIndex": 1,
                            "startColumnIndex": 5,  # колонка F (Status)
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
            worksheet.update_cell(last_row_index, 6, new_status)  # колонка F
            logger.info(f"✅ Статус для user {user_id} обновлён на {new_status}")
            return True
        return False
    except Exception as e:
        logger.error(f"❌ Ошибка обновления статуса: {e}")
        return False

# === ДОБАВЛЕНИЕ ЗАМЕТКИ ===
def add_note_to_client(user_id, note):
    """Добавляет заметку к клиенту в колонку G"""
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
            # Получаем текущую заметку
            current_note = ""
            if len(all_data[last_row_index-1]) >= 7:
                current_note = all_data[last_row_index-1][6]
            
            # Добавляем новую заметку с датой
            timestamp = datetime.now().strftime("%d.%m %H:%M")
            new_note = f"{current_note}\n[{timestamp}] {note}".strip()
            
            worksheet.update_cell(last_row_index, 7, new_note)  # колонка G
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
def log_to_sheets(user_id, username, first_name, message_text, status="1️⃣ Новый"):
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
                ""  # пустая колонка для заметок
            ]
            worksheet.append_row(row)
            logger.info(f"✅ Новая запись для user {user_id}")
            
            # Форматируем при первом добавлении
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
        return "1️⃣ Новый"
    
    try:
        all_data = worksheet.get_all_values()
        if len(all_data) < 2:
            return "1️⃣ Новый"
        
        for i in range(len(all_data)-1, 0, -1):
            if len(all_data[i]) > 1 and all_data[i][1] == str(user_id):
                if len(all_data[i]) >= 6:
                    return all_data[i][5]
                return "1️⃣ Новый"
        return "1️⃣ Новый"
    except Exception as e:
        logger.error(f"❌ Ошибка получения статуса: {e}")
        return "1️⃣ Новый"

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

# === СТАТИСТИКА ===
async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает статистику по клиентам"""
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
        
        # Общая статистика
        total_clients = len(set(row[1] for row in all_data[1:] if len(row) > 1))
        
        # Статистика по статусам
        status_counts = {status: 0 for status in CLIENT_STATUSES}
        last_statuses = {}
        
        # Ищем последний статус для каждого клиента
        for row in reversed(all_data[1:]):
            if len(row) >= 6 and row[1] not in last_statuses:
                last_statuses[row[1]] = row[5]
        
        for status in last_statuses.values():
            if status in status_counts:
                status_counts[status] += 1
        
        # Статистика за сегодня/неделю/месяц
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
        
        # Формируем ответ
        text = "📊 **СТАТИСТИКА**\n\n"
        text += f"👥 **Всего клиентов:** {total_clients}\n\n"
        text += "**По статусам:**\n"
        for status in CLIENT_STATUSES:
            count = status_counts.get(status, 0)
            text += f"{status}: {count}\n"
        
        text += f"\n📅 **Активность:**\n"
        text += f"• За сегодня: {today_count} сообщ.\n"
        text += f"• За неделю: {week_count} сообщ.\n"
        text += f"• За месяц: {month_count} сообщ.\n"
        
        # Активные клиенты (писали сегодня)
        active_today = set()
        for row in all_data[1:]:
            if len(row) >= 2 and row[0]:
                try:
                    msg_date = datetime.strptime(row[0].split()[0], "%Y-%m-%d").date()
                    if msg_date == today:
                        active_today.add(row[1])
                except:
                    continue
        
        text += f"\n🔥 **Активных сегодня:** {len(active_today)} клиентов"
        
        await update.message.reply_text(text)
        
    except Exception as e:
        logger.error(f"❌ Ошибка статистики: {e}")
        await update.message.reply_text("❌ Ошибка при получении статистики")

# === ЗАМЕТКИ ===
async def note_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Добавляет заметку к клиенту /note ID текст или через Reply"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    message = update.message
    
    # Вариант 1: через Reply
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
            
            # Отправляем уведомление в тему
            if client_id in user_topics:
                topic_id = user_topics[client_id]
                await context.bot.send_message(
                    chat_id=GROUP_ID,
                    message_thread_id=topic_id,
                    text=f"📝 **Новая заметка:**\n{note_text}"
                )
        else:
            await message.reply_text("❌ Ошибка при добавлении заметки")
    
    # Вариант 2: /note ID текст
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
                        text=f"📝 **Новая заметка:**\n{note_text}"
                    )
            else:
                await message.reply_text("❌ Ошибка при добавлении заметки")
        except ValueError:
            await message.reply_text("❌ Неверный формат. Используй: /note ID текст")
    else:
        await message.reply_text(
            "📝 **Как добавить заметку:**\n"
            "1. Ответь на сообщение клиента: `/note текст`\n"
            "2. Или напиши: `/note ID текст`"
        )

# === ПОИСК ===
async def search_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ищет по сообщениям клиентов"""
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
        
        # Разбиваем на части если много результатов
        text = f"🔍 **Найдено {len(results)} совпадений:**\n\n"
        
        if len(results) > 20:
            results = results[:20]
            text += "(показаны первые 20)\n\n"
        
        for i, res in enumerate(results, 1):
            text += f"{i}. {res}\n"
        
        # Разбиваем длинные сообщения
        if len(text) > 4000:
            parts = [text[i:i+4000] for i in range(0, len(text), 4000)]
            for part in parts:
                await update.message.reply_text(part)
        else:
            await update.message.reply_text(text)
        
    except Exception as e:
        logger.error(f"❌ Ошибка поиска: {e}")
        await update.message.reply_text("❌ Ошибка при поиске")

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
            
            # Проверяем заголовки
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

# Инициализация
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
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    save_message(user.id, user.username or "нет", user.first_name or "нет", "/start")
    
    topic_id = await get_or_create_topic(context, user.id, user.username, user.first_name)
    if not topic_id:
        await update.message.reply_text("❌ Ошибка")
        return
    
    log_to_sheets(user.id, user.username, user.first_name, "/start", status="1️⃣ Новый")
    
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
    
    current_status = get_client_status(user_id)
    current_note = get_client_note(user_id)
    
    log_to_sheets(user_id, user.username, user.first_name, message.text, status=current_status)
    
    note_text = f"\n\n📝 **Заметка:** {current_note}" if current_note else ""
    await context.bot.send_message(
        chat_id=GROUP_ID, 
        message_thread_id=topic_id, 
        text=f"👤 **Клиент:**\n{message.text}\n\n**Статус:** {current_status}{note_text}"
    )
    
    # Автоответы
    if "цен" in message.text.lower() or "стоимост" in message.text.lower():
        await message.reply_text("💰 Подробную информацию по ценам можете уточнить у нашего менеджера!")
    else:
        await message.reply_text("Спасибо за сообщение! Скоро ответим.")

# === КОМАНДА СТАТУС ===
async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    
    keyboard = []
    row = []
    for i, status in enumerate(CLIENT_STATUSES):
        status_num = str(i + 1)
        row.append(InlineKeyboardButton(status, callback_data=f"status_{status_num}"))
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
        status_num = data.replace("status_", "")
        selected_status = CLIENT_STATUSES[int(status_num) - 1]
        
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
                            text=f"📊 **Статус обновлён**\nНовый статус: {selected_status}"
                        )
                else:
                    await query.edit_message_text("❌ Ошибка обновления статуса")
            else:
                await query.edit_message_text("❌ Не могу найти ID клиента")
        else:
            await query.edit_message_text("❌ Используй Reply на сообщение клиента")

# === РАССЫЛКА ===
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
    if not users:
        await message.reply_text("📭 Нет пользователей")
        return
    
    await message.reply_text(
        f"📊 **Подтверждение рассылки**\n\n"
        f"Сообщение: `{broadcast_text[:100]}{'...' if len(broadcast_text) > 100 else ''}`\n"
        f"Получателей: {len(users)}\n\n"
        f"Начать рассылку?",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Да", callback_data="broadcast_confirm"),
             InlineKeyboardButton("❌ Нет", callback_data="broadcast_cancel")]
        ])
    )
    
    context.user_data['broadcast_data'] = {'text': broadcast_text, 'users': users}

# === ОБРАБОТКА РАССЫЛКИ ===
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
        
        await query.edit_message_text("📤 **Начинаю рассылку...**\nЭто может занять некоторое время.")
        
        text = data['text']
        users = data['users']
        success = 0
        failed = 0
        failed_list = []
        
        for i, uid in enumerate(users):
            try:
                await context.bot.send_message(chat_id=uid, text=text)
                success += 1
            except Exception as e:
                failed += 1
                failed_list.append(str(uid))
            
            if (i + 1) % 10 == 0:
                await query.message.edit_text(
                    f"📤 **Рассылка в процессе...**\n"
                    f"Обработано: {i + 1}/{len(users)}\n"
                    f"✅ Успешно: {success}\n"
                    f"❌ Ошибок: {failed}"
                )
            
            await asyncio.sleep(0.05)
        
        result = f"✅ **Рассылка завершена!**\n\n✅ Успешно: {success}\n❌ Ошибок: {failed}"
        if failed_list:
            result += f"\n\n❌ Не удалось отправить: {', '.join(failed_list[:5])}"
            if len(failed_list) > 5:
                result += f" и ещё {len(failed_list) - 5}"
        
        await query.message.edit_text(result)
        context.user_data.pop('broadcast_data', None)

# === КОМАНДА ЛОГОВ ===
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
        users_count = len(get_all_users_from_sheets())
        await update.message.reply_text(
            f"✅ **Google Sheets подключен**\n"
            f"📊 Всего клиентов: {users_count}\n"
            f"📋 Название листа: {WORKSHEET_NAME}"
        )
    else:
        await update.message.reply_text("❌ Google Sheets не подключен")

# === ОТВЕТЫ АДМИНА ===
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
    
    current_status = get_client_status(user.id)
    log_to_sheets(user.id, user.username, user.first_name, f"[{media_type}]", status=current_status)
    
    await message.forward(chat_id=GROUP_ID, message_thread_id=topic_id)
    await message.reply_text("✅ Файл получен!")

# === ГЛАВНАЯ ===
def main():
    app = Application.builder().token(BOT_TOKEN).build()
    
    # Основные команды
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("admin_logs", admin_logs))
    app.add_handler(CommandHandler("check_sheets", check_sheets))
    app.add_handler(CommandHandler("status", status_command))
    app.add_handler(CommandHandler("broadcast", broadcast_command))
    
    # Новые команды
    app.add_handler(CommandHandler("stats", stats_command))
    app.add_handler(CommandHandler("note", note_command))
    app.add_handler(CommandHandler("search", search_command))
    
    # Обработчики кнопок
    app.add_handler(CallbackQueryHandler(button_callback, pattern="^status_"))
    app.add_handler(CallbackQueryHandler(broadcast_callback, pattern="^broadcast_"))
    
    # Обработчики сообщений
    app.add_handler(MessageHandler(filters.Chat(GROUP_ID) & filters.TEXT & ~filters.COMMAND, handle_admin_reply_in_topic))
    app.add_handler(MessageHandler(~filters.Chat(ADMIN_ID) & (filters.PHOTO | filters.VIDEO | filters.Document.ALL | filters.VOICE), handle_media))
    app.add_handler(MessageHandler(~filters.Chat(ADMIN_ID) & filters.TEXT & ~filters.COMMAND, handle_client_message))
    
    app.run_polling()

if __name__ == "__main__":
    health_thread = threading.Thread(target=run_health_server, daemon=True)
    health_thread.start()
    main()
