import os
import json
import logging
import threading
import re
import asyncio
import time
from datetime import datetime
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

# === ФОРМАТИРОВАНИЕ ТАБЛИЦЫ (ИСПРАВЛЕННОЕ) ===
def format_worksheet(worksheet):
    """Делает таблицу красивой: заголовки, границы, выравнивание"""
    try:
        # Получаем ID листа
        sheet_id = worksheet.id
        
        # Настраиваем форматирование через batch_update
        requests = [
            # 1. Жирный шрифт и заливка для заголовков
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
                            "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9},
                            "horizontalAlignment": "CENTER"
                        }
                    },
                    "fields": "userEnteredFormat(textFormat,backgroundColor,horizontalAlignment)"
                }
            },
            # 2. Авто-ширина для всех колонок
            {
                "autoResizeDimensions": {
                    "dimensions": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": 0,
                        "endIndex": 8
                    }
                }
            },
            # 3. Границы для всех ячеек с данными
            {
                "updateBorders": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 0,
                        "endRowIndex": worksheet.row_count,
                        "startColumnIndex": 0,
                        "endColumnIndex": 8
                    },
                    "top": {"style": "SOLID", "color": {"red": 0.8, "green": 0.8, "blue": 0.8}},
                    "bottom": {"style": "SOLID", "color": {"red": 0.8, "green": 0.8, "blue": 0.8}},
                    "left": {"style": "SOLID", "color": {"red": 0.8, "green": 0.8, "blue": 0.8}},
                    "right": {"style": "SOLID", "color": {"red": 0.8, "green": 0.8, "blue": 0.8}},
                    "innerHorizontal": {"style": "SOLID", "color": {"red": 0.8, "green": 0.8, "blue": 0.8}},
                    "innerVertical": {"style": "SOLID", "color": {"red": 0.8, "green": 0.8, "blue": 0.8}}
                }
            },
            # 4. Фиксация заголовков (первая строка всегда видна)
            {
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": sheet_id,
                        "gridProperties": {
                            "frozenRowCount": 1
                        }
                    },
                    "fields": "gridProperties.frozenRowCount"
                }
            }
        ]
        
        # Добавляем условное форматирование для статусов
        status_colors = [
            {"red": 0.9, "green": 0.9, "blue": 1.0},  # 1️⃣ Новый - голубой
            {"red": 1.0, "green": 1.0, "blue": 0.8},  # 2️⃣ Думает - желтый
            {"red": 0.8, "green": 1.0, "blue": 0.8},  # 3️⃣ В работе - зеленый
            {"red": 0.8, "green": 0.8, "blue": 0.8},  # 4️⃣ Завершён - серый
            {"red": 1.0, "green": 0.8, "blue": 0.8},  # 5️⃣ Отказ - красный
            {"red": 0.7, "green": 0.7, "blue": 0.7}   # 6️⃣ Удалил - темно-серый
        ]
        
        for i, status in enumerate(CLIENT_STATUSES):
            requests.append({
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": [{
                            "sheetId": sheet_id,
                            "startRowIndex": 1,
                            "startColumnIndex": 7,
                            "endColumnIndex": 8
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
        
        # Применяем все изменения
        worksheet.spreadsheet.batch_update({"requests": requests})
        logger.info("✅ Таблица отформатирована: заголовки, границы, цвета статусов")
        
    except Exception as e:
        logger.error(f"❌ Ошибка форматирования таблицы: {e}")

# === ПРОВЕРКА НА ДУБЛИКАТЫ ===
def is_first_message(user_id):
    """Проверяет, есть ли уже запись для этого пользователя"""
    if not worksheet:
        return True
    
    try:
        # Получаем все User ID из колонки B (индекс 1)
        user_ids = worksheet.col_values(2)[1:]
        return str(user_id) not in user_ids
        
    except Exception as e:
        logger.error(f"❌ Ошибка проверки дубликатов: {e}")
        return True

# === ОБНОВЛЕНИЕ СТАТУСА БЕЗ НОВОЙ СТРОКИ ===
def update_client_status(user_id, new_status):
    """Обновляет статус клиента (без создания новой строки)"""
    if not worksheet:
        logger.warning("⚠️ Невозможно обновить статус: worksheet не инициализирован")
        return False
    
    try:
        all_data = worksheet.get_all_values()
        if len(all_data) < 2:
            return False
        
        # Ищем последнюю строку с этим user_id (с конца)
        last_row_index = None
        for i in range(len(all_data)-1, 0, -1):
            if len(all_data[i]) > 1 and all_data[i][1] == str(user_id):
                last_row_index = i + 1
                break
        
        if last_row_index:
            worksheet.update_cell(last_row_index, 8, new_status)
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            worksheet.update_cell(last_row_index, 1, current_time)
            
            logger.info(f"✅ Статус для user {user_id} обновлён на {new_status} (без дубликата)")
            return True
        else:
            logger.warning(f"⚠️ Не найдена запись для user {user_id}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Ошибка обновления статуса: {e}")
        return False

# === ЗАПИСЬ В ТАБЛИЦУ (БЕЗ ДУБЛЕЙ) ===
def log_to_sheets(user_id, username, first_name, message_text, stage=0, topic_id=None, status="1️⃣ Новый"):
    """Записывает данные в таблицу, но только первое сообщение создаёт новую строку"""
    if not worksheet:
        logger.warning(f"⚠️ Пропускаем запись в таблицу для user {user_id}: worksheet не инициализирован")
        return
    
    try:
        if is_first_message(user_id):
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            row = [
                timestamp, 
                str(user_id), 
                username or "нет", 
                first_name or "нет", 
                message_text, 
                str(stage), 
                str(topic_id) if topic_id else "", 
                status
            ]
            worksheet.append_row(row)
            logger.info(f"✅ Новая запись в таблице для user {user_id}")
            
            # При первом сообщении форматируем таблицу (если ещё не отформатирована)
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
            logger.info(f"✅ Обновлён статус для user {user_id}, новая строка не создана")
            
    except Exception as e:
        logger.error(f"❌ Ошибка записи в таблицу для user {user_id}: {e}")

# === ИНИЦИАЛИЗАЦИЯ GOOGLE SHEETS ===
def init_google_sheets():
    """Подключается к Google Sheets и возвращает объект листа"""
    logger.info("🔄 Начинаем подключение к Google Sheets...")
    
    try:
        creds_json = os.environ.get('GOOGLE_CREDS_JSON')
        if not creds_json:
            logger.error("❌ GOOGLE_CREDS_JSON не найден в переменных окружения")
            return None
        
        logger.info("✅ GOOGLE_CREDS_JSON найден, пробуем распарсить JSON...")
        
        try:
            creds_dict = json.loads(creds_json)
            logger.info(f"✅ JSON распаршен успешно. client_email: {creds_dict.get('client_email', 'не найден')}")
        except json.JSONDecodeError as e:
            logger.error(f"❌ Ошибка парсинга JSON: {e}")
            return None
        
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        logger.info(f"🔄 Scope настроен: {scope}")
        
        try:
            creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
            logger.info("✅ Credentials созданы успешно")
        except Exception as e:
            logger.error(f"❌ Ошибка создания credentials: {e}")
            return None
        
        try:
            client = gspread.authorize(creds)
            logger.info("✅ Авторизация в gspread успешна")
        except Exception as e:
            logger.error(f"❌ Ошибка авторизации в gspread: {e}")
            return None
        
        logger.info(f"🔄 Пробуем открыть таблицу с ID: {SPREADSHEET_ID}")
        try:
            spreadsheet = client.open_by_key(SPREADSHEET_ID)
            logger.info(f"✅ Таблица открыта успешно. Название: {spreadsheet.title}")
        except gspread.exceptions.SpreadsheetNotFound:
            logger.error(f"❌ Таблица с ID {SPREADSHEET_ID} не найдена")
            return None
        except gspread.exceptions.APIError as e:
            logger.error(f"❌ Ошибка API при открытии таблицы: {e}")
            return None
        
        logger.info(f"🔄 Пробуем получить лист с названием '{WORKSHEET_NAME}'")
        try:
            worksheet = spreadsheet.worksheet(WORKSHEET_NAME)
            logger.info(f"✅ Лист '{WORKSHEET_NAME}' найден")
            
            headers = worksheet.row_values(1)
            if "Status" not in headers:
                logger.info("🔄 Добавляем колонку Status в таблицу")
                worksheet.add_cols(1)
                worksheet.update_cell(1, len(headers) + 1, "Status")
                all_rows = worksheet.get_all_values()
                for i in range(2, len(all_rows) + 1):
                    worksheet.update_cell(i, len(headers) + 1, "1️⃣ Новый")
                logger.info("✅ Колонка Status добавлена")
            
            format_worksheet(worksheet)
            
        except gspread.exceptions.WorksheetNotFound:
            logger.info(f"🔄 Лист '{WORKSHEET_NAME}' не найден, создаем новый...")
            try:
                worksheet = spreadsheet.add_worksheet(title=WORKSHEET_NAME, rows=1000, cols=20)
                headers = ["Timestamp", "User ID", "Username", "First Name", "Message", "Stage", "Topic ID", "Status"]
                worksheet.append_row(headers)
                logger.info(f"✅ Создан новый лист с заголовками, включая Status")
                format_worksheet(worksheet)
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

# === ПОЛУЧЕНИЕ СПИСКА УНИКАЛЬНЫХ ПОЛЬЗОВАТЕЛЕЙ ИЗ ТАБЛИЦЫ ===
def get_all_users_from_sheets():
    """Получает список уникальных user_id из Google Sheets"""
    if not worksheet:
        logger.warning("⚠️ Невозможно получить список пользователей: worksheet не инициализирован")
        return []
    
    try:
        all_data = worksheet.get_all_values()
        if not all_data or len(all_data) < 2:
            return []
        
        users = set()
        for row in all_data[1:]:
            if len(row) > 1 and row[1]:
                try:
                    user_id = int(row[1])
                    users.add(user_id)
                except (ValueError, TypeError):
                    continue
        
        user_list = list(users)
        logger.info(f"📊 Найдено {len(user_list)} уникальных пользователей")
        return user_list
    except Exception as e:
        logger.error(f"❌ Ошибка получения списка пользователей: {e}")
        return []

# === РАССЫЛКА СООБЩЕНИЙ ===
async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для массовой рассылки сообщений всем клиентам"""
    user_id = update.effective_user.id
    
    if user_id != ADMIN_ID:
        await update.message.reply_text("⛔ Нет прав для этой команды.")
        return
    
    message = update.message
    broadcast_text = None
    
    if context.args:
        broadcast_text = " ".join(context.args)
    elif message.reply_to_message:
        broadcast_text = message.reply_to_message.text
    
    if not broadcast_text:
        await message.reply_text(
            "❌ Укажи текст рассылки:\n"
            "`/broadcast Ваше сообщение`\n\n"
            "Или используй Reply на любое сообщение: `/broadcast`"
        )
        return
    
    users = get_all_users_from_sheets()
    if not users:
        await message.reply_text("📭 Нет пользователей для рассылки.")
        return
    
    confirm_msg = await message.reply_text(
        f"📊 **Подтверждение рассылки**\n\n"
        f"Сообщение:\n`{broadcast_text[:100]}{'...' if len(broadcast_text) > 100 else ''}`\n\n"
        f"Количество получателей: {len(users)}\n\n"
        f"Начать рассылку?",
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Да", callback_data="broadcast_confirm"),
                InlineKeyboardButton("❌ Нет", callback_data="broadcast_cancel")
            ]
        ])
    )
    
    context.user_data['broadcast_data'] = {
        'text': broadcast_text,
        'users': users,
        'confirm_msg_id': confirm_msg.message_id
    }

# === ОБРАБОТКА ПОДТВЕРЖДЕНИЯ РАССЫЛКИ ===
async def broadcast_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает подтверждение рассылки"""
    query = update.callback_query
    await query.answer()
    
    if query.from_user.id != ADMIN_ID:
        await query.edit_message_text("⛔ Нет прав")
        return
    
    data = query.data
    
    if data == "broadcast_cancel":
        await query.edit_message_text("❌ Рассылка отменена")
        return
    
    if data == "broadcast_confirm":
        broadcast_data = context.user_data.get('broadcast_data')
        if not broadcast_data:
            await query.edit_message_text("❌ Ошибка: данные рассылки не найдены")
            return
        
        await query.edit_message_text(
            "📤 **Начинаю рассылку...**\n"
            "Это может занять некоторое время."
        )
        
        text = broadcast_data['text']
        users = broadcast_data['users']
        
        success = 0
        failed = 0
        failed_list = []
        
        for i, uid in enumerate(users):
            try:
                await context.bot.send_message(chat_id=uid, text=text)
                success += 1
                logger.info(f"✅ Рассылка: отправлено пользователю {uid}")
            except Exception as e:
                failed += 1
                failed_list.append(str(uid))
                logger.warning(f"❌ Рассылка: не удалось отправить {uid}: {e}")
            
            if (i + 1) % 10 == 0:
                await query.message.edit_text(
                    f"📤 **Рассылка в процессе...**\n"
                    f"Обработано: {i + 1}/{len(users)}\n"
                    f"✅ Успешно: {success}\n"
                    f"❌ Ошибок: {failed}"
                )
            
            await asyncio.sleep(0.05)
        
        result_text = (
            f"✅ **Рассылка завершена!**\n\n"
            f"📊 Статистика:\n"
            f"• Всего получателей: {len(users)}\n"
            f"• Успешно отправлено: {success}\n"
            f"• Ошибок: {failed}"
        )
        
        if failed_list:
            result_text += f"\n\n❌ Не удалось отправить пользователям:\n" + ", ".join(failed_list[:10])
            if len(failed_list) > 10:
                result_text += f" и ещё {len(failed_list) - 10}"
        
        await query.message.edit_text(result_text)
        context.user_data.pop('broadcast_data', None)

# === ПОЛУЧИТЬ ТЕКУЩИЙ СТАТУС ===
def get_client_status(user_id):
    """Получает текущий статус клиента из Google Sheets"""
    if not worksheet:
        return "1️⃣ Новый"
    
    try:
        all_data = worksheet.get_all_values()
        if not all_data:
            return "1️⃣ Новый"
        
        for i in range(len(all_data)-1, 0, -1):
            if len(all_data[i]) > 1 and all_data[i][1] == str(user_id):
                if len(all_data[i]) >= 8:
                    return all_data[i][7]
                else:
                    return "1️⃣ Новый"
        return "1️⃣ Новый"
    except Exception as e:
        logger.error(f"❌ Ошибка получения статуса: {e}")
        return "1️⃣ Новый"

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
    logger.info(f"🔄 get_or_create_topic вызван для user {user_id}")
    
    if user_id in user_topics:
        topic_id = user_topics[user_id]
        logger.info(f"✅ Найдена существующая тема {topic_id} для user {user_id}")
        return topic_id
    
    topic_name = f"{first_name} (@{username if username else 'no_username'})"
    
    try:
        result = await context.bot.create_forum_topic(chat_id=GROUP_ID, name=topic_name[:128])
        topic_id = result.message_thread_id
        user_topics[user_id] = topic_id
        
        current_status = get_client_status(user_id)
        
        await context.bot.send_message(
            chat_id=GROUP_ID, message_thread_id=topic_id,
            text=f"🆕 **Новый клиент!**\n"
                 f"Имя: {first_name}\n"
                 f"Username: @{username if username else 'нет'}\n"
                 f"ID: `{user_id}`\n"
                 f"Статус: {current_status}\n\n"
                 f"Используй /status для смены статуса"
        )
        logger.info(f"✅ Создана тема {topic_id} для пользователя {user_id}")
        return topic_id
        
    except Exception as e:
        logger.error(f"❌ Ошибка создания темы для user {user_id}: {e}")
        return None

# === КОМАНДА /START ===
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    logger.info(f"🚀 Команда /start от пользователя {user.id} (@{user.username})")
    
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

# === КОМАНДА ДЛЯ СМЕНЫ СТАТУСА ===
async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для смены статуса клиента /status [номер]"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    message = update.message
    
    keyboard = []
    row = []
    for i, status in enumerate(CLIENT_STATUSES):
        status_num = str(i + 1)
        button_text = status
        row.append(InlineKeyboardButton(button_text, callback_data=f"status_{status_num}"))
        if (i + 1) % 2 == 0:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    
    keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="status_cancel")])
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if message.reply_to_message:
        await message.reply_text("📊 Выбери новый статус для клиента:", reply_markup=reply_markup)
    else:
        await message.reply_text(
            "📊 Выбери статус для клиента\n\n"
            "💡 Используй Reply на сообщение клиента, чтобы выбрать статус для конкретного клиента",
            reply_markup=reply_markup
        )

# === ОБРАБОТКА НАЖАТИЙ НА КНОПКИ ===
async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает нажатия на кнопки статусов и рассылки"""
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
        selected_status = None
        for status in CLIENT_STATUSES:
            if status.startswith(f"{status_num}️⃣"):
                selected_status = status
                break
        
        if not selected_status:
            await query.edit_message_text("❌ Неверный статус")
            return
        
        if query.message.reply_to_message:
            original_text = query.message.reply_to_message.text or ""
            match = re.search(r'ID: `?(\d+)`?', original_text)
            
            if match:
                client_id = int(match.group(1))
                if update_client_status(client_id, selected_status):
                    await query.edit_message_text(
                        f"✅ Статус клиента обновлён на: {selected_status}"
                    )
                    
                    if client_id in user_topics:
                        topic_id = user_topics[client_id]
                        try:
                            await context.bot.send_message(
                                chat_id=GROUP_ID,
                                message_thread_id=topic_id,
                                text=f"📊 **Статус обновлён**\nНовый статус: {selected_status}"
                            )
                        except:
                            pass
                else:
                    await query.edit_message_text("❌ Ошибка обновления статуса")
            else:
                await query.edit_message_text("❌ Не могу найти ID клиента. Используй Reply на сообщение клиента.")
        else:
            await query.edit_message_text("❌ Используй Reply на сообщение клиента")

# === СООБЩЕНИЯ КЛИЕНТОВ ===
async def handle_client_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.message
    user_id = user.id
    
    logger.info(f"💬 Сообщение от клиента {user_id}: {message.text[:50]}...")
    
    save_message(user_id, user.username or "нет", user.first_name or "нет", message.text)
    
    topic_id = await get_or_create_topic(context, user_id, user.username, user.first_name)
    if not topic_id:
        await message.reply_text("❌ Ошибка")
        return
    
    stage = user_stage.get(user_id, 0)
    current_status = get_client_status(user_id)
    log_to_sheets(user_id, user.username, user.first_name, message.text, stage=stage, topic_id=topic_id, status=current_status)
    
    await context.bot.send_message(
        chat_id=GROUP_ID, 
        message_thread_id=topic_id, 
        text=f"👤 **Клиент (статус: {current_status}):**\n{message.text}"
    )
    
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
    
    logger.info(f"📎 Медиа от клиента {user.id}")
    
    topic_id = await get_or_create_topic(context, user.id, user.username, user.first_name)
    if not topic_id:
        await message.reply_text("❌ Ошибка")
        return
    
    media_type = "фото"
    if message.video: media_type = "видео"
    elif message.document: media_type = "документ"
    elif message.voice: media_type = "голосовое"
    
    stage = user_stage.get(user.id, 0)
    current_status = get_client_status(user.id)
    log_to_sheets(user.id, user.username, user.first_name, f"[{media_type}]", stage=stage, topic_id=topic_id, status=current_status)
    
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
    logger.info(f"📊 Команда /check_sheets от пользователя {user_id}")
    
    if user_id != ADMIN_ID:
        logger.warning(f"Пользователь {user_id} не админ, доступ запрещен")
        return
    
    if worksheet:
        users_count = len(get_all_users_from_sheets())
        await update.message.reply_text(
            f"✅ Google Sheets подключен и работает!\n"
            f"📊 Всего уникальных клиентов: {users_count}"
        )
        logger.info("✅ Ответ на /check_sheets: подключен")
    else:
        error_msg = "❌ Google Sheets не подключен. Проверь логи на Render для деталей."
        await update.message.reply_text(error_msg)
        logger.error(f"Ответ на /check_sheets: не подключен. worksheet = {worksheet}")

# === ГЛАВНАЯ ===
def main():
    logger.info("🚀 Запуск бота со статусами и рассылкой...")
    
    app = Application.builder().token(BOT_TOKEN).build()
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("admin_logs", admin_logs))
    app.add_handler(CommandHandler("check_sheets", check_sheets))
    app.add_handler(CommandHandler("status", status_command))
    app.add_handler(CommandHandler("broadcast", broadcast_command))
    
    app.add_handler(CallbackQueryHandler(button_callback, pattern="^status_"))
    app.add_handler(CallbackQueryHandler(broadcast_callback, pattern="^broadcast_"))
    
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
