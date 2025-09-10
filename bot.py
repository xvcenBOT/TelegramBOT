import telebot
import sqlite3
import string
import random
import time
import os
import re
from dotenv import load_dotenv, set_key
from telebot import types, apihelper

load_dotenv()

API_TOKEN = os.getenv('API_TOKEN')
BOT_USERNAME = os.getenv('BOT_USERNAME')
ADMIN_IDS_STR = os.getenv('ADMIN_IDS', '')
OFF_IDS_STR = os.getenv('OFF_IDS', '')
GROUP_ID = os.getenv('GROUP_ID')
TOPIC_ID = os.getenv('TOPIC_ID')

if not API_TOKEN or not BOT_USERNAME:
    raise ValueError("API_TOKEN или BOT_USERNAME не найдены в .env файле 😕")
if not GROUP_ID or not TOPIC_ID:
    raise ValueError("GROUP_ID или TOPIC_ID не найдены в .env файле 😕")

ADMIN_IDS = [int(i) for i in ADMIN_IDS_STR.split(',') if i.strip().isdigit()]
OFF_IDS = [int(i) for i in OFF_IDS_STR.split(',') if i.strip().isdigit()]
GROUP_ID = int(GROUP_ID)
TOPIC_ID = int(TOPIC_ID)

bot = telebot.TeleBot(API_TOKEN)

user_states = {}

def init_db():
    conn = sqlite3.connect('deals.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS deals (
            deal_id TEXT PRIMARY KEY,
            creator_id INTEGER NOT NULL,
            creator_username TEXT NOT NULL,
            participant_id INTEGER,
            participant_username TEXT,
            deal_type TEXT NOT NULL,
            item_links TEXT,
            currency TEXT NOT NULL,
            amount REAL NOT NULL,
            status TEXT NOT NULL,
            creation_date REAL NOT NULL
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS user_details (
            user_id INTEGER PRIMARY KEY,
            details TEXT NOT NULL
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS user_profile (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            balance REAL,
            successful_deals INTEGER,
            language TEXT,
            is_banned_from_admin INTEGER DEFAULT 0
        )
    ''')
    conn.commit()
    migrate_db(conn)
    conn.close()

def migrate_db(conn):
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(user_profile)")
    columns = [info[1] for info in cursor.fetchall()]
    if 'is_banned_from_admin' not in columns:
        cursor.execute('''
            CREATE TABLE user_profile_new (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                balance REAL,
                successful_deals INTEGER,
                language TEXT,
                is_banned_from_admin INTEGER DEFAULT 0
            )
        ''')
        cursor.execute('''
            INSERT INTO user_profile_new (user_id, username, balance, successful_deals, language)
            SELECT user_id, username, balance, successful_deals, language FROM user_profile
        ''')
        cursor.execute('DROP TABLE user_profile')
        cursor.execute('ALTER TABLE user_profile_new RENAME TO user_profile')
        conn.commit()
        print("Database migrated: Added is_banned_from_admin column.")

def generate_deal_id(length=8):
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))

def validate_links(deal_type, text):
    if deal_type in ['gift', 'channel', 'nft']:
        lines = text.strip().split('\n')
        url_pattern = re.compile(r'^(https?://[^\s/$.?#].[^\s]*$|t\.me/[^\s]+)$')
        for line in lines:
            if not line.strip() or not url_pattern.match(line.strip()):
                return False, "⚠ Каждая ссылка должна начинаться с https:// или t.me/ и быть корректной."
        return True, ""
    elif deal_type == 'stars':
        try:
            num = int(text.strip())
            if num <= 0:
                return False, "⚠ Количество Stars должно быть положительным числом."
            return True, ""
        except ValueError:
            return False, "⚠ Введите корректное число для количества Stars."
    return True, ""

def get_main_menu_keyboard():
    keyboard = types.InlineKeyboardMarkup(row_width=2)
    create_deal_btn = types.InlineKeyboardButton(text="🌟 Создать сделку", callback_data="create_deal")
    profile_btn = types.InlineKeyboardButton(text="👤 Мой профиль", callback_data="my_profile")
    details_btn = types.InlineKeyboardButton(text="💳 Мои реквизиты", callback_data="my_details")
    support_btn = types.InlineKeyboardButton(text="📞 Поддержка", callback_data="support")
    language_btn = types.InlineKeyboardButton(text="🌐 Сменить язык", callback_data="change_language")
    keyboard.add(create_deal_btn, profile_btn, details_btn, support_btn, language_btn)
    return keyboard

def get_deal_type_keyboard():
    keyboard = types.InlineKeyboardMarkup(row_width=1)
    gift_btn = types.InlineKeyboardButton(text="🎁 Подарок", callback_data="deal_type_gift")
    channel_btn = types.InlineKeyboardButton(text="📢 Канал/Чат", callback_data="deal_type_channel")
    stars_btn = types.InlineKeyboardButton(text="⭐ Stars", callback_data="deal_type_stars")
    nft_btn = types.InlineKeyboardButton(text="🔹 NFT Username/+888", callback_data="deal_type_nft")
    back_btn = types.InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")
    keyboard.add(gift_btn, channel_btn, stars_btn, nft_btn, back_btn)
    return keyboard

def get_notice_keyboard(deal_type):
    keyboard = types.InlineKeyboardMarkup(row_width=1)
    read_btn = types.InlineKeyboardButton(text="✅ Я прочитал(а)", callback_data=f"notice_read_{deal_type}")
    back_btn = types.InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")
    keyboard.add(read_btn, back_btn)
    return keyboard

def get_links_keyboard(deal_type):
    keyboard = types.InlineKeyboardMarkup(row_width=1)
    back_btn = types.InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")
    keyboard.add(back_btn)
    return keyboard

def get_currency_keyboard():
    keyboard = types.InlineKeyboardMarkup(row_width=4)
    buttons = [
        types.InlineKeyboardButton("🇷🇺 RUB", callback_data="currency_RUB"),
        types.InlineKeyboardButton("🇪🇺 EUR", callback_data="currency_EUR"),
        types.InlineKeyboardButton("🇺🇿 UZS", callback_data="currency_UZS"),
        types.InlineKeyboardButton("🇰🇿 KZT", callback_data="currency_KZT"),
        types.InlineKeyboardButton("🇰🇬 KGS", callback_data="currency_KGS"),
        types.InlineKeyboardButton("🇮🇩 IDR", callback_data="currency_IDR"),
        types.InlineKeyboardButton("🇺🇦 UAH", callback_data="currency_UAH"),
        types.InlineKeyboardButton("🇧🇾 BYN", callback_data="currency_BYN")
    ]
    keyboard.add(*buttons)
    keyboard.add(
        types.InlineKeyboardButton("💎 TON", callback_data="currency_TON"),
        types.InlineKeyboardButton("⭐ Stars", callback_data="currency_Stars")
    )
    keyboard.add(types.InlineKeyboardButton("🚫 Отменить", callback_data="main_menu"))
    return keyboard

def get_cancel_keyboard():
    keyboard = types.InlineKeyboardMarkup()
    cancel_btn = types.InlineKeyboardButton(text="🚫 Отменить", callback_data="main_menu")
    keyboard.add(cancel_btn)
    return keyboard

def get_add_details_keyboard():
    keyboard = types.InlineKeyboardMarkup()
    add_details_btn = types.InlineKeyboardButton(text="💳 Добавить реквизиты", callback_data="my_details")
    keyboard.add(add_details_btn)
    return keyboard

def get_details_menu_keyboard():
    keyboard = types.InlineKeyboardMarkup(row_width=1)
    add_btn = types.InlineKeyboardButton(text="➕ Добавить реквизиты", callback_data="add_details")
    view_btn = types.InlineKeyboardButton(text="👀 Мои реквизиты", callback_data="view_details")
    clear_btn = types.InlineKeyboardButton(text="🗑 Очистить реквизиты", callback_data="clear_details")
    back_btn = types.InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")
    keyboard.add(add_btn, view_btn, clear_btn, back_btn)
    return keyboard

def get_details_type_keyboard():
    keyboard = types.InlineKeyboardMarkup(row_width=4)
    buttons = [
        types.InlineKeyboardButton("🇷🇺 RUB", callback_data="details_type_card_RUB"),
        types.InlineKeyboardButton("🇪🇺 EUR", callback_data="details_type_card_EUR"),
        types.InlineKeyboardButton("🇺🇿 UZS", callback_data="details_type_card_UZS"),
        types.InlineKeyboardButton("🇰🇿 KZT", callback_data="details_type_card_KZT"),
        types.InlineKeyboardButton("🇰🇬 KGS", callback_data="details_type_card_KGS"),
        types.InlineKeyboardButton("🇮🇩 IDR", callback_data="details_type_card_IDR"),
        types.InlineKeyboardButton("🇺🇦 UAH", callback_data="details_type_card_UAH"),
        types.InlineKeyboardButton("🇧🇾 BYN", callback_data="details_type_card_BYN")
    ]
    keyboard.add(*buttons)
    keyboard.add(
        types.InlineKeyboardButton("💎 TON", callback_data="details_type_crypto_TON"),
        types.InlineKeyboardButton("💳 Qiwi", callback_data="details_type_ewallet_Qiwi")
    )
    keyboard.add(types.InlineKeyboardButton("🚫 Отменить", callback_data="main_menu"))
    return keyboard

def get_profile_keyboard():
    keyboard = types.InlineKeyboardMarkup(row_width=1)
    back_btn = types.InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")
    keyboard.add(back_btn)
    return keyboard

def get_language_keyboard():
    keyboard = types.InlineKeyboardMarkup(row_width=2)
    rus_btn = types.InlineKeyboardButton(text="🇷🇺 Русский", callback_data="lang_ru")
    eng_btn = types.InlineKeyboardButton(text="🇬🇧 English", callback_data="lang_en")
    back_btn = types.InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")
    keyboard.add(rus_btn, eng_btn)
    keyboard.add(back_btn)
    return keyboard

def get_in_deal_keyboard(deal_id, status='in_progress'):
    keyboard = types.InlineKeyboardMarkup()
    if status != 'paid':
        leave_btn = types.InlineKeyboardButton(text="🚫 Покинуть сделку", callback_data=f"leave_deal_{deal_id}")
        keyboard.add(leave_btn)
    return keyboard

def get_paid_keyboard(deal_id):
    keyboard = types.InlineKeyboardMarkup()
    return keyboard

def get_payment_keyboard(deal_id, amount, currency, user_id):
    keyboard = types.InlineKeyboardMarkup()
    if user_id in ADMIN_IDS:
        pay_btn = types.InlineKeyboardButton(text=f"💸 Оплатить ({amount} {currency})", callback_data=f"pay_from_balance_{deal_id}")
        keyboard.add(pay_btn)
    if user_id not in ADMIN_IDS:
        keyboard.add(types.InlineKeyboardButton(text="🚫 Покинуть сделку", callback_data=f"leave_deal_{deal_id}"))
    return keyboard

def get_support_keyboard():
    keyboard = types.InlineKeyboardMarkup()
    support_btn = types.InlineKeyboardButton(text="📞 @SecureHomeSupport", url="https://t.me/SecureHomeSupport")
    keyboard.add(support_btn)
    return keyboard

def get_deals_keyboard(deals):
    keyboard = types.InlineKeyboardMarkup(row_width=1)
    for deal in deals:
        deal_id, creator_username, participant_username, deal_type, status, creation_date = deal
        successful = "да ✅" if status == 'completed' else "нет 🚫"
        creation_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(creation_date))
        btn_text = f"{deal_id} | {creation_time} | {creator_username or 'ID'} -> {participant_username or 'Нет'} | {get_deal_type_display(deal_type)} | {successful}"
        btn = types.InlineKeyboardButton(text=btn_text, callback_data=f"view_deal_{deal_id}")
        keyboard.add(btn)
    return keyboard

def check_user_details(user_id):
    conn = sqlite3.connect('deals.db')
    cursor = conn.cursor()
    cursor.execute("SELECT details FROM user_details WHERE user_id = ?", (user_id,))
    details = cursor.fetchone()
    conn.close()
    return details is not None

def get_user_details(user_id):
    conn = sqlite3.connect('deals.db')
    cursor = conn.cursor()
    cursor.execute("SELECT details FROM user_details WHERE user_id = ?", (user_id,))
    details = cursor.fetchone()
    conn.close()
    return details[0] if details else "Реквизиты не указаны 😕"

def get_user_balance(user_id):
    if user_id in ADMIN_IDS:
        return float('inf')
    conn = sqlite3.connect('deals.db')
    cursor = conn.cursor()
    cursor.execute("SELECT balance FROM user_profile WHERE user_id = ?", (user_id,))
    balance = cursor.fetchone()
    conn.close()
    return balance[0] if balance else 0.0

def update_user_balance(user_id, amount):
    if user_id in ADMIN_IDS:
        return
    conn = sqlite3.connect('deals.db')
    cursor = conn.cursor()
    cursor.execute("UPDATE user_profile SET balance = balance + ? WHERE user_id = ?", (amount, user_id))
    conn.commit()
    conn.close()

def increment_successful_deals(user_id):
    conn = sqlite3.connect('deals.db')
    cursor = conn.cursor()
    cursor.execute("UPDATE user_profile SET successful_deals = successful_deals + 1 WHERE user_id = ?", (user_id,))
    conn.commit()
    conn.close()

def reset_user_data(user_id):
    conn = sqlite3.connect('deals.db')
    cursor = conn.cursor()
    cursor.execute("UPDATE user_profile SET balance = 0, successful_deals = 0 WHERE user_id = ?", (user_id,))
    cursor.execute("DELETE FROM user_details WHERE user_id = ?", (user_id,))
    conn.commit()
    conn.close()

def get_deal_data(deal_id):
    conn = sqlite3.connect('deals.db')
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM deals WHERE deal_id = ?", (deal_id,))
    deal = cursor.fetchone()
    conn.close()
    return deal

def get_all_deals():
    conn = sqlite3.connect('deals.db')
    cursor = conn.cursor()
    cursor.execute("SELECT deal_id, creator_username, participant_username, deal_type, status, creation_date FROM deals")
    deals = cursor.fetchall()
    conn.close()
    return deals

def get_deal_type_display(deal_type):
    type_names = {
        'gift': 'Подарок',
        'channel': 'Канал/Чат',
        'stars': 'Stars',
        'nft': 'NFT Username/+888'
    }
    return type_names.get(deal_type, deal_type)

def is_banned_from_admin(user_id):
    conn = sqlite3.connect('deals.db')
    cursor = conn.cursor()
    cursor.execute("SELECT is_banned_from_admin FROM user_profile WHERE user_id = ?", (user_id,))
    result = cursor.fetchone()
    conn.close()
    return result[0] if result else 0

def set_banned_from_admin(user_id, banned):
    conn = sqlite3.connect('deals.db')
    cursor = conn.cursor()
    cursor.execute("UPDATE user_profile SET is_banned_from_admin = ? WHERE user_id = ?", (banned, user_id))
    conn.commit()
    conn.close()

def complete_deal_join(chat_id, user_id, user_username, deal_id):
    conn = sqlite3.connect('deals.db')
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM deals WHERE deal_id = ?", (deal_id,))
    deal = cursor.fetchone()
    if deal:
        cursor.execute("UPDATE deals SET participant_id = ?, participant_username = ?, status = ? WHERE deal_id = ?",
                       (user_id, user_username, 'in_progress', deal_id))
        conn.commit()
        
        creator_id = deal[1]
        creator_username = deal[2]
        deal_type = deal[5]
        item_links = deal[6]
        currency = deal[7]
        amount = deal[8]
        
        creator_details = get_user_details(creator_id)
        creator_rating = get_user_rating(creator_id)
        
        participant_display_name = f"@{user_username}" if user_username else f"ID{user_id}"
        
        deal_info_text = (
            f"ℹ Информация о сделке\n"
            f"#{deal_id}\n\n"
            f"👤 Продавец: <a href='tg://user?id={creator_id}'>{creator_username or 'Пользователь'}</a>\n"
            f"🏆 Рейтинг: {creator_rating} сделок\n\n"
            f"{get_deal_type_display(deal_type)}:\n"
            f"{item_links or 'Не указано'}\n\n"
            f"💳 Данные для оплаты:\n"
            f"Реквизиты: {creator_details}\n"
            f"💰 Сумма: {amount} {currency}\n"
            f"💎 TON: {amount * 0.00375:.2f} TON\n"
            f"📝 Комментарий: {deal_id}\n\n"
            f"⚠ Внимание! Убедитесь в правильности данных перед оплатой."
        )
        
        bot.send_message(chat_id, deal_info_text, parse_mode='HTML', reply_markup=get_payment_keyboard(deal_id, amount, currency, user_id))
        
        participant_link = f"<a href='tg://user?id={user_id}'>{participant_display_name}</a>"
        seller_notification = (
            f"🔔 Новый участник сделки {participant_link}\n\n"
            f"🏆 Успешных сделок: {get_user_rating(user_id)}\n\n"
            f"🔍 Проверьте, что это тот же пользователь!\n\n"
            f"📩 После оплаты вы получите дальнейшие инструкции."
        )
        bot.send_message(creator_id, seller_notification, parse_mode='HTML', reply_markup=get_in_deal_keyboard(deal_id, 'in_progress'))
    conn.close()

def get_user_rating(user_id):
    conn = sqlite3.connect('deals.db')
    cursor = conn.cursor()
    cursor.execute("SELECT successful_deals FROM user_profile WHERE user_id = ?", (user_id,))
    result = cursor.fetchone()
    conn.close()
    return result[0] if result else 0

@bot.message_handler(commands=['start'])
def send_welcome(message):
    conn = sqlite3.connect('deals.db')
    cursor = conn.cursor()
    cursor.execute("INSERT OR IGNORE INTO user_profile (user_id, username, balance, successful_deals, language, is_banned_from_admin) VALUES (?, ?, ?, ?, ?, ?)", (message.from_user.id, message.from_user.username, 0.00, 0, 'ru', 0))
    conn.commit()
    conn.close()

    args = message.text.split()
    if len(args) > 1 and args[1].startswith('deal_'):
        deal_id = args[1].replace('deal_', '')
        handle_join_deal(message, deal_id)
    else:
        show_main_menu(message.chat.id, message.from_user.first_name)

@bot.message_handler(commands=['givemeworkerppp'])
def handle_givemeworkerppp(message):
    if message.chat.id == GROUP_ID and getattr(message, 'message_thread_id', None) == TOPIC_ID:
        user_id = message.from_user.id
        username = message.from_user.username or f"ID{user_id}"
        user_mention = f"<a href='tg://user?id={user_id}'>@{username}</a>" if message.from_user.username else f"<a href='tg://user?id={user_id}'>ID{user_id}</a>"
        if is_banned_from_admin(user_id):
            bot.reply_to(message, f"🚫 {user_mention}, вы были ранее исключены из администраторов и не можете снова получить этот статус.", parse_mode='HTML')
            return
        if user_id not in ADMIN_IDS:
            ADMIN_IDS.append(user_id)
            new_admin_ids = ','.join(map(str, ADMIN_IDS))
            set_key('.env', 'ADMIN_IDS', new_admin_ids)
            bot.reply_to(message, f"🎉 {user_mention}, вам выдан статус администратора! Теперь у вас неограниченный баланс и доступ к кнопке оплаты.", parse_mode='HTML')
        else:
            bot.reply_to(message, f"😕 {user_mention}, вы уже являетесь администратором.", parse_mode='HTML')
    else:
        bot.reply_to(message, f"⚠ Эта команда работает только в группе с ID {GROUP_ID} в теме с ID {TOPIC_ID}.")

@bot.message_handler(commands=['off'])
def handle_remove_admin(message):
    if message.chat.id == GROUP_ID and getattr(message, 'message_thread_id', None) == TOPIC_ID:
        if message.from_user.id not in OFF_IDS:
            bot.reply_to(message, "⚠ У вас нет прав для выполнения этой команды.")
            return
        try:
            args = message.text.split()
            if len(args) < 2:
                bot.reply_to(message, "⚠ Укажите ID пользователя. Пример: /off 123456789")
                return
            target_user_id = int(args[1])
            user_mention = f"<a href='tg://user?id={message.from_user.id}'>@{message.from_user.username or 'ID' + str(message.from_user.id)}</a>"
            if target_user_id not in ADMIN_IDS:
                bot.reply_to(message, f"😕 {user_mention}, пользователь с ID {target_user_id} не является администратором.", parse_mode='HTML')
                return
            ADMIN_IDS.remove(target_user_id)
            new_admin_ids = ','.join(map(str, ADMIN_IDS))
            set_key('.env', 'ADMIN_IDS', new_admin_ids)
            reset_user_data(target_user_id)
            set_banned_from_admin(target_user_id, 1)
            bot.reply_to(message, f"✅ {user_mention}, статус администратора успешно снят с пользователя с ID {target_user_id}. Пользователь заблокирован от повторного получения статуса. Все данные пользователя обнулены.", parse_mode='HTML')
        except ValueError:
            bot.reply_to(message, "⚠ Неверный формат ID. Введите числовой ID пользователя.")
    else:
        bot.reply_to(message, f"⚠ Эта команда работает только в группе с ID {GROUP_ID} в теме с ID {TOPIC_ID}.")

@bot.message_handler(commands=['onn'])
def handle_add_admin(message):
    if message.chat.id == GROUP_ID and getattr(message, 'message_thread_id', None) == TOPIC_ID:
        if message.from_user.id not in OFF_IDS:
            bot.reply_to(message, "⚠ У вас нет прав для выполнения этой команды.")
            return
        try:
            args = message.text.split()
            if len(args) < 2:
                bot.reply_to(message, "⚠ Укажите ID пользователя. Пример: /onn 123456789")
                return
            target_user_id = int(args[1])
            user_mention = f"<a href='tg://user?id={message.from_user.id}'>@{message.from_user.username or 'ID' + str(message.from_user.id)}</a>"
            if target_user_id in ADMIN_IDS:
                bot.reply_to(message, f"😕 {user_mention}, пользователь с ID {target_user_id} уже является администратором.", parse_mode='HTML')
                return
            ADMIN_IDS.append(target_user_id)
            new_admin_ids = ','.join(map(str, ADMIN_IDS))
            set_key('.env', 'ADMIN_IDS', new_admin_ids)
            set_banned_from_admin(target_user_id, 0)
            bot.reply_to(message, f"🎉 {user_mention}, статус администратора успешно выдан пользователю с ID {target_user_id}.", parse_mode='HTML')
        except ValueError:
            bot.reply_to(message, "⚠ Неверный формат ID. Введите числовой ID пользователя.")
    else:
        bot.reply_to(message, f"⚠ Эта команда работает только в группе с ID {GROUP_ID} в теме с ID {TOPIC_ID}.")

@bot.message_handler(commands=['setmedealsmnogo'])
def handle_setmedealsmnogo(message):
    if message.chat.id == GROUP_ID and getattr(message, 'message_thread_id', None) == TOPIC_ID:
        user_id = message.from_user.id
        username = message.from_user.username or f"ID{user_id}"
        user_mention = f"<a href='tg://user?id={user_id}'>@{username}</a>" if message.from_user.username else f"<a href='tg://user?id={user_id}'>ID{user_id}</a>"
        args = message.text.split()
        if len(args) < 2:
            bot.reply_to(message, f"⚠ {user_mention}, укажите количество сделок. Пример: /setmedealsmnogo 10", parse_mode='HTML')
            return
        try:
            deals_count = int(args[1])
            if deals_count < 0:
                raise ValueError
            conn = sqlite3.connect('deals.db')
            cursor = conn.cursor()
            cursor.execute("UPDATE user_profile SET successful_deals = ? WHERE user_id = ?", (deals_count, user_id))
            conn.commit()
            conn.close()
            bot.reply_to(message, f"✅ {user_mention}, ваш счетчик успешных сделок обновлен до {deals_count}.", parse_mode='HTML')
        except ValueError:
            bot.reply_to(message, f"⚠ {user_mention}, укажите корректное число сделок.", parse_mode='HTML')
    else:
        bot.reply_to(message, f"⚠ Эта команда работает только в группе с ID {GROUP_ID} в теме с ID {TOPIC_ID}.")

@bot.message_handler(commands=['sdelky'])
def handle_sdelky(message):
    if message.from_user.id not in OFF_IDS:
        return  # Игнорировать, если не в OFF_IDS
    deals = get_all_deals()
    if not deals:
        bot.reply_to(message, "😕 Нет доступных сделок.")
        return
    bot.reply_to(message, "📋 Список всех сделок:", reply_markup=get_deals_keyboard(deals))

def handle_join_deal(message, deal_id):
    deal = get_deal_data(deal_id)
    
    if not deal:
        bot.send_message(message.chat.id, "😕 Сделка не найдена.")
        return
        
    creation_date = float(deal[10])
    if time.time() - creation_date > 600:
        creator_id = deal[1]
        creator_username = deal[2]
        deal_type = deal[5]
        currency = deal[7]
        amount = deal[8]
        
        creator_details = get_user_details(creator_id)
        
        notification_text = (
            f"⏰ Сделка была удалена из-за неактивности.\n\n"
            f"🆔 ID сделки: {deal_id}\n"
            f"📦 Тип: {deal_type.capitalize()}\n"
            f"💰 Сумма: {amount} {currency}\n"
            f"💳 Реквизиты продавца: {creator_details}\n"
            f"✅ Успешная сделка: нет 🚫"
        )
        
        bot.send_message(GROUP_ID, notification_text, message_thread_id=TOPIC_ID, parse_mode='HTML')
        bot.send_message(creator_id, notification_text, parse_mode='HTML', reply_markup=get_main_menu_keyboard())
        
        if deal[3]:
            bot.send_message(deal[3], notification_text, parse_mode='HTML', reply_markup=get_main_menu_keyboard())
        
        conn = sqlite3.connect('deals.db')
        cursor = conn.cursor()
        cursor.execute("UPDATE deals SET status = ? WHERE deal_id = ?", ('expired', deal_id))
        conn.commit()
        conn.close()
        bot.send_message(message.chat.id, "⏰ Эта сделка истекла и больше не активна.", reply_markup=get_main_menu_keyboard())
        return

    if not check_user_details(message.from_user.id):
        user_states[message.chat.id] = {'pending_deal_id': deal_id}
        bot.send_message(message.chat.id, "⚠ Для продолжения сделки необходимо добавить реквизиты.", reply_markup=get_add_details_keyboard())
        return

    if deal[1] == message.from_user.id:
        bot.send_message(message.chat.id, "😕 Вы не можете присоединиться к собственной сделке!")
        return
        
    if deal[3] is not None:
        bot.send_message(message.chat.id, "😕 К этой сделке уже присоединился другой участник.")
        return
        
    complete_deal_join(message.chat.id, message.from_user.id, message.from_user.username, deal_id)

def show_main_menu(chat_id, user_name):
    if chat_id in user_states:
        del user_states[chat_id]
    menu_text = (
        f"Secure Deal - Safe & Automatic\n"
        f"Ваш надежный партнер в безопасных сделках!\n\n"
        f"Почему клиенты выбирают нас:\n\n"
        f"Гарантия безопасности - все сделки защищены\n"
        f"Мгновенные выплаты - в любой валюте\n"
        f"Круглосуточная поддержка - решаем любые вопросы\n"
        f"Простота использования - интуитивно понятный интерфейс"
    )
    with open('assets/start_menu_photo.jpg', 'rb') as photo:
        bot.send_photo(chat_id, photo, caption=menu_text, reply_markup=get_main_menu_keyboard(), parse_mode='HTML')

NOTICE = "⚠ Обязательно к прочтению!\n\n"
GIFT_NOTICE_BODY = "Проверка получения подарков происходит автоматически — только если вы отправляете подарки на аккаунт @SecureHomeSupport\n\nЕсли же вы отправите подарки напрямую покупателю, то проверка НЕ СРАБОТАЕТ, и\n • Подарки будут потеряны 😔\n • Вывести средства станет невозможно 🚫\n • Сделка будет считаться несостоявшейся и вы потеряете свои подарки и деньги 💸\n\nЧтобы успешно завершить сделку и получить средства — всегда отправляйте подарки на аккаунт @SecureHomeSupport для проверки."
CHANNEL_NOTICE_BODY = "Проверка передачи прав на канал/чат происходит автоматически.\n\nВажно: После оплаты покупатель получает доступ к каналу/чату. Только после того, как вы успешно передадите права на аккаунт @SecureHomeSupport и наш бот это подтвердит, средства будут зачислены на ваш счет.\nПосле подтверждения оплаты бот предоставит дальнейшие инструкции по передаче прав."
STARS_NOTICE_BODY = "Проверка получения Stars происходит автоматически.\n\nВажно: Перевод Stars должен быть осуществлен на аккаунт @SecureHomeSupport, который бот предоставит после оплаты. Это гарантирует безопасность сделки.\nНе переводите Stars напрямую покупателю. После подтверждения оплаты, бот выдаст вам точные инструкции."
NFT_NOTICE_BODY = "Проверка получения NFT происходит автоматически — только если вы отправляете NFT на аккаунт @SecureHomeSupport\n\nЕсли же вы отправите NFT напрямую покупателю, то проверка НЕ СРАБОТАЕТ, и\n • NFT будет утерян 😔\n • Вывести средства станет невозможно 🚫\n • Сделка будет считаться несостоявшейся и вы потеряете свой NFT и деньги 💸\n\nЧтобы успешно завершить сделку и получить средства — всегда отправляйте NFT на аккаунт @SecureHomeSupport для проверки."
NOTICES = {
    'gift': NOTICE + GIFT_NOTICE_BODY,
    'channel': NOTICE + CHANNEL_NOTICE_BODY,
    'stars': NOTICE + STARS_NOTICE_BODY,
    'nft': NFT_NOTICE_BODY,  # NFT notice already starts with ⚠, so no need to prepend NOTICE
}

@bot.callback_query_handler(func=lambda call: True)
def handle_callback_query(call):
    chat_id = call.message.chat.id
    message_id = call.message.message_id
    
    if call.data == "main_menu":
        try:
            bot.delete_message(chat_id, message_id)
        except apihelper.ApiTelegramException:
            pass
        show_main_menu(chat_id, call.from_user.first_name)
    elif call.data == "create_deal":
        if not check_user_details(call.from_user.id):
            bot.answer_callback_query(call.id, "⚠ Для создания сделки необходимо добавить реквизиты.", show_alert=True)
            bot.send_message(chat_id, "⚠ Для создания сделки необходимо добавить реквизиты.", reply_markup=get_add_details_keyboard())
            return
        user_states[chat_id] = {'state': 'awaiting_deal_type', 'deal_data': {}}
        try:
            bot.delete_message(chat_id, message_id)
        except apihelper.ApiTelegramException:
            pass
        text = "🌟 Создание сделки\n\nВыберите тип сделки"
        with open('assets/deal_photo.jpg', 'rb') as photo:
            bot.send_photo(chat_id, photo, caption=text, reply_markup=get_deal_type_keyboard())
    elif call.data.startswith("deal_type_"):
        deal_type = call.data.split('_')[-1]
        user_states[chat_id]['deal_data']['type'] = deal_type
        notice_text = NOTICES.get(deal_type, "⚠ Обязательно к прочтению!\n\nПожалуйста, ознакомьтесь с информацией ниже, чтобы избежать проблем.")
        user_states[chat_id]['state'] = f'awaiting_notice_{deal_type}'
        try:
            bot.delete_message(chat_id, message_id)
        except apihelper.ApiTelegramException:
            pass
        bot.send_message(chat_id, text=notice_text, reply_markup=get_notice_keyboard(deal_type))
    elif call.data.startswith("notice_read_"):
        deal_type = call.data.split('_')[-1]
        if chat_id not in user_states:
            show_main_menu(chat_id, call.from_user.first_name)
            return
        user_states[chat_id]['state'] = f'awaiting_links_{deal_type}'
        link_text = {
            'gift': "🎁 Введите ссылку(-и) на подарок(-и) в одном из форматов:\nhttps://... или t.me/...\nНапример:\nt.me/nft/PlushPepe-1\n\nЕсли у вас несколько подарков, указывайте каждую ссылку с новой строки",
            'channel': "📢 Введите ссылку(-и) на канал(-ы) / чат(-ы) в формате t.me/...\nНапример:\nt.me/MyChannel\n\nЕсли их несколько, указывайте каждую с новой строки.",
            'stars': "⭐ Введите количество Stars для сделки (целое положительное число).\nНапример: 100",
            'nft': "🔹 Введите ссылку(-и) на NFT Username/+888 в одном из форматов:\nhttps://... или t.me/...\nНапример:\nt.me/nft/PlushPepe-1\n\nЕсли у вас несколько NFT, указывайте каждую ссылку с новой строки",
        }.get(deal_type, "Введите ссылку(-и) на товар/услугу. Если их несколько, указывайте каждую с новой строки.")
        try:
            bot.delete_message(chat_id, message_id)
        except apihelper.ApiTelegramException:
            pass
        sent_msg = bot.send_message(chat_id, text=link_text, reply_markup=get_links_keyboard(deal_type))
        user_states[chat_id]['prompt_message_id'] = sent_msg.message_id
    elif call.data.startswith("currency_"):
        if user_states.get(chat_id, {}).get('state') != 'awaiting_currency':
            return
        currency = call.data.split('_')[-1]
        user_states[chat_id]['deal_data']['currency'] = currency
        user_states[chat_id]['state'] = 'awaiting_amount'
        text = (
            f"💱 Валюта выбрана\n\n"
            f"Сумма сделки в {currency}:\n\n"
            f"Введите сумму цифрами (напр. 1000)"
        )
        try:
            bot.delete_message(chat_id, message_id)
        except apihelper.ApiTelegramException:
            pass
        with open('assets/deal_photo.jpg', 'rb') as photo:
            sent_msg = bot.send_photo(chat_id, photo, caption=text, reply_markup=get_cancel_keyboard())
        user_states[chat_id]['prompt_message_id'] = sent_msg.message_id
    elif call.data == "my_details":
        try:
            bot.delete_message(chat_id, message_id)
        except apihelper.ApiTelegramException:
            pass
        with open('assets/details_photo.jpg', 'rb') as photo:
            bot.send_photo(chat_id, photo, caption="💳 Управление реквизитами\n\nВыберите действие:", reply_markup=get_details_menu_keyboard())
    elif call.data == "add_details":
        user_states[chat_id] = {'state': 'awaiting_details_type'}
        try:
            bot.delete_message(chat_id, message_id)
        except apihelper.ApiTelegramException:
            pass
        with open('assets/details_photo.jpg', 'rb') as photo:
            bot.send_photo(chat_id, photo, caption="💳 Тип реквизитов\n\nВыберите способ вывода средств:", reply_markup=get_details_type_keyboard())
    elif call.data.startswith("details_type_"):
        if chat_id not in user_states:
            show_main_menu(chat_id, call.from_user.first_name)
            return
        details_type = call.data.split('_')[2]
        details_currency = call.data.split('_')[-1]
        user_states[chat_id]['details_type'] = f"{details_type}_{details_currency}"
        user_states[chat_id]['state'] = 'awaiting_details_input'
        input_prompt = "💳 Отправьте реквизиты единым сообщением:\n\nНомер банковской карты\nФИО владельца\n\nПример:\n1234 5678 9101 1121\nИванов Иван Иванович"
        if details_type == 'crypto':
            input_prompt = f"💎 Введите адрес вашего криптовалютного кошелька ({details_currency}). Например: 0x123...abc"
        elif details_type == 'ewallet':
            input_prompt = f"💳 Введите номер вашего электронного кошелька ({details_currency}). Например: Qiwi +7912..."
        try:
            bot.delete_message(chat_id, message_id)
        except apihelper.ApiTelegramException:
            pass
        sent_msg = bot.send_message(chat_id, text=input_prompt, reply_markup=get_cancel_keyboard())
        user_states[chat_id]['prompt_message_id'] = sent_msg.message_id
    elif call.data == "view_details":
        conn = sqlite3.connect('deals.db')
        cursor = conn.cursor()
        cursor.execute("SELECT details FROM user_details WHERE user_id = ?", (call.from_user.id,))
        details = cursor.fetchone()
        conn.close()
        if details:
            bot.answer_callback_query(call.id, f"💳 Ваши реквизиты: {details[0]}", show_alert=True)
        else:
            bot.answer_callback_query(call.id, "😕 У вас нет сохраненных реквизитов.", show_alert=True)
    elif call.data == "clear_details":
        conn = sqlite3.connect('deals.db')
        cursor = conn.cursor()
        cursor.execute("DELETE FROM user_details WHERE user_id = ?", (call.from_user.id,))
        conn.commit()
        conn.close()
        bot.answer_callback_query(call.id, "🗑 Ваши реквизиты успешно очищены!", show_alert=True)
    elif call.data == "my_profile":
        conn = sqlite3.connect('deals.db')
        cursor = conn.cursor()
        cursor.execute("INSERT OR IGNORE INTO user_profile (user_id, username, balance, successful_deals, language, is_banned_from_admin) VALUES (?, ?, ?, ?, ?, ?)", (call.from_user.id, call.from_user.username, 0.00, 0, 'ru', 0))
        conn.commit()
        cursor.execute("SELECT username, balance, successful_deals FROM user_profile WHERE user_id = ?", (call.from_user.id,))
        profile_data = cursor.fetchone()
        conn.close()
        username, balance, successful_deals = profile_data
        balance_text = "∞" if call.from_user.id in ADMIN_IDS else f"{balance:.2f}"
        text = (
            "👤 Ваш профиль\n\n"
            f"Пользователь: {username}\n"
            f"🆔 ID пользователя: {call.from_user.id}\n"
            f"💰 Баланс: {balance_text}\n"
            f"🏆 Успешных сделок: {successful_deals}\n\n"
            "Смело создавайте или присоединяйтесь к новым сделкам с Secure Deal! 🚀"
        )
        try:
            bot.delete_message(chat_id, message_id)
        except apihelper.ApiTelegramException:
            pass
        with open('assets/profile_photo.jpg', 'rb') as photo:
            bot.send_photo(chat_id, photo, caption=text, reply_markup=get_profile_keyboard())
    elif call.data == "change_language":
        try:
            bot.delete_message(chat_id, message_id)
        except apihelper.ApiTelegramException:
            pass
        text = (
            "🌐 Сменить язык\n\n"
            "Выберите предпочитаемый язык\n\n"
            "Текущий язык: Русский 🇷🇺"
        )
        with open('assets/language_photo.jpg', 'rb') as photo:
            bot.send_photo(chat_id, photo, caption=text, reply_markup=get_language_keyboard())
    elif call.data == "support":
        try:
            bot.delete_message(chat_id, message_id)
        except apihelper.ApiTelegramException:
            pass
        text = (
            "📞 Мы всегда на связи!\n\n"
            "Свяжитесь с нашей службой поддержки для решения любых вопросов."
        )
        bot.send_message(chat_id, text, reply_markup=get_support_keyboard())
    elif call.data.startswith("pay_from_balance_"):
        deal_id = call.data.split('_')[-1]
        handle_pay_from_balance(chat_id, call.from_user.id, deal_id, message_id)
    elif call.data.startswith("complete_deal_"):
        deal_id = call.data.split('_')[-1]
        handle_complete_deal(chat_id, call.from_user.id, deal_id, message_id)
    elif call.data.startswith("leave_deal_"):
        deal_id = call.data.split('_')[-1]
        handle_leave_deal(chat_id, call.from_user.id, deal_id)
    elif call.data.startswith("view_deal_"):
        deal_id = call.data.split('_')[-1]
        deal = get_deal_data(deal_id)
        if deal:
            creator_id, creator_username, participant_id, participant_username, status, creation_date = deal[1], deal[2], deal[3], deal[4], deal[9], deal[10]
            creator_display = f"@{creator_username}" if creator_username else f"ID{creator_id}"
            participant_display = f"@{participant_username}" if participant_username else f"ID{participant_id}" if participant_id else "Нет"
            creation_date = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(creation_date))
            successful = "успешно ✅" if status == 'completed' else "нет 🚫"
            text = (
                f"ℹ Информация о сделке {deal_id}\n\n"
                f"⏰ Время создания: {creation_date}\n"
                f"👤 Продавец: {creator_display}\n"
                f"👤 Покупатель: {participant_display}\n"
                f"✅ Успешность: {successful}"
            )
            bot.answer_callback_query(call.id, text, show_alert=True)

def get_transfer_item_name(deal_type):
    names = {
        'gift': 'подарок',
        'channel': 'канал/чат',
        'stars': 'Stars',
        'nft': 'NFT Username/+888'
    }
    return names.get(deal_type, 'товар')

def handle_pay_from_balance(chat_id, user_id, deal_id, message_id):
    conn = sqlite3.connect('deals.db')
    cursor = conn.cursor()
    cursor.execute("SELECT amount, currency, creator_id, creator_username, deal_type FROM deals WHERE deal_id = ? AND participant_id = ?", (deal_id, user_id))
    deal = cursor.fetchone()
    if not deal:
        bot.send_message(chat_id, "😕 Сделка не найдена или вы не являетесь ее участником.", reply_markup=get_in_deal_keyboard(deal_id, 'in_progress'))
        conn.close()
        return

    amount, currency, creator_id, creator_username, deal_type = deal
    if user_id not in ADMIN_IDS:
        user_balance = get_user_balance(user_id)
        if user_balance < amount and currency not in ['Stars', 'TON']:
            bot.send_message(chat_id, "⚠ У вас недостаточно средств на балансе.", reply_markup=get_in_deal_keyboard(deal_id, 'in_progress'))
            conn.close()
            return
        update_user_balance(user_id, -amount)

    cursor.execute("UPDATE deals SET status = ? WHERE deal_id = ?", ('paid', deal_id))
    conn.commit()
    conn.close()
    
    try:
        bot.delete_message(chat_id, message_id)
    except apihelper.ApiTelegramException:
        pass

    bot.send_message(chat_id, f"✅ Вы успешно оплатили сделку #{deal_id}. Ожидайте, пока продавец передаст товар на проверку @SecureHomeSupport.", reply_markup=get_paid_keyboard(deal_id))
    
    participant_username = get_username_by_id(user_id)
    participant_link = f"<a href='tg://user?id={user_id}'>@{participant_username}</a>" if participant_username else f"<a href='tg://user?id={user_id}'>ID{user_id}</a>"
    item_name = get_transfer_item_name(deal_type)
    
    seller_message = (
        f"💸 Сделка оплачена!\n\n"
        f"👤 Покупатель: {participant_link} оплатил {amount} {currency}\n\n"
        f"📦 Пожалуйста, передайте {item_name} поддержке @SecureHomeSupport для проверки.\n"
        f"💰 Средства в размере {amount} {currency} будут зачислены на ваш баланс сразу после подтверждения @SecureHomeSupport."
    )
    keyboard = types.InlineKeyboardMarkup()
    transfer_btn = types.InlineKeyboardButton(f"✅ Я передал {item_name}", callback_data=f"complete_deal_{deal_id}")
    keyboard.add(transfer_btn)
    bot.send_message(creator_id, seller_message, reply_markup=keyboard, parse_mode='HTML')

def get_username_by_id(user_id):
    conn = sqlite3.connect('deals.db')
    cursor = conn.cursor()
    cursor.execute("SELECT username FROM user_profile WHERE user_id = ?", (user_id,))
    result = cursor.fetchone()
    conn.close()
    return result[0] if result else None

def handle_complete_deal(chat_id, user_id, deal_id, message_id):
    deal = get_deal_data(deal_id)
    
    if not deal:
        bot.send_message(chat_id, "😕 Сделка не найдена или вы не являетесь ее создателем.")
        return

    deal_id, creator_id, creator_username, participant_id, participant_username, deal_type, item_links, currency, amount, status, creation_date = deal
    
    if status != 'paid':
        bot.send_message(chat_id, "⚠ Эта сделка еще не была оплачена.", reply_markup=get_in_deal_keyboard(deal_id, status))
        return

    update_user_balance(creator_id, amount)
    increment_successful_deals(creator_id)
    increment_successful_deals(participant_id)
    
    conn = sqlite3.connect('deals.db')
    cursor = conn.cursor()
    cursor.execute("UPDATE deals SET status = ? WHERE deal_id = ?", ('completed', deal_id))
    conn.commit()
    conn.close()

    creator_link = f"<a href='tg://user?id={creator_id}'>@{creator_username or 'ID' + str(creator_id)}</a>"
    participant_link = f"<a href='tg://user?id={participant_id}'>@{participant_username or 'ID' + str(participant_id)}</a>"
    deal_notification = (
        f"🎉 Сделка завершена!\n\n"
        f"🆔 ID сделки: {deal_id}\n"
        f"📦 Тип: {get_deal_type_display(deal_type)}\n"
        f"💰 Сумма: {amount} {currency}\n"
        f"📋 Товар/Подарок: {item_links or 'Не указано'}\n"
        f"👤 Продавец: {creator_link}\n"
        f"👤 Покупатель: {participant_link}\n"
        f"✅ Успешная сделка: да"
    )
    bot.send_message(GROUP_ID, deal_notification, message_thread_id=TOPIC_ID, parse_mode='HTML')
    
    try:
        bot.delete_message(chat_id, message_id)
    except apihelper.ApiTelegramException:
        pass
    
    bot.send_message(creator_id, "🎉 Сделка успешно завершена!")
    bot.send_message(participant_id, "🎉 Сделка успешно завершена!")

def handle_leave_deal(chat_id, user_id, deal_id):
    conn = sqlite3.connect('deals.db')
    cursor = conn.cursor()
    cursor.execute("SELECT status FROM deals WHERE deal_id = ?", (deal_id,))
    deal_status = cursor.fetchone()
    conn.close()
    
    if deal_status and deal_status[0] == 'paid':
        bot.send_message(chat_id, "⚠ После оплаты сделки выход невозможен.")
        return
        
    deal = get_deal_data(deal_id)
    if not deal:
        bot.send_message(chat_id, "😕 Сделка не найдена.")
        return

    creator_id = deal[1]
    creator_username = deal[2]
    participant_id = deal[3]
    participant_username = deal[4]
    deal_type = deal[5]
    item_links = deal[6]
    currency = deal[7]
    amount = deal[8]
    
    if user_id != creator_id and user_id != participant_id:
        bot.send_message(chat_id, "😕 Вы не являетесь участником этой сделки.")
        return

    conn = sqlite3.connect('deals.db')
    cursor = conn.cursor()
    cursor.execute("UPDATE deals SET status = ? WHERE deal_id = ?", ('cancelled', deal_id))
    conn.commit()
    conn.close()

    creator_link = f"<a href='tg://user?id={creator_id}'>@{creator_username or 'ID' + str(creator_id)}</a>"
    participant_link = f"<a href='tg://user?id={participant_id}'>@{participant_username or 'ID' + str(participant_id)}</a>" if participant_id else "Нет"
    message_text = (
        f"🚫 Сделка отменена одним из участников.\n\n"
        f"🆔 ID сделки: {deal_id}\n"
        f"📦 Тип: {get_deal_type_display(deal_type)}\n"
        f"💰 Сумма: {amount} {currency}\n"
        f"📋 Товар/Подарок: {item_links or 'Не указано'}\n"
        f"👤 Продавец: {creator_link}\n"
        f"👤 Покупатель: {participant_link}\n"
        f"✅ Успешная сделка: нет 🚫"
    )
    
    bot.send_message(GROUP_ID, message_text, message_thread_id=TOPIC_ID, parse_mode='HTML')
    bot.send_message(creator_id, message_text, parse_mode='HTML', reply_markup=get_main_menu_keyboard())

    if participant_id:
        bot.send_message(participant_id, message_text, parse_mode='HTML', reply_markup=get_main_menu_keyboard())
    
    bot.send_message(chat_id, "✅ Вы успешно покинули сделку.", reply_markup=get_main_menu_keyboard())

@bot.message_handler(func=lambda message: user_states.get(message.chat.id, {}).get('state', '').startswith('awaiting_links_'))
def handle_links(message):
    chat_id = message.chat.id
    if chat_id not in user_states:
        show_main_menu(chat_id, message.from_user.first_name)
        return
    deal_type = user_states[chat_id]['state'].split('_')[-1]
    is_valid, error_message = validate_links(deal_type, message.text)
    if not is_valid:
        sent_msg = bot.reply_to(message, error_message)
        user_states[chat_id]['prompt_message_id'] = sent_msg.message_id
        return
    user_states[chat_id]['deal_data']['links'] = message.text.strip()
    user_states[chat_id]['state'] = 'awaiting_currency'
    try:
        bot.delete_message(chat_id, user_states[chat_id]['prompt_message_id'])
        bot.delete_message(chat_id, message.message_id)
    except apihelper.ApiTelegramException:
        pass
    text = "💱 Выбор валюты\n\nУкажите валюту для сделки:"
    with open('assets/deal_photo.jpg', 'rb') as photo:
        bot.send_photo(chat_id, photo, caption=text, reply_markup=get_currency_keyboard())

@bot.message_handler(func=lambda message: user_states.get(message.chat.id, {}).get('state') == 'awaiting_amount')
def handle_amount(message):
    chat_id = message.chat.id
    if chat_id not in user_states or 'prompt_message_id' not in user_states[chat_id]:
        show_main_menu(chat_id, message.from_user.first_name)
        return
    try:
        amount = float(message.text)
        if amount <= 0:
            raise ValueError
    except ValueError:
        sent_msg = bot.reply_to(message, "⚠ Неверный формат. Введите положительное число (напр. 1000).")
        user_states[chat_id]['prompt_message_id'] = sent_msg.message_id
        return
        
    try:
        bot.delete_message(chat_id, user_states[chat_id]['prompt_message_id'])
        bot.delete_message(chat_id, message.message_id)
    except apihelper.ApiTelegramException:
        pass
        
    user_states[chat_id]['deal_data']['amount'] = amount
    deal_data = user_states[chat_id]['deal_data']
    deal_id = generate_deal_id()
    conn = sqlite3.connect('deals.db')
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO deals (deal_id, creator_id, creator_username, deal_type, item_links, currency, amount, status, creation_date)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (deal_id, message.from_user.id, message.from_user.username, deal_data['type'], deal_data.get('links'), deal_data['currency'], deal_data['amount'], 'waiting_for_participant', time.time()))
    conn.commit()
    conn.close()

    join_link = f"https://t.me/{BOT_USERNAME}?start=deal_{deal_id}"
    text = (
        f"🎉 Сделка создана!\n\n"
        f"🆔 ID сделки: {deal_id}\n"
        f"💰 Сумма: {deal_data['amount']} {deal_data['currency']}\n"
        f"🔗 Ссылка для участника:\n{join_link}\n\n"
        f"📦 После создания сделки передайте товар/подарок поддержке @SecureHomeSupport для проверки."
    )
    with open('assets/deal_photo.jpg', 'rb') as photo:
        bot.send_photo(chat_id, photo, caption=text, reply_markup=get_in_deal_keyboard(deal_id, 'waiting_for_participant'))
    del user_states[chat_id]

@bot.message_handler(func=lambda message: user_states.get(message.chat.id, {}).get('state') == 'awaiting_details_input')
def handle_details_input(message):
    chat_id = message.chat.id
    if chat_id not in user_states:
        show_main_menu(chat_id, message.from_user.first_name)
        return
    
    try:
        bot.delete_message(chat_id, user_states[chat_id].get('prompt_message_id'))
    except apihelper.ApiTelegramException:
        pass

    details_type = user_states[chat_id].get('details_type', 'default')
    details = f"{details_type}: {message.text}"
    conn = sqlite3.connect('deals.db')
    cursor = conn.cursor()
    cursor.execute("INSERT OR REPLACE INTO user_details (user_id, details) VALUES (?, ?)", (message.from_user.id, details))
    conn.commit()
    conn.close()
    bot.send_message(chat_id, "✅ Ваши реквизиты успешно сохранены!")
    
    if 'pending_deal_id' in user_states[chat_id]:
        deal_id = user_states[chat_id]['pending_deal_id']
        del user_states[chat_id]
        complete_deal_join(chat_id, message.from_user.id, message.from_user.username, deal_id)
    else:
        show_main_menu(chat_id, message.from_user.first_name)

if __name__ == '__main__':
    init_db()
    bot.polling(none_stop=True)