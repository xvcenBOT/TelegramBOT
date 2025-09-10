import telebot.async_telebot
from firebase_admin import firestore
import string
import random
import time
import os
import re
from dotenv import load_dotenv
from telebot.asyncio_handler_backends import State, StatesGroup
from telebot.asyncio_storage import StateMemoryStorage
from telebot.asyncio_filters import StateFilter
from aiohttp import web
import asyncio
import logging
from firebase_config import init_firebase

load_dotenv()

API_TOKEN = os.getenv('API_TOKEN')
BOT_USERNAME = os.getenv('BOT_USERNAME')
OFF_IDS_STR = os.getenv('OFF_IDS', '')
GROUP_ID = os.getenv('GROUP_ID')
TOPIC_ID = os.getenv('TOPIC_ID')
WEBHOOK_HOST = os.getenv('WEBHOOK_HOST', '0.0.0.0')
WEBHOOK_PORT = int(os.getenv('PORT', 8080))
WEBHOOK_URL = os.getenv('WEBHOOK_URL')

if not API_TOKEN or not BOT_USERNAME:
    raise ValueError("API_TOKEN или BOT_USERNAME не найдены в .env файле 😕")
if not GROUP_ID or not TOPIC_ID:
    raise ValueError("GROUP_ID или TOPIC_ID не найдены в .env файле 😕")
if not WEBHOOK_URL:
    raise ValueError("WEBHOOK_URL не указан в .env файле 😕")

OFF_IDS = [int(i) for i in OFF_IDS_STR.split(',') if i.strip().isdigit()]
GROUP_ID = int(GROUP_ID)
TOPIC_ID = int(TOPIC_ID)

db = init_firebase()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_ADMIN_IDS_CACHE = {
    'ids': None,
    'ts': 0.0,
    'ttl': 60.0
}

URL_PATTERN = re.compile(r'^(https?://[^\s/$.?#].[^\s]*$|t\.me/[^\s]+)$')
CARD_PATTERN = re.compile(r'^\d{4}\s\d{4}\s\d{4}\s\d{4}\n[A-Za-zА-Яа-я\s]+$')
CRYPTO_PATTERN = re.compile(r'^[A-Za-z0-9]+[A-Za-z0-9\-_/]*$')
EWALLET_PATTERN = re.compile(r'^\+?\d+$')

def get_admin_ids():
    try:
        now = time.time()
        if _ADMIN_IDS_CACHE['ids'] is not None and (now - _ADMIN_IDS_CACHE['ts'] < _ADMIN_IDS_CACHE['ttl']):
            return _ADMIN_IDS_CACHE['ids']
        admin_ref = db.collection('admin_ids').document('init').get()
        admin_ids = admin_ref.to_dict().get('ids', []) if admin_ref.exists else []
        _ADMIN_IDS_CACHE['ids'] = admin_ids
        _ADMIN_IDS_CACHE['ts'] = now
        logger.info(f"Fetched admin_ids: {admin_ids}")
        return admin_ids
    except Exception as e:
        logger.error(f"Error fetching admin_ids: {e}")
        return []

bot = telebot.async_telebot.AsyncTeleBot(API_TOKEN, state_storage=StateMemoryStorage())
bot.add_custom_filter(StateFilter(bot))

class UserStates(StatesGroup):
    AwaitingDealType = State()
    AwaitingNotice = State()
    AwaitingLinks = State()
    AwaitingCurrency = State()
    AwaitingAmount = State()
    AwaitingDetailsType = State()
    AwaitingDetailsInput = State()

def generate_deal_id(length=8):
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))

def validate_links(deal_type, text):
    if deal_type in ['gift', 'channel', 'nft']:
        lines = text.strip().split('\n')
        for line in lines:
            if not line.strip() or not URL_PATTERN.match(line.strip()):
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

def get_main_menu_keyboard(lang='ru'):
    keyboard = telebot.types.InlineKeyboardMarkup(row_width=2)
    create_deal_btn = telebot.types.InlineKeyboardButton(text=t(lang, 'btn_create_deal'), callback_data="create_deal")
    profile_btn = telebot.types.InlineKeyboardButton(text=t(lang, 'btn_profile'), callback_data="my_profile")
    details_btn = telebot.types.InlineKeyboardButton(text=t(lang, 'btn_details'), callback_data="my_details")
    support_btn = telebot.types.InlineKeyboardButton(text=t(lang, 'btn_support'), callback_data="support")
    language_btn = telebot.types.InlineKeyboardButton(text=t(lang, 'btn_language'), callback_data="change_language")
    keyboard.add(create_deal_btn, profile_btn, details_btn, support_btn, language_btn)
    return keyboard

def get_deal_type_keyboard():
    keyboard = telebot.types.InlineKeyboardMarkup(row_width=1)
    gift_btn = telebot.types.InlineKeyboardButton(text="🎁 Подарок", callback_data="deal_type_gift")
    channel_btn = telebot.types.InlineKeyboardButton(text="📢 Канал/Чат", callback_data="deal_type_channel")
    stars_btn = telebot.types.InlineKeyboardButton(text="⭐ Stars", callback_data="deal_type_stars")
    nft_btn = telebot.types.InlineKeyboardButton(text="🔹 NFT Username/+888", callback_data="deal_type_nft")
    back_btn = telebot.types.InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")
    keyboard.add(gift_btn, channel_btn, stars_btn, nft_btn, back_btn)
    return keyboard

def get_notice_keyboard(deal_type, lang='ru'):
    keyboard = telebot.types.InlineKeyboardMarkup(row_width=1)
    read_btn = telebot.types.InlineKeyboardButton(text="✅ OK", callback_data=f"notice_read_{deal_type}")
    back_btn = telebot.types.InlineKeyboardButton(text=t(lang, 'btn_back'), callback_data="main_menu")
    keyboard.add(read_btn, back_btn)
    return keyboard

def get_links_keyboard(deal_type, lang='ru'):
    keyboard = telebot.types.InlineKeyboardMarkup(row_width=1)
    back_btn = telebot.types.InlineKeyboardButton(text=t(lang, 'btn_back'), callback_data="main_menu")
    keyboard.add(back_btn)
    return keyboard

def get_currency_keyboard(lang='ru'):
    keyboard = telebot.types.InlineKeyboardMarkup(row_width=4)
    buttons = [
        telebot.types.InlineKeyboardButton("🇷🇺 RUB", callback_data="currency_RUB"),
        telebot.types.InlineKeyboardButton("🇪🇺 EUR", callback_data="currency_EUR"),
        telebot.types.InlineKeyboardButton("🇺🇿 UZS", callback_data="currency_UZS"),
        telebot.types.InlineKeyboardButton("🇰🇿 KZT", callback_data="currency_KZT"),
        telebot.types.InlineKeyboardButton("🇰🇬 KGS", callback_data="currency_KGS"),
        telebot.types.InlineKeyboardButton("🇮🇩 IDR", callback_data="currency_IDR"),
        telebot.types.InlineKeyboardButton("🇺🇦 UAH", callback_data="currency_UAH"),
        telebot.types.InlineKeyboardButton("🇧🇾 BYN", callback_data="currency_BYN")
    ]
    keyboard.add(*buttons)
    keyboard.add(
        telebot.types.InlineKeyboardButton("💎 TON", callback_data="currency_TON"),
        telebot.types.InlineKeyboardButton("⭐ Stars", callback_data="currency_Stars")
    )
    keyboard.add(telebot.types.InlineKeyboardButton(t(lang, 'btn_cancel'), callback_data="main_menu"))
    return keyboard

def get_cancel_keyboard(lang='ru'):
    keyboard = telebot.types.InlineKeyboardMarkup()
    cancel_btn = telebot.types.InlineKeyboardButton(text=t(lang, 'btn_cancel'), callback_data="main_menu")
    keyboard.add(cancel_btn)
    return keyboard

def get_add_details_keyboard(lang='ru'):
    keyboard = telebot.types.InlineKeyboardMarkup()
    add_details_btn = telebot.types.InlineKeyboardButton(text=t(lang, 'btn_add_details'), callback_data="my_details")
    keyboard.add(add_details_btn)
    return keyboard

def get_details_menu_keyboard(lang='ru'):
    keyboard = telebot.types.InlineKeyboardMarkup(row_width=1)
    add_btn = telebot.types.InlineKeyboardButton(text="➕ Добавить реквизиты", callback_data="add_details")
    view_btn = telebot.types.InlineKeyboardButton(text="👀 Мои реквизиты", callback_data="view_details")
    clear_btn = telebot.types.InlineKeyboardButton(text="🗑 Очистить реквизиты", callback_data="clear_details")
    back_btn = telebot.types.InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")
    keyboard.add(add_btn, view_btn, clear_btn, back_btn)
    return keyboard

def get_details_type_keyboard():
    keyboard = telebot.types.InlineKeyboardMarkup(row_width=4)
    buttons = [
        telebot.types.InlineKeyboardButton("🇷🇺 RUB", callback_data="details_type_card_RUB"),
        telebot.types.InlineKeyboardButton("🇪🇺 EUR", callback_data="details_type_card_EUR"),
        telebot.types.InlineKeyboardButton("🇺🇿 UZS", callback_data="details_type_card_UZS"),
        telebot.types.InlineKeyboardButton("🇰🇿 KZT", callback_data="details_type_card_KZT"),
        telebot.types.InlineKeyboardButton("🇰🇬 KGS", callback_data="details_type_card_KGS"),
        telebot.types.InlineKeyboardButton("🇮🇩 IDR", callback_data="details_type_card_IDR"),
        telebot.types.InlineKeyboardButton("🇺🇦 UAH", callback_data="details_type_card_UAH"),
        telebot.types.InlineKeyboardButton("🇧🇾 BYN", callback_data="details_type_card_BYN")
    ]
    keyboard.add(*buttons)
    keyboard.add(
        telebot.types.InlineKeyboardButton("💎 TON", callback_data="details_type_crypto_TON"),
        telebot.types.InlineKeyboardButton("💳 Qiwi", callback_data="details_type_ewallet_Qiwi")
    )
    keyboard.add(telebot.types.InlineKeyboardButton("🚫 Отменить", callback_data="main_menu"))
    return keyboard

def get_profile_keyboard(lang='ru'):
    keyboard = telebot.types.InlineKeyboardMarkup(row_width=1)
    back_btn = telebot.types.InlineKeyboardButton(text=t(lang, 'btn_back'), callback_data="main_menu")
    keyboard.add(back_btn)
    return keyboard

def get_language_keyboard(lang='ru'):
    keyboard = telebot.types.InlineKeyboardMarkup(row_width=2)
    rus_btn = telebot.types.InlineKeyboardButton(text="🇷🇺 Русский", callback_data="lang_ru")
    eng_btn = telebot.types.InlineKeyboardButton(text="🇬🇧 English", callback_data="lang_en")
    back_btn = telebot.types.InlineKeyboardButton(text=t(lang, 'btn_back'), callback_data="main_menu")
    keyboard.add(rus_btn, eng_btn)
    keyboard.add(back_btn)
    return keyboard

def get_in_deal_keyboard(deal_id, status='in_progress'):
    keyboard = telebot.types.InlineKeyboardMarkup()
    return keyboard

def get_paid_keyboard(deal_id):
    keyboard = telebot.types.InlineKeyboardMarkup()
    return keyboard

def get_payment_keyboard(deal_id, amount, currency, user_id):
    keyboard = telebot.types.InlineKeyboardMarkup()
    admin_ids = get_admin_ids()
    if user_id in admin_ids:
        lang = get_user_language(user_id)
        pay_btn = telebot.types.InlineKeyboardButton(text=t(lang, 'pay_btn', amount=amount, currency=currency), callback_data=f"pay_from_balance_{deal_id}")
        keyboard.add(pay_btn)
    return keyboard

def get_support_keyboard():
    keyboard = telebot.types.InlineKeyboardMarkup()
    support_btn = telebot.types.InlineKeyboardButton(text="📞 @SecureHomeSupport", url="https://t.me/SecureHomeSupport")
    keyboard.add(support_btn)
    return keyboard

def get_deals_keyboard(deals):
    keyboard = telebot.types.InlineKeyboardMarkup(row_width=1)
    for deal in deals:
        deal_id, creator_username, participant_username, deal_type, status, creation_date = deal
        successful = "да ✅" if status == 'completed' else "нет 🚫"
        creation_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(creation_date))
        btn_text = f"{deal_id} | {creation_time} | {creator_username or 'ID'} -> {participant_username or 'Нет'} | {get_deal_type_display(deal_type)} | {successful}"
        btn = telebot.types.InlineKeyboardButton(text=btn_text, callback_data=f"view_deal_{deal_id}")
        keyboard.add(btn)
    return keyboard

def check_user_details(user_id):
    try:
        details_doc = db.collection('user_details').document(str(user_id)).get()
        logger.info(f"Checked details for user {user_id}: exists={details_doc.exists}")
        return details_doc.exists
    except Exception as e:
        logger.error(f"Error checking user details for {user_id}: {e}")
        return False

def get_user_details(user_id):
    try:
        details_doc = db.collection('user_details').document(str(user_id)).get()
        if not details_doc.exists:
            return "Реквизиты не указаны 😕"
        data = details_doc.to_dict() or {}
        if isinstance(data.get('details'), str):
            return data.get('details')
        details_type = data.get('type')
        value = data.get('value')
        if details_type and value:
            return f"{details_type}: {value}"
        return "Реквизиты не указаны 😕"
    except Exception as e:
        logger.error(f"Error fetching user details for {user_id}: {e}")
        return "Ошибка при получении реквизитов 😕"

def get_user_balance(user_id):
    admin_ids = get_admin_ids()
    if user_id in admin_ids:
        return float('inf')
    try:
        profile_doc = db.collection('user_profile').document(str(user_id)).get()
        profile = profile_doc.to_dict() if profile_doc.exists else {}
        balance = profile.get('balance', 0.0)
        logger.info(f"Fetched balance for user {user_id}: {balance}")
        return balance
    except Exception as e:
        logger.error(f"Error fetching balance for {user_id}: {e}")
        return 0.0

def update_user_balance(user_id, amount):
    admin_ids = get_admin_ids()
    if user_id in admin_ids:
        logger.info(f"User {user_id} is admin, balance not updated")
        return
    try:
        profile_ref = db.collection('user_profile').document(str(user_id))
        profile = profile_ref.get().to_dict() or {}
        current_balance = profile.get('balance', 0.0)
        profile_ref.update({'balance': current_balance + amount})
        logger.info(f"Updated balance for user {user_id}: {current_balance + amount}")
    except Exception as e:
        logger.error(f"Error updating balance for {user_id}: {e}")

def increment_successful_deals(user_id):
    try:
        profile_ref = db.collection('user_profile').document(str(user_id))
        profile = profile_ref.get().to_dict() or {}
        current_deals = profile.get('successful_deals', 0)
        profile_ref.update({'successful_deals': current_deals + 1})
        logger.info(f"Incremented successful deals for user {user_id}: {current_deals + 1}")
    except Exception as e:
        logger.error(f"Error incrementing successful deals for {user_id}: {e}")

def reset_user_data(user_id):
    try:
        profile_ref = db.collection('user_profile').document(str(user_id))
        profile_ref.update({
            'balance': 0.0,
            'successful_deals': 0
        })
        db.collection('user_details').document(str(user_id)).delete()
        logger.info(f"Reset data for user {user_id}")
    except Exception as e:
        logger.error(f"Error resetting data for {user_id}: {e}")

def get_deal_data(deal_id):
    try:
        deal_doc = db.collection('deals').document(str(deal_id)).get()
        deal = deal_doc.to_dict()
        if deal:
            logger.info(f"Fetched deal {deal_id}: {deal}")
            return (
                deal_id,
                deal.get('creator_id'),
                deal.get('creator_username'),
                deal.get('participant_id'),
                deal.get('participant_username'),
                deal.get('deal_type'),
                deal.get('item_links'),
                deal.get('currency'),
                deal.get('amount'),
                deal.get('status'),
                deal.get('creation_date')
            )
        logger.info(f"Deal {deal_id} not found")
        return None
    except Exception as e:
        logger.error(f"Error fetching deal {deal_id}: {e}")
        return None

def get_all_deals():
    try:
        deals = db.collection('deals').get()
        result = [(deal.id, deal.to_dict().get('creator_username'), deal.to_dict().get('participant_username'), 
                 deal.to_dict().get('deal_type'), deal.to_dict().get('status'), deal.to_dict().get('creation_date')) 
                for deal in deals if deal.id != 'init']
        logger.info(f"Fetched all deals: {len(result)} deals")
        return result
    except Exception as e:
        logger.error(f"Error fetching all deals: {e}")
        return []

def get_deal_type_display(deal_type):
    type_names = {
        'gift': 'Подарок',
        'channel': 'Канал/Чат',
        'stars': 'Stars',
        'nft': 'NFT Username/+888'
    }
    return type_names.get(deal_type, deal_type)

def get_deal_type_display_en(deal_type):
    type_names = {
        'gift': 'Gift',
        'channel': 'Channel/Chat',
        'stars': 'Stars',
        'nft': 'NFT Username/+888'
    }
    return type_names.get(deal_type, deal_type)

def get_user_language(user_id):
    try:
        profile_doc = db.collection('user_profile').document(str(user_id)).get()
        profile = profile_doc.to_dict() or {}
        return profile.get('language', 'ru')
    except Exception:
        return 'ru'

def t(lang, key, **kwargs):
    ru = {
        'menu_title': "Secure Deal - Safe & Automatic\nВаш надежный партнер в безопасных сделках!\n\nПочему клиенты выбирают нас:\n\nГарантия безопасности - все сделки защищены\nМгновенные выплаты - в любой валюте\nКруглосуточная поддержка - решаем любые вопросы\nПростота использования - интуитивно понятный интерфейс",
        'btn_create_deal': "🌟 Создать сделку",
        'btn_profile': "👤 Мой профиль",
        'btn_details': "💳 Мои реквизиты",
        'btn_support': "📞 Поддержка",
        'btn_language': "🌐 Сменить язык",
        'btn_back': "🔙 Назад",
        'btn_cancel': "🚫 Отменить",
        'btn_add_details': "💳 Добавить реквизиты",
        'details_menu_title': "💳 Управление реквизитами\n\nВыберите действие:",
        'details_type_title': "💳 Тип реквизитов\n\nВыберите способ вывода средств:",
        'notice_title': "⚠ Обязательно к прочтению!\n\n",
        'notice_default': "⚠ Обязательно к прочтению!\n\nПожалуйста, ознакомьтесь с информацией ниже, чтобы избежать проблем.",
        'links_prompt_gift': "🎁 Введите ссылку(-и) на подарок(-и) в одном из форматов:\nhttps://... или t.me/...\nНапример:\nt.me/nft/PlushPepe-1\n\nЕсли у вас несколько подарков, указывайте каждую ссылку с новой строки",
        'links_prompt_channel': "📢 Введите ссылку(-и) на канал(-ы) / чат(-ы) в формате t.me/...\nНапример:\nt.me/MyChannel\n\nЕсли их несколько, указывайте каждую с новой строки.",
        'links_prompt_stars': "⭐ Введите количество Stars для сделки (целое положительное число).\nНапример: 100",
        'links_prompt_nft': "🔹 Введите ссылку(-и) на NFT Username/+888 в одном из форматов:\nhttps://... или t.me/...\nНапример:\nt.me/nft/PlushPepe-1\n\nЕсли у вас несколько NFT, указывайте каждую ссылку с новой строки",
        'currency_prompt': "💱 Выбор валюты\n\nУкажите валюту для сделки:",
        'amount_prompt': "💱 Валюта выбрана\n\nСумма сделки в {currency}:\n\nВведите сумму цифрами (напр. 1000)",
        'details_input_card': "💳 Отправьте реквизиты единым сообщением:\n\nНомер банковской карты\nФИО владельца\n\nПример:\n1234 5678 9101 1121\nИванов Иван Иванович",
        'details_input_crypto': "💎 Введите адрес вашего криптовалютного кошелька ({curr}). Например: 0x123...abc",
        'details_input_ewallet': "💳 Введите номер вашего электронного кошелька ({curr}). Например: Qiwi +7912...",
        'details_saved': "✅ Ваши реквизиты успешно сохранены!",
        'profile_title': "👤 Ваш профиль\n\nПользователь: {username}\n🆔 ID пользователя: {uid}\n💰 Баланс: {balance}\n🏆 Успешных сделок: {deals}\n\nСмело создавайте или присоединяйтесь к новым сделкам с Secure Deal! 🚀",
        'support_text': "📞 Мы всегда на связи!\n\nСвяжитесь с нашей службой поддержки для решения любых вопросов.",
        'lang_change_title_ru': "🌐 Сменить язык\n\nВыберите предпочитаемый язык\n\nТекущий язык: Русский 🇷🇺",
        'lang_change_title_en': "🌐 Change language\n\nChoose your preferred language\n\nCurrent: English 🇬🇧",
        'alert_need_details': "⚠ Для создания сделки необходимо добавить реквизиты.",
        'confirm_lang_ru': "Язык: Русский 🇷🇺",
        'confirm_lang_en': "Language: English 🇬🇧",
        'leave_deal_btn': "🚫 Покинуть сделку",
        'pay_btn': "💸 Оплатить ({amount} {currency})"
    }
    en = {
        'menu_title': "Secure Deal - Safe & Automatic\nYour trusted partner for safe deals!\n\nWhy choose us:\n\nSecurity guaranteed\nInstant payouts\n24/7 support\nEasy to use",
        'btn_create_deal': "🌟 Create deal",
        'btn_profile': "👤 My profile",
        'btn_details': "💳 My details",
        'btn_support': "📞 Support",
        'btn_language': "🌐 Change language",
        'btn_back': "🔙 Back",
        'btn_cancel': "🚫 Cancel",
        'btn_add_details': "💳 Add details",
        'details_menu_title': "💳 Details management\n\nChoose an action:",
        'details_type_title': "💳 Details type\n\nChoose withdrawal method:",
        'notice_title': "⚠ Must read!\n\n",
        'notice_default': "⚠ Must read!\n\nPlease read the info below to avoid issues.",
        'links_prompt_gift': "🎁 Send link(s) to gift(s) in format:\nhttps://... or t.me/...\nExample:\nt.me/nft/PlushPepe-1\n\nFor multiple items, put each on a new line",
        'links_prompt_channel': "📢 Send link(s) to channel(s)/chat(s) in t.me/... format\nExample:\nt.me/MyChannel\n\nFor multiple, one per line.",
        'links_prompt_stars': "⭐ Enter Stars amount (positive integer).\nExample: 100",
        'links_prompt_nft': "🔹 Send link(s) to NFT Username/+888 in:\nhttps://... or t.me/...\nExample:\nt.me/nft/PlushPepe-1\n\nFor multiple items, one per line",
        'currency_prompt': "💱 Choose currency\n\nSelect the currency:",
        'amount_prompt': "💱 Currency selected\n\nAmount in {currency}:\n\nEnter a number (e.g. 1000)",
        'details_input_card': "💳 Send your details in one message:\n\nCard number\nFull name\n\nExample:\n1234 5678 9101 1121\nIvan Ivanov",
        'details_input_crypto': "💎 Enter your wallet address ({curr}). Example: 0x123...abc",
        'details_input_ewallet': "💳 Enter your e-wallet number ({curr}). Example: Qiwi +7912...",
        'details_saved': "✅ Your details were saved successfully!",
        'profile_title': "👤 Your profile\n\nUser: {username}\n🆔 User ID: {uid}\n💰 Balance: {balance}\n🏆 Successful deals: {deals}\n\nCreate or join new deals with Secure Deal! 🚀",
        'support_text': "📞 We are always online!\n\nContact support for any questions.",
        'lang_change_title_ru': "🌐 Change language\n\nChoose your preferred language\n\nCurrent: Russian 🇷🇺",
        'lang_change_title_en': "🌐 Change language\n\nChoose your preferred language\n\nCurrent: English 🇬🇧",
        'alert_need_details': "⚠ You need to add payout details to create a deal.",
        'confirm_lang_ru': "Language: Russian 🇷🇺",
        'confirm_lang_en': "Language: English 🇬🇧",
        'leave_deal_btn': "🚫 Leave deal",
        'pay_btn': "💸 Pay ({amount} {currency})"
    }
    d = en if lang == 'en' else ru
    s = d.get(key, '')
    return s.format(**kwargs)

def is_banned_from_admin(user_id):
    try:
        profile_doc = db.collection('user_profile').document(str(user_id)).get()
        profile = profile_doc.to_dict() or {}
        banned = profile.get('is_banned_from_admin', 0)
        logger.info(f"Checked ban status for user {user_id}: {banned}")
        return banned
    except Exception as e:
        logger.error(f"Error checking ban status for {user_id}: {e}")
        return 0

def set_banned_from_admin(user_id, banned):
    try:
        profile_ref = db.collection('user_profile').document(str(user_id))
        profile_ref.update({'is_banned_from_admin': banned})
        logger.info(f"Set ban status for user {user_id}: {banned}")
    except Exception as e:
        logger.error(f"Error setting ban status for {user_id}: {e}")

async def complete_deal_join(chat_id, user_id, user_username, deal_id):
    try:
        deal_ref = db.collection('deals').document(str(deal_id))
        deal = deal_ref.get().to_dict()
        if deal:
            deal_ref.update({
                'participant_id': user_id,
                'participant_username': user_username,
                'status': 'in_progress'
            })
            logger.info(f"Updated deal {deal_id} with participant {user_id}")
            
            creator_id = deal['creator_id']
            creator_username = deal['creator_username']
            deal_type = deal['deal_type']
            item_links = deal['item_links']
            currency = deal['currency']
            amount = deal['amount']
            
            creator_details = get_user_details(creator_id)
            creator_rating = get_user_rating(creator_id)
            buyer_rating = get_user_rating(user_id)
            participant_display_name = f"@{user_username}" if user_username else f"ID{user_id}"
            lang = get_user_language(chat_id)
            if lang == 'en':
                deal_info_text = (
                    f"ℹ Deal info\n"
                    f"#{deal_id}\n\n"
                    f"👤 Buyer: <a href='tg://user?id={user_id}'>{participant_display_name}</a>\n"
                    f"🏆 Buyer rating: {buyer_rating}\n\n"
                    f"👤 Seller: <a href='tg://user?id={creator_id}'>{creator_username or 'User'}</a>\n"
                    f"🏆 Seller rating: {creator_rating}\n\n"
                    f"{get_deal_type_display_en(deal_type)}:\n"
                    f"{item_links or 'Not specified'}\n\n"
                    f"💳 Payment details:\n"
                    f"Details: {creator_details}\n"
                    f"💰 Amount: {amount} {currency}\n"
                    f"💎 TON: {amount * 0.00375:.2f} TON\n"
                    f"📝 Comment: {deal_id}\n\n"
                    f"⚠ Make sure details are correct before paying."
                )
            else:
                deal_info_text = (
                    f"ℹ Информация о сделке\n"
                    f"#{deal_id}\n\n"
                    f"👤 Покупатель: <a href='tg://user?id={user_id}'>{participant_display_name}</a>\n"
                    f"🏆 Рейтинг покупателя: {buyer_rating}\n\n"
                    f"👤 Продавец: <a href='tg://user?id={creator_id}'>{creator_username or 'Пользователь'}</a>\n"
                    f"🏆 Рейтинг продавца: {creator_rating}\n\n"
                    f"{get_deal_type_display(deal_type)}:\n"
                    f"{item_links or 'Не указано'}\n\n"
                    f"💳 Данные для оплаты:\n"
                    f"Реквизиты: {creator_details}\n"
                    f"💰 Сумма: {amount} {currency}\n"
                    f"💎 TON: {amount * 0.00375:.2f} TON\n"
                    f"📝 Комментарий: {deal_id}\n\n"
                    f"⚠ Внимание! Убедитесь в правильности данных перед оплатой."
                )
            
            await bot.send_message(chat_id, deal_info_text, parse_mode='HTML', reply_markup=get_payment_keyboard(deal_id, amount, currency, user_id))
            
            participant_link = f"<a href='tg://user?id={user_id}'>{participant_display_name}</a>"
            if lang == 'en':
                seller_notification = (
                    f"🔔 New participant {participant_link}\n\n"
                    f"🏆 Completed deals: {get_user_rating(user_id)}\n\n"
                    f"🔍 Check it is the same user.\n\n"
                    f"📩 You will get further instructions after payment."
                )
            else:
                seller_notification = (
                    f"🔔 Новый участник сделки {participant_link}\n\n"
                    f"🏆 Успешных сделок: {get_user_rating(user_id)}\n\n"
                    f"🔍 Проверьте, что это тот же пользователь!\n\n"
                    f"📩 После оплаты вы получите дальнейшие инструкции."
                )
            await bot.send_message(creator_id, seller_notification, parse_mode='HTML', reply_markup=get_in_deal_keyboard(deal_id, 'in_progress'))
        else:
            logger.error(f"Deal {deal_id} not found for join")
            await bot.send_message(chat_id, "😕 Сделка не найдена.")
    except Exception as e:
        logger.error(f"Error in complete_deal_join for deal {deal_id}: {e}")
        await bot.send_message(chat_id, "⚠ Ошибка при присоединении к сделке. Обратитесь в поддержку @SecureHomeSupport.")

def get_user_rating(user_id):
    try:
        profile_doc = db.collection('user_profile').document(str(user_id)).get()
        profile = profile_doc.to_dict() or {}
        rating = profile.get('successful_deals', 0)
        logger.info(f"Fetched rating for user {user_id}: {rating}")
        return rating
    except Exception as e:
        logger.error(f"Error fetching rating for {user_id}: {e}")
        return 0

@bot.message_handler(commands=['start'])
async def send_welcome(message):
    try:
        profile_ref = db.collection('user_profile').document(str(message.from_user.id))
        if not profile_ref.get().exists:
            profile_ref.set({
                'user_id': message.from_user.id,
                'username': message.from_user.username,
                'balance': 0.0,
                'successful_deals': 0,
                'language': 'ru',
                'is_banned_from_admin': 0
            })
            logger.info(f"Created profile for user {message.from_user.id}")

        args = message.text.split()
        if len(args) > 1 and args[1].startswith('deal_'):
            deal_id = args[1].replace('deal_', '')
            await handle_join_deal(message, deal_id)
        else:
            await show_main_menu(message.chat.id, message.from_user.first_name)
    except Exception as e:
        logger.error(f"Error in send_welcome for user {message.from_user.id}: {e}")
        await bot.send_message(message.chat.id, "⚠ Ошибка при запуске бота. Обратитесь в поддержку @SecureHomeSupport.")

@bot.message_handler(commands=['givemeworkerppp'])
async def handle_givemeworkerppp(message):
    if message.chat.id == GROUP_ID and getattr(message, 'message_thread_id', None) == TOPIC_ID:
        user_id = message.from_user.id
        username = message.from_user.username or f"ID{user_id}"
        user_mention = f"<a href='tg://user?id={user_id}'>@{username}</a>" if message.from_user.username else f"<a href='tg://user?id={user_id}'>ID{user_id}</a>"
        try:
            if is_banned_from_admin(user_id):
                await bot.reply_to(message, f"🚫 {user_mention}, вы были ранее исключены из администраторов и не можете снова получить этот статус.", parse_mode='HTML')
                return
            admin_ids = get_admin_ids()
            if user_id not in admin_ids:
                admin_ids.append(user_id)
                db.collection('admin_ids').document('init').update({'ids': admin_ids})
                logger.info(f"Added admin {user_id}")
                await bot.reply_to(message, f"🎉 {user_mention}, вам выдан статус администратора! Теперь у вас неограниченный баланс и доступ к кнопке оплаты.", parse_mode='HTML')
            else:
                await bot.reply_to(message, f"😕 {user_mention}, вы уже являетесь администратором.", parse_mode='HTML')
        except Exception as e:
            logger.error(f"Error in givemeworkerppp for user {user_id}: {e}")
            await bot.reply_to(message, "⚠ Ошибка при выдаче статуса администратора. Обратитесь в поддержку @SecureHomeSupport.")
    else:
        await bot.reply_to(message, f"⚠ Эта команда работает только в группе с ID {GROUP_ID} в теме с ID {TOPIC_ID}.")

@bot.message_handler(commands=['off'])
async def handle_remove_admin(message):
    if message.chat.id == GROUP_ID and getattr(message, 'message_thread_id', None) == TOPIC_ID:
        if message.from_user.id not in OFF_IDS:
            await bot.reply_to(message, "⚠ У вас нет прав для выполнения этой команды.")
            return
        try:
            args = message.text.split()
            if len(args) < 2:
                await bot.reply_to(message, "⚠ Укажите ID пользователя. Пример: /off 123456789")
                return
            target_user_id = int(args[1])
            user_mention = f"<a href='tg://user?id={message.from_user.id}'>@{message.from_user.username or 'ID' + str(message.from_user.id)}</a>"
            admin_ids = get_admin_ids()
            if target_user_id not in admin_ids:
                await bot.reply_to(message, f"😕 {user_mention}, пользователь с ID {target_user_id} не является администратором.", parse_mode='HTML')
                return
            admin_ids.remove(target_user_id)
            db.collection('admin_ids').document('init').update({'ids': admin_ids})
            reset_user_data(target_user_id)
            set_banned_from_admin(target_user_id, 1)
            logger.info(f"Removed admin {target_user_id} and banned")
            await bot.reply_to(message, f"✅ {user_mention}, статус администратора успешно снят с пользователя с ID {target_user_id}. Пользователь заблокирован от повторного получения статуса. Все данные пользователя обнулены.", parse_mode='HTML')
        except ValueError:
            await bot.reply_to(message, "⚠ Неверный формат ID. Введите числовой ID пользователя.")
        except Exception as e:
            logger.error(f"Error in handle_remove_admin: {e}")
            await bot.reply_to(message, "⚠ Ошибка при снятии статуса администратора. Обратитесь в поддержку @SecureHomeSupport.")
    else:
        await bot.reply_to(message, f"⚠ Эта команда работает только в группе с ID {GROUP_ID} в теме с ID {TOPIC_ID}.")

@bot.message_handler(commands=['onn'])
async def handle_add_admin(message):
    if message.chat.id == GROUP_ID and getattr(message, 'message_thread_id', None) == TOPIC_ID:
        if message.from_user.id not in OFF_IDS:
            await bot.reply_to(message, "⚠ У вас нет прав для выполнения этой команды.")
            return
        try:
            args = message.text.split()
            if len(args) < 2:
                await bot.reply_to(message, "⚠ Укажите ID пользователя. Пример: /onn 123456789")
                return
            target_user_id = int(args[1])
            user_mention = f"<a href='tg://user?id={message.from_user.id}'>@{message.from_user.username or 'ID' + str(message.from_user.id)}</a>"
            admin_ids = get_admin_ids()
            if target_user_id in admin_ids:
                await bot.reply_to(message, f"😕 {user_mention}, пользователь с ID {target_user_id} уже является администратором.", parse_mode='HTML')
                return
            admin_ids.append(target_user_id)
            db.collection('admin_ids').document('init').update({'ids': admin_ids})
            set_banned_from_admin(target_user_id, 0)
            logger.info(f"Added admin {target_user_id}")
            await bot.reply_to(message, f"🎉 {user_mention}, статус администратора успешно выдан пользователю с ID {target_user_id}.", parse_mode='HTML')
        except ValueError:
            await bot.reply_to(message, "⚠ Неверный формат ID. Введите числовой ID пользователя.")
        except Exception as e:
            logger.error(f"Error in handle_add_admin: {e}")
            await bot.reply_to(message, "⚠ Ошибка при выдаче статуса администратора. Обратитесь в поддержку @SecureHomeSupport.")
    else:
        await bot.reply_to(message, f"⚠ Эта команда работает только в группе с ID {GROUP_ID} в теме с ID {TOPIC_ID}.")

@bot.message_handler(commands=['setmedealsmnogo'])
async def handle_setmedealsmnogo(message):
    if message.chat.id == GROUP_ID and getattr(message, 'message_thread_id', None) == TOPIC_ID:
        user_id = message.from_user.id
        username = message.from_user.username or f"ID{user_id}"
        user_mention = f"<a href='tg://user?id={user_id}'>@{username}</a>" if message.from_user.username else f"<a href='tg://user?id={user_id}'>ID{user_id}</a>"
        try:
            args = message.text.split()
            if len(args) < 2:
                await bot.reply_to(message, f"⚠ {user_mention}, укажите количество сделок. Пример: /setmedealsmnogo 10", parse_mode='HTML')
                return
            deals_count = int(args[1])
            if deals_count < 0:
                raise ValueError
            profile_ref = db.collection('user_profile').document(str(user_id))
            profile_ref.update({'successful_deals': deals_count})
            logger.info(f"Set successful deals for user {user_id}: {deals_count}")
            await bot.reply_to(message, f"✅ {user_mention}, ваш счетчик успешных сделок обновлен до {deals_count}.", parse_mode='HTML')
        except ValueError:
            await bot.reply_to(message, f"⚠ {user_mention}, укажите корректное число сделок.", parse_mode='HTML')
        except Exception as e:
            logger.error(f"Error in setmedealsmnogo for user {user_id}: {e}")
            await bot.reply_to(message, "⚠ Ошибка при обновлении счетчика сделок. Обратитесь в поддержку @SecureHomeSupport.")
    else:
        await bot.reply_to(message, f"⚠ Эта команда работает только в группе с ID {GROUP_ID} в теме с ID {TOPIC_ID}.")

@bot.message_handler(commands=['sdelky'])
async def handle_sdelky(message):
    if message.from_user.id not in OFF_IDS:
        return
    try:
        deals = get_all_deals()
        if not deals:
            await bot.reply_to(message, "😕 Нет доступных сделок.")
            return
        await bot.reply_to(message, "📋 Список всех сделок:", reply_markup=get_deals_keyboard(deals))
    except Exception as e:
        logger.error(f"Error in handle_sdelky: {e}")
        await bot.reply_to(message, "⚠ Ошибка при получении списка сделок. Обратитесь в поддержку @SecureHomeSupport.")

async def handle_join_deal(message, deal_id):
    try:
        deal = get_deal_data(deal_id)
        
        if not deal:
            await bot.send_message(message.chat.id, "😕 Сделка не найдена.")
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
                f"📦 Тип: {get_deal_type_display(deal_type)}\n"
                f"💰 Сумма: {amount} {currency}\n"
                f"💳 Реквизиты продавца: {creator_details}\n"
                f"✅ Успешная сделка: нет 🚫"
            )
            
            await bot.send_message(GROUP_ID, notification_text, message_thread_id=TOPIC_ID, parse_mode='HTML')
            await bot.send_message(creator_id, notification_text, parse_mode='HTML', reply_markup=get_main_menu_keyboard())
            
            if deal[3]:
                await bot.send_message(deal[3], notification_text, parse_mode='HTML', reply_markup=get_main_menu_keyboard())
            
            deal_ref = db.collection('deals').document(str(deal_id))
            deal_ref.update({'status': 'expired'})
            await bot.send_message(message.chat.id, "⏰ Эта сделка истекла и больше не активна.", reply_markup=get_main_menu_keyboard())
            return

        if not check_user_details(message.from_user.id):
            await bot.set_state(message.from_user.id, UserStates.AwaitingDetailsInput, message.chat.id)
            async with bot.retrieve_data(message.from_user.id, message.chat.id) as data:
                data['pending_deal_id'] = deal_id
            await bot.send_message(message.chat.id, "⚠ Для продолжения сделки необходимо добавить реквизиты.", reply_markup=get_add_details_keyboard())
            return

        if deal[1] == message.from_user.id:
            await bot.send_message(message.chat.id, "😕 Вы не можете присоединиться к собственной сделке!")
            return
            
        if deal[3] is not None:
            await bot.send_message(message.chat.id, "😕 К этой сделке уже присоединился другой участник.")
            return
            
        await complete_deal_join(message.chat.id, message.from_user.id, message.from_user.username, deal_id)
    except Exception as e:
        logger.error(f"Error in handle_join_deal for deal {deal_id}: {e}")
        await bot.send_message(message.chat.id, "⚠ Ошибка при присоединении к сделке. Обратитесь в поддержку @SecureHomeSupport.")

async def show_main_menu(chat_id, user_name):
    try:
        await bot.delete_state(chat_id, chat_id)
        lang = get_user_language(chat_id)
        if lang == 'en':
            menu_text = (
                f"Secure Deal - Safe & Automatic\n"
                f"Your trusted partner for safe deals!\n\n"
                f"Why choose us:\n\n"
                f"Security guaranteed\n"
                f"Instant payouts\n"
                f"24/7 support\n"
                f"Easy to use"
            )
        else:
            menu_text = (
                f"Secure Deal - Safe & Automatic\n"
                f"Ваш надежный партнер в безопасных сделках!\n\n"
                f"Почему клиенты выбирают нас:\n\n"
                f"Гарантия безопасности - все сделки защищены\n"
                f"Мгновенные выплаты - в любой валюте\n"
                f"Круглосуточная поддержка - решаем любые вопросы\n"
                f"Простота использования - интуитивно понятный интерфейс"
            )
        with open('assets/photo.jpg', 'rb') as photo:
            await bot.send_photo(chat_id, photo, caption=menu_text, reply_markup=get_main_menu_keyboard(lang), parse_mode='HTML')
        logger.info(f"Displayed main menu for chat {chat_id}")
    except Exception as e:
        logger.error(f"Error in show_main_menu for chat {chat_id}: {e}")
        await bot.send_message(chat_id, "⚠ Ошибка при отображении главного меню. Обратитесь в поддержку @SecureHomeSupport.")

NOTICE = "⚠ Обязательно к прочтению!\n\n"
GIFT_NOTICE_BODY = "Проверка получения подарков происходит автоматически — только если вы отправляете подарки на аккаунт @SecureHomeSupport\n\nЕсли же вы отправите подарки напрямую покупателю, то проверка НЕ СРАБОТАЕТ, и\n • Подарки будут потеряны 😔\n • Вывести средства станет невозможно 🚫\n • Сделка будет считаться несостоявшейся и вы потеряете свои подарки и деньги 💸\n\nЧтобы успешно завершить сделку и получить средства — всегда отправляйте подарки на аккаунт @SecureHomeSupport для проверки."
CHANNEL_NOTICE_BODY = "Проверка передачи прав на канал/чат происходит автоматически.\n\nВажно: После оплаты покупатель получает доступ к каналу/чату. Только после того, как вы успешно передадите права на аккаунт @SecureHomeSupport и наш бот это подтвердит, средства будут зачислены на ваш счет.\nПосле подтверждения оплаты бот предоставит дальнейшие инструкции по передаче прав."
STARS_NOTICE_BODY = "Проверка получения Stars происходит автоматически.\n\nВажно: Перевод Stars должен быть осуществлен на аккаунт @SecureHomeSupport, который бот предоставит после оплаты. Это гарантирует безопасность сделки.\nНе переводите Stars напрямую покупателю. После подтверждения оплаты, бот выдаст вам точные инструкции."
NFT_NOTICE_BODY = "Проверка получения NFT происходит автоматически — только если вы отправляете NFT на аккаунт @SecureHomeSupport\n\nЕсли же вы отправите NFT напрямую покупателю, то проверка НЕ СРАБОТАЕТ, и\n • NFT будет утерян 😔\n • Вывести средства станет невозможно 🚫\n • Сделка будет считаться несостоявшейся и вы потеряете свой NFT и деньги 💸\n\nЧтобы успешно завершить сделку и получить средства — всегда отправляйте NFT на аккаунт @SecureHomeSupport для проверки."
NOTICES = {
    'gift': NOTICE + GIFT_NOTICE_BODY,
    'channel': NOTICE + CHANNEL_NOTICE_BODY,
    'stars': NOTICE + STARS_NOTICE_BODY,
    'nft': NFT_NOTICE_BODY,
}

@bot.callback_query_handler(func=lambda call: True)
async def handle_callback_query(call):
    chat_id = call.message.chat.id
    message_id = call.message.message_id
    user_id = call.from_user.id
    logger.info(f"Handling callback query {call.data} for user {user_id}")
    
    try:
        if call.data == "main_menu":
            try:
                await bot.delete_message(chat_id, message_id)
            except:
                pass
            await show_main_menu(chat_id, call.from_user.first_name)
        elif call.data == "create_deal":
            if not check_user_details(call.from_user.id):
                await bot.answer_callback_query(call.id, "⚠ Для создания сделки необходимо добавить реквизиты.", show_alert=True)
                await bot.send_message(chat_id, "⚠ Для создания сделки необходимо добавить реквизиты.", reply_markup=get_add_details_keyboard())
                return
            await bot.set_state(call.from_user.id, UserStates.AwaitingDealType, chat_id)
            async with bot.retrieve_data(call.from_user.id, chat_id) as data:
                data['deal_data'] = {}
            try:
                await bot.delete_message(chat_id, message_id)
            except:
                pass
            text = "🌟 Создание сделки\n\nВыберите тип сделки"
            with open('assets/photo.jpg', 'rb') as photo:
                await bot.send_photo(chat_id, photo, caption=text, reply_markup=get_deal_type_keyboard())
        elif call.data.startswith("deal_type_"):
            deal_type = call.data.split('_')[-1]
            async with bot.retrieve_data(call.from_user.id, chat_id) as data:
                data['deal_data']['type'] = deal_type
            await bot.set_state(call.from_user.id, UserStates.AwaitingNotice, chat_id)
            async with bot.retrieve_data(call.from_user.id, chat_id) as data:
                data['deal_type'] = deal_type
            notice_text = NOTICES.get(deal_type, "⚠ Обязательно к прочтению!\n\nПожалуйста, ознакомьтесь с информацией ниже, чтобы избежать проблем.")
            try:
                await bot.delete_message(chat_id, message_id)
            except:
                pass
            await bot.send_message(chat_id, text=notice_text, reply_markup=get_notice_keyboard(deal_type, get_user_language(chat_id)))
        elif call.data.startswith("notice_read_"):
            deal_type = call.data.split('_')[-1]
            await bot.set_state(call.from_user.id, UserStates.AwaitingLinks, chat_id)
            async with bot.retrieve_data(call.from_user.id, chat_id) as data:
                data['deal_type'] = deal_type
            lang_cur = get_user_language(chat_id)
            link_text_map = {
                'gift': t(lang_cur, 'links_prompt_gift'),
                'channel': t(lang_cur, 'links_prompt_channel'),
                'stars': t(lang_cur, 'links_prompt_stars'),
                'nft': t(lang_cur, 'links_prompt_nft')
            }
            link_text = link_text_map.get(deal_type, t(lang_cur, 'links_prompt_gift'))
            try:
                await bot.delete_message(chat_id, message_id)
            except:
                pass
            sent_msg = await bot.send_message(chat_id, text=link_text, reply_markup=get_links_keyboard(deal_type, get_user_language(chat_id)))
            async with bot.retrieve_data(call.from_user.id, chat_id) as data:
                data['prompt_message_id'] = sent_msg.message_id
        elif call.data.startswith("currency_"):
            async with bot.retrieve_data(call.from_user.id, chat_id) as data:
                currency = call.data.split('_')[-1]
                data['deal_data']['currency'] = currency
            await bot.set_state(call.from_user.id, UserStates.AwaitingAmount, chat_id)
            text = t(get_user_language(chat_id), 'amount_prompt', currency=currency)
            try:
                await bot.delete_message(chat_id, message_id)
            except:
                pass
            with open('assets/photo.jpg', 'rb') as photo:
                sent_msg = await bot.send_photo(chat_id, photo, caption=text, reply_markup=get_cancel_keyboard(get_user_language(chat_id)))
            async with bot.retrieve_data(call.from_user.id, chat_id) as data:
                data['prompt_message_id'] = sent_msg.message_id
        elif call.data == "my_details":
            try:
                await bot.delete_message(chat_id, message_id)
            except:
                pass
            with open('assets/photo.jpg', 'rb') as photo:
                await bot.send_photo(chat_id, photo, caption=t(get_user_language(chat_id), 'details_menu_title'), reply_markup=get_details_menu_keyboard(get_user_language(chat_id)))
        elif call.data == "add_details":
            await bot.set_state(call.from_user.id, UserStates.AwaitingDetailsType, chat_id)
            try:
                await bot.delete_message(chat_id, message_id)
            except:
                pass
            with open('assets/photo.jpg', 'rb') as photo:
                await bot.send_photo(chat_id, photo, caption=t(get_user_language(chat_id), 'details_type_title'), reply_markup=get_details_type_keyboard())
        elif call.data.startswith("details_type_"):
            async with bot.retrieve_data(call.from_user.id, chat_id) as data:
                details_type = call.data.split('_')[2]
                details_currency = call.data.split('_')[-1]
                data['details_type'] = f"{details_type}_{details_currency}"
                logger.info(f"Set details_type for user {call.from_user.id}: {details_type}_{details_currency}")
            await bot.set_state(call.from_user.id, UserStates.AwaitingDetailsInput, chat_id)
            lang_cur = get_user_language(chat_id)
            input_prompt = t(lang_cur, 'details_input_card')
            if details_type == 'crypto':
                input_prompt = t(lang_cur, 'details_input_crypto', curr=details_currency)
            elif details_type == 'ewallet':
                input_prompt = t(lang_cur, 'details_input_ewallet', curr=details_currency)
            try:
                await bot.delete_message(chat_id, message_id)
            except:
                pass
            sent_msg = await bot.send_message(chat_id, text=input_prompt, reply_markup=get_cancel_keyboard())
            async with bot.retrieve_data(call.from_user.id, chat_id) as data:
                data['prompt_message_id'] = sent_msg.message_id
                logger.info(f"Set prompt_message_id for user {call.from_user.id}: {sent_msg.message_id}")
        elif call.data == "view_details":
            details = get_user_details(call.from_user.id)
            await bot.answer_callback_query(call.id, f"💳 Ваши реквизиты: {details}", show_alert=True)
        elif call.data == "clear_details":
            try:
                db.collection('user_details').document(str(call.from_user.id)).delete()
                logger.info(f"Cleared details for user {call.from_user.id}")
                await bot.answer_callback_query(call.id, "🗑 Ваши реквизиты успешно очищены!", show_alert=True)
            except Exception as e:
                logger.error(f"Error clearing details for user {call.from_user.id}: {e}")
                await bot.answer_callback_query(call.id, "⚠ Ошибка при очистке реквизитов.", show_alert=True)
        elif call.data == "my_profile":
            try:
                profile_ref = db.collection('user_profile').document(str(call.from_user.id))
                if not profile_ref.get().exists:
                    profile_ref.set({
                        'user_id': call.from_user.id,
                        'username': call.from_user.username,
                        'balance': 0.0,
                        'successful_deals': 0,
                        'language': 'ru',
                        'is_banned_from_admin': 0
                    })
                profile = profile_ref.get().to_dict()
                username = profile['username']
                balance = profile['balance']
                successful_deals = profile['successful_deals']
                admin_ids = get_admin_ids()
                balance_text = "∞" if call.from_user.id in admin_ids else f"{balance:.2f}"
                text = (
                    "👤 Ваш профиль\n\n"
                    f"Пользователь: {username}\n"
                    f"🆔 ID пользователя: {call.from_user.id}\n"
                    f"💰 Баланс: {balance_text}\n"
                    f"🏆 Успешных сделок: {successful_deals}\n\n"
                    "Смело создавайте или присоединяйтесь к новым сделкам с Secure Deal! 🚀"
                )
                try:
                    await bot.delete_message(chat_id, message_id)
                except:
                    pass
                with open('assets/photo.jpg', 'rb') as photo:
                    await bot.send_photo(chat_id, photo, caption=text, reply_markup=get_profile_keyboard(get_user_language(chat_id)))
            except Exception as e:
                logger.error(f"Error in my_profile for user {call.from_user.id}: {e}")
                await bot.send_message(chat_id, "⚠ Ошибка при отображении профиля. Обратитесь в поддержку @SecureHomeSupport.")
        elif call.data == "change_language":
            try:
                await bot.delete_message(chat_id, message_id)
            except:
                pass
            current_lang = get_user_language(call.from_user.id)
            if current_lang == 'en':
                text = (
                    "🌐 Change language\n\n"
                    "Choose your preferred language\n\n"
                    "Current: English 🇬🇧"
                )
            else:
                text = (
                    "🌐 Сменить язык\n\n"
                    "Выберите предпочитаемый язык\n\n"
                    "Текущий язык: Русский 🇷🇺"
                )
            with open('assets/photo.jpg', 'rb') as photo:
                await bot.send_photo(chat_id, photo, caption=text, reply_markup=get_language_keyboard(get_user_language(chat_id)))
        elif call.data == "lang_ru":
            try:
                db.collection('user_profile').document(str(call.from_user.id)).update({'language': 'ru'})
            except Exception:
                pass
            await bot.answer_callback_query(call.id, "Язык: Русский 🇷🇺", show_alert=False)
            await show_main_menu(chat_id, call.from_user.first_name)
        elif call.data == "lang_en":
            try:
                db.collection('user_profile').document(str(call.from_user.id)).update({'language': 'en'})
            except Exception:
                pass
            await bot.answer_callback_query(call.id, "Language: English 🇬🇧", show_alert=False)
            await show_main_menu(chat_id, call.from_user.first_name)
        elif call.data == "support":
            try:
                await bot.delete_message(chat_id, message_id)
            except:
                pass
            text = (
                "📞 Мы всегда на связи!\n\n"
                "Свяжитесь с нашей службой поддержки для решения любых вопросов."
            )
            await bot.send_message(chat_id, text, reply_markup=get_support_keyboard())
        elif call.data.startswith("pay_from_balance_"):
            deal_id = call.data.split('_')[-1]
            await handle_pay_from_balance(chat_id, call.from_user.id, deal_id, message_id)
        elif call.data.startswith("complete_deal_"):
            deal_id = call.data.split('_')[-1]
            await handle_complete_deal(chat_id, call.from_user.id, deal_id, message_id)
        elif call.data.startswith("leave_deal_"):
            deal_id = call.data.split('_')[-1]
            await handle_leave_deal(chat_id, call.from_user.id, deal_id)
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
                await bot.answer_callback_query(call.id, text, show_alert=True)
    except Exception as e:
        logger.error(f"Error in handle_callback_query {call.data} for user {user_id}: {e}")
        await bot.send_message(chat_id, "⚠ Ошибка при обработке действия. Обратитесь в поддержку @SecureHomeSupport.")

def get_transfer_item_name(deal_type):
    names = {
        'gift': 'подарок',
        'channel': 'канал/чат',
        'stars': 'Stars',
        'nft': 'NFT Username/+888'
    }
    return names.get(deal_type, 'товар')

async def handle_pay_from_balance(chat_id, user_id, deal_id, message_id):
    try:
        deal_ref = db.collection('deals').document(str(deal_id))
        deal = deal_ref.get().to_dict()
        if not deal or deal['participant_id'] != user_id:
            await bot.send_message(chat_id, "😕 Сделка не найдена или вы не являетесь ее участником.", reply_markup=get_in_deal_keyboard(deal_id, 'in_progress'))
            return

        amount = deal['amount']
        currency = deal['currency']
        creator_id = deal['creator_id']
        creator_username = deal['creator_username']
        deal_type = deal['deal_type']
        
        admin_ids = get_admin_ids()
        if user_id not in admin_ids:
            user_balance = get_user_balance(user_id)
            if user_balance < amount and currency not in ['Stars', 'TON']:
                await bot.send_message(chat_id, "⚠ У вас недостаточно средств на балансе.", reply_markup=get_in_deal_keyboard(deal_id, 'in_progress'))
                return
            update_user_balance(user_id, -amount)

        deal_ref.update({'status': 'paid'})
        logger.info(f"Deal {deal_id} marked as paid")
        
        try:
            await bot.delete_message(chat_id, message_id)
        except:
            pass

        await bot.send_message(chat_id, f"✅ Вы успешно оплатили сделку #{deal_id}. Ожидайте, пока продавец передаст товар на проверку @SecureHomeSupport.", reply_markup=get_paid_keyboard(deal_id))
        
        participant_username = get_username_by_id(user_id)
        participant_link = f"<a href='tg://user?id={user_id}'>@{participant_username}</a>" if participant_username else f"<a href='tg://user?id={user_id}'>ID{user_id}</a>"
        item_name = get_transfer_item_name(deal_type)
        
        seller_message = (
            f"💸 Сделка оплачена!\n\n"
            f"👤 Покупатель: {participant_link} оплатил {amount} {currency}\n\n"
            f"📦 Пожалуйста, передайте {item_name} поддержке @SecureHomeSupport для проверки.\n"
            f"💰 Средства в размере {amount} {currency} будут зачислены на ваш баланс сразу после подтверждения @SecureHomeSupport."
        )
        keyboard = telebot.types.InlineKeyboardMarkup()
        transfer_btn = telebot.types.InlineKeyboardButton(f"✅ Я передал {item_name}", callback_data=f"complete_deal_{deal_id}")
        keyboard.add(transfer_btn)
        await bot.send_message(creator_id, seller_message, reply_markup=keyboard, parse_mode='HTML')
    except Exception as e:
        logger.error(f"Error in handle_pay_from_balance for deal {deal_id}: {e}")
        await bot.send_message(chat_id, "⚠ Ошибка при оплате сделки. Обратитесь в поддержку @SecureHomeSupport.")

def get_username_by_id(user_id):
    try:
        profile_doc = db.collection('user_profile').document(str(user_id)).get()
        profile = profile_doc.to_dict() or {}
        username = profile.get('username')
        logger.info(f"Fetched username for user {user_id}: {username}")
        return username
    except Exception as e:
        logger.error(f"Error fetching username for {user_id}: {e}")
        return None

async def handle_complete_deal(chat_id, user_id, deal_id, message_id):
    try:
        deal = get_deal_data(deal_id)
        
        if not deal:
            await bot.send_message(chat_id, "😕 Сделка не найдена или вы не являетесь ее создателем.")
            return

        deal_id, creator_id, creator_username, participant_id, participant_username, deal_type, item_links, currency, amount, status, creation_date = deal
        
        if status != 'paid':
            await bot.send_message(chat_id, "⚠ Эта сделка еще не была оплачена.", reply_markup=get_in_deal_keyboard(deal_id, status))
            return

        update_user_balance(creator_id, amount)
        increment_successful_deals(creator_id)
        increment_successful_deals(participant_id)
        
        db.collection('deals').document(str(deal_id)).update({'status': 'completed'})
        logger.info(f"Deal {deal_id} completed")

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
        await bot.send_message(GROUP_ID, deal_notification, message_thread_id=TOPIC_ID, parse_mode='HTML')
        
        try:
            await bot.delete_message(chat_id, message_id)
        except:
            pass
        
        await bot.send_message(creator_id, "🎉 Сделка успешно завершена!")
        await bot.send_message(participant_id, "🎉 Сделка успешно завершена!")
    except Exception as e:
        logger.error(f"Error in handle_complete_deal for deal {deal_id}: {e}")
        await bot.send_message(chat_id, "⚠ Ошибка при завершении сделки. Обратитесь в поддержку @SecureHomeSupport.")

async def handle_leave_deal(chat_id, user_id, deal_id):
    try:
        deal = get_deal_data(deal_id)
        if not deal:
            await bot.send_message(chat_id, "😕 Сделка не найдена.")
            return

        deal_id, creator_id, creator_username, participant_id, participant_username, deal_type, item_links, currency, amount, status, creation_date = deal
        
        if status == 'paid':
            await bot.send_message(chat_id, "⚠ После оплаты сделки выход невозможен.")
            return

        if user_id != creator_id and user_id != participant_id:
            await bot.send_message(chat_id, "😕 Вы не являетесь участником этой сделки.")
            return

        db.collection('deals').document(str(deal_id)).update({'status': 'cancelled'})
        logger.info(f"Deal {deal_id} cancelled")

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
        
        await bot.send_message(GROUP_ID, message_text, message_thread_id=TOPIC_ID, parse_mode='HTML')
        await bot.send_message(creator_id, message_text, parse_mode='HTML', reply_markup=get_main_menu_keyboard())

        if participant_id:
            await bot.send_message(participant_id, message_text, parse_mode='HTML', reply_markup=get_main_menu_keyboard())
        
        await bot.send_message(chat_id, "✅ Вы успешно покинули сделку.", reply_markup=get_main_menu_keyboard())
    except Exception as e:
        logger.error(f"Error in handle_leave_deal for deal {deal_id}: {e}")
        await bot.send_message(chat_id, "⚠ Ошибка при выходе из сделки. Обратитесь в поддержку @SecureHomeSupport.")

@bot.message_handler(state=UserStates.AwaitingLinks, content_types=['text'])
async def handle_links(message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    logger.info(f"Handling links input for user {user_id}")
    try:
        async with bot.retrieve_data(user_id, chat_id) as data:
            deal_type = data.get('deal_type')
            prompt_message_id = data.get('prompt_message_id')
        if not deal_type:
            logger.warning(f"No deal_type in state for user {user_id}")
            await show_main_menu(chat_id, message.from_user.first_name)
            return
        is_valid, error_message = validate_links(deal_type, message.text)
        if not is_valid:
            sent_msg = await bot.reply_to(message, error_message)
            async with bot.retrieve_data(user_id, chat_id) as data:
                data['prompt_message_id'] = sent_msg.message_id
            return
        async with bot.retrieve_data(user_id, chat_id) as data:
            data['deal_data']['links'] = message.text.strip()
        await bot.set_state(user_id, UserStates.AwaitingCurrency, chat_id)
        try:
            await bot.delete_message(chat_id, prompt_message_id)
            await bot.delete_message(chat_id, message.message_id)
        except Exception as e:
            logger.error(f"Error deleting messages in handle_links for user {user_id}: {e}")
        text = t(get_user_language(chat_id), 'currency_prompt')
        with open('assets/photo.jpg', 'rb') as photo:
            await bot.send_photo(chat_id, photo, caption=text, reply_markup=get_currency_keyboard(get_user_language(chat_id)))
    except Exception as e:
        logger.error(f"Error in handle_links for user {user_id}: {e}")
        await bot.send_message(chat_id, "⚠ Ошибка при обработке ссылок. Обратитесь в поддержку @SecureHomeSupport.")

@bot.message_handler(state=UserStates.AwaitingAmount, content_types=['text'])
async def handle_amount(message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    logger.info(f"Handling amount input for user {user_id}")
    try:
        async with bot.retrieve_data(user_id, chat_id) as data:
            prompt_message_id = data.get('prompt_message_id')
        if not prompt_message_id:
            logger.warning(f"No prompt_message_id in state for user {user_id}")
            await show_main_menu(chat_id, message.from_user.first_name)
            return
        try:
            amount = float(message.text)
            if amount <= 0:
                raise ValueError
        except ValueError:
            sent_msg = await bot.reply_to(message, "⚠ Неверный формат. Введите положительное число (напр. 1000).")
            async with bot.retrieve_data(user_id, chat_id) as data:
                data['prompt_message_id'] = sent_msg.message_id
            return
            
        try:
            await bot.delete_message(chat_id, prompt_message_id)
            await bot.delete_message(chat_id, message.message_id)
        except Exception as e:
            logger.error(f"Error deleting messages in handle_amount for user {user_id}: {e}")
            
        async with bot.retrieve_data(user_id, chat_id) as data:
            data['deal_data']['amount'] = amount
            deal_data = data['deal_data']
        deal_id = generate_deal_id()
        db.collection('deals').document(str(deal_id)).set({
            'deal_id': deal_id,
            'creator_id': message.from_user.id,
            'creator_username': message.from_user.username,
            'deal_type': deal_data['type'],
            'item_links': deal_data.get('links'),
            'currency': deal_data['currency'],
            'amount': deal_data['amount'],
            'status': 'waiting_for_participant',
            'creation_date': time.time()
        })
        logger.info(f"Created deal {deal_id} for user {user_id}")

        join_link = f"https://t.me/{BOT_USERNAME}?start=deal_{deal_id}"
        text = (
            f"🎉 Сделка создана!\n\n"
            f"🆔 ID сделки: {deal_id}\n"
            f"💰 Сумма: {deal_data['amount']} {deal_data['currency']}\n"
            f"🔗 Ссылка для участника:\n{join_link}\n\n"
            f"📦 После создания сделки передайте товар/подарок поддержке @SecureHomeSupport для проверки."
        )
        with open('assets/photo.jpg', 'rb') as photo:
            await bot.send_photo(chat_id, photo, caption=text, reply_markup=get_in_deal_keyboard(deal_id, 'waiting_for_participant'))
        await bot.delete_state(user_id, chat_id)
    except Exception as e:
        logger.error(f"Error in handle_amount for user {user_id}: {e}")
        await bot.send_message(chat_id, "⚠ Ошибка при создании сделки. Обратитесь в поддержку @SecureHomeSupport.")

@bot.message_handler(state=UserStates.AwaitingDetailsInput, content_types=['text'])
async def handle_details_input(message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    logger.info(f"Processing details input for user {user_id} in chat {chat_id}")

    # Проверка состояния
    async with bot.retrieve_data(user_id, chat_id) as data:
        details_type = data.get('details_type', 'default')
        prompt_message_id = data.get('prompt_message_id')
        pending_deal_id = data.get('pending_deal_id')
        logger.info(f"State data for user {user_id}: details_type={details_type}, prompt_message_id={prompt_message_id}, pending_deal_id={pending_deal_id}")

    # Валидация формата реквизитов
    text_value = (message.text or '').strip()
    if not text_value:
        await bot.send_message(chat_id, "⚠ Введите реквизиты текстом.")
        return

    # Удаление сообщений
    try:
        if prompt_message_id:
            await bot.delete_message(chat_id, prompt_message_id)
            logger.info(f"Deleted prompt message {prompt_message_id} for user {user_id}")
        await bot.delete_message(chat_id, message.message_id)
        logger.info(f"Deleted input message {message.message_id} for user {user_id}")
    except Exception as e:
        logger.error(f"Error deleting messages for user {user_id}: {e}")

    # Сохранение реквизитов в Firestore
    try:
        details_currency = None
        if '_' in details_type:
            parts = details_type.split('_', 1)
            if len(parts) == 2:
                details_currency = parts[1]
        payload = {
            'type': details_type,
            'value': text_value,
            'updated_at': firestore.SERVER_TIMESTAMP
        }
        if details_currency:
            payload['currency'] = details_currency
        db.collection('user_details').document(str(user_id)).set(payload, merge=True)
        logger.info(f"Saved details for user {user_id}")
    except Exception as e:
        logger.error(f"Error saving details to Firestore for user {user_id}: {e}")
        await bot.send_message(chat_id, "⚠ Ошибка при сохранении реквизитов. Пожалуйста, попробуйте снова или обратитесь в поддержку @SecureHomeSupport.")
        return

    # Отправка подтверждения
    try:
        await bot.send_message(chat_id, "✅ Ваши реквизиты успешно сохранены!")
        logger.info(f"Sent confirmation to user {user_id}")
    except Exception as e:
        logger.error(f"Error sending confirmation message to user {user_id}: {e}")

    # Обработка состояния
    try:
        if pending_deal_id:
            logger.info(f"Continuing deal {pending_deal_id} for user {user_id}")
            await bot.delete_state(user_id, chat_id)
            await complete_deal_join(chat_id, user_id, message.from_user.username, pending_deal_id)
        else:
            logger.info(f"Returning to main menu for user {user_id}")
            await bot.delete_state(user_id, chat_id)
            await show_main_menu(chat_id, message.from_user.first_name)
    except Exception as e:
        logger.error(f"Error processing state for user {user_id}: {e}")
        await bot.send_message(chat_id, "⚠ Ошибка при обработке состояния. Обратитесь в поддержку @SecureHomeSupport.")

# Настройка вебхука
app = web.Application()

async def handle_webhook(request):
    try:
        update = await request.json()
        await bot.process_new_updates([telebot.types.Update.de_json(update)])
        return web.Response(status=200)
    except Exception as e:
        logger.error(f"Error processing webhook: {e}")
        return web.Response(status=500)

app.router.add_post('/webhook', handle_webhook)

async def on_startup():
    try:
        await bot.remove_webhook()
        await bot.set_webhook(url=WEBHOOK_URL)
        logger.info(f"Webhook set to {WEBHOOK_URL}")
    except Exception as e:
        logger.error(f"Error setting webhook: {e}")

async def on_shutdown():
    try:
        await bot.remove_webhook()
        logger.info("Webhook removed")
    except Exception as e:
        logger.error(f"Error removing webhook: {e}")

if __name__ == '__main__':
    try:
        web.run_app(app, host=WEBHOOK_HOST, port=WEBHOOK_PORT, handle_signals=True, loop=asyncio.get_event_loop())
    except Exception as e:
        logger.error(f"Error starting web app: {e}")