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

# Лёгкие кэши для ускорения повторных обращений
_LANG_CACHE = {'map': {}, 'ttl': 60.0}           # user_id -> (lang, ts)
_PROFILE_CACHE = {'map': {}, 'ttl': 30.0}        # user_id -> (profile_dict, ts)
_DETAILS_CACHE = {'map': {}, 'ttl': 30.0}        # user_id -> (details_text, ts)

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
        
        if admin_ref.exists:
            admin_ids = admin_ref.to_dict().get('ids', [])
        else:
            # Если документ не существует, создаем его с пустым массивом
            db.collection('admin_ids').document('init').set({'ids': []})
            admin_ids = []
        
        _ADMIN_IDS_CACHE['ids'] = admin_ids
        _ADMIN_IDS_CACHE['ts'] = now
        logger.info(f"Fetched admin_ids: {admin_ids}")
        return admin_ids
        
    except Exception as e:
        logger.error(f"Error fetching admin_ids: {e}")
        return []  # Возвращаем пустой массив в случае ошибки

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
        # Поддержка нескольких ссылок, разделённых пробелами и/или переводами строк
        parts = [p.strip() for p in re.split(r"\s+", (text or '').strip()) if p.strip()]
        for part in parts:
            if not URL_PATTERN.match(part):
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
    support_btn = telebot.types.InlineKeyboardButton(text=t(lang, 'btn_support'), url="https://t.me/GiftGuarantHelp")
    language_btn = telebot.types.InlineKeyboardButton(text=t(lang, 'btn_language'), callback_data="change_language")
    keyboard.add(create_deal_btn, profile_btn, details_btn, support_btn, language_btn)
    return keyboard

def get_deal_type_keyboard():
    keyboard = telebot.types.InlineKeyboardMarkup(row_width=2)
    gift_btn = telebot.types.InlineKeyboardButton(text="🎁 Подарок", callback_data="deal_type_gift")
    channel_btn = telebot.types.InlineKeyboardButton(text="📣 Канал / Чат", callback_data="deal_type_channel")
    stars_btn = telebot.types.InlineKeyboardButton(text="⭐ Звёзды", callback_data="deal_type_stars")
    nft_btn = telebot.types.InlineKeyboardButton(text="🔸 NFT username / +888", callback_data="deal_type_nft")
    back_btn = telebot.types.InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu")
    keyboard.add(gift_btn, channel_btn)
    keyboard.add(stars_btn, nft_btn)
    keyboard.add(back_btn)
    return keyboard

def get_notice_keyboard(deal_type, lang='ru'):
    keyboard = telebot.types.InlineKeyboardMarkup(row_width=1)
    read_btn = telebot.types.InlineKeyboardButton(text="👌 OK", callback_data=f"notice_read_{deal_type}")
    back_btn = telebot.types.InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu")
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
        now = time.time()
        cached = _DETAILS_CACHE['map'].get(user_id)
        if cached and now - cached[1] < _DETAILS_CACHE['ttl']:
            return cached[0]
        details_doc = db.collection('user_details').document(str(user_id)).get()
        if not details_doc.exists:
            result = "Реквизиты не указаны 😕"
            _DETAILS_CACHE['map'][user_id] = (result, now)
            return result
        data = details_doc.to_dict() or {}
        if isinstance(data.get('details'), str):
            result = data.get('details')
            _DETAILS_CACHE['map'][user_id] = (result, now)
            return result
        details_type = data.get('type')
        value = data.get('value')
        if details_type and value:
            result = f"{details_type}: {value}"
            _DETAILS_CACHE['map'][user_id] = (result, now)
            return result
        result = "Реквизиты не указаны 😕"
        _DETAILS_CACHE['map'][user_id] = (result, now)
        return result
    except Exception as e:
        logger.error(f"Error fetching user details for {user_id}: {e}")
        return "Ошибка при получении реквизитов 😕"

def get_user_balance(user_id):
    admin_ids = get_admin_ids()
    if user_id in admin_ids:
        return float('inf')
    try:
        now = time.time()
        cached_profile = _PROFILE_CACHE['map'].get(user_id)
        if cached_profile and now - cached_profile[1] < _PROFILE_CACHE['ttl']:
            balance = (cached_profile[0] or {}).get('balance', 0.0)
            logger.info(f"Fetched balance from cache for user {user_id}: {balance}")
            return balance
        profile_doc = db.collection('user_profile').document(str(user_id)).get()
        profile = profile_doc.to_dict() if profile_doc.exists else {}
        _PROFILE_CACHE['map'][user_id] = (profile, now)
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
        _PROFILE_CACHE['map'].pop(user_id, None)
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
        _PROFILE_CACHE['map'].pop(user_id, None)
        _DETAILS_CACHE['map'].pop(user_id, None)
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
        now = time.time()
        cached = _LANG_CACHE['map'].get(user_id)
        if cached and now - cached[1] < _LANG_CACHE['ttl']:
            return cached[0]
        profile_doc = db.collection('user_profile').document(str(user_id)).get()
        profile = profile_doc.to_dict() or {}
        lang = profile.get('language', 'ru')
        _LANG_CACHE['map'][user_id] = (lang, now)
        return lang
    except Exception:
        return 'ru'

async def send_video_without_sound(chat_id, video_path, caption=None, reply_markup=None, parse_mode=None):
    """Отправляет видео без звука"""
    try:
        with open(video_path, 'rb') as video:
            # Пытаемся отправить как анимацию (GIF) - это гарантирует отсутствие звука
            try:
                return await bot.send_animation(chat_id, video, caption=caption, reply_markup=reply_markup, parse_mode=parse_mode)
            except:
                # Если не получается как анимация, отправляем как видео
                return await bot.send_video(chat_id, video, caption=caption, reply_markup=reply_markup, parse_mode=parse_mode, supports_streaming=True)
    except Exception as e:
        logger.error(f"Error sending video {video_path} to chat {chat_id}: {e}")
        # Fallback - отправляем сообщение без видео
        if caption:
            return await bot.send_message(chat_id, caption, reply_markup=reply_markup, parse_mode=parse_mode)
        return None

async def edit_video_message(chat_id, message_id, video_path=None, caption=None, reply_markup=None, parse_mode=None):
    """Редактирует текущее сообщение бота: сначала пытается заменить медиа, затем подпись.
    Если заменить медиа не удается (например, сообщение не медиа), пробует обновить подпись.
    Возвращает True при успешном редактировании, иначе False.
    """
    try:
        if video_path:
            try:
                with open(video_path, 'rb') as f:
                    media = telebot.types.InputMediaVideo(f, caption=caption, parse_mode=parse_mode)
                    await bot.edit_message_media(media=media, chat_id=chat_id, message_id=message_id, reply_markup=reply_markup)
                    return True
            except Exception:
                # Если не получилось заменить медиа, пробуем обновить подпись
                pass
        # Обновление подписи, если медиа менять не нужно или не получилось
        await bot.edit_message_caption(chat_id=chat_id, message_id=message_id, caption=caption, reply_markup=reply_markup, parse_mode=parse_mode)
        return True
    except Exception:
        return False

def t(lang, key, **kwargs):
    ru = {
        'menu_title': "👋 *Добро пожаловать!*\n\n🔐 *Надёжный сервис для безопасных сделок!*\n⚡ *Автоматизировано, быстро и без лишних хлопот!*\n\n*Теперь ваши сделки под защитой!* 🔒",
        'btn_create_deal': "💼 Создать сделку",
        'btn_profile': "👤 Мой профиль",
        'btn_details': "💳 Мои реквизиты",
        'btn_support': "📞 Поддержка",
        'btn_language': "🌍 Сменить язык",
        'btn_back': "🔙 Назад",
        'btn_cancel': "🚫 Отменить",
        'btn_add_details': "💳 Добавить реквизиты",
        'details_menu_title': "💳 *Управление реквизитами*\n\n*Выберите действие:*",
        'details_type_title': "💳 *Тип реквизитов*\n\n*Выберите способ вывода средств:*",
        'notice_title': "⚠ Обязательно к прочтению!\n\n",
        'notice_default': "⚠ Обязательно к прочтению!\n\nПожалуйста, ознакомьтесь с информацией ниже, чтобы избежать проблем.",
        'links_prompt_gift': "🎁 *Введите ссылку(-и) на подарок(-и) в одном из форматов:*\nhttps://ссылка или t.me/ссылка\nНапример:\nt.me/nft/PlushPepe-1\n\n*Если у вас несколько подарков, указывайте каждую ссылку с новой строки*",
        'links_prompt_channel': "📢 *Введите ссылку(-и) на канал(-ы) / чат(-ы) в формате t.me/ссылка*\nНапример:\nt.me/MyChannel\n\n*Если их несколько, указывайте каждую с новой строки.*",
        'links_prompt_stars': "⭐ *Введите количество Stars для сделки (целое положительное число).*\nНапример: 100",
        'links_prompt_nft': "🔹 *Введите ссылку(-и) на NFT Username/+888 в одном из форматов:*\nhttps://ссылка или t.me/ссылка\nНапример:\nt.me/nft/PlushPepe-1\n\n*Если у вас несколько NFT, указывайте каждую ссылку с новой строки*",
        'currency_prompt': "💬 *Выберите валюту для сделки!*",
        'amount_prompt': "💱 *Валюта выбрана*\n\n*Сумма сделки в {currency}:*\n\n*Введите сумму цифрами (напр. 1000)*",
        'details_input_card': "💳 *Отправьте реквизиты единым сообщением:*\n\n*Номер банковской карты*\n*ФИО владельца*\n\nПример:\n1234 5678 9101 1121\nИванов Иван Иванович",
        'details_input_crypto': "💎 *Введите адрес вашего криптовалютного кошелька ({curr}).* Например: 0x123...abc",
        'details_input_ewallet': "💳 *Введите номер вашего электронного кошелька ({curr}).* Например: Qiwi +7912...",
        'details_saved': "✅ *Ваши реквизиты успешно сохранены!*",
        'profile_title': "*👤 Ваш профиль*\n\n*👋 Пользователь:* {username}\n*💰 Баланс:* {balance}\n*🏆 Успешных сделок:* {deals}\n\n🚀 *Осуществляйте новые сделки с Secure Deal — с нами вы можете быть уверены в надежности и честности каждой операции. ⚡️*",
        'lang_change_title_ru': "🌐 *Сменить язык*\n\n*Выберите предпочитаемый язык*\n\nТекущий язык: Русский 🇷🇺",
        'lang_change_title_en': "🌐 *Change language*\n\n*Choose your preferred language*\n\nCurrent: English 🇬🇧",
        'alert_need_details': "⚠ Для создания сделки необходимо добавить реквизиты.",
        'confirm_lang_ru': "Язык: Русский 🇷🇺",
        'confirm_lang_en': "Language: English 🇬🇧",
        'leave_deal_btn': "🚫 Покинуть сделку",
        'pay_btn': "💸 Оплатить ({amount} {currency})"
    }
    en = {
        'menu_title': "👋 *Welcome!*\n\n🔐 *Reliable service for safe deals!*\n⚡ *Automated, fast and hassle-free!*\n\n*Now your deals are protected!* 🔒",
        'btn_create_deal': "💼 Create deal",
        'btn_profile': "👤 My profile",
        'btn_details': "💳 My details",
        'btn_support': "📞 Support",
        'btn_language': "🌍 Change language",
        'btn_back': "🔙 Back",
        'btn_cancel': "🚫 Cancel",
        'btn_add_details': "💳 Add details",
        'details_menu_title': "💳 *Details management*\n\n*Choose an action:*",
        'details_type_title': "💳 *Details type*\n\n*Choose withdrawal method:*",
        'notice_title': "⚠ Must read!\n\n",
        'notice_default': "⚠ Must read!\n\nPlease read the info below to avoid issues.",
        'links_prompt_gift': "🎁 *Send link(s) to gift(s) in format:*\nhttps://link or t.me/link\nExample:\nt.me/nft/PlushPepe-1\n\n*For multiple items, put each on a new line*",
        'links_prompt_channel': "📢 *Send link(s) to channel(s)/chat(s) in t.me/link format*\nExample:\nt.me/MyChannel\n\n*For multiple, one per line.*",
        'links_prompt_stars': "⭐ *Enter Stars amount (positive integer).*\nExample: 100",
        'links_prompt_nft': "🔹 *Send link(s) to NFT Username/+888 in:*\nhttps://link or t.me/link\nExample:\nt.me/nft/PlushPepe-1\n\n*For multiple items, one per line*",
        'currency_prompt': "💬 *Choose a currency for the deal!*",
        'amount_prompt': "💱 *Currency selected*\n\n*Amount in {currency}:*\n\n*Enter a number (e.g. 1000)*",
        'details_input_card': "💳 *Send your details in one message:*\n\n*Card number*\n*Full name*\n\nExample:\n1234 5678 9101 1121\nIvan Ivanov",
        'details_input_crypto': "💎 *Enter your wallet address ({curr}).* Example: 0x123...abc",
        'details_input_ewallet': "💳 *Enter your e-wallet number ({curr}).* Example: Qiwi +7912...",
        'details_saved': "✅ *Your details were saved successfully!*",
        'profile_title': "👤 Your profile\n\nUser: {username}\n🆔 User ID: {uid}\n💰 Balance: {balance}\n🏆 Successful deals: {deals}\n\nCreate or join new deals with Secure Deal! 🚀",
        'lang_change_title_ru': "🌐 *Change language*\n\n*Choose your preferred language*\n\nCurrent: Russian 🇷🇺",
        'lang_change_title_en': "🌐 *Change language*\n\n*Choose your preferred language*\n\nCurrent: English 🇬🇧",
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
            
            # Экранирование специальных символов
            def escape_markdown_v2(text):
                if not text:
                    return text
                
                # Сначала убираем уже существующие экранирования, чтобы избежать двойного экранирования
                text = text.replace('\\', '')
                
                escape_chars = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}']
                for char in escape_chars:
                    text = text.replace(char, f'\\{char}')
                return text
            
            participant_display_name = f"@{user_username}" if user_username else f"ID{user_id}"
            creator_display_name = f"@{creator_username}" if creator_username else f"ID{creator_id}"
            
            escaped_participant_name = escape_markdown_v2(participant_display_name)
            escaped_creator_name = escape_markdown_v2(creator_display_name)
            escaped_creator_details = escape_markdown_v2(creator_details)
            escaped_item_links = escape_markdown_v2(item_links or 'Не указано')
            
            lang = get_user_language(chat_id)
            def escape_html(text):
                if text is None:
                    return ''
                return str(text).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')

            participant_link_html = f"<a href='tg://user?id={user_id}'>" + escape_html(participant_display_name) + "</a>"
            creator_link_html = f"<a href='tg://user?id={creator_id}'>" + escape_html(creator_display_name) + "</a>"
            item_links_html = escape_html(item_links or 'Не указано')
            creator_details_html = escape_html(creator_details)

            if lang == 'en':
                deal_info_text = (
                    f"ℹ <b>Deal info</b>\n"
                    f"<code>#{deal_id}</code>\n\n"
                    f"👤 Buyer: {participant_link_html}\n"
                    f"🏆 Buyer rating: <b>{buyer_rating}</b>\n\n"
                    f"👤 Seller: {creator_link_html}\n"
                    f"🏆 Seller rating: <b>{creator_rating}</b>\n\n"
                    f"<b>{escape_html(get_deal_type_display_en(deal_type))}:</b>\n"
                    f"{item_links_html}\n\n"
                    f"💳 <b>Payment details:</b>\n"
                    f"Details: {creator_details_html}\n"
                    f"💰 Amount: <b>{amount} {currency}</b>\n"
                    f"💎 TON: <b>{amount * 0.00375:.2f} TON</b>\n"
                    f"📝 Comment: <code>{deal_id}</code>\n\n"
                    f"⚠ <b>Make sure details are correct before paying!</b>"
                )
            else:
                deal_info_text = (
                    f"ℹ <b>Информация о сделке</b>\n"
                    f"<code>#{deal_id}</code>\n\n"
                    f"👤 Покупатель: {participant_link_html}\n"
                    f"🏆 Рейтинг покупателя: <b>{buyer_rating}</b>\n\n"
                    f"👤 Продавец: {creator_link_html}\n"
                    f"🏆 Рейтинг продавца: <b>{creator_rating}</b>\n\n"
                    f"<b>{escape_html(get_deal_type_display(deal_type))}:</b>\n"
                    f"{item_links_html}\n\n"
                    f"💳 <b>Данные для оплаты:</b>\n"
                    f"Реквизиты: {creator_details_html}\n"
                    f"💰 Сумма: <b>{amount} {currency}</b>\n"
                    f"💎 TON: <b>{amount * 0.00375:.2f} TON</b>\n"
                    f"📝 Комментарий: <code>{deal_id}</code>\n\n"
                    f"⚠ <b>Внимание! Убедитесь в правильности данных перед оплатой!</b>"
                )
            
            await send_video_without_sound(
                chat_id, 
                'assets/6.mp4', 
                caption=deal_info_text, 
                parse_mode='HTML', 
                reply_markup=get_payment_keyboard(deal_id, amount, currency, user_id)
            )
            
            participant_link = f"[{escaped_participant_name}](tg://user?id={user_id})"
            if lang == 'en':
                seller_notification = (
                    f"👤 <b>New participant!</b> {participant_link_html}\n\n"
                    f"🏆 <b>Completed deals:</b> {get_user_rating(user_id)}\n\n"
                    f"🔍 <b>Check it is the same user.</b>\n\n"
                    f"⏳ <b>You will get further instructions after payment.</b>"
                )
            else:
                seller_notification = (
                    f"👤 <b>Новый участник сделки!</b> {participant_link_html}\n\n"
                    f"🏆 <b>Успешных сделок:</b> {get_user_rating(user_id)}\n\n"
                    f"🔍 <b>Проверьте, что это тот же пользователь!</b>\n\n"
                    f"⏳ <b>После оплаты вы получите дальнейшие инструкции.</b>"
                )
            
            await send_video_without_sound(
                creator_id, 
                'assets/6.mp4', 
                caption=seller_notification, 
                parse_mode='HTML', 
                reply_markup=get_in_deal_keyboard(deal_id, 'in_progress')
            )
        else:
            logger.error(f"Deal {deal_id} not found for join")
            await bot.send_message(chat_id, "😕 Сделка не найдена.")
    except Exception as e:
        logger.error(f"Error in complete_deal_join for deal {deal_id}: {e}")
        await bot.send_message(chat_id, "⚠ Ошибка при присоединении к сделке. Обратитесь в поддержку @GiftGuarantHelp.")

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
        await bot.send_message(message.chat.id, "⚠ Ошибка при запуске бота. Обратитесь в поддержку @GiftGuarantHelp.")

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
            await bot.reply_to(message, "⚠ Ошибка при выдаче статуса администратора. Обратитесь в поддержку @GiftGuarantHelp.")
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
            await bot.reply_to(message, "⚠ Ошибка при снятии статуса администратора. Обратитесь в поддержку @GiftGuarantHelp.")
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
            await bot.reply_to(message, "⚠ Ошибка при выдаче статуса администратора. Обратитесь в поддержку @GiftGuarantHelp.")
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
            await bot.reply_to(message, "⚠ Ошибка при обновлении счетчика сделок. Обратитесь в поддержку @GiftGuarantHelp.")
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
        await bot.reply_to(message, "⚠ Ошибка при получении списка сделок. Обратитесь в поддержку @GiftGuarantHelp.")

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
            await send_video_without_sound(creator_id, 'assets/1.mp4', caption=notification_text, parse_mode='HTML', reply_markup=get_main_menu_keyboard())
            
            if deal[3]:
                await send_video_without_sound(deal[3], 'assets/1.mp4', caption=notification_text, parse_mode='HTML', reply_markup=get_main_menu_keyboard())
            
            deal_ref = db.collection('deals').document(str(deal_id))
            deal_ref.update({'status': 'expired'})
            await send_video_without_sound(message.chat.id, 'assets/1.mp4', caption="⏰ Эта сделка истекла и больше не активна.", reply_markup=get_main_menu_keyboard())
            return

        if not check_user_details(message.from_user.id):
            await bot.set_state(message.from_user.id, UserStates.AwaitingDetailsInput, message.chat.id)
            async with bot.retrieve_data(message.from_user.id, message.chat.id) as data:
                data['pending_deal_id'] = deal_id
            await send_video_without_sound(message.chat.id, 'assets/1.mp4', caption="⚠ Для продолжения сделки необходимо добавить реквизиты.", reply_markup=get_add_details_keyboard())
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
        await bot.send_message(message.chat.id, "⚠ Ошибка при присоединении к сделке. Обратитесь в поддержку @GiftGuarantHelp.")

async def show_main_menu(chat_id, user_name):
    try:
        await bot.delete_state(chat_id, chat_id)
        lang = get_user_language(chat_id)
        menu_text = t(lang, 'menu_title')
        
        await send_video_without_sound(chat_id, 'assets/1.mp4', caption=menu_text, reply_markup=get_main_menu_keyboard(lang), parse_mode='Markdown')
        
        logger.info(f"Displayed main menu for chat {chat_id}")
    except Exception as e:
        logger.error(f"Error in show_main_menu for chat {chat_id}: {e}")
        await bot.send_message(chat_id, "⚠ Ошибка при отображении главного меню. Обратитесь в поддержку @GiftGuarantHelp.")

NOTICE = "⚠ Обязательно к прочтению!\n\n"
GIFT_NOTICE_BODY = (
    "*‼️ Важная информация!*\n\n"
    "*Проверка получения подарков осуществляется автоматически только при отправке на аккаунт @GiftGuarantHelp.*\n\n"
    "*Если NFT username/+888 отправлены напрямую покупателю:*\n"
    "• Они будут утеряны 😔\n"
    "• Сделка будет считаться несостоявшейся, что приведет к потери username/+888 и денежных средств 💸\n\n"
    "Для успешного завершения сделки и получения средств обязательно отправляйте подарки на аккаунт @GiftGuarantHelp для проверки."
)
CHANNEL_NOTICE_BODY = (
    "*‼️ Важная информация!*\n\n"
    "*Проверка получения подарков осуществляется автоматически только при отправке на аккаунт @GiftGuarantHelp.*\n\n"
    "*Если Каналы/Чаты отправлены напрямую покупателю:*\n"
    "• Они будут утеряны 😔\n"
    "• Сделка будет считаться несостоявшейся, что приведет к потери Каналов/Чатов и денежных средств 💸\n\n"
    "Для успешного завершения сделки и получения средств обязательно отправляйте подарки на аккаунт @GiftGuarantHelp для проверки."
)
STARS_NOTICE_BODY = (
    "*‼️ Важная информация!*\n\n"
    "*Проверка получения подарков осуществляется автоматически только при отправке на аккаунт @GiftGuarantHelp.*\n\n"
    "*Если звёзды отправлены напрямую покупателю:*\n"
    "• Они будут утеряны 😔\n"
    "• Сделка будет считаться несостоявшейся, что приведет к потери звёзд и денежных средств 💸\n\n"
    "Для успешного завершения сделки и получения средств обязательно отправляйте подарки на аккаунт @GiftGuarantHelp для проверки."
)
NFT_NOTICE_BODY = GIFT_NOTICE_BODY
NOTICES = {
    'gift': GIFT_NOTICE_BODY,
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
            # Меняем текущее сообщение на главное меню
            lang = get_user_language(chat_id)
            menu_text = t(lang, 'menu_title')
            edited = await edit_video_message(chat_id, message_id, video_path='assets/1.mp4', caption=menu_text, reply_markup=get_main_menu_keyboard(lang), parse_mode='Markdown')
            if not edited:
                await show_main_menu(chat_id, call.from_user.first_name)
        elif call.data == "create_deal":
            if not check_user_details(call.from_user.id):
                await bot.answer_callback_query(call.id, "⚠ Для создания сделки необходимо добавить реквизиты.", show_alert=True)
                await send_video_without_sound(chat_id, 'assets/1.mp4', caption="⚠ Для создания сделки необходимо добавить реквизиты.", reply_markup=get_add_details_keyboard())
                return
            await bot.set_state(call.from_user.id, UserStates.AwaitingDealType, chat_id)
            async with bot.retrieve_data(call.from_user.id, chat_id) as data:
                data['deal_data'] = {}
            text = "💭 *Выберите тип сделки!*"
            if not await edit_video_message(chat_id, message_id, video_path='assets/2.mp4', caption=text, reply_markup=get_deal_type_keyboard(), parse_mode='Markdown'):
                await send_video_without_sound(chat_id, 'assets/2.mp4', caption=text, reply_markup=get_deal_type_keyboard(), parse_mode='Markdown')
        elif call.data.startswith("deal_type_"):
            deal_type = call.data.split('_')[-1]
            # Гарантируем наличие deal_data
            async with bot.retrieve_data(call.from_user.id, chat_id) as data:
                if 'deal_data' not in data or not isinstance(data.get('deal_data'), dict):
                    data['deal_data'] = {}
                data['deal_data']['type'] = deal_type
            await bot.set_state(call.from_user.id, UserStates.AwaitingNotice, chat_id)
            async with bot.retrieve_data(call.from_user.id, chat_id) as data:
                data['deal_type'] = deal_type
            notice_text = NOTICES.get(deal_type, "⚠ Обязательно к прочтению!\n\nПожалуйста, ознакомьтесь с информацией ниже, чтобы избежать проблем.")
            if not await edit_video_message(chat_id, message_id, video_path='assets/1.mp4', caption=notice_text, reply_markup=get_notice_keyboard(deal_type, get_user_language(chat_id)), parse_mode='Markdown'):
                await send_video_without_sound(chat_id, 'assets/1.mp4', caption=notice_text, reply_markup=get_notice_keyboard(deal_type, get_user_language(chat_id)), parse_mode='Markdown')
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
            if not await edit_video_message(chat_id, message_id, video_path='assets/3.mp4', caption=link_text, reply_markup=get_links_keyboard(deal_type, get_user_language(chat_id)), parse_mode='Markdown'):
                sent_msg = await send_video_without_sound(chat_id, 'assets/3.mp4', caption=link_text, reply_markup=get_links_keyboard(deal_type, get_user_language(chat_id)), parse_mode='Markdown')
                async with bot.retrieve_data(call.from_user.id, chat_id) as data:
                    data['prompt_message_id'] = sent_msg.message_id
            else:
                # Сохраняем текущий message_id как промпт, если редактирование прошло успешно
                async with bot.retrieve_data(call.from_user.id, chat_id) as data:
                    data['prompt_message_id'] = message_id
        elif call.data.startswith("currency_"):
            async with bot.retrieve_data(call.from_user.id, chat_id) as data:
                currency = call.data.split('_')[-1]
                data['deal_data']['currency'] = currency
            await bot.set_state(call.from_user.id, UserStates.AwaitingAmount, chat_id)
            text = t(get_user_language(chat_id), 'amount_prompt', currency=currency)
            if not await edit_video_message(chat_id, message_id, video_path='assets/3.mp4', caption=text, reply_markup=get_cancel_keyboard(get_user_language(chat_id)), parse_mode='Markdown'):
                sent_msg = await send_video_without_sound(chat_id, 'assets/3.mp4', caption=text, reply_markup=get_cancel_keyboard(get_user_language(chat_id)), parse_mode='Markdown')
                async with bot.retrieve_data(call.from_user.id, chat_id) as data:
                    data['prompt_message_id'] = sent_msg.message_id
            else:
                async with bot.retrieve_data(call.from_user.id, chat_id) as data:
                    data['prompt_message_id'] = message_id
        elif call.data == "my_details":
            if not await edit_video_message(chat_id, message_id, video_path='assets/7.mp4', caption=t(get_user_language(chat_id), 'details_menu_title'), reply_markup=get_details_menu_keyboard(get_user_language(chat_id)), parse_mode='Markdown'):
                await send_video_without_sound(chat_id, 'assets/7.mp4', caption=t(get_user_language(chat_id), 'details_menu_title'), reply_markup=get_details_menu_keyboard(get_user_language(chat_id)), parse_mode='Markdown')
        elif call.data == "add_details":
            await bot.set_state(call.from_user.id, UserStates.AwaitingDetailsType, chat_id)
            if not await edit_video_message(chat_id, message_id, video_path='assets/7.mp4', caption=t(get_user_language(chat_id), 'details_type_title'), reply_markup=get_details_type_keyboard(), parse_mode='Markdown'):
                await send_video_without_sound(chat_id, 'assets/7.mp4', caption=t(get_user_language(chat_id), 'details_type_title'), reply_markup=get_details_type_keyboard(), parse_mode='Markdown')
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
            if not await edit_video_message(chat_id, message_id, video_path='assets/7.mp4', caption=input_prompt, reply_markup=get_cancel_keyboard(), parse_mode='Markdown'):
                sent_msg = await send_video_without_sound(chat_id, 'assets/7.mp4', caption=input_prompt, reply_markup=get_cancel_keyboard(), parse_mode='Markdown')
                async with bot.retrieve_data(call.from_user.id, chat_id) as data:
                    data['prompt_message_id'] = sent_msg.message_id
                    logger.info(f"Set prompt_message_id for user {call.from_user.id}: {sent_msg.message_id}")
            else:
                async with bot.retrieve_data(call.from_user.id, chat_id) as data:
                    data['prompt_message_id'] = message_id
                    logger.info(f"Set prompt_message_id for user {call.from_user.id}: {message_id}")
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
                username = profile.get('username', 'Без имени')
                balance = profile.get('balance', 0.0)
                successful_deals = profile.get('successful_deals', 0)
                
                admin_ids = get_admin_ids()
                balance_text = "∞" if call.from_user.id in admin_ids else f"{balance:.2f}"
                
                # Экранируем специальные символы; добавляем @ к username
                display_username = f"@{username}" if username else 'Без имени'
                escaped_username = escape_markdown_v2(display_username)
                
                text = (
                    "*👤 Ваш профиль*\n\n"
                    f"*👋 Пользователь:* {escaped_username}\n"
                    f"*💰 Баланс:* `{balance_text}`\n"
                    f"*🏆 Успешных сделок:* `{successful_deals}`\n\n"
                    f"🚀 *Осуществляйте новые сделки с Secure Deal — с нами вы можете быть уверены в надежности и честности каждой операции. ⚡️*"
                )
                
                # Редактируем текущее сообщение вместо удаления
                if not await edit_video_message(chat_id, message_id, video_path='assets/1.mp4', caption=text, reply_markup=get_profile_keyboard(get_user_language(chat_id)), parse_mode='Markdown'):
                    await send_video_without_sound(
                        chat_id, 
                        'assets/1.mp4', 
                        caption=text, 
                        reply_markup=get_profile_keyboard(get_user_language(chat_id)), 
                        parse_mode='Markdown'
                    )
            except Exception as e:
                logger.error(f"Error in my_profile for user {call.from_user.id}: {e}")
                await bot.send_message(chat_id, "⚠ Ошибка при отображении профиля. Обратитесь в поддержку @GiftGuarantHelp.")
        elif call.data == "change_language":
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
            if not await edit_video_message(chat_id, message_id, video_path='assets/1.mp4', caption=text, reply_markup=get_language_keyboard(get_user_language(chat_id))):
                await send_video_without_sound(chat_id, 'assets/1.mp4', caption=text, reply_markup=get_language_keyboard(get_user_language(chat_id)))
        elif call.data == "lang_ru":
            try:
                db.collection('user_profile').document(str(call.from_user.id)).update({'language': 'ru'})
            except Exception:
                pass
            # Инвалидируем кэш языка, чтобы не "откатывался"
            _LANG_CACHE['map'].pop(call.from_user.id, None)
            await bot.answer_callback_query(call.id, "Язык: Русский 🇷🇺", show_alert=False)
            # Мгновенно обновить текущее сообщение на главное меню с новым языком
            lang = 'ru'
            menu_text = t(lang, 'menu_title')
            if not await edit_video_message(chat_id, message_id, video_path='assets/1.mp4', caption=menu_text, reply_markup=get_main_menu_keyboard(lang), parse_mode='Markdown'):
                await show_main_menu(chat_id, call.from_user.first_name)
        elif call.data == "lang_en":
            try:
                db.collection('user_profile').document(str(call.from_user.id)).update({'language': 'en'})
            except Exception:
                pass
            _LANG_CACHE['map'].pop(call.from_user.id, None)
            await bot.answer_callback_query(call.id, "Language: English 🇬🇧", show_alert=False)
            lang = 'en'
            menu_text = t(lang, 'menu_title')
            if not await edit_video_message(chat_id, message_id, video_path='assets/1.mp4', caption=menu_text, reply_markup=get_main_menu_keyboard(lang), parse_mode='Markdown'):
                await show_main_menu(chat_id, call.from_user.first_name)
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
        await bot.send_message(chat_id, "⚠ Ошибка при обработке действия. Обратитесь в поддержку @GiftGuarantHelp.")

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
        
        # Убираем проверку статуса сделки, так как она может быть в статусе 'in_progress'
        if not deal:
            await send_video_without_sound(chat_id, 'assets/1.mp4', caption="😕 Сделка не найдена.", reply_markup=get_in_deal_keyboard(deal_id, 'in_progress'))
            return
        
        if deal['participant_id'] != user_id:
            await send_video_without_sound(chat_id, 'assets/1.mp4', caption="😕 Вы не являетесь участником этой сделки.", reply_markup=get_in_deal_keyboard(deal_id, 'in_progress'))
            return

        amount = deal['amount']
        currency = deal['currency']
        creator_id = deal['creator_id']
        creator_username = deal['creator_username']
        deal_type = deal['deal_type']
        
        # Функция экранирования для Markdown V2
        def escape_markdown_v2(text):
            if not text:
                return text
            
            # Сначала убираем уже существующие экранирования, чтобы избежать двойного экранирования
            text = text.replace('\\', '')
            
            escape_chars = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}']
            for char in escape_chars:
                text = text.replace(char, f'\\{char}')
            return text
        
        admin_ids = get_admin_ids()
        if user_id not in admin_ids:
            user_balance = get_user_balance(user_id)
            if user_balance < amount and currency not in ['Stars', 'TON']:
                await send_video_without_sound(chat_id, 'assets/1.mp4', caption="⚠ *У вас недостаточно средств на балансе*", parse_mode='Markdown', reply_markup=get_in_deal_keyboard(deal_id, 'in_progress'))
                return
            update_user_balance(user_id, -amount)

        deal_ref.update({'status': 'paid'})
        logger.info(f"Deal {deal_id} marked as paid")
        
        try:
            await bot.delete_message(chat_id, message_id)
        except:
            pass

        # Экранируем специальные символы
        escaped_deal_id = escape_markdown_v2(deal_id)
        escaped_amount = escape_markdown_v2(str(amount))
        escaped_currency = escape_markdown_v2(currency)
        
        await send_video_without_sound(
            chat_id, 
            'assets/1.mp4', 
            caption=f"✅ <b>Вы успешно оплатили сделку</b> <code>#{escaped_deal_id}</code>. <b>Ожидайте, пока продавец передаст товар на проверку @GiftGuarantHelp</b>", 
            reply_markup=get_paid_keyboard(deal_id), 
            parse_mode='HTML'
        )
        
        participant_username = get_username_by_id(user_id)
        participant_display_name = f"@{participant_username}" if participant_username else f"ID{user_id}"
        participant_link_html = f"<a href='tg://user?id={user_id}'>" + (participant_display_name) + "</a>"

        item_name = get_transfer_item_name(deal_type)

        seller_message_html = (
            f"💸 <b>Сделка оплачена!</b>\n\n"
            f"👤 <b>Покупатель</b>: {participant_link_html} <b>оплатил</b> <code>{escaped_amount} {escaped_currency}</code>\n\n"
            f"📦 <b>Пожалуйста, передайте {item_name} поддержке @GiftGuarantHelp для проверки.</b>\n"
            f"💰 <b>Средства в размере</b> <code>{escaped_amount} {escaped_currency}</code> <b>будут зачислены на ваш баланс сразу после подтверждения @GiftGuarantHelp.</b>\n"
        )

        keyboard = telebot.types.InlineKeyboardMarkup()
        transfer_btn = telebot.types.InlineKeyboardButton(f"✅ Я передал {item_name}", callback_data=f"complete_deal_{deal_id}")
        keyboard.add(transfer_btn)

        await send_video_without_sound(
            creator_id, 
            'assets/1.mp4', 
            caption=seller_message_html, 
            reply_markup=keyboard, 
            parse_mode='HTML'
        )
    except Exception as e:
        logger.error(f"Error in handle_pay_from_balance for deal {deal_id}: {e}")
        await bot.send_message(chat_id, "⚠ Ошибка при оплате сделки. Обратитесь в поддержку @GiftGuarantHelp.")

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
            await send_video_without_sound(chat_id, 'assets/1.mp4', caption="⚠ Эта сделка еще не была оплачена.", reply_markup=get_in_deal_keyboard(deal_id, status))
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
        
        await bot.send_message(creator_id, "*🎉 Сделка успешно завершена!*", parse_mode='Markdown')
        await bot.send_message(participant_id, "*🎉 Сделка успешно завершена!*", parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Error in handle_complete_deal for deal {deal_id}: {e}")
        await bot.send_message(chat_id, "⚠ Ошибка при завершении сделки. Обратитесь в поддержку @GiftGuarantHelp.")

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
        await send_video_without_sound(creator_id, 'assets/1.mp4', caption=message_text, parse_mode='HTML', reply_markup=get_main_menu_keyboard())

        if participant_id:
            await send_video_without_sound(participant_id, 'assets/1.mp4', caption=message_text, parse_mode='HTML', reply_markup=get_main_menu_keyboard())
        
        await send_video_without_sound(chat_id, 'assets/1.mp4', caption="✅ Вы успешно покинули сделку.", reply_markup=get_main_menu_keyboard())
    except Exception as e:
        logger.error(f"Error in handle_leave_deal for deal {deal_id}: {e}")
        await bot.send_message(chat_id, "⚠ Ошибка при выходе из сделки. Обратитесь в поддержку @GiftGuarantHelp.")

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
        await send_video_without_sound(chat_id, 'assets/4.mp4', caption=text, reply_markup=get_currency_keyboard(get_user_language(chat_id)), parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Error in handle_links for user {user_id}: {e}")
        await bot.send_message(chat_id, "⚠ Ошибка при обработке ссылок. Обратитесь в поддержку @GiftGuarantHelp.")

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
            # Не уходим в меню. Просто сформируем сделку и отправим результат отдельным сообщением
        try:
            amount = float(message.text)
            if amount <= 0:
                raise ValueError
        except ValueError:
            sent_msg = await bot.reply_to(message, "⚠ Неверный формат. Введите положительное число (напр. 1000).")
            async with bot.retrieve_data(user_id, chat_id) as data:
                data['prompt_message_id'] = sent_msg.message_id
            return
            
        # Удаляем сообщение пользователя с суммой
        try:
            await bot.delete_message(chat_id, message.message_id)
        except Exception:
            pass
        # Пытаемся отредактировать промпт под сумму на итог сделки
        try:
            preview_text = (
                f"🎉 *Сделка создаётся...*\n\n"
                f"💰 *Сумма:* `{amount}`\n"
                f"⏳ *Момент...*"
            )
            if prompt_message_id:
                await edit_video_message(chat_id, prompt_message_id, video_path='assets/5.mp4', caption=preview_text, parse_mode='Markdown')
        except Exception as e:
            logger.error(f"Error editing prompt in handle_amount for user {user_id}: {e}")
            
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

        join_link = generate_join_link(deal_id)
        text = (
            f"🎉 *Сделка создана!*\n\n"
            f"🆔 *ID сделки:* `{deal_id}`\n"
            f"💰 *Сумма:* `{deal_data['amount']} {deal_data['currency']}`\n"
            f"🔗 *Ссылка для участника:*\n`{join_link}`\n\n"
            f"⏳ *Чтобы присоединиться к сделке, отправьте это сообщение покупателю!*"
        )
        # Пробуем заменить существующий промпт, иначе отправим новое
        edited = False
        try:
            if prompt_message_id:
                edited = await edit_video_message(chat_id, prompt_message_id, video_path='assets/5.mp4', caption=text, reply_markup=get_in_deal_keyboard(deal_id, 'waiting_for_participant'), parse_mode='Markdown')
        except Exception:
            edited = False
        if not edited:
            await send_video_without_sound(chat_id, 'assets/5.mp4', caption=text, reply_markup=get_in_deal_keyboard(deal_id, 'waiting_for_participant'), parse_mode='Markdown')
        await bot.delete_state(user_id, chat_id)
    except Exception as e:
        logger.error(f"Error in handle_amount for user {user_id}: {e}")
        await bot.send_message(chat_id, "⚠ Ошибка при создании сделки. Обратитесь в поддержку @GiftGuarantHelp.")

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

    # Удаляем сообщения пользователя и предыдущее сообщение бота
    try:
        # Удаляем сообщение пользователя
        await bot.delete_message(chat_id, message.message_id)
        logger.info(f"Deleted input message {message.message_id} for user {user_id}")
    except Exception as e:
        logger.error(f"Error deleting user message for user {user_id}: {e}")

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
        await bot.send_message(chat_id, "⚠ Ошибка при сохранении реквизитов. Пожалуйста, попробуйте снова или обратитесь в поддержку @GiftGuarantHelp.")
        return

    # Обработка состояния
    try:
        # Отправляем видео 8.mp4 с подтверждением сохранения реквизитов
        # Удаляем сообщение-промпт с инструкцией, если оно было
        if prompt_message_id:
            try:
                await bot.delete_message(chat_id, prompt_message_id)
            except Exception:
                pass
        # Отправляем подтверждение отдельным сообщением
        await send_video_without_sound(
            chat_id, 
            'assets/8.mp4', 
            caption="✅ *Ваши реквизиты успешно сохранены!*", 
            parse_mode='Markdown'
        )
        
        if pending_deal_id:
            # Для сделки переходим к сделке после показа подтверждения
            logger.info(f"Continuing deal {pending_deal_id} for user {user_id}")
            await bot.delete_state(user_id, chat_id)
            
            # Небольшая задержка для показа подтверждения
            await asyncio.sleep(2)
            
            # Переходим к сделке
            await complete_deal_join(chat_id, user_id, message.from_user.username, pending_deal_id)
        else:
            # Если это не сделка, просто удаляем состояние
            logger.info(f"Details saved for user {user_id}, no pending deal")
            await bot.delete_state(user_id, chat_id)
    except Exception as e:
        logger.error(f"Error processing state for user {user_id}: {e}")
        await bot.send_message(chat_id, "⚠ Ошибка при обработке состояния. Обратитесь в поддержку @GiftGuarantHelp.")

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

def escape_telegram_markdown(text):
    """
    Экранирует специальные символы для Markdown в Telegram
    """
    escape_chars = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}']
    for char in escape_chars:
        text = text.replace(char, f'\\{char}')
    return text

def get_escaped_bot_username():
    """
    Возвращает экранированное имя бота для использования в ссылках
    """
    return escape_telegram_markdown(BOT_USERNAME)

# Обновляем создание ссылки для присоединения к сделке
def generate_join_link(deal_id):
    """
    Генерирует ссылку для присоединения к сделке
    """
    return f"https://t.me/{BOT_USERNAME}?start=deal_{deal_id}"

def escape_markdown_v2(text):
    """
    Экранирует специальные символы для Markdown V2 в Telegram
    """
    if not text:
        return text
    
    # Сначала убираем уже существующие экранирования, чтобы избежать двойного экранирования
    text = text.replace('\\', '')
    
    escape_chars = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}']
    for char in escape_chars:
        text = text.replace(char, f'\\{char}')
    return text

@bot.callback_query_handler(func=lambda call: call.data == "my_profile")
async def my_profile_handler(call):
    chat_id = call.message.chat.id
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
        username = profile.get('username', 'Без имени')
        balance = profile.get('balance', 0.0)
        successful_deals = profile.get('successful_deals', 0)
        
        admin_ids = get_admin_ids()
        balance_text = "∞" if call.from_user.id in admin_ids else f"{balance:.2f}"
        
        # Экранируем; добавляем @ к username
        display_username = f"@{username}" if username else 'Без имени'
        escaped_username = escape_markdown_v2(display_username)
        
        text = (
            "*👤 Ваш профиль*\n\n"
            f"*👋 Пользователь:* {escaped_username}\n"
            f"*💰 Баланс:* `{balance_text}`\n"
            f"*🏆 Успешных сделок:* `{successful_deals}`\n\n"
            f"🚀 *Осуществляйте новые сделки с Secure Deal — с нами вы можете быть уверены в надежности и честности каждой операции. ⚡️*"
        )
        
        # Редактируем текущее сообщение вместо удаления
        if not await edit_video_message(chat_id, call.message.message_id, video_path='assets/1.mp4', caption=text, reply_markup=get_profile_keyboard(get_user_language(chat_id)), parse_mode='Markdown'):
            await send_video_without_sound(
                chat_id, 
                'assets/1.mp4', 
                caption=text, 
                reply_markup=get_profile_keyboard(get_user_language(chat_id)), 
                parse_mode='Markdown'
            )
    except Exception as e:
        logger.error(f"Error in my_profile for user {call.from_user.id}: {e}")
        await bot.send_message(chat_id, "⚠ Ошибка при отображении профиля. Обратитесь в поддержку @GiftGuarantHelp.")

if __name__ == '__main__':
    try:
        web.run_app(app, host=WEBHOOK_HOST, port=WEBHOOK_PORT, handle_signals=True, loop=asyncio.get_event_loop())
    except Exception as e:
        logger.error(f"Error starting web app: {e}")