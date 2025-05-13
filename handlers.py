import re
import uuid
from datetime import datetime, timedelta
import asyncio
import time
import random
import traceback # For logging detailed errors
import html # For escaping HTML in messages
import math # For pagination calculations

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup, ParseMode, User, CallbackQuery, Message
)
from telegram.ext import (
    CommandHandler, MessageHandler, CallbackQueryHandler, ConversationHandler,
    Filters, CallbackContext, Dispatcher # Import Dispatcher for run_async type hint
)
from telegram.error import BadRequest, TelegramError, RetryAfter # Import specific errors

import database as db
import telethon_utils as telethon_api

# Import constants, keys, states, and helpers from central locations
from config import (
    log, ADMIN_IDS, is_admin, LITHUANIA_TZ, UTC_TZ, SESSION_DIR, ITEMS_PER_PAGE,
    # States
    STATE_WAITING_FOR_CODE, STATE_WAITING_FOR_PHONE, STATE_WAITING_FOR_API_ID,
    STATE_WAITING_FOR_API_HASH, STATE_WAITING_FOR_CODE_USERBOT,
    STATE_WAITING_FOR_PASSWORD, STATE_WAITING_FOR_SUB_DETAILS,
    STATE_WAITING_FOR_FOLDER_CHOICE, # Deprecated
    STATE_WAITING_FOR_FOLDER_NAME,
    STATE_WAITING_FOR_FOLDER_SELECTION, STATE_TASK_SETUP,
    STATE_WAITING_FOR_LANGUAGE, STATE_WAITING_FOR_EXTEND_CODE,
    STATE_WAITING_FOR_EXTEND_DAYS, STATE_WAITING_FOR_ADD_USERBOTS_CODE,
    STATE_WAITING_FOR_ADD_USERBOTS_COUNT, STATE_SELECT_TARGET_GROUPS,
    STATE_WAITING_FOR_USERBOT_SELECTION, STATE_WAITING_FOR_GROUP_LINKS,
    STATE_WAITING_FOR_FOLDER_ACTION, STATE_WAITING_FOR_PRIMARY_MESSAGE_LINK,
    STATE_WAITING_FOR_FALLBACK_MESSAGE_LINK, STATE_FOLDER_EDIT_REMOVE_SELECT,
    STATE_FOLDER_RENAME_PROMPT, STATE_ADMIN_CONFIRM_USERBOT_RESET,
    STATE_WAITING_FOR_START_TIME, STATE_ADMIN_TASK_MESSAGE,
    STATE_ADMIN_TASK_SCHEDULE, STATE_ADMIN_TASK_TARGET,
    STATE_WAITING_FOR_TASK_BOT,
    STATE_ADMIN_TASK_CONFIRM,

    # Context Keys
    CTX_USER_ID, CTX_LANG, CTX_PHONE, CTX_API_ID, CTX_API_HASH, CTX_AUTH_DATA,
    CTX_INVITE_DETAILS, CTX_EXTEND_CODE, CTX_ADD_BOTS_CODE, CTX_FOLDER_ID,
    CTX_FOLDER_NAME, CTX_FOLDER_ACTION, CTX_SELECTED_BOTS,
    CTX_TARGET_GROUP_IDS_TO_REMOVE, CTX_TASK_PHONE, CTX_TASK_SETTINGS, CTX_PAGE,
    CTX_MESSAGE_ID, CTX_TASK_BOT, CTX_TASK_MESSAGE, CTX_TASK_SCHEDULE,
    CTX_TASK_TARGET, CTX_TASK_TARGET_TYPE, CTX_TASK_TARGET_FOLDER,

    # Callback Prefixes
    CALLBACK_ADMIN_PREFIX, CALLBACK_CLIENT_PREFIX, CALLBACK_TASK_PREFIX,
    CALLBACK_FOLDER_PREFIX, CALLBACK_JOIN_PREFIX, CALLBACK_LANG_PREFIX,
    CALLBACK_INTERVAL_PREFIX, CALLBACK_GENERIC_PREFIX
)
from translations import get_text, language_names, translations
from utils import get_user_id_and_lang, send_or_edit_message, clear_conversation_data

# Import admin handlers for use as state handlers (ensure they are async)
from admin_handlers import (
    admin_process_task_message,
    admin_process_task_schedule,
    admin_process_task_target
)


# --- Async Error Handler ---
async def async_error_handler(update: object, context: CallbackContext) -> None:
    log.error(msg="[async_error_handler] Exception while handling an update:", exc_info=context.error)
    user_id = None; chat_id = None
    try:
        if isinstance(update, Update):
            if update.effective_user: user_id = update.effective_user.id
            if update.effective_chat: chat_id = update.effective_chat.id
        
        if not chat_id and isinstance(update, Update) and update.callback_query:
            if update.callback_query.message and update.callback_query.message.chat:
                 chat_id = update.callback_query.message.chat.id
            if not user_id and update.callback_query.from_user:
                 user_id = update.callback_query.from_user.id

        if not chat_id and user_id: chat_id = user_id
        
        if chat_id:
            log.info(f"[async_error_handler] Attempting to send generic error to user {user_id}, chat {chat_id}")
            current_lang = 'en'
            if context and hasattr(context, 'user_data') and isinstance(context.user_data, dict):
                current_lang = context.user_data.get(CTX_LANG, 'en')
            elif user_id:
                try: current_lang = db.get_user_language(user_id) or 'en'
                except: pass

            error_message = get_text(user_id, 'error_generic', lang_override=current_lang)
            await context.bot.send_message(chat_id=chat_id, text=error_message, parse_mode=ParseMode.HTML)
    except Exception as e: log.error(f"[async_error_handler] Failed to send async error message: {e}", exc_info=True)

# --- Formatting and Menu Builders (Synchronous) ---
def format_dt(timestamp: int | None, tz=LITHUANIA_TZ, fmt='%Y-%m-%d %H:%M') -> str:
    if not timestamp: return get_text(0, 'task_value_not_set', lang_override='en')
    try: dt_utc = datetime.fromtimestamp(timestamp, UTC_TZ); dt_local = dt_utc.astimezone(tz); return dt_local.strftime(fmt)
    except (ValueError, TypeError, OSError) as e: log.warning(f"Could not format invalid timestamp: {timestamp}. Error: {e}"); return "Invalid Date"

def build_client_menu(user_id, context: CallbackContext):
    lang = 'en'
    if context and hasattr(context, 'user_data') and isinstance(context.user_data, dict):
        lang = context.user_data.get(CTX_LANG, 'en')
    elif user_id:
        try: lang = db.get_user_language(user_id) or 'en'
        except: pass

    client_info = db.find_client_by_user_id(user_id)
    if not client_info: return get_text(user_id, 'unknown_user', lang_override=lang), None, ParseMode.HTML
    code = client_info['invitation_code']; sub_end_ts = client_info['subscription_end']; now_ts = int(datetime.now(UTC_TZ).timestamp())
    is_expired = sub_end_ts < now_ts; end_date = format_dt(sub_end_ts, fmt='%Y-%m-%d') if sub_end_ts else 'N/A'; 
    expiry_warning = f" ⚠️ <b>{get_text(user_id, 'subscription_expired_short', lang_override=lang, default_text='Expired')}</b>" if is_expired else ""
    userbot_phones = db.get_client_bots(user_id); bot_count = len(userbot_phones); parse_mode = ParseMode.HTML
    menu_text = f"<b>{get_text(user_id, 'client_menu_title', lang_override=lang, code=html.escape(code))}</b>{expiry_warning}\n"
    menu_text += get_text(user_id, 'client_menu_sub_end', lang_override=lang, end_date=end_date) + "\n\n"; menu_text += f"<u>{get_text(user_id, 'client_menu_userbots_title', lang_override=lang, count=bot_count)}</u>\n"
    if userbot_phones:
        for i, phone in enumerate(userbot_phones, 1):
            bot_db_info = db.find_userbot(phone); username = bot_db_info['username'] if bot_db_info else None; 
            status_str = bot_db_info['status'].capitalize() if bot_db_info and bot_db_info['status'] else 'Unknown'
            last_error = bot_db_info['last_error'] if bot_db_info else None; display_name = html.escape(f"@{username}" if username else phone); status_icon = "⚪️"
            if bot_db_info:
                status = bot_db_info['status']
                if status == 'active': status_icon = "🟢"
                elif status == 'error': status_icon = "🔴"
                elif status in ['connecting', 'authenticating', 'initializing']: status_icon = "⏳"
                elif status in ['needs_code', 'needs_password']: status_icon = "⚠️"
            menu_text += get_text(user_id, 'client_menu_userbot_line', lang_override=lang, index=i, status_icon=status_icon, display_name=display_name, status=html.escape(status_str)) + "\n"
            if last_error: 
                escaped_error = html.escape(last_error); 
                error_line = get_text(user_id, 'client_menu_userbot_error', lang_override=lang, error=f"{escaped_error[:100]}{'...' if len(escaped_error)>100 else ''}"); 
                menu_text += f"  {error_line}\n"
    else: menu_text += get_text(user_id, 'client_menu_no_userbots', lang_override=lang) + "\n"
    keyboard = [
        [InlineKeyboardButton(get_text(user_id, 'client_menu_button_setup_tasks', lang_override=lang), callback_data=f"{CALLBACK_CLIENT_PREFIX}select_bot_task")], 
        [InlineKeyboardButton(get_text(user_id, 'client_menu_button_manage_folders', lang_override=lang), callback_data=f"{CALLBACK_CLIENT_PREFIX}manage_folders")], 
        [InlineKeyboardButton(get_text(user_id, 'client_menu_button_join_groups', lang_override=lang), callback_data=f"{CALLBACK_CLIENT_PREFIX}select_bot_join")], 
        [InlineKeyboardButton(get_text(user_id, 'client_menu_button_stats', lang_override=lang), callback_data=f"{CALLBACK_CLIENT_PREFIX}view_stats")], 
        [InlineKeyboardButton(get_text(user_id, 'client_menu_button_language', lang_override=lang), callback_data=f"{CALLBACK_CLIENT_PREFIX}language")],
    ]
    markup = InlineKeyboardMarkup(keyboard); return menu_text, markup, parse_mode

def build_admin_menu(user_id, context: CallbackContext):
    lang = 'en'
    if context and hasattr(context, 'user_data') and isinstance(context.user_data, dict):
        lang = context.user_data.get(CTX_LANG, 'en')
    elif user_id:
        try: lang = db.get_user_language(user_id) or 'en'
        except: pass
    title = f"<b>{get_text(user_id, 'admin_panel_title', lang_override=lang)}</b>"; parse_mode = ParseMode.HTML
    keyboard = [
        [InlineKeyboardButton(get_text(user_id, 'admin_button_add_userbot', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}add_bot_prompt"), 
         InlineKeyboardButton(get_text(user_id, 'admin_button_remove_userbot', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}remove_bot_select?page=0")], 
        [InlineKeyboardButton(get_text(user_id, 'admin_button_list_userbots', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}list_bots?page=0")], 
        [InlineKeyboardButton(get_text(user_id, 'admin_button_manage_tasks', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}manage_tasks"), 
         InlineKeyboardButton(get_text(user_id, 'admin_button_view_tasks', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}view_tasks?page=0")], 
        [InlineKeyboardButton(get_text(user_id, 'admin_button_gen_invite', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}gen_invite_prompt")], 
        [InlineKeyboardButton(get_text(user_id, 'admin_button_view_subs', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}view_subs?page=0")], 
        [InlineKeyboardButton(get_text(user_id, 'admin_button_extend_sub', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}extend_sub_prompt"), 
         InlineKeyboardButton(get_text(user_id, 'admin_button_assign_bots_client', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}assign_bots_prompt")], 
        [InlineKeyboardButton(get_text(user_id, 'admin_button_view_logs', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}view_logs?page=0")],
    ]
    markup = InlineKeyboardMarkup(keyboard); return title, markup, parse_mode

def build_pagination_buttons(base_callback_data: str, current_page: int, total_items: int, items_per_page: int, lang: str = 'en') -> list:
    buttons = [];
    if total_items <= items_per_page: return []
    total_pages = math.ceil(total_items / items_per_page); row = []
    if current_page > 0: prev_text = get_text(0, 'pagination_prev', lang_override=lang); row.append(InlineKeyboardButton(prev_text, callback_data=f"{base_callback_data}?page={current_page - 1}"))
    if total_pages > 1: page_text = get_text(0,'pagination_page',lang_override=lang).format(current=current_page + 1, total=total_pages); row.append(InlineKeyboardButton(page_text, callback_data=f"{CALLBACK_GENERIC_PREFIX}noop"))
    if current_page < total_pages - 1: next_text = get_text(0, 'pagination_next', lang_override=lang); row.append(InlineKeyboardButton(next_text, callback_data=f"{base_callback_data}?page={current_page + 1}"))
    if row: buttons.append(row)
    return buttons

# --- Internal Async Menu Helper ---
async def _show_menu_async(update: Update, context: CallbackContext, menu_builder_func):
    user_id, lang = get_user_id_and_lang(update, context)
    title, markup, parse_mode = menu_builder_func(user_id, context)
    await send_or_edit_message(update, context, title, reply_markup=markup, parse_mode=parse_mode)

# --- COMMAND HANDLERS (NOW ASYNC) ---
async def start_command(update: Update, context: CallbackContext) -> int | None:
    log.debug("Async start_command called.")
    user_id, lang = get_user_id_and_lang(update, context)
    
    if CTX_USER_ID not in context.user_data and user_id:
        context.user_data[CTX_USER_ID] = user_id
    if CTX_LANG not in context.user_data and lang:
        context.user_data[CTX_LANG] = lang
        
    clear_conversation_data(context)

    try:
        if is_admin(user_id):
            await _show_menu_async(update, context, build_admin_menu)
            log.debug("Async start_command (admin) showed admin menu, returning ConversationHandler.END.")
            return ConversationHandler.END
        else:
            client = db.find_client_by_user_id(user_id)
            if client:
                await _show_menu_async(update, context, build_client_menu)
                log.debug("Async start_command (client) showed client menu, returning ConversationHandler.END.")
                return ConversationHandler.END
            else:
                await send_or_edit_message(update, context, get_text(user_id, 'ask_invitation_code', lang_override=lang), parse_mode=ParseMode.HTML)
                log.debug("Async start_command (new user) asked for code, returning STATE_WAITING_FOR_CODE.")
                return STATE_WAITING_FOR_CODE
    except Exception as e:
        log.error(f"Error in async start_command for user {user_id}: {e}", exc_info=True)
        try:
            err_user_id, err_lang = get_user_id_and_lang(update, context)
            await send_or_edit_message(update, context, get_text(err_user_id, 'error_generic', lang_override=err_lang), parse_mode=ParseMode.HTML)
        except Exception as send_err:
            log.error(f"Failed to send error message in async_start_command handler: {send_err}")
        return ConversationHandler.END

async def admin_command(update: Update, context: CallbackContext) -> int | None:
    log.debug("Async admin_command called.")
    user_id, lang = get_user_id_and_lang(update, context)
    
    if CTX_USER_ID not in context.user_data and user_id:
        context.user_data[CTX_USER_ID] = user_id
    if CTX_LANG not in context.user_data and lang:
        context.user_data[CTX_LANG] = lang

    clear_conversation_data(context)

    try:
        if not is_admin(user_id):
            log.warning(f"Unauthorized admin access attempt in admin_command from user {user_id}")
            await send_or_edit_message(update, context, get_text(user_id, 'not_admin', lang_override=lang), parse_mode=ParseMode.HTML)
        else:
            await _show_menu_async(update, context, build_admin_menu)
        log.debug("Async admin_command finished, returning ConversationHandler.END.")
        return ConversationHandler.END
    except Exception as e:
        log.error(f"Error in async admin_command for user {user_id}: {e}", exc_info=True)
        try:
            err_user_id, err_lang = get_user_id_and_lang(update, context)
            await send_or_edit_message(update, context, get_text(err_user_id, 'error_generic', lang_override=err_lang), parse_mode=ParseMode.HTML)
        except Exception as send_err:
            log.error(f"Failed to send error message in async_admin_command handler: {send_err}")
        return ConversationHandler.END

async def cancel_command(update: Update, context: CallbackContext) -> int:
    try:
        user_id, lang = get_user_id_and_lang(update, context)
        await send_or_edit_message(update, context, get_text(user_id, 'cancelled', lang_override=lang), parse_mode=ParseMode.HTML, reply_markup=None)
        clear_conversation_data(context)
        log.debug("Cancel command processed, returning ConversationHandler.END")
        return ConversationHandler.END
    except Exception as e:
        log.error(f"Error in cancel_command: {e}", exc_info=True)
        if update and update.effective_chat:
            try: await context.bot.send_message(chat_id=update.effective_chat.id, text="An error occurred cancelling. State cleared.")
            except: pass
        clear_conversation_data(context)
        return ConversationHandler.END

# --- Client Menu (separate async function for direct call if needed) ---
async def client_menu(update: Update, context: CallbackContext):
    await _show_menu_async(update, context, build_client_menu)
    return ConversationHandler.END

# --- Language Selection ---
async def client_ask_select_language(update: Update, context: CallbackContext) -> str:
    query = update.callback_query
    if query: await query.answer()
    user_id, lang = get_user_id_and_lang(update, context)
    
    buttons = []
    for code, name in language_names.items():
        buttons.append([InlineKeyboardButton(name, callback_data=f"{CALLBACK_LANG_PREFIX}{code}")])
    buttons.append([InlineKeyboardButton(get_text(user_id, 'button_back', lang_override=lang), callback_data=f"{CALLBACK_CLIENT_PREFIX}back_to_menu")])
    markup = InlineKeyboardMarkup(buttons)
    
    await send_or_edit_message(update, context, get_text(user_id, 'select_language', lang_override=lang), reply_markup=markup)
    return STATE_WAITING_FOR_LANGUAGE

async def set_language_handler(update: Update, context: CallbackContext) -> int | None:
    query = update.callback_query
    user_id, _ = get_user_id_and_lang(update, context)
    
    selected_lang_code = query.data.split(CALLBACK_LANG_PREFIX)[1]
    
    if selected_lang_code in language_names:
        if db.set_user_language(user_id, selected_lang_code):
            context.user_data[CTX_LANG] = selected_lang_code
            await query.answer(get_text(user_id, 'language_set', lang_override=selected_lang_code, lang_name=language_names[selected_lang_code]), show_alert=True)
            log.info(f"User {user_id} changed language to {selected_lang_code}")
            await client_menu(update, context) 
            return ConversationHandler.END
        else:
            await query.answer(get_text(user_id, 'language_set_error', lang_override=context.user_data.get(CTX_LANG)), show_alert=True)
            log.error(f"Failed to set language to {selected_lang_code} for user {user_id} in DB.")
    else:
        await query.answer(get_text(user_id, 'error_invalid_action', lang_override=context.user_data.get(CTX_LANG)), show_alert=True)
        log.warning(f"User {user_id} selected invalid language code: {selected_lang_code}")

    await client_menu(update, context)
    return ConversationHandler.END

# --- Conversation State Handlers ---
async def process_invitation_code(update: Update, context: CallbackContext) -> int:
    user_id, lang = get_user_id_and_lang(update, context); code_input = update.message.text.strip(); log.info(f"Processing invitation code '{code_input}' for user {user_id}")
    client_info_db = db.find_client_by_user_id(user_id)
    if client_info_db:
        now_ts = int(datetime.now(UTC_TZ).timestamp())
        if client_info_db['subscription_end'] > now_ts: await send_or_edit_message(update, context, get_text(user_id, 'user_already_active', lang_override=lang)); await client_menu(update, context); return ConversationHandler.END
    success, reason_or_client_data = db.activate_client(code_input, user_id)
    if success:
        if reason_or_client_data == "activation_success":
            client_data = db.find_client_by_code(code_input)
            if client_data: 
                if client_data.get('language'):
                    context.user_data[CTX_LANG] = client_data['language']
                    lang = client_data['language']
                await send_or_edit_message(update, context, get_text(user_id, 'activation_success', lang_override=lang)); 
                await client_menu(update, context); return ConversationHandler.END
            else:
                 await send_or_edit_message(update, context, get_text(user_id, 'activation_error', lang_override=lang)); return STATE_WAITING_FOR_CODE
        elif reason_or_client_data == "already_active": await send_or_edit_message(update, context, get_text(user_id, 'already_active', lang_override=lang)); await client_menu(update, context); return ConversationHandler.END
        else: log.error(f"activate_client returned True but unexpected reason: {reason_or_client_data}"); await send_or_edit_message(update, context, get_text(user_id, 'activation_error', lang_override=lang)); return STATE_WAITING_FOR_CODE
    else:
        error_key = str(reason_or_client_data)
        translation_map = {
            "user_already_active": "user_already_active", "code_not_found": "code_not_found", 
            "code_already_used": "code_already_used", "subscription_expired": "subscription_expired", 
            "activation_error": "activation_error", "activation_db_error": "activation_db_error",
        }
        message_key = translation_map.get(error_key, 'activation_error')
        await send_or_edit_message(update, context, get_text(user_id, message_key, lang_override=lang)); return STATE_WAITING_FOR_CODE

async def process_admin_phone(update: Update, context: CallbackContext) -> str | int:
    user_id, lang = get_user_id_and_lang(update, context); phone = update.message.text.strip(); log.info(f"process_admin_phone: Processing phone {phone} for user {user_id}")
    if not re.match(r"^\+[1-9]\d{1,14}$", phone): await send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_invalid_phone', lang_override=lang)); return STATE_WAITING_FOR_PHONE
    context.user_data[CTX_PHONE] = phone; await send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_prompt_api_id', lang_override=lang)); return STATE_WAITING_FOR_API_ID

async def process_admin_api_id(update: Update, context: CallbackContext) -> str | int:
    user_id, lang = get_user_id_and_lang(update, context); api_id_str = update.message.text.strip(); api_id = None
    try:
        api_id = int(api_id_str)
        if api_id <= 0: log.warning(f"Admin {user_id} entered non-positive API ID: {api_id}"); await send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_invalid_api_id', lang_override=lang)); return STATE_WAITING_FOR_API_ID
        else: context.user_data[CTX_API_ID] = api_id; log.info(f"Admin {user_id} API ID OK for {context.user_data.get(CTX_PHONE)}"); await send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_prompt_api_hash', lang_override=lang)); return STATE_WAITING_FOR_API_HASH
    except (ValueError, TypeError): log.warning(f"Admin {user_id} entered invalid API ID format: {api_id_str}"); await send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_invalid_api_id', lang_override=lang)); return STATE_WAITING_FOR_API_ID

async def process_admin_api_hash(update: Update, context: CallbackContext) -> str | int:
    user_id, lang = get_user_id_and_lang(update, context); api_hash = update.message.text.strip()
    if not api_hash or len(api_hash) < 30 or not re.match('^[a-fA-F0-9]+$', api_hash): 
        await send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_invalid_api_hash', lang_override=lang)); return STATE_WAITING_FOR_API_HASH
    context.user_data[CTX_API_HASH] = api_hash; phone = context.user_data.get(CTX_PHONE); api_id = context.user_data.get(CTX_API_ID)
    if not phone or not api_id: await send_or_edit_message(update, context, get_text(user_id, 'session_expired', lang_override=lang)); clear_conversation_data(context); return ConversationHandler.END
    log.info(f"Admin {user_id} API Hash OK for {phone}. Starting authentication flow."); await send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_auth_connecting', lang_override=lang, phone=html.escape(phone)))
    try:
        current_api_id = context.user_data.get(CTX_API_ID)
        current_api_hash = context.user_data.get(CTX_API_HASH)
        auth_status, auth_data = await telethon_api.start_authentication_flow(phone, current_api_id, current_api_hash); 
        log.info(f"Authentication start result for {phone}: Status='{auth_status}'")
        if auth_status == 'code_needed': 
            context.user_data[CTX_AUTH_DATA] = auth_data;
            await send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_prompt_code', lang_override=lang, phone=html.escape(phone))); return STATE_WAITING_FOR_CODE_USERBOT
        elif auth_status == 'password_needed': 
            context.user_data[CTX_AUTH_DATA] = auth_data; 
            await send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_prompt_password', lang_override=lang, phone=html.escape(phone))); return STATE_WAITING_FOR_PASSWORD
        elif auth_status == 'already_authorized':
             log.warning(f"Userbot {phone} is already authorized (Telethon check) during start_authentication_flow.");
             if not db.find_userbot(phone): 
                safe_phone_part = re.sub(r'[^\d]', '', phone) or f'unknown_{random.randint(1000,9999)}'
                session_file_rel = f"{safe_phone_part}.session"
                db.add_userbot(phone, session_file_rel, current_api_id, current_api_hash, 'active')
             else: db.update_userbot_status(phone, 'active')
             telethon_api.get_userbot_runtime_info(phone)
             await send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_already_auth', lang_override=lang, display_name=html.escape(phone))); 
             clear_conversation_data(context); return ConversationHandler.END
        else:
            error_msg = auth_data.get('error_message', 'Unknown error during auth start'); log.error(f"Auth start error for {phone}: {error_msg}"); 
            locals_for_format = {'phone': html.escape(phone), 'error': html.escape(error_msg)}; 
            key = 'admin_userbot_auth_error_unknown'
            if "flood wait" in error_msg.lower(): 
                key = 'admin_userbot_auth_error_flood'; seconds_match = re.search(r'\d+', error_msg); 
                locals_for_format['seconds'] = seconds_match.group(0) if seconds_match else '?'
            elif "config" in error_msg.lower() or "invalid api" in error_msg.lower(): key = 'admin_userbot_auth_error_config'
            elif "invalid phone" in error_msg.lower() or "phone number invalid" in error_msg.lower() : key = 'admin_userbot_auth_error_phone_invalid'
            elif "connection" in error_msg.lower() or "timeout" in error_msg.lower(): key = 'admin_userbot_auth_error_connect'
            await send_or_edit_message(update, context, get_text(user_id, key, lang_override=lang, **locals_for_format)); 
            clear_conversation_data(context); return ConversationHandler.END
    except Exception as e: 
        log.error(f"Exception during start_authentication_flow call for {phone}: {e}", exc_info=True); 
        await send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_auth_error_unknown', lang_override=lang, phone=html.escape(phone), error=html.escape(str(e)))); 
        clear_conversation_data(context); return ConversationHandler.END

async def process_admin_userbot_code(update: Update, context: CallbackContext) -> str | int:
    user_id, lang = get_user_id_and_lang(update, context); auth_data = context.user_data.get(CTX_AUTH_DATA); 
    original_phone_input = context.user_data.get(CTX_PHONE)
    
    if not auth_data or not original_phone_input: 
        log.error(f"process_admin_userbot_code: Missing auth_data or original_phone_input for user {user_id}"); 
        await send_or_edit_message(update, context, get_text(user_id, 'session_expired', lang_override=lang)); 
        clear_conversation_data(context); return ConversationHandler.END
        
    code = update.message.text.strip(); 
    log.info(f"process_admin_userbot_code: Processing code for original phone input {original_phone_input}")
    try:
        status, result_data = await telethon_api.complete_authentication_flow(auth_data, code=code)
        
        if status == 'success':
            final_phone = result_data.get('phone', original_phone_input); username = result_data.get('username'); 
            display_name = f"@{username}" if username else final_phone
            log.info(f"Code accepted for {final_phone}. Authentication successful."); 
            telethon_api.get_userbot_runtime_info(final_phone)
            await send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_auth_success', lang_override=lang, display_name=html.escape(display_name)))
            clear_conversation_data(context); return ConversationHandler.END
        elif status == 'password_needed':
            log.warning(f"Password unexpectedly needed after code submission for {original_phone_input}."); 
            await send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_prompt_password', lang_override=lang, phone=html.escape(original_phone_input))); 
            return STATE_WAITING_FOR_PASSWORD
        else: 
            error_msg = result_data.get('error_message', "Unknown error during code submission."); 
            log.warning(f"Code submission failed for {original_phone_input}: {error_msg}"); 
            error_key = 'admin_userbot_auth_error_code_invalid' 
            seconds_val = 'N/A'
            if "flood wait" in error_msg.lower(): 
                error_key = 'admin_userbot_auth_error_flood'
                seconds_match = re.search(r'(\d+)', error_msg)
                if seconds_match: seconds_val = seconds_match.group(1)

            await send_or_edit_message(update, context, get_text(user_id, error_key, lang_override=lang, phone=html.escape(original_phone_input), error=html.escape(error_msg), seconds=seconds_val))
            if error_key != 'admin_userbot_auth_error_code_invalid':
                clear_conversation_data(context); return ConversationHandler.END
            return STATE_WAITING_FOR_CODE_USERBOT
    except Exception as e: 
        log.error(f"process_admin_userbot_code: Exception submitting code for {original_phone_input}: {e}", exc_info=True); 
        await send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_auth_error_unknown', lang_override=lang, phone=html.escape(original_phone_input), error=html.escape(str(e)))); 
        clear_conversation_data(context); return ConversationHandler.END

async def process_admin_userbot_password(update: Update, context: CallbackContext) -> str | int:
    user_id, lang = get_user_id_and_lang(update, context); auth_data = context.user_data.get(CTX_AUTH_DATA); 
    original_phone_input = context.user_data.get(CTX_PHONE)

    if not auth_data or not original_phone_input: 
        log.error(f"process_admin_userbot_password: Missing auth_data or original_phone_input for user {user_id}"); 
        await send_or_edit_message(update, context, get_text(user_id, 'session_expired', lang_override=lang)); 
        clear_conversation_data(context); return ConversationHandler.END
        
    password = update.message.text.strip(); 
    log.info(f"process_admin_userbot_password: Processing 2FA password for original phone input {original_phone_input}")
    try:
        status, result_data = await telethon_api.complete_authentication_flow(auth_data, password=password)
        
        if status == 'success':
            final_phone = result_data.get('phone', original_phone_input); username = result_data.get('username'); 
            display_name = f"@{username}" if username else final_phone
            log.info(f"Password accepted for {final_phone}. Authentication successful."); 
            telethon_api.get_userbot_runtime_info(final_phone)
            await send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_auth_success', lang_override=lang, display_name=html.escape(display_name)))
            clear_conversation_data(context); return ConversationHandler.END
        else: 
            error_msg = result_data.get('error_message', "Unknown error during password submission."); 
            log.warning(f"Password submission failed for {original_phone_input}: {error_msg}"); 
            error_key = 'admin_userbot_auth_error_password_invalid'
            seconds_val = 'N/A'
            if "flood wait" in error_msg.lower(): 
                error_key = 'admin_userbot_auth_error_flood'
                seconds_match = re.search(r'(\d+)', error_msg)
                if seconds_match: seconds_val = seconds_match.group(1)
                
            await send_or_edit_message(update, context, get_text(user_id, error_key, lang_override=lang, phone=html.escape(original_phone_input), error=html.escape(error_msg), seconds=seconds_val))
            if error_key != 'admin_userbot_auth_error_password_invalid':
                clear_conversation_data(context); return ConversationHandler.END
            return STATE_WAITING_FOR_PASSWORD
    except Exception as e: 
        log.error(f"process_admin_userbot_password: Exception submitting password for {original_phone_input}: {e}", exc_info=True); 
        await send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_auth_error_unknown', lang_override=lang, phone=html.escape(original_phone_input), error=html.escape(str(e)))); 
        clear_conversation_data(context); return ConversationHandler.END

async def process_admin_invite_details(update: Update, context: CallbackContext) -> int:
    user_id, lang = get_user_id_and_lang(update, context)
    try:
        days_str = update.message.text.strip(); days = int(days_str)
        if days <= 0: await send_or_edit_message(update, context, get_text(user_id, 'admin_invite_invalid_days', lang_override=lang)); return STATE_WAITING_FOR_SUB_DETAILS
        invite_code = db.generate_invite_code()
        if invite_code:
            end_datetime = datetime.now(UTC_TZ) + timedelta(days=days); sub_end_ts = int(end_datetime.timestamp())
            if db.create_invitation(invite_code, sub_end_ts): 
                db.log_event_db("Invite Code Generated", f"Code: {invite_code}, Days: {days}", user_id=user_id); 
                await send_or_edit_message(update, context, get_text(user_id, 'admin_invite_generated', lang_override=lang, code=invite_code, days=days))
            else: 
                db.log_event_db("Invite Code Store Failed", f"Code: {invite_code}, Days: {days}", user_id=user_id); 
                await send_or_edit_message(update, context, get_text(user_id, 'admin_invite_db_error', lang_override=lang))
        else: await send_or_edit_message(update, context, get_text(user_id, 'admin_invite_db_error', lang_override=lang))
    except ValueError: await send_or_edit_message(update, context, get_text(user_id, 'admin_invite_invalid_days', lang_override=lang)); return STATE_WAITING_FOR_SUB_DETAILS
    except Exception as e: log.error(f"Error processing admin invite details: {e}", exc_info=True); await send_or_edit_message(update, context, get_text(user_id, 'error_generic', lang_override=lang))
    clear_conversation_data(context); return ConversationHandler.END

async def process_admin_extend_code(update: Update, context: CallbackContext) -> int:
    user_id, lang = get_user_id_and_lang(update, context); code = update.message.text.strip()
    client = db.find_client_by_code(code)
    if not client: await send_or_edit_message(update, context, get_text(user_id, 'admin_extend_invalid_code', lang_override=lang)); return STATE_WAITING_FOR_EXTEND_CODE
    context.user_data[CTX_EXTEND_CODE] = code; end_date_str = format_dt(client['subscription_end'])
    await send_or_edit_message(update, context, get_text(user_id, 'admin_extend_prompt_days', lang_override=lang, code=html.escape(code), end_date=end_date_str)); return STATE_WAITING_FOR_EXTEND_DAYS

async def process_admin_extend_days(update: Update, context: CallbackContext) -> int:
    user_id, lang = get_user_id_and_lang(update, context); days_str = update.message.text.strip(); code = context.user_data.get(CTX_EXTEND_CODE)
    if not code: await send_or_edit_message(update, context, get_text(user_id, 'session_expired', lang_override=lang)); clear_conversation_data(context); return ConversationHandler.END
    try:
        days_to_add = int(days_str)
        if days_to_add <= 0: raise ValueError("Days must be positive")
    except (ValueError, AssertionError): await send_or_edit_message(update, context, get_text(user_id, 'admin_extend_invalid_days', lang_override=lang)); return STATE_WAITING_FOR_EXTEND_DAYS
    client = db.find_client_by_code(code)
    if not client: await send_or_edit_message(update, context, get_text(user_id, 'admin_extend_invalid_code', lang_override=lang)); clear_conversation_data(context); return ConversationHandler.END
    current_end_ts = client['subscription_end']; now_ts = int(datetime.now(UTC_TZ).timestamp())
    start_ts = max(now_ts, current_end_ts); start_dt = datetime.fromtimestamp(start_ts, UTC_TZ); new_end_dt = start_dt + timedelta(days=days_to_add); new_end_ts = int(new_end_dt.timestamp())
    if db.extend_subscription(code, new_end_ts):
        new_end_date_str = format_dt(new_end_ts); client_user_id_for_log = client.get('user_id')
        db.log_event_db("Subscription Extended", f"Code: {code}, Added: {days_to_add} days, New End: {new_end_date_str}", user_id=user_id, details=f"Client UserID: {client_user_id_for_log}")
        await send_or_edit_message(update, context, get_text(user_id, 'admin_extend_success', lang_override=lang, code=html.escape(code), days=days_to_add, new_end_date=new_end_date_str))
    else: db.log_event_db("Sub Extend Failed", f"Code: {code}", user_id=user_id); await send_or_edit_message(update, context, get_text(user_id, 'admin_extend_db_error', lang_override=lang))
    clear_conversation_data(context); return ConversationHandler.END

async def process_admin_add_bots_code(update: Update, context: CallbackContext) -> int:
    user_id, lang = get_user_id_and_lang(update, context); code = update.message.text.strip();
    client = db.find_client_by_code(code)
    if not client: await send_or_edit_message(update, context, get_text(user_id, 'admin_assignbots_invalid_code', lang_override=lang)); return STATE_WAITING_FOR_ADD_USERBOTS_CODE
    context.user_data[CTX_ADD_BOTS_CODE] = code; 
    # Fetch current bot count for this client
    current_bots_for_client = db.get_all_userbots(assigned_status=True) # Get all assigned bots
    current_count = sum(1 for b_row in current_bots_for_client if dict(b_row).get('assigned_client') == code)

    await send_or_edit_message(update, context, get_text(user_id, 'admin_assignbots_prompt_count', lang_override=lang, code=html.escape(code), current_count=current_count)); return STATE_WAITING_FOR_ADD_USERBOTS_COUNT

async def process_admin_add_bots_count(update: Update, context: CallbackContext) -> int:
    user_id, lang = get_user_id_and_lang(update, context); count_str = update.message.text.strip(); code = context.user_data.get(CTX_ADD_BOTS_CODE)
    if not code: await send_or_edit_message(update, context, get_text(user_id, 'session_expired', lang_override=lang)); clear_conversation_data(context); return ConversationHandler.END
    try:
        count_to_add = int(count_str)
        if count_to_add <= 0: raise ValueError("Count must be positive")
    except (ValueError, AssertionError): await send_or_edit_message(update, context, get_text(user_id, 'admin_assignbots_invalid_count', lang_override=lang)); return STATE_WAITING_FOR_ADD_USERBOTS_COUNT
    
    available_bots_phones = db.get_unassigned_userbots(limit=count_to_add) # Returns list of phone numbers
    if len(available_bots_phones) < count_to_add: 
        await send_or_edit_message(update, context, get_text(user_id, 'admin_assignbots_no_bots_available', lang_override=lang, needed=count_to_add, available=len(available_bots_phones))); 
        clear_conversation_data(context); return ConversationHandler.END
    
    bots_to_actually_assign = available_bots_phones[:count_to_add]; # Already limited by DB query if enough found
    success, message_from_db = db.assign_userbots_to_client(code, bots_to_actually_assign)
    client_db_data = db.find_client_by_code(code)
    client_user_id_for_log = client_db_data['user_id'] if client_db_data else None

    if success:
        assigned_count_match = re.search(r"Successfully assigned (\d+)", message_from_db); 
        actually_assigned_in_db = int(assigned_count_match.group(1)) if assigned_count_match else 0
        
        final_message_key = 'admin_assignbots_success'; 
        format_params = {'count': actually_assigned_in_db, 'code': html.escape(code)}
        
        if actually_assigned_in_db != len(bots_to_actually_assign) or "Failed:" in message_from_db : 
            final_message_key = 'admin_assignbots_partial_success'; 
            format_params = {'assigned_count': actually_assigned_in_db, 'requested_count': len(bots_to_actually_assign), 'code': html.escape(code)}
        
        response_text = get_text(user_id, final_message_key, lang_override=lang, **format_params)
        if "Failed:" in message_from_db: response_text += f"\nDetails: {html.escape(message_from_db)}"
        
        await send_or_edit_message(update, context, response_text)
        db.log_event_db("Userbots Assigned", f"Code: {code}, Req: {count_to_add}, Assigned: {bots_to_actually_assign}, DB_Msg: {message_from_db}", user_id=user_id, details=f"ClientUID: {client_user_id_for_log}")
        for phone in bots_to_actually_assign: 
            if phone in available_bots_phones: # ensure only actually assigned ones are started
                telethon_api.get_userbot_runtime_info(phone) # This initializes if not running
    else:
        db.log_event_db("Bot Assign Failed Overall", f"Code: {code}, Reason: {message_from_db}", user_id=user_id, details=f"ClientUID: {client_user_id_for_log}")
        fail_message = get_text(user_id, 'admin_assignbots_failed', lang_override=lang, code=html.escape(code)) + f"\nError: {html.escape(message_from_db)}"
        await send_or_edit_message(update, context, fail_message)
    clear_conversation_data(context); return ConversationHandler.END

# --- Folder Management Handlers ---
async def client_folder_menu(update: Update, context: CallbackContext) -> int:
    user_id, lang = get_user_id_and_lang(update, context); 
    log.info(f"client_folder_menu: UserID={user_id}, Lang={lang}")
    await _show_menu_async(update, context, build_folder_menu); # build_folder_menu needs to be defined
    return ConversationHandler.END # Menu display, actions are callbacks

def build_folder_menu(user_id, context: CallbackContext): # Synchronous part
    lang = context.user_data.get(CTX_LANG, 'en')
    title = get_text(user_id, 'folder_menu_title', lang_override=lang)
    keyboard = [
        [InlineKeyboardButton(get_text(user_id, 'folder_menu_create', lang_override=lang), callback_data=f"{CALLBACK_FOLDER_PREFIX}create_prompt")],
        [InlineKeyboardButton(get_text(user_id, 'folder_menu_edit', lang_override=lang), callback_data=f"{CALLBACK_FOLDER_PREFIX}select_edit?page=0")],
        [InlineKeyboardButton(get_text(user_id, 'folder_menu_delete', lang_override=lang), callback_data=f"{CALLBACK_FOLDER_PREFIX}select_delete?page=0")],
        [InlineKeyboardButton(get_text(user_id, 'button_back', lang_override=lang), callback_data=f"{CALLBACK_CLIENT_PREFIX}back_to_menu")]
    ]
    markup = InlineKeyboardMarkup(keyboard)
    return title, markup, ParseMode.HTML


async def process_folder_name(update: Update, context: CallbackContext) -> int:
    user_id, lang = get_user_id_and_lang(update, context); folder_name = update.message.text.strip()
    if not folder_name: await send_or_edit_message(update, context, get_text(user_id, 'error_invalid_input', lang_override=lang)); return STATE_WAITING_FOR_FOLDER_NAME
    log.info(f"User {user_id} attempting to create folder: {folder_name}"); 
    folder_id_or_status = db.add_folder(folder_name, user_id) # Returns ID, None (exists), or False (error)
    if isinstance(folder_id_or_status, int) and folder_id_or_status > 0:
        folder_id = folder_id_or_status; 
        db.log_event_db("Folder Created", f"Name: {folder_name}, ID: {folder_id}", user_id=user_id); 
        await send_or_edit_message(update, context, get_text(user_id, 'folder_create_success', lang_override=lang, name=html.escape(folder_name))); 
        return await client_folder_menu(update, context) # Show folder menu again
    elif folder_id_or_status is None: # Duplicate
        await send_or_edit_message(update, context, get_text(user_id, 'folder_create_error_exists', lang_override=lang, name=html.escape(folder_name))); 
        return STATE_WAITING_FOR_FOLDER_NAME # Re-ask for name
    else: # DB error (False)
        db.log_event_db("Folder Create Failed", f"Name: {folder_name}, Reason: DB Error", user_id=user_id); 
        await send_or_edit_message(update, context, get_text(user_id, 'folder_create_error_db', lang_override=lang)); 
        clear_conversation_data(context); return ConversationHandler.END

async def client_select_folder_to_edit_or_delete(update: Update, context: CallbackContext, action: str) -> int:
    query = update.callback_query; await query.answer()
    user_id, lang = get_user_id_and_lang(update, context); current_page = 0
    try:
        if query and query.data and '?page=' in query.data: current_page = int(query.data.split('?page=')[1])
    except (ValueError, IndexError, AttributeError): current_page = 0
    
    folders_rows = db.get_folders_by_user(user_id) # List of sqlite3.Row
    folders = [dict(f_row) for f_row in folders_rows] # Convert to list of dicts

    if not folders: 
        await send_or_edit_message(update, context, get_text(user_id, 'folder_no_folders', lang_override=lang), 
                                   reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(get_text(user_id,'button_back',lang_override=lang), callback_data=f"{CALLBACK_FOLDER_PREFIX}back_to_manage")]]))
        return ConversationHandler.END # Or return to folder menu state
        
    total_items = len(folders); start_index = current_page * ITEMS_PER_PAGE; end_index = start_index + ITEMS_PER_PAGE; folders_page = folders[start_index:end_index]
    text_key = 'folder_select_edit' if action == 'edit' else 'folder_select_delete'; 
    text = get_text(user_id, text_key, lang_override=lang); keyboard = []
    for folder in folders_page:
        button_text = html.escape(folder['name']); 
        callback_action_prefix = "edit_selected" if action == 'edit' else "delete_selected_prompt"
        callback_data = f"{CALLBACK_FOLDER_PREFIX}{callback_action_prefix}?id={folder['id']}"; 
        keyboard.append([InlineKeyboardButton(button_text, callback_data=callback_data)])
        
    base_callback = f"{CALLBACK_FOLDER_PREFIX}select_{action}"; 
    pagination_buttons = build_pagination_buttons(base_callback, current_page, total_items, ITEMS_PER_PAGE, lang=lang); keyboard.extend(pagination_buttons)
    keyboard.append([InlineKeyboardButton(get_text(user_id, 'button_back', lang_override=lang), callback_data=f"{CALLBACK_FOLDER_PREFIX}back_to_manage")]); 
    markup = InlineKeyboardMarkup(keyboard)
    await send_or_edit_message(update, context, text, reply_markup=markup)
    return STATE_WAITING_FOR_FOLDER_SELECTION # A state handled by main_callback_handler

async def client_show_folder_edit_options(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    if query: await query.answer()
    
    user_id, lang = get_user_id_and_lang(update, context); folder_id = context.user_data.get(CTX_FOLDER_ID)
    if not folder_id and query and query.data and '?id=' in query.data:
         try: folder_id = int(query.data.split('?id=')[1]); context.user_data[CTX_FOLDER_ID] = folder_id
         except (ValueError, IndexError): folder_id = None
            
    if not folder_id: 
        log.error(f"Could not determine folder ID for edit options. User: {user_id}, UpdateType: {type(update)}"); 
        return await client_folder_menu(update, context)
        
    folder_name = db.get_folder_name(folder_id)
    if not folder_name: 
        await send_or_edit_message(update, context, get_text(user_id, 'folder_not_found_error', lang_override=lang)); 
        clear_conversation_data(context); return await client_folder_menu(update, context)
        
    context.user_data[CTX_FOLDER_NAME] = folder_name; 
    groups_in_folder_rows = db.get_target_groups_details_by_folder(folder_id)
    groups_in_folder = [dict(g_row) for g_row in groups_in_folder_rows]

    text = get_text(user_id, 'folder_edit_title', lang_override=lang, name=html.escape(folder_name)) + "\n" + get_text(user_id, 'folder_edit_groups_intro', lang_override=lang)
    if groups_in_folder:
        display_limit = 10
        for i, group in enumerate(groups_in_folder):
            if i >= display_limit: text += f"\n... and {len(groups_in_folder) - display_limit} more."; break
            link = group.get('group_link'); name = group.get('group_name') or f"ID: {group.get('group_id')}"; 
            escaped_name = html.escape(name)
            if link: escaped_link = html.escape(link); text += f"\n- <a href='{escaped_link}'>{escaped_name}</a>"
            else: text += f"\n- {escaped_name}"
    else: text += "\n" + get_text(user_id, 'folder_edit_no_groups', lang_override=lang)
    
    keyboard = [
        [InlineKeyboardButton(get_text(user_id, 'folder_edit_action_add', lang_override=lang), callback_data=f"{CALLBACK_FOLDER_PREFIX}edit_add_prompt")], 
        [InlineKeyboardButton(get_text(user_id, 'folder_edit_action_remove', lang_override=lang), callback_data=f"{CALLBACK_FOLDER_PREFIX}edit_remove_select?page=0")], 
        [InlineKeyboardButton(get_text(user_id, 'folder_edit_action_rename', lang_override=lang), callback_data=f"{CALLBACK_FOLDER_PREFIX}edit_rename_prompt")], 
        [InlineKeyboardButton(get_text(user_id, 'button_back', lang_override=lang), callback_data=f"{CALLBACK_FOLDER_PREFIX}back_to_manage")]
    ]; 
    markup = InlineKeyboardMarkup(keyboard)
    await send_or_edit_message(update, context, text, reply_markup=markup, disable_web_page_preview=True)
    return STATE_WAITING_FOR_FOLDER_ACTION # State handled by main_callback_handler

async def process_folder_links(update: Update, context: CallbackContext) -> int:
    user_id, lang = get_user_id_and_lang(update, context); 
    folder_id = context.user_data.get(CTX_FOLDER_ID); 
    folder_name = context.user_data.get(CTX_FOLDER_NAME)
    
    if not folder_id or not folder_name: 
        await send_or_edit_message(update, context, get_text(user_id, 'session_expired', lang_override=lang)); 
        clear_conversation_data(context); return ConversationHandler.END
        
    links_text = update.message.text; raw_links = [link.strip() for link in links_text.splitlines() if link.strip()]
    if not raw_links: 
        await send_or_edit_message(update, context, get_text(user_id, 'join_no_links', lang_override=lang)); # Using join_no_links, ensure it's generic enough
        return STATE_WAITING_FOR_GROUP_LINKS # Re-ask for links in folder context
        
    await send_or_edit_message(update, context, get_text(user_id, 'folder_processing_links', lang_override=lang))
    
    results = {}; added_count = 0; failed_count = 0; ignored_count = 0
    client_bots_phones = db.get_client_bots(user_id); resolver_bot_phone = None
    if client_bots_phones:
        active_client_bots_info = [b_phone for b_phone in client_bots_phones if (bot_info := db.find_userbot(b_phone)) and dict(bot_info).get('status') == 'active']
        if active_client_bots_info: resolver_bot_phone = random.choice(active_client_bots_info)
            
    log.info(f"User {user_id} adding links to folder '{folder_name}'. Using bot {resolver_bot_phone or 'None'} for resolution.")
    link_details = {}; # Stores {link_url: {id: 123, name: "Test", error: None}}
    
    if resolver_bot_phone:
        try: 
            resolved_data = await telethon_api.resolve_links_info(resolver_bot_phone, raw_links); 
            if resolved_data: link_details.update(resolved_data); 
            log.debug(f"Resolved {len(link_details)}/{len(raw_links)} links via bot {resolver_bot_phone}.")
        except Exception as resolve_e: 
            log.error(f"Error resolving folder links via bot {resolver_bot_phone}: {resolve_e}")
            # Continue without resolved info, will use links directly or fail more often
            
    for link in raw_links:
        group_id_resolved = None; group_name_resolved = None; 
        reason = None; status_code = 'failed'; # Default to failed
        
        resolved = link_details.get(link) # Get pre-resolved info if available
        
        if resolved and not resolved.get('error'):
            group_id_resolved = resolved.get('id'); 
            group_name_resolved = resolved.get('name')
            
            if group_id_resolved:
                 # Try to add to DB
                 added_status = db.add_target_group(group_id_resolved, group_name_resolved, link, user_id, folder_id)
                 if added_status is True: status_code = 'added'; added_count += 1
                 elif added_status is None: status_code = 'ignored'; ignored_count += 1; reason = 'Duplicate in folder' # Or unresolvable if ID was missing
                 else: status_code = 'failed'; reason = get_text(user_id, 'folder_add_db_error', lang_override=lang); failed_count += 1
            else: # Resolved but no ID (e.g. only name from invite)
                 status_code = 'failed'; reason = get_text(user_id, 'folder_resolve_error', lang_override=lang) + " (No ID from resolver)"; failed_count += 1
        elif resolved and resolved.get('error'): # Resolution attempt failed
            status_code = 'failed'; reason = resolved.get('error'); failed_count += 1
        else: # Not resolved or resolver not used
            # Here, we might attempt to parse the link for a numeric ID if it's a tg://join?id= type or c/ type
            # For simplicity, if resolver fails or isn't used, mark as failed if no obvious ID in link itself.
            # This part could be enhanced to try direct parsing of IDs from certain link types.
            status_code = 'failed'; reason = get_text(user_id, 'folder_resolve_error', lang_override=lang) + " (Not resolved)"; failed_count += 1
            
        results[link] = {'status': status_code, 'reason': reason}

    result_text = get_text(user_id, 'folder_results_title', lang_override=lang, name=html.escape(folder_name)) + f"\n(Added: {added_count}, Ignored: {ignored_count}, Failed: {failed_count})\n"
    display_limit = 20; displayed_count = 0
    for link, res in results.items():
        if displayed_count >= display_limit: result_text += f"\n...and {len(results) - displayed_count} more."; break
        
        status_key = f"folder_results_{res['status']}"; 
        status_text_template = get_text(user_id, status_key, lang_override=lang) # e.g., "❌ Failed ({reason})"
        
        # Format the reason into the status text if the template supports it
        if res['status'] != 'added' and res.get('reason'):
            current_reason_escaped = html.escape(str(res['reason']))
            if "{reason}" in status_text_template: 
                status_text = status_text_template.format(reason=current_reason_escaped)
            else: # Append reason if template doesn't have placeholder
                status_text = status_text_template + f" ({current_reason_escaped})"
        else: # 'added' or no reason
            status_text = status_text_template 
            
        result_text += "\n" + get_text(user_id, 'folder_results_line', lang_override=lang, link=html.escape(link), status=status_text); 
        displayed_count += 1
        
    await send_or_edit_message(update, context, result_text, disable_web_page_preview=True)
    context.user_data.pop(CTX_TARGET_GROUP_IDS_TO_REMOVE, None); # Clear any leftover selection from remove flow
    return await client_show_folder_edit_options(update, context) # Back to edit options for the folder


# ... (rest of your handlers, ensuring they are correctly defined and placed)

async def client_select_groups_to_remove(update: Update, context: CallbackContext) -> int:
    query = update.callback_query; await query.answer()
    user_id, lang = get_user_id_and_lang(update, context); folder_id = context.user_data.get(CTX_FOLDER_ID); folder_name = context.user_data.get(CTX_FOLDER_NAME)
    if not folder_id or not folder_name: await send_or_edit_message(update, context, get_text(user_id, 'session_expired', lang_override=lang)); return await client_folder_menu(update, context)
    try:
        current_page = 0
        if query and query.data and '?page=' in query.data: current_page = int(query.data.split('?page=')[1])
    except (ValueError, IndexError, AttributeError): current_page = 0
    
    groups_rows = db.get_target_groups_details_by_folder(folder_id)
    groups = [dict(g_row) for g_row in groups_rows]

    if not groups: await send_or_edit_message(update, context, get_text(user_id, 'folder_edit_no_groups', lang_override=lang)); return await client_show_folder_edit_options(update, context)
    
    selected_ids = set(context.user_data.get(CTX_TARGET_GROUP_IDS_TO_REMOVE, [])); 
    total_items = len(groups); start_index = current_page * ITEMS_PER_PAGE
    end_index = start_index + ITEMS_PER_PAGE; groups_page = groups[start_index:end_index]
    text = get_text(user_id, 'folder_edit_remove_select', lang_override=lang, name=html.escape(folder_name)); keyboard = []
    
    for group in groups_page:
        db_id = group['id']; is_selected = db_id in selected_ids; prefix = "✅ " if is_selected else "➖ "
        link_text = group.get('group_link') or f"ID: {group.get('group_id')}"; 
        display_text = group.get('group_name') or link_text; max_len = 40
        truncated_text = display_text[:max_len] + ("..." if len(display_text) > max_len else ""); 
        button_text = prefix + html.escape(truncated_text)
        callback_data = f"{CALLBACK_FOLDER_PREFIX}edit_toggle_remove?id={db_id}&page={current_page}"; 
        keyboard.append([InlineKeyboardButton(button_text, callback_data=callback_data)])
        
    base_callback = f"{CALLBACK_FOLDER_PREFIX}edit_remove_select"; 
    pagination_buttons = build_pagination_buttons(base_callback, current_page, total_items, ITEMS_PER_PAGE, lang=lang); keyboard.extend(pagination_buttons)
    action_row = [];
    if selected_ids: 
        confirm_text = get_text(user_id, 'folder_edit_remove_confirm_title', lang_override=lang) + f" ({len(selected_ids)})"; 
        action_row.append(InlineKeyboardButton(confirm_text, callback_data=f"{CALLBACK_FOLDER_PREFIX}edit_remove_confirm"))
    action_row.append(InlineKeyboardButton(get_text(user_id, 'button_back', lang_override=lang), callback_data=f"{CALLBACK_FOLDER_PREFIX}back_to_edit_options"))
    keyboard.append(action_row); markup = InlineKeyboardMarkup(keyboard)
    await send_or_edit_message(update, context, text, reply_markup=markup)
    return STATE_FOLDER_EDIT_REMOVE_SELECT

async def client_toggle_group_for_removal(update: Update, context: CallbackContext) -> int:
    query = update.callback_query; await query.answer()
    user_id, lang = get_user_id_and_lang(update, context)
    try:
        params_str = query.data.split('?', 1)[1]; params = dict(qc.split('=') for qc in params_str.split('&'))
        group_db_id = int(params['id']); current_page = int(params['page']) # page is for re-rendering
    except (ValueError, IndexError, KeyError): 
        log.error(f"Could not parse group ID/page from callback: {query.data}"); 
        await send_or_edit_message(update,context, get_text(user_id, 'error_generic', lang_override=lang)); 
        return STATE_FOLDER_EDIT_REMOVE_SELECT # Or back to main folder edit options
        
    if CTX_TARGET_GROUP_IDS_TO_REMOVE not in context.user_data: 
        context.user_data[CTX_TARGET_GROUP_IDS_TO_REMOVE] = set()
        
    if group_db_id in context.user_data[CTX_TARGET_GROUP_IDS_TO_REMOVE]: 
        context.user_data[CTX_TARGET_GROUP_IDS_TO_REMOVE].remove(group_db_id)
    else: 
        context.user_data[CTX_TARGET_GROUP_IDS_TO_REMOVE].add(group_db_id)
        
    # Re-render the selection list
    return await client_select_groups_to_remove(update, context)

async def client_confirm_remove_selected_groups(update: Update, context: CallbackContext) -> int:
    query = update.callback_query; await query.answer()
    user_id, lang = get_user_id_and_lang(update, context); 
    folder_id = context.user_data.get(CTX_FOLDER_ID); 
    folder_name = context.user_data.get(CTX_FOLDER_NAME); 
    ids_to_remove = list(context.user_data.get(CTX_TARGET_GROUP_IDS_TO_REMOVE, []))
    
    if not folder_id or not folder_name: 
        await send_or_edit_message(update, context, get_text(user_id, 'session_expired', lang_override=lang)); 
        return await client_folder_menu(update, context)
        
    if not ids_to_remove: 
        await send_or_edit_message(update, context, get_text(user_id, 'folder_edit_remove_none_selected', lang_override=lang)); 
        return await client_show_folder_edit_options(update, context)
        
    removed_count = db.remove_target_groups_by_db_id(ids_to_remove, user_id)
    if removed_count >= 0: 
        db.log_event_db("Folder Groups Removed", f"Folder: {folder_name}({folder_id}), Count: {removed_count}, IDs: {ids_to_remove}", user_id=user_id); 
        await send_or_edit_message(update, context, get_text(user_id, 'folder_edit_remove_success', lang_override=lang, count=removed_count, name=html.escape(folder_name)))
    else: 
        db.log_event_db("Folder Group Remove Failed", f"Folder: {folder_name}({folder_id}), DB Error", user_id=user_id); 
        await send_or_edit_message(update, context, get_text(user_id, 'folder_edit_remove_error', lang_override=lang))
        
    context.user_data.pop(CTX_TARGET_GROUP_IDS_TO_REMOVE, None); 
    return await client_show_folder_edit_options(update, context)

async def process_folder_rename(update: Update, context: CallbackContext) -> int:
    user_id, lang = get_user_id_and_lang(update, context); new_name = update.message.text.strip(); 
    folder_id = context.user_data.get(CTX_FOLDER_ID); current_name = context.user_data.get(CTX_FOLDER_NAME)
    
    if not folder_id or not current_name: 
        await send_or_edit_message(update, context, get_text(user_id, 'session_expired', lang_override=lang)); 
        clear_conversation_data(context); return ConversationHandler.END
        
    if not new_name: 
        await send_or_edit_message(update, context, get_text(user_id, 'error_invalid_input', lang_override=lang)); 
        return STATE_FOLDER_RENAME_PROMPT
        
    if new_name == current_name: # No change
        return await client_show_folder_edit_options(update, context)
        
    success, reason = db.rename_folder(folder_id, user_id, new_name)
    if success: 
        db.log_event_db("Folder Renamed", f"ID: {folder_id}, From: {current_name}, To: {new_name}", user_id=user_id); 
        await send_or_edit_message(update, context, get_text(user_id, 'folder_edit_rename_success', lang_override=lang, new_name=html.escape(new_name))); 
        context.user_data[CTX_FOLDER_NAME] = new_name; # Update context
        return await client_show_folder_edit_options(update, context)
    else:
        if reason == "name_exists": 
            await send_or_edit_message(update, context, get_text(user_id, 'folder_edit_rename_error_exists', lang_override=lang, new_name=html.escape(new_name))); 
            return STATE_FOLDER_RENAME_PROMPT
        else: # db_error or other
            db.log_event_db("Folder Rename Failed", f"ID: {folder_id}, To: {new_name}, Reason: {reason}", user_id=user_id); 
            await send_or_edit_message(update, context, get_text(user_id, 'folder_edit_rename_error_db', lang_override=lang)); 
            return await client_show_folder_edit_options(update, context) # Back to options

async def client_confirm_folder_delete_prompt(update: Update, context: CallbackContext) -> int:
    query = update.callback_query; await query.answer()
    user_id, lang = get_user_id_and_lang(update, context)
    try: folder_id = int(query.data.split('?id=')[1])
    except (ValueError, IndexError, AttributeError): 
        log.error(f"Could not parse folder ID for delete confirm: {query.data}"); 
        await send_or_edit_message(update,context, get_text(user_id, 'error_generic', lang_override=lang)); 
        return await client_folder_menu(update, context)
        
    folder_name = db.get_folder_name(folder_id)
    if not folder_name: 
        await send_or_edit_message(update,context, get_text(user_id, 'folder_not_found_error', lang_override=lang)); 
        return await client_folder_menu(update, context)
        
    text = get_text(user_id, 'folder_delete_confirm', lang_override=lang, name=html.escape(folder_name))
    keyboard = [
        [InlineKeyboardButton(get_text(user_id, 'button_yes', lang_override=lang), callback_data=f"{CALLBACK_FOLDER_PREFIX}delete_confirmed_execute?id={folder_id}")], 
        [InlineKeyboardButton(get_text(user_id, 'button_no', lang_override=lang), callback_data=f"{CALLBACK_FOLDER_PREFIX}back_to_manage")]
    ]; 
    markup = InlineKeyboardMarkup(keyboard)
    await send_or_edit_message(update, context, text, reply_markup=markup)
    # The state STATE_WAITING_FOR_FOLDER_SELECTION is used for the list from which delete was chosen.
    # This confirmation step also expects a callback that will be routed by main_callback_handler.
    # So, returning the same state or ConversationHandler.END is fine if callbacks are handled globally.
    return STATE_WAITING_FOR_FOLDER_SELECTION 

async def client_delete_folder_confirmed_execute(update: Update, context: CallbackContext) -> int:
    query = update.callback_query; await query.answer()
    user_id, lang = get_user_id_and_lang(update, context)
    try: folder_id = int(query.data.split('?id=')[1])
    except (ValueError, IndexError, AttributeError): 
        log.error(f"Could not parse folder ID for delete confirmed execute: {query.data}"); 
        await send_or_edit_message(update,context, get_text(user_id, 'error_generic', lang_override=lang)); 
        return await client_folder_menu(update, context)
        
    folder_name_before_delete = db.get_folder_name(folder_id) # Get name for log/message
    if db.delete_folder(folder_id, user_id): 
        log.info(f"User {user_id} deleted folder ID {folder_id} (Name: {folder_name_before_delete or 'N/A'})"); 
        await send_or_edit_message(update, context, get_text(user_id, 'folder_delete_success', lang_override=lang, name=html.escape(folder_name_before_delete or 'Unknown')))
    else: 
        log.warning(f"Failed delete folder ID {folder_id} by user {user_id}"); 
        await send_or_edit_message(update, context, get_text(user_id, 'folder_delete_error', lang_override=lang))
    return await client_folder_menu(update, context) # Back to main folder menu

# --- Generic Bot Selection & Handling ---
async def client_select_bot_generic(update: Update, context: CallbackContext, action_prefix: str, next_state_on_message: str | None, title_key: str) -> int | None:
    query = update.callback_query
    if query: await query.answer()
    user_id, lang = get_user_id_and_lang(update, context); 
    log.info(f"client_select_bot_generic: User {user_id}, ActionPrefix {action_prefix}, NextState {next_state_on_message}, TitleKey {title_key}")
    
    available_bots_phones = db.get_client_bots(user_id); keyboard = []; active_bots_for_selection = []
    for phone in available_bots_phones:
        bot_info_row = db.find_userbot(phone)
        if not bot_info_row: continue
        bot_info = dict(bot_info_row)
        if bot_info.get('status') != 'active': continue
        
        active_bots_for_selection.append(bot_info); 
        username = bot_info.get('username', phone)
        display_name = f"@{username}" if username and username != phone else phone; # Show phone if username is same as phone or no username
        keyboard.append([InlineKeyboardButton(html.escape(display_name), callback_data=f"{action_prefix}select_{phone}")])
        
    if not active_bots_for_selection:
        no_active_bots_key = 'join_no_active_bots' if action_prefix == CALLBACK_JOIN_PREFIX else 'task_error_no_active_bots'
        # Check if task_error_no_active_bots key exists, otherwise fallback
        if 'task_error_no_active_bots' not in translations.get(lang, {}): 
            no_active_bots_key = 'join_no_active_bots' # Fallback to a general key
        await send_or_edit_message(update, context, get_text(user_id, no_active_bots_key, lang_override=lang)); 
        return ConversationHandler.END
        
    # Add "All Active" button only for Join flow and if multiple active bots
    if action_prefix == CALLBACK_JOIN_PREFIX and len(active_bots_for_selection) > 1 : 
        keyboard.insert(0, [InlineKeyboardButton(get_text(user_id, 'join_select_userbot_active', lang_override=lang, count=len(active_bots_for_selection)), callback_data=f"{action_prefix}select_active")])
        
    keyboard.append([InlineKeyboardButton(get_text(user_id, 'button_back', lang_override=lang), callback_data=f"{CALLBACK_CLIENT_PREFIX}back_to_menu")]); 
    markup = InlineKeyboardMarkup(keyboard)
    await send_or_edit_message(update, context, get_text(user_id, title_key, lang_override=lang), reply_markup=markup)
    
    # This function shows a menu. The next state is for handling the *callback* from this menu.
    return STATE_WAITING_FOR_USERBOT_SELECTION


async def handle_userbot_selection_callback(update: Update, context: CallbackContext, action_prefix: str) -> int | None:
    query = update.callback_query; await query.answer()
    user_id, lang = get_user_id_and_lang(update, context); data = query.data
    selected_option_part = data.split(f"{action_prefix}select_")[1]; selected_bots_phones = []
    
    if selected_option_part == 'active':
        all_client_bots_phones = db.get_client_bots(user_id); 
        selected_bots_phones = [p for p in all_client_bots_phones if (b_row := db.find_userbot(p)) and dict(b_row).get('status') == 'active']
        if not selected_bots_phones: 
            await send_or_edit_message(update, context, get_text(user_id, 'join_no_active_bots', lang_override=lang)); 
            # Re-show selection if no active bots somehow, or end.
            next_state_on_msg_for_reselect = STATE_WAITING_FOR_GROUP_LINKS if action_prefix == CALLBACK_JOIN_PREFIX else None # tasks don't have a direct text input after bot select
            title_key_for_reselect = 'join_select_userbot' if action_prefix == CALLBACK_JOIN_PREFIX else 'task_select_userbot'
            return await client_select_bot_generic(update, context, action_prefix, next_state_on_msg_for_reselect, title_key_for_reselect)
    else:
        phone = selected_option_part; 
        bot_info_row = db.find_userbot(phone); 
        client_owns_bot = phone in db.get_client_bots(user_id)
        
        if not bot_info_row or not client_owns_bot or dict(bot_info_row).get('status') != 'active': 
            log.warning(f"User {user_id} selected unauthorized/invalid/inactive bot: {phone} for prefix {action_prefix}"); 
            await send_or_edit_message(update, context, get_text(user_id, 'error_invalid_input', lang_override=lang)); 
            next_state_on_msg_for_reselect = STATE_WAITING_FOR_GROUP_LINKS if action_prefix == CALLBACK_JOIN_PREFIX else None
            title_key_for_reselect = 'join_select_userbot' if action_prefix == CALLBACK_JOIN_PREFIX else 'task_select_userbot'
            return await client_select_bot_generic(update, context, action_prefix, next_state_on_msg_for_reselect, title_key_for_reselect)
        selected_bots_phones = [phone]
        
    context.user_data[CTX_SELECTED_BOTS] = selected_bots_phones; 
    log.info(f"User {user_id} selected bot(s): {selected_bots_phones} for action prefix {action_prefix}")
    
    if action_prefix == CALLBACK_JOIN_PREFIX: 
        await send_or_edit_message(update, context, get_text(user_id, 'join_enter_group_links', lang_override=lang)); 
        return STATE_WAITING_FOR_GROUP_LINKS # This state uses process_join_group_links
    elif action_prefix == CALLBACK_TASK_PREFIX: 
        context.user_data[CTX_TASK_PHONE] = selected_bots_phones[0]; # Task setup is per-bot
        return await task_show_settings_menu(update, context) # Shows task settings, returns STATE_TASK_SETUP
    else: 
        log.error(f"Unhandled action prefix in handle_userbot_selection_callback: {action_prefix}"); 
        clear_conversation_data(context); return ConversationHandler.END

# --- Join Groups ---
async def process_join_group_links(update: Update, context: CallbackContext) -> int:
    # This handler is for the "Join Groups" flow specifically.
    # If adding to folders uses the same STATE_WAITING_FOR_GROUP_LINKS, it needs to be routed elsewhere
    # or this function needs to be more generic.
    # Based on current ConversationHandler, this is for the "Join" flow.
    user_id, lang = get_user_id_and_lang(update, context); 
    selected_bots = context.user_data.get(CTX_SELECTED_BOTS) # List of phone numbers
    
    if not selected_bots: 
        await send_or_edit_message(update, context, get_text(user_id, 'session_expired', lang_override=lang)); 
        clear_conversation_data(context); return ConversationHandler.END
        
    links_text = update.message.text; raw_links = [link.strip() for link in links_text.splitlines() if link.strip()]
    if not raw_links: 
        await send_or_edit_message(update, context, get_text(user_id, 'join_no_links', lang_override=lang)); 
        return STATE_WAITING_FOR_GROUP_LINKS # Re-ask
        
    await send_or_edit_message(update, context, get_text(user_id, 'join_processing', lang_override=lang))
    
    all_results_text = get_text(user_id, 'join_results_title', lang_override=lang); 
    join_tasks = [telethon_api.join_groups_batch(phone, raw_links) for phone in selected_bots]
    results_list_from_gather = await asyncio.gather(*join_tasks, return_exceptions=True)
    
    for i, result_item in enumerate(results_list_from_gather):
        phone = selected_bots[i]; 
        bot_db_info_row = db.find_userbot(phone); 
        bot_db_info = dict(bot_db_info_row) if bot_db_info_row else {}
        bot_display_name = html.escape(f"@{bot_db_info.get('username')}" if bot_db_info.get('username') else phone)
        
        all_results_text += "\n" + get_text(user_id, 'join_results_bot_header', lang_override=lang, display_name=bot_display_name)
        
        if isinstance(result_item, Exception): 
            log.error(f"Join batch task for {phone} raised exception: {result_item}", exc_info=True); 
            all_results_text += f"\n  -> {get_text(user_id, 'error_generic', lang_override=lang)} ({html.escape(str(result_item))})"; 
            continue
            
        # result_item is (error_info_dict, results_dict_for_bot)
        error_info, results_dict_for_bot = result_item 
        
        if error_info and error_info.get("error"): 
            error_message_detail = error_info['error']; 
            log.error(f"Join batch error for {phone}: {error_message_detail}"); 
            generic_error_text = get_text(user_id, 'error_generic', lang_override=lang); 
            all_results_text += f"\n  -> {generic_error_text} ({html.escape(error_message_detail)})"; 
            continue
            
        if not results_dict_for_bot: 
            all_results_text += f"\n  -> ({get_text(user_id, 'error_no_results', lang_override=lang)})"; 
            continue
            
        processed_links_count = 0
        for link, (status_code, detail_dict_or_str) in results_dict_for_bot.items():
             status_key_from_join = f"join_results_{status_code}"; 
             status_text = get_text(user_id, status_key_from_join, lang_override=lang) # e.g. "✅ Joined" or "❌ Failed ({reason})"
             
             if status_code not in ['success', 'already_member'] and isinstance(detail_dict_or_str, dict):
                  reason_code = detail_dict_or_str.get('reason'); 
                  error_detail = detail_dict_or_str.get('error'); 
                  seconds_detail = detail_dict_or_str.get('seconds'); 
                  reason_text_parts = []
                  
                  if reason_code:
                      reason_key_from_join = f"join_results_reason_{reason_code}"; 
                      reason_base_text = get_text(user_id, reason_key_from_join, lang_override=lang) # e.g. "invalid/expired invite link"
                      
                      if reason_base_text != reason_key_from_join: # Key was found
                          try: 
                              # Attempt to format with error/seconds if they exist and template expects them
                              formatted_reason = reason_base_text
                              if "{error}" in reason_base_text and error_detail:
                                  formatted_reason = formatted_reason.replace("{error}", html.escape(str(error_detail)))
                              if "{seconds}" in reason_base_text and seconds_detail:
                                  formatted_reason = formatted_reason.replace("{seconds}", str(seconds_detail))
                              reason_text_parts.append(formatted_reason.replace("{error}","").replace("{seconds}","").strip()) # Clean up unused placeholders
                          except KeyError: # Template might not have all placeholders
                              reason_text_parts.append(reason_base_text)
                      else: # Key not found, use raw reason_code
                          reason_text_parts.append(html.escape(str(reason_code)))
                          
                  # Append raw error_detail if no specific reason text or for generic errors
                  if error_detail and (not reason_code or reason_code in ['internal_error', 'batch_error']):
                      if not any(str(error_detail) in part for part in reason_text_parts): # Avoid duplicate
                          reason_text_parts.append(f"({html.escape(str(error_detail))})")
                          
                  if reason_text_parts:
                      full_reason_str = ", ".join(p for p in reason_text_parts if p)
                      if "{reason}" in status_text: # If status_text itself is a template like "Failed ({reason})"
                          status_text = status_text.format(reason=full_reason_str)
                      else: # Append if not a template
                          status_text += " (" + full_reason_str + ")"
                          
             elif status_code == 'flood_wait' and isinstance(detail_dict_or_str, dict) and detail_dict_or_str.get('seconds'): 
                 status_text = get_text(user_id, 'join_results_flood_wait', lang_override=lang, seconds=detail_dict_or_str.get('seconds'))
                 
             escaped_link = html.escape(link); 
             all_results_text += "\n" + get_text(user_id, 'join_results_line', lang_override=lang, url=escaped_link, status=status_text); 
             processed_links_count +=1
             if len(all_results_text) > 3800: 
                 all_results_text += f"\n\n... (message truncated, {len(raw_links) - processed_links_count} links remaining for this bot)"; 
                 break # Per bot truncation
                 
    keyboard = [[InlineKeyboardButton(get_text(user_id, 'button_main_menu', lang_override=lang), callback_data=f"{CALLBACK_CLIENT_PREFIX}back_to_menu")]]; 
    markup = InlineKeyboardMarkup(keyboard)
    
    if len(all_results_text) > 4096:
        log.warning(f"Join results message too long ({len(all_results_text)} chars). Splitting."); 
        parts = []; current_part = ""
        for line in all_results_text.splitlines(keepends=True):
            if len(current_part) + len(line) > 4000: parts.append(current_part); current_part = line
            else: current_part += line
        if current_part: parts.append(current_part)
        
        for i, part_text in enumerate(parts):
            part_markup = markup if i == len(parts) - 1 else None
            try: 
                await context.bot.send_message(user_id, part_text, parse_mode=ParseMode.HTML, reply_markup=part_markup, disable_web_page_preview=True); 
                if i < len(parts) - 1: await asyncio.sleep(0.5)
            except Exception as send_e: 
                log.error(f"Error sending split join results part {i+1}: {send_e}"); 
                await context.bot.send_message(user_id, get_text(user_id, 'error_generic', lang_override=lang)); break
    else: 
        await send_or_edit_message(update, context, all_results_text, reply_markup=markup, disable_web_page_preview=True)
        
    clear_conversation_data(context); return ConversationHandler.END

# --- Client Stats ---
async def client_show_stats(update: Update, context: CallbackContext) -> int:
    query = update.callback_query; await query.answer()
    user_id, lang = get_user_id_and_lang(update, context); 
    stats_row = db.get_client_stats(user_id) # Returns dict or None
    stats = dict(stats_row) if stats_row else {}

    if not stats: text = get_text(user_id, 'client_stats_no_data', lang_override=lang)
    else: 
        text = f"<b>{get_text(user_id, 'client_stats_title', lang_override=lang)}</b>\n\n"; 
        text += get_text(user_id, 'client_stats_messages', lang_override=lang, total_sent=stats.get('total_messages_sent', 0)) + "\n"; 
        text += get_text(user_id, 'client_stats_forwards', lang_override=lang, forwards_count=stats.get('forwards_count', 0)) + "\n"
        # groups_reached is deprecated, so not shown.
        
    keyboard = [[InlineKeyboardButton(get_text(user_id, 'button_back', lang_override=lang), callback_data=f"{CALLBACK_CLIENT_PREFIX}back_to_menu")]]; 
    markup = InlineKeyboardMarkup(keyboard)
    await send_or_edit_message(update, context, text, reply_markup=markup, parse_mode=ParseMode.HTML)
    return ConversationHandler.END

# --- Task Setup ---
async def task_show_settings_menu(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    if query: await query.answer()
    user_id, lang = get_user_id_and_lang(update, context); phone = context.user_data.get(CTX_TASK_PHONE)
    
    if not phone:
        if query and query.data and f"{CALLBACK_TASK_PREFIX}select_" in query.data:
             try: phone = query.data.split(f"{CALLBACK_TASK_PREFIX}select_")[1]; context.user_data[CTX_TASK_PHONE] = phone
             except IndexError: phone = None
        if not phone: 
            log.error(f"Task setup called without phone for user {user_id}. CB Data: {query.data if query else 'N/A'}"); 
            await send_or_edit_message(update, context, get_text(user_id, 'session_expired', lang_override=lang)); 
            return await client_menu(update, context) # Back to main client menu
            
    bot_db_info_row = db.find_userbot(phone); 
    bot_db_info = dict(bot_db_info_row) if bot_db_info_row else {}
    display_name = html.escape(f"@{bot_db_info.get('username')}" if bot_db_info.get('username') else phone)
    
    # Load or initialize task settings in context.user_data
    if CTX_TASK_SETTINGS not in context.user_data or context.user_data.get(CTX_TASK_SETTINGS, {}).get('_phone_marker_') != phone :
        task_settings_db_row = db.get_userbot_task_settings(user_id, phone)
        if task_settings_db_row: context.user_data[CTX_TASK_SETTINGS] = dict(task_settings_db_row)
        else: context.user_data[CTX_TASK_SETTINGS] = {} # Initialize empty if no DB entry
        context.user_data[CTX_TASK_SETTINGS]['_phone_marker_'] = phone # Mark which bot these settings are for
        
    current_settings = context.user_data.get(CTX_TASK_SETTINGS, {}); 
    status = current_settings.get('status', 'inactive')
    status_icon_key = f'task_status_icon_{status}'; 
    status_icon = get_text(user_id, status_icon_key, lang_override=lang) if status_icon_key in translations.get(lang,{}) else ("🟢" if status == 'active' else "⚪️"); 
    status_text = get_text(user_id, f'task_status_{status}', lang_override=lang)
    
    primary_link_raw = current_settings.get('message_link'); 
    primary_link = html.escape(primary_link_raw) if primary_link_raw else get_text(user_id, 'task_value_not_set', lang_override=lang)
    fallback_link_raw = current_settings.get('fallback_message_link'); 
    fallback_link = html.escape(fallback_link_raw) if fallback_link_raw else get_text(user_id, 'task_value_not_set', lang_override=lang)
    
    start_time_ts = current_settings.get('start_time'); 
    start_time_str = format_dt(start_time_ts, fmt='%H:%M') if start_time_ts else get_text(user_id, 'task_value_not_set', lang_override=lang)
    
    interval_min = current_settings.get('repetition_interval'); 
    interval_str = get_text(user_id, 'task_value_not_set', lang_override=lang)
    if interval_min:
         if interval_min < 60: interval_disp = f"{interval_min} min"
         elif interval_min % (60*24) == 0: interval_disp = f"{interval_min // (60*24)} d"
         elif interval_min % 60 == 0: interval_disp = f"{interval_min // 60} h"
         else: interval_disp = f"{interval_min // 60} h {interval_min % 60} min"
         interval_str = get_text(user_id, 'task_interval_button', lang_override=lang, value=interval_disp) # Uses 'Every {value}'
         
    target_str = get_text(user_id, 'task_value_not_set', lang_override=lang)
    if current_settings.get('send_to_all_groups'): 
        target_str = get_text(user_id, 'task_value_all_groups', lang_override=lang)
    elif current_settings.get('folder_id'):
        folder_id_val = current_settings['folder_id']; 
        folder_name = db.get_folder_name(folder_id_val)
        if folder_name: target_str = get_text(user_id, 'task_value_folder', lang_override=lang, name=html.escape(folder_name))
        else: target_str = get_text(user_id, 'task_value_folder', lang_override=lang, name=f"ID: {folder_id_val}") + " (Deleted?)"
        
    last_run_str = format_dt(current_settings.get('last_run')) if current_settings.get('last_run') else get_text(user_id, 'task_value_not_set', lang_override=lang, default_text="Never")
    last_error_raw = current_settings.get('last_error')
    last_error = html.escape(last_error_raw[:100]) + ('...' if last_error_raw and len(last_error_raw) > 100 else '') if last_error_raw else get_text(user_id, 'task_value_not_set', lang_override=lang)
    
    text = f"<b>{get_text(user_id, 'task_setup_title', lang_override=lang, display_name=display_name)}</b>\n\n"; 
    text += f"{get_text(user_id, 'task_setup_status_line', lang_override=lang, status_icon=status_icon, status_text=status_text)}\n"
    text += f"{get_text(user_id, 'task_setup_primary_msg', lang_override=lang, link=primary_link)}\n"; 
    # Fallback message can be added here if desired:
    # text += f"{get_text(user_id, 'task_setup_fallback_msg', lang_override=lang, link=fallback_link)}\n"; 
    text += f"{get_text(user_id, 'task_setup_start_time', lang_override=lang, time=start_time_str)}\n"
    text += f"{get_text(user_id, 'task_setup_interval', lang_override=lang, interval=interval_str)}\n"; 
    text += f"{get_text(user_id, 'task_setup_target', lang_override=lang, target=target_str)}\n\n"
    text += f"{get_text(user_id, 'task_setup_last_run', lang_override=lang, time=last_run_str)}\n"; 
    text += f"{get_text(user_id, 'task_setup_last_error', lang_override=lang, error=last_error)}\n"
    
    keyboard = [
        [InlineKeyboardButton(get_text(user_id, 'task_button_set_message', lang_override=lang), callback_data=f"{CALLBACK_TASK_PREFIX}set_primary_link")], 
        [InlineKeyboardButton(get_text(user_id, 'task_button_set_time', lang_override=lang), callback_data=f"{CALLBACK_TASK_PREFIX}set_time"), 
         InlineKeyboardButton(get_text(user_id, 'task_button_set_interval', lang_override=lang), callback_data=f"{CALLBACK_TASK_PREFIX}set_interval")], 
        [InlineKeyboardButton(get_text(user_id, 'task_button_set_target', lang_override=lang), callback_data=f"{CALLBACK_TASK_PREFIX}set_target_type")], 
        [InlineKeyboardButton(get_text(user_id, 'task_button_deactivate' if status == 'active' else 'task_button_activate', lang_override=lang), callback_data=f"{CALLBACK_TASK_PREFIX}toggle_status"), 
         InlineKeyboardButton(get_text(user_id, 'task_button_save', lang_override=lang), callback_data=f"{CALLBACK_TASK_PREFIX}save")], 
        [InlineKeyboardButton(get_text(user_id, 'button_back', lang_override=lang), callback_data=f"{CALLBACK_TASK_PREFIX}back_to_bot_select")]
    ]; 
    markup = InlineKeyboardMarkup(keyboard)
    await send_or_edit_message(update, context, text, reply_markup=markup, disable_web_page_preview=True)
    return STATE_TASK_SETUP # Stays in this state, actions are via callbacks

async def task_prompt_set_link(update: Update, context: CallbackContext, link_type: str) -> int:
    query = update.callback_query; await query.answer()
    user_id, lang = get_user_id_and_lang(update, context)
    prompt_key = 'task_prompt_primary_link' if link_type == 'primary' else 'task_prompt_fallback_link'; 
    next_state = STATE_WAITING_FOR_PRIMARY_MESSAGE_LINK if link_type == 'primary' else STATE_WAITING_FOR_FALLBACK_MESSAGE_LINK
    text = get_text(user_id, prompt_key, lang_override=lang); 
    keyboard = [[InlineKeyboardButton(get_text(user_id, 'button_back', lang_override=lang), callback_data=f"{CALLBACK_TASK_PREFIX}back_to_task_menu")]]; 
    markup = InlineKeyboardMarkup(keyboard)
    await send_or_edit_message(update, context, text, reply_markup=markup)
    return next_state

async def process_task_link(update: Update, context: CallbackContext, link_type: str) -> int | None:
    user_id, lang = get_user_id_and_lang(update, context); phone = context.user_data.get(CTX_TASK_PHONE); task_settings = context.user_data.get(CTX_TASK_SETTINGS)
    if not phone or task_settings is None: await send_or_edit_message(update, context, get_text(user_id, 'session_expired', lang_override=lang)); clear_conversation_data(context); return ConversationHandler.END
    
    link_text = update.message.text.strip(); 
    expected_next_state = STATE_WAITING_FOR_PRIMARY_MESSAGE_LINK if link_type == 'primary' else STATE_WAITING_FOR_FALLBACK_MESSAGE_LINK
    
    if link_type == 'fallback' and link_text.lower() == 'skip': 
        task_settings['fallback_message_link'] = None; 
        await send_or_edit_message(update, context, get_text(user_id, 'task_set_skipped_fallback', lang_override=lang)); 
        return await task_show_settings_menu(update, context) # Returns STATE_TASK_SETUP
        
    link_parsed_type, _ = telethon_api.parse_telegram_url_simple(link_text)
    if link_parsed_type != "message_link": 
        await send_or_edit_message(update, context, get_text(user_id, 'task_error_invalid_link', lang_override=lang)); 
        return expected_next_state
        
    await send_or_edit_message(update, context, get_text(user_id, 'task_verifying_link', lang_override=lang)); link_verified = False
    try:
        log.info(f"Verifying link access for {link_text} via bot {phone}..."); 
        accessible = await telethon_api.check_message_link_access(phone, link_text)
        if not accessible: 
            log.warning(f"Link {link_text} not accessible by bot {phone}."); 
            await send_or_edit_message(update, context, get_text(user_id, 'task_error_link_unreachable', lang_override=lang, bot_phone=html.escape(phone))); 
            return expected_next_state
        else: 
            log.info(f"User {user_id} link {link_text} verified successfully by bot {phone}."); link_verified = True
    except Exception as e: 
        log.error(f"Error checking link access {phone} -> {link_text}: {e}", exc_info=True); 
        await send_or_edit_message(update, context, get_text(user_id, 'error_telegram_api', lang_override=lang, error=html.escape(str(e)))); 
        return expected_next_state
        
    if link_verified:
        success_msg_key = 'task_set_success_msg' if link_type == 'primary' else 'task_set_success_fallback'
        if link_type == 'primary': task_settings['message_link'] = link_text
        else: task_settings['fallback_message_link'] = link_text
        await send_or_edit_message(update, context, get_text(user_id, success_msg_key, lang_override=lang)); 
        return await task_show_settings_menu(update, context) # Returns STATE_TASK_SETUP
    else: # Should not be reached if verification fails and returns early
        log.error(f"Link verification failed unexpectedly for {link_text}."); 
        await send_or_edit_message(update, context, get_text(user_id, 'error_generic', lang_override=lang)); 
        return expected_next_state

async def task_prompt_start_time(update: Update, context: CallbackContext) -> int:
     query = update.callback_query; await query.answer()
     user_id, lang = get_user_id_and_lang(update, context); 
     local_tz_name = LITHUANIA_TZ.zone if hasattr(LITHUANIA_TZ, 'zone') else str(LITHUANIA_TZ)
     text = get_text(user_id, 'task_prompt_start_time', lang_override=lang, timezone_name=local_tz_name); 
     keyboard = [[InlineKeyboardButton(get_text(user_id, 'button_back', lang_override=lang), callback_data=f"{CALLBACK_TASK_PREFIX}back_to_task_menu")]]; 
     markup = InlineKeyboardMarkup(keyboard)
     await send_or_edit_message(update, context, text, reply_markup=markup)
     return STATE_WAITING_FOR_START_TIME

async def process_task_start_time(update: Update, context: CallbackContext) -> int | None:
    user_id, lang = get_user_id_and_lang(update, context); time_str = update.message.text.strip(); 
    task_settings = context.user_data.get(CTX_TASK_SETTINGS)
    if task_settings is None: await send_or_edit_message(update, context, get_text(user_id, 'session_expired', lang_override=lang)); clear_conversation_data(context); return ConversationHandler.END
    
    try: 
        hour, minute = map(int, time_str.split(':')); 
        if not (0 <= hour <= 23 and 0 <= minute <= 59): raise ValueError("Time out of range")
    except (ValueError, TypeError): 
        await send_or_edit_message(update, context, get_text(user_id, 'task_error_invalid_time', lang_override=lang)); 
        return STATE_WAITING_FOR_START_TIME
        
    try:
        now_local = datetime.now(LITHUANIA_TZ); 
        input_time_obj = datetime.strptime(f"{hour:02d}:{minute:02d}", "%H:%M").time()
        # Combine with today's date in local timezone
        target_local_dt_naive = datetime.combine(now_local.date(), input_time_obj); 
        target_local_dt = LITHUANIA_TZ.localize(target_local_dt_naive) # Make it timezone-aware
        
        # If the resulting time is in the past for today, schedule for tomorrow
        if target_local_dt <= now_local: target_local_dt += timedelta(days=1)
            
        target_utc = target_local_dt.astimezone(UTC_TZ); 
        start_timestamp = int(target_utc.timestamp())
        task_settings['start_time'] = start_timestamp; 
        log.info(f"User {user_id} set task start time: {time_str} LT -> {start_timestamp} UTC ({target_utc.strftime('%Y-%m-%d %H:%M:%S %Z')})")
        await send_or_edit_message(update, context, get_text(user_id, 'task_set_success_time', lang_override=lang, time=time_str)); 
        return await task_show_settings_menu(update, context) # Returns STATE_TASK_SETUP
    except Exception as e: 
        log.error(f"Error converting start time '{time_str}' for user {user_id}: {e}", exc_info=True); 
        await send_or_edit_message(update, context, get_text(user_id, 'error_generic', lang_override=lang)); 
        return STATE_WAITING_FOR_START_TIME

async def task_select_interval(update: Update, context: CallbackContext) -> int:
    query = update.callback_query; await query.answer()
    user_id, lang = get_user_id_and_lang(update, context); 
    intervals = [5, 10, 15, 30, 60, 120, 180, 240, 360, 720, 1440]; keyboard = []; row = []
    for minutes in intervals:
        if minutes < 60: label = f"{minutes} min"
        elif minutes % (60*24) == 0: label = f"{minutes // (60*24)} d"
        elif minutes % 60 == 0: label = f"{minutes // 60} h"
        else: label = f"{minutes // 60} h {minutes % 60} min"
        button_text = get_text(user_id, 'task_interval_button', lang_override=lang, value=label); 
        row.append(InlineKeyboardButton(button_text, callback_data=f"{CALLBACK_INTERVAL_PREFIX}{minutes}"))
        if len(row) >= 3: keyboard.append(row); row = []
    if row: keyboard.append(row)
    keyboard.append([InlineKeyboardButton(get_text(user_id, 'button_back', lang_override=lang), callback_data=f"{CALLBACK_TASK_PREFIX}back_to_task_menu")]); 
    markup = InlineKeyboardMarkup(keyboard)
    await send_or_edit_message(update, context, get_text(user_id, 'task_select_interval_title', lang_override=lang), reply_markup=markup)
    return STATE_TASK_SETUP # Stays in task setup, callback will modify context and re-render

async def process_interval_callback(update: Update, context: CallbackContext) -> int | None:
    query = update.callback_query; await query.answer()
    user_id, lang = get_user_id_and_lang(update, context); task_settings = context.user_data.get(CTX_TASK_SETTINGS)
    if task_settings is None: await send_or_edit_message(update,context, get_text(user_id, 'session_expired', lang_override=lang)); return await client_menu(update, context)
    try: interval_minutes = int(query.data.split(CALLBACK_INTERVAL_PREFIX)[1])
    except (ValueError, IndexError, AssertionError): 
        log.error(f"Invalid interval callback data: {query.data}"); 
        await send_or_edit_message(update,context, get_text(user_id, 'error_invalid_input', lang_override=lang)); 
        return STATE_TASK_SETUP # Back to task menu
        
    task_settings['repetition_interval'] = interval_minutes; 
    log.info(f"User {user_id} set task interval to {interval_minutes} minutes (unsaved).")
    # Interval value like "30 min" for message
    interval_display_val = ""
    if interval_minutes < 60: interval_display_val = f"{interval_minutes} min"
    elif interval_minutes % (60*24) == 0: interval_display_val = f"{interval_minutes // (60*24)} d"
    elif interval_minutes % 60 == 0: interval_display_val = f"{interval_minutes // 60} h"
    else: interval_display_val = f"{interval_minutes // 60} h {interval_minutes % 60} min"
    await send_or_edit_message(update, context, get_text(user_id, 'task_set_success_interval', lang_override=lang, interval=interval_display_val))

    # Re-show task menu to reflect change
    return await task_show_settings_menu(update, context) # Returns STATE_TASK_SETUP

async def task_select_target_type(update: Update, context: CallbackContext) -> int:
    query = update.callback_query; await query.answer()
    user_id, lang = get_user_id_and_lang(update, context); 
    keyboard = [
        [InlineKeyboardButton(get_text(user_id, 'task_button_target_folder', lang_override=lang), callback_data=f"{CALLBACK_TASK_PREFIX}select_folder_target?page=0")], 
        [InlineKeyboardButton(get_text(user_id, 'task_button_target_all', lang_override=lang), callback_data=f"{CALLBACK_TASK_PREFIX}set_target_all")], 
        [InlineKeyboardButton(get_text(user_id, 'button_back', lang_override=lang), callback_data=f"{CALLBACK_TASK_PREFIX}back_to_task_menu")]
    ]; 
    markup = InlineKeyboardMarkup(keyboard)
    await send_or_edit_message(update, context, get_text(user_id, 'task_select_target_title', lang_override=lang), reply_markup=markup)
    return STATE_TASK_SETUP # Stays in task setup state

async def task_select_folder_for_target(update: Update, context: CallbackContext) -> int:
    query = update.callback_query; await query.answer()
    user_id, lang = get_user_id_and_lang(update, context); current_page = 0
    try:
        if query and query.data and '?page=' in query.data: current_page = int(query.data.split('?page=')[1])
    except (ValueError, IndexError, AttributeError): current_page = 0
    
    folders_rows = db.get_folders_by_user(user_id)
    folders = [dict(f_row) for f_row in folders_rows]

    if not folders: 
        await send_or_edit_message(update,context, get_text(user_id, 'task_error_no_folders', lang_override=lang)); 
        return await task_select_target_type(update, context) # Back to target type selection
        
    total_items = len(folders); start_index = current_page * ITEMS_PER_PAGE; end_index = start_index + ITEMS_PER_PAGE; folders_page = folders[start_index:end_index]
    text = get_text(user_id, 'task_select_folder_title', lang_override=lang); keyboard = []
    for folder in folders_page: 
        button_text = html.escape(folder['name']); 
        callback_data = f"{CALLBACK_TASK_PREFIX}set_target_folder?id={folder['id']}"; 
        keyboard.append([InlineKeyboardButton(button_text, callback_data=callback_data)])
        
    base_callback = f"{CALLBACK_TASK_PREFIX}select_folder_target"; 
    pagination_buttons = build_pagination_buttons(base_callback, current_page, total_items, ITEMS_PER_PAGE, lang=lang); keyboard.extend(pagination_buttons)
    keyboard.append([InlineKeyboardButton(get_text(user_id, 'button_back', lang_override=lang), callback_data=f"{CALLBACK_TASK_PREFIX}back_to_target_type")]); 
    markup = InlineKeyboardMarkup(keyboard)
    await send_or_edit_message(update, context, text, reply_markup=markup)
    return STATE_TASK_SETUP # Stays in task setup

async def task_set_target(update: Update, context: CallbackContext, target_type_from_cb: str) -> int | None:
    query = update.callback_query; await query.answer()
    user_id, lang = get_user_id_and_lang(update, context); task_settings = context.user_data.get(CTX_TASK_SETTINGS)
    if task_settings is None: await send_or_edit_message(update,context, get_text(user_id, 'session_expired', lang_override=lang)); return await client_menu(update, context)
    
    if target_type_from_cb == 'all': 
        task_settings['send_to_all_groups'] = 1; task_settings['folder_id'] = None; 
        log.info(f"User {user_id} set task target to all groups (unsaved).")
        await send_or_edit_message(update, context, get_text(user_id, 'task_set_success_target_all', lang_override=lang))
    elif target_type_from_cb == 'folder':
        try: folder_id = int(query.data.split('?id=')[1])
        except (ValueError, IndexError, AttributeError): 
            log.error(f"Could not parse folder ID from callback: {query.data}"); 
            await send_or_edit_message(update,context, get_text(user_id, 'error_generic', lang_override=lang)); 
            return STATE_TASK_SETUP
            
        folder_name = db.get_folder_name(folder_id)
        if not folder_name: 
            await send_or_edit_message(update,context, get_text(user_id, 'folder_not_found_error', lang_override=lang)); 
            return STATE_TASK_SETUP # Or back to folder selection? For now, task menu.
            
        task_settings['send_to_all_groups'] = 0; task_settings['folder_id'] = folder_id; 
        log.info(f"User {user_id} set task target to folder {folder_name} ({folder_id}) (unsaved).")
        await send_or_edit_message(update, context, get_text(user_id, 'task_set_success_target_folder', lang_override=lang, name=html.escape(folder_name)))
    else: 
        log.error(f"Invalid target_type '{target_type_from_cb}' in task_set_target."); 
        return STATE_TASK_SETUP # Back to task menu
        
    return await task_show_settings_menu(update, context) # Re-render menu

async def task_toggle_status(update: Update, context: CallbackContext) -> int | None:
    query = update.callback_query; await query.answer()
    user_id, lang = get_user_id_and_lang(update, context); task_settings = context.user_data.get(CTX_TASK_SETTINGS)
    if task_settings is None: await send_or_edit_message(update,context, get_text(user_id, 'session_expired', lang_override=lang)); return await client_menu(update, context)
    
    current_status = task_settings.get('status', 'inactive'); 
    new_status = 'inactive' if current_status == 'active' else 'active'
    
    if new_status == 'active': # Validate before activating
        missing_fields = []
        if not task_settings.get('message_link'): missing_fields.append(get_text(user_id, 'task_required_message', lang_override=lang))
        if not task_settings.get('start_time'): missing_fields.append(get_text(user_id, 'task_required_start_time', lang_override=lang))
        if not task_settings.get('repetition_interval'): missing_fields.append(get_text(user_id, 'task_required_interval', lang_override=lang))
        if not task_settings.get('folder_id') and not task_settings.get('send_to_all_groups'): 
            missing_fields.append(get_text(user_id, 'task_required_target', lang_override=lang))
            
        if missing_fields: 
            missing_str = ", ".join(missing_fields); 
            await send_or_edit_message(update, context, get_text(user_id, 'task_save_validation_fail', lang_override=lang, missing=missing_str)); 
            return await task_show_settings_menu(update, context) # Stay on menu, don't change status
            
    task_settings['status'] = new_status; 
    log.info(f"User {user_id} toggled task status to {new_status} (unsaved).")
    await send_or_edit_message(update, context, get_text(user_id, 'task_status_toggled_success', lang_override=lang, status=new_status.capitalize()))
    return await task_show_settings_menu(update, context) # Re-render menu

async def task_save_settings(update: Update, context: CallbackContext) -> int | None:
    query = update.callback_query; await query.answer()
    user_id, lang = get_user_id_and_lang(update, context); phone = context.user_data.get(CTX_TASK_PHONE); 
    settings_to_save = context.user_data.get(CTX_TASK_SETTINGS)
    
    if not phone or settings_to_save is None: 
        await send_or_edit_message(update,context, get_text(user_id, 'session_expired', lang_override=lang)); 
        return await client_menu(update, context)
        
    # Final validation if activating or saving as active
    if settings_to_save.get('status') == 'active':
        missing_fields = []
        if not settings_to_save.get('message_link'): missing_fields.append(get_text(user_id, 'task_required_message', lang_override=lang))
        if not settings_to_save.get('start_time'): missing_fields.append(get_text(user_id, 'task_required_start_time', lang_override=lang))
        if not settings_to_save.get('repetition_interval'): missing_fields.append(get_text(user_id, 'task_required_interval', lang_override=lang))
        if not settings_to_save.get('folder_id') and not settings_to_save.get('send_to_all_groups'): 
            missing_fields.append(get_text(user_id, 'task_required_target', lang_override=lang))
            
        if missing_fields: 
            missing_str = ", ".join(missing_fields); 
            await send_or_edit_message(update, context, get_text(user_id, 'task_save_validation_fail', lang_override=lang, missing=missing_str)); 
            return STATE_TASK_SETUP # Stay on menu for corrections
            
    settings_to_save['last_error'] = None # Clear last error on manual save attempt
    
    if db.save_userbot_task_settings(user_id, phone, settings_to_save):
        db.log_event_db("Task Settings Saved", f"User: {user_id}, Bot: {phone}, Status: {settings_to_save.get('status')}", user_id=user_id, userbot_phone=phone); 
        bot_db_info_row = db.find_userbot(phone)
        bot_db_info = dict(bot_db_info_row) if bot_db_info_row else {}
        display_name = html.escape(f"@{bot_db_info.get('username')}" if bot_db_info.get('username') else phone)
        await send_or_edit_message(update, context, get_text(user_id, 'task_save_success', lang_override=lang, display_name=display_name))
        clear_conversation_data(context); # Clear task-specific context
        await client_menu(update, context); # Back to main client menu
        return ConversationHandler.END
    else: 
        db.log_event_db("Task Save Failed", f"User: {user_id}, Bot: {phone}, DB Error", user_id=user_id, userbot_phone=phone); 
        await send_or_edit_message(update, context, get_text(user_id, 'task_save_error', lang_override=lang)); 
        return STATE_TASK_SETUP # Stay on menu if DB error

# --- Admin Handlers ---
async def admin_list_userbots(update: Update, context: CallbackContext) -> int:
    query = update.callback_query; await query.answer()
    user_id, lang = get_user_id_and_lang(update, context); current_page = 0
    try:
        if query and query.data and '?page=' in query.data: current_page = int(query.data.split('?page=')[1])
    except (ValueError, IndexError, AttributeError): current_page = 0
    
    all_bots_rows = db.get_all_userbots()
    all_bots = [dict(b_row) for b_row in all_bots_rows]

    if not all_bots: 
        text = get_text(user_id, 'admin_userbot_list_no_bots', lang_override=lang); 
        markup = InlineKeyboardMarkup([[InlineKeyboardButton(get_text(user_id, 'button_back', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}back_to_menu")]]); 
        await send_or_edit_message(update, context, text, reply_markup=markup); 
        return ConversationHandler.END
        
    total_items = len(all_bots); start_index = current_page * ITEMS_PER_PAGE; end_index = start_index + ITEMS_PER_PAGE; bots_page = all_bots[start_index:end_index]
    text = f"<b>{get_text(user_id, 'admin_userbot_list_title', lang_override=lang)}</b> (Page {current_page + 1}/{math.ceil(total_items / ITEMS_PER_PAGE)})\n\n"
    for bot in bots_page:
        phone = bot.get('phone_number'); username = bot.get('username'); status = bot.get('status','unknown'); 
        assigned_client_code = bot.get('assigned_client') or get_text(user_id, 'admin_userbot_list_unassigned', lang_override=lang); 
        last_error = bot.get('last_error'); 
        display_name = f"@{username}" if username else phone
        status_icon_key = f'admin_userbot_list_status_icon_{status}'; 
        icon_fallback = {'active': "🟢", 'inactive': "⚪️", 'error': "🔴", 'connecting': "🔌", 'needs_code': "🔢", 'needs_password': "🔒", 'authenticating': "⏳", 'initializing': "⚙️"}.get(status, "❓"); 
        status_icon = get_text(user_id, status_icon_key, lang_override=lang) if status_icon_key in translations.get(lang, {}) else icon_fallback
        text += get_text(user_id, 'admin_userbot_list_line', lang_override=lang, status_icon=status_icon, display_name=html.escape(display_name), phone=html.escape(phone), client_code=html.escape(assigned_client_code), status=html.escape(status.capitalize())) + "\n"
        if last_error: 
            error_text = html.escape(last_error); 
            text += get_text(user_id, 'admin_userbot_list_error_line', lang_override=lang, error=error_text[:150] + ("..." if len(error_text)>150 else "")) + "\n"
            
    keyboard = []; base_callback = f"{CALLBACK_ADMIN_PREFIX}list_bots"; 
    pagination_buttons = build_pagination_buttons(base_callback, current_page, total_items, ITEMS_PER_PAGE, lang=lang); keyboard.extend(pagination_buttons)
    keyboard.append([InlineKeyboardButton(get_text(user_id, 'button_back', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}back_to_menu")]); 
    markup = InlineKeyboardMarkup(keyboard)
    await send_or_edit_message(update, context, text, reply_markup=markup, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    return ConversationHandler.END

async def admin_select_userbot_to_remove(update: Update, context: CallbackContext) -> int:
    query = update.callback_query; await query.answer()
    user_id, lang = get_user_id_and_lang(update, context); current_page = 0
    try:
        if query and query.data and '?page=' in query.data: current_page = int(query.data.split('?page=')[1])
    except (ValueError, IndexError, AttributeError): current_page = 0
        
    all_bots_rows = db.get_all_userbots()
    all_bots = [dict(b_row) for b_row in all_bots_rows]

    if not all_bots: 
        text = get_text(user_id, 'admin_userbot_no_bots_to_remove', lang_override=lang); 
        markup = InlineKeyboardMarkup([[InlineKeyboardButton(get_text(user_id, 'button_back', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}back_to_menu")]]); 
        await send_or_edit_message(update, context, text, reply_markup=markup); 
        return ConversationHandler.END
        
    total_items = len(all_bots); start_index = current_page * ITEMS_PER_PAGE; end_index = start_index + ITEMS_PER_PAGE; bots_page = all_bots[start_index:end_index]
    text = get_text(user_id, 'admin_userbot_select_remove', lang_override=lang); keyboard = []
    for bot in bots_page: 
        phone = bot.get('phone_number'); username = bot.get('username'); 
        display_name = f"@{username}" if username else phone; 
        button_text = f"🗑️ {html.escape(display_name)}"; 
        keyboard.append([InlineKeyboardButton(button_text, callback_data=f"{CALLBACK_ADMIN_PREFIX}remove_bot_confirm_prompt_{phone}")])
        
    base_callback = f"{CALLBACK_ADMIN_PREFIX}remove_bot_select"; 
    pagination_buttons = build_pagination_buttons(base_callback, current_page, total_items, ITEMS_PER_PAGE, lang=lang); keyboard.extend(pagination_buttons)
    keyboard.append([InlineKeyboardButton(get_text(user_id, 'button_back', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}back_to_menu")]); 
    markup = InlineKeyboardMarkup(keyboard)
    await send_or_edit_message(update, context, text, reply_markup=markup)
    return STATE_ADMIN_CONFIRM_USERBOT_RESET # State for handling callbacks from this menu

async def admin_confirm_remove_userbot_prompt(update: Update, context: CallbackContext) -> int:
     query = update.callback_query; await query.answer()
     user_id, lang = get_user_id_and_lang(update, context); phone_to_remove = None
     try: phone_to_remove = query.data.split(f"{CALLBACK_ADMIN_PREFIX}remove_bot_confirm_prompt_")[1]
     except IndexError: 
         log.error(f"Could not parse phone from remove confirm prompt callback: {query.data}"); 
         await send_or_edit_message(update,context, get_text(user_id, 'error_generic', lang_override=lang)); 
         await admin_command(update, context); return ConversationHandler.END # Go to admin menu
         
     bot_info_row = db.find_userbot(phone_to_remove)
     if not bot_info_row: 
         await send_or_edit_message(update,context, get_text(user_id, 'admin_userbot_not_found', lang_override=lang)); 
         await admin_command(update, context); return ConversationHandler.END
         
     bot_info = dict(bot_info_row)
     username = bot_info.get('username'); display_name = html.escape(f"@{username}" if username else phone_to_remove)
     text = get_text(user_id, 'admin_userbot_remove_confirm_text', lang_override=lang, display_name=display_name)
     keyboard = [
         [InlineKeyboardButton(get_text(user_id, 'button_yes', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}remove_bot_confirmed_execute_{phone_to_remove}")], 
         [InlineKeyboardButton(get_text(user_id, 'button_no', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}back_to_menu")]
     ]; 
     markup = InlineKeyboardMarkup(keyboard)
     await send_or_edit_message(update, context, text, reply_markup=markup)
     return STATE_ADMIN_CONFIRM_USERBOT_RESET # Stays in this state for the Yes/No callback

async def admin_remove_userbot_confirmed_execute(update: Update, context: CallbackContext) -> int:
    query = update.callback_query; await query.answer()
    user_id, lang = get_user_id_and_lang(update, context); phone_to_remove = None
    try: phone_to_remove = query.data.split(f"{CALLBACK_ADMIN_PREFIX}remove_bot_confirmed_execute_")[1]
    except IndexError: 
        log.error(f"Could not parse phone from remove confirmed execute callback: {query.data}"); 
        await send_or_edit_message(update,context, get_text(user_id, 'error_generic', lang_override=lang)); 
        return await admin_command(update, context)
        
    bot_info_row = db.find_userbot(phone_to_remove); display_name = "N/A";
    if bot_info_row: 
        bot_info = dict(bot_info_row)
        display_name = html.escape(f"@{bot_info.get('username')}" if bot_info.get('username') else phone_to_remove)
        
    log.info(f"Admin {user_id} confirmed removal of userbot {phone_to_remove}")
    stopped = telethon_api.stop_userbot_runtime(phone_to_remove); 
    log.info(f"Runtime stop request for {phone_to_remove}: {'Successful' if stopped else 'Not running/Failed'}")
    
    if db.remove_userbot(phone_to_remove):
        log.info(f"Attempting to remove session files for {phone_to_remove}..."); 
        telethon_api.delete_session_files_for_phone(phone_to_remove) # Ensure this uses the public name if it was renamed
        db.log_event_db("Userbot Removed", f"Phone: {phone_to_remove}", user_id=user_id, userbot_phone=phone_to_remove); 
        await send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_remove_success', lang_override=lang, display_name=display_name))
    else: 
        await send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_remove_error', lang_override=lang))
        
    return await admin_command(update, context)

async def admin_view_subscriptions(update: Update, context: CallbackContext) -> int:
    query = update.callback_query; await query.answer()
    user_id, lang = get_user_id_and_lang(update, context); current_page = 0
    try:
        if query and query.data and '?page=' in query.data: current_page = int(query.data.split('?page=')[1])
    except (ValueError, IndexError, AttributeError): current_page = 0
        
    subs_rows = db.get_all_subscriptions() # Returns list of Row objects
    subs = [dict(s_row) for s_row in subs_rows]

    if not subs: 
        text = get_text(user_id, 'admin_subs_none', lang_override=lang); 
        markup = InlineKeyboardMarkup([[InlineKeyboardButton(get_text(user_id, 'button_back', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}back_to_menu")]]); 
        await send_or_edit_message(update, context, text, reply_markup=markup); 
        return ConversationHandler.END
        
    total_items = len(subs); start_index = current_page * ITEMS_PER_PAGE; end_index = start_index + ITEMS_PER_PAGE; subs_page = subs[start_index:end_index]
    text = f"<b>{get_text(user_id, 'admin_subs_title', lang_override=lang)}</b> (Page {current_page + 1}/{math.ceil(total_items / ITEMS_PER_PAGE)})\n\n"
    for sub_item in subs_page: # sub_item is a dict now
        client_user_id = sub_item.get('user_id'); user_link = get_text(user_id, 'admin_subs_no_user', lang_override=lang)
        if client_user_id:
             try: user_link = f"<a href='tg://user?id={client_user_id}'>{client_user_id}</a>"
             except Exception as e: log.debug(f"Could not create user link for {client_user_id}: {e}"); user_link = f"ID: `{client_user_id}`"
        end_date = format_dt(sub_item.get('subscription_end')); 
        code = sub_item.get('invitation_code'); 
        bot_count = sub_item.get('bot_count', 0) # Ensure bot_count has a default
        text += get_text(user_id, 'admin_subs_line', lang_override=lang, user_link=user_link, code=html.escape(code), end_date=end_date, bot_count=bot_count) + "\n\n"
        
    keyboard = []; base_callback = f"{CALLBACK_ADMIN_PREFIX}view_subs"; 
    pagination_buttons = build_pagination_buttons(base_callback, current_page, total_items, ITEMS_PER_PAGE, lang=lang); keyboard.extend(pagination_buttons)
    keyboard.append([InlineKeyboardButton(get_text(user_id, 'button_back', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}back_to_menu")]); 
    markup = InlineKeyboardMarkup(keyboard)
    await send_or_edit_message(update, context, text, reply_markup=markup, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    return ConversationHandler.END

async def admin_view_system_logs(update: Update, context: CallbackContext) -> int:
    query = update.callback_query; await query.answer()
    user_id, lang = get_user_id_and_lang(update, context); limit = 25
    logs_from_db_rows = db.get_recent_logs(limit=limit)
    logs_from_db = [dict(l_row) for l_row in logs_from_db_rows]

    if not logs_from_db: 
        text = get_text(user_id, 'admin_logs_none', lang_override=lang); 
        markup = InlineKeyboardMarkup([[InlineKeyboardButton(get_text(user_id, 'button_back', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}back_to_menu")]]); 
        await send_or_edit_message(update, context, text, reply_markup=markup); 
        return ConversationHandler.END
        
    text = f"<b>{get_text(user_id, 'admin_logs_title', lang_override=lang, limit=limit)}</b>\n\n"
    for log_entry in logs_from_db:
        ts = log_entry.get('timestamp'); event = log_entry.get('event'); 
        log_user_id = log_entry.get('user_id'); log_bot_phone = log_entry.get('userbot_phone'); 
        details = log_entry.get('details'); time_str = format_dt(ts)
        
        user_str = get_text(user_id, 'admin_logs_user_none', lang_override=lang)
        if log_user_id: 
            user_str = get_text(user_id, 'admin_logs_user_admin', lang_override=lang) + f" ({log_user_id})" if is_admin(log_user_id) else f"Client ({log_user_id})"
            
        bot_str = html.escape(log_bot_phone) if log_bot_phone else get_text(user_id, 'admin_logs_bot_none', lang_override=lang); 
        details_str = html.escape(details[:100]) + ('...' if details and len(details)>100 else '') if details else ""
        text += get_text(user_id, 'admin_logs_line', lang_override=lang, time=time_str, event=html.escape(event), user=user_str, bot=bot_str, details=details_str) + "\n"
        
    keyboard = [[InlineKeyboardButton(get_text(user_id, 'button_back', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}back_to_menu")]]; 
    markup = InlineKeyboardMarkup(keyboard)
    await send_or_edit_message(update, context, text, reply_markup=markup, parse_mode=ParseMode.HTML)
    return ConversationHandler.END

# --- Admin Task Management Handlers ---
async def admin_task_menu(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    if query: await query.answer()
    user_id, lang = get_user_id_and_lang(update, context)
    keyboard = [
        [InlineKeyboardButton(get_text(user_id, 'admin_task_view', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}view_tasks?page=0")], 
        [InlineKeyboardButton(get_text(user_id, 'admin_task_create', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}create_task")], 
        [InlineKeyboardButton(get_text(user_id, 'button_back', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}back_to_menu")]
    ]; 
    markup = InlineKeyboardMarkup(keyboard)
    await send_or_edit_message(update, context, get_text(user_id, 'admin_task_menu_title', lang_override=lang), reply_markup=markup)
    return ConversationHandler.END

async def admin_view_tasks(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    if query: await query.answer()
    user_id, lang = get_user_id_and_lang(update, context); current_page = 0
    try:
        if query and query.data and '?page=' in query.data: current_page = int(query.data.split('?page=')[1])
    except (ValueError, IndexError, AttributeError): current_page = 0
    
    tasks_rows, total_tasks = db.get_admin_tasks(page=current_page, per_page=ITEMS_PER_PAGE); 
    tasks = [dict(t_row) for t_row in tasks_rows]
    keyboard = []
    text = f"<b>{get_text(user_id, 'admin_task_list_title', lang_override=lang)}</b>"
    if tasks:
        text += f" (Page {current_page + 1}/{math.ceil(total_tasks / ITEMS_PER_PAGE)})\n\n"
        for task in tasks:
            status_icon = "🟢" if task.get('status') == 'active' else "⚪️"
            task_info_line = f"{status_icon} Bot: {html.escape(task.get('userbot_phone','N/A'))} -> Target: {html.escape(task.get('target','N/A'))}"
            if task.get('schedule'): task_info_line += f" | Schedule: <code>{html.escape(task['schedule'])}</code>"
            task_id_for_cb = task.get('id')
            if task_id_for_cb is None:
                log.error(f"Admin task missing ID in admin_view_tasks: {task}")
                continue
            keyboard.append([InlineKeyboardButton(task_info_line, callback_data=f"{CALLBACK_ADMIN_PREFIX}task_options_{task_id_for_cb}")])
    else: text += "\n" + get_text(user_id, 'admin_task_list_empty', lang_override=lang)
    
    pagination_buttons = build_pagination_buttons(f"{CALLBACK_ADMIN_PREFIX}view_tasks", current_page, total_tasks, ITEMS_PER_PAGE, lang)
    keyboard.extend(pagination_buttons); 
    keyboard.append([InlineKeyboardButton(get_text(user_id, 'button_back', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}manage_tasks")])
    markup = InlineKeyboardMarkup(keyboard); 
    await send_or_edit_message(update, context, text, reply_markup=markup, parse_mode=ParseMode.HTML)
    return ConversationHandler.END

async def admin_create_task_start(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    if query: await query.answer()
    
    user_id, lang = get_user_id_and_lang(update, context); 
    if CTX_USER_ID not in context.user_data and user_id: context.user_data[CTX_USER_ID] = user_id
    if CTX_LANG not in context.user_data and lang: context.user_data[CTX_LANG] = lang
        
    context.user_data.pop(CTX_TASK_BOT, None)
    context.user_data.pop(CTX_TASK_MESSAGE, None)
    context.user_data.pop(CTX_TASK_SCHEDULE, None)
    context.user_data.pop(CTX_TASK_TARGET, None)
    
    return await admin_select_task_bot(update, context)

async def admin_select_task_bot(update: Update, context: CallbackContext) -> int:
    user_id, lang = get_user_id_and_lang(update, context); keyboard = []
    all_bots_db_rows = db.get_all_userbots(); 
    all_bots_db = [dict(b_row) for b_row in all_bots_db_rows]
    active_bots = [bot for bot in all_bots_db if bot.get('status') == 'active']
    
    if not active_bots: 
        await send_or_edit_message(update, context, get_text(user_id, 'admin_task_no_bots', lang_override=lang), 
                                   reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(get_text(user_id,'button_back',lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}manage_tasks")]]))
        return ConversationHandler.END
        
    for bot in active_bots: 
        display_name = f"@{bot.get('username')}" if bot.get('username') else bot.get('phone_number', 'Unknown Bot')
        bot_phone = bot.get('phone_number')
        if not bot_phone: 
            log.error(f"Active bot found with no phone_number: {bot}")
            continue
        keyboard.append([InlineKeyboardButton(html.escape(display_name), callback_data=f"{CALLBACK_ADMIN_PREFIX}task_bot_{bot_phone}")])
        
    keyboard.append([InlineKeyboardButton(get_text(user_id, 'button_back', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}manage_tasks")]); 
    markup = InlineKeyboardMarkup(keyboard)
    await send_or_edit_message(update, context, get_text(user_id, 'admin_task_select_bot', lang_override=lang), reply_markup=markup)
    return ConversationHandler.END 

async def admin_task_options(update: Update, context: CallbackContext) -> int:
    query = update.callback_query; await query.answer()
    user_id, lang = get_user_id_and_lang(update, context); task_id_str = None
    try: task_id_str = query.data.split(f"{CALLBACK_ADMIN_PREFIX}task_options_")[1]; task_id = int(task_id_str)
    except (IndexError, ValueError): 
        log.error(f"Failed to parse task_id from CB: {query.data}"); 
        await send_or_edit_message(update, context, get_text(user_id, 'error_generic', lang_override=lang)); 
        query.data = f"{CALLBACK_ADMIN_PREFIX}view_tasks?page=0" # Prepare for view_tasks
        return await admin_view_tasks(update, context)
    
    task_row = db.get_admin_task(task_id)
    if not task_row: 
        await send_or_edit_message(update, context, get_text(user_id, 'admin_task_not_found', lang_override=lang)); 
        query.data = f"{CALLBACK_ADMIN_PREFIX}view_tasks?page=0"
        return await admin_view_tasks(update, context)
    
    task = dict(task_row)
    status_icon = "🟢" if task.get('status') == 'active' else "⚪️"; 
    toggle_text_key = 'admin_task_deactivate' if task.get('status') == 'active' else 'admin_task_activate'; 
    toggle_text = get_text(user_id, toggle_text_key, lang_override=lang)
    
    keyboard = [
        [InlineKeyboardButton(toggle_text, callback_data=f"{CALLBACK_ADMIN_PREFIX}toggle_task_status_{task_id}")], 
        [InlineKeyboardButton(get_text(user_id, 'admin_task_delete_button', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}delete_task_confirm_{task_id}")], 
        [InlineKeyboardButton(get_text(user_id, 'button_back', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}view_tasks?page=0")]
    ]; 
    markup = InlineKeyboardMarkup(keyboard)
    
    details_text = f"<b>Task #{task_id} Details</b>\n"; 
    details_text += f"Status: {status_icon} {html.escape(task.get('status','N/A').capitalize())}\n"; 
    details_text += f"Bot: {html.escape(task.get('userbot_phone','N/A'))}\n"; 
    message_content = task.get('message', '')
    details_text += f"Message: <pre>{html.escape(message_content[:100])}{'...' if len(message_content) > 100 else ''}</pre>\n"; 
    details_text += f"Schedule: <code>{html.escape(task.get('schedule','N/A'))}</code>\n"; 
    details_text += f"Target: {html.escape(task.get('target','N/A'))}\n"; 
    details_text += f"Last Run: {format_dt(task.get('last_run')) if task.get('last_run') else 'Never'}\n"; 
    details_text += f"Next Run Estimate: {format_dt(task.get('next_run')) if task.get('next_run') else 'Not Scheduled'}\n"
    
    await send_or_edit_message(update, context, details_text, reply_markup=markup, parse_mode=ParseMode.HTML)
    return ConversationHandler.END

async def admin_toggle_task_status(update: Update, context: CallbackContext) -> int:
    query = update.callback_query; await query.answer()
    user_id, lang = get_user_id_and_lang(update, context); task_id_str = None
    try: task_id_str = query.data.split(f"{CALLBACK_ADMIN_PREFIX}toggle_task_status_")[1]; task_id = int(task_id_str)
    except (IndexError, ValueError): 
        log.error(f"Failed to parse task_id for toggle status: {query.data}"); 
        await send_or_edit_message(update, context, get_text(user_id, 'error_generic', lang_override=lang)); 
        query.data = f"{CALLBACK_ADMIN_PREFIX}view_tasks?page=0"
        return await admin_view_tasks(update, context)
    
    if db.toggle_admin_task_status(task_id):
        query.data = f"{CALLBACK_ADMIN_PREFIX}task_options_{task_id}"
        return await admin_task_options(update, context)
    else: 
        await send_or_edit_message(update, context, get_text(user_id, 'admin_task_error', lang_override=lang)); 
        query.data = f"{CALLBACK_ADMIN_PREFIX}view_tasks?page=0"
        return await admin_view_tasks(update, context)

async def admin_delete_task_confirm(update: Update, context: CallbackContext) -> int:
    query = update.callback_query; await query.answer()
    user_id, lang = get_user_id_and_lang(update, context); task_id_str = None
    try: task_id_str = query.data.split(f"{CALLBACK_ADMIN_PREFIX}delete_task_confirm_")[1]; task_id = int(task_id_str)
    except (IndexError, ValueError): 
        log.error(f"Failed to parse task_id for delete confirm: {query.data}"); 
        await send_or_edit_message(update, context, get_text(user_id, 'error_generic', lang_override=lang)); 
        query.data = f"{CALLBACK_ADMIN_PREFIX}view_tasks?page=0"
        return await admin_view_tasks(update, context)
    
    task = db.get_admin_task(task_id)
    if not task: 
        await send_or_edit_message(update, context, get_text(user_id, 'admin_task_not_found', lang_override=lang)); 
        query.data = f"{CALLBACK_ADMIN_PREFIX}view_tasks?page=0"
        return await admin_view_tasks(update, context)
    
    confirm_text = f"Are you sure you want to delete Task #{task_id}?\nBot: {html.escape(task.get('userbot_phone','N/A'))}\nTarget: {html.escape(task.get('target','N/A'))}"
    keyboard = [
        [InlineKeyboardButton(get_text(user_id, 'button_yes', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}delete_task_execute_{task_id}")], 
        [InlineKeyboardButton(get_text(user_id, 'button_no', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}task_options_{task_id}")]
    ]; 
    markup = InlineKeyboardMarkup(keyboard)
    await send_or_edit_message(update, context, confirm_text, reply_markup=markup, parse_mode=ParseMode.HTML)
    return ConversationHandler.END

async def admin_delete_task_execute(update: Update, context: CallbackContext) -> int:
    query = update.callback_query; await query.answer()
    user_id, lang = get_user_id_and_lang(update, context); task_id_str = None
    try: task_id_str = query.data.split(f"{CALLBACK_ADMIN_PREFIX}delete_task_execute_")[1]; task_id = int(task_id_str)
    except (IndexError, ValueError): 
        log.error(f"Failed to parse task_id for delete execute: {query.data}"); 
        await send_or_edit_message(update, context, get_text(user_id, 'error_generic', lang_override=lang)); 
        query.data = f"{CALLBACK_ADMIN_PREFIX}view_tasks?page=0"
        return await admin_view_tasks(update, context)
    
    if db.delete_admin_task(task_id): 
        await send_or_edit_message(update, context, get_text(user_id, 'admin_task_deleted', lang_override=lang))
    else: 
        await send_or_edit_message(update, context, get_text(user_id, 'admin_task_error', lang_override=lang))
    
    query.data = f"{CALLBACK_ADMIN_PREFIX}view_tasks?page=0"
    return await admin_view_tasks(update, context)

# --- Fallback Handler ---
async def conversation_fallback(update: Update, context: CallbackContext) -> int:
    try:
        user_id, lang = get_user_id_and_lang(update, context)
        current_state_key = None
        if context and hasattr(context, '_active_conversations'):
            conv_key = (update.effective_user.id, update.effective_chat.id) if update.effective_user and update.effective_chat else None
            if conv_key and conv_key in context._active_conversations:
                current_state_key = context._active_conversations[conv_key]

        log.warning(f"Conversation fallback triggered for user {user_id}. Current internal state key: {current_state_key}. Update: {update.to_json() if update else 'N/A'}")
        await send_or_edit_message(update, context, get_text(user_id, 'session_expired', lang_override=lang), parse_mode=ParseMode.HTML, reply_markup=None)
        clear_conversation_data(context)
        return ConversationHandler.END
    except Exception as e:
        log.error(f"Error in conversation_fallback: {e}", exc_info=True)
        if update and update.effective_chat:
            try: await context.bot.send_message(chat_id=update.effective_chat.id, text="An error occurred. Please try /start again.")
            except: pass
        clear_conversation_data(context)
        return ConversationHandler.END

# --- Main Callback Router ---
async def main_callback_handler(update: Update, context: CallbackContext) -> str | int | None:
    query = update.callback_query
    user_id = query.from_user.id; data = query.data
    log.info(f"Main CB Router: User {user_id}, Data '{data}'")
    
    _, lang = get_user_id_and_lang(update, context)
    
    next_state = ConversationHandler.END 
    processed_by_sub_handler = False

    try:
        if data.startswith(CALLBACK_CLIENT_PREFIX): 
            next_state = await handle_client_callback(update, context)
            processed_by_sub_handler = True
        elif data.startswith(CALLBACK_ADMIN_PREFIX): 
            next_state = await handle_admin_callback(update, context)
            processed_by_sub_handler = True
        elif data.startswith(CALLBACK_FOLDER_PREFIX): 
            next_state = await handle_folder_callback(update, context)
            processed_by_sub_handler = True
        elif data.startswith(CALLBACK_TASK_PREFIX): 
            next_state = await handle_task_callback(update, context)
            processed_by_sub_handler = True
        elif data.startswith(CALLBACK_JOIN_PREFIX): 
            next_state = await handle_join_callback(update, context)
            processed_by_sub_handler = True
        elif data.startswith(CALLBACK_LANG_PREFIX): 
            next_state = await handle_language_callback(update, context)
            processed_by_sub_handler = True
        elif data.startswith(CALLBACK_INTERVAL_PREFIX): 
            next_state = await handle_interval_callback(update, context)
            processed_by_sub_handler = True
        elif data.startswith(CALLBACK_GENERIC_PREFIX): 
            next_state = await handle_generic_callback(update, context)
            processed_by_sub_handler = True
        else: 
            if not query._answered: await query.answer(get_text(user_id, 'error_invalid_action', lang_override=lang, default_text="Unknown button."), show_alert=True)
            log.warning(f"Unknown callback data pattern in main_callback_handler: {data}")
            next_state = ConversationHandler.END
        
        if not processed_by_sub_handler and query and not query._answered:
            try: await query.answer() 
            except Exception: pass

        log.debug(f"main_callback_handler returning state: {next_state}")
        return next_state
        
    except Exception as e:
        log.error(f"Error in main_callback_handler for data '{data}': {e}", exc_info=True)
        if query and not query._answered:
            try: await query.answer(get_text(user_id, 'error_generic', lang_override=lang), show_alert=True)
            except Exception: pass
        log.debug("main_callback_handler returning END due to exception")
        return ConversationHandler.END

# --- Callback Sub-handlers ---
async def handle_client_callback(update: Update, context: CallbackContext) -> str | int | None:
    query = update.callback_query; user_id, lang = get_user_id_and_lang(update, context); data = query.data
    client_info_row = db.find_client_by_user_id(user_id)
    client_info = dict(client_info_row) if client_info_row else None
    log.info(f"handle_client_callback: User {user_id}, Data {data}, Lang {lang}")

    if not client_info or client_info.get('subscription_end', 0) < int(datetime.now(UTC_TZ).timestamp()): 
        log.warning(f"Expired/Invalid client {user_id} tried client action: {data}")
        if not query._answered: await query.answer(get_text(user_id, 'subscription_expired', lang_override=lang), show_alert=True)
        clear_conversation_data(context)
        return ConversationHandler.END

    action = data.split(CALLBACK_CLIENT_PREFIX)[1].split('?')[0]; 
    log.debug(f"Client CB Route: Action='{action}', Data='{data}'")
    next_state: int | str | None = ConversationHandler.END

    if action == "select_bot_task": 
        if not query._answered: await query.answer()
        next_state = await client_select_bot_generic(update, context, CALLBACK_TASK_PREFIX, STATE_TASK_SETUP, 'task_select_userbot')
    elif action == "manage_folders": 
        if not query._answered: await query.answer()
        await client_folder_menu(update, context); next_state = ConversationHandler.END
    elif action == "select_bot_join": 
        if not query._answered: await query.answer()
        next_state = await client_select_bot_generic(update, context, CALLBACK_JOIN_PREFIX, STATE_WAITING_FOR_GROUP_LINKS, 'join_select_userbot')
    elif action == "view_stats": 
        next_state = await client_show_stats(update, context)
    elif action == "language": 
        if not query._answered: await query.answer()
        next_state = await client_ask_select_language(update, context)
    elif action == "back_to_menu": 
        if not query._answered: await query.answer()
        clear_conversation_data(context); 
        await client_menu(update, context); next_state = ConversationHandler.END
    else: 
        log.warning(f"Unhandled CLIENT CB: Action='{action}', Data='{data}'"); 
        if not query._answered: await query.answer(get_text(user_id, 'error_invalid_action', lang_override=lang, default_text="Action not recognized."), show_alert=True)
        next_state = ConversationHandler.END
    return next_state

async def handle_admin_callback(update: Update, context: CallbackContext) -> str | int | None:
    query = update.callback_query; user_id, lang = get_user_id_and_lang(update, context); data = query.data
    
    if not is_admin(user_id): 
        if not query._answered: await query.answer(get_text(user_id, 'unauthorized', lang_override=lang), show_alert=True)
        return ConversationHandler.END

    action_full = data.split(CALLBACK_ADMIN_PREFIX)[1]; action_main = action_full.split('?')[0]; 
    log.info(f"handle_admin_callback: User {user_id}, ActionFull '{action_full}', ActionMain '{action_main}'")
    next_state: int | str | None = ConversationHandler.END

    if action_main == "back_to_menu": 
        if not query._answered: await query.answer()
        await admin_command(update, context); 
        next_state = ConversationHandler.END
    elif action_main == "add_bot_prompt": 
        if not query._answered: await query.answer()
        await send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_prompt_phone', lang_override=lang))
        next_state = STATE_WAITING_FOR_PHONE
    elif action_main == "remove_bot_select": 
        next_state = await admin_select_userbot_to_remove(update, context)
    elif action_main.startswith("remove_bot_confirm_prompt_"): 
        next_state = await admin_confirm_remove_userbot_prompt(update, context)
    elif action_main.startswith("remove_bot_confirmed_execute_"): 
        next_state = await admin_remove_userbot_confirmed_execute(update, context)
    elif action_main == "list_bots": 
        next_state = await admin_list_userbots(update, context)
    elif action_main == "gen_invite_prompt": 
        if not query._answered: await query.answer()
        await send_or_edit_message(update, context, get_text(user_id, 'admin_invite_prompt_details', lang_override=lang))
        next_state = STATE_WAITING_FOR_SUB_DETAILS
    elif action_main == "view_subs": 
        next_state = await admin_view_subscriptions(update, context)
    elif action_main == "extend_sub_prompt": 
        if not query._answered: await query.answer()
        await send_or_edit_message(update, context, get_text(user_id, 'admin_extend_prompt_code', lang_override=lang))
        next_state = STATE_WAITING_FOR_EXTEND_CODE
    elif action_main == "assign_bots_prompt": 
        if not query._answered: await query.answer()
        await send_or_edit_message(update, context, get_text(user_id, 'admin_assignbots_prompt_code', lang_override=lang))
        next_state = STATE_WAITING_FOR_ADD_USERBOTS_CODE
    elif action_main == "view_logs": 
        next_state = await admin_view_system_logs(update, context)
    elif action_main == "manage_tasks": 
        next_state = await admin_task_menu(update, context)
    elif action_main == "view_tasks": 
        next_state = await admin_view_tasks(update, context)
    elif action_main == "create_task": 
        next_state = await admin_create_task_start(update, context)
    elif action_main.startswith("task_bot_"): 
        if not query._answered: await query.answer()
        context.user_data[CTX_TASK_BOT] = action_main.replace("task_bot_", "")
        await send_or_edit_message(update, context, get_text(user_id, 'admin_task_enter_message', lang_override=lang))
        next_state = STATE_ADMIN_TASK_MESSAGE
    elif action_main.startswith("task_options_"): 
        next_state = await admin_task_options(update, context)
    elif action_main.startswith("toggle_task_status_"): 
        next_state = await admin_toggle_task_status(update, context)
    elif action_main.startswith("delete_task_confirm_"): 
        next_state = await admin_delete_task_confirm(update, context)
    elif action_main.startswith("delete_task_execute_"): 
        next_state = await admin_delete_task_execute(update, context)
    else: 
        if not query._answered: await query.answer(get_text(user_id, 'error_invalid_action', lang_override=lang, default_text="Admin action not recognized."), show_alert=True)
        next_state = ConversationHandler.END
    return next_state

async def handle_folder_callback(update: Update, context: CallbackContext) -> str | int | None:
    query = update.callback_query; user_id, lang = get_user_id_and_lang(update, context); data = query.data
    action = data.split(CALLBACK_FOLDER_PREFIX)[1].split('?')[0]; 
    log.info(f"handle_folder_callback: User {user_id}, Data {data}, Action {action}, Lang {lang}")
    next_state: int | str | None = ConversationHandler.END

    if action == "create_prompt": 
        if not query._answered: await query.answer()
        await send_or_edit_message(update, context, get_text(user_id, 'folder_create_prompt', lang_override=lang))
        next_state = STATE_WAITING_FOR_FOLDER_NAME
    elif action == "select_edit": 
        next_state = await client_select_folder_to_edit_or_delete(update, context, 'edit')
    elif action == "select_delete": 
        next_state = await client_select_folder_to_edit_or_delete(update, context, 'delete')
    elif action == "edit_selected": 
        next_state = await client_show_folder_edit_options(update, context)
    elif action == "delete_selected_prompt": 
        next_state = await client_confirm_folder_delete_prompt(update, context)
    elif action == "delete_confirmed_execute": 
        next_state = await client_delete_folder_confirmed_execute(update, context)
    elif action == "back_to_manage": 
        if not query._answered: await query.answer()
        clear_conversation_data(context); 
        await client_folder_menu(update, context)
        next_state = ConversationHandler.END
    elif action == "edit_add_prompt": 
        if not query._answered: await query.answer()
        folder_name = context.user_data.get(CTX_FOLDER_NAME, get_text(user_id, "this_folder", lang_override=lang, default_text="this folder"))
        await send_or_edit_message(update, context, get_text(user_id, 'folder_edit_add_prompt', lang_override=lang, name=html.escape(folder_name)))
        next_state = STATE_WAITING_FOR_GROUP_LINKS # This state will use process_folder_links
    elif action == "edit_remove_select": 
        next_state = await client_select_groups_to_remove(update, context)
    elif action == "edit_toggle_remove": 
        next_state = await client_toggle_group_for_removal(update, context)
    elif action == "edit_remove_confirm": 
        next_state = await client_confirm_remove_selected_groups(update, context)
    elif action == "edit_rename_prompt": 
        if not query._answered: await query.answer()
        current_name = context.user_data.get(CTX_FOLDER_NAME, get_text(user_id, "this_folder", lang_override=lang, default_text="this folder"))
        await send_or_edit_message(update, context, get_text(user_id, 'folder_edit_rename_prompt', lang_override=lang, current_name=html.escape(current_name)))
        next_state = STATE_FOLDER_RENAME_PROMPT
    elif action == "back_to_edit_options": 
        if not query._answered: await query.answer()
        context.user_data.pop(CTX_TARGET_GROUP_IDS_TO_REMOVE, None); 
        next_state = await client_show_folder_edit_options(update, context)
    else: 
        log.warning(f"Unhandled FOLDER CB: Action='{action}', Data='{data}'"); 
        if not query._answered: await query.answer(get_text(user_id, 'error_invalid_action', lang_override=lang, default_text="Folder action not recognized."), show_alert=True)
        next_state = ConversationHandler.END
    return next_state

async def handle_task_callback(update: Update, context: CallbackContext) -> str | int | None:
    query = update.callback_query; user_id, lang = get_user_id_and_lang(update, context); data = query.data
    action = data.split(CALLBACK_TASK_PREFIX)[1].split('?')[0]; 
    log.info(f"handle_task_callback: User {user_id}, Data {data}, Action {action}, Lang {lang}")
    next_state: int | str | None = STATE_TASK_SETUP

    if action.startswith("select_"): 
        next_state = await handle_userbot_selection_callback(update, context, CALLBACK_TASK_PREFIX)
    elif action == "back_to_bot_select": 
        if not query._answered: await query.answer()
        clear_conversation_data(context); 
        next_state = await client_select_bot_generic(update, context, CALLBACK_TASK_PREFIX, STATE_TASK_SETUP, 'task_select_userbot')
    elif action == "back_to_task_menu": 
        next_state = await task_show_settings_menu(update, context)
    elif action == "set_primary_link": 
        next_state = await task_prompt_set_link(update, context, 'primary')
    elif action == "set_time": 
        next_state = await task_prompt_start_time(update, context)
    elif action == "set_interval": 
        next_state = await task_select_interval(update, context)
    elif action == "set_target_type": 
        next_state = await task_select_target_type(update, context)
    elif action == "select_folder_target": 
        next_state = await task_select_folder_for_target(update, context)
    elif action == "set_target_all": 
        next_state = await task_set_target(update, context, 'all')
    elif action == "set_target_folder": 
        next_state = await task_set_target(update, context, 'folder')
    elif action == "back_to_target_type": 
        next_state = await task_select_target_type(update, context)
    elif action == "toggle_status": 
        next_state = await task_toggle_status(update, context)
    elif action == "save": 
        next_state = await task_save_settings(update, context)
    else: 
        log.warning(f"Unhandled TASK CB: Action='{action}', Data='{data}'"); 
        if not query._answered: await query.answer(get_text(user_id, 'error_invalid_action', lang_override=lang, default_text="Task action not recognized."), show_alert=True)
        next_state = ConversationHandler.END
    return next_state

async def handle_join_callback(update: Update, context: CallbackContext) -> str | int | None:
    query = update.callback_query; user_id, lang = get_user_id_and_lang(update, context); data = query.data
    log.info(f"handle_join_callback: User {user_id}, Data {data}, Lang {lang}")
    next_state: int | str | None = ConversationHandler.END

    if data.startswith(CALLBACK_JOIN_PREFIX + "select_"): 
        next_state = await handle_userbot_selection_callback(update, context, CALLBACK_JOIN_PREFIX)
    else: 
        log.warning(f"Unhandled JOIN CB: Data='{data}'"); 
        if not query._answered: await query.answer(get_text(user_id, 'error_invalid_action', lang_override=lang, default_text="Join action not recognized."), show_alert=True)
        next_state = ConversationHandler.END
    return next_state

async def handle_language_callback(update: Update, context: CallbackContext) -> str | int | None:
     query = update.callback_query; user_id, lang = get_user_id_and_lang(update,context); data = query.data
     log.info(f"handle_language_callback: User {user_id}, Data {data}, Lang {lang}")
     next_state: int | str | None = ConversationHandler.END

     if data.startswith(CALLBACK_LANG_PREFIX): 
         next_state = await set_language_handler(update, context)
     else: 
         log.warning(f"Unhandled LANG CB: Data='{data}'"); 
         if not query._answered: await query.answer(get_text(user_id, 'error_invalid_action', lang_override=lang, default_text="Language action not recognized."), show_alert=True)
         next_state = ConversationHandler.END
     return next_state

async def handle_interval_callback(update: Update, context: CallbackContext) -> str | int | None:
     query = update.callback_query; user_id, lang = get_user_id_and_lang(update,context); data = query.data
     log.info(f"handle_interval_callback: User {user_id}, Data {data}, Lang {lang}")
     next_state: int | str | None = STATE_TASK_SETUP

     if data.startswith(CALLBACK_INTERVAL_PREFIX): 
         next_state = await process_interval_callback(update, context) 
     else: 
         log.warning(f"Unhandled INTERVAL CB: Data='{data}'"); 
         if not query._answered: await query.answer(get_text(user_id, 'error_invalid_action', lang_override=lang, default_text="Interval action not recognized."), show_alert=True)
         next_state = ConversationHandler.END
     return next_state

async def handle_generic_callback(update: Update, context: CallbackContext) -> str | int | None:
    query = update.callback_query
    user_id, lang = get_user_id_and_lang(update, context); data = query.data
    action = data.split(CALLBACK_GENERIC_PREFIX)[1] if CALLBACK_GENERIC_PREFIX in data else "unknown"
    log.info(f"handle_generic_callback: User {user_id}, Data {data}, Action {action}, Lang {lang}")
    next_state: int | str | None = ConversationHandler.END

    if action == "cancel" or action == "confirm_no":
        if not query._answered: await query.answer()
        await send_or_edit_message(update, context, get_text(user_id, 'cancelled', lang_override=lang), reply_markup=None)
        clear_conversation_data(context); 
        next_state = ConversationHandler.END
    elif action == "noop": 
        if not query._answered: await query.answer()
        next_state = None
    else: 
        log.warning(f"Unhandled GENERIC CB: Action='{action}', Data='{data}'"); 
        if not query._answered: await query.answer()
        await send_or_edit_message(update,context,get_text(user_id, 'error_invalid_action', lang_override=lang, default_text="Generic action not recognized.")); 
        next_state = ConversationHandler.END
    return next_state


# --- Conversation Handler Definition ---
# THIS MUST BE AT THE END OF THE FILE, AFTER ALL HANDLER FUNCTIONS ARE DEFINED
main_conversation = ConversationHandler(
    entry_points=[
        CommandHandler('start', start_command),
        CommandHandler('admin', admin_command),
        CommandHandler('cancel', cancel_command)
    ],
    states={
        STATE_WAITING_FOR_CODE: [MessageHandler(Filters.text & ~Filters.command, process_invitation_code)],
        STATE_WAITING_FOR_PHONE: [MessageHandler(Filters.text & ~Filters.command, process_admin_phone)],
        STATE_WAITING_FOR_API_ID: [MessageHandler(Filters.text & ~Filters.command, process_admin_api_id)],
        STATE_WAITING_FOR_API_HASH: [MessageHandler(Filters.text & ~Filters.command, process_admin_api_hash)],
        STATE_WAITING_FOR_CODE_USERBOT: [MessageHandler(Filters.text & ~Filters.command, process_admin_userbot_code)],
        STATE_WAITING_FOR_PASSWORD: [MessageHandler(Filters.text & ~Filters.command, process_admin_userbot_password)],
        STATE_WAITING_FOR_SUB_DETAILS: [MessageHandler(Filters.text & ~Filters.command, process_admin_invite_details)],
        STATE_WAITING_FOR_EXTEND_CODE: [MessageHandler(Filters.text & ~Filters.command, process_admin_extend_code)],
        STATE_WAITING_FOR_EXTEND_DAYS: [MessageHandler(Filters.text & ~Filters.command, process_admin_extend_days)],
        STATE_WAITING_FOR_ADD_USERBOTS_CODE: [MessageHandler(Filters.text & ~Filters.command, process_admin_add_bots_code)],
        STATE_WAITING_FOR_ADD_USERBOTS_COUNT: [MessageHandler(Filters.text & ~Filters.command, process_admin_add_bots_count)],
        STATE_WAITING_FOR_FOLDER_NAME: [MessageHandler(Filters.text & ~Filters.command, process_folder_name)],
        STATE_WAITING_FOR_GROUP_LINKS: [MessageHandler(Filters.text & ~Filters.command, process_folder_links)], # Changed to process_folder_links
        STATE_FOLDER_RENAME_PROMPT: [MessageHandler(Filters.text & ~Filters.command, process_folder_rename)],
        STATE_WAITING_FOR_PRIMARY_MESSAGE_LINK: [MessageHandler(Filters.text & ~Filters.command, lambda u, c: process_task_link(u, c, 'primary'))],
        STATE_WAITING_FOR_FALLBACK_MESSAGE_LINK: [MessageHandler(Filters.text & ~Filters.command, lambda u, c: process_task_link(u, c, 'fallback'))],
        STATE_WAITING_FOR_START_TIME: [MessageHandler(Filters.text & ~Filters.command, process_task_start_time)],
        
        STATE_ADMIN_TASK_MESSAGE: [MessageHandler(Filters.text & ~Filters.command, admin_process_task_message)],
        STATE_ADMIN_TASK_SCHEDULE: [MessageHandler(Filters.text & ~Filters.command, admin_process_task_schedule)],
        STATE_ADMIN_TASK_TARGET: [MessageHandler(Filters.text & ~Filters.command, admin_process_task_target)],

        STATE_TASK_SETUP: [CallbackQueryHandler(main_callback_handler)],
        STATE_WAITING_FOR_FOLDER_SELECTION: [CallbackQueryHandler(main_callback_handler)],
        STATE_WAITING_FOR_USERBOT_SELECTION: [CallbackQueryHandler(main_callback_handler)],
        STATE_WAITING_FOR_FOLDER_ACTION: [CallbackQueryHandler(main_callback_handler)],
        STATE_FOLDER_EDIT_REMOVE_SELECT: [CallbackQueryHandler(main_callback_handler)],
        STATE_ADMIN_CONFIRM_USERBOT_RESET: [CallbackQueryHandler(main_callback_handler)],
        STATE_ADMIN_TASK_CONFIRM: [CallbackQueryHandler(main_callback_handler)], 
        STATE_WAITING_FOR_LANGUAGE: [CallbackQueryHandler(main_callback_handler)],
    },
    fallbacks=[
        CommandHandler('cancel', cancel_command),
        CallbackQueryHandler(main_callback_handler), 
        MessageHandler(Filters.all, conversation_fallback)
    ],
    name="main_conversation",
    persistent=False,
    allow_reentry=True
)

log.info("Handlers module loaded and structure updated (async command handlers, ConversationHandler at end).")
