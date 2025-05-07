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
    STATE_WAITING_FOR_TASK_BOT, # This state isn't used if selection is only via CB
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
from utils import get_user_id_and_lang, send_or_edit_message, clear_conversation_data # Import helpers

# Import admin handlers for use as state handlers (ensure they are async)
from admin_handlers import (
    admin_process_task_message, # Assume these are async now in admin_handlers.py
    admin_process_task_schedule,
    admin_process_task_target
)

# --- Helper Functions (Specific to handlers, kept here) ---

def simple_async_test(update: Update, context: CallbackContext, message: str):
    # This seems like a test function, keeping it here.
    log.info(f"simple_async_test: ENTERED! Message: '{message}', ChatID: {update.effective_chat.id if update and update.effective_chat else 'N/A'}")
    try:
        if update and update.effective_chat:
            # Use the proper async send_or_edit_message from utils
            # For a simple test, maybe just use context.bot.send_message directly
            context.bot.send_message(chat_id=update.effective_chat.id, text=f"Async test successful: {message}")
            log.info(f"simple_async_test: Message sent via context.bot.send_message!")
        else:
            log.error("simple_async_test: Update or effective_chat is None.")
    except Exception as e:
        log.error(f"simple_async_test: EXCEPTION - {e}", exc_info=True)
    log.info("simple_async_test: EXITED!")

# --- Synchronous Error Handler (for testing) ---
def sync_error_handler(update: object, context: CallbackContext) -> None:
    """Log the error (synchronous version for testing)."""
    log.error(msg="[sync_error_handler] Exception while handling an update:", exc_info=context.error)
    if isinstance(update, Update) and update.effective_chat:
        try:
            # context.bot.send_message(update.effective_chat.id, "An error occurred.")
            pass # Focus on logging
        except Exception as e:
            log.error(f"[sync_error_handler] Failed to send sync error message: {e}")

# --- Async Error Handler ---
async def async_error_handler(update: object, context: CallbackContext) -> None:
    """Log the error and send a telegram message to notify the developer."""
    log.error(msg="[async_error_handler] Exception while handling an update:", exc_info=context.error)

    user_id = None
    chat_id = None
    try:
        if isinstance(update, Update):
            if update.effective_user: user_id = update.effective_user.id
            if update.effective_chat: chat_id = update.effective_chat.id

        if not chat_id and user_id: chat_id = user_id

        if chat_id:
            log.info(f"[async_error_handler] Attempting to send generic error to user {user_id}, chat {chat_id}")
            current_lang = 'en'
            if context and hasattr(context, 'user_data') and isinstance(context.user_data, dict):
                current_lang = context.user_data.get(CTX_LANG, 'en')

            error_message = get_text(user_id, 'error_generic', lang_override=current_lang)
            # Use the imported send_or_edit_message (or a direct send)
            await context.bot.send_message(chat_id=chat_id, text=error_message) # Direct send is simpler here
    except Exception as e:
        log.error(f"[async_error_handler] Failed to send async error message: {e}", exc_info=True)

# --- Formatting and Menu Builders (Synchronous) ---
def format_dt(timestamp: int | None, tz=LITHUANIA_TZ, fmt='%Y-%m-%d %H:%M') -> str:
    if not timestamp: return get_text(0, 'task_value_not_set', lang_override='en')
    try:
        dt_utc = datetime.fromtimestamp(timestamp, UTC_TZ)
        dt_local = dt_utc.astimezone(tz)
        return dt_local.strftime(fmt)
    except (ValueError, TypeError, OSError) as e:
        log.warning(f"Could not format invalid timestamp: {timestamp}. Error: {e}")
        return "Invalid Date"

def build_client_menu(user_id, context: CallbackContext): # Sync
    lang = context.user_data.get(CTX_LANG, 'en') # Use context lang first

    client_info = db.find_client_by_user_id(user_id)
    if not client_info:
        return get_text(user_id, 'unknown_user', lang_override=lang), None, ParseMode.HTML

    code = client_info['invitation_code']
    sub_end_ts = client_info['subscription_end']
    now_ts = int(datetime.now(UTC_TZ).timestamp())
    is_expired = sub_end_ts < now_ts
    end_date = format_dt(sub_end_ts, fmt='%Y-%m-%d') if sub_end_ts else 'N/A'
    expiry_warning = " ⚠️ <b>Expired</b>" if is_expired else ""

    userbot_phones = db.get_client_bots(user_id)
    bot_count = len(userbot_phones)
    parse_mode = ParseMode.HTML

    menu_text = f"<b>{get_text(user_id, 'client_menu_title', lang_override=lang, code=html.escape(code))}</b>{expiry_warning}\n"
    menu_text += get_text(user_id, 'client_menu_sub_end', lang_override=lang, end_date=end_date) + "\n\n"
    menu_text += f"<u>{get_text(user_id, 'client_menu_userbots_title', lang_override=lang, count=bot_count)}</u>\n"

    if userbot_phones:
        for i, phone in enumerate(userbot_phones, 1):
            bot_db_info = db.find_userbot(phone)
            username = bot_db_info['username'] if bot_db_info else None
            status = bot_db_info['status'].capitalize() if bot_db_info else 'Unknown'
            last_error = bot_db_info['last_error'] if bot_db_info else None
            display_name = html.escape(f"@{username}" if username else phone)

            status_icon = "⚪️"
            if bot_db_info:
                if bot_db_info['status'] == 'active': status_icon = "🟢"
                elif bot_db_info['status'] == 'error': status_icon = "🔴"
                elif bot_db_info['status'] in ['connecting', 'authenticating', 'initializing']: status_icon = "⏳"
                elif bot_db_info['status'] in ['needs_code', 'needs_password']: status_icon = "⚠️"

            menu_text += get_text(user_id, 'client_menu_userbot_line', lang_override=lang, index=i, status_icon=status_icon, display_name=display_name, status=html.escape(status)) + "\n"
            if last_error:
                escaped_error = html.escape(last_error)
                error_line = get_text(user_id, 'client_menu_userbot_error', lang_override=lang, error=f"{escaped_error[:100]}{'...' if len(escaped_error)>100 else ''}")
                menu_text += f"  {error_line}\n"
    else:
        menu_text += get_text(user_id, 'client_menu_no_userbots', lang_override=lang) + "\n"

    keyboard = [
        [InlineKeyboardButton(get_text(user_id, 'client_menu_button_setup_tasks', lang_override=lang), callback_data=f"{CALLBACK_CLIENT_PREFIX}select_bot_task")],
        [InlineKeyboardButton(get_text(user_id, 'client_menu_button_manage_folders', lang_override=lang), callback_data=f"{CALLBACK_CLIENT_PREFIX}manage_folders")],
        [InlineKeyboardButton(get_text(user_id, 'client_menu_button_join_groups', lang_override=lang), callback_data=f"{CALLBACK_CLIENT_PREFIX}select_bot_join")],
        [InlineKeyboardButton(get_text(user_id, 'client_menu_button_stats', lang_override=lang), callback_data=f"{CALLBACK_CLIENT_PREFIX}view_stats")],
        [InlineKeyboardButton(get_text(user_id, 'client_menu_button_language', lang_override=lang), callback_data=f"{CALLBACK_CLIENT_PREFIX}language")],
    ]
    markup = InlineKeyboardMarkup(keyboard)
    return menu_text, markup, parse_mode

def build_admin_menu(user_id, context: CallbackContext): # Sync
    lang = context.user_data.get(CTX_LANG, 'en')

    title = f"<b>{get_text(user_id, 'admin_panel_title', lang_override=lang)}</b>"
    parse_mode = ParseMode.HTML
    keyboard = [
        [
            InlineKeyboardButton(get_text(user_id, 'admin_button_add_userbot', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}add_bot_prompt"),
            InlineKeyboardButton(get_text(user_id, 'admin_button_remove_userbot', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}remove_bot_select?page=0")
        ],
        [InlineKeyboardButton(get_text(user_id, 'admin_button_list_userbots', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}list_bots?page=0")],
        [
            InlineKeyboardButton(get_text(user_id, 'admin_button_manage_tasks', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}manage_tasks"),
            InlineKeyboardButton(get_text(user_id, 'admin_button_view_tasks', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}view_tasks?page=0")
        ],
        [InlineKeyboardButton(get_text(user_id, 'admin_button_gen_invite', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}gen_invite_prompt")],
        [InlineKeyboardButton(get_text(user_id, 'admin_button_view_subs', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}view_subs?page=0")],
        [
            InlineKeyboardButton(get_text(user_id, 'admin_button_extend_sub', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}extend_sub_prompt"),
            InlineKeyboardButton(get_text(user_id, 'admin_button_assign_bots_client', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}assign_bots_prompt")
        ],
        [InlineKeyboardButton(get_text(user_id, 'admin_button_view_logs', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}view_logs?page=0")], # Note: view_logs CB needs pagination handling
    ]
    markup = InlineKeyboardMarkup(keyboard)
    return title, markup, parse_mode

def build_pagination_buttons(base_callback_data: str, current_page: int, total_items: int, items_per_page: int, lang: str = 'en') -> list: # Sync
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
    """Internal: Asynchronously shows a menu using the provided menu builder function."""
    user_id, lang = get_user_id_and_lang(update, context)
    title, markup, parse_mode = menu_builder_func(user_id, context) # menu_builder_func must be synchronous
    await send_or_edit_message(update, context, title, reply_markup=markup, parse_mode=parse_mode)

# --- Command Handlers ---
async def start_command(update: Update, context: CallbackContext) -> int:
    """Handle /start command."""
    try:
        user_id, lang = get_user_id_and_lang(update, context)
        log.info(f"Start command received from user {user_id}")

        clear_conversation_data(context)
        context.user_data[CTX_USER_ID] = user_id
        context.user_data[CTX_LANG] = lang

        if is_admin(user_id):
            await _show_menu_async(update, context, build_admin_menu)
            return ConversationHandler.END # End conversation if admin menu shown

        client = db.find_client_by_user_id(user_id)
        if client:
            await _show_menu_async(update, context, build_client_menu)
            return ConversationHandler.END # End conversation if client menu shown

        # If not admin and not an existing client, ask for code
        await send_or_edit_message(
            update, context,
            get_text(user_id, 'ask_invitation_code', lang_override=lang),
            parse_mode=ParseMode.HTML
        )
        return STATE_WAITING_FOR_CODE # Transition to wait for code

    except Exception as e:
        log.error(f"Error in start_command: {e}", exc_info=True)
        user_id_err, lang_err = get_user_id_and_lang(update, context)
        try:
            await send_or_edit_message(
                update, context,
                get_text(user_id_err, 'error_generic', lang_override=lang_err),
                parse_mode=ParseMode.HTML
            )
        except Exception as send_err:
            log.error(f"Failed to send error message in start_command handler: {send_err}")
        return ConversationHandler.END

async def admin_command(update: Update, context: CallbackContext) -> int:
    """Handle /admin command."""
    try:
        user_id, lang = get_user_id_and_lang(update, context)
        log.info(f"Admin command received from user {user_id}")

        clear_conversation_data(context)
        context.user_data[CTX_USER_ID] = user_id
        context.user_data[CTX_LANG] = lang

        if not is_admin(user_id):
            log.warning(f"Unauthorized admin access attempt from user {user_id}")
            await send_or_edit_message(
                update, context,
                get_text(user_id, 'not_admin', lang_override=lang),
                parse_mode=ParseMode.HTML
            )
            return ConversationHandler.END # End if not admin

        await _show_menu_async(update, context, build_admin_menu)
        return ConversationHandler.END # End conversation after showing admin menu

    except Exception as e:
        log.error(f"Error in admin_command: {e}", exc_info=True)
        user_id_err, lang_err = get_user_id_and_lang(update, context)
        try:
             await send_or_edit_message(
                 update, context,
                 get_text(user_id_err, 'error_generic', lang_override=lang_err),
                 parse_mode=ParseMode.HTML
             )
        except Exception as send_err:
             log.error(f"Failed to send error message in admin_command handler: {send_err}")
        return ConversationHandler.END

async def cancel_command(update: Update, context: CallbackContext) -> int:
    """Cancel command handler."""
    try:
        user_id, lang = get_user_id_and_lang(update, context)

        await send_or_edit_message(
            update, context,
            get_text(user_id, 'cancelled', lang_override=lang),
            parse_mode=ParseMode.HTML,
            reply_markup=None # Remove buttons on cancel
        )
        clear_conversation_data(context) # Clear data *after* sending message

        # Determine where to go after cancel
        if is_admin(user_id):
             return await admin_command(update, context) # Go back to admin menu
        client = db.find_client_by_user_id(user_id)
        if client:
             return await client_menu(update, context) # Go back to client menu

        return ConversationHandler.END # Default end

    except Exception as e:
        log.error(f"Error in cancel_command: {e}", exc_info=True)
        await async_error_handler(update, context)
        return ConversationHandler.END

# --- Conversation State Handlers ---

async def process_invitation_code(update: Update, context: CallbackContext) -> str | int:
    user_id, lang = get_user_id_and_lang(update, context)
    code_input = update.message.text.strip()
    log.info(f"Processing invitation code '{code_input}' for user {user_id}")

    client_info_db = db.find_client_by_user_id(user_id)
    if client_info_db:
        now_ts = int(datetime.now(UTC_TZ).timestamp())
        if client_info_db['subscription_end'] > now_ts:
            await send_or_edit_message(update, context, get_text(user_id, 'user_already_active', lang_override=lang))
            return await client_menu(update, context) # Already active, show menu

    success, reason_or_client_data = db.activate_client(code_input, user_id)

    if success:
        if reason_or_client_data == "activation_success":
            client_data = db.find_client_by_code(code_input)
            if client_data:
                await send_or_edit_message(update, context, get_text(user_id, 'activation_success', lang_override=lang))
                return await client_menu(update, context)
            else:
                await send_or_edit_message(update, context, get_text(user_id, 'activation_error', lang_override=lang))
                return STATE_WAITING_FOR_CODE
        elif reason_or_client_data == "already_active":
             await send_or_edit_message(update, context, get_text(user_id, 'already_active', lang_override=lang))
             return await client_menu(update, context)
        else:
             log.error(f"activate_client returned True but unexpected reason: {reason_or_client_data}")
             await send_or_edit_message(update, context, get_text(user_id, 'activation_error', lang_override=lang))
             return STATE_WAITING_FOR_CODE
    else:
        error_key = reason_or_client_data
        translation_map = {
            "user_already_active": "user_already_active", "code_not_found": "code_not_found",
            "code_already_used": "code_already_used", "subscription_expired": "subscription_expired",
            "activation_error": "activation_error", "activation_db_error": "activation_db_error",
        }
        message_key = translation_map.get(error_key, 'activation_error')
        await send_or_edit_message(update, context, get_text(user_id, message_key, lang_override=lang))
        return STATE_WAITING_FOR_CODE

async def process_admin_phone(update: Update, context: CallbackContext) -> str | int:
    user_id, lang = get_user_id_and_lang(update, context)
    phone = update.message.text.strip()
    log.info(f"process_admin_phone: Processing phone {phone} for user {user_id}")

    if not re.match(r"^\+[1-9]\d{1,14}$", phone):
        await send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_invalid_phone', lang_override=lang))
        return STATE_WAITING_FOR_PHONE

    context.user_data[CTX_PHONE] = phone
    await send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_prompt_api_id', lang_override=lang))
    return STATE_WAITING_FOR_API_ID

async def process_admin_api_id(update: Update, context: CallbackContext) -> str | int:
    user_id, lang = get_user_id_and_lang(update, context); api_id_str = update.message.text.strip()
    try:
        api_id = int(api_id_str)
        if api_id <= 0: raise ValueError("API ID must be positive")
        context.user_data[CTX_API_ID] = api_id
        log.info(f"Admin {user_id} API ID OK for {context.user_data.get(CTX_PHONE)}")
    except (ValueError, TypeError):
        await send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_invalid_api_id', lang_override=lang))
        return STATE_WAITING_FOR_API_ID
    await send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_prompt_api_hash', lang_override=lang))
    return STATE_WAITING_FOR_API_HASH

async def process_admin_api_hash(update: Update, context: CallbackContext) -> str | int:
    user_id, lang = get_user_id_and_lang(update, context); api_hash = update.message.text.strip()
    if not api_hash or len(api_hash) < 30 or not re.match('^[a-fA-F0-9]+$', api_hash):
        await send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_invalid_api_hash', lang_override=lang))
        return STATE_WAITING_FOR_API_HASH

    context.user_data[CTX_API_HASH] = api_hash
    phone = context.user_data.get(CTX_PHONE)
    api_id = context.user_data.get(CTX_API_ID)

    if not phone or not api_id:
        await send_or_edit_message(update, context, get_text(user_id, 'session_expired', lang_override=lang))
        clear_conversation_data(context); return ConversationHandler.END

    log.info(f"Admin {user_id} API Hash OK for {phone}. Starting authentication flow.")
    await send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_auth_connecting', lang_override=lang, phone=html.escape(phone)))

    try:
        auth_status, auth_data = await telethon_api.start_authentication_flow(phone, api_id, api_hash)
        log.info(f"Authentication start result for {phone}: Status='{auth_status}'")

        if auth_status == 'code_needed':
            context.user_data[CTX_AUTH_DATA] = auth_data
            await send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_prompt_code', lang_override=lang, phone=html.escape(phone)))
            return STATE_WAITING_FOR_CODE_USERBOT
        elif auth_status == 'password_needed':
            context.user_data[CTX_AUTH_DATA] = auth_data
            await send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_prompt_password', lang_override=lang, phone=html.escape(phone)))
            return STATE_WAITING_FOR_PASSWORD
        elif auth_status == 'already_authorized':
             log.warning(f"Userbot {phone} is already authorized (Telethon check).")
             if not db.find_userbot(phone):
                 safe_phone_part = re.sub(r'[^\d]', '', phone)
                 session_file_rel = f"{safe_phone_part or f'unknown_{random.randint(1000,9999)}'}.session"
                 db.add_userbot(phone, session_file_rel, api_id, api_hash, 'active')
             else:
                 db.update_userbot_status(phone, 'active')
             context.dispatcher.run_async(telethon_api.get_userbot_runtime_info, phone)
             await send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_already_auth', lang_override=lang, display_name=html.escape(phone)))
             clear_conversation_data(context); return ConversationHandler.END
        else: # Error case
            error_msg = auth_data.get('error_message', 'Unknown error during auth start')
            log.error(f"Auth start error for {phone}: {error_msg}")
            locals_for_format = {'phone': html.escape(phone), 'error': html.escape(error_msg)}
            key = 'admin_userbot_auth_error_unknown'
            if "flood wait" in error_msg.lower():
                key = 'admin_userbot_auth_error_flood'
                seconds_match = re.search(r'\d+', error_msg)
                locals_for_format['seconds'] = seconds_match.group(0) if seconds_match else '?'
            elif "config" in error_msg.lower() or "invalid api" in error_msg.lower(): key = 'admin_userbot_auth_error_config'
            elif "invalid phone" in error_msg.lower(): key = 'admin_userbot_auth_error_phone_invalid'
            elif "connection" in error_msg.lower() or "timeout" in error_msg.lower(): key = 'admin_userbot_auth_error_connect'
            await send_or_edit_message(update, context, get_text(user_id, key, lang_override=lang, **locals_for_format))
            clear_conversation_data(context); return ConversationHandler.END
    except Exception as e:
        log.error(f"Exception during start_authentication_flow for {phone}: {e}", exc_info=True)
        await send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_auth_error_unknown', lang_override=lang, phone=html.escape(phone), error=html.escape(str(e))))
        clear_conversation_data(context); return ConversationHandler.END

async def process_admin_userbot_code(update: Update, context: CallbackContext) -> str | int:
    user_id, lang = get_user_id_and_lang(update, context)
    auth_data = context.user_data.get(CTX_AUTH_DATA)
    phone_num = context.user_data.get(CTX_PHONE)

    if not auth_data or not phone_num:
        log.error(f"process_admin_userbot_code: Missing auth_data or phone_num for user {user_id}")
        await send_or_edit_message(update, context, get_text(user_id, 'session_expired', lang_override=lang))
        clear_conversation_data(context)
        return ConversationHandler.END

    code = update.message.text.strip()
    log.info(f"process_admin_userbot_code: Processing code for phone {phone_num}")

    try:
        status, result_data = await telethon_api.complete_authentication_flow(auth_data, code=code)

        if status == 'success':
            final_phone = result_data.get('phone', phone_num)
            username = result_data.get('username')
            display_name = f"@{username}" if username else final_phone
            log.info(f"Code accepted for {final_phone}. Authentication successful.")
            context.dispatcher.run_async(telethon_api.get_userbot_runtime_info, final_phone)
            await send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_auth_success', lang_override=lang, display_name=html.escape(display_name)))
            clear_conversation_data(context)
            return ConversationHandler.END
        elif status == 'password_needed':
            log.warning(f"Password unexpectedly needed after code submission for {phone_num}.")
            context.user_data[CTX_AUTH_DATA] = result_data
            await send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_prompt_password', lang_override=lang, phone=html.escape(phone_num)))
            return STATE_WAITING_FOR_PASSWORD
        else: # Error
            error_msg = result_data.get('error_message', "Unknown error during code submission.")
            log.warning(f"Code submission failed for {phone_num}: {error_msg}")
            error_key = 'admin_userbot_auth_error_code_invalid'
            if "flood wait" in error_msg.lower(): error_key = 'admin_userbot_auth_error_flood'
            await send_or_edit_message(update, context, get_text(user_id, error_key, lang_override=lang, phone=html.escape(phone_num), error=html.escape(error_msg), seconds=re.search(r'\d+', error_msg).group(0) if "flood" in error_key else 'N/A'))
            if error_key != 'admin_userbot_auth_error_code_invalid':
                clear_conversation_data(context)
                return ConversationHandler.END
            return STATE_WAITING_FOR_CODE_USERBOT
    except Exception as e:
        log.error(f"process_admin_userbot_code: Exception submitting code for {phone_num}: {e}", exc_info=True)
        await send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_auth_error_unknown', lang_override=lang, phone=html.escape(phone_num), error=html.escape(str(e))))
        clear_conversation_data(context)
        return ConversationHandler.END

async def process_admin_userbot_password(update: Update, context: CallbackContext) -> str | int:
    user_id, lang = get_user_id_and_lang(update, context)
    auth_data = context.user_data.get(CTX_AUTH_DATA)
    phone_num = context.user_data.get(CTX_PHONE)

    if not auth_data or not phone_num:
        log.error(f"process_admin_userbot_password: Missing auth_data or phone_num for user {user_id}")
        await send_or_edit_message(update, context, get_text(user_id, 'session_expired', lang_override=lang))
        clear_conversation_data(context)
        return ConversationHandler.END

    password = update.message.text.strip()
    log.info(f"process_admin_userbot_password: Processing 2FA password for phone {phone_num}")

    try:
        status, result_data = await telethon_api.complete_authentication_flow(auth_data, password=password)

        if status == 'success':
            final_phone = result_data.get('phone', phone_num)
            username = result_data.get('username')
            display_name = f"@{username}" if username else final_phone
            log.info(f"Password accepted for {final_phone}. Authentication successful.")
            context.dispatcher.run_async(telethon_api.get_userbot_runtime_info, final_phone)
            await send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_auth_success', lang_override=lang, display_name=html.escape(display_name)))
            clear_conversation_data(context)
            return ConversationHandler.END
        else: # Error
            error_msg = result_data.get('error_message', "Unknown error during password submission.")
            log.warning(f"Password submission failed for {phone_num}: {error_msg}")
            error_key = 'admin_userbot_auth_error_password_invalid'
            if "flood wait" in error_msg.lower(): error_key = 'admin_userbot_auth_error_flood'
            await send_or_edit_message(update, context, get_text(user_id, error_key, lang_override=lang, phone=html.escape(phone_num), error=html.escape(error_msg), seconds=re.search(r'\d+', error_msg).group(0) if "flood" in error_key else 'N/A'))
            if error_key != 'admin_userbot_auth_error_password_invalid':
                clear_conversation_data(context)
                return ConversationHandler.END
            return STATE_WAITING_FOR_PASSWORD
    except Exception as e:
        log.error(f"process_admin_userbot_password: Exception submitting password for {phone_num}: {e}", exc_info=True)
        await send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_auth_error_unknown', lang_override=lang, phone=html.escape(phone_num), error=html.escape(str(e))))
        clear_conversation_data(context)
        return ConversationHandler.END

async def process_admin_invite_details(update: Update, context: CallbackContext) -> int:
    user_id, lang = get_user_id_and_lang(update, context)
    try:
        days_str = update.message.text.strip()
        days = int(days_str)
        if days <= 0:
            await send_or_edit_message(update, context, get_text(user_id, 'admin_invite_invalid_days', lang_override=lang))
            return STATE_WAITING_FOR_SUB_DETAILS

        invite_code = db.generate_invite_code()
        if invite_code:
            end_datetime = datetime.now(UTC_TZ) + timedelta(days=days)
            sub_end_ts = int(end_datetime.timestamp())
            if db.create_invitation(invite_code, sub_end_ts):
                db.log_event_db("Invite Code Generated", f"Code: {invite_code}, Days: {days}", user_id=user_id)
                await send_or_edit_message(
                    update, context,
                    get_text(user_id, 'admin_invite_generated', lang_override=lang, code=invite_code, days=days)
                )
            else:
                db.log_event_db("Invite Code Store Failed", f"Code: {invite_code}, Days: {days}", user_id=user_id)
                await send_or_edit_message(update, context, get_text(user_id, 'admin_invite_db_error', lang_override=lang))
        else:
            await send_or_edit_message(update, context, get_text(user_id, 'admin_invite_db_error', lang_override=lang))
    except ValueError:
        await send_or_edit_message(update, context, get_text(user_id, 'admin_invite_invalid_days', lang_override=lang))
        return STATE_WAITING_FOR_SUB_DETAILS
    except Exception as e:
        log.error(f"Error processing admin invite details: {e}", exc_info=True)
        await send_or_edit_message(update, context, get_text(user_id, 'error_generic', lang_override=lang))

    clear_conversation_data(context)
    return ConversationHandler.END

async def process_admin_extend_code(update: Update, context: CallbackContext) -> int:
    user_id, lang = get_user_id_and_lang(update, context); code = update.message.text.strip()
    client = db.find_client_by_code(code)
    if not client:
        await send_or_edit_message(update, context, get_text(user_id, 'admin_extend_invalid_code', lang_override=lang))
        return STATE_WAITING_FOR_EXTEND_CODE

    context.user_data[CTX_EXTEND_CODE] = code
    end_date_str = format_dt(client['subscription_end'])
    await send_or_edit_message(update, context, get_text(user_id, 'admin_extend_prompt_days', lang_override=lang, code=html.escape(code), end_date=end_date_str))
    return STATE_WAITING_FOR_EXTEND_DAYS

async def process_admin_extend_days(update: Update, context: CallbackContext) -> int:
    user_id, lang = get_user_id_and_lang(update, context); days_str = update.message.text.strip(); code = context.user_data.get(CTX_EXTEND_CODE)
    if not code:
        await send_or_edit_message(update, context, get_text(user_id, 'session_expired', lang_override=lang))
        clear_conversation_data(context); return ConversationHandler.END
    try:
        days_to_add = int(days_str)
        if days_to_add <= 0: raise ValueError("Days must be positive")
    except (ValueError, AssertionError):
        await send_or_edit_message(update, context, get_text(user_id, 'admin_extend_invalid_days', lang_override=lang))
        return STATE_WAITING_FOR_EXTEND_DAYS

    client = db.find_client_by_code(code)
    if not client:
        await send_or_edit_message(update, context, get_text(user_id, 'admin_extend_invalid_code', lang_override=lang))
        clear_conversation_data(context); return ConversationHandler.END

    current_end_ts = client['subscription_end']; now_ts = int(datetime.now(UTC_TZ).timestamp())
    start_ts = max(now_ts, current_end_ts)
    start_dt = datetime.fromtimestamp(start_ts, UTC_TZ); new_end_dt = start_dt + timedelta(days=days_to_add); new_end_ts = int(new_end_dt.timestamp())

    if db.extend_subscription(code, new_end_ts):
        new_end_date_str = format_dt(new_end_ts)
        client_user_id_for_log = client.get('user_id')
        db.log_event_db("Subscription Extended", f"Code: {code}, Added: {days_to_add} days, New End: {new_end_date_str}", user_id=user_id, details=f"Client UserID: {client_user_id_for_log}")
        await send_or_edit_message(update, context, get_text(user_id, 'admin_extend_success', lang_override=lang, code=html.escape(code), days=days_to_add, new_end_date=new_end_date_str))
    else:
        db.log_event_db("Sub Extend Failed", f"Code: {code}", user_id=user_id)
        await send_or_edit_message(update, context, get_text(user_id, 'admin_extend_db_error', lang_override=lang))

    clear_conversation_data(context); return ConversationHandler.END

async def process_admin_add_bots_code(update: Update, context: CallbackContext) -> int:
    user_id, lang = get_user_id_and_lang(update, context); code = update.message.text.strip();
    client = db.find_client_by_code(code)
    if not client:
        await send_or_edit_message(update, context, get_text(user_id, 'admin_assignbots_invalid_code', lang_override=lang))
        return STATE_WAITING_FOR_ADD_USERBOTS_CODE

    context.user_data[CTX_ADD_BOTS_CODE] = code
    current_bots_for_client = db.get_all_userbots(assigned_status=True)
    current_count = sum(1 for b in current_bots_for_client if b['assigned_client'] == code) if current_bots_for_client else 0

    await send_or_edit_message(update, context, get_text(user_id, 'admin_assignbots_prompt_count', lang_override=lang, code=html.escape(code), current_count=current_count))
    return STATE_WAITING_FOR_ADD_USERBOTS_COUNT

async def process_admin_add_bots_count(update: Update, context: CallbackContext) -> int:
    user_id, lang = get_user_id_and_lang(update, context); count_str = update.message.text.strip(); code = context.user_data.get(CTX_ADD_BOTS_CODE)
    if not code:
        await send_or_edit_message(update, context, get_text(user_id, 'session_expired', lang_override=lang))
        clear_conversation_data(context); return ConversationHandler.END
    try:
        count_to_add = int(count_str)
        if count_to_add <= 0: raise ValueError("Count must be positive")
    except (ValueError, AssertionError):
        await send_or_edit_message(update, context, get_text(user_id, 'admin_assignbots_invalid_count', lang_override=lang))
        return STATE_WAITING_FOR_ADD_USERBOTS_COUNT

    available_bots_phones = db.get_unassigned_userbots(limit=count_to_add)

    if len(available_bots_phones) < count_to_add:
        await send_or_edit_message(update, context, get_text(user_id, 'admin_assignbots_no_bots_available', lang_override=lang, needed=count_to_add, available=len(available_bots_phones)))
        clear_conversation_data(context); return ConversationHandler.END

    bots_to_actually_assign = available_bots_phones[:count_to_add]
    success, message_from_db = db.assign_userbots_to_client(code, bots_to_actually_assign)
    client_user_id_for_log = db.find_client_by_code(code)['user_id'] if db.find_client_by_code(code) else None

    if success:
        assigned_count_match = re.search(r"Successfully assigned (\d+)", message_from_db)
        actually_assigned_in_db = int(assigned_count_match.group(1)) if assigned_count_match else 0
        final_message_key = 'admin_assignbots_success'
        format_params = {'count': actually_assigned_in_db, 'code': html.escape(code)}
        if actually_assigned_in_db != len(bots_to_actually_assign) or "Failed:" in message_from_db :
            final_message_key = 'admin_assignbots_partial_success'
            format_params = {'assigned_count': actually_assigned_in_db, 'requested_count': len(bots_to_actually_assign), 'code': html.escape(code)}
        response_text = get_text(user_id, final_message_key, lang_override=lang, **format_params)
        if "Failed:" in message_from_db: response_text += f"\nDetails: {html.escape(message_from_db)}"
        await send_or_edit_message(update, context, response_text)
        db.log_event_db("Userbots Assigned", f"Code: {code}, Req: {count_to_add}, Assigned: {bots_to_actually_assign}, DB_Msg: {message_from_db}", user_id=user_id, details=f"ClientUID: {client_user_id_for_log}")
        for phone in bots_to_actually_assign: context.dispatcher.run_async(telethon_api.get_userbot_runtime_info, phone)
    else:
        db.log_event_db("Bot Assign Failed Overall", f"Code: {code}, Reason: {message_from_db}", user_id=user_id, details=f"ClientUID: {client_user_id_for_log}")
        fail_message = get_text(user_id, 'admin_assignbots_failed', lang_override=lang, code=html.escape(code)) + f"\nError: {html.escape(message_from_db)}"
        await send_or_edit_message(update, context, fail_message)

    clear_conversation_data(context); return ConversationHandler.END

# --- Folder Management Handlers ---
async def client_folder_menu(update: Update, context: CallbackContext) -> int:
    user_id, lang = get_user_id_and_lang(update, context)
    log.info(f"client_folder_menu: UserID={user_id}, Lang={lang}")
    await _show_menu_async(update, context, build_folder_menu)
    return ConversationHandler.END

async def process_folder_name(update: Update, context: CallbackContext) -> int:
    user_id, lang = get_user_id_and_lang(update, context); folder_name = update.message.text.strip()
    if not folder_name:
        await send_or_edit_message(update, context, get_text(user_id, 'error_invalid_input', lang_override=lang))
        return STATE_WAITING_FOR_FOLDER_NAME

    log.info(f"User {user_id} attempting to create folder: {folder_name}")
    folder_id_or_status = db.add_folder(folder_name, user_id)

    if isinstance(folder_id_or_status, int) and folder_id_or_status > 0:
        folder_id = folder_id_or_status
        db.log_event_db("Folder Created", f"Name: {folder_name}, ID: {folder_id}", user_id=user_id)
        await send_or_edit_message(update, context, get_text(user_id, 'folder_create_success', lang_override=lang, name=html.escape(folder_name)))
        return await client_folder_menu(update, context)
    elif folder_id_or_status is None:
        await send_or_edit_message(update, context, get_text(user_id, 'folder_create_error_exists', lang_override=lang, name=html.escape(folder_name)))
        return STATE_WAITING_FOR_FOLDER_NAME
    else:
        db.log_event_db("Folder Create Failed", f"Name: {folder_name}, Reason: DB Error", user_id=user_id)
        await send_or_edit_message(update, context, get_text(user_id, 'folder_create_error_db', lang_override=lang))
        clear_conversation_data(context); return ConversationHandler.END

async def client_select_folder_to_edit_or_delete(update: Update, context: CallbackContext, action: str) -> int:
    query = update.callback_query; await query.answer()
    user_id, lang = get_user_id_and_lang(update, context);
    try:
        current_page = 0
        if query and query.data and '?page=' in query.data:
            current_page = int(query.data.split('?page=')[1])
    except (ValueError, IndexError, AttributeError): current_page = 0

    folders = db.get_folders_by_user(user_id)
    if not folders:
        await send_or_edit_message(update, context, get_text(user_id, 'folder_no_folders', lang_override=lang), reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(get_text(user_id,'button_back',lang_override=lang), callback_data=f"{CALLBACK_FOLDER_PREFIX}back_to_manage")]]))
        return ConversationHandler.END

    total_items = len(folders); start_index = current_page * ITEMS_PER_PAGE; end_index = start_index + ITEMS_PER_PAGE; folders_page = folders[start_index:end_index]
    text_key = 'folder_select_edit' if action == 'edit' else 'folder_select_delete'; text = get_text(user_id, text_key, lang_override=lang); keyboard = []
    for folder in folders_page:
        button_text = html.escape(folder['name']);
        callback_action_prefix = "edit_selected" if action == 'edit' else "delete_selected_prompt"
        callback_data = f"{CALLBACK_FOLDER_PREFIX}{callback_action_prefix}?id={folder['id']}";
        keyboard.append([InlineKeyboardButton(button_text, callback_data=callback_data)])

    base_callback = f"{CALLBACK_FOLDER_PREFIX}select_{action}"; pagination_buttons = build_pagination_buttons(base_callback, current_page, total_items, ITEMS_PER_PAGE, lang=lang); keyboard.extend(pagination_buttons)
    keyboard.append([InlineKeyboardButton(get_text(user_id, 'button_back', lang_override=lang), callback_data=f"{CALLBACK_FOLDER_PREFIX}back_to_manage")]); markup = InlineKeyboardMarkup(keyboard)

    await send_or_edit_message(update, context, text, reply_markup=markup)
    return STATE_WAITING_FOR_FOLDER_SELECTION

# Other handlers (client_show_folder_edit_options, process_folder_links, etc.) should use `await send_or_edit_message` now.
# ... (rest of the handlers remain largely the same, just ensure await send_or_edit_message is used) ...

# --- Callback Query Routers ---

async def handle_client_callback(update: Update, context: CallbackContext) -> str | int | None:
    query = update.callback_query
    user_id, lang = get_user_id_and_lang(update, context); data = query.data
    client_info = db.find_client_by_user_id(user_id)
    log.info(f"handle_client_callback: User {user_id}, Data {data}, Lang {lang}")

    if not client_info or client_info['subscription_end'] < int(datetime.now(UTC_TZ).timestamp()):
        log.warning(f"Expired/Invalid client {user_id} tried action: {data}")
        await query.answer(get_text(user_id, 'subscription_expired', lang_override=lang), show_alert=True)
        clear_conversation_data(context); return ConversationHandler.END

    action = data.split(CALLBACK_CLIENT_PREFIX)[1].split('?')[0]
    log.debug(f"Client CB Route: Action='{action}', Data='{data}'")

    if action == "select_bot_task":
        return await client_select_bot_generic(update, context, CALLBACK_TASK_PREFIX, STATE_TASK_SETUP, 'task_select_userbot')
    elif action == "manage_folders":
        return await client_folder_menu(update, context)
    elif action == "select_bot_join":
        return await client_select_bot_generic(update, context, CALLBACK_JOIN_PREFIX, STATE_WAITING_FOR_GROUP_LINKS, 'join_select_userbot')
    elif action == "view_stats":
        return await client_show_stats(update, context)
    elif action == "language":
        return await client_ask_select_language(update, context)
    elif action == "back_to_menu":
        await query.answer()
        clear_conversation_data(context)
        return await client_menu(update, context)
    else:
        log.warning(f"Unhandled CLIENT CB: Action='{action}', Data='{data}'");
        await query.answer(get_text(user_id, 'error_invalid_action', lang_override=lang, default_text="Action not recognized."), show_alert=True)
        return ConversationHandler.END


async def handle_admin_callback(update: Update, context: CallbackContext) -> str | int | None:
    query = update.callback_query
    user_id, lang = get_user_id_and_lang(update, context); data = query.data

    if not is_admin(user_id):
        await query.answer(get_text(user_id, 'unauthorized', lang_override=lang), show_alert=True)
        return ConversationHandler.END

    action_full = data.split(CALLBACK_ADMIN_PREFIX)[1]
    action_main = action_full.split('?')[0]
    log.info(f"handle_admin_callback: User {user_id}, ActionFull '{action_full}', ActionMain '{action_main}'")

    # Route admin actions
    if action_main == "back_to_menu":
        await query.answer()
        return await admin_command(update, context)
    elif action_main == "add_bot_prompt":
        await query.answer()
        await send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_prompt_phone', lang_override=lang))
        return STATE_WAITING_FOR_PHONE
    elif action_main == "remove_bot_select":
        return await admin_select_userbot_to_remove(update, context)
    elif action_main.startswith("remove_bot_confirm_prompt_"):
        return await admin_confirm_remove_userbot_prompt(update, context)
    elif action_main.startswith("remove_bot_confirmed_execute_"):
        return await admin_remove_userbot_confirmed_execute(update, context)
    elif action_main == "list_bots":
        return await admin_list_userbots(update, context)
    elif action_main == "gen_invite_prompt":
        await query.answer()
        await send_or_edit_message(update, context, get_text(user_id, 'admin_invite_prompt_details', lang_override=lang))
        return STATE_WAITING_FOR_SUB_DETAILS
    elif action_main == "view_subs":
        return await admin_view_subscriptions(update, context)
    elif action_main == "extend_sub_prompt":
        await query.answer()
        await send_or_edit_message(update, context, get_text(user_id, 'admin_extend_prompt_code', lang_override=lang))
        return STATE_WAITING_FOR_EXTEND_CODE
    elif action_main == "assign_bots_prompt":
        await query.answer()
        await send_or_edit_message(update, context, get_text(user_id, 'admin_assignbots_prompt_code', lang_override=lang))
        return STATE_WAITING_FOR_ADD_USERBOTS_CODE
    elif action_main == "view_logs":
        return await admin_view_system_logs(update, context)
    # Admin Task Management Callbacks
    elif action_main == "manage_tasks":
        return await admin_task_menu(update, context)
    elif action_main == "view_tasks":
        return await admin_view_tasks(update, context)
    elif action_main == "create_task":
        return await admin_create_task_start(update, context)
    elif action_main.startswith("task_bot_"):
        await query.answer()
        context.user_data[CTX_TASK_BOT] = action_main.replace("task_bot_", "")
        await send_or_edit_message(update, context, get_text(user_id, 'admin_task_enter_message', lang_override=lang))
        return STATE_ADMIN_TASK_MESSAGE
    elif action_main.startswith("task_options_"):
        return await admin_task_options(update, context)
    elif action_main.startswith("toggle_task_status_"):
        return await admin_toggle_task_status(update, context)
    elif action_main.startswith("delete_task_confirm_"):
        return await admin_delete_task_confirm(update, context)
    elif action_main.startswith("delete_task_execute_"):
        return await admin_delete_task_execute(update, context)
    else:
        await query.answer(get_text(user_id, 'error_invalid_action', lang_override=lang, default_text="Admin action not recognized."), show_alert=True)
        return ConversationHandler.END

# ... (handle_folder_callback, handle_task_callback, etc. remain similar, ensure they use `await send_or_edit_message`) ...

# --- Callback Query Routers (Continued) ---

async def handle_folder_callback(update: Update, context: CallbackContext) -> str | int | None:
    query = update.callback_query
    user_id, lang = get_user_id_and_lang(update, context); data = query.data
    action = data.split(CALLBACK_FOLDER_PREFIX)[1].split('?')[0]
    log.info(f"handle_folder_callback: User {user_id}, Data {data}, Action {action}, Lang {lang}")

    if action == "create_prompt":
        await query.answer()
        await send_or_edit_message(update, context, get_text(user_id, 'folder_create_prompt', lang_override=lang))
        return STATE_WAITING_FOR_FOLDER_NAME
    elif action == "select_edit":
        return await client_select_folder_to_edit_or_delete(update, context, 'edit')
    elif action == "select_delete":
        return await client_select_folder_to_edit_or_delete(update, context, 'delete')
    elif action == "edit_selected":
        return await client_show_folder_edit_options(update, context)
    elif action == "delete_selected_prompt":
        return await client_confirm_folder_delete_prompt(update, context)
    elif action == "delete_confirmed_execute":
        return await client_delete_folder_confirmed_execute(update, context)
    elif action == "back_to_manage":
        await query.answer()
        clear_conversation_data(context)
        return await client_folder_menu(update, context)
    elif action == "edit_add_prompt":
        await query.answer()
        folder_name = context.user_data.get(CTX_FOLDER_NAME, get_text(user_id, "this_folder", lang_override=lang, default_text="this folder"))
        await send_or_edit_message(update, context, get_text(user_id, 'folder_edit_add_prompt', lang_override=lang, name=html.escape(folder_name)))
        return STATE_WAITING_FOR_GROUP_LINKS
    elif action == "edit_remove_select":
        return await client_select_groups_to_remove(update, context)
    elif action == "edit_toggle_remove":
        return await client_toggle_group_for_removal(update, context)
    elif action == "edit_remove_confirm":
        return await client_confirm_remove_selected_groups(update, context)
    elif action == "edit_rename_prompt":
        await query.answer()
        current_name = context.user_data.get(CTX_FOLDER_NAME, get_text(user_id, "this_folder", lang_override=lang, default_text="this folder"))
        await send_or_edit_message(update, context, get_text(user_id, 'folder_edit_rename_prompt', lang_override=lang, current_name=html.escape(current_name)))
        return STATE_FOLDER_RENAME_PROMPT
    elif action == "back_to_edit_options":
        await query.answer()
        context.user_data.pop(CTX_TARGET_GROUP_IDS_TO_REMOVE, None)
        return await client_show_folder_edit_options(update, context)
    else:
        log.warning(f"Unhandled FOLDER CB: Action='{action}', Data='{data}'");
        await query.answer(get_text(user_id, 'error_invalid_action', lang_override=lang, default_text="Folder action not recognized."), show_alert=True)
        return ConversationHandler.END

async def handle_task_callback(update: Update, context: CallbackContext) -> str | int | None:
    query = update.callback_query
    user_id, lang = get_user_id_and_lang(update, context); data = query.data
    action = data.split(CALLBACK_TASK_PREFIX)[1].split('?')[0]
    log.info(f"handle_task_callback: User {user_id}, Data {data}, Action {action}, Lang {lang}")

    if action.startswith("select_"):
        return await handle_userbot_selection_callback(update, context, CALLBACK_TASK_PREFIX)
    elif action == "back_to_bot_select":
        await query.answer()
        clear_conversation_data(context)
        return await client_select_bot_generic(update, context, CALLBACK_TASK_PREFIX, STATE_TASK_SETUP, 'task_select_userbot')
    elif action == "back_to_task_menu":
        # query answered in task_show_settings_menu
        return await task_show_settings_menu(update, context)
    elif action == "set_primary_link":
        return await task_prompt_set_link(update, context, 'primary')
    elif action == "set_time":
        return await task_prompt_start_time(update, context)
    elif action == "set_interval":
        return await task_select_interval(update, context)
    elif action == "set_target_type":
        return await task_select_target_type(update, context)
    elif action == "select_folder_target":
        return await task_select_folder_for_target(update, context)
    elif action == "set_target_all":
        return await task_set_target(update, context, 'all')
    elif action == "set_target_folder":
        return await task_set_target(update, context, 'folder')
    elif action == "back_to_target_type":
        return await task_select_target_type(update, context)
    elif action == "toggle_status":
        return await task_toggle_status(update, context)
    elif action == "save":
        return await task_save_settings(update, context)
    else:
        log.warning(f"Unhandled TASK CB: Action='{action}', Data='{data}'");
        await query.answer(get_text(user_id, 'error_invalid_action', lang_override=lang, default_text="Task action not recognized."), show_alert=True)
        return ConversationHandler.END

async def handle_join_callback(update: Update, context: CallbackContext) -> str | int | None:
    query = update.callback_query
    user_id, lang = get_user_id_and_lang(update, context); data = query.data
    log.info(f"handle_join_callback: User {user_id}, Data {data}, Lang {lang}")

    if data.startswith(CALLBACK_JOIN_PREFIX + "select_"):
        return await handle_userbot_selection_callback(update, context, CALLBACK_JOIN_PREFIX)
    else:
        log.warning(f"Unhandled JOIN CB: Data='{data}'");
        await query.answer(get_text(user_id, 'error_invalid_action', lang_override=lang, default_text="Join action not recognized."), show_alert=True)
        return ConversationHandler.END

async def handle_language_callback(update: Update, context: CallbackContext) -> str | int | None:
     query = update.callback_query
     user_id, lang = get_user_id_and_lang(update,context); data = query.data
     log.info(f"handle_language_callback: User {user_id}, Data {data}, Lang {lang}")

     if data.startswith(CALLBACK_LANG_PREFIX):
         return await set_language_handler(update, context)
     else:
         log.warning(f"Unhandled LANG CB: Data='{data}'");
         await query.answer(get_text(user_id, 'error_invalid_action', lang_override=lang, default_text="Language action not recognized."), show_alert=True)
         return ConversationHandler.END

async def handle_interval_callback(update: Update, context: CallbackContext) -> str | int | None:
     query = update.callback_query
     user_id, lang = get_user_id_and_lang(update,context); data = query.data
     log.info(f"handle_interval_callback: User {user_id}, Data {data}, Lang {lang}")

     if data.startswith(CALLBACK_INTERVAL_PREFIX):
         return await process_interval_callback(update, context)
     else:
         log.warning(f"Unhandled INTERVAL CB: Data='{data}'");
         await query.answer(get_text(user_id, 'error_invalid_action', lang_override=lang, default_text="Interval action not recognized."), show_alert=True)
         return ConversationHandler.END

async def handle_generic_callback(update: Update, context: CallbackContext) -> str | int | None:
    query = update.callback_query; await query.answer()
    user_id, lang = get_user_id_and_lang(update, context); data = query.data
    action = data.split(CALLBACK_GENERIC_PREFIX)[1] if CALLBACK_GENERIC_PREFIX in data else "unknown"
    log.info(f"handle_generic_callback: User {user_id}, Data {data}, Action {action}, Lang {lang}")

    if action == "cancel" or action == "confirm_no":
        await send_or_edit_message(update, context, get_text(user_id, 'cancelled', lang_override=lang), reply_markup=None)
        clear_conversation_data(context)
        if is_admin(user_id): return await admin_command(update, context)
        elif db.find_client_by_user_id(user_id): return await client_menu(update, context)
        else: return ConversationHandler.END
    elif action == "noop":
        return None # Do nothing
    else:
        log.warning(f"Unhandled GENERIC CB: Action='{action}', Data='{data}'");
        await send_or_edit_message(update,context,get_text(user_id, 'error_invalid_action', lang_override=lang, default_text="Generic action not recognized."))
        return ConversationHandler.END

# --- Fallback Handler ---
async def conversation_fallback(update: Update, context: CallbackContext) -> int:
    """Fallback handler for unexpected messages during a conversation."""
    try:
        user_id, lang = get_user_id_and_lang(update, context)
        current_state = context.user_data.get(ConversationHandler.CURRENT_STATE) # Check state if needed
        log.warning(f"Conversation fallback triggered for user {user_id}. State: {current_state}. Update: {update.to_json()}")

        await send_or_edit_message(
            update, context,
            get_text(user_id, 'session_expired', lang_override=lang),
            parse_mode=ParseMode.HTML,
            reply_markup=None # Remove any inline keyboards
        )
        clear_conversation_data(context)
        return ConversationHandler.END # Always end the conversation

    except Exception as e:
        log.error(f"Error in conversation_fallback: {e}", exc_info=True)
        await async_error_handler(update, context) # Try to notify user
        return ConversationHandler.END

# --- Main Callback Router ---
async def main_callback_handler(update: Update, context: CallbackContext) -> str | int | None:
    """Master router for all callback queries."""
    query = update.callback_query
    user_id = query.from_user.id
    data = query.data
    log.info(f"Main CB Router: User {user_id}, Data '{data}'")

    next_state = ConversationHandler.END # Default if not handled or sub-handler ends
    try:
        if data.startswith(CALLBACK_CLIENT_PREFIX):
            next_state = await handle_client_callback(update, context)
        elif data.startswith(CALLBACK_ADMIN_PREFIX):
            next_state = await handle_admin_callback(update, context)
        elif data.startswith(CALLBACK_FOLDER_PREFIX):
            next_state = await handle_folder_callback(update, context)
        elif data.startswith(CALLBACK_TASK_PREFIX):
            next_state = await handle_task_callback(update, context)
        elif data.startswith(CALLBACK_JOIN_PREFIX):
            next_state = await handle_join_callback(update, context)
        elif data.startswith(CALLBACK_LANG_PREFIX):
            next_state = await handle_language_callback(update, context)
        elif data.startswith(CALLBACK_INTERVAL_PREFIX):
            next_state = await handle_interval_callback(update, context)
        elif data.startswith(CALLBACK_GENERIC_PREFIX):
            next_state = await handle_generic_callback(update, context)
        else:
            await query.answer(get_text(user_id, 'error_invalid_action', lang_override=context.user_data.get(CTX_LANG,'en'), default_text="Unknown button."), show_alert=True)
            log.warning(f"Unknown callback data pattern in main_callback_handler: {data}")
            next_state = ConversationHandler.END

        if not query._answered: # Answer silently if sub-handler didn't
            try: await query.answer()
            except Exception: pass

        return next_state

    except Exception as e:
        log.error(f"Error in main_callback_handler for data '{data}': {e}", exc_info=True)
        if not query._answered:
            try: await query.answer(get_text(user_id, 'error_generic', lang_override=context.user_data.get(CTX_LANG,'en')), show_alert=True)
            except Exception: pass
        return ConversationHandler.END

# --- Conversation Handler Definition ---
main_conversation = ConversationHandler(
    entry_points=[
        CommandHandler('start', start_command),
        CommandHandler('admin', admin_command),
        CommandHandler('cancel', cancel_command)
    ],
    states={
        # States expecting text input
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
        STATE_WAITING_FOR_GROUP_LINKS: [MessageHandler(Filters.text & ~Filters.command, process_folder_links)], # Used by folder add
        STATE_FOLDER_RENAME_PROMPT: [MessageHandler(Filters.text & ~Filters.command, process_folder_rename)],
        STATE_WAITING_FOR_PRIMARY_MESSAGE_LINK: [MessageHandler(Filters.text & ~Filters.command, lambda u, c: process_task_link(u, c, 'primary'))],
        STATE_WAITING_FOR_FALLBACK_MESSAGE_LINK: [MessageHandler(Filters.text & ~Filters.command, lambda u, c: process_task_link(u, c, 'fallback'))],
        STATE_WAITING_FOR_START_TIME: [MessageHandler(Filters.text & ~Filters.command, process_task_start_time)],
        STATE_ADMIN_TASK_MESSAGE: [MessageHandler(Filters.text & ~Filters.command, admin_process_task_message)],
        STATE_ADMIN_TASK_SCHEDULE: [MessageHandler(Filters.text & ~Filters.command, admin_process_task_schedule)],
        STATE_ADMIN_TASK_TARGET: [MessageHandler(Filters.text & ~Filters.command, admin_process_task_target)],

        # States primarily driven by callbacks
        STATE_TASK_SETUP: [CallbackQueryHandler(main_callback_handler)],
        STATE_WAITING_FOR_FOLDER_SELECTION: [CallbackQueryHandler(main_callback_handler)],
        STATE_WAITING_FOR_USERBOT_SELECTION: [CallbackQueryHandler(main_callback_handler)],
        STATE_WAITING_FOR_FOLDER_ACTION: [CallbackQueryHandler(main_callback_handler)],
        STATE_FOLDER_EDIT_REMOVE_SELECT: [CallbackQueryHandler(main_callback_handler)],
        STATE_ADMIN_CONFIRM_USERBOT_RESET: [CallbackQueryHandler(main_callback_handler)],
        STATE_ADMIN_TASK_CONFIRM: [CallbackQueryHandler(main_callback_handler)], # If using CB for admin task confirm
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

log.info("Handlers module loaded and structure updated.")

# --- Admin Task Management Handlers (specific implementation moved to admin_handlers.py) ---
# Keep the definitions here if they are still needed for routing or complex logic within handlers.py,
# otherwise, they can be potentially removed if fully handled by the imported functions + main_callback_handler.
# For now, keeping the async function signatures needed for routing callbacks.

async def admin_task_menu(update: Update, context: CallbackContext) -> int:
    # This function is called via callback 'admin_manage_tasks'
    query = update.callback_query
    if query: await query.answer()
    user_id, lang = get_user_id_and_lang(update, context)
    keyboard = [
        [InlineKeyboardButton(get_text(user_id, 'admin_task_view', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}view_tasks?page=0")],
        [InlineKeyboardButton(get_text(user_id, 'admin_task_create', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}create_task")],
        [InlineKeyboardButton(get_text(user_id, 'button_back', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}back_to_menu")]
    ]
    markup = InlineKeyboardMarkup(keyboard)
    await send_or_edit_message(
        update, context,
        get_text(user_id, 'admin_task_menu_title', lang_override=lang),
        reply_markup=markup
    )
    return ConversationHandler.END # Menu endpoint

async def admin_view_tasks(update: Update, context: CallbackContext) -> int:
    # Called via callback 'admin_view_tasks'
    query = update.callback_query
    if query: await query.answer()
    user_id, lang = get_user_id_and_lang(update, context)
    try:
        current_page = 0
        if query and query.data and '?page=' in query.data:
            current_page = int(query.data.split('?page=')[1])
    except (ValueError, IndexError, AttributeError): current_page = 0

    tasks, total_tasks = db.get_admin_tasks(page=current_page, per_page=ITEMS_PER_PAGE)
    keyboard = []
    text = f"<b>{get_text(user_id, 'admin_task_list_title', lang_override=lang)}</b>"

    if tasks:
        text += f" (Page {current_page + 1}/{math.ceil(total_tasks / ITEMS_PER_PAGE)})\n\n"
        for task in tasks:
            status_icon = "🟢" if task['status'] == 'active' else "⚪️"
            task_info_line = f"{status_icon} Bot: {html.escape(task['userbot_phone'])} -> Target: {html.escape(task['target'])}"
            if task['schedule']: task_info_line += f" | Schedule: <code>{html.escape(task['schedule'])}</code>"
            keyboard.append([InlineKeyboardButton(task_info_line, callback_data=f"{CALLBACK_ADMIN_PREFIX}task_options_{task['id']}")])
    else:
        text += "\n" + get_text(user_id, 'admin_task_list_empty', lang_override=lang)

    pagination_buttons = build_pagination_buttons(f"{CALLBACK_ADMIN_PREFIX}view_tasks", current_page, total_tasks, ITEMS_PER_PAGE, lang)
    keyboard.extend(pagination_buttons)
    keyboard.append([InlineKeyboardButton(get_text(user_id, 'button_back', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}manage_tasks")])
    markup = InlineKeyboardMarkup(keyboard)
    await send_or_edit_message(update, context, text, reply_markup=markup, parse_mode=ParseMode.HTML)
    return ConversationHandler.END # Display only

async def admin_create_task_start(update: Update, context: CallbackContext) -> int:
    # Called via callback 'admin_create_task'
    query = update.callback_query
    if query: await query.answer()
    user_id, lang = get_user_id_and_lang(update, context)
    clear_conversation_data(context)
    context.user_data[CTX_USER_ID] = user_id # Restore essentials
    context.user_data[CTX_LANG] = lang
    return await admin_select_task_bot(update, context)

async def admin_select_task_bot(update: Update, context: CallbackContext) -> int:
    # Called by admin_create_task_start
    user_id, lang = get_user_id_and_lang(update, context)
    keyboard = []
    all_bots_db = db.get_all_userbots()
    active_bots = [bot for bot in all_bots_db if bot['status'] == 'active']

    if not active_bots:
        await send_or_edit_message(update, context, get_text(user_id, 'admin_task_no_bots', lang_override=lang),
                                           reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(get_text(user_id,'button_back',lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}manage_tasks")]]))
        return ConversationHandler.END

    for bot in active_bots:
        display_name = f"@{bot['username']}" if bot['username'] else bot['phone_number']
        keyboard.append([InlineKeyboardButton(html.escape(display_name), callback_data=f"{CALLBACK_ADMIN_PREFIX}task_bot_{bot['phone_number']}")])

    keyboard.append([InlineKeyboardButton(get_text(user_id, 'button_back', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}manage_tasks")])
    markup = InlineKeyboardMarkup(keyboard)
    await send_or_edit_message(update, context, get_text(user_id, 'admin_task_select_bot', lang_override=lang), reply_markup=markup)
    # This expects a callback, the state transition happens in handle_admin_callback
    return STATE_ADMIN_TASK_MESSAGE # Stay in a state expecting the *next* input (message) after CB selection

async def admin_task_options(update: Update, context: CallbackContext) -> int:
    # Called via callback 'admin_task_options_{id}'
    query = update.callback_query; await query.answer()
    user_id, lang = get_user_id_and_lang(update, context)
    task_id = None
    try: task_id = int(query.data.split(f"{CALLBACK_ADMIN_PREFIX}task_options_")[1])
    except (IndexError, ValueError):
        log.error(f"Failed to parse task_id from CB: {query.data}")
        await send_or_edit_message(update, context, get_text(user_id, 'error_generic', lang_override=lang))
        return await admin_view_tasks(update, context)

    task = db.get_admin_task(task_id)
    if not task:
        await send_or_edit_message(update, context, get_text(user_id, 'admin_task_not_found', lang_override=lang))
        return await admin_view_tasks(update, context)

    status_icon = "🟢" if task['status'] == 'active' else "⚪️"
    toggle_text_key = 'admin_task_deactivate' if task['status'] == 'active' else 'admin_task_activate'
    toggle_text = get_text(user_id, toggle_text_key, lang_override=lang)

    keyboard = [
        [InlineKeyboardButton(toggle_text, callback_data=f"{CALLBACK_ADMIN_PREFIX}toggle_task_status_{task_id}")],
        [InlineKeyboardButton(get_text(user_id, 'admin_task_delete_button', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}delete_task_confirm_{task_id}")],
        [InlineKeyboardButton(get_text(user_id, 'button_back', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}view_tasks?page=0")]
    ]
    markup = InlineKeyboardMarkup(keyboard)

    details_text = f"<b>Task #{task_id} Details</b>\n"
    details_text += f"Status: {status_icon} {html.escape(task['status'].capitalize())}\n"
    details_text += f"Bot: {html.escape(task['userbot_phone'])}\n"
    details_text += f"Message: <pre>{html.escape(task['message'][:100])}{'...' if len(task['message']) > 100 else ''}</pre>\n"
    details_text += f"Schedule: <code>{html.escape(task['schedule'])}</code>\n"
    details_text += f"Target: {html.escape(task['target'])}\n"
    details_text += f"Last Run: {format_dt(task['last_run']) if task['last_run'] else 'Never'}\n"
    details_text += f"Next Run Estimate: {format_dt(task['next_run']) if task['next_run'] else 'Not Scheduled'}\n"

    await send_or_edit_message(update, context, details_text, reply_markup=markup, parse_mode=ParseMode.HTML)
    return ConversationHandler.END # Display only

async def admin_toggle_task_status(update: Update, context: CallbackContext) -> int:
    # Called via callback 'admin_toggle_task_status_{id}'
    query = update.callback_query; await query.answer()
    user_id, lang = get_user_id_and_lang(update, context)
    task_id = None
    try: task_id = int(query.data.split(f"{CALLBACK_ADMIN_PREFIX}toggle_task_status_")[1])
    except (IndexError, ValueError):
        log.error(f"Failed to parse task_id for toggle status: {query.data}")
        await send_or_edit_message(update, context, get_text(user_id, 'error_generic', lang_override=lang))
        return await admin_view_tasks(update, context)

    if db.toggle_admin_task_status(task_id):
        await send_or_edit_message(update, context, get_text(user_id, 'admin_task_toggled', lang_override=lang))
        query.data = f"{CALLBACK_ADMIN_PREFIX}task_options_{task_id}" # Modify query data to go back to options
        return await admin_task_options(update, context)
    else:
        await send_or_edit_message(update, context, get_text(user_id, 'admin_task_error', lang_override=lang))
        return await admin_view_tasks(update, context)

async def admin_delete_task_confirm(update: Update, context: CallbackContext) -> int:
    # Called via callback 'admin_delete_task_confirm_{id}'
    query = update.callback_query; await query.answer()
    user_id, lang = get_user_id_and_lang(update, context)
    task_id = None
    try: task_id = int(query.data.split(f"{CALLBACK_ADMIN_PREFIX}delete_task_confirm_")[1])
    except (IndexError, ValueError):
        log.error(f"Failed to parse task_id for delete confirm: {query.data}")
        await send_or_edit_message(update, context, get_text(user_id, 'error_generic', lang_override=lang))
        return await admin_view_tasks(update, context)

    task = db.get_admin_task(task_id)
    if not task:
        await send_or_edit_message(update, context, get_text(user_id, 'admin_task_not_found', lang_override=lang))
        return await admin_view_tasks(update, context)

    confirm_text = f"Are you sure you want to delete Task #{task_id}?\nBot: {html.escape(task['userbot_phone'])}\nTarget: {html.escape(task['target'])}"
    keyboard = [
        [InlineKeyboardButton(get_text(user_id, 'button_yes', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}delete_task_execute_{task_id}")],
        [InlineKeyboardButton(get_text(user_id, 'button_no', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}task_options_{task_id}")]
    ]
    markup = InlineKeyboardMarkup(keyboard)
    await send_or_edit_message(update, context, confirm_text, reply_markup=markup)
    return ConversationHandler.END # Waiting for Yes/No CB

async def admin_delete_task_execute(update: Update, context: CallbackContext) -> int:
    # Called via callback 'admin_delete_task_execute_{id}'
    query = update.callback_query; await query.answer()
    user_id, lang = get_user_id_and_lang(update, context)
    task_id = None
    try: task_id = int(query.data.split(f"{CALLBACK_ADMIN_PREFIX}delete_task_execute_")[1])
    except (IndexError, ValueError):
        log.error(f"Failed to parse task_id for delete execute: {query.data}")
        await send_or_edit_message(update, context, get_text(user_id, 'error_generic', lang_override=lang))
        return await admin_view_tasks(update, context)

    if db.delete_admin_task(task_id):
        await send_or_edit_message(update, context, get_text(user_id, 'admin_task_deleted', lang_override=lang))
    else:
        await send_or_edit_message(update, context, get_text(user_id, 'admin_task_error', lang_override=lang))

    return await admin_view_tasks(update, context)
