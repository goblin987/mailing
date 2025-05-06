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
from config import (
    log, ADMIN_IDS, is_admin, LITHUANIA_TZ, UTC_TZ, SESSION_DIR,
    # States
    STATE_WAITING_FOR_CODE, STATE_WAITING_FOR_PHONE, STATE_WAITING_FOR_API_ID,
    STATE_WAITING_FOR_API_HASH, STATE_WAITING_FOR_CODE_USERBOT,
    STATE_WAITING_FOR_PASSWORD, STATE_WAITING_FOR_SUB_DETAILS,
    # STATE_WAITING_FOR_FOLDER_CHOICE, # Deprecated
    STATE_WAITING_FOR_FOLDER_NAME,
    STATE_WAITING_FOR_FOLDER_SELECTION, STATE_TASK_SETUP,
    STATE_WAITING_FOR_LANGUAGE, STATE_WAITING_FOR_EXTEND_CODE,
    STATE_WAITING_FOR_EXTEND_DAYS, STATE_WAITING_FOR_ADD_USERBOTS_CODE,
    STATE_WAITING_FOR_ADD_USERBOTS_COUNT, STATE_SELECT_TARGET_GROUPS,
    STATE_WAITING_FOR_USERBOT_SELECTION, STATE_WAITING_FOR_GROUP_LINKS,
    STATE_WAITING_FOR_FOLDER_ACTION, STATE_WAITING_FOR_PRIMARY_MESSAGE_LINK,
    STATE_WAITING_FOR_FALLBACK_MESSAGE_LINK, STATE_FOLDER_EDIT_REMOVE_SELECT,
    STATE_FOLDER_RENAME_PROMPT, STATE_ADMIN_CONFIRM_USERBOT_RESET,
    STATE_WAITING_FOR_START_TIME,

    # Callback Prefixes
    CALLBACK_ADMIN_PREFIX, CALLBACK_CLIENT_PREFIX, CALLBACK_TASK_PREFIX,
    CALLBACK_FOLDER_PREFIX, CALLBACK_JOIN_PREFIX, CALLBACK_LANG_PREFIX,
    CALLBACK_INTERVAL_PREFIX, CALLBACK_GENERIC_PREFIX
)
from translations import get_text, language_names, translations

# --- Constants ---
ITEMS_PER_PAGE = 5 # For pagination in lists

# --- Conversation Context Keys ---
CTX_USER_ID = "_user_id"; CTX_LANG = "_lang"; CTX_PHONE = "phone"; CTX_API_ID = "api_id"
CTX_API_HASH = "api_hash"; CTX_AUTH_DATA = "auth_data"; CTX_INVITE_DETAILS = "invite_details"
CTX_EXTEND_CODE = "extend_code"; CTX_ADD_BOTS_CODE = "add_bots_code"; CTX_FOLDER_ID = "folder_id"
CTX_FOLDER_NAME = "folder_name"; CTX_FOLDER_ACTION = "folder_action"; CTX_SELECTED_BOTS = "selected_bots"
CTX_TARGET_GROUP_IDS_TO_REMOVE = "target_group_ids_to_remove"; CTX_TASK_PHONE = "task_phone"
CTX_TASK_SETTINGS = "task_settings"; CTX_PAGE = "page"; CTX_MESSAGE_ID = "message_id"

# --- Helper Functions ---

def clear_conversation_data(context: CallbackContext):
    """Clears volatile keys from user_data, preserving user_id and lang."""
    if not hasattr(context, 'user_data') or context.user_data is None: return
    user_id = context.user_data.get(CTX_USER_ID); lang = context.user_data.get(CTX_LANG)
    keys_to_clear = [
        CTX_PHONE, CTX_API_ID, CTX_API_HASH, CTX_AUTH_DATA, CTX_EXTEND_CODE,
        CTX_ADD_BOTS_CODE, CTX_FOLDER_ID, CTX_FOLDER_NAME, CTX_SELECTED_BOTS,
        CTX_TARGET_GROUP_IDS_TO_REMOVE, CTX_TASK_PHONE, CTX_TASK_SETTINGS,
        CTX_PAGE, CTX_MESSAGE_ID
    ]
    for key in keys_to_clear: context.user_data.pop(key, None)
    if user_id: context.user_data[CTX_USER_ID] = user_id
    if lang: context.user_data[CTX_LANG] = lang
    log.debug(f"Cleared volatile conversation user_data for user {user_id or 'N/A'}")

def get_user_id_and_lang(update: Update, context: CallbackContext) -> tuple[int | None, str]:
    user_id = context.user_data.get(CTX_USER_ID) if context.user_data else None
    lang = context.user_data.get(CTX_LANG) if context.user_data else None
    if not user_id and update and update.effective_user:
        user_id = update.effective_user.id
        if not context.user_data: context.user_data = {}
        context.user_data[CTX_USER_ID] = user_id
    if user_id and not lang:
        lang = db.get_user_language(user_id)
        if not context.user_data: context.user_data = {}
        context.user_data[CTX_LANG] = lang
    elif not lang: lang = 'en'
    if lang and user_id and context.user_data and CTX_LANG not in context.user_data:
        context.user_data[CTX_LANG] = lang # Ensure lang is stored if fetched/defaulted
    return user_id, lang or 'en'

async def _send_or_edit_message(update: Update, context: CallbackContext, text: str, **kwargs):
    """Internal async helper to actually send/edit messages."""
    user_id, lang = get_user_id_and_lang(update, context)
    parse_mode = kwargs.get('parse_mode', ParseMode.HTML)
    kwargs['parse_mode'] = parse_mode
    chat_id = update.effective_chat.id if update.effective_chat else user_id
    message_id = None
    query = update.callback_query

    if query:
        message_id = query.message.message_id
        if not chat_id: chat_id = query.from_user.id
    elif context.user_data and 'message_id' in context.user_data:
        message_id = context.user_data.get(CTX_MESSAGE_ID)
        if not chat_id: chat_id = user_id

    if not chat_id:
        log.error(f"Cannot determine chat_id for sending/editing. User ID: {user_id}")
        return

    reply_markup = kwargs.get('reply_markup')
    if reply_markup and not isinstance(reply_markup, InlineKeyboardMarkup):
        kwargs['reply_markup'] = None

    sent_message = None
    answered_callback = False
    query_id = query.id if query else None

    try:
        if query:
            try:
                if query_id and not context.bot_data.get(f'answered_{query_id}', False):
                    await query.answer()
                    context.bot_data[f'answered_{query_id}'] = True
                answered_callback = True
            except (BadRequest, TelegramError) as cb_e:
                log.debug(f"Ignoring callback answer error: {cb_e}")

            await context.bot.edit_message_text(
                chat_id=chat_id, message_id=query.message.message_id, text=text, **kwargs
            )
            if context.user_data: context.user_data[CTX_MESSAGE_ID] = query.message.message_id
        elif update.message:
            sent_message = await update.message.reply_text(text=text, **kwargs)
            if context.user_data: context.user_data[CTX_MESSAGE_ID] = sent_message.message_id
        elif message_id and chat_id:
             await context.bot.edit_message_text(
                 chat_id=chat_id, message_id=message_id, text=text, **kwargs
             )
        else:
            sent_message = await context.bot.send_message(chat_id=chat_id, text=text, **kwargs)
            if context.user_data: context.user_data[CTX_MESSAGE_ID] = sent_message.message_id
    except BadRequest as e:
        if "message is not modified" in str(e).lower():
            log.debug(f"Ignoring 'message is not modified' error for user {user_id}.")
            if query_id and not answered_callback and not context.bot_data.get(f'answered_{query_id}', False):
                try: await query.answer(); context.bot_data[f'answered_{query_id}'] = True
                except Exception: pass
        elif "message to edit not found" in str(e).lower() or "chat not found" in str(e).lower():
            log.warning(f"Failed to edit (maybe deleted: {message_id}): {e}. Sending new.")
            try:
                sent_message = await context.bot.send_message(chat_id=chat_id, text=text, **kwargs)
                if context.user_data: context.user_data[CTX_MESSAGE_ID] = sent_message.message_id
            except Exception as send_e: log.error(f"Failed to send fallback message: {send_e}")
            if context.user_data: context.user_data.pop(CTX_MESSAGE_ID, None)
        elif "reply message not found" in str(e).lower():
             log.warning(f"Failed reply (original deleted?): {e}. Sending standalone.")
             try:
                 sent_message = await context.bot.send_message(chat_id=chat_id, text=text, **kwargs)
                 if context.user_data: context.user_data[CTX_MESSAGE_ID] = sent_message.message_id
             except Exception as send_e: log.error(f"Failed to send fallback standalone: {send_e}")
        else:
            log.error(f"BadRequest sending/editing for user {user_id}: {e}", exc_info=True)
            try: await context.bot.send_message(chat_id=chat_id, text=get_text(user_id, 'error_generic', lang=lang))
            except Exception as send_e: log.error(f"Failed to send fallback error msg: {send_e}")
    except TelegramError as e:
         log.error(f"TelegramError sending/editing for user {user_id}: {e}", exc_info=True)
         if isinstance(e, RetryAfter): log.warning(f"Flood control: Wait {e.retry_after}s.")
         try: await context.bot.send_message(chat_id=chat_id, text=get_text(user_id, 'error_generic', lang=lang))
         except Exception as send_e: log.error(f"Failed to send fallback error msg after TelegramError: {send_e}")
    except Exception as e:
        log.error(f"Unexpected error in _send_or_edit_message for user {user_id}: {e}", exc_info=True)
        try: await context.bot.send_message(chat_id=chat_id, text=get_text(user_id, 'error_generic', lang=lang))
        except Exception as send_e: log.error(f"Failed to send fallback error msg after unexpected error: {send_e}")

async def _show_menu_async(update: Update, context: CallbackContext, menu_builder_func):
    """Async helper to build and send/edit a menu."""
    user_id, lang = get_user_id_and_lang(update, context)
    message, markup, parse_mode = menu_builder_func(user_id, context)
    await _send_or_edit_message(update, context, message, reply_markup=markup, parse_mode=parse_mode)

async def error_handler(update: object, context: CallbackContext) -> None:
    """Log Errors caused by Updates and notify user."""
    log.error(f"Exception while handling an update:", exc_info=context.error)
    if isinstance(update, Update) and update.effective_chat:
        user_id, lang = get_user_id_and_lang(update, context)
        error_message = get_text(user_id, 'error_generic', lang=lang)
        context.dispatcher.run_async(_send_or_edit_message, update, context, error_message)

def format_dt(timestamp: int | None, tz=LITHUANIA_TZ, fmt='%Y-%m-%d %H:%M') -> str:
    if not timestamp: return get_text(0, 'task_value_not_set', lang='en')
    try:
        dt_utc = datetime.fromtimestamp(timestamp, UTC_TZ)
        dt_local = dt_utc.astimezone(tz)
        return dt_local.strftime(fmt)
    except (ValueError, TypeError, OSError) as e:
        log.warning(f"Could not format invalid timestamp: {timestamp}. Error: {e}")
        return "Invalid Date"

def build_client_menu(user_id, context: CallbackContext):
    client_info = db.find_client_by_user_id(user_id); lang = context.user_data.get(CTX_LANG, 'en') if context.user_data else 'en'
    if not client_info: return get_text(user_id, 'unknown_user', lang=lang), None, ParseMode.HTML
    code = client_info['invitation_code']; sub_end_ts = client_info['subscription_end']; now_ts = int(datetime.now(UTC_TZ).timestamp())
    is_expired = sub_end_ts < now_ts; end_date = format_dt(sub_end_ts, fmt='%Y-%m-%d') if sub_end_ts else 'N/A'; expiry_warning = " ⚠️ <b>Expired</b>" if is_expired else ""
    userbot_phones = db.get_client_bots(user_id); bot_count = len(userbot_phones); parse_mode = ParseMode.HTML
    menu_text = f"<b>{get_text(user_id, 'client_menu_title', lang=lang, code=html.escape(code))}</b>{expiry_warning}\n"
    menu_text += get_text(user_id, 'client_menu_sub_end', lang=lang, end_date=end_date) + "\n\n" + f"<u>{get_text(user_id, 'client_menu_userbots_title', lang=lang, count=bot_count)}</u>\n"
    if userbot_phones:
        for i, phone in enumerate(userbot_phones, 1):
            bot_db_info = db.find_userbot(phone); username = bot_db_info['username'] if bot_db_info else None; status = bot_db_info['status'].capitalize() if bot_db_info else 'Unknown'
            last_error = bot_db_info['last_error'] if bot_db_info else None; display_name = html.escape(f"@{username}" if username else phone)
            status_icon = "⚪️";
            if bot_db_info:
                if bot_db_info['status'] == 'active': status_icon = "🟢"
                elif bot_db_info['status'] == 'error': status_icon = "🔴"
                elif bot_db_info['status'] in ['connecting', 'authenticating', 'initializing']: status_icon = "⏳"
                elif bot_db_info['status'] in ['needs_code', 'needs_password']: status_icon = "⚠️"
            menu_text += get_text(user_id, 'client_menu_userbot_line', lang=lang, index=i, status_icon=status_icon, display_name=display_name, status=html.escape(status)) + "\n"
            if last_error: escaped_error = html.escape(last_error); error_line = get_text(user_id, 'client_menu_userbot_error', lang=lang, error=f"{escaped_error[:100]}{'...' if len(escaped_error)>100 else ''}"); menu_text += f"  {error_line}\n"
    else: menu_text += get_text(user_id, 'client_menu_no_userbots', lang=lang) + "\n"
    keyboard = [ [InlineKeyboardButton(get_text(user_id, 'client_menu_button_setup_tasks', lang=lang), callback_data=f"{CALLBACK_CLIENT_PREFIX}select_bot_task")], [InlineKeyboardButton(get_text(user_id, 'client_menu_button_manage_folders', lang=lang), callback_data=f"{CALLBACK_CLIENT_PREFIX}manage_folders")], [ InlineKeyboardButton(get_text(user_id, 'client_menu_button_join_groups', lang=lang), callback_data=f"{CALLBACK_CLIENT_PREFIX}select_bot_join"), ], [InlineKeyboardButton(get_text(user_id, 'client_menu_button_stats', lang=lang), callback_data=f"{CALLBACK_CLIENT_PREFIX}view_stats")], [ InlineKeyboardButton(get_text(user_id, 'client_menu_button_language', lang=lang), callback_data=f"{CALLBACK_CLIENT_PREFIX}language") ], ]
    markup = InlineKeyboardMarkup(keyboard); return menu_text, markup, parse_mode

def build_admin_menu(user_id, context: CallbackContext):
    lang = context.user_data.get(CTX_LANG, 'en') if context.user_data else 'en'; title = f"<b>{get_text(user_id, 'admin_panel_title', lang=lang)}</b>"; parse_mode = ParseMode.HTML
    keyboard = [ [ InlineKeyboardButton(get_text(user_id, 'admin_button_add_userbot', lang=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}add_bot_prompt"), InlineKeyboardButton(get_text(user_id, 'admin_button_remove_userbot', lang=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}remove_bot_select?page=0") ], [InlineKeyboardButton(get_text(user_id, 'admin_button_list_userbots', lang=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}list_bots?page=0")], [InlineKeyboardButton(get_text(user_id, 'admin_button_gen_invite', lang=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}gen_invite_prompt")], [InlineKeyboardButton(get_text(user_id, 'admin_button_view_subs', lang=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}view_subs?page=0")], [ InlineKeyboardButton(get_text(user_id, 'admin_button_extend_sub', lang=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}extend_sub_prompt"), InlineKeyboardButton(get_text(user_id, 'admin_button_assign_bots_client', lang=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}assign_bots_prompt") ], [InlineKeyboardButton(get_text(user_id, 'admin_button_view_logs', lang=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}view_logs?page=0")], ]
    markup = InlineKeyboardMarkup(keyboard); return title, markup, parse_mode

def build_pagination_buttons(base_callback_data: str, current_page: int, total_items: int, items_per_page: int, lang: str = 'en') -> list:
    buttons = [];
    if total_items <= items_per_page: return []
    total_pages = math.ceil(total_items / items_per_page); row = []
    if current_page > 0: prev_text = get_text(0, 'pagination_prev', lang=lang); row.append(InlineKeyboardButton(prev_text, callback_data=f"{base_callback_data}?page={current_page - 1}"))
    if total_pages > 1: page_text = get_text(0,'pagination_page',lang=lang).format(current=current_page + 1, total=total_pages); row.append(InlineKeyboardButton(page_text, callback_data=f"{CALLBACK_GENERIC_PREFIX}noop"))
    if current_page < total_pages - 1: next_text = get_text(0, 'pagination_next', lang=lang); row.append(InlineKeyboardButton(next_text, callback_data=f"{base_callback_data}?page={current_page + 1}"))
    if row: buttons.append(row)
    return buttons

def start_command(update: Update, context: CallbackContext) -> str | int:
    user_id, lang = get_user_id_and_lang(update, context); clear_conversation_data(context)
    log.info(f"Start cmd: UserID={user_id}, User={update.effective_user.username}")
    if is_admin(user_id):
        log.info(f"Admin user {user_id} used /start, showing admin menu.")
        context.dispatcher.run_async(_show_menu_async, update, context, build_admin_menu)
        return ConversationHandler.END
    else:
        client_info = db.find_client_by_user_id(user_id)
        if client_info:
            now_ts = int(datetime.now(UTC_TZ).timestamp())
            if client_info['subscription_end'] < now_ts:
                context.dispatcher.run_async(_send_or_edit_message, update, context, get_text(user_id, 'subscription_expired', lang=lang))
                return ConversationHandler.END
            else:
                context.dispatcher.run_async(_show_menu_async, update, context, build_client_menu)
                return ConversationHandler.END
        else:
            context.dispatcher.run_async(_send_or_edit_message, update, context, get_text(user_id, 'welcome', lang=lang))
            return STATE_WAITING_FOR_CODE

async def process_invitation_code(update: Update, context: CallbackContext) -> str | int:
    user_id, lang = get_user_id_and_lang(update, context); code = update.message.text.strip().lower()
    log.info(f"UserID={user_id} submitted code: {code}")
    if not re.fullmatch(r'[a-f0-9]{8}', code): await _send_or_edit_message(update, context, get_text(user_id, 'invalid_code_format', lang=lang)); return STATE_WAITING_FOR_CODE
    success, status_key = db.activate_client(code, user_id); text_to_send = get_text(user_id, status_key, lang=lang, code=code)
    if success:
        log.info(f"Activated client {user_id} code {code}"); db.log_event_db("Client Activated", f"Code: {code}", user_id=user_id)
        context.user_data[CTX_LANG] = db.get_user_language(user_id); await _send_or_edit_message(update, context, text_to_send)
        await _show_menu_async(update, context, build_client_menu); return ConversationHandler.END
    else:
        log.warning(f"Failed activation user {user_id} code {code}: {status_key}"); await _send_or_edit_message(update, context, text_to_send)
        clear_conversation_data(context); return ConversationHandler.END

def admin_command(update: Update, context: CallbackContext) -> str | int:
    user_id, lang = get_user_id_and_lang(update, context); clear_conversation_data(context)
    log.info(f"Admin cmd: UserID={user_id}, User={update.effective_user.username}")
    if not is_admin(user_id): context.dispatcher.run_async(_send_or_edit_message, update, context, get_text(user_id, 'unauthorized', lang=lang)); return ConversationHandler.END
    context.dispatcher.run_async(_show_menu_async, update, context, build_admin_menu); return ConversationHandler.END

def cancel_command(update: Update, context: CallbackContext) -> int:
    user_id, lang = get_user_id_and_lang(update, context); log.info(f"Cancel cmd: UserID={user_id}")
    current_state = context.user_data.get(ConversationHandler.CURRENT_STATE if hasattr(ConversationHandler, 'CURRENT_STATE') else '_user_data') if context.user_data else 'None'
    log.debug(f"Cancel called from state: {current_state}"); clear_conversation_data(context)
    context.dispatcher.run_async(_send_or_edit_message, update, context, get_text(user_id, 'cancelled', lang=lang))
    if is_admin(user_id): context.dispatcher.run_async(_show_menu_async, update, context, build_admin_menu)
    elif db.find_client_by_user_id(user_id): context.dispatcher.run_async(_show_menu_async, update, context, build_client_menu)
    return ConversationHandler.END

def conversation_fallback(update: Update, context: CallbackContext) -> int:
     user_id, lang = get_user_id_and_lang(update, context)
     state = context.user_data.get(ConversationHandler.CURRENT_STATE if hasattr(ConversationHandler, 'CURRENT_STATE') else '_user_data') if context.user_data else 'None'
     msg_text = update.effective_message.text if update.effective_message else 'Non-text update'
     log.warning(f"Conv fallback: UserID={user_id}. State={state}. Msg='{msg_text[:50]}...'")
     if update.message and update.message.text and update.message.text.startswith('/'):
         command = update.message.text
         if command == '/cancel': return cancel_command(update, context)
         if command == '/start': context.dispatcher.run_async(_send_or_edit_message, update, context, get_text(user_id, 'state_cleared', lang=lang)); return start_command(update, context)
         if command == '/admin' and is_admin(user_id): context.dispatcher.run_async(_send_or_edit_message, update, context, get_text(user_id, 'state_cleared', lang=lang)); return admin_command(update, context)
     context.dispatcher.run_async(_send_or_edit_message, update, context, get_text(user_id, 'conversation_fallback', lang=lang))
     clear_conversation_data(context); return ConversationHandler.END

async def client_menu(update: Update, context: CallbackContext) -> int:
    user_id, lang = get_user_id_and_lang(update, context)
    if update.callback_query: clear_conversation_data(context); lang = context.user_data.get(CTX_LANG, 'en')
    await _show_menu_async(update, context, build_client_menu); return ConversationHandler.END

async def client_ask_select_language(update: Update, context: CallbackContext) -> int:
    user_id, lang = get_user_id_and_lang(update, context); buttons = []; row = []; sorted_languages = sorted(language_names.items(), key=lambda item: item[1])
    for code, name in sorted_languages: row.append(InlineKeyboardButton(name, callback_data=f"{CALLBACK_LANG_PREFIX}{code}"));
    if len(row) >= 2: buttons.append(row); row = []
    if row: buttons.append(row)
    buttons.append([InlineKeyboardButton(get_text(user_id, 'button_back', lang=lang), callback_data=f"{CALLBACK_CLIENT_PREFIX}back_to_menu")]); markup = InlineKeyboardMarkup(buttons)
    await _send_or_edit_message(update, context, get_text(user_id, 'select_language', lang=lang), reply_markup=markup); return ConversationHandler.END

async def set_language_handler(update: Update, context: CallbackContext) -> int:
    query = update.callback_query; user_id = query.from_user.id; lang_code = query.data.split(CALLBACK_LANG_PREFIX)[1]; current_lang = context.user_data.get(CTX_LANG, 'en')
    async def answer_callback_task(text=None, show_alert=False):
        try:
             query_id = query.id
             if not context.bot_data.get(f'answered_{query_id}', False): await query.answer(text, show_alert=show_alert); context.bot_data[f'answered_{query_id}'] = True
        except Exception as e: log.debug(f"Failed to answer language CB: {e}")
    if lang_code not in language_names: await answer_callback_task(get_text(user_id, 'error_invalid_input', lang=current_lang), show_alert=True); return ConversationHandler.END
    if db.set_user_language(user_id, lang_code):
         context.user_data[CTX_LANG] = lang_code; lang = lang_code
         success_text = get_text(user_id, 'language_set', lang=lang, lang_name=language_names[lang_code])
         keyboard = [[ InlineKeyboardButton(get_text(user_id, 'button_main_menu', lang=lang), callback_data=f"{CALLBACK_CLIENT_PREFIX}back_to_menu")]]
         markup = InlineKeyboardMarkup(keyboard)
         await _send_or_edit_message(update, context, success_text, reply_markup=markup)
         await answer_callback_task()
    else: await answer_callback_task(get_text(user_id, 'language_set_error', lang=current_lang), show_alert=True)
    return ConversationHandler.END

async def process_admin_phone(update: Update, context: CallbackContext) -> str | int:
     user_id, lang = get_user_id_and_lang(update, context); phone_raw = update.message.text.strip()
     if not re.fullmatch(r'\+\d{9,15}', phone_raw): await _send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_invalid_phone', lang=lang)); return STATE_WAITING_FOR_PHONE
     phone = phone_raw; context.user_data[CTX_PHONE] = phone; log.info(f"Admin {user_id} entered phone: {phone}")
     await _send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_prompt_api_id', lang=lang)); return STATE_WAITING_FOR_API_ID

async def process_admin_api_id(update: Update, context: CallbackContext) -> str | int:
    user_id, lang = get_user_id_and_lang(update, context); api_id_str = update.message.text.strip()
    try:
        api_id = int(api_id_str)
        if api_id <= 0: raise ValueError("API ID must be positive") # Check positivity
        context.user_data[CTX_API_ID] = api_id
        log.info(f"Admin {user_id} API ID OK for {context.user_data.get(CTX_PHONE)}")
    except (ValueError, TypeError):
        await _send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_invalid_api_id', lang=lang))
        return STATE_WAITING_FOR_API_ID
    await _send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_prompt_api_hash', lang=lang))
    return STATE_WAITING_FOR_API_HASH

async def process_admin_api_hash(update: Update, context: CallbackContext) -> str | int:
    user_id, lang = get_user_id_and_lang(update, context); api_hash = update.message.text.strip()
    if not api_hash or len(api_hash) < 30 or not re.match('^[a-fA-F0-9]+$', api_hash): await _send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_invalid_api_hash', lang=lang)); return STATE_WAITING_FOR_API_HASH
    context.user_data[CTX_API_HASH] = api_hash; phone = context.user_data.get(CTX_PHONE); api_id = context.user_data.get(CTX_API_ID)
    if not phone or not api_id: await _send_or_edit_message(update, context, get_text(user_id, 'session_expired', lang=lang)); clear_conversation_data(context); return ConversationHandler.END
    log.info(f"Admin {user_id} API Hash OK for {phone}. Starting authentication flow.")
    await _send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_auth_connecting', lang=lang, phone=html.escape(phone)))
    try:
        auth_status, auth_data = await telethon_api.start_authentication_flow(phone, api_id, api_hash); log.info(f"Authentication start result for {phone}: Status='{auth_status}'")
        if auth_status == 'code_needed': context.user_data[CTX_AUTH_DATA] = auth_data; await _send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_prompt_code', lang=lang, phone=html.escape(phone))); return STATE_WAITING_FOR_CODE_USERBOT
        elif auth_status == 'password_needed': context.user_data[CTX_AUTH_DATA] = auth_data; await _send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_prompt_password', lang=lang, phone=html.escape(phone))); return STATE_WAITING_FOR_PASSWORD
        elif auth_status == 'already_authorized':
             log.warning(f"Userbot {phone} is already authorized.");
             if not db.find_userbot(phone): safe_phone_part = re.sub(r'[^\d]', '', phone); session_file_rel = f"{safe_phone_part or f'unknown_{random.randint(1000,9999)}'}.session"; db.add_userbot(phone, session_file_rel, api_id, api_hash, 'active')
             else: db.update_userbot_status(phone, 'active')
             context.dispatcher.run_async(telethon_api.get_userbot_runtime_info, phone)
             await _send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_already_auth', lang=lang, display_name=html.escape(phone))); clear_conversation_data(context); return ConversationHandler.END
        else:
            error_msg = auth_data.get('error_message', 'Unknown error'); log.error(f"Auth start error for {phone}: {error_msg}"); locals_for_format = {'phone': html.escape(phone), 'error': html.escape(error_msg)}; key = 'admin_userbot_auth_error_unknown'
            if "flood wait" in error_msg.lower(): key = 'admin_userbot_auth_error_flood'; locals_for_format['seconds'] = re.search(r'\d+', error_msg).group(0) if re.search(r'\d+', error_msg) else '?'
            elif "config" in error_msg.lower() or "invalid api" in error_msg.lower(): key = 'admin_userbot_auth_error_config'
            elif "invalid phone" in error_msg.lower(): key = 'admin_userbot_auth_error_phone_invalid'
            elif "connection" in error_msg.lower(): key = 'admin_userbot_auth_error_connect'
            await _send_or_edit_message(update, context, get_text(user_id, key, lang=lang, **locals_for_format)); clear_conversation_data(context); return ConversationHandler.END
    except Exception as e: log.error(f"Exception during start_authentication_flow for {phone}: {e}", exc_info=True); await _send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_auth_error_unknown', lang=lang, phone=html.escape(phone), error=html.escape(str(e)))); clear_conversation_data(context); return ConversationHandler.END

async def process_admin_userbot_code(update: Update, context: CallbackContext) -> str | int:
    user_id, lang = get_user_id_and_lang(update, context); code = update.message.text.strip(); auth_data = context.user_data.get(CTX_AUTH_DATA)
    phone = context.user_data.get(CTX_PHONE, "N/A"); api_id = context.user_data.get(CTX_API_ID); api_hash = context.user_data.get(CTX_API_HASH)
    if not auth_data: await _send_or_edit_message(update, context, get_text(user_id, 'session_expired', lang=lang)); clear_conversation_data(context); return ConversationHandler.END
    if not code.isdigit(): await _send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_auth_error_code_invalid', lang=lang, phone=html.escape(phone), error="Format incorrect")); return STATE_WAITING_FOR_CODE_USERBOT
    await _send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_auth_signing_in', lang=lang, phone=html.escape(phone)))
    try:
        comp_status, comp_data = await telethon_api.complete_authentication_flow(auth_data, code=code); log.info(f"Authentication code complete result for {phone}: Status='{comp_status}'")
        context.user_data.pop(CTX_AUTH_DATA, None)
        if comp_status == 'success':
            phone_num = comp_data.get('phone', phone); username = comp_data.get('username'); display_name = f"@{username}" if username else phone_num
            await _send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_add_success', lang=lang, display_name=html.escape(display_name)))
            context.dispatcher.run_async(telethon_api.get_userbot_runtime_info, phone_num); clear_conversation_data(context); return ConversationHandler.END
        elif comp_status == 'error' and "Password required" in comp_data.get('error_message','').lower(): await _send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_auth_error_password_needed_unexpected', lang=lang)); clear_conversation_data(context); return ConversationHandler.END
        else:
            error_msg = comp_data.get('error_message', 'Unknown error.'); log.error(f"Auth code completion error for {phone}: {error_msg}"); locals_for_format = {'phone': html.escape(phone), 'error': html.escape(error_msg)}; key = 'admin_userbot_auth_error_unknown'
            if "invalid or expired code" in error_msg.lower(): key = 'admin_userbot_auth_error_code_invalid'
            elif "flood wait" in error_msg.lower(): key = 'admin_userbot_auth_error_flood'; locals_for_format['seconds'] = re.search(r'\d+', error_msg).group(0) if re.search(r'\d+', error_msg) else '?'
            elif "banned" in error_msg.lower() or "deactivated" in error_msg.lower(): key = 'admin_userbot_auth_error_account_issue'
            elif "connection" in error_msg.lower(): key = 'admin_userbot_auth_error_connect'
            await _send_or_edit_message(update, context, get_text(user_id, key, lang=lang, **locals_for_format)); clear_conversation_data(context); return ConversationHandler.END
    except Exception as e: log.error(f"Exception during complete_authentication_flow (code) for {phone}: {e}", exc_info=True); context.user_data.pop(CTX_AUTH_DATA, None); await _send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_auth_error_unknown', lang=lang, phone=html.escape(phone), error=html.escape(str(e)))); clear_conversation_data(context); return ConversationHandler.END

async def process_admin_userbot_password(update: Update, context: CallbackContext) -> str | int:
    user_id, lang = get_user_id_and_lang(update, context); password = update.message.text.strip(); auth_data = context.user_data.get(CTX_AUTH_DATA)
    phone = context.user_data.get(CTX_PHONE, "N/A"); api_id = context.user_data.get(CTX_API_ID); api_hash = context.user_data.get(CTX_API_HASH)
    if not auth_data: await _send_or_edit_message(update, context, get_text(user_id, 'session_expired', lang=lang)); clear_conversation_data(context); return ConversationHandler.END
    if not password: await _send_or_edit_message(update, context, get_text(user_id, 'error_invalid_input', lang=lang)); return STATE_WAITING_FOR_PASSWORD
    await _send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_auth_signing_in', lang=lang, phone=html.escape(phone)))
    try:
        comp_status, comp_data = await telethon_api.complete_authentication_flow(auth_data, password=password); log.info(f"Authentication password complete result for {phone}: Status='{comp_status}'")
        context.user_data.pop(CTX_AUTH_DATA, None)
        if comp_status == 'success':
            phone_num = comp_data.get('phone', phone); username = comp_data.get('username'); display_name = f"@{username}" if username else phone_num
            await _send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_add_success', lang=lang, display_name=html.escape(display_name)))
            context.dispatcher.run_async(telethon_api.get_userbot_runtime_info, phone_num); clear_conversation_data(context); return ConversationHandler.END
        else:
            error_msg = comp_data.get('error_message', 'Unknown error.'); log.error(f"Auth password completion error for {phone}: {error_msg}"); locals_for_format = {'phone': html.escape(phone), 'error': html.escape(error_msg)}; key = 'admin_userbot_auth_error_unknown'
            if "incorrect password" in error_msg.lower() or "password_hash_invalid" in error_msg.lower(): key = 'admin_userbot_auth_error_password_invalid'
            elif "flood wait" in error_msg.lower(): key = 'admin_userbot_auth_error_flood'; locals_for_format['seconds'] = re.search(r'\d+', error_msg).group(0) if re.search(r'\d+', error_msg) else '?'
            elif "banned" in error_msg.lower() or "deactivated" in error_msg.lower(): key = 'admin_userbot_auth_error_account_issue'
            elif "connection" in error_msg.lower(): key = 'admin_userbot_auth_error_connect'
            await _send_or_edit_message(update, context, get_text(user_id, key, lang=lang, **locals_for_format)); clear_conversation_data(context); return ConversationHandler.END
    except Exception as e: log.error(f"Exception during complete_authentication_flow (password) for {phone}: {e}", exc_info=True); context.user_data.pop(CTX_AUTH_DATA, None); await _send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_auth_error_unknown', lang=lang, phone=html.escape(phone), error=html.escape(str(e)))); clear_conversation_data(context); return ConversationHandler.END

async def process_admin_invite_details(update: Update, context: CallbackContext) -> int:
    user_id, lang = get_user_id_and_lang(update, context); details_text = update.message.text.strip().lower(); match = re.match(r'(\d+)\s*d\s+(\d+)\s*b', details_text)
    if not match: await _send_or_edit_message(update, context, get_text(user_id, 'admin_invite_invalid_format', lang=lang)); return STATE_WAITING_FOR_SUB_DETAILS
    try:
        days = int(match.group(1))
        bots_needed = int(match.group(2))
        if days <= 0 or bots_needed <= 0:
            raise ValueError("Days and bots must be positive") # More specific error for the raise
    except (ValueError, AssertionError):
        await _send_or_edit_message(update, context, get_text(user_id, 'admin_invite_invalid_numbers', lang=lang)); return STATE_WAITING_FOR_SUB_DETAILS
    await _send_or_edit_message(update, context, get_text(user_id, 'admin_invite_generating', lang=lang)); code = str(uuid.uuid4().hex)[:8]
    end_datetime = datetime.now(UTC_TZ) + timedelta(days=days); sub_end_ts = int(end_datetime.timestamp())
    if db.create_invitation(code, sub_end_ts): end_date_str = format_dt(sub_end_ts, fmt='%Y-%m-%d %H:%M UTC'); db.log_event_db("Invite Generated", f"Code: {code}, Days: {days}, Bot Count: {bots_needed}", user_id=user_id); await _send_or_edit_message(update, context, get_text(user_id, 'admin_invite_success', lang=lang, code=code, end_date=end_date_str, count=bots_needed))
    else: db.log_event_db("Invite Gen Failed", f"Code: {code} (duplicate?)", user_id=user_id); await _send_or_edit_message(update, context, get_text(user_id, 'admin_invite_db_error', lang=lang))
    clear_conversation_data(context); return ConversationHandler.END

async def process_admin_extend_code(update: Update, context: CallbackContext) -> int:
    user_id, lang = get_user_id_and_lang(update, context); code = update.message.text.strip().lower(); client = db.find_client_by_code(code)
    if not client: await _send_or_edit_message(update, context, get_text(user_id, 'admin_extend_invalid_code', lang=lang)); return STATE_WAITING_FOR_EXTEND_CODE
    context.user_data[CTX_EXTEND_CODE] = code; end_date_str = format_dt(client['subscription_end'])
    await _send_or_edit_message(update, context, get_text(user_id, 'admin_extend_prompt_days', lang=lang, code=html.escape(code), end_date=end_date_str)); return STATE_WAITING_FOR_EXTEND_DAYS

async def process_admin_extend_days(update: Update, context: CallbackContext) -> int:
    user_id, lang = get_user_id_and_lang(update, context); days_str = update.message.text.strip(); code = context.user_data.get(CTX_EXTEND_CODE)
    if not code: await _send_or_edit_message(update, context, get_text(user_id, 'session_expired', lang=lang)); clear_conversation_data(context); return ConversationHandler.END
    try: days_to_add = int(days_str); if days_to_add <= 0: raise ValueError("Days must be positive")
    except (ValueError, AssertionError): await _send_or_edit_message(update, context, get_text(user_id, 'admin_extend_invalid_days', lang=lang)); return STATE_WAITING_FOR_EXTEND_DAYS
    client = db.find_client_by_code(code)
    if not client: await _send_or_edit_message(update, context, get_text(user_id, 'admin_extend_invalid_code', lang=lang)); clear_conversation_data(context); return ConversationHandler.END
    current_end_ts = client['subscription_end']; now_ts = int(datetime.now(UTC_TZ).timestamp()); start_ts = max(now_ts, current_end_ts)
    start_dt = datetime.fromtimestamp(start_ts, UTC_TZ); new_end_dt = start_dt + timedelta(days=days_to_add); new_end_ts = int(new_end_dt.timestamp())
    if db.extend_subscription(code, new_end_ts): new_end_date_str = format_dt(new_end_ts); db.log_event_db("Subscription Extended", f"Code: {code}, Added: {days_to_add} days", user_id=user_id, client_id=client.get('user_id')); await _send_or_edit_message(update, context, get_text(user_id, 'admin_extend_success', lang=lang, code=html.escape(code), days=days_to_add, new_end_date=new_end_date_str))
    else: db.log_event_db("Sub Extend Failed", f"Code: {code}", user_id=user_id); await _send_or_edit_message(update, context, get_text(user_id, 'admin_extend_db_error', lang=lang))
    clear_conversation_data(context); return ConversationHandler.END

async def process_admin_add_bots_code(update: Update, context: CallbackContext) -> int:
    user_id, lang = get_user_id_and_lang(update, context); code = update.message.text.strip().lower(); client = db.find_client_by_code(code)
    if not client: await _send_or_edit_message(update, context, get_text(user_id, 'admin_assignbots_invalid_code', lang=lang)); return STATE_WAITING_FOR_ADD_USERBOTS_CODE
    context.user_data[CTX_ADD_BOTS_CODE] = code; current_bots = [b for b in db.get_all_userbots(assigned_status=True) if b['assigned_client'] == code]; current_count = len(current_bots)
    await _send_or_edit_message(update, context, get_text(user_id, 'admin_assignbots_prompt_count', lang=lang, code=html.escape(code), current_count=current_count)); return STATE_WAITING_FOR_ADD_USERBOTS_COUNT

async def process_admin_add_bots_count(update: Update, context: CallbackContext) -> int:
    user_id, lang = get_user_id_and_lang(update, context); count_str = update.message.text.strip(); code = context.user_data.get(CTX_ADD_BOTS_CODE)
    if not code: await _send_or_edit_message(update, context, get_text(user_id, 'session_expired', lang=lang)); clear_conversation_data(context); return ConversationHandler.END
    try: count_to_add = int(count_str); if count_to_add <= 0: raise ValueError("Count must be positive")
    except (ValueError, AssertionError): await _send_or_edit_message(update, context, get_text(user_id, 'admin_assignbots_invalid_count', lang=lang)); return STATE_WAITING_FOR_ADD_USERBOTS_COUNT
    available_bots = db.get_unassigned_userbots(limit=count_to_add)
    if len(available_bots) < count_to_add: await _send_or_edit_message(update, context, get_text(user_id, 'admin_assignbots_no_bots_available', lang=lang, needed=count_to_add, available=len(available_bots))); return STATE_WAITING_FOR_ADD_USERBOTS_COUNT
    success, message = db.assign_userbots_to_client(code, available_bots)
    if success:
        client_user_id = db.find_client_by_code(code)['user_id'] if db.find_client_by_code(code) else None; assigned_count = len(available_bots)
        final_message = message
        if "Successfully assigned" in message:
            assigned_match = re.search(r"Successfully assigned (\d+)", message); actually_assigned = int(assigned_match.group(1)) if assigned_match else assigned_count
            if actually_assigned == assigned_count and "Failed:" not in message: final_message = get_text(user_id, 'admin_assignbots_success', lang=lang, count=assigned_count, code=html.escape(code))
            else: final_message = get_text(user_id, 'admin_assignbots_partial_success', lang=lang, assigned_count=actually_assigned, requested_count=assigned_count, code=html.escape(code)); final_message += f"\nDetails: {html.escape(message)}"
        await _send_or_edit_message(update, context, final_message); db.log_event_db("Userbots Assigned", f"Code: {code}, Attempted: {assigned_count}, Details: {message}", user_id=user_id, client_id=client_user_id)
        for phone in available_bots: context.dispatcher.run_async(telethon_api.get_userbot_runtime_info, phone)
    else: db.log_event_db("Bot Assign Failed", f"Code: {code}, Reason: {message}", user_id=user_id); fail_message = get_text(user_id, 'admin_assignbots_failed', lang=lang, code=html.escape(code)) + f"\nError: {html.escape(message)}"; await _send_or_edit_message(update, context, fail_message)
    clear_conversation_data(context); return ConversationHandler.END

async def client_folder_menu(update: Update, context: CallbackContext) -> int:
    user_id, lang = get_user_id_and_lang(update, context)
    if update.callback_query: clear_conversation_data(context); lang = context.user_data.get(CTX_LANG, 'en')
    await _show_menu_async(update, context, build_folder_menu); return ConversationHandler.END

def build_folder_menu(user_id, context: CallbackContext):
    lang = context.user_data.get(CTX_LANG, 'en') if context.user_data else 'en'
    folders = db.get_folders_by_user(user_id); text = get_text(user_id, 'folder_menu_title', lang=lang); keyboard = []
    keyboard.append([InlineKeyboardButton(get_text(user_id, 'folder_menu_create', lang=lang), callback_data=f"{CALLBACK_FOLDER_PREFIX}create_prompt")])
    if folders: keyboard.append([InlineKeyboardButton(get_text(user_id, 'folder_menu_edit', lang=lang), callback_data=f"{CALLBACK_FOLDER_PREFIX}select_edit?page=0")]); keyboard.append([InlineKeyboardButton(get_text(user_id, 'folder_menu_delete', lang=lang), callback_data=f"{CALLBACK_FOLDER_PREFIX}select_delete?page=0")])
    else: text += "\n" + get_text(user_id, 'folder_no_folders', lang=lang)
    keyboard.append([InlineKeyboardButton(get_text(user_id, 'button_back', lang=lang), callback_data=f"{CALLBACK_CLIENT_PREFIX}back_to_menu")]); markup = InlineKeyboardMarkup(keyboard);
    return text, markup, ParseMode.HTML

async def process_folder_name(update: Update, context: CallbackContext) -> int:
    user_id, lang = get_user_id_and_lang(update, context); folder_name = update.message.text.strip()
    if not folder_name: await _send_or_edit_message(update, context, get_text(user_id, 'error_invalid_input', lang=lang)); return STATE_WAITING_FOR_FOLDER_NAME
    log.info(f"User {user_id} attempting to create folder: {folder_name}"); folder_id_or_status = db.add_folder(folder_name, user_id)
    if isinstance(folder_id_or_status, int) and folder_id_or_status > 0: folder_id = folder_id_or_status; db.log_event_db("Folder Created", f"Name: {folder_name}, ID: {folder_id}", user_id=user_id); await _send_or_edit_message(update, context, get_text(user_id, 'folder_create_success', lang=lang, name=html.escape(folder_name))); return await client_folder_menu(update, context)
    elif folder_id_or_status is None: await _send_or_edit_message(update, context, get_text(user_id, 'folder_create_error_exists', lang=lang, name=html.escape(folder_name))); return STATE_WAITING_FOR_FOLDER_NAME
    else: db.log_event_db("Folder Create Failed", f"Name: {folder_name}", user_id=user_id); await _send_or_edit_message(update, context, get_text(user_id, 'folder_create_error_db', lang=lang)); clear_conversation_data(context); return ConversationHandler.END

async def client_select_folder_to_edit_or_delete(update: Update, context: CallbackContext, action: str) -> int:
    query = update.callback_query; user_id, lang = get_user_id_and_lang(update, context);
    try: _, page_data = query.data.split('?', 1); current_page = int(page_data.split('=')[1])
    except (ValueError, IndexError, AttributeError): current_page = 0
    folders = db.get_folders_by_user(user_id)
    if not folders: if query and hasattr(query, 'answer'): await query.answer(get_text(user_id, 'folder_no_folders', lang=lang), show_alert=True); return await client_folder_menu(update, context)
    total_items = len(folders); start_index = current_page * ITEMS_PER_PAGE; end_index = start_index + ITEMS_PER_PAGE; folders_page = folders[start_index:end_index]
    text_key = 'folder_select_edit' if action == 'edit' else 'folder_select_delete'; text = get_text(user_id, text_key, lang=lang); keyboard = []
    for folder in folders_page: button_text = html.escape(folder['name']); callback_action = "edit_selected" if action == 'edit' else "delete_selected"; callback_data = f"{CALLBACK_FOLDER_PREFIX}{callback_action}?id={folder['id']}"; keyboard.append([InlineKeyboardButton(button_text, callback_data=callback_data)])
    base_callback = f"{CALLBACK_FOLDER_PREFIX}select_{action}"; pagination_buttons = build_pagination_buttons(base_callback, current_page, total_items, ITEMS_PER_PAGE, lang=lang); keyboard.extend(pagination_buttons)
    keyboard.append([InlineKeyboardButton(get_text(user_id, 'button_back', lang=lang), callback_data=f"{CALLBACK_FOLDER_PREFIX}back_to_manage")]); markup = InlineKeyboardMarkup(keyboard)
    await _send_or_edit_message(update, context, text, reply_markup=markup); return STATE_WAITING_FOR_FOLDER_SELECTION

async def client_show_folder_edit_options(update: Update, context: CallbackContext) -> int:
    query = update.callback_query; user_id, lang = get_user_id_and_lang(update, context); folder_id = context.user_data.get(CTX_FOLDER_ID)
    if not folder_id and query and '?' in query.data:
         try: _, params = query.data.split('?', 1); folder_id = int(params.split('=')[1]); context.user_data[CTX_FOLDER_ID] = folder_id
         except (ValueError, IndexError): folder_id = None
    if not folder_id: log.error(f"Could not determine folder ID for edit options. User: {user_id}, Callback: {query.data if query else 'N/A'}");
    if query and hasattr(query, 'answer'): await query.answer(get_text(user_id, 'error_generic', lang=lang), show_alert=True); return await client_folder_menu(update, context)
    folder_name = db.get_folder_name(folder_id)
    if not folder_name:
        if query and hasattr(query, 'answer'): await query.answer(get_text(user_id, 'folder_not_found_error', lang=lang), show_alert=True); clear_conversation_data(context); return await client_folder_menu(update, context)
    context.user_data[CTX_FOLDER_NAME] = folder_name; groups_in_folder = db.get_target_groups_details_by_folder(folder_id)
    text = get_text(user_id, 'folder_edit_title', lang=lang, name=html.escape(folder_name)) + "\n" + get_text(user_id, 'folder_edit_groups_intro', lang=lang)
    if groups_in_folder:
        display_limit = 10
        for i, group in enumerate(groups_in_folder):
            if i >= display_limit: text += f"\n... and {len(groups_in_folder) - display_limit} more."; break
            link = group['group_link']; name = group['group_name'] or f"ID: {group['group_id']}"; escaped_name = html.escape(name)
            if link: escaped_link = html.escape(link); text += f"\n- <a href='{escaped_link}'>{escaped_name}</a>"
            else: text += f"\n- {escaped_name}"
    else: text += "\n" + get_text(user_id, 'folder_edit_no_groups', lang=lang)
    keyboard = [ [InlineKeyboardButton(get_text(user_id, 'folder_edit_action_add', lang=lang), callback_data=f"{CALLBACK_FOLDER_PREFIX}edit_add_prompt")], [InlineKeyboardButton(get_text(user_id, 'folder_edit_action_remove', lang=lang), callback_data=f"{CALLBACK_FOLDER_PREFIX}edit_remove_select?page=0")], [InlineKeyboardButton(get_text(user_id, 'folder_edit_action_rename', lang=lang), callback_data=f"{CALLBACK_FOLDER_PREFIX}edit_rename_prompt")], [InlineKeyboardButton(get_text(user_id, 'button_back', lang=lang), callback_data=f"{CALLBACK_FOLDER_PREFIX}back_to_manage")] ]; markup = InlineKeyboardMarkup(keyboard)
    await _send_or_edit_message(update, context, text, reply_markup=markup, disable_web_page_preview=True); return STATE_WAITING_FOR_FOLDER_ACTION

async def process_folder_links(update: Update, context: CallbackContext) -> int:
    user_id, lang = get_user_id_and_lang(update, context); folder_id = context.user_data.get(CTX_FOLDER_ID); folder_name = context.user_data.get(CTX_FOLDER_NAME)
    if not folder_id or not folder_name: await _send_or_edit_message(update, context, get_text(user_id, 'session_expired', lang=lang)); clear_conversation_data(context); return ConversationHandler.END
    links_text = update.message.text; raw_links = [link.strip() for link in links_text.splitlines() if link.strip()]
    if not raw_links: await _send_or_edit_message(update, context, get_text(user_id, 'join_no_links', lang=lang)); return STATE_WAITING_FOR_GROUP_LINKS
    await _send_or_edit_message(update, context, get_text(user_id, 'folder_processing_links', lang=lang))
    results = {}; added_count = 0; failed_count = 0; ignored_count = 0; client_bots = db.get_client_bots(user_id); resolver_bot_phone = None
    if client_bots: active_client_bots = [b for b in client_bots if (bot_info := db.find_userbot(b)) and bot_info['status'] == 'active'];
    if active_client_bots: resolver_bot_phone = random.choice(active_client_bots); log.info(f"Using bot {resolver_bot_phone} for resolving folder links.")
    link_details = {};
    if resolver_bot_phone:
        try: resolved_data = await telethon_api.resolve_links_info(resolver_bot_phone, raw_links); link_details.update(resolved_data); log.debug(f"Resolved {len(link_details)}/{len(raw_links)} links via bot {resolver_bot_phone}.")
        except Exception as resolve_e: log.error(f"Error resolving folder links via bot {resolver_bot_phone}: {resolve_e}")
    for link in raw_links:
        group_id = None; group_name = None; reason = None; status_code = 'failed'; resolved = link_details.get(link)
        if resolved and not resolved.get('error'):
            group_id = resolved.get('id'); group_name = resolved.get('name')
            if group_id:
                 added_status = db.add_target_group(group_id, group_name, link, user_id, folder_id)
                 if added_status is True: status_code = 'added'; added_count += 1
                 elif added_status is None: status_code = 'ignored'; ignored_count += 1; reason = 'Duplicate in folder'
                 else: status_code = 'failed'; reason = get_text(user_id, 'folder_add_db_error', lang=lang); failed_count += 1
            else: status_code = 'failed'; reason = get_text(user_id, 'folder_resolve_error', lang=lang) + " (No ID)"; failed_count += 1
        elif resolved and resolved.get('error'): status_code = 'failed'; reason = resolved.get('error'); failed_count += 1
        else: status_code = 'failed'; reason = get_text(user_id, 'folder_resolve_error', lang=lang); failed_count += 1
        results[link] = {'status': status_code, 'reason': reason}
    result_text = get_text(user_id, 'folder_results_title', lang=lang, name=html.escape(folder_name)) + f"\n(Added: {added_count}, Ignored: {ignored_count}, Failed: {failed_count})\n"
    display_limit = 20; displayed_count = 0
    for link, res in results.items():
        if displayed_count >= display_limit: result_text += f"\n...and {len(results) - displayed_count} more."; break
        status_key = f"folder_results_{res['status']}"; status_text = get_text(user_id, status_key, lang=lang)
        if res['status'] != 'added' and res['reason']: status_text += f" ({html.escape(str(res['reason']))})"
        result_text += "\n" + get_text(user_id, 'folder_results_line', lang=lang, link=html.escape(link), status=status_text); displayed_count += 1
    await _send_or_edit_message(update, context, result_text, disable_web_page_preview=True); return await client_show_folder_edit_options(update, context)

async def client_select_groups_to_remove(update: Update, context: CallbackContext) -> int:
    query = update.callback_query; user_id, lang = get_user_id_and_lang(update, context); folder_id = context.user_data.get(CTX_FOLDER_ID); folder_name = context.user_data.get(CTX_FOLDER_NAME)
    if not folder_id or not folder_name: await query.answer(get_text(user_id, 'session_expired', lang=lang), show_alert=True); return await client_folder_menu(update, context)
    try: _, params = query.data.split('?', 1); current_page = int(params.split('=')[1])
    except (ValueError, IndexError, AttributeError): current_page = 0
    groups = db.get_target_groups_details_by_folder(folder_id)
    if not groups: await query.answer(get_text(user_id, 'folder_edit_no_groups', lang=lang), show_alert=True); return await client_show_folder_edit_options(update, context)
    selected_ids = set(context.user_data.get(CTX_TARGET_GROUP_IDS_TO_REMOVE, [])); total_items = len(groups); start_index = current_page * ITEMS_PER_PAGE
    end_index = start_index + ITEMS_PER_PAGE; groups_page = groups[start_index:end_index]
    text = get_text(user_id, 'folder_edit_remove_select', lang=lang, name=html.escape(folder_name)); keyboard = []
    for group in groups_page:
        db_id = group['id']; is_selected = db_id in selected_ids; prefix = "✅ " if is_selected else "➖ "; link_text = group['group_link'] or f"ID: {group['group_id']}"
        display_text = group['group_name'] or link_text; max_len = 40; truncated_text = display_text[:max_len] + ("..." if len(display_text) > max_len else "")
        button_text = prefix + html.escape(truncated_text); callback_data = f"{CALLBACK_FOLDER_PREFIX}edit_toggle_remove?id={db_id}&page={current_page}"; keyboard.append([InlineKeyboardButton(button_text, callback_data=callback_data)])
    base_callback = f"{CALLBACK_FOLDER_PREFIX}edit_remove_select"; pagination_buttons = build_pagination_buttons(base_callback, current_page, total_items, ITEMS_PER_PAGE, lang=lang); keyboard.extend(pagination_buttons)
    action_row = [];
    if selected_ids: confirm_text = get_text(user_id, 'folder_edit_remove_confirm_title', lang=lang) + f" ({len(selected_ids)})"; action_row.append(InlineKeyboardButton(confirm_text, callback_data=f"{CALLBACK_FOLDER_PREFIX}edit_remove_confirm"))
    action_row.append(InlineKeyboardButton(get_text(user_id, 'button_back', lang=lang), callback_data=f"{CALLBACK_FOLDER_PREFIX}back_to_edit_options")); keyboard.append(action_row)
    markup = InlineKeyboardMarkup(keyboard); await _send_or_edit_message(update, context, text, reply_markup=markup); return STATE_FOLDER_EDIT_REMOVE_SELECT

async def client_toggle_group_for_removal(update: Update, context: CallbackContext) -> int:
    query = update.callback_query; user_id, lang = get_user_id_and_lang(update, context)
    try:
        params_str = query.data.split('?', 1)[1]; params = dict(qc.split('=') for qc in params_str.split('&'))
        group_db_id = int(params['id']); current_page = int(params['page'])
    except (ValueError, IndexError, KeyError): log.error(f"Could not parse group ID/page from callback: {query.data}"); await query.answer(get_text(user_id, 'error_generic', lang=lang), show_alert=True); return STATE_FOLDER_EDIT_REMOVE_SELECT
    if CTX_TARGET_GROUP_IDS_TO_REMOVE not in context.user_data: context.user_data[CTX_TARGET_GROUP_IDS_TO_REMOVE] = set()
    if group_db_id in context.user_data[CTX_TARGET_GROUP_IDS_TO_REMOVE]: context.user_data[CTX_TARGET_GROUP_IDS_TO_REMOVE].remove(group_db_id)
    else: context.user_data[CTX_TARGET_GROUP_IDS_TO_REMOVE].add(group_db_id)
    query.data = f"{CALLBACK_FOLDER_PREFIX}edit_remove_select?page={current_page}"; return await client_select_groups_to_remove(update, context)

async def client_confirm_remove_selected_groups(update: Update, context: CallbackContext) -> int:
    query = update.callback_query; user_id, lang = get_user_id_and_lang(update, context); folder_id = context.user_data.get(CTX_FOLDER_ID); folder_name = context.user_data.get(CTX_FOLDER_NAME); ids_to_remove = list(context.user_data.get(CTX_TARGET_GROUP_IDS_TO_REMOVE, []))
    if not folder_id or not folder_name: await query.answer(get_text(user_id, 'session_expired', lang=lang), show_alert=True); return await client_folder_menu(update, context)
    if not ids_to_remove: await query.answer(get_text(user_id, 'folder_edit_remove_none_selected', lang=lang), show_alert=True); query.data = f"{CALLBACK_FOLDER_PREFIX}edit_remove_select?page=0"; return await client_select_groups_to_remove(update, context)
    removed_count = db.remove_target_groups_by_db_id(ids_to_remove, user_id)
    if removed_count >= 0: db.log_event_db("Folder Groups Removed", f"Folder: {folder_name}({folder_id}), Count: {removed_count}, IDs: {ids_to_remove}", user_id=user_id); await _send_or_edit_message(update, context, get_text(user_id, 'folder_edit_remove_success', lang=lang, count=removed_count, name=html.escape(folder_name))); context.user_data.pop(CTX_TARGET_GROUP_IDS_TO_REMOVE, None); return await client_show_folder_edit_options(update, context)
    else: db.log_event_db("Folder Group Remove Failed", f"Folder: {folder_name}({folder_id})", user_id=user_id); await _send_or_edit_message(update, context, get_text(user_id, 'folder_edit_remove_error', lang=lang)); context.user_data.pop(CTX_TARGET_GROUP_IDS_TO_REMOVE, None); return await client_show_folder_edit_options(update, context)

async def process_folder_rename(update: Update, context: CallbackContext) -> int:
    user_id, lang = get_user_id_and_lang(update, context); new_name = update.message.text.strip(); folder_id = context.user_data.get(CTX_FOLDER_ID); current_name = context.user_data.get(CTX_FOLDER_NAME)
    if not folder_id or not current_name: await _send_or_edit_message(update, context, get_text(user_id, 'session_expired', lang=lang)); clear_conversation_data(context); return ConversationHandler.END
    if not new_name: await _send_or_edit_message(update, context, get_text(user_id, 'error_invalid_input', lang=lang)); return STATE_FOLDER_RENAME_PROMPT
    if new_name == current_name: return await client_show_folder_edit_options(update, context)
    success, reason = db.rename_folder(folder_id, user_id, new_name)
    if success: db.log_event_db("Folder Renamed", f"ID: {folder_id}, From: {current_name}, To: {new_name}", user_id=user_id); await _send_or_edit_message(update, context, get_text(user_id, 'folder_edit_rename_success', lang=lang, new_name=html.escape(new_name))); context.user_data[CTX_FOLDER_NAME] = new_name; return await client_show_folder_edit_options(update, context)
    else:
        if reason == "name_exists": await _send_or_edit_message(update, context, get_text(user_id, 'folder_edit_rename_error_exists', lang=lang, new_name=html.escape(new_name))); return STATE_FOLDER_RENAME_PROMPT
        else: db.log_event_db("Folder Rename Failed", f"ID: {folder_id}, To: {new_name}, Reason: {reason}", user_id=user_id); await _send_or_edit_message(update, context, get_text(user_id, 'folder_edit_rename_error_db', lang=lang)); return await client_show_folder_edit_options(update, context)

async def client_confirm_folder_delete(update: Update, context: CallbackContext) -> int:
    query = update.callback_query; user_id, lang = get_user_id_and_lang(update, context)
    try: _, params = query.data.split('?', 1); folder_id = int(params.split('=')[1])
    except (ValueError, IndexError, AttributeError): log.error(f"Could not parse folder ID for delete confirm: {query.data}"); await query.answer(get_text(user_id, 'error_generic', lang=lang), show_alert=True); return await client_folder_menu(update, context)
    folder_name = db.get_folder_name(folder_id)
    if not folder_name: await query.answer(get_text(user_id, 'folder_not_found_error', lang=lang), show_alert=True); return await client_folder_menu(update, context)
    text = get_text(user_id, 'folder_delete_confirm', lang=lang, name=html.escape(folder_name))
    keyboard = [[ InlineKeyboardButton(get_text(user_id, 'button_yes', lang=lang), callback_data=f"{CALLBACK_FOLDER_PREFIX}delete_confirmed?id={folder_id}"), InlineKeyboardButton(get_text(user_id, 'button_no', lang=lang), callback_data=f"{CALLBACK_FOLDER_PREFIX}back_to_manage") ]]
    markup = InlineKeyboardMarkup(keyboard); await _send_or_edit_message(update, context, text, reply_markup=markup); return STATE_WAITING_FOR_FOLDER_SELECTION

async def client_delete_folder_confirmed(update: Update, context: CallbackContext) -> int:
    query = update.callback_query; user_id, lang = get_user_id_and_lang(update, context)
    try: _, params = query.data.split('?', 1); folder_id = int(params.split('=')[1])
    except (ValueError, IndexError, AttributeError): log.error(f"Could not parse folder ID for delete confirmed: {query.data}"); await query.answer(get_text(user_id, 'error_generic', lang=lang), show_alert=True); return await client_folder_menu(update, context)
    folder_name = db.get_folder_name(folder_id)
    if db.delete_folder(folder_id, user_id): log.info(f"User {user_id} deleted folder ID {folder_id} (Name: {folder_name})"); await _send_or_edit_message(update, context, get_text(user_id, 'folder_delete_success', lang=lang, name=html.escape(folder_name or '')))
    else: log.warning(f"Failed delete folder ID {folder_id} by user {user_id}"); await _send_or_edit_message(update, context, get_text(user_id, 'folder_delete_error', lang=lang))
    return await client_folder_menu(update, context)

async def client_select_bot_generic(update: Update, context: CallbackContext, action_prefix: str, next_state: str, title_key: str) -> int | None:
    query = update.callback_query; user_id, lang = get_user_id_and_lang(update, context); user_bots = db.get_client_bots(user_id)
    if not user_bots: await query.answer(get_text(user_id, 'client_menu_no_userbots', lang=lang), show_alert=True); return await client_menu(update, context)
    keyboard = []; text = get_text(user_id, title_key, lang=lang); active_bots_count = 0; all_assigned_bots = []
    for phone in user_bots:
        bot_db_info = db.find_userbot(phone);
        if bot_db_info: all_assigned_bots.append(bot_db_info);
        if bot_db_info and bot_db_info['status'] == 'active': active_bots_count += 1
    if action_prefix == CALLBACK_JOIN_PREFIX:
        keyboard.append([InlineKeyboardButton(get_text(user_id, 'join_select_userbot_all', lang=lang), callback_data=f"{action_prefix}select_all")])
        if active_bots_count > 0 : keyboard.append([InlineKeyboardButton(get_text(user_id, 'join_select_userbot_active', lang=lang, count=active_bots_count), callback_data=f"{action_prefix}select_active")])
    for bot_db_info in all_assigned_bots:
        phone = bot_db_info['phone_number']; username = bot_db_info['username']; display_name = f"@{username}" if username else phone; status = bot_db_info['status']
        status_icon = "⚪️";
        if status == 'active': status_icon = "🟢"
        elif status == 'error': status_icon = "🔴"
        elif status in ['connecting', 'authenticating', 'initializing']: status_icon = "⏳"
        elif status in ['needs_code', 'needs_password']: status_icon = "⚠️"
        button_text = f"{status_icon} {html.escape(display_name)}"; keyboard.append([InlineKeyboardButton(button_text, callback_data=f"{action_prefix}select_{phone}")])
    keyboard.append([InlineKeyboardButton(get_text(user_id, 'button_back', lang=lang), callback_data=f"{CALLBACK_CLIENT_PREFIX}back_to_menu")]); markup = InlineKeyboardMarkup(keyboard)
    await _send_or_edit_message(update, context, text, reply_markup=markup); return STATE_WAITING_FOR_USERBOT_SELECTION

async def handle_userbot_selection(update: Update, context: CallbackContext, action_prefix: str, next_state: str) -> int | None:
    query = update.callback_query; user_id, lang = get_user_id_and_lang(update, context); data = query.data; selected_option = data.split(f"{action_prefix}select_")[1]
    selected_bots = []
    if selected_option == 'all':
        selected_bots = db.get_client_bots(user_id);
        if not selected_bots: await query.answer(get_text(user_id, 'client_menu_no_userbots', lang=lang), show_alert=True); return await client_menu(update, context)
    elif selected_option == 'active':
        all_client_bots = db.get_client_bots(user_id); selected_bots = [p for p in all_client_bots if (b := db.find_userbot(p)) and b['status'] == 'active'];
        if not selected_bots: await query.answer(get_text(user_id, 'join_no_active_bots', lang=lang), show_alert=True); return await client_select_bot_generic(update, context, action_prefix, next_state, 'join_select_userbot' if action_prefix == CALLBACK_JOIN_PREFIX else 'task_select_userbot')
    else:
        phone = selected_option; bot_info = db.find_userbot(phone);
        if not bot_info or phone not in db.get_client_bots(user_id): log.warning(f"User {user_id} tried to select unauthorized/invalid bot: {phone}"); await query.answer(get_text(user_id, 'error_invalid_input', lang=lang), show_alert=True); return STATE_WAITING_FOR_USERBOT_SELECTION
        selected_bots = [phone]
    context.user_data[CTX_SELECTED_BOTS] = selected_bots; log.info(f"User {user_id} selected bot(s): {selected_bots} for action {action_prefix}")
    if action_prefix == CALLBACK_JOIN_PREFIX: await _send_or_edit_message(update, context, get_text(user_id, 'join_enter_group_links', lang=lang)); return STATE_WAITING_FOR_GROUP_LINKS
    elif action_prefix == CALLBACK_TASK_PREFIX: context.user_data[CTX_TASK_PHONE] = selected_bots[0]; return await task_show_settings_menu(update, context)
    else: log.error(f"Unhandled action prefix in handle_userbot_selection: {action_prefix}"); clear_conversation_data(context); return ConversationHandler.END

async def process_join_group_links(update: Update, context: CallbackContext) -> int:
    user_id, lang = get_user_id_and_lang(update, context); selected_bots = context.user_data.get(CTX_SELECTED_BOTS)
    if not selected_bots: await _send_or_edit_message(update, context, get_text(user_id, 'session_expired', lang=lang)); clear_conversation_data(context); return ConversationHandler.END
    links_text = update.message.text; raw_links = [link.strip() for link in links_text.splitlines() if link.strip()]
    if not raw_links: await _send_or_edit_message(update, context, get_text(user_id, 'join_no_links', lang=lang)); return STATE_WAITING_FOR_GROUP_LINKS
    await _send_or_edit_message(update, context, get_text(user_id, 'join_processing', lang=lang))
    all_results_text = get_text(user_id, 'join_results_title', lang=lang); tasks = []
    for phone in selected_bots: tasks.append(telethon_api.join_groups_batch(phone, raw_links))
    results_list = await asyncio.gather(*tasks, return_exceptions=True)
    for i, result_item in enumerate(results_list):
        phone = selected_bots[i]; bot_db_info = db.find_userbot(phone); bot_display_name = html.escape(f"@{bot_db_info['username']}" if bot_db_info and bot_db_info['username'] else phone)
        all_results_text += "\n" + get_text(user_id, 'join_results_bot_header', lang=lang, display_name=bot_display_name)
        if isinstance(result_item, Exception): log.error(f"Join batch task for {phone} raised exception: {result_item}"); all_results_text += f"\n  -> {get_text(user_id, 'error_generic', lang=lang)} ({html.escape(str(result_item))})"; continue
        error_info, results_dict = result_item
        if error_info and error_info.get("error"): error_message_detail = error_info['error']; log.error(f"Join batch error for {phone}: {error_message_detail}"); generic_error_text = get_text(user_id, 'error_generic', lang=lang); all_results_text += f"\n  -> {generic_error_text} ({html.escape(error_message_detail)})"; continue
        if not results_dict: all_results_text += f"\n  -> ({get_text(user_id, 'error_no_results', lang=lang)})"; continue
        processed_links_count = 0
        for link, (status, detail) in results_dict.items():
             status_key = f"join_results_{status}"; status_text = get_text(user_id, status_key, lang=lang)
             if status not in ['success', 'already_member'] and isinstance(detail, dict):
                  reason = detail.get('reason'); error = detail.get('error'); seconds = detail.get('seconds'); reason_text = ""
                  if reason:
                      reason_key = f"join_results_reason_{reason}"; reason_base_text = get_text(user_id, reason_key, lang=lang)
                      if reason_base_text != reason_key: reason_text = reason_base_text.format(error=html.escape(str(error or '')), seconds=seconds or '')
                      else: reason_text = html.escape(str(reason));
                      if error: reason_text += f" ({html.escape(str(error))})"
                  elif error: reason_text = html.escape(str(error))
                  if reason_text: status_text = get_text(user_id, 'join_results_failed', lang=lang, reason=reason_text)
                  elif status == 'flood_wait' and seconds: status_text = get_text(user_id, 'join_results_flood_wait', lang=lang, seconds=seconds)
             escaped_link = html.escape(link); all_results_text += "\n" + get_text(user_id, 'join_results_line', lang=lang, url=escaped_link, status=status_text); processed_links_count +=1
             if len(all_results_text) > 3800: all_results_text += f"\n\n... (message truncated, {len(raw_links) - processed_links_count} links remaining for this bot)"; break
    keyboard = [[InlineKeyboardButton(get_text(user_id, 'button_main_menu', lang=lang), callback_data=f"{CALLBACK_CLIENT_PREFIX}back_to_menu")]]; markup = InlineKeyboardMarkup(keyboard)
    if len(all_results_text) > 4096:
        log.warning(f"Join results message too long ({len(all_results_text)} chars). Splitting."); parts = []; current_part = ""
        for line in all_results_text.splitlines(keepends=True):
            if len(current_part) + len(line) > 4000: parts.append(current_part); current_part = line
            else: current_part += line
        parts.append(current_part)
        for i, part in enumerate(parts):
            part_markup = markup if i == len(parts) - 1 else None;
            try: await context.bot.send_message(user_id, part, parse_mode=ParseMode.HTML, reply_markup=part_markup, disable_web_page_preview=True);
            if i < len(parts) - 1: await asyncio.sleep(0.5)
            except Exception as send_e: log.error(f"Error sending split join results part {i+1}: {send_e}"); break
    else: await _send_or_edit_message(update, context, all_results_text, reply_markup=markup, disable_web_page_preview=True)
    clear_conversation_data(context); return ConversationHandler.END

async def client_show_stats(update: Update, context: CallbackContext) -> int:
    query = update.callback_query; user_id, lang = get_user_id_and_lang(update, context); stats = db.get_client_stats(user_id)
    if not stats: text = get_text(user_id, 'client_stats_no_data', lang=lang)
    else: text = f"<b>{get_text(user_id, 'client_stats_title', lang=lang)}</b>\n\n" + get_text(user_id, 'client_stats_messages', lang=lang, total_sent=stats.get('total_messages_sent', 0)) + "\n"
    keyboard = [[InlineKeyboardButton(get_text(user_id, 'button_back', lang=lang), callback_data=f"{CALLBACK_CLIENT_PREFIX}back_to_menu")]]; markup = InlineKeyboardMarkup(keyboard)
    await _send_or_edit_message(update, context, text, reply_markup=markup, parse_mode=ParseMode.HTML); return ConversationHandler.END

async def task_show_settings_menu(update: Update, context: CallbackContext) -> int:
    query = update.callback_query; user_id, lang = get_user_id_and_lang(update, context); phone = context.user_data.get(CTX_TASK_PHONE)
    if not phone:
        if query and query.data and f"{CALLBACK_TASK_PREFIX}select_" in query.data:
             try: phone = query.data.split(f"{CALLBACK_TASK_PREFIX}select_")[1]; context.user_data[CTX_TASK_PHONE] = phone
             except IndexError: phone = None
        if not phone:
             log.error(f"Task setup called without phone for user {user_id}");
             if query and hasattr(query,'answer'): await query.answer(get_text(user_id, 'session_expired', lang=lang), show_alert=True)
             return await client_menu(update, context)
    bot_db_info = db.find_userbot(phone); display_name = html.escape(f"@{bot_db_info['username']}" if bot_db_info and bot_db_info['username'] else phone)
    task_settings_db = db.get_userbot_task_settings(user_id, phone)
    if task_settings_db:
        if CTX_TASK_SETTINGS not in context.user_data: context.user_data[CTX_TASK_SETTINGS] = dict(task_settings_db)
    else:
        if CTX_TASK_SETTINGS not in context.user_data: context.user_data[CTX_TASK_SETTINGS] = {}
    current_settings = context.user_data.get(CTX_TASK_SETTINGS, {})
    status = current_settings.get('status', 'inactive'); status_icon_key = f'task_status_icon_{status}'; status_icon = get_text(user_id, status_icon_key, lang=lang); status_text = get_text(user_id, f'task_status_{status}', lang=lang)
    primary_link_raw = current_settings.get('message_link'); primary_link = html.escape(primary_link_raw) if primary_link_raw else get_text(user_id, 'task_value_not_set', lang=lang)
    fallback_link_raw = current_settings.get('fallback_message_link'); fallback_link = html.escape(fallback_link_raw) if fallback_link_raw else get_text(user_id, 'task_value_not_set', lang=lang)
    start_time_ts = current_settings.get('start_time'); start_time_str = format_dt(start_time_ts, fmt='%H:%M')
    interval_min = current_settings.get('repetition_interval'); interval_str = get_text(user_id, 'task_value_not_set', lang=lang)
    if interval_min:
         if interval_min < 60: interval_disp = f"{interval_min} min"
         elif interval_min % (60*24) == 0: interval_disp = f"{interval_min // (60*24)} d"
         else: interval_disp = f"{interval_min // 60} h {interval_min % 60} min"
         interval_str = get_text(user_id, 'task_interval_button', lang=lang, value=interval_disp)
    target_str = get_text(user_id, 'task_value_not_set', lang=lang)
    if current_settings.get('send_to_all_groups'): target_str = get_text(user_id, 'task_value_all_groups', lang=lang)
    elif current_settings.get('folder_id'):
        folder_id = current_settings['folder_id']; folder_name = db.get_folder_name(folder_id);
        if folder_name: target_str = get_text(user_id, 'task_value_folder', lang=lang, name=html.escape(folder_name))
        else: target_str = get_text(user_id, 'task_value_folder', lang=lang, name=f"ID: {folder_id}") + " (Deleted?)"
    last_run_str = format_dt(current_settings.get('last_run')); last_error_raw = current_settings.get('last_error'); last_error = html.escape(last_error_raw[:100]) + ('...' if last_error_raw and len(last_error_raw) > 100 else '') if last_error_raw else get_text(user_id, 'task_value_not_set', lang=lang)
    text = f"<b>{get_text(user_id, 'task_setup_title', lang=lang, display_name=display_name)}</b>\n\n" + f"{get_text(user_id, 'task_setup_status_line', lang=lang, status_icon=status_icon, status_text=status_text)}\n" + f"{get_text(user_id, 'task_setup_primary_msg', lang=lang, link=primary_link)}\n" + f"{get_text(user_id, 'task_setup_start_time', lang=lang, time=start_time_str)}\n" + f"{get_text(user_id, 'task_setup_interval', lang=lang, interval=interval_str)}\n" + f"{get_text(user_id, 'task_setup_target', lang=lang, target=target_str)}\n\n" + f"{get_text(user_id, 'task_setup_last_run', lang=lang, time=last_run_str)}\n" + f"{get_text(user_id, 'task_setup_last_error', lang=lang, error=last_error)}\n"
    keyboard = [ [InlineKeyboardButton(get_text(user_id, 'task_button_set_message', lang=lang), callback_data=f"{CALLBACK_TASK_PREFIX}set_primary_link")], [InlineKeyboardButton(get_text(user_id, 'task_button_set_time', lang=lang), callback_data=f"{CALLBACK_TASK_PREFIX}set_time"), InlineKeyboardButton(get_text(user_id, 'task_button_set_interval', lang=lang), callback_data=f"{CALLBACK_TASK_PREFIX}set_interval")], [InlineKeyboardButton(get_text(user_id, 'task_button_set_target', lang=lang), callback_data=f"{CALLBACK_TASK_PREFIX}set_target_type")], [InlineKeyboardButton(get_text(user_id, 'task_button_deactivate' if status == 'active' else 'task_button_activate', lang=lang), callback_data=f"{CALLBACK_TASK_PREFIX}toggle_status"), InlineKeyboardButton(get_text(user_id, 'task_button_save', lang=lang), callback_data=f"{CALLBACK_TASK_PREFIX}save")], [InlineKeyboardButton(get_text(user_id, 'button_back', lang=lang), callback_data=f"{CALLBACK_TASK_PREFIX}back_to_bot_select")] ]
    markup = InlineKeyboardMarkup(keyboard); await _send_or_edit_message(update, context, text, reply_markup=markup, disable_web_page_preview=True); return STATE_TASK_SETUP

async def task_prompt_set_link(update: Update, context: CallbackContext, link_type: str) -> int:
    query = update.callback_query; user_id, lang = get_user_id_and_lang(update, context); prompt_key = 'task_prompt_primary_link' if link_type == 'primary' else 'task_prompt_fallback_link'; next_state = STATE_WAITING_FOR_PRIMARY_MESSAGE_LINK if link_type == 'primary' else STATE_WAITING_FOR_FALLBACK_MESSAGE_LINK; text = get_text(user_id, prompt_key, lang=lang); keyboard = [[InlineKeyboardButton(get_text(user_id, 'button_back', lang=lang), callback_data=f"{CALLBACK_TASK_PREFIX}back_to_task_menu")]]; markup = InlineKeyboardMarkup(keyboard); await _send_or_edit_message(update, context, text, reply_markup=markup); return next_state

async def process_task_link(update: Update, context: CallbackContext, link_type: str) -> int:
    user_id, lang = get_user_id_and_lang(update, context); phone = context.user_data.get(CTX_TASK_PHONE); task_settings = context.user_data.get(CTX_TASK_SETTINGS)
    if not phone or task_settings is None: await _send_or_edit_message(update, context, get_text(user_id, 'session_expired', lang=lang)); clear_conversation_data(context); return ConversationHandler.END
    link_text = update.message.text.strip()
    next_state = STATE_WAITING_FOR_PRIMARY_MESSAGE_LINK if link_type == 'primary' else STATE_WAITING_FOR_FALLBACK_MESSAGE_LINK
    if link_type == 'fallback' and link_text.lower() == 'skip': task_settings['fallback_message_link'] = None; await _send_or_edit_message(update, context, get_text(user_id, 'task_set_skipped_fallback', lang=lang)); return await task_show_settings_menu(update, context)
    link_parsed_type, _ = telethon_api.parse_telegram_url_simple(link_text)
    if link_parsed_type != "message_link": await _send_or_edit_message(update, context, get_text(user_id, 'task_error_invalid_link', lang=lang)); return next_state
    link_verified = False; await _send_or_edit_message(update, context, get_text(user_id, 'task_verifying_link', lang=lang))
    try:
        log.info(f"Verifying link access for {link_text} via bot {phone}..."); accessible = await telethon_api.check_message_link_access(phone, link_text)
        if not accessible: log.warning(f"Link {link_text} not accessible by bot {phone}."); await _send_or_edit_message(update, context, get_text(user_id, 'task_error_link_unreachable', lang=lang, bot_phone=html.escape(phone))); return next_state
        else: log.info(f"User {user_id} link {link_text} verified successfully by bot {phone}."); link_verified = True
    except Exception as e: log.error(f"Error checking link access {phone} -> {link_text}: {e}", exc_info=True); await _send_or_edit_message(update, context, get_text(user_id, 'error_telegram_api', lang=lang, error=html.escape(str(e)))); return next_state
    if link_verified:
        if link_type == 'primary': task_settings['message_link'] = link_text; await _send_or_edit_message(update, context, get_text(user_id, 'task_set_success_msg', lang=lang)); return await task_show_settings_menu(update, context)
        else: task_settings['fallback_message_link'] = link_text; await _send_or_edit_message(update, context, get_text(user_id, 'task_set_success_fallback', lang=lang)); return await task_show_settings_menu(update, context)
    else: log.error(f"Link verification failed for {link_text}."); await _send_or_edit_message(update, context, get_text(user_id, 'error_generic', lang=lang)); return next_state

async def task_prompt_start_time(update: Update, context: CallbackContext) -> int:
     query = update.callback_query; user_id, lang = get_user_id_and_lang(update, context); local_tz_name = LITHUANIA_TZ.zone if hasattr(LITHUANIA_TZ, 'zone') else 'Europe/Vilnius'; text = get_text(user_id, 'task_prompt_start_time', lang=lang, timezone_name=local_tz_name); keyboard = [[InlineKeyboardButton(get_text(user_id, 'button_back', lang=lang), callback_data=f"{CALLBACK_TASK_PREFIX}back_to_task_menu")]]; markup = InlineKeyboardMarkup(keyboard); await _send_or_edit_message(update, context, text, reply_markup=markup); return STATE_WAITING_FOR_START_TIME

async def process_task_start_time(update: Update, context: CallbackContext) -> int:
    user_id, lang = get_user_id_and_lang(update, context); time_str = update.message.text.strip(); task_settings = context.user_data.get(CTX_TASK_SETTINGS)
    if task_settings is None: await _send_or_edit_message(update, context, get_text(user_id, 'session_expired', lang=lang)); clear_conversation_data(context); return ConversationHandler.END
    try: hour, minute = map(int, time_str.split(':')); if not (0 <= hour <= 23 and 0 <= minute <= 59): raise ValueError("Invalid hour/minute")
    except (ValueError, TypeError): await _send_or_edit_message(update, context, get_text(user_id, 'task_error_invalid_time', lang=lang)); return STATE_WAITING_FOR_START_TIME
    try:
        now_local = datetime.now(LITHUANIA_TZ); input_time_obj = datetime.strptime(f"{hour:02}:{minute:02}", "%H:%M").time()
        target_local_dt_naive = datetime.combine(now_local.date(), input_time_obj); target_local_dt = LITHUANIA_TZ.localize(target_local_dt_naive)
        if target_local_dt <= now_local: target_local_dt += timedelta(days=1)
        target_utc = target_local_dt.astimezone(UTC_TZ); start_timestamp = int(target_utc.timestamp())
        task_settings['start_time'] = start_timestamp; log.info(f"User {user_id} set task start time: {time_str} LT -> {start_timestamp} UTC ({target_utc.strftime('%Y-%m-%d %H:%M')})")
        await _send_or_edit_message(update, context, get_text(user_id, 'task_set_success_time', lang=lang, time=time_str)); return await task_show_settings_menu(update, context)
    except Exception as e: log.error(f"Error converting start time '{time_str}' for user {user_id}: {e}", exc_info=True); await _send_or_edit_message(update, context, get_text(user_id, 'error_generic', lang=lang)); return STATE_WAITING_FOR_START_TIME

async def task_select_interval(update: Update, context: CallbackContext) -> int:
    query = update.callback_query; user_id, lang = get_user_id_and_lang(update, context); intervals = [5, 10, 15, 30, 60, 120, 180, 240, 360, 720, 1440]; keyboard = []; row = []
    for minutes in intervals:
        if minutes < 60: label = f"{minutes} min"
        elif minutes % (60*24) == 0: label = f"{minutes // (60*24)} d"
        else: label = f"{minutes // 60} h {minutes % 60} min"
        button_text = get_text(user_id, 'task_interval_button', lang=lang, value=label); row.append(InlineKeyboardButton(button_text, callback_data=f"{CALLBACK_INTERVAL_PREFIX}{minutes}"));
        if len(row) >= 3: keyboard.append(row); row = []
    if row: keyboard.append(row)
    keyboard.append([InlineKeyboardButton(get_text(user_id, 'button_back', lang=lang), callback_data=f"{CALLBACK_TASK_PREFIX}back_to_task_menu")]); markup = InlineKeyboardMarkup(keyboard)
    await _send_or_edit_message(update, context, get_text(user_id, 'task_select_interval_title', lang=lang), reply_markup=markup); return STATE_TASK_SETUP

async def process_interval_callback(update: Update, context: CallbackContext) -> int:
    query = update.callback_query; user_id, lang = get_user_id_and_lang(update, context); task_settings = context.user_data.get(CTX_TASK_SETTINGS)
    if task_settings is None: await query.answer(get_text(user_id, 'session_expired', lang=lang), show_alert=True); return await client_menu(update, context)
    try: interval_minutes = int(query.data.split(CALLBACK_INTERVAL_PREFIX)[1]); if interval_minutes <= 0: raise ValueError("Interval must be positive")
    except (ValueError, IndexError, AssertionError): log.error(f"Invalid interval callback data: {query.data}"); await query.answer(get_text(user_id, 'error_invalid_input', lang=lang), show_alert=True); return STATE_TASK_SETUP
    task_settings['repetition_interval'] = interval_minutes; log.info(f"User {user_id} set task interval to {interval_minutes} minutes.")
    if interval_minutes < 60: interval_disp = f"{interval_minutes} min"
    elif interval_minutes % (60*24) == 0: interval_disp = f"{interval_minutes // (60*24)} d"
    else: interval_disp = f"{interval_minutes // 60} h {interval_minutes % 60} min"
    interval_str = get_text(user_id, 'task_interval_button', lang=lang, value=interval_disp)
    await query.answer() ; return await task_show_settings_menu(update, context)

async def task_select_target_type(update: Update, context: CallbackContext) -> int:
    query = update.callback_query; user_id, lang = get_user_id_and_lang(update, context)
    keyboard = [ [InlineKeyboardButton(get_text(user_id, 'task_button_target_folder', lang=lang), callback_data=f"{CALLBACK_TASK_PREFIX}select_folder_target?page=0")], [InlineKeyboardButton(get_text(user_id, 'task_button_target_all', lang=lang), callback_data=f"{CALLBACK_TASK_PREFIX}set_target_all")], [InlineKeyboardButton(get_text(user_id, 'button_back', lang=lang), callback_data=f"{CALLBACK_TASK_PREFIX}back_to_task_menu")] ]
    markup = InlineKeyboardMarkup(keyboard); await _send_or_edit_message(update, context, get_text(user_id, 'task_select_target_title', lang=lang), reply_markup=markup); return STATE_TASK_SETUP

async def task_select_folder_for_target(update: Update, context: CallbackContext) -> int:
    query = update.callback_query; user_id, lang = get_user_id_and_lang(update, context)
    try: _, params = query.data.split('?', 1); current_page = int(params.split('=')[1])
    except (ValueError, IndexError, AttributeError): current_page = 0
    folders = db.get_folders_by_user(user_id)
    if not folders: await query.answer(get_text(user_id, 'task_error_no_folders', lang=lang), show_alert=True); return await task_select_target_type(update, context)
    total_items = len(folders); start_index = current_page * ITEMS_PER_PAGE; end_index = start_index + ITEMS_PER_PAGE; folders_page = folders[start_index:end_index]
    text = get_text(user_id, 'task_select_folder_title', lang=lang); keyboard = []
    for folder in folders_page: button_text = html.escape(folder['name']); callback_data = f"{CALLBACK_TASK_PREFIX}set_target_folder?id={folder['id']}"; keyboard.append([InlineKeyboardButton(button_text, callback_data=callback_data)])
    base_callback = f"{CALLBACK_TASK_PREFIX}select_folder_target"; pagination_buttons = build_pagination_buttons(base_callback, current_page, total_items, ITEMS_PER_PAGE, lang=lang); keyboard.extend(pagination_buttons)
    keyboard.append([InlineKeyboardButton(get_text(user_id, 'button_back', lang=lang), callback_data=f"{CALLBACK_TASK_PREFIX}back_to_target_type")]); markup = InlineKeyboardMarkup(keyboard)
    await _send_or_edit_message(update, context, text, reply_markup=markup); return STATE_SELECT_TARGET_GROUPS

async def task_set_target(update: Update, context: CallbackContext, target_type: str) -> int:
    query = update.callback_query; user_id, lang = get_user_id_and_lang(update, context); task_settings = context.user_data.get(CTX_TASK_SETTINGS)
    if task_settings is None: await query.answer(get_text(user_id, 'session_expired', lang=lang), show_alert=True); return await client_menu(update, context)
    if target_type == 'all': task_settings['send_to_all_groups'] = 1; task_settings['folder_id'] = None; log.info(f"User {user_id} set task target to all groups.")
    elif target_type == 'folder':
        try: _, params = query.data.split('?', 1); folder_id = int(params.split('=')[1])
        except (ValueError, IndexError, AttributeError): log.error(f"Could not parse folder ID: {query.data}"); await query.answer(get_text(user_id, 'error_generic', lang=lang), show_alert=True); return STATE_TASK_SETUP
        folder_name = db.get_folder_name(folder_id)
        if not folder_name: await query.answer(get_text(user_id, 'folder_not_found_error', lang=lang), show_alert=True); return STATE_TASK_SETUP
        task_settings['send_to_all_groups'] = 0; task_settings['folder_id'] = folder_id; log.info(f"User {user_id} set task target to folder {folder_name} ({folder_id}).")
    else: log.error(f"Invalid target_type '{target_type}'"); return STATE_TASK_SETUP
    await query.answer() ; return await task_show_settings_menu(update, context)

async def task_toggle_status(update: Update, context: CallbackContext) -> int:
    query = update.callback_query; user_id, lang = get_user_id_and_lang(update, context); task_settings = context.user_data.get(CTX_TASK_SETTINGS)
    if task_settings is None: await query.answer(get_text(user_id, 'session_expired', lang=lang), show_alert=True); return await client_menu(update, context)
    current_status = task_settings.get('status', 'inactive'); new_status = 'inactive' if current_status == 'active' else 'active'
    if new_status == 'active':
        missing_fields = []
        if not task_settings.get('message_link'): missing_fields.append(get_text(user_id, 'task_required_message', lang=lang))
        if not task_settings.get('start_time'): missing_fields.append(get_text(user_id, 'task_required_start_time', lang=lang))
        if not task_settings.get('repetition_interval'): missing_fields.append(get_text(user_id, 'task_required_interval', lang=lang))
        if not task_settings.get('folder_id') and not task_settings.get('send_to_all_groups'): missing_fields.append(get_text(user_id, 'task_required_target', lang=lang))
        if missing_fields: missing_str = ", ".join(missing_fields); await query.answer(get_text(user_id, 'task_save_validation_fail', lang=lang, missing=missing_str), show_alert=True); return STATE_TASK_SETUP
    task_settings['status'] = new_status; log.info(f"User {user_id} toggled task status to {new_status} (unsaved)")
    await query.answer() ; return await task_show_settings_menu(update, context)

async def task_save_settings(update: Update, context: CallbackContext) -> int:
    query = update.callback_query; user_id, lang = get_user_id_and_lang(update, context); phone = context.user_data.get(CTX_TASK_PHONE); settings_to_save = context.user_data.get(CTX_TASK_SETTINGS)
    if not phone or settings_to_save is None: await query.answer(get_text(user_id, 'session_expired', lang=lang), show_alert=True); return await client_menu(update, context)
    if settings_to_save.get('status') == 'active':
        missing_fields = []
        if not settings_to_save.get('message_link'): missing_fields.append(get_text(user_id, 'task_required_message', lang=lang))
        if not settings_to_save.get('start_time'): missing_fields.append(get_text(user_id, 'task_required_start_time', lang=lang))
        if not settings_to_save.get('repetition_interval'): missing_fields.append(get_text(user_id, 'task_required_interval', lang=lang))
        if not settings_to_save.get('folder_id') and not settings_to_save.get('send_to_all_groups'): missing_fields.append(get_text(user_id, 'task_required_target', lang=lang))
        if missing_fields: missing_str = ", ".join(missing_fields); await query.answer(get_text(user_id, 'task_save_validation_fail', lang=lang, missing=missing_str), show_alert=True); return STATE_TASK_SETUP
    settings_to_save['last_error'] = None
    if db.save_userbot_task_settings(user_id, phone, settings_to_save):
        db.log_event_db("Task Settings Saved", f"User: {user_id}, Bot: {phone}, Status: {settings_to_save.get('status')}", user_id=user_id, userbot_phone=phone); bot_db_info = db.find_userbot(phone)
        display_name = html.escape(f"@{bot_db_info['username']}" if bot_db_info and bot_db_info['username'] else phone)
        await _send_or_edit_message(update, context, get_text(user_id, 'task_save_success', lang=lang, display_name=display_name))
        clear_conversation_data(context); return await client_menu(update, context)
    else: db.log_event_db("Task Save Failed", f"User: {user_id}, Bot: {phone}", user_id=user_id, userbot_phone=phone); await _send_or_edit_message(update, context, get_text(user_id, 'task_save_error', lang=lang)); return STATE_TASK_SETUP

async def admin_list_userbots(update: Update, context: CallbackContext) -> int:
    query = update.callback_query; user_id, lang = get_user_id_and_lang(update, context)
    try: _, params = query.data.split('?', 1); current_page = int(params.split('=')[1])
    except (ValueError, IndexError, AttributeError): current_page = 0
    all_bots = db.get_all_userbots()
    if not all_bots: text = get_text(user_id, 'admin_userbot_list_no_bots', lang=lang); markup = InlineKeyboardMarkup([[InlineKeyboardButton(get_text(user_id, 'button_back', lang=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}back_to_menu")]]); await _send_or_edit_message(update, context, text, reply_markup=markup); return ConversationHandler.END
    total_items = len(all_bots); start_index = current_page * ITEMS_PER_PAGE; end_index = start_index + ITEMS_PER_PAGE; bots_page = all_bots[start_index:end_index]
    text = f"<b>{get_text(user_id, 'admin_userbot_list_title', lang=lang)}</b> (Page {current_page + 1}/{math.ceil(total_items / ITEMS_PER_PAGE)})\n\n"
    for bot in bots_page:
        phone = bot['phone_number']; username = bot['username']; status = bot['status']; assigned_client_code = bot['assigned_client'] or get_text(user_id, 'admin_userbot_list_unassigned', lang=lang); last_error = bot['last_error']; display_name = f"@{username}" if username else phone; status_icon_key = f'admin_userbot_list_status_icon_{status}'; icon_fallback = {'active': "🟢", 'inactive': "⚪️", 'error': "🔴", 'connecting': "🔌", 'needs_code': "🔢", 'needs_password': "🔒", 'authenticating': "⏳", 'initializing': "⚙️"}.get(status, "❓"); status_icon = get_text(user_id, status_icon_key, lang=lang) if status_icon_key in translations.get(lang, {}) else icon_fallback
        text += get_text(user_id, 'admin_userbot_list_line', lang=lang, status_icon=status_icon, display_name=html.escape(display_name), phone=html.escape(phone), client_code=html.escape(assigned_client_code), status=html.escape(status.capitalize())) + "\n"
        if last_error: error_text = html.escape(last_error); text += get_text(user_id, 'admin_userbot_list_error_line', lang=lang, error=error_text[:150]) + "\n"
    keyboard = []; base_callback = f"{CALLBACK_ADMIN_PREFIX}list_bots"; pagination_buttons = build_pagination_buttons(base_callback, current_page, total_items, ITEMS_PER_PAGE, lang=lang); keyboard.extend(pagination_buttons)
    keyboard.append([InlineKeyboardButton(get_text(user_id, 'button_back', lang=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}back_to_menu")]); markup = InlineKeyboardMarkup(keyboard)
    await _send_or_edit_message(update, context, text, reply_markup=markup, parse_mode=ParseMode.HTML, disable_web_page_preview=True); return ConversationHandler.END

async def admin_select_userbot_to_remove(update: Update, context: CallbackContext) -> int:
    query = update.callback_query; user_id, lang = get_user_id_and_lang(update, context)
    try: _, params = query.data.split('?', 1); current_page = int(params.split('=')[1])
    except (ValueError, IndexError, AttributeError): current_page = 0
    all_bots = db.get_all_userbots()
    if not all_bots: text = get_text(user_id, 'admin_userbot_no_bots_to_remove', lang=lang); markup = InlineKeyboardMarkup([[InlineKeyboardButton(get_text(user_id, 'button_back', lang=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}back_to_menu")]]); await _send_or_edit_message(update, context, text, reply_markup=markup); return ConversationHandler.END
    total_items = len(all_bots); start_index = current_page * ITEMS_PER_PAGE; end_index = start_index + ITEMS_PER_PAGE; bots_page = all_bots[start_index:end_index]
    text = get_text(user_id, 'admin_userbot_select_remove', lang=lang); keyboard = []
    for bot in bots_page: phone = bot['phone_number']; username = bot['username']; display_name = f"@{username}" if username else phone; button_text = f"🗑️ {html.escape(display_name)}"; keyboard.append([InlineKeyboardButton(button_text, callback_data=f"{CALLBACK_ADMIN_PREFIX}remove_bot_confirm_{phone}")])
    base_callback = f"{CALLBACK_ADMIN_PREFIX}remove_bot_select"; pagination_buttons = build_pagination_buttons(base_callback, current_page, total_items, ITEMS_PER_PAGE, lang=lang); keyboard.extend(pagination_buttons)
    keyboard.append([InlineKeyboardButton(get_text(user_id, 'button_back', lang=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}back_to_menu")]); markup = InlineKeyboardMarkup(keyboard)
    await _send_or_edit_message(update, context, text, reply_markup=markup); return STATE_ADMIN_CONFIRM_USERBOT_RESET

async def admin_confirm_remove_userbot(update: Update, context: CallbackContext) -> int:
     query = update.callback_query; user_id, lang = get_user_id_and_lang(update, context)
     try: phone_to_remove = query.data.split(f"{CALLBACK_ADMIN_PREFIX}remove_bot_confirm_")[1]
     except IndexError: log.error(f"Could not parse phone: {query.data}");
     if hasattr(query, 'answer'): await query.answer(get_text(user_id, 'error_generic', lang=lang), show_alert=True)
     return admin_command(update, context)
     bot_info = db.find_userbot(phone_to_remove)
     if not bot_info:
         if hasattr(query, 'answer'): await query.answer(get_text(user_id, 'admin_userbot_not_found', lang=lang), show_alert=True)
         return admin_command(update, context)
     username = bot_info['username']; display_name = html.escape(f"@{username}" if username else phone_to_remove)
     text = get_text(user_id, 'admin_userbot_remove_confirm_text', lang=lang, display_name=display_name)
     keyboard = [[ InlineKeyboardButton(get_text(user_id, 'button_yes', lang=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}remove_bot_confirmed_{phone_to_remove}"), InlineKeyboardButton(get_text(user_id, 'button_no', lang=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}back_to_menu") ]]; markup = InlineKeyboardMarkup(keyboard)
     await _send_or_edit_message(update, context, text, reply_markup=markup); return STATE_ADMIN_CONFIRM_USERBOT_RESET

async def admin_remove_userbot_confirmed(update: Update, context: CallbackContext) -> int:
    query = update.callback_query; user_id, lang = get_user_id_and_lang(update, context)
    try: phone_to_remove = query.data.split(f"{CALLBACK_ADMIN_PREFIX}remove_bot_confirmed_")[1]
    except IndexError: log.error(f"Could not parse phone: {query.data}");
    if hasattr(query, 'answer'): await query.answer(get_text(user_id, 'error_generic', lang=lang), show_alert=True)
    return admin_command(update, context)
    bot_info = db.find_userbot(phone_to_remove); display_name = "N/A";
    if bot_info: display_name = html.escape(f"@{bot_info['username']}" if bot_info['username'] else phone_to_remove)
    log.info(f"Admin {user_id} confirmed removal of userbot {phone_to_remove}")
    stopped = telethon_api.stop_userbot_runtime(phone_to_remove); log.info(f"Runtime stop for {phone_to_remove}: {'Success' if stopped else 'Not running/Failed'}")
    if db.remove_userbot(phone_to_remove):
        log.info(f"Attempting to remove session files for {phone_to_remove}...")
        telethon_api.delete_session_files_for_phone(phone_to_remove)
        db.log_event_db("Userbot Removed", f"Phone: {phone_to_remove}", user_id=user_id, userbot_phone=phone_to_remove); await _send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_remove_success', lang=lang, display_name=display_name))
    else: await _send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_remove_error', lang=lang))
    return admin_command(update, context)

async def admin_view_subscriptions(update: Update, context: CallbackContext) -> int:
    query = update.callback_query; user_id, lang = get_user_id_and_lang(update, context)
    try: _, params = query.data.split('?', 1); current_page = int(params.split('=')[1])
    except (ValueError, IndexError, AttributeError): current_page = 0
    subs = db.get_all_subscriptions()
    if not subs: text = get_text(user_id, 'admin_subs_none', lang=lang); markup = InlineKeyboardMarkup([[InlineKeyboardButton(get_text(user_id, 'button_back', lang=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}back_to_menu")]]); await _send_or_edit_message(update, context, text, reply_markup=markup); return ConversationHandler.END
    total_items = len(subs); start_index = current_page * ITEMS_PER_PAGE; end_index = start_index + ITEMS_PER_PAGE; subs_page = subs[start_index:end_index]
    text = f"<b>{get_text(user_id, 'admin_subs_title', lang=lang)}</b> (Page {current_page + 1}/{math.ceil(total_items / ITEMS_PER_PAGE)})\n\n"
    for sub in subs_page:
        client_user_id = sub['user_id']; user_link = f"ID: `{client_user_id}`"
        if client_user_id:
             try: user_link = f"<a href='tg://user?id={client_user_id}'>{client_user_id}</a>"
             except Exception as e: log.debug(f"Could not create user link for {client_user_id}: {e}")
        end_date = format_dt(sub['subscription_end']); code = sub['invitation_code']; bot_count = sub['bot_count']
        text += get_text(user_id, 'admin_subs_line', lang=lang, user_link=user_link, code=html.escape(code), end_date=end_date, bot_count=bot_count) + "\n\n"
    keyboard = []; base_callback = f"{CALLBACK_ADMIN_PREFIX}view_subs"; pagination_buttons = build_pagination_buttons(base_callback, current_page, total_items, ITEMS_PER_PAGE, lang=lang); keyboard.extend(pagination_buttons)
    keyboard.append([InlineKeyboardButton(get_text(user_id, 'button_back', lang=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}back_to_menu")]); markup = InlineKeyboardMarkup(keyboard)
    await _send_or_edit_message(update, context, text, reply_markup=markup, parse_mode=ParseMode.HTML, disable_web_page_preview=True); return ConversationHandler.END

async def admin_view_system_logs(update: Update, context: CallbackContext) -> int:
    query = update.callback_query; user_id, lang = get_user_id_and_lang(update, context); limit = 25
    logs = db.get_recent_logs(limit=limit)
    if not logs: text = get_text(user_id, 'admin_logs_none', lang=lang); markup = InlineKeyboardMarkup([[InlineKeyboardButton(get_text(user_id, 'button_back', lang=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}back_to_menu")]]); await _send_or_edit_message(update, context, text, reply_markup=markup); return ConversationHandler.END
    text = f"<b>{get_text(user_id, 'admin_logs_title', lang=lang, limit=limit)}</b>\n\n"
    for log_entry in logs:
        ts = log_entry['timestamp']; event = log_entry['event']; log_user_id = log_entry['user_id']; log_bot_phone = log_entry['userbot_phone']; details = log_entry['details']; time_str = format_dt(ts)
        user_str = get_text(user_id, 'admin_logs_user_none', lang=lang)
        if log_user_id: user_str = get_text(user_id, 'admin_logs_user_admin', lang=lang) + f" ({log_user_id})" if is_admin(log_user_id) else f"{log_user_id}"
        bot_str = html.escape(log_bot_phone) if log_bot_phone else get_text(user_id, 'admin_logs_bot_none', lang=lang); details_str = html.escape(details[:100]) + ('...' if details and len(details)>100 else '') if details else ""
        text += get_text(user_id, 'admin_logs_line', lang=lang, time=time_str, event=html.escape(event), user=user_str, bot=bot_str, details=details_str) + "\n"
    keyboard = [[InlineKeyboardButton(get_text(user_id, 'button_back', lang=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}back_to_menu")]]; markup = InlineKeyboardMarkup(keyboard)
    await _send_or_edit_message(update, context, text, reply_markup=markup, parse_mode=ParseMode.HTML); return ConversationHandler.END

async def handle_client_callback(update: Update, context: CallbackContext) -> str | int | None:
    query = update.callback_query; user_id, lang = get_user_id_and_lang(update, context); data = query.data; client_info = db.find_client_by_user_id(user_id)
    query_id = query.id if query else None
    if not client_info or client_info['subscription_end'] < int(datetime.now(UTC_TZ).timestamp()):
        log.warning(f"Expired/Invalid client {user_id} tried action: {data}")
        if query_id and not context.bot_data.get(f'answered_{query_id}', False): await query.answer(get_text(user_id, 'subscription_expired', lang=lang), show_alert=True); context.bot_data[f'answered_{query_id}'] = True
        clear_conversation_data(context); return ConversationHandler.END
    action = data.split(CALLBACK_CLIENT_PREFIX)[1].split('?')[0]; log.debug(f"Client Callback Route: Action='{action}', Data='{data}'")
    if action == "select_bot_task": return await client_select_bot_generic(update, context, CALLBACK_TASK_PREFIX, STATE_TASK_SETUP, 'task_select_userbot')
    elif action == "manage_folders": return await client_folder_menu(update, context)
    elif action == "select_bot_join": return await client_select_bot_generic(update, context, CALLBACK_JOIN_PREFIX, STATE_WAITING_FOR_GROUP_LINKS, 'join_select_userbot')
    elif action == "view_stats": return await client_show_stats(update, context)
    elif action == "language": return await client_ask_select_language(update, context)
    elif action == "back_to_menu": clear_conversation_data(context); return await client_menu(update, context)
    else: log.warning(f"Unhandled CLIENT CB: Action='{action}', Data='{data}'");
    if query_id and not context.bot_data.get(f'answered_{query_id}', False): await query.answer("Action not recognized.", show_alert=True); context.bot_data[f'answered_{query_id}'] = True
    return None

async def handle_admin_callback(update: Update, context: CallbackContext) -> str | int | None:
    query = update.callback_query; user_id, lang = get_user_id_and_lang(update, context); query_id = query.id if query else None
    if not is_admin(user_id):
        if query_id and not context.bot_data.get(f'answered_{query_id}', False): await query.answer(get_text(user_id, 'unauthorized', lang=lang), show_alert=True); context.bot_data[f'answered_{query_id}'] = True; return ConversationHandler.END
    data = query.data;
    if data.startswith(f"{CALLBACK_ADMIN_PREFIX}remove_bot_confirm_"): action = "remove_bot_confirm"
    elif data.startswith(f"{CALLBACK_ADMIN_PREFIX}remove_bot_confirmed_"): action = "remove_bot_confirmed"
    else: action = data.split(CALLBACK_ADMIN_PREFIX)[1].split('?')[0]
    log.debug(f"Admin Callback Route: Action='{action}', Data='{data}'")
    if action == "add_bot_prompt": await _send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_prompt_phone', lang=lang)); return STATE_WAITING_FOR_PHONE
    elif action == "remove_bot_select": return await admin_select_userbot_to_remove(update, context)
    elif action == "list_bots": return await admin_list_userbots(update, context)
    elif action == "gen_invite_prompt": await _send_or_edit_message(update, context, get_text(user_id, 'admin_invite_prompt_details', lang=lang)); return STATE_WAITING_FOR_SUB_DETAILS
    elif action == "view_subs": return await admin_view_subscriptions(update, context)
    elif action == "extend_sub_prompt": await _send_or_edit_message(update, context, get_text(user_id, 'admin_extend_prompt_code', lang=lang)); return STATE_WAITING_FOR_EXTEND_CODE
    elif action == "assign_bots_prompt": await _send_or_edit_message(update, context, get_text(user_id, 'admin_assignbots_prompt_code', lang=lang)); return STATE_WAITING_FOR_ADD_USERBOTS_CODE
    elif action == "view_logs": return await admin_view_system_logs(update, context)
    elif action == "remove_bot_confirm": return await admin_confirm_remove_userbot(update, context)
    elif action == "remove_bot_confirmed": return await admin_remove_userbot_confirmed(update, context)
    elif action == "back_to_menu": clear_conversation_data(context); return admin_command(update, context)
    else: log.warning(f"Unhandled ADMIN CB: Action='{action}', Data='{data}'");
    if query_id and not context.bot_data.get(f'answered_{query_id}', False): await query.answer("Action not recognized.", show_alert=True); context.bot_data[f'answered_{query_id}'] = True
    return None

async def handle_folder_callback(update: Update, context: CallbackContext) -> str | int | None:
    query = update.callback_query; user_id, lang = get_user_id_and_lang(update, context); data = query.data; action = data.split(CALLBACK_FOLDER_PREFIX)[1].split('?')[0]; query_id = query.id
    log.debug(f"Folder Callback Route: Action='{action}', Data='{data}'")
    if action == "create_prompt": await _send_or_edit_message(update, context, get_text(user_id, 'folder_create_prompt', lang=lang)); return STATE_WAITING_FOR_FOLDER_NAME
    elif action == "select_edit": return await client_select_folder_to_edit_or_delete(update, context, 'edit')
    elif action == "select_delete": return await client_select_folder_to_edit_or_delete(update, context, 'delete')
    elif action == "edit_selected": return await client_show_folder_edit_options(update, context)
    elif action == "delete_selected": return await client_confirm_folder_delete(update, context)
    elif action == "delete_confirmed": return await client_delete_folder_confirmed(update, context)
    elif action == "back_to_manage": clear_conversation_data(context); return await client_folder_menu(update, context)
    elif action == "edit_add_prompt": folder_name = context.user_data.get(CTX_FOLDER_NAME, "this folder"); await _send_or_edit_message(update, context, get_text(user_id, 'folder_edit_add_prompt', lang=lang, name=html.escape(folder_name))); return STATE_WAITING_FOR_GROUP_LINKS
    elif action == "edit_remove_select": return await client_select_groups_to_remove(update, context)
    elif action == "edit_toggle_remove": return await client_toggle_group_for_removal(update, context)
    elif action == "edit_remove_confirm": return await client_confirm_remove_selected_groups(update, context)
    elif action == "edit_rename_prompt": current_name = context.user_data.get(CTX_FOLDER_NAME, "this folder"); await _send_or_edit_message(update, context, get_text(user_id, 'folder_edit_rename_prompt', lang=lang, current_name=html.escape(current_name))); return STATE_FOLDER_RENAME_PROMPT
    elif action == "back_to_edit_options": context.user_data.pop(CTX_TARGET_GROUP_IDS_TO_REMOVE, None); return await client_show_folder_edit_options(update, context)
    else: log.warning(f"Unhandled FOLDER CB: Action='{action}', Data='{data}'");
    if query_id and not context.bot_data.get(f'answered_{query_id}', False): await query.answer("Action not recognized.", show_alert=True); context.bot_data[f'answered_{query_id}'] = True
    return None

async def handle_task_callback(update: Update, context: CallbackContext) -> str | int | None:
    query = update.callback_query; user_id, lang = get_user_id_and_lang(update, context); data = query.data; action = data.split(CALLBACK_TASK_PREFIX)[1].split('?')[0]; query_id = query.id
    log.debug(f"Task Callback Route: Action='{action}', Data='{data}'")
    if action.startswith("select_"): return await handle_userbot_selection(update, context, CALLBACK_TASK_PREFIX, STATE_TASK_SETUP)
    elif action == "back_to_bot_select": clear_conversation_data(context); return await client_select_bot_generic(update, context, CALLBACK_TASK_PREFIX, STATE_TASK_SETUP, 'task_select_userbot')
    elif action == "back_to_task_menu": return await task_show_settings_menu(update, context)
    elif action == "set_primary_link": return await task_prompt_set_link(update, context, 'primary')
    elif action == "set_time": return await task_prompt_start_time(update, context)
    elif action == "set_interval": return await task_select_interval(update, context)
    elif action == "set_target_type": return await task_select_target_type(update, context)
    elif action == "select_folder_target": return await task_select_folder_for_target(update, context)
    elif action == "set_target_all": return await task_set_target(update, context, 'all')
    elif action == "set_target_folder": return await task_set_target(update, context, 'folder')
    elif action == "back_to_target_type": return await task_select_target_type(update, context)
    elif action == "toggle_status": return await task_toggle_status(update, context)
    elif action == "save": return await task_save_settings(update, context)
    else: log.warning(f"Unhandled TASK CB: Action='{action}', Data='{data}'");
    if query_id and not context.bot_data.get(f'answered_{query_id}', False): await query.answer("Action not recognized.", show_alert=True); context.bot_data[f'answered_{query_id}'] = True
    return None

async def handle_join_callback(update: Update, context: CallbackContext) -> str | int | None:
    query = update.callback_query; user_id, lang = get_user_id_and_lang(update, context); data = query.data; log.debug(f"Join Callback Route: Data='{data}'"); query_id = query.id
    if data.startswith(CALLBACK_JOIN_PREFIX + "select_"): return await handle_userbot_selection(update, context, CALLBACK_JOIN_PREFIX, STATE_WAITING_FOR_GROUP_LINKS)
    else: log.warning(f"Unhandled JOIN CB: Data='{data}'");
    if query_id and not context.bot_data.get(f'answered_{query_id}', False): await query.answer("Action not recognized.", show_alert=True); context.bot_data[f'answered_{query_id}'] = True
    return None

async def handle_language_callback(update: Update, context: CallbackContext) -> str | int | None:
    query = update.callback_query; data = query.data; log.debug(f"Language Callback Route: Data='{data}'"); query_id = query.id
    if data.startswith(CALLBACK_LANG_PREFIX): return await set_language_handler(update, context)
    else: log.warning(f"Unhandled LANG CB: Data='{data}'");
    if query_id and not context.bot_data.get(f'answered_{query_id}', False): await query.answer("Action not recognized.", show_alert=True); context.bot_data[f'answered_{query_id}'] = True
    return None

async def handle_interval_callback(update: Update, context: CallbackContext) -> str | int | None:
     query = update.callback_query; data = query.data; log.debug(f"Interval Callback Route: Data='{data}'"); query_id = query.id
     if data.startswith(CALLBACK_INTERVAL_PREFIX): return await process_interval_callback(update, context)
     else: log.warning(f"Unhandled INTERVAL CB: Data='{data}'");
     if query_id and not context.bot_data.get(f'answered_{query_id}', False): await query.answer("Action not recognized.", show_alert=True); context.bot_data[f'answered_{query_id}'] = True
     return None

async def handle_generic_callback(update: Update, context: CallbackContext) -> str | int | None:
    query = update.callback_query; user_id, lang = get_user_id_and_lang(update, context); data = query.data; action = data.split(CALLBACK_GENERIC_PREFIX)[1] if CALLBACK_GENERIC_PREFIX in data else None; log.debug(f"Generic Callback Route: Action='{action}', Data='{data}'"); query_id = query.id
    if action == "cancel":
        clear_conversation_data(context); await _send_or_edit_message(update, context, get_text(user_id, 'cancelled', lang=lang))
        if is_admin(user_id): return admin_command(update, context)
        elif db.find_client_by_user_id(user_id): return await client_menu(update, context)
        else: return ConversationHandler.END
    elif action == "confirm_no":
        clear_conversation_data(context); await _send_or_edit_message(update, context, get_text(user_id, 'cancelled', lang=lang))
        if is_admin(user_id): return admin_command(update, context)
        elif db.find_client_by_user_id(user_id): return await client_menu(update, context)
        else: return ConversationHandler.END
    elif action == "noop":
        if query_id and not context.bot_data.get(f'answered_{query_id}', False): await query.answer(); context.bot_data[f'answered_{query_id}'] = True
        return None
    else:
        log.warning(f"Unhandled GENERIC CB: Action='{action}', Data='{data}'");
        if query_id and not context.bot_data.get(f'answered_{query_id}', False): await query.answer("Action not recognized.", show_alert=True); context.bot_data[f'answered_{query_id}'] = True
    return None

async def main_callback_handler(update: Update, context: CallbackContext) -> str | int | None:
    query = update.callback_query
    if not query or not query.data: log.warning("Callback without query/data."); return None
    data = query.data; user_id, lang = get_user_id_and_lang(update, context)
    log.info(f"CB Route: User={user_id}, Data='{data}'")
    next_state = None; query_id = query.id

    try:
        if data.startswith(CALLBACK_CLIENT_PREFIX): next_state = await handle_client_callback(update, context)
        elif data.startswith(CALLBACK_ADMIN_PREFIX): next_state = await handle_admin_callback(update, context)
        elif data.startswith(CALLBACK_FOLDER_PREFIX): next_state = await handle_folder_callback(update, context)
        elif data.startswith(CALLBACK_TASK_PREFIX): next_state = await handle_task_callback(update, context)
        elif data.startswith(CALLBACK_JOIN_PREFIX): next_state = await handle_join_callback(update, context)
        elif data.startswith(CALLBACK_LANG_PREFIX): next_state = await handle_language_callback(update, context)
        elif data.startswith(CALLBACK_INTERVAL_PREFIX): next_state = await handle_interval_callback(update, context)
        elif data.startswith(CALLBACK_GENERIC_PREFIX): next_state = await handle_generic_callback(update, context)
        else:
            log.warning(f"Unhandled CB prefix: User={user_id}, Data='{data}'")
            if query_id and not context.bot_data.get(f'answered_{query_id}', False): await query.answer("Unknown button.", show_alert=True); context.bot_data[f'answered_{query_id}'] = True
            next_state = ConversationHandler.END

        if query_id and not context.bot_data.get(f'answered_{query_id}', False):
             await query.answer(); context.bot_data[f'answered_{query_id}'] = True
    except Exception as e:
        log.error(f"Error processing callback data '{data}' for user {user_id}: {e}", exc_info=True)
        try:
             if query_id and not context.bot_data.get(f'answered_{query_id}', False): await query.answer(get_text(user_id, 'error_generic', lang=lang), show_alert=True); context.bot_data[f'answered_{query_id}'] = True
        except Exception: pass
        context.dispatcher.run_async(_send_or_edit_message, update, context, get_text(user_id, 'error_generic', lang=lang))
        next_state = ConversationHandler.END
    return next_state

main_conversation = ConversationHandler(
    entry_points=[
        CommandHandler('start', start_command, filters=Filters.chat_type.private),
        CommandHandler('admin', admin_command, filters=Filters.chat_type.private),
        CallbackQueryHandler(main_callback_handler)
    ],
    states={
        STATE_WAITING_FOR_CODE: [MessageHandler(Filters.text & ~Filters.command & Filters.chat_type.private, process_invitation_code)],
        STATE_WAITING_FOR_PHONE: [MessageHandler(Filters.text & ~Filters.command & Filters.chat_type.private, process_admin_phone)],
        STATE_WAITING_FOR_API_ID: [MessageHandler(Filters.text & ~Filters.command & Filters.chat_type.private, process_admin_api_id)],
        STATE_WAITING_FOR_API_HASH: [MessageHandler(Filters.text & ~Filters.command & Filters.chat_type.private, process_admin_api_hash)],
        STATE_WAITING_FOR_CODE_USERBOT: [MessageHandler(Filters.text & ~Filters.command & Filters.chat_type.private, process_admin_userbot_code)],
        STATE_WAITING_FOR_PASSWORD: [MessageHandler(Filters.text & ~Filters.command & Filters.chat_type.private, process_admin_userbot_password)],
        STATE_WAITING_FOR_SUB_DETAILS: [MessageHandler(Filters.text & ~Filters.command & Filters.chat_type.private, process_admin_invite_details)],
        STATE_WAITING_FOR_EXTEND_CODE: [MessageHandler(Filters.text & ~Filters.command & Filters.chat_type.private, process_admin_extend_code)],
        STATE_WAITING_FOR_EXTEND_DAYS: [MessageHandler(Filters.text & ~Filters.command & Filters.chat_type.private, process_admin_extend_days)],
        STATE_WAITING_FOR_ADD_USERBOTS_CODE: [MessageHandler(Filters.text & ~Filters.command & Filters.chat_type.private, process_admin_add_bots_code)],
        STATE_WAITING_FOR_ADD_USERBOTS_COUNT: [MessageHandler(Filters.text & ~Filters.command & Filters.chat_type.private, process_admin_add_bots_count)],
        STATE_ADMIN_CONFIRM_USERBOT_RESET: [CallbackQueryHandler(main_callback_handler)],
        STATE_WAITING_FOR_FOLDER_NAME: [MessageHandler(Filters.text & ~Filters.command & Filters.chat_type.private, process_folder_name)],
        STATE_WAITING_FOR_FOLDER_ACTION: [CallbackQueryHandler(main_callback_handler)],
        STATE_FOLDER_RENAME_PROMPT: [MessageHandler(Filters.text & ~Filters.command & Filters.chat_type.private, process_folder_rename)],
        STATE_FOLDER_EDIT_REMOVE_SELECT: [CallbackQueryHandler(main_callback_handler)],
        STATE_WAITING_FOR_GROUP_LINKS: [MessageHandler(Filters.text & ~Filters.command & Filters.chat_type.private, process_folder_links)],
        STATE_WAITING_FOR_FOLDER_SELECTION: [CallbackQueryHandler(main_callback_handler)],
        STATE_TASK_SETUP: [CallbackQueryHandler(main_callback_handler)],
        STATE_WAITING_FOR_PRIMARY_MESSAGE_LINK: [MessageHandler(Filters.text & ~Filters.command & Filters.chat_type.private, lambda u, c: process_task_link(u, c, 'primary'))],
        STATE_WAITING_FOR_FALLBACK_MESSAGE_LINK: [MessageHandler(Filters.text & ~Filters.command & Filters.chat_type.private, lambda u, c: process_task_link(u, c, 'fallback'))],
        STATE_WAITING_FOR_START_TIME: [MessageHandler(Filters.text & ~Filters.command & Filters.chat_type.private, process_task_start_time)],
        STATE_SELECT_TARGET_GROUPS: [CallbackQueryHandler(main_callback_handler)],
        STATE_WAITING_FOR_USERBOT_SELECTION: [CallbackQueryHandler(main_callback_handler)],
    },
    fallbacks=[
        CommandHandler('cancel', cancel_command, filters=Filters.chat_type.private),
        CommandHandler('start', start_command, filters=Filters.chat_type.private),
        CommandHandler('admin', admin_command, filters=Filters.chat_type.private & Filters.user(ADMIN_IDS)),
        CallbackQueryHandler(main_callback_handler),
        MessageHandler(Filters.text & ~Filters.command & Filters.chat_type.private, conversation_fallback),
    ],
    allow_reentry=True,
    name="main_conversation",
    persistent=True,
)

log.info("Handlers module loaded with corrected sync/async definitions.")
