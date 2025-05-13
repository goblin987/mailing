# --- START OF FILE handlers.py ---

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
    admin_process_task_message,
    admin_process_task_schedule,
    admin_process_task_target
)

# --- Helper Functions (Specific to handlers, kept here) ---

# This function is not used, can be removed if not planned for use.
# def simple_async_test(update: Update, context: CallbackContext, message: str):
#     log.info(f"simple_async_test: ENTERED! Message: '{message}', ChatID: {update.effective_chat.id if update and update.effective_chat else 'N/A'}")
#     try:
#         if update and update.effective_chat:
#             context.bot.send_message(chat_id=update.effective_chat.id, text=f"Async test successful: {message}")
#             log.info(f"simple_async_test: Message sent via context.bot.send_message!")
#         else:
#             log.error("simple_async_test: Update or effective_chat is None.")
#     except Exception as e:
#         log.error(f"simple_async_test: EXCEPTION - {e}", exc_info=True)
#     log.info("simple_async_test: EXITED!")

# --- Synchronous Error Handler (Will be replaced by async_error_handler) ---
# def sync_error_handler(update: object, context: CallbackContext) -> None:
#     log.error(msg="[sync_error_handler] Exception while handling an update:", exc_info=context.error)
#     if isinstance(update, Update) and update.effective_chat:
#         try:
#             user_id_err, lang_err = get_user_id_and_lang(update, context) # Attempt to get lang
#             error_message = get_text(user_id_err, 'error_generic', lang_override=lang_err)
#             # context.bot.send_message is async, can't be called directly in sync handler
#             # This is a best-effort log. For sending message, async handler is better.
#             log.info(f"[sync_error_handler] Would send: {error_message} to {update.effective_chat.id}")
#         except Exception as e:
#             log.error(f"[sync_error_handler] Failed to prepare/log error message: {e}")

# --- Async Error Handler ---
async def async_error_handler(update: object, context: CallbackContext) -> None:
    log.error(msg="[async_error_handler] Exception while handling an update:", exc_info=context.error)
    user_id = None; chat_id = None
    try:
        if isinstance(update, Update):
            if update.effective_user: user_id = update.effective_user.id
            if update.effective_chat: chat_id = update.effective_chat.id
        
        # If it's a CallbackQuery, the chat_id might be in message.chat.id
        if not chat_id and isinstance(update, Update) and update.callback_query:
            if update.callback_query.message and update.callback_query.message.chat:
                 chat_id = update.callback_query.message.chat.id
            if not user_id and update.callback_query.from_user:
                 user_id = update.callback_query.from_user.id


        if not chat_id and user_id: chat_id = user_id # Fallback for user-only updates if any
        
        if chat_id:
            log.info(f"[async_error_handler] Attempting to send generic error to user {user_id}, chat {chat_id}")
            # Ensure context.user_data exists and is a dict before accessing
            current_lang = 'en'
            if context and hasattr(context, 'user_data') and isinstance(context.user_data, dict):
                current_lang = context.user_data.get(CTX_LANG, 'en')
            else: # Try to get lang from DB if context.user_data is not available/populated
                if user_id:
                    try: current_lang = db.get_user_language(user_id) or 'en'
                    except: pass # Keep 'en' on DB error

            error_message = get_text(user_id, 'error_generic', lang_override=current_lang)
            await context.bot.send_message(chat_id=chat_id, text=error_message, parse_mode=ParseMode.HTML)
    except Exception as e: log.error(f"[async_error_handler] Failed to send async error message: {e}", exc_info=True)

# --- Formatting and Menu Builders (Synchronous) ---
def format_dt(timestamp: int | None, tz=LITHUANIA_TZ, fmt='%Y-%m-%d %H:%M') -> str:
    if not timestamp: return get_text(0, 'task_value_not_set', lang_override='en') # Ensure get_text can handle user_id=0
    try: dt_utc = datetime.fromtimestamp(timestamp, UTC_TZ); dt_local = dt_utc.astimezone(tz); return dt_local.strftime(fmt)
    except (ValueError, TypeError, OSError) as e: log.warning(f"Could not format invalid timestamp: {timestamp}. Error: {e}"); return "Invalid Date"

def build_client_menu(user_id, context: CallbackContext): # Sync
    lang = 'en'
    if context and hasattr(context, 'user_data') and isinstance(context.user_data, dict):
        lang = context.user_data.get(CTX_LANG, 'en')
    elif user_id: # If context.user_data is not available, try DB
        try: lang = db.get_user_language(user_id) or 'en'
        except: pass

    client_info = db.find_client_by_user_id(user_id)
    if not client_info: return get_text(user_id, 'unknown_user', lang_override=lang), None, ParseMode.HTML
    code = client_info['invitation_code']; sub_end_ts = client_info['subscription_end']; now_ts = int(datetime.now(UTC_TZ).timestamp())
    is_expired = sub_end_ts < now_ts; end_date = format_dt(sub_end_ts, fmt='%Y-%m-%d') if sub_end_ts else 'N/A'; expiry_warning = f" ⚠️ <b>{get_text(user_id, 'subscription_expired_short', lang_override=lang, default_text='Expired')}</b>" if is_expired else "" # Add translation for "Expired"
    userbot_phones = db.get_client_bots(user_id); bot_count = len(userbot_phones); parse_mode = ParseMode.HTML
    menu_text = f"<b>{get_text(user_id, 'client_menu_title', lang_override=lang, code=html.escape(code))}</b>{expiry_warning}\n"
    menu_text += get_text(user_id, 'client_menu_sub_end', lang_override=lang, end_date=end_date) + "\n\n"; menu_text += f"<u>{get_text(user_id, 'client_menu_userbots_title', lang_override=lang, count=bot_count)}</u>\n"
    if userbot_phones:
        for i, phone in enumerate(userbot_phones, 1):
            bot_db_info = db.find_userbot(phone); username = bot_db_info['username'] if bot_db_info else None; status_str = bot_db_info['status'].capitalize() if bot_db_info else 'Unknown'
            last_error = bot_db_info['last_error'] if bot_db_info else None; display_name = html.escape(f"@{username}" if username else phone); status_icon = "⚪️"
            if bot_db_info:
                status = bot_db_info['status']
                if status == 'active': status_icon = "🟢"
                elif status == 'error': status_icon = "🔴"
                elif status in ['connecting', 'authenticating', 'initializing']: status_icon = "⏳"
                elif status in ['needs_code', 'needs_password']: status_icon = "⚠️"
            menu_text += get_text(user_id, 'client_menu_userbot_line', lang_override=lang, index=i, status_icon=status_icon, display_name=display_name, status=html.escape(status_str)) + "\n"
            if last_error: escaped_error = html.escape(last_error); error_line = get_text(user_id, 'client_menu_userbot_error', lang_override=lang, error=f"{escaped_error[:100]}{'...' if len(escaped_error)>100 else ''}"); menu_text += f"  {error_line}\n"
    else: menu_text += get_text(user_id, 'client_menu_no_userbots', lang_override=lang) + "\n"
    keyboard = [[InlineKeyboardButton(get_text(user_id, 'client_menu_button_setup_tasks', lang_override=lang), callback_data=f"{CALLBACK_CLIENT_PREFIX}select_bot_task")], [InlineKeyboardButton(get_text(user_id, 'client_menu_button_manage_folders', lang_override=lang), callback_data=f"{CALLBACK_CLIENT_PREFIX}manage_folders")], [InlineKeyboardButton(get_text(user_id, 'client_menu_button_join_groups', lang_override=lang), callback_data=f"{CALLBACK_CLIENT_PREFIX}select_bot_join")], [InlineKeyboardButton(get_text(user_id, 'client_menu_button_stats', lang_override=lang), callback_data=f"{CALLBACK_CLIENT_PREFIX}view_stats")], [InlineKeyboardButton(get_text(user_id, 'client_menu_button_language', lang_override=lang), callback_data=f"{CALLBACK_CLIENT_PREFIX}language")],]
    markup = InlineKeyboardMarkup(keyboard); return menu_text, markup, parse_mode

def build_admin_menu(user_id, context: CallbackContext):
    lang = 'en'
    if context and hasattr(context, 'user_data') and isinstance(context.user_data, dict):
        lang = context.user_data.get(CTX_LANG, 'en')
    elif user_id:
        try: lang = db.get_user_language(user_id) or 'en'
        except: pass
    title = f"<b>{get_text(user_id, 'admin_panel_title', lang_override=lang)}</b>"; parse_mode = ParseMode.HTML
    keyboard = [[InlineKeyboardButton(get_text(user_id, 'admin_button_add_userbot', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}add_bot_prompt"), InlineKeyboardButton(get_text(user_id, 'admin_button_remove_userbot', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}remove_bot_select?page=0")], [InlineKeyboardButton(get_text(user_id, 'admin_button_list_userbots', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}list_bots?page=0")], [InlineKeyboardButton(get_text(user_id, 'admin_button_manage_tasks', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}manage_tasks"), InlineKeyboardButton(get_text(user_id, 'admin_button_view_tasks', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}view_tasks?page=0")], [InlineKeyboardButton(get_text(user_id, 'admin_button_gen_invite', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}gen_invite_prompt")], [InlineKeyboardButton(get_text(user_id, 'admin_button_view_subs', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}view_subs?page=0")], [InlineKeyboardButton(get_text(user_id, 'admin_button_extend_sub', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}extend_sub_prompt"), InlineKeyboardButton(get_text(user_id, 'admin_button_assign_bots_client', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}assign_bots_prompt")], [InlineKeyboardButton(get_text(user_id, 'admin_button_view_logs', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}view_logs?page=0")],]
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
    """Internal: Asynchronously shows a menu using the provided menu builder function."""
    user_id, lang = get_user_id_and_lang(update, context) # Ensures lang is set in context
    title, markup, parse_mode = menu_builder_func(user_id, context) # menu_builder_func must be synchronous
    await send_or_edit_message(update, context, title, reply_markup=markup, parse_mode=parse_mode)

# --- COMMAND HANDLERS (NOW ASYNC) ---
async def start_command(update: Update, context: CallbackContext) -> int | None:
    """Async handler for /start command."""
    log.debug("Async start_command called.")
    user_id, lang = get_user_id_and_lang(update, context) # This also sets them in context.user_data
    
    # Make sure CTX_USER_ID and CTX_LANG are in user_data if not already by get_user_id_and_lang
    if CTX_USER_ID not in context.user_data and user_id:
        context.user_data[CTX_USER_ID] = user_id
    if CTX_LANG not in context.user_data and lang:
        context.user_data[CTX_LANG] = lang
        
    clear_conversation_data(context) # Clear volatile data but keep user_id/lang/message_id

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
                # New user, ask for invitation code
                await send_or_edit_message(update, context, get_text(user_id, 'ask_invitation_code', lang_override=lang), parse_mode=ParseMode.HTML)
                log.debug("Async start_command (new user) asked for code, returning STATE_WAITING_FOR_CODE.")
                return STATE_WAITING_FOR_CODE
    except Exception as e:
        log.error(f"Error in async start_command for user {user_id}: {e}", exc_info=True)
        # Attempt to send a generic error message to the user
        try:
            err_user_id, err_lang = get_user_id_and_lang(update, context) # Re-fetch to be sure
            await send_or_edit_message(update, context, get_text(err_user_id, 'error_generic', lang_override=err_lang), parse_mode=ParseMode.HTML)
        except Exception as send_err:
            log.error(f"Failed to send error message in async_start_command handler: {send_err}")
        return ConversationHandler.END # End conversation on error

async def admin_command(update: Update, context: CallbackContext) -> int | None:
    """Async handler for /admin command."""
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


# Cancel remains async as it's simple and clearly ends
async def cancel_command(update: Update, context: CallbackContext) -> int:
    """Cancel command handler."""
    try:
        user_id, lang = get_user_id_and_lang(update, context)
        await send_or_edit_message(update, context, get_text(user_id, 'cancelled', lang_override=lang), parse_mode=ParseMode.HTML, reply_markup=None)
        clear_conversation_data(context) # Important: clear data AFTER sending message
        log.debug("Cancel command processed, returning ConversationHandler.END")
        return ConversationHandler.END
    except Exception as e:
        log.error(f"Error in cancel_command: {e}", exc_info=True)
        # Don't call async_error_handler from itself, but try to send a simple message if possible
        if update and update.effective_chat:
            try: await context.bot.send_message(chat_id=update.effective_chat.id, text="An error occurred cancelling. State cleared.")
            except: pass
        clear_conversation_data(context) # Still attempt to clear data
        return ConversationHandler.END

# --- Client Menu (separate async function for direct call if needed) ---
async def client_menu(update: Update, context: CallbackContext):
    """Shows the client menu. Can be called directly or via _show_menu_async."""
    await _show_menu_async(update, context, build_client_menu)
    return ConversationHandler.END # Typically ends the current conversation turn

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
    user_id, _ = get_user_id_and_lang(update, context) # Current lang not needed here
    
    selected_lang_code = query.data.split(CALLBACK_LANG_PREFIX)[1]
    
    if selected_lang_code in language_names:
        if db.set_user_language(user_id, selected_lang_code):
            context.user_data[CTX_LANG] = selected_lang_code # Update context immediately
            await query.answer(get_text(user_id, 'language_set', lang_override=selected_lang_code, lang_name=language_names[selected_lang_code]), show_alert=True)
            log.info(f"User {user_id} changed language to {selected_lang_code}")
            # Reshow client menu with new language
            await client_menu(update, context) 
            return ConversationHandler.END
        else:
            await query.answer(get_text(user_id, 'language_set_error', lang_override=context.user_data.get(CTX_LANG)), show_alert=True)
            log.error(f"Failed to set language to {selected_lang_code} for user {user_id} in DB.")
    else:
        await query.answer(get_text(user_id, 'error_invalid_action', lang_override=context.user_data.get(CTX_LANG)), show_alert=True)
        log.warning(f"User {user_id} selected invalid language code: {selected_lang_code}")

    # Fallback to client menu if something went wrong or just to refresh
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
            client_data = db.find_client_by_code(code_input) # Re-fetch to get all client data
            if client_data: 
                # Set language from client data if available, otherwise keep current/default
                if client_data.get('language'):
                    context.user_data[CTX_LANG] = client_data['language']
                    lang = client_data['language'] # Update local lang for this message
                await send_or_edit_message(update, context, get_text(user_id, 'activation_success', lang_override=lang)); 
                await client_menu(update, context); return ConversationHandler.END
            else: # Should not happen if activation_success
                 await send_or_edit_message(update, context, get_text(user_id, 'activation_error', lang_override=lang)); return STATE_WAITING_FOR_CODE
        elif reason_or_client_data == "already_active": await send_or_edit_message(update, context, get_text(user_id, 'already_active', lang_override=lang)); await client_menu(update, context); return ConversationHandler.END
        else: log.error(f"activate_client returned True but unexpected reason: {reason_or_client_data}"); await send_or_edit_message(update, context, get_text(user_id, 'activation_error', lang_override=lang)); return STATE_WAITING_FOR_CODE
    else: # success is False
        error_key = str(reason_or_client_data) # Ensure it's a string
        translation_map = {
            "user_already_active": "user_already_active", 
            "code_not_found": "code_not_found", 
            "code_already_used": "code_already_used", 
            "subscription_expired": "subscription_expired", # This case from activate_client logic
            "activation_error": "activation_error", 
            "activation_db_error": "activation_db_error",
        }
        message_key = translation_map.get(error_key, 'activation_error') # Default to generic error
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
    # Common API hashes are 32 hex chars, but let's be a bit lenient
    if not api_hash or len(api_hash) < 30 or not re.match('^[a-fA-F0-9]+$', api_hash): 
        await send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_invalid_api_hash', lang_override=lang)); return STATE_WAITING_FOR_API_HASH
    context.user_data[CTX_API_HASH] = api_hash; phone = context.user_data.get(CTX_PHONE); api_id = context.user_data.get(CTX_API_ID)
    if not phone or not api_id: await send_or_edit_message(update, context, get_text(user_id, 'session_expired', lang_override=lang)); clear_conversation_data(context); return ConversationHandler.END
    log.info(f"Admin {user_id} API Hash OK for {phone}. Starting authentication flow."); await send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_auth_connecting', lang_override=lang, phone=html.escape(phone)))
    try:
        # Ensure API ID/Hash from context.user_data are used, not defaults from config initially
        current_api_id = context.user_data.get(CTX_API_ID)
        current_api_hash = context.user_data.get(CTX_API_HASH)

        auth_status, auth_data = await telethon_api.start_authentication_flow(phone, current_api_id, current_api_hash); 
        log.info(f"Authentication start result for {phone}: Status='{auth_status}'")
        if auth_status == 'code_needed': 
            context.user_data[CTX_AUTH_DATA] = auth_data; # auth_data includes client, loop, thread, phone_code_hash
            await send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_prompt_code', lang_override=lang, phone=html.escape(phone))); return STATE_WAITING_FOR_CODE_USERBOT
        elif auth_status == 'password_needed': 
            context.user_data[CTX_AUTH_DATA] = auth_data; # auth_data includes client, loop, thread, pwd_state
            await send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_prompt_password', lang_override=lang, phone=html.escape(phone))); return STATE_WAITING_FOR_PASSWORD
        elif auth_status == 'already_authorized': # This path in start_authentication_flow is less likely now with pre-deletion of session
             log.warning(f"Userbot {phone} is already authorized (Telethon check) during start_authentication_flow. This might indicate an issue with session cleanup or logic flow.");
             # Attempt to add/update DB anyway and get runtime info
             if not db.find_userbot(phone): 
                safe_phone_part = re.sub(r'[^\d]', '', phone) or f'unknown_{random.randint(1000,9999)}'
                session_file_rel = f"{safe_phone_part}.session" # Assuming SQLiteSession
                db.add_userbot(phone, session_file_rel, current_api_id, current_api_hash, 'active')
             else: db.update_userbot_status(phone, 'active')
             # Start the runtime if it's not already running or if this is a re-auth
             telethon_api.get_userbot_runtime_info(phone) # This will initialize if not running
             await send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_already_auth', lang_override=lang, display_name=html.escape(phone))); 
             clear_conversation_data(context); return ConversationHandler.END
        else: # Error status
            error_msg = auth_data.get('error_message', 'Unknown error during auth start'); log.error(f"Auth start error for {phone}: {error_msg}"); 
            locals_for_format = {'phone': html.escape(phone), 'error': html.escape(error_msg)}; 
            key = 'admin_userbot_auth_error_unknown'
            if "flood wait" in error_msg.lower(): 
                key = 'admin_userbot_auth_error_flood'; seconds_match = re.search(r'\d+', error_msg); 
                locals_for_format['seconds'] = seconds_match.group(0) if seconds_match else '?'
            elif "config" in error_msg.lower() or "invalid api" in error_msg.lower(): key = 'admin_userbot_auth_error_config'
            elif "invalid phone" in error_msg.lower() or "phone number invalid" in error_msg.lower() : key = 'admin_userbot_auth_error_phone_invalid' # Match Telethon's actual error
            elif "connection" in error_msg.lower() or "timeout" in error_msg.lower(): key = 'admin_userbot_auth_error_connect'
            await send_or_edit_message(update, context, get_text(user_id, key, lang_override=lang, **locals_for_format)); 
            clear_conversation_data(context); return ConversationHandler.END
    except Exception as e: 
        log.error(f"Exception during start_authentication_flow call for {phone}: {e}", exc_info=True); 
        await send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_auth_error_unknown', lang_override=lang, phone=html.escape(phone), error=html.escape(str(e)))); 
        clear_conversation_data(context); return ConversationHandler.END

async def process_admin_userbot_code(update: Update, context: CallbackContext) -> str | int:
    user_id, lang = get_user_id_and_lang(update, context); auth_data = context.user_data.get(CTX_AUTH_DATA); 
    original_phone_input = context.user_data.get(CTX_PHONE) # Phone used to initiate
    
    if not auth_data or not original_phone_input: 
        log.error(f"process_admin_userbot_code: Missing auth_data or original_phone_input for user {user_id}"); 
        await send_or_edit_message(update, context, get_text(user_id, 'session_expired', lang_override=lang)); 
        clear_conversation_data(context); return ConversationHandler.END
        
    code = update.message.text.strip(); 
    log.info(f"process_admin_userbot_code: Processing code for original phone input {original_phone_input}")
    try:
        # Pass the full auth_data which contains client, loop, thread, phone_code_hash
        status, result_data = await telethon_api.complete_authentication_flow(auth_data, code=code)
        
        if status == 'success':
            final_phone = result_data.get('phone', original_phone_input); username = result_data.get('username'); 
            display_name = f"@{username}" if username else final_phone
            log.info(f"Code accepted for {final_phone}. Authentication successful."); 
            # Initialize or get existing runtime to update its status/info
            telethon_api.get_userbot_runtime_info(final_phone) # This should connect and update DB if not already
            await send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_auth_success', lang_override=lang, display_name=html.escape(display_name)))
            clear_conversation_data(context); return ConversationHandler.END
        elif status == 'password_needed': # This case should ideally not happen if start_auth_flow detected it
            log.warning(f"Password unexpectedly needed after code submission for {original_phone_input}. This might indicate an issue in the auth flow detection."); 
            # The auth_data should have been updated by complete_authentication_flow if it hits this
            # context.user_data[CTX_AUTH_DATA] = result_data # If complete_auth_flow doesn't update it itself.
            await send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_prompt_password', lang_override=lang, phone=html.escape(original_phone_input))); 
            return STATE_WAITING_FOR_PASSWORD
        else: # Error status
            error_msg = result_data.get('error_message', "Unknown error during code submission."); 
            log.warning(f"Code submission failed for {original_phone_input}: {error_msg}"); 
            error_key = 'admin_userbot_auth_error_code_invalid' # Default for code errors
            seconds_val = 'N/A'
            if "flood wait" in error_msg.lower(): 
                error_key = 'admin_userbot_auth_error_flood'
                seconds_match = re.search(r'(\d+)', error_msg)
                if seconds_match: seconds_val = seconds_match.group(1)

            await send_or_edit_message(update, context, get_text(user_id, error_key, lang_override=lang, phone=html.escape(original_phone_input), error=html.escape(error_msg), seconds=seconds_val))
            if error_key != 'admin_userbot_auth_error_code_invalid': # If it's flood or other critical, end.
                clear_conversation_data(context); return ConversationHandler.END
            return STATE_WAITING_FOR_CODE_USERBOT # Re-ask for code
    except Exception as e: 
        log.error(f"process_admin_userbot_code: Exception submitting code for {original_phone_input}: {e}", exc_info=True); 
        await send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_auth_error_unknown', lang_override=lang, phone=html.escape(original_phone_input), error=html.escape(str(e)))); 
        clear_conversation_data(context); return ConversationHandler.END

async def process_admin_userbot_password(update: Update, context: CallbackContext) -> str | int:
    user_id, lang = get_user_id_and_lang(update, context); auth_data = context.user_data.get(CTX_AUTH_DATA); 
    original_phone_input = context.user_data.get(CTX_PHONE) # Phone used to initiate

    if not auth_data or not original_phone_input: 
        log.error(f"process_admin_userbot_password: Missing auth_data or original_phone_input for user {user_id}"); 
        await send_or_edit_message(update, context, get_text(user_id, 'session_expired', lang_override=lang)); 
        clear_conversation_data(context); return ConversationHandler.END
        
    password = update.message.text.strip(); 
    log.info(f"process_admin_userbot_password: Processing 2FA password for original phone input {original_phone_input}")
    try:
        # Pass the full auth_data which contains client, loop, thread, pwd_state
        status, result_data = await telethon_api.complete_authentication_flow(auth_data, password=password)
        
        if status == 'success':
            final_phone = result_data.get('phone', original_phone_input); username = result_data.get('username'); 
            display_name = f"@{username}" if username else final_phone
            log.info(f"Password accepted for {final_phone}. Authentication successful."); 
            telethon_api.get_userbot_runtime_info(final_phone) # Initialize or get existing runtime
            await send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_auth_success', lang_override=lang, display_name=html.escape(display_name)))
            clear_conversation_data(context); return ConversationHandler.END
        else: # Error status
            error_msg = result_data.get('error_message', "Unknown error during password submission."); 
            log.warning(f"Password submission failed for {original_phone_input}: {error_msg}"); 
            error_key = 'admin_userbot_auth_error_password_invalid' # Default for password errors
            seconds_val = 'N/A'
            if "flood wait" in error_msg.lower(): 
                error_key = 'admin_userbot_auth_error_flood'
                seconds_match = re.search(r'(\d+)', error_msg)
                if seconds_match: seconds_val = seconds_match.group(1)
                
            await send_or_edit_message(update, context, get_text(user_id, error_key, lang_override=lang, phone=html.escape(original_phone_input), error=html.escape(error_msg), seconds=seconds_val))
            if error_key != 'admin_userbot_auth_error_password_invalid': # If flood or other critical, end.
                clear_conversation_data(context); return ConversationHandler.END
            return STATE_WAITING_FOR_PASSWORD # Re-ask for password
    except Exception as e: 
        log.error(f"process_admin_userbot_password: Exception submitting password for {original_phone_input}: {e}", exc_info=True); 
        await send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_auth_error_unknown', lang_override=lang, phone=html.escape(original_phone_input), error=html.escape(str(e)))); 
        clear_conversation_data(context); return ConversationHandler.END

# ... (rest of your handlers.py file, unchanged from provided version starting from process_admin_invite_details)
# Make sure all other async def handlers are correctly implemented and use `await send_or_edit_message`
# For brevity, I'm not re-pasting the entire file if only the top parts needed changing for the /start issue.
# The key is ensuring that any handler that needs to send a message is async and uses `await`.

# --- Conversation Handler Definition ---
# THIS MUST BE AT THE END OF THE FILE, AFTER ALL HANDLER FUNCTIONS ARE DEFINED
main_conversation = ConversationHandler(
    entry_points=[
        # Use the ASYNC command handlers as entry points
        CommandHandler('start', start_command),
        CommandHandler('admin', admin_command),
        CommandHandler('cancel', cancel_command) # cancel_command was already async
    ],
    states={
        # States expecting text input (these handlers are already async)
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
        # process_join_group_links is used for "Join Groups" and "Add to Folder"
        # If "Add to Folder" needs a different handler, split this state or logic.
        STATE_WAITING_FOR_GROUP_LINKS: [MessageHandler(Filters.text & ~Filters.command, process_join_group_links)], 
        STATE_FOLDER_RENAME_PROMPT: [MessageHandler(Filters.text & ~Filters.command, process_folder_rename)],
        STATE_WAITING_FOR_PRIMARY_MESSAGE_LINK: [MessageHandler(Filters.text & ~Filters.command, lambda u, c: process_task_link(u, c, 'primary'))],
        STATE_WAITING_FOR_FALLBACK_MESSAGE_LINK: [MessageHandler(Filters.text & ~Filters.command, lambda u, c: process_task_link(u, c, 'fallback'))],
        STATE_WAITING_FOR_START_TIME: [MessageHandler(Filters.text & ~Filters.command, process_task_start_time)],
        
        # Admin Task Creation (Text Input States) - using imported async handlers
        STATE_ADMIN_TASK_MESSAGE: [MessageHandler(Filters.text & ~Filters.command, admin_process_task_message)],
        STATE_ADMIN_TASK_SCHEDULE: [MessageHandler(Filters.text & ~Filters.command, admin_process_task_schedule)],
        STATE_ADMIN_TASK_TARGET: [MessageHandler(Filters.text & ~Filters.command, admin_process_task_target)],

        # States primarily driven by callbacks (use main_callback_handler for all)
        # main_callback_handler is async and will correctly await further async operations.
        STATE_TASK_SETUP: [CallbackQueryHandler(main_callback_handler)],
        STATE_WAITING_FOR_FOLDER_SELECTION: [CallbackQueryHandler(main_callback_handler)],
        STATE_WAITING_FOR_USERBOT_SELECTION: [CallbackQueryHandler(main_callback_handler)],
        STATE_WAITING_FOR_FOLDER_ACTION: [CallbackQueryHandler(main_callback_handler)],
        STATE_FOLDER_EDIT_REMOVE_SELECT: [CallbackQueryHandler(main_callback_handler)],
        STATE_ADMIN_CONFIRM_USERBOT_RESET: [CallbackQueryHandler(main_callback_handler)],
        STATE_ADMIN_TASK_CONFIRM: [CallbackQueryHandler(main_callback_handler)], # For admin task confirmation via CB
        STATE_WAITING_FOR_LANGUAGE: [CallbackQueryHandler(main_callback_handler)], # For language selection CB
    },
    fallbacks=[
        CommandHandler('cancel', cancel_command),
        CallbackQueryHandler(main_callback_handler), # Catch-all for callbacks not tied to a specific state if they occur
        MessageHandler(Filters.all, conversation_fallback) # Fallback for any unhandled message
    ],
    name="main_conversation", # Optional: for debugging
    persistent=False, # As per your config
    allow_reentry=True # As per your config
)

log.info("Handlers module loaded and structure updated (async command handlers).")

# --- Admin Task Management Handlers ---
# (Definitions included above)
async def admin_task_menu(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    if query: await query.answer()
    user_id, lang = get_user_id_and_lang(update, context)
    keyboard = [[InlineKeyboardButton(get_text(user_id, 'admin_task_view', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}view_tasks?page=0")], [InlineKeyboardButton(get_text(user_id, 'admin_task_create', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}create_task")], [InlineKeyboardButton(get_text(user_id, 'button_back', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}back_to_menu")]]; markup = InlineKeyboardMarkup(keyboard)
    await send_or_edit_message(update, context, get_text(user_id, 'admin_task_menu_title', lang_override=lang), reply_markup=markup)
    return ConversationHandler.END # This menu itself doesn't change state, actions do.

async def admin_view_tasks(update: Update, context: CallbackContext) -> int:
    # This is a display function, doesn't change conversation state directly.
    # Actions from this menu (buttons) will be handled by main_callback_handler.
    query = update.callback_query
    if query: await query.answer()
    user_id, lang = get_user_id_and_lang(update, context); current_page = 0
    try:
        if query and query.data and '?page=' in query.data: current_page = int(query.data.split('?page=')[1])
    except (ValueError, IndexError, AttributeError): current_page = 0
    
    tasks, total_tasks = db.get_admin_tasks(page=current_page, per_page=ITEMS_PER_PAGE); keyboard = []
    text = f"<b>{get_text(user_id, 'admin_task_list_title', lang_override=lang)}</b>"
    if tasks:
        text += f" (Page {current_page + 1}/{math.ceil(total_tasks / ITEMS_PER_PAGE)})\n\n"
        for task_row in tasks: # task_row is a sqlite3.Row object
            task = dict(task_row) # Convert to dict for easier access
            status_icon = "🟢" if task.get('status') == 'active' else "⚪️"
            task_info_line = f"{status_icon} Bot: {html.escape(task.get('userbot_phone','N/A'))} -> Target: {html.escape(task.get('target','N/A'))}"
            if task.get('schedule'): task_info_line += f" | Schedule: <code>{html.escape(task['schedule'])}</code>"
            # Make sure task ID is present and correct
            task_id_for_cb = task.get('id')
            if task_id_for_cb is None:
                log.error(f"Admin task missing ID in admin_view_tasks: {task}")
                continue # Skip if no ID
            keyboard.append([InlineKeyboardButton(task_info_line, callback_data=f"{CALLBACK_ADMIN_PREFIX}task_options_{task_id_for_cb}")])
    else: text += "\n" + get_text(user_id, 'admin_task_list_empty', lang_override=lang)
    
    pagination_buttons = build_pagination_buttons(f"{CALLBACK_ADMIN_PREFIX}view_tasks", current_page, total_tasks, ITEMS_PER_PAGE, lang)
    keyboard.extend(pagination_buttons); 
    keyboard.append([InlineKeyboardButton(get_text(user_id, 'button_back', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}manage_tasks")])
    markup = InlineKeyboardMarkup(keyboard); 
    await send_or_edit_message(update, context, text, reply_markup=markup, parse_mode=ParseMode.HTML)
    return ConversationHandler.END # Display only, no state change here.

async def admin_create_task_start(update: Update, context: CallbackContext) -> int:
    # This function is an entry point into the admin task creation flow.
    # It's called via a callback, so query.answer() is good.
    query = update.callback_query
    if query: await query.answer()
    
    user_id, lang = get_user_id_and_lang(update, context); 
    # Clear any previous task creation data, but keep user_id/lang
    # Note: clear_conversation_data preserves user_id, lang, message_id
    # We might want a more specific clear for *task* data if other convos could be active
    # For now, this standard clear is okay if admin tasks are a self-contained flow.
    
    # Ensure user_id and lang are in user_data for subsequent steps
    if CTX_USER_ID not in context.user_data and user_id:
        context.user_data[CTX_USER_ID] = user_id
    if CTX_LANG not in context.user_data and lang:
        context.user_data[CTX_LANG] = lang
        
    # Clear specific admin task context keys before starting a new one
    context.user_data.pop(CTX_TASK_BOT, None)
    context.user_data.pop(CTX_TASK_MESSAGE, None)
    context.user_data.pop(CTX_TASK_SCHEDULE, None)
    context.user_data.pop(CTX_TASK_TARGET, None)
    # CTX_TASK_TARGET_TYPE and CTX_TASK_TARGET_FOLDER are not used in this admin flow directly it seems.
    
    return await admin_select_task_bot(update, context) # Transitions to first step

async def admin_select_task_bot(update: Update, context: CallbackContext) -> int:
    # This function shows a list of bots and expects a callback.
    # The state returned should be the one that handles the *callback data* from bot selection,
    # or if it expects text input *after* this message, that state.
    # Given the callback `task_bot_{phone_number}`, the next step is text input for message.
    user_id, lang = get_user_id_and_lang(update, context); keyboard = []
    all_bots_db = db.get_all_userbots(); active_bots = [bot for bot in all_bots_db if bot['status'] == 'active']
    if not active_bots: 
        await send_or_edit_message(update, context, get_text(user_id, 'admin_task_no_bots', lang_override=lang), 
                                   reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(get_text(user_id,'button_back',lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}manage_tasks")]]))
        return ConversationHandler.END # Cannot proceed without bots
        
    for bot_row in active_bots: # bot_row is sqlite3.Row
        bot = dict(bot_row)
        display_name = f"@{bot.get('username')}" if bot.get('username') else bot.get('phone_number', 'Unknown Bot')
        bot_phone = bot.get('phone_number')
        if not bot_phone: 
            log.error(f"Active bot found with no phone_number: {bot}")
            continue
        keyboard.append([InlineKeyboardButton(html.escape(display_name), callback_data=f"{CALLBACK_ADMIN_PREFIX}task_bot_{bot_phone}")])
        
    keyboard.append([InlineKeyboardButton(get_text(user_id, 'button_back', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}manage_tasks")])
    markup = InlineKeyboardMarkup(keyboard)
    await send_or_edit_message(update, context, get_text(user_id, 'admin_task_select_bot', lang_override=lang), reply_markup=markup)
    
    # After the user clicks a bot, the callback `admin_task_bot_{phone}` is handled by `handle_admin_callback`.
    # `handle_admin_callback` then sets CTX_TASK_BOT and transitions to STATE_ADMIN_TASK_MESSAGE.
    # So, this function itself doesn't need to return a state that waits for the callback,
    # but rather the state that the *next message handler* (for the task message) expects.
    # However, the way it's structured, the callback will be processed by main_callback_handler,
    # which then routes to handle_admin_callback, which then sets the state.
    # So, this function returning STATE_ADMIN_TASK_MESSAGE here is if we were to directly enter a text state.
    # Given the current structure with callback handlers, this might be better as ConversationHandler.END
    # and let the callback handler set the next state.
    # Let's verify the callback route in handle_admin_callback:
    # elif action_main.startswith("task_bot_"): 
    #    await query.answer(); 
    #    context.user_data[CTX_TASK_BOT] = action_main.replace("task_bot_", ""); 
    #    await send_or_edit_message(update, context, get_text(user_id, 'admin_task_enter_message', lang_override=lang)); 
    #    return STATE_ADMIN_TASK_MESSAGE
    # This is correct. So admin_select_task_bot should NOT return a state if it's exited by a callback that then sets the state.
    # It should return ConversationHandler.END or a state that is handled by CallbackQueryHandler.
    # Let's assume the ConversationHandler is set up to catch these callbacks via a more general state.
    # The main_conversation has `STATE_ADMIN_TASK_CONFIRM: [CallbackQueryHandler(main_callback_handler)]`
    # but not one for this specific step.
    # The FALLBACK `CallbackQueryHandler(main_callback_handler)` will catch it.
    # So, returning ConversationHandler.END from here is fine as the callback will re-enter or set state.
    # To be very explicit, the states in ConversationHandler for admin task creation should be:
    # STATE_WAITING_FOR_TASK_BOT (if selection itself needs a state for CB)
    # STATE_ADMIN_TASK_MESSAGE (for text input)
    # ... etc.
    # The `STATE_WAITING_FOR_TASK_BOT` is defined in config.py but not used in the ConversationHandler.
    # Let's use `STATE_ADMIN_TASK_CONFIRM` as a generic CB state for this admin task flow if needed.
    # Or, more simply, the global CB handler will catch it. This function ending is okay.
    return ConversationHandler.END 


async def admin_task_options(update: Update, context: CallbackContext) -> int:
    query = update.callback_query; await query.answer()
    user_id, lang = get_user_id_and_lang(update, context); task_id_str = None
    try: task_id_str = query.data.split(f"{CALLBACK_ADMIN_PREFIX}task_options_")[1]; task_id = int(task_id_str)
    except (IndexError, ValueError): log.error(f"Failed to parse task_id from CB: {query.data}"); await send_or_edit_message(update, context, get_text(user_id, 'error_generic', lang_override=lang)); return await admin_view_tasks(update, context) # Recursive call needs to be careful.
    
    task_row = db.get_admin_task(task_id) # task_row is dict or None
    if not task_row: await send_or_edit_message(update, context, get_text(user_id, 'admin_task_not_found', lang_override=lang)); return await admin_view_tasks(update, context)
    
    task = dict(task_row) # Ensure it's a dict

    status_icon = "🟢" if task.get('status') == 'active' else "⚪️"; 
    toggle_text_key = 'admin_task_deactivate' if task.get('status') == 'active' else 'admin_task_activate'; 
    toggle_text = get_text(user_id, toggle_text_key, lang_override=lang)
    
    keyboard = [
        [InlineKeyboardButton(toggle_text, callback_data=f"{CALLBACK_ADMIN_PREFIX}toggle_task_status_{task_id}")], 
        [InlineKeyboardButton(get_text(user_id, 'admin_task_delete_button', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}delete_task_confirm_{task_id}")], 
        [InlineKeyboardButton(get_text(user_id, 'button_back', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}view_tasks?page=0")]
    ]
    markup = InlineKeyboardMarkup(keyboard)
    
    details_text = f"<b>Task #{task_id} Details</b>\n"
    details_text += f"Status: {status_icon} {html.escape(task.get('status','N/A').capitalize())}\n"
    details_text += f"Bot: {html.escape(task.get('userbot_phone','N/A'))}\n"
    message_content = task.get('message', '')
    details_text += f"Message: <pre>{html.escape(message_content[:100])}{'...' if len(message_content) > 100 else ''}</pre>\n"
    details_text += f"Schedule: <code>{html.escape(task.get('schedule','N/A'))}</code>\n"
    details_text += f"Target: {html.escape(task.get('target','N/A'))}\n"
    details_text += f"Last Run: {format_dt(task.get('last_run')) if task.get('last_run') else 'Never'}\n"
    details_text += f"Next Run Estimate: {format_dt(task.get('next_run')) if task.get('next_run') else 'Not Scheduled'}\n" # Requires next_run to be calculated and stored by scheduler
    
    await send_or_edit_message(update, context, details_text, reply_markup=markup, parse_mode=ParseMode.HTML)
    return ConversationHandler.END # Display only.

async def admin_toggle_task_status(update: Update, context: CallbackContext) -> int:
    query = update.callback_query; await query.answer()
    user_id, lang = get_user_id_and_lang(update, context); task_id_str = None
    try: task_id_str = query.data.split(f"{CALLBACK_ADMIN_PREFIX}toggle_task_status_")[1]; task_id = int(task_id_str)
    except (IndexError, ValueError): log.error(f"Failed to parse task_id for toggle status: {query.data}"); await send_or_edit_message(update, context, get_text(user_id, 'error_generic', lang_override=lang)); return await admin_view_tasks(update, context)
    
    if db.toggle_admin_task_status(task_id):
        # Instead of sending a new message, re-render the options menu for the same task
        # await context.bot.send_message(chat_id=update.effective_chat.id, text=get_text(user_id, 'admin_task_toggled', lang_override=lang))
        query.data = f"{CALLBACK_ADMIN_PREFIX}task_options_{task_id}" # Modify query data to call options again
        return await admin_task_options(update, context) # Re-show options to see updated status
    else: 
        await send_or_edit_message(update, context, get_text(user_id, 'admin_task_error', lang_override=lang))
        return await admin_view_tasks(update, context) # Fallback to list if toggle fails critically

async def admin_delete_task_confirm(update: Update, context: CallbackContext) -> int:
    query = update.callback_query; await query.answer()
    user_id, lang = get_user_id_and_lang(update, context); task_id_str = None
    try: task_id_str = query.data.split(f"{CALLBACK_ADMIN_PREFIX}delete_task_confirm_")[1]; task_id = int(task_id_str)
    except (IndexError, ValueError): log.error(f"Failed to parse task_id for delete confirm: {query.data}"); await send_or_edit_message(update, context, get_text(user_id, 'error_generic', lang_override=lang)); return await admin_view_tasks(update, context)
    
    task = db.get_admin_task(task_id) # Returns dict or None
    if not task: await send_or_edit_message(update, context, get_text(user_id, 'admin_task_not_found', lang_override=lang)); return await admin_view_tasks(update, context)
    
    confirm_text = f"Are you sure you want to delete Task #{task_id}?\nBot: {html.escape(task.get('userbot_phone','N/A'))}\nTarget: {html.escape(task.get('target','N/A'))}"
    keyboard = [
        [InlineKeyboardButton(get_text(user_id, 'button_yes', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}delete_task_execute_{task_id}")], 
        [InlineKeyboardButton(get_text(user_id, 'button_no', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}task_options_{task_id}")]
    ]
    markup = InlineKeyboardMarkup(keyboard)
    await send_or_edit_message(update, context, confirm_text, reply_markup=markup, parse_mode=ParseMode.HTML) # Ensure HTML for bold/escapes
    return ConversationHandler.END # Waiting for CB from yes/no

async def admin_delete_task_execute(update: Update, context: CallbackContext) -> int:
    query = update.callback_query; await query.answer()
    user_id, lang = get_user_id_and_lang(update, context); task_id_str = None
    try: task_id_str = query.data.split(f"{CALLBACK_ADMIN_PREFIX}delete_task_execute_")[1]; task_id = int(task_id_str)
    except (IndexError, ValueError): log.error(f"Failed to parse task_id for delete execute: {query.data}"); await send_or_edit_message(update, context, get_text(user_id, 'error_generic', lang_override=lang)); return await admin_view_tasks(update, context)
    
    if db.delete_admin_task(task_id): 
        await send_or_edit_message(update, context, get_text(user_id, 'admin_task_deleted', lang_override=lang))
    else: 
        await send_or_edit_message(update, context, get_text(user_id, 'admin_task_error', lang_override=lang))
    
    # After deletion, go back to the task list.
    # We need to make sure `admin_view_tasks` can be called like this if `query.data` is not set for page 0.
    # Create a dummy query or pass parameters differently if needed.
    # For simplicity, let's assume admin_view_tasks handles a None query.data gracefully for page 0.
    # A better way: query.data = f"{CALLBACK_ADMIN_PREFIX}view_tasks?page=0" and call directly.
    # Let's just call it directly if it handles it.
    # The admin_view_tasks has a try-except for page parsing, defaulting to 0, so this should be okay.
    return await admin_view_tasks(update, context)

# --- END OF FILE handlers.py ---
