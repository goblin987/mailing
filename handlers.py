import re
import uuid
from datetime import datetime, timedelta
import asyncio
import time
import random
import traceback # For logging detailed errors
import html # For escaping HTML in messages

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup, ParseMode, User, CallbackQuery, Message
)
from telegram.ext import (
    CommandHandler, MessageHandler, CallbackQueryHandler, ConversationHandler,
    Filters, CallbackContext
)
from telegram.error import BadRequest # To handle errors editing messages etc.

import database as db
import telethon_utils as telethon_api
from config import (
    log, ADMIN_IDS, is_admin, LITHUANIA_TZ, UTC_TZ,
    # States (Import ALL defined states from config.py)
    STATE_WAITING_FOR_CODE, STATE_WAITING_FOR_PHONE, STATE_WAITING_FOR_API_ID,
    STATE_WAITING_FOR_API_HASH, STATE_WAITING_FOR_CODE_USERBOT,
    STATE_WAITING_FOR_PASSWORD, STATE_WAITING_FOR_SUB_DETAILS,
    STATE_WAITING_FOR_FOLDER_CHOICE, STATE_WAITING_FOR_FOLDER_NAME, # Note: Some states might be unused/deprecated later
    STATE_WAITING_FOR_FOLDER_SELECTION, STATE_TASK_SETUP,
    STATE_WAITING_FOR_LANGUAGE, STATE_WAITING_FOR_EXTEND_CODE,
    STATE_WAITING_FOR_EXTEND_DAYS, STATE_WAITING_FOR_ADD_USERBOTS_CODE,
    STATE_WAITING_FOR_ADD_USERBOTS_COUNT, STATE_SELECT_TARGET_GROUPS,
    STATE_WAITING_FOR_USERBOT_SELECTION, STATE_WAITING_FOR_GROUP_LINKS,
    STATE_WAITING_FOR_FOLDER_ACTION, STATE_WAITING_FOR_PRIMARY_MESSAGE_LINK,
    STATE_WAITING_FOR_FALLBACK_MESSAGE_LINK, STATE_FOLDER_EDIT_REMOVE_SELECT,
    STATE_FOLDER_RENAME_PROMPT, STATE_ADMIN_CONFIRM_USERBOT_RESET,
    STATE_WAITING_FOR_START_TIME, # Ensure this one is included

    # Callback Prefixes
    CALLBACK_ADMIN_PREFIX, CALLBACK_CLIENT_PREFIX, CALLBACK_TASK_PREFIX,
    CALLBACK_FOLDER_PREFIX, CALLBACK_JOIN_PREFIX, CALLBACK_LANG_PREFIX,
    CALLBACK_REMOVE_PREFIX, CALLBACK_INTERVAL_PREFIX, CALLBACK_GENERIC_PREFIX
)
from translations import get_text, language_names, translations

# --- Conversation Context Keys ---
CTX_USER_ID = "_user_id"
CTX_LANG = "_lang"
CTX_PHONE = "phone"
CTX_API_ID = "api_id"
CTX_API_HASH = "api_hash"
CTX_AUTH_DATA = "auth_data"
CTX_INVITE_DETAILS = "invite_details"
CTX_EXTEND_CODE = "extend_code"
CTX_ADD_BOTS_CODE = "add_bots_code"
CTX_FOLDER_ID = "folder_id"
CTX_FOLDER_NAME = "folder_name"
CTX_FOLDER_ACTION = "folder_action"
CTX_SELECTED_BOTS = "selected_bots"
CTX_TARGET_GROUP_IDS = "target_group_ids_to_remove"
CTX_TASK_PHONE = "task_phone"
CTX_TASK_SETTINGS = "task_settings"

# --- Helper Functions ---

def clear_conversation_data(context: CallbackContext):
    """Clears sensitive or state-specific keys from user_data."""
    # ** FIX HERE: Check if user_data exists **
    if not context.user_data:
         log.debug("Skipping clear_conversation_data: context.user_data is None.")
         return

    keys_to_clear = [
        CTX_PHONE, CTX_API_ID, CTX_API_HASH, CTX_AUTH_DATA, CTX_INVITE_DETAILS,
        CTX_EXTEND_CODE, CTX_ADD_BOTS_CODE, CTX_FOLDER_ID, CTX_FOLDER_NAME,
        CTX_FOLDER_ACTION, CTX_SELECTED_BOTS, CTX_TARGET_GROUP_IDS,
        CTX_TASK_PHONE, CTX_TASK_SETTINGS
    ]
    user_id = context.user_data.get(CTX_USER_ID, 'N/A') # Keep user_id and lang
    lang = context.user_data.get(CTX_LANG, 'en')
    context.user_data.clear()
    # Restore persistent context data if needed
    context.user_data[CTX_USER_ID] = user_id
    context.user_data[CTX_LANG] = lang
    log.debug(f"Cleared volatile conversation user_data for user {user_id}")

def get_user_id_and_lang(update: Update, context: CallbackContext) -> tuple:
     """Gets user ID and language, storing them in context if missing."""
     user_id = context.user_data.get(CTX_USER_ID)
     lang = context.user_data.get(CTX_LANG)
     if not user_id and update.effective_user:
          user_id = update.effective_user.id
          context.user_data[CTX_USER_ID] = user_id
     # Fetch language only if we have a user ID and lang is not already cached
     if user_id and not lang:
          lang = db.get_user_language(user_id)
          context.user_data[CTX_LANG] = lang
     elif not lang:
          lang = 'en' # Default if no user_id found or DB error
     return user_id, lang

def reply_or_edit_text(update: Update, context: CallbackContext, text: str, **kwargs):
     """Safely replies or edits a message, handling potential errors."""
     user_id, lang = get_user_id_and_lang(update, context)
     answered_callback = False
     parse_mode = kwargs.get('parse_mode', ParseMode.HTML) # Default to HTML
     kwargs['parse_mode'] = parse_mode # Ensure parse_mode is in kwargs

     try:
          if update.callback_query:
               # Try to answer callback query silently first
               try:
                    update.callback_query.answer()
                    answered_callback = True
               except BadRequest: pass # Ignore if already answered or too old

               update.callback_query.edit_message_text(text=text, **kwargs)
          elif update.message:
               update.message.reply_text(text=text, **kwargs)
          else:
               log.warning(f"Cannot reply_or_edit_text for update type: {type(update)}. Sending new message.")
               if user_id: context.bot.send_message(chat_id=user_id, text=text, **kwargs)
     except BadRequest as e:
          if "message is not modified" in str(e).lower():
               log.debug(f"Ignoring 'message is not modified' error for user {user_id}.")
               if update.callback_query and not answered_callback:
                    try: update.callback_query.answer()
                    except: pass
          elif "message to edit not found" in str(e).lower() or "chat not found" in str(e).lower():
                log.warning(f"Failed to edit message for user {user_id} (maybe deleted): {e}")
                if user_id: context.bot.send_message(chat_id=user_id, text=text, **kwargs)
          else:
                log.error(f"BadRequest sending/editing message for user {user_id}: {e}", exc_info=True)
                if user_id:
                    try: context.bot.send_message(chat_id=user_id, text=get_text(user_id, 'error_generic', lang=lang), parse_mode=parse_mode)
                    except Exception as send_e: log.error(f"Failed to send fallback error msg to user {user_id}: {send_e}")
     except Exception as e:
          log.error(f"Unexpected error in reply_or_edit_text for user {user_id}: {e}", exc_info=True)
          if user_id:
               try: context.bot.send_message(chat_id=user_id, text=get_text(user_id, 'error_generic', lang=lang), parse_mode=parse_mode)
               except Exception as send_e: log.error(f"Failed to send fallback error msg after unexpected error to user {user_id}: {send_e}")


# --- PTB Generic Error Handler ---
def error_handler(update: object, context: CallbackContext) -> None:
    """Log Errors caused by Updates and notify user."""
    # Log the error before attempting anything else
    log.error(f"Exception while handling an update:", exc_info=context.error)

    # Attempt to notify the user only if the update context is available
    if isinstance(update, Update) and update.effective_user:
        user_id, lang = get_user_id_and_lang(update, context)
        if user_id: # Ensure we have a user ID to reply to
            reply_or_edit_text(update, context, get_text(user_id, 'error_generic', lang=lang))

    # Always clear conversation data after logging the error (if user_data exists)
    clear_conversation_data(context)


# --- Format Timestamp Helper ---
def format_dt(timestamp: int, tz=LITHUANIA_TZ, fmt='%Y-%m-%d %H:%M') -> str:
    """Formats a UTC timestamp into a human-readable string in a specific timezone."""
    if not timestamp: return "N/A"
    try:
        dt_utc = datetime.fromtimestamp(timestamp, UTC_TZ)
        dt_local = dt_utc.astimezone(tz)
        return dt_local.strftime(fmt)
    except (ValueError, TypeError):
        log.warning(f"Could not format invalid timestamp: {timestamp}")
        return "Invalid Date"

# --- Menu Builders ---
def build_client_menu(user_id, context: CallbackContext):
    """Builds the client menu message and keyboard."""
    client_info = db.find_client_by_user_id(user_id)
    lang = context.user_data.get(CTX_LANG, 'en')
    if not client_info: return get_text(user_id, 'unknown_user', lang=lang), None, ParseMode.HTML

    code = client_info['invitation_code']
    sub_end_ts = client_info['subscription_end']
    end_date = format_dt(sub_end_ts, fmt='%Y-%m-%d') if sub_end_ts else 'N/A'

    userbot_phones = db.get_client_bots(user_id)
    bot_count = len(userbot_phones)
    parse_mode = ParseMode.HTML # Using HTML for easier formatting

    menu_text = f"<b>{get_text(user_id, 'client_menu_title', lang=lang, code=code)}</b>\n" # Example usage of code
    menu_text += get_text(user_id, 'client_menu_sub_end', lang=lang, end_date=end_date) + "\n\n"
    menu_text += f"<u>{get_text(user_id, 'client_menu_userbots_title', lang=lang, count=bot_count)}</u>\n"

    if userbot_phones:
        for i, phone in enumerate(userbot_phones, 1):
            bot_db_info = db.find_userbot(phone)
            username = bot_db_info['username'] if bot_db_info else None
            status = bot_db_info['status'].capitalize() if bot_db_info else 'Unknown'
            last_error = bot_db_info['last_error'] if bot_db_info else None
            display_name = f"@{username}" if username else phone
            status_icon = "🟢" if bot_db_info and bot_db_info['status'] == 'active' else \
                          "🟡" if bot_db_info and bot_db_info['status'] not in ['active', 'inactive', 'error'] else \
                          "🔴" if bot_db_info and bot_db_info['status'] == 'error' else "⚪️"

            menu_text += f"{i}. {status_icon} {display_name} (<i>Status: {status}</i>)\n"
            if last_error:
                 escaped_error = html.escape(last_error) # Escape potential HTML in error message
                 menu_text += f"  └─ <pre>Error: {escaped_error[:100]}{'...' if len(escaped_error)>100 else ''}</pre>\n"
    else:
        menu_text += get_text(user_id, 'client_menu_no_userbots', lang=lang) + "\n"

    keyboard = [
        [InlineKeyboardButton(get_text(user_id, 'client_menu_button_setup_tasks', lang=lang), callback_data=f"{CALLBACK_CLIENT_PREFIX}setup_tasks")],
        [InlineKeyboardButton(get_text(user_id, 'client_menu_button_manage_folders', lang=lang), callback_data=f"{CALLBACK_CLIENT_PREFIX}manage_folders")],
        [
             InlineKeyboardButton(get_text(user_id, 'client_menu_button_join_groups', lang=lang), callback_data=f"{CALLBACK_CLIENT_PREFIX}join_groups"),
             InlineKeyboardButton(get_text(user_id, 'client_menu_button_view_joined', lang=lang), callback_data=f"{CALLBACK_CLIENT_PREFIX}view_joined"),
        ],
        [
             InlineKeyboardButton(get_text(user_id, 'client_menu_button_logs', lang=lang), callback_data=f"{CALLBACK_CLIENT_PREFIX}view_logs"),
             InlineKeyboardButton(get_text(user_id, 'client_menu_button_language', lang=lang), callback_data=f"{CALLBACK_CLIENT_PREFIX}language")
        ],
    ]
    markup = InlineKeyboardMarkup(keyboard)
    return menu_text, markup, parse_mode

def build_admin_menu(user_id, context: CallbackContext):
    """Builds the admin menu message and keyboard."""
    lang = context.user_data.get(CTX_LANG, 'en')
    title = f"<b>{get_text(user_id, 'admin_panel_title', lang=lang)}</b>"
    parse_mode = ParseMode.HTML
    keyboard = [
        [
             InlineKeyboardButton(get_text(user_id, 'admin_button_add_userbot', lang=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}add_bot_prompt"),
             InlineKeyboardButton(get_text(user_id, 'admin_button_remove_userbot', lang=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}remove_bot_select")
        ],
        [InlineKeyboardButton(get_text(user_id, 'admin_button_list_userbots', lang=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}list_bots")],
        [InlineKeyboardButton(get_text(user_id, 'admin_button_gen_invite', lang=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}gen_invite_prompt")],
        [InlineKeyboardButton(get_text(user_id, 'admin_button_view_subs', lang=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}view_subs")],
        [
             InlineKeyboardButton(get_text(user_id, 'admin_button_extend_sub', lang=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}extend_sub_prompt"),
             InlineKeyboardButton(get_text(user_id, 'admin_button_assign_bots_client', lang=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}assign_bots_prompt")
        ],
        [InlineKeyboardButton(get_text(user_id, 'admin_button_view_logs', lang=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}view_logs")],
    ]
    markup = InlineKeyboardMarkup(keyboard)
    return title, markup, parse_mode

# --- Command Handlers ---

def start_command(update: Update, context: CallbackContext) -> str | int:
    """Handles /start: directs users based on status."""
    user_id, lang = get_user_id_and_lang(update, context)
    clear_conversation_data(context)
    log.info(f"Start cmd: UserID={user_id}, User={update.effective_user.username}")
    client_info = db.find_client_by_user_id(user_id)
    if client_info:
        now_ts = int(datetime.now(UTC_TZ).timestamp())
        if client_info['subscription_end'] < now_ts:
            reply_or_edit_text(update, context, get_text(user_id, 'activation_expired', lang=lang))
            return ConversationHandler.END
        else:
            return client_menu(update, context)
    else:
        reply_or_edit_text(update, context, get_text(user_id, 'welcome', lang=lang))
        return STATE_WAITING_FOR_CODE

def process_invitation_code(update: Update, context: CallbackContext) -> str | int:
    """Handles the user sending an invitation code."""
    user_id, lang = get_user_id_and_lang(update, context)
    code = update.message.text.strip()
    log.info(f"UserID={user_id} submitted code: {code}")
    if not re.fullmatch(r'[a-f0-9]{8}', code, re.IGNORECASE):
        reply_or_edit_text(update, context, get_text(user_id, 'invalid_code_format', lang=lang))
        return STATE_WAITING_FOR_CODE
    success, status_key = db.activate_client(code, user_id)
    if success:
        log.info(f"Activated client {user_id} code {code}")
        db.log_event_db("Client Activated", f"Code: {code}", user_id=user_id)
        context.user_data[CTX_LANG] = db.get_user_language(user_id); lang=context.user_data[CTX_LANG]
        reply_or_edit_text(update, context, get_text(user_id, 'activation_success', lang=lang))
        return client_menu(update, context)
    else:
        log.warning(f"Failed activation user {user_id} code {code}: {status_key}")
        reply_or_edit_text(update, context, get_text(user_id, status_key, lang=lang))
        clear_conversation_data(context); return ConversationHandler.END

def admin_command(update: Update, context: CallbackContext) -> str | int:
    """Handles the /admin command for authorized admins."""
    user_id, lang = get_user_id_and_lang(update, context)
    clear_conversation_data(context)
    log.info(f"Admin cmd: UserID={user_id}, User={update.effective_user.username}")
    if not is_admin(user_id):
        reply_or_edit_text(update, context, get_text(user_id, 'unauthorized', lang=lang))
        return ConversationHandler.END
    title, markup, parse_mode = build_admin_menu(user_id, context)
    reply_or_edit_text(update, context, title, reply_markup=markup, parse_mode=parse_mode)
    return ConversationHandler.END

def cancel_command(update: Update, context: CallbackContext) -> int:
    """Generic cancel handler."""
    user_id, lang = get_user_id_and_lang(update, context)
    log.info(f"Cancel cmd: UserID={user_id}")
    reply_or_edit_text(update, context, get_text(user_id, 'cancelled', lang=lang))
    clear_conversation_data(context)
    return ConversationHandler.END

def conversation_fallback(update: Update, context: CallbackContext) -> int:
     """Handles messages not matched in a conversation state."""
     user_id, lang = get_user_id_and_lang(update, context)
     state = context.user_data.get(ConversationHandler.CURRENT_STATE)
     msg_text = update.effective_message.text if update.effective_message else 'Non-text update'
     log.warning(f"Conv fallback: UserID={user_id}. State={state}. Msg='{msg_text[:50]}...'")
     reply_or_edit_text(update, context, get_text(user_id, 'conversation_fallback', lang=lang))
     clear_conversation_data(context)
     return ConversationHandler.END

# --- Main Menu & Language ---
def client_menu(update: Update, context: CallbackContext) -> int:
    """Builds and sends the main client menu."""
    user_id, lang = get_user_id_and_lang(update, context)
    if update.callback_query:
        try: update.callback_query.answer()
        except BadRequest: pass
    message, markup, parse_mode = build_client_menu(user_id, context)
    reply_or_edit_text(update, context, message, reply_markup=markup, parse_mode=parse_mode)
    clear_conversation_data(context)
    return ConversationHandler.END

def client_ask_select_language(update: Update, context: CallbackContext):
    """Shows language selection buttons."""
    query = update.callback_query
    user_id, lang = get_user_id_and_lang(update, context)
    buttons = []; row = []
    for code, name in language_names.items():
        row.append(InlineKeyboardButton(name, callback_data=f"{CALLBACK_LANG_PREFIX}{code}"))
        if len(row) == 2: buttons.append(row); row = []
    if row: buttons.append(row)
    buttons.append([InlineKeyboardButton(get_text(user_id, 'button_back', lang=lang), callback_data=f"{CALLBACK_CLIENT_PREFIX}back_to_menu")])
    markup = InlineKeyboardMarkup(buttons)
    reply_or_edit_text(update, context, get_text(user_id, 'select_language', lang=lang), reply_markup=markup)
    return ConversationHandler.END

def set_language_handler(update: Update, context: CallbackContext):
    """Handles language selection callback."""
    query = update.callback_query
    user_id = query.from_user.id
    lang_code = query.data.split(CALLBACK_LANG_PREFIX)[1]
    current_lang = context.user_data.get(CTX_LANG, 'en')

    if lang_code not in language_names: query.answer("Invalid selection", show_alert=True); return ConversationHandler.END

    if db.set_user_language(user_id, lang_code):
         context.user_data[CTX_LANG] = lang_code
         lang = lang_code
         reply_or_edit_text(
             update, context, get_text(user_id, 'language_set', lang=lang, lang_name=language_names[lang_code]),
             reply_markup=InlineKeyboardMarkup([[ InlineKeyboardButton(get_text(user_id, 'button_main_menu', lang=lang), callback_data=f"{CALLBACK_CLIENT_PREFIX}back_to_menu")]])
         )
    else: query.answer(get_text(user_id, 'language_set_error', lang=current_lang), show_alert=True)
    return ConversationHandler.END

# --- Admin Userbot Add Flow (Implemented) ---
def process_admin_phone(update: Update, context: CallbackContext) -> str | int:
     user_id, lang = get_user_id_and_lang(update, context)
     phone_raw = update.message.text.strip()
     if not re.fullmatch(r'\+\d{9,15}', phone_raw):
          reply_or_edit_text(update, context, get_text(user_id, 'admin_userbot_invalid_phone', lang=lang))
          return STATE_WAITING_FOR_PHONE
     phone = phone_raw; context.user_data[CTX_PHONE] = phone
     log.info(f"Admin {user_id} entered phone: {phone}")
     reply_or_edit_text(update, context, get_text(user_id, 'admin_userbot_prompt_api_id', lang=lang))
     return STATE_WAITING_FOR_API_ID

def process_admin_api_id(update: Update, context: CallbackContext) -> str | int:
    user_id, lang = get_user_id_and_lang(update, context)
    api_id_str = update.message.text.strip()
    try:
        api_id = int(api_id_str);
        if api_id <= 0: raise ValueError("API ID must be positive")
        context.user_data[CTX_API_ID] = api_id
        log.info(f"Admin {user_id} API ID OK for {context.user_data.get(CTX_PHONE)}")
        reply_or_edit_text(update, context, get_text(user_id, 'admin_userbot_prompt_api_hash', lang=lang))
        return STATE_WAITING_FOR_API_HASH
    except (ValueError, TypeError): reply_or_edit_text(update, context, get_text(user_id, 'admin_userbot_invalid_api_id', lang=lang)); return STATE_WAITING_FOR_API_ID

def process_admin_api_hash(update: Update, context: CallbackContext) -> str | int:
    user_id, lang = get_user_id_and_lang(update, context); api_hash = update.message.text.strip()
    if not api_hash or len(api_hash) < 30 or not re.match('^[a-fA-F0-9]+$', api_hash): # Slightly stricter hash check
        reply_or_edit_text(update, context, get_text(user_id, 'admin_userbot_invalid_api_hash', lang=lang)); return STATE_WAITING_FOR_API_HASH
    context.user_data[CTX_API_HASH] = api_hash; phone = context.user_data[CTX_PHONE]; api_id = context.user_data[CTX_API_ID]
    log.info(f"Admin {user_id} API Hash OK for {phone}. Start auth."); reply_or_edit_text(update, context, get_text(user_id, 'admin_userbot_auth_connecting', lang=lang, phone=phone))
    try:
        auth_status, auth_data = asyncio.run(telethon_api.start_authentication_flow(phone, api_id, api_hash))
        log.info(f"Auth start result {phone}: {auth_status}")
        if auth_status == 'code_needed':
            context.user_data[CTX_AUTH_DATA] = auth_data; reply_or_edit_text(update, context, get_text(user_id, 'admin_userbot_prompt_code', lang=lang, phone=phone)); return STATE_WAITING_FOR_CODE_USERBOT
        elif auth_status == 'password_needed':
            context.user_data[CTX_AUTH_DATA] = auth_data; reply_or_edit_text(update, context, get_text(user_id, 'admin_userbot_prompt_password', lang=lang, phone=phone)); return STATE_WAITING_FOR_PASSWORD
        elif auth_status == 'already_authorized':
            bot_info = db.find_userbot(phone); display = f"@{bot_info['username']}" if bot_info and bot_info['username'] else phone
            reply_or_edit_text(update, context, get_text(user_id, 'admin_userbot_already_auth', lang=lang, display_name=display)); telethon_api.get_userbot_runtime_info(phone); clear_conversation_data(context); return ConversationHandler.END
        else: # Error case
            error_msg = auth_data.get('error_message', 'Unknown error'); locals_for_format = {'phone': phone, 'error': error_msg}
            if "flood wait" in error_msg.lower(): wait = re.search(r'\d+', error_msg); locals_for_format['seconds'] = wait.group(0) if wait else '?'; key = 'admin_userbot_auth_error_flood'
            elif "config" in error_msg.lower() or "invalid api" in error_msg.lower(): key = 'admin_userbot_auth_error_config'
            elif "invalid phone" in error_msg.lower(): key = 'admin_userbot_auth_error_phone_invalid'
            elif "connection" in error_msg.lower(): key = 'admin_userbot_auth_error_connect'
            else: key = 'admin_userbot_auth_error_unknown'
            reply_or_edit_text(update, context, get_text(user_id, key, lang=lang, **locals_for_format)); clear_conversation_data(context); return ConversationHandler.END
    except Exception as e: log.error(f"Error running start_auth {phone}: {e}", exc_info=True); reply_or_edit_text(update, context, get_text(user_id, 'admin_userbot_auth_error_unknown', lang=lang, phone=phone, error=str(e))); clear_conversation_data(context); return ConversationHandler.END

def process_admin_userbot_code(update: Update, context: CallbackContext) -> str | int:
    user_id, lang = get_user_id_and_lang(update, context); code = update.message.text.strip()
    auth_data = context.user_data.get(CTX_AUTH_DATA); phone = context.user_data.get(CTX_PHONE, "N/A")
    if not auth_data: reply_or_edit_text(update, context, get_text(user_id, 'session_expired', lang=lang)); clear_conversation_data(context); return ConversationHandler.END
    reply_or_edit_text(update, context, get_text(user_id, 'admin_userbot_auth_signing_in', lang=lang, phone=phone))
    try:
        comp_status, comp_data = asyncio.run(telethon_api.complete_authentication_flow(auth_data, code=code)); log.info(f"Auth code complete {phone}: {comp_status}")
        context.user_data.pop(CTX_AUTH_DATA, None)
        if comp_status == 'success':
            phone_num = comp_data.get('phone', phone); display = f"@{comp_data['username']}" if comp_data.get('username') else phone_num; reply_or_edit_text(update, context, get_text(user_id, 'admin_userbot_add_success', lang=lang, display_name=display)); clear_conversation_data(context); return ConversationHandler.END
        elif comp_status == 'error' and "Password required" in comp_data.get('error_message',''):
             log.warning(f"Password needed unexpectedly code {phone}. Restart."); reply_or_edit_text(update, context, "Password required. Start over."); clear_conversation_data(context); return ConversationHandler.END
        else:
             error_msg = comp_data.get('error_message', 'Unknown error.'); locals_for_format = {'phone': phone, 'error': error_msg}
             if "invalid or expired code" in error_msg.lower(): key = 'admin_userbot_auth_error_code_invalid'
             elif "flood wait" in error_msg.lower(): key = 'admin_userbot_auth_error_flood'; wait = re.search(r'\d+', error_msg); locals_for_format['seconds'] = wait.group(0) if wait else '?'
             elif "banned" in error_msg.lower() or "deactivated" in error_msg.lower(): key = 'admin_userbot_auth_error_account_issue'
             elif "connection" in error_msg.lower(): key = 'admin_userbot_auth_error_connect'
             else: key = 'admin_userbot_auth_error_unknown'
             reply_or_edit_text(update, context, get_text(user_id, key, lang=lang, **locals_for_format)); clear_conversation_data(context); return ConversationHandler.END
    except Exception as e: log.error(f"Error running complete_auth (code) {phone}: {e}", exc_info=True); reply_or_edit_text(update, context, get_text(user_id, 'admin_userbot_auth_error_unknown', lang=lang, phone=phone, error=str(e))); context.user_data.pop(CTX_AUTH_DATA, None); clear_conversation_data(context); return ConversationHandler.END

def process_admin_userbot_password(update: Update, context: CallbackContext) -> str | int:
    user_id, lang = get_user_id_and_lang(update, context); password = update.message.text.strip()
    auth_data = context.user_data.get(CTX_AUTH_DATA); phone = context.user_data.get(CTX_PHONE, "N/A")
    if not auth_data: reply_or_edit_text(update, context, get_text(user_id, 'session_expired', lang=lang)); clear_conversation_data(context); return ConversationHandler.END
    reply_or_edit_text(update, context, get_text(user_id, 'admin_userbot_auth_signing_in', lang=lang, phone=phone))
    try:
        comp_status, comp_data = asyncio.run(telethon_api.complete_authentication_flow(auth_data, password=password)); log.info(f"Auth pass complete {phone}: {comp_status}")
        context.user_data.pop(CTX_AUTH_DATA, None)
        if comp_status == 'success':
            phone_num = comp_data.get('phone', phone); display = f"@{comp_data['username']}" if comp_data.get('username') else phone_num; reply_or_edit_text(update, context, get_text(user_id, 'admin_userbot_add_success', lang=lang, display_name=display)); clear_conversation_data(context); return ConversationHandler.END
        else:
            error_msg = comp_data.get('error_message', 'Unknown error.'); locals_for_format = {'phone': phone, 'error': error_msg}
            if "incorrect password" in error_msg.lower(): key = 'admin_userbot_auth_error_password_invalid'
            elif "flood wait" in error_msg.lower(): key = 'admin_userbot_auth_error_flood'; wait = re.search(r'\d+', error_msg); locals_for_format['seconds'] = wait.group(0) if wait else '?'
            elif "banned" in error_msg.lower() or "deactivated" in error_msg.lower(): key = 'admin_userbot_auth_error_account_issue'
            elif "connection" in error_msg.lower(): key = 'admin_userbot_auth_error_connect'
            else: key = 'admin_userbot_auth_error_unknown'
            reply_or_edit_text(update, context, get_text(user_id, key, lang=lang, **locals_for_format)); clear_conversation_data(context); return ConversationHandler.END
    except Exception as e: log.error(f"Error running complete_auth (pass) {phone}: {e}", exc_info=True); reply_or_edit_text(update, context, get_text(user_id, 'admin_userbot_auth_error_unknown', lang=lang, phone=phone, error=str(e))); context.user_data.pop(CTX_AUTH_DATA, None); clear_conversation_data(context); return ConversationHandler.END


# --- Admin - Other Flows (Stubs - Needs Implementation) ---
def not_implemented_stub(update: Update, context: CallbackContext) -> int:
    """Placeholder for unimplemented handlers."""
    user_id, lang = get_user_id_and_lang(update, context)
    msg_or_cb = update.effective_message.text[:50] if update.effective_message else f"Callback:{update.callback_query.data}"
    log.warning(f"Stub hit: User={user_id}, Update={msg_or_cb}...")
    reply_or_edit_text(update, context, get_text(user_id, 'not_implemented', lang=lang))
    clear_conversation_data(context); return ConversationHandler.END

process_admin_invite_details = not_implemented_stub
process_admin_extend_code = not_implemented_stub
process_admin_extend_days = not_implemented_stub
process_admin_add_bots_code = not_implemented_stub
process_admin_add_bots_count = not_implemented_stub
process_folder_name = not_implemented_stub
process_folder_links = not_implemented_stub
process_folder_rename = not_implemented_stub
process_join_group_links = not_implemented_stub
process_task_primary_link = not_implemented_stub
process_task_fallback_link = not_implemented_stub
process_task_start_time = not_implemented_stub

# Callback function stubs
def admin_list_userbots(update: Update, context: CallbackContext): return not_implemented_stub(update, context)
def admin_view_subscriptions(update: Update, context: CallbackContext): return not_implemented_stub(update, context)
def admin_view_system_logs(update: Update, context: CallbackContext): return not_implemented_stub(update, context)
def admin_select_userbot_to_remove(update: Update, context: CallbackContext): return not_implemented_stub(update, context)
def admin_remove_userbot_confirmed(update: Update, context: CallbackContext): return not_implemented_stub(update, context)
def client_select_bot_for_task(update: Update, context: CallbackContext): return not_implemented_stub(update, context)
def client_folder_menu(update: Update, context: CallbackContext): return not_implemented_stub(update, context)
def client_select_bot_for_join(update: Update, context: CallbackContext): return not_implemented_stub(update, context)
def client_select_bot_for_view_joined(update: Update, context: CallbackContext): return not_implemented_stub(update, context)
def client_view_joined_groups(update: Update, context: CallbackContext): return not_implemented_stub(update, context)
def client_show_stats(update: Update, context: CallbackContext): return not_implemented_stub(update, context)
def client_select_folder_to_edit(update: Update, context: CallbackContext): return not_implemented_stub(update, context)
def client_show_folder_edit_options(update: Update, context: CallbackContext): return not_implemented_stub(update, context)
def client_select_groups_to_remove(update: Update, context: CallbackContext, page=0): return not_implemented_stub(update, context)
def client_confirm_folder_delete(update: Update, context: CallbackContext): return not_implemented_stub(update, context)
def client_remove_selected_groups(update: Update, context: CallbackContext): return not_implemented_stub(update, context)
def client_delete_folder_confirmed(update: Update, context: CallbackContext): return not_implemented_stub(update, context)
def task_show_settings_menu(update: Update, context: CallbackContext): return not_implemented_stub(update, context)
def task_select_interval(update: Update, context: CallbackContext): return not_implemented_stub(update, context)
def task_select_target_type(update: Update, context: CallbackContext): return not_implemented_stub(update, context)
def task_toggle_status(update: Update, context: CallbackContext): return not_implemented_stub(update, context)
def task_save_settings(update: Update, context: CallbackContext): return not_implemented_stub(update, context)
def task_select_folder_for_target(update: Update, context: CallbackContext): return not_implemented_stub(update, context)
def handle_interval_callback(update: Update, context: CallbackContext): return not_implemented_stub(update, context)
def handle_userbot_selection_for_join(update: Update, context: CallbackContext): return not_implemented_stub(update, context)

# Helper to build simple back button markup for task setup
def task_back_button_markup(user_id, context):
    lang = context.user_data.get(CTX_LANG, 'en')
    return InlineKeyboardMarkup([[
        InlineKeyboardButton(get_text(user_id, 'button_back', lang=lang), callback_data=f"{CALLBACK_TASK_PREFIX}back_to_task_menu")
    ]])

# --- Callback Routers ---
def handle_client_callback(update: Update, context: CallbackContext) -> str | int | None:
    query = update.callback_query; user_id, lang = get_user_id_and_lang(update, context); data = query.data
    client_info = db.find_client_by_user_id(user_id)
    if not client_info or client_info['subscription_end'] < int(datetime.now(UTC_TZ).timestamp()): return ConversationHandler.END # Exit silently if expired
    action = data.split(CALLBACK_CLIENT_PREFIX)[1]
    if action == "setup_tasks": return client_select_bot_for_task(update, context)
    elif action == "manage_folders": return client_folder_menu(update, context)
    elif action == "join_groups": return client_select_bot_for_join(update, context)
    elif action == "view_joined": return client_select_bot_for_view_joined(update, context)
    elif action == "view_logs": return client_show_stats(update, context)
    elif action == "language": return client_ask_select_language(update, context)
    elif action == "back_to_menu": return client_menu(update, context)
    elif data.startswith(CALLBACK_CLIENT_PREFIX + "view_joined_"): return client_view_joined_groups(update, context)
    else: log.warning(f"Unhandled CLIENT CB: '{action}'")
    return None # Return None if action is handled by stub for now

def handle_admin_callback(update: Update, context: CallbackContext) -> str | int | None:
     query = update.callback_query; user_id, lang = get_user_id_and_lang(update, context)
     if not is_admin(user_id): return ConversationHandler.END # Exit silently
     data = query.data; action = data.split(CALLBACK_ADMIN_PREFIX)[1]
     if action == "add_bot_prompt": reply_or_edit_text(update, context, get_text(user_id, 'admin_userbot_prompt_phone', lang=lang)); return STATE_WAITING_FOR_PHONE
     elif action == "remove_bot_select": return admin_select_userbot_to_remove(update, context)
     elif action == "list_bots": return admin_list_userbots(update, context)
     elif action == "gen_invite_prompt": reply_or_edit_text(update, context, get_text(user_id, 'admin_invite_prompt_details', lang=lang)); return STATE_WAITING_FOR_SUB_DETAILS
     elif action == "view_subs": return admin_view_subscriptions(update, context)
     elif action == "extend_sub_prompt": reply_or_edit_text(update, context, get_text(user_id, 'admin_extend_prompt_code', lang=lang)); return STATE_WAITING_FOR_EXTEND_CODE
     elif action == "assign_bots_prompt": reply_or_edit_text(update, context, get_text(user_id, 'admin_assignbots_prompt_code', lang=lang)); return STATE_WAITING_FOR_ADD_USERBOTS_CODE
     elif action == "view_logs": return admin_view_system_logs(update, context)
     elif data.startswith(CALLBACK_ADMIN_PREFIX + "remove_bot_confirm_"): return admin_remove_userbot_confirmed(update, context)
     else: log.warning(f"Unhandled ADMIN CB: '{action}'")
     return None

def handle_folder_callback(update: Update, context: CallbackContext) -> str | int | None:
     query = update.callback_query; user_id, lang = get_user_id_and_lang(update, context)
     log.warning(f"Folder CB STUB: User={user_id}, Data='{query.data}'")
     return not_implemented_stub(update, context)

def handle_task_callback(update: Update, context: CallbackContext) -> str | int | None:
     query = update.callback_query; user_id, lang = get_user_id_and_lang(update, context)
     log.warning(f"Task CB STUB: User={user_id}, Data='{query.data}'")
     return not_implemented_stub(update, context)

def handle_join_callback(update: Update, context: CallbackContext) -> str | int | None:
    query = update.callback_query; user_id, lang = get_user_id_and_lang(update, context); data = query.data
    if data.startswith(CALLBACK_JOIN_PREFIX + "select_"): return handle_userbot_selection_for_join(update, context)
    else: log.warning(f"Unhandled JOIN CB: '{data}'"); return None

def handle_language_callback(update: Update, context: CallbackContext) -> str | int | None:
     query = update.callback_query; data = query.data
     if data.startswith(CALLBACK_LANG_PREFIX): return set_language_handler(update, context)
     else: log.warning(f"Unhandled LANG CB: '{data}'"); return None

def handle_generic_callback(update: Update, context: CallbackContext) -> str | int | None:
     query = update.callback_query; user_id, lang = get_user_id_and_lang(update, context); data = query.data
     action = data.split(CALLBACK_GENERIC_PREFIX)[1] if CALLBACK_GENERIC_PREFIX in data else None
     if action == "cancel": clear_conversation_data(context); return ConversationHandler.END
     elif action == "confirm_no":
        # Assuming confirm_no always cancels the current flow and shows relevant menu
        clear_conversation_data(context)
        if is_admin(user_id): return admin_command(update, context)
        # Add check if user is client to return client_menu?
        return ConversationHandler.END
     else: log.warning(f"Unhandled GENERIC CB: '{action}'"); return None

# --- Main Callback Router ---
def main_callback_handler(update: Update, context: CallbackContext) -> str | int | None:
    """Handles all Inline Keyboard Button presses by routing based on prefix."""
    query = update.callback_query; data = query.data; user_id, lang = get_user_id_and_lang(update, context)
    log.info(f"CB Route: User={user_id}, Data='{data}'")
    try: query.answer() # Answer immediately
    except BadRequest: pass # Ignore if too old

    if data.startswith(CALLBACK_CLIENT_PREFIX): return handle_client_callback(update, context)
    elif data.startswith(CALLBACK_ADMIN_PREFIX): return handle_admin_callback(update, context)
    elif data.startswith(CALLBACK_FOLDER_PREFIX): return handle_folder_callback(update, context) # STUB
    elif data.startswith(CALLBACK_TASK_PREFIX): return handle_task_callback(update, context) # STUB
    elif data.startswith(CALLBACK_JOIN_PREFIX): return handle_join_callback(update, context) # STUB
    elif data.startswith(CALLBACK_LANG_PREFIX): return handle_language_callback(update, context)
    elif data.startswith(CALLBACK_INTERVAL_PREFIX): return handle_interval_callback(update, context) # STUB
    elif data.startswith(CALLBACK_GENERIC_PREFIX): return handle_generic_callback(update, context)
    else: log.warning(f"Unhandled CB prefix: User={user_id}, Data='{data}'"); return None # Let state persist

# --- Conversation Handler Definition ---
main_conversation = ConversationHandler(
    entry_points=[
        CommandHandler('start', start_command, filters=Filters.chat_type.private),
        CommandHandler('admin', admin_command, filters=Filters.chat_type.private),
        CallbackQueryHandler(main_callback_handler) # Handles button presses outside specific states
    ],
    states={
        STATE_WAITING_FOR_CODE: [MessageHandler(Filters.text & ~Filters.command & Filters.chat_type.private, process_invitation_code)],
        STATE_WAITING_FOR_PHONE: [MessageHandler(Filters.text & ~Filters.command & Filters.chat_type.private, process_admin_phone)],
        STATE_WAITING_FOR_API_ID: [MessageHandler(Filters.text & ~Filters.command & Filters.chat_type.private, process_admin_api_id)],
        STATE_WAITING_FOR_API_HASH: [MessageHandler(Filters.text & ~Filters.command & Filters.chat_type.private, process_admin_api_hash)],
        STATE_WAITING_FOR_CODE_USERBOT: [MessageHandler(Filters.text & ~Filters.command & Filters.chat_type.private, process_admin_userbot_code)],
        STATE_WAITING_FOR_PASSWORD: [MessageHandler(Filters.text & ~Filters.command & Filters.chat_type.private, process_admin_userbot_password)],
        STATE_WAITING_FOR_SUB_DETAILS: [MessageHandler(Filters.text & ~Filters.command & Filters.chat_type.private, process_admin_invite_details)], # Stub
        STATE_WAITING_FOR_EXTEND_CODE: [MessageHandler(Filters.text & ~Filters.command & Filters.chat_type.private, process_admin_extend_code)], # Stub
        STATE_WAITING_FOR_EXTEND_DAYS: [MessageHandler(Filters.text & ~Filters.command & Filters.chat_type.private, process_admin_extend_days)], # Stub
        STATE_WAITING_FOR_ADD_USERBOTS_CODE: [MessageHandler(Filters.text & ~Filters.command & Filters.chat_type.private, process_admin_add_bots_code)], # Stub
        STATE_WAITING_FOR_ADD_USERBOTS_COUNT: [MessageHandler(Filters.text & ~Filters.command & Filters.chat_type.private, process_admin_add_bots_count)], # Stub
        STATE_WAITING_FOR_USERBOT_SELECTION: [CallbackQueryHandler(main_callback_handler)],
        STATE_WAITING_FOR_GROUP_LINKS: [MessageHandler(Filters.text & ~Filters.command & Filters.chat_type.private, process_join_group_links)], # Stub
        STATE_WAITING_FOR_FOLDER_ACTION: [CallbackQueryHandler(main_callback_handler)],
        STATE_WAITING_FOR_FOLDER_NAME: [MessageHandler(Filters.text & ~Filters.command & Filters.chat_type.private, process_folder_name)], # Stub
        STATE_WAITING_FOR_FOLDER_SELECTION: [CallbackQueryHandler(main_callback_handler)],
        STATE_FOLDER_EDIT_REMOVE_SELECT: [CallbackQueryHandler(main_callback_handler)],
        STATE_FOLDER_RENAME_PROMPT: [MessageHandler(Filters.text & ~Filters.command & Filters.chat_type.private, process_folder_rename)], # Stub
        STATE_TASK_SETUP: [CallbackQueryHandler(main_callback_handler)],
        STATE_WAITING_FOR_PRIMARY_MESSAGE_LINK: [MessageHandler(Filters.text & ~Filters.command & Filters.chat_type.private, process_task_primary_link)], # Stub
        STATE_WAITING_FOR_FALLBACK_MESSAGE_LINK: [MessageHandler(Filters.text & ~Filters.command & Filters.chat_type.private, process_task_fallback_link)], # Stub
        STATE_WAITING_FOR_START_TIME: [MessageHandler(Filters.text & ~Filters.command & Filters.chat_type.private, process_task_start_time)], # Stub
        STATE_SELECT_TARGET_GROUPS: [CallbackQueryHandler(main_callback_handler)],
    },
    fallbacks=[
        CommandHandler('cancel', cancel_command, filters=Filters.chat_type.private),
        MessageHandler(Filters.all & Filters.chat_type.private, conversation_fallback)
    ],
    allow_reentry=True
)

log.info("Handlers module loaded.")
