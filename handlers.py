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
    Filters, CallbackContext
)
from telegram.error import BadRequest, TelegramError # To handle errors editing messages etc.

import database as db
import telethon_utils as telethon_api
from config import (
    log, ADMIN_IDS, is_admin, LITHUANIA_TZ, UTC_TZ, SESSION_DIR,
    # States (Import ALL defined states from config.py)
    STATE_WAITING_FOR_CODE, STATE_WAITING_FOR_PHONE, STATE_WAITING_FOR_API_ID,
    STATE_WAITING_FOR_API_HASH, STATE_WAITING_FOR_CODE_USERBOT,
    STATE_WAITING_FOR_PASSWORD, STATE_WAITING_FOR_SUB_DETAILS,
    STATE_WAITING_FOR_FOLDER_CHOICE, STATE_WAITING_FOR_FOLDER_NAME,
    STATE_WAITING_FOR_FOLDER_SELECTION, STATE_TASK_SETUP,
    STATE_WAITING_FOR_LANGUAGE, STATE_WAITING_FOR_EXTEND_CODE,
    STATE_WAITING_FOR_EXTEND_DAYS, STATE_WAITING_FOR_ADD_USERBOTS_CODE,
    STATE_WAITING_FOR_ADD_USERBOTS_COUNT, STATE_SELECT_TARGET_GROUPS,
    STATE_WAITING_FOR_USERBOT_SELECTION, STATE_WAITING_FOR_GROUP_LINKS,
    STATE_WAITING_FOR_FOLDER_ACTION, STATE_WAITING_FOR_PRIMARY_MESSAGE_LINK,
    STATE_WAITING_FOR_FALLBACK_MESSAGE_LINK, STATE_FOLDER_EDIT_REMOVE_SELECT,
    STATE_FOLDER_RENAME_PROMPT, STATE_ADMIN_CONFIRM_USERBOT_RESET, # Note: Reset state not used yet
    STATE_WAITING_FOR_START_TIME,

    # Callback Prefixes
    CALLBACK_ADMIN_PREFIX, CALLBACK_CLIENT_PREFIX, CALLBACK_TASK_PREFIX,
    CALLBACK_FOLDER_PREFIX, CALLBACK_JOIN_PREFIX, CALLBACK_LANG_PREFIX,
    # CALLBACK_REMOVE_PREFIX, # *** REMOVED THIS LINE ***
    CALLBACK_INTERVAL_PREFIX, CALLBACK_GENERIC_PREFIX
)
from translations import get_text, language_names, translations

# --- Constants ---
ITEMS_PER_PAGE = 5 # For pagination in lists

# --- Conversation Context Keys ---
CTX_USER_ID = "_user_id"
CTX_LANG = "_lang"
CTX_PHONE = "phone"
CTX_API_ID = "api_id"
CTX_API_HASH = "api_hash"
CTX_AUTH_DATA = "auth_data"
CTX_INVITE_DETAILS = "invite_details" # Store {'days': int, 'bots': int}
CTX_EXTEND_CODE = "extend_code"
CTX_ADD_BOTS_CODE = "add_bots_code"
CTX_FOLDER_ID = "folder_id"
CTX_FOLDER_NAME = "folder_name"
CTX_FOLDER_ACTION = "folder_action" # e.g., 'add', 'remove', 'rename'
CTX_SELECTED_BOTS = "selected_bots" # List of phone numbers
CTX_TARGET_GROUP_IDS_TO_REMOVE = "target_group_ids_to_remove" # List of DB IDs
CTX_TASK_PHONE = "task_phone"
CTX_TASK_SETTINGS = "task_settings" # Dict to hold settings during setup
CTX_PAGE = "page" # For pagination
CTX_MESSAGE_ID = "message_id" # To store the ID of the message being edited

# --- Helper Functions ---

def clear_conversation_data(context: CallbackContext):
    """Clears volatile keys from user_data, preserving user_id and lang."""
    if not hasattr(context, 'user_data') or context.user_data is None:
        log.debug("Skipping clear_conversation_data: context.user_data is None.")
        return

    user_id = context.user_data.get(CTX_USER_ID)
    lang = context.user_data.get(CTX_LANG)

    # List of keys to specifically remove
    keys_to_clear = [
        CTX_PHONE, CTX_API_ID, CTX_API_HASH, CTX_AUTH_DATA, CTX_INVITE_DETAILS,
        CTX_EXTEND_CODE, CTX_ADD_BOTS_CODE, CTX_FOLDER_ID, CTX_FOLDER_NAME,
        CTX_FOLDER_ACTION, CTX_SELECTED_BOTS, CTX_TARGET_GROUP_IDS_TO_REMOVE,
        CTX_TASK_PHONE, CTX_TASK_SETTINGS, CTX_PAGE, CTX_MESSAGE_ID
    ]

    # Remove the specified keys if they exist
    for key in keys_to_clear:
        context.user_data.pop(key, None)

    # Ensure essential keys remain or are restored if accidentally cleared
    if user_id:
        context.user_data[CTX_USER_ID] = user_id
    if lang:
        context.user_data[CTX_LANG] = lang

    log.debug(f"Cleared volatile conversation user_data for user {user_id or 'N/A'}")


def get_user_id_and_lang(update: Update, context: CallbackContext) -> tuple:
    """Gets user ID and language, storing them in context if missing."""
    user_id = context.user_data.get(CTX_USER_ID) if context.user_data else None
    lang = context.user_data.get(CTX_LANG) if context.user_data else None

    if not user_id and update and update.effective_user:
        user_id = update.effective_user.id
        if not context.user_data: context.user_data = {} # Initialize if needed
        context.user_data[CTX_USER_ID] = user_id

    # Fetch language only if we have a user ID and lang is not already cached
    if user_id and not lang:
        lang = db.get_user_language(user_id)
        if not context.user_data: context.user_data = {} # Initialize if needed
        context.user_data[CTX_LANG] = lang
    elif not lang:
        lang = 'en' # Default if no user_id found or DB error
        if user_id and context.user_data: # Store default if user_id known
             context.user_data[CTX_LANG] = lang

    return user_id, lang

async def reply_or_edit_text(update: Update, context: CallbackContext, text: str, **kwargs):
    """Safely replies or edits a message, handling potential errors."""
    user_id, lang = get_user_id_and_lang(update, context)
    answered_callback = False
    parse_mode = kwargs.get('parse_mode', ParseMode.HTML) # Default to HTML
    kwargs['parse_mode'] = parse_mode # Ensure parse_mode is in kwargs
    chat_id = update.effective_chat.id if update.effective_chat else user_id
    message_id = None

    # Try to get message_id for editing
    if update.callback_query:
        message_id = update.callback_query.message.message_id
    elif 'message_id' in context.user_data: # Check if we stored it for editing
        message_id = context.user_data.get(CTX_MESSAGE_ID)

    # Ensure reply_markup is properly handled
    reply_markup = kwargs.get('reply_markup')
    if reply_markup and not isinstance(reply_markup, InlineKeyboardMarkup):
        log.warning(f"Invalid reply_markup type passed: {type(reply_markup)}. Setting to None.")
        kwargs['reply_markup'] = None

    try:
        if update.callback_query:
            # Answer callback query silently first
            try:
                await update.callback_query.answer()
                answered_callback = True
            except (BadRequest, TelegramError) as cb_e:
                log.debug(f"Ignoring callback answer error: {cb_e}")

            # Use the message_id from the callback query's message
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=update.callback_query.message.message_id,
                text=text,
                **kwargs
            )
            # Store the message_id for potential future edits in the same flow
            if context.user_data: context.user_data[CTX_MESSAGE_ID] = update.callback_query.message.message_id

        elif update.message:
            sent_message = await update.message.reply_text(text=text, **kwargs)
            if context.user_data: context.user_data[CTX_MESSAGE_ID] = sent_message.message_id
        # Fallback: Try editing a stored message ID if no direct update source
        elif message_id and chat_id:
             log.debug(f"Attempting edit using stored message_id: {message_id}")
             await context.bot.edit_message_text(
                 chat_id=chat_id,
                 message_id=message_id,
                 text=text,
                 **kwargs
             )
        # Final Fallback: Send new message if editing isn't possible
        else:
            log.warning(f"Cannot reply or edit for update type: {type(update)}. Sending new message to {chat_id}.")
            if chat_id:
                sent_message = await context.bot.send_message(chat_id=chat_id, text=text, **kwargs)
                if context.user_data: context.user_data[CTX_MESSAGE_ID] = sent_message.message_id


    except BadRequest as e:
        if "message is not modified" in str(e).lower():
            log.debug(f"Ignoring 'message is not modified' error for user {user_id}.")
            if update.callback_query and not answered_callback:
                try: await update.callback_query.answer()
                except: pass
        elif "message to edit not found" in str(e).lower() or "chat not found" in str(e).lower():
            log.warning(f"Failed to edit message for user {user_id} (maybe deleted or wrong ID): {e}. Sending new.")
            if chat_id:
                try:
                    sent_message = await context.bot.send_message(chat_id=chat_id, text=text, **kwargs)
                    if context.user_data: context.user_data[CTX_MESSAGE_ID] = sent_message.message_id
                except Exception as send_e: log.error(f"Failed to send fallback message to {chat_id}: {send_e}")
            if context.user_data: context.user_data.pop(CTX_MESSAGE_ID, None) # Clear invalid message ID
        else:
            log.error(f"BadRequest sending/editing message for user {user_id}: {e}", exc_info=True)
            if chat_id:
                try: await context.bot.send_message(chat_id=chat_id, text=get_text(user_id, 'error_generic', lang=lang), parse_mode=parse_mode)
                except Exception as send_e: log.error(f"Failed to send fallback error msg to user {user_id}: {send_e}")
    except TelegramError as e:
         log.error(f"TelegramError sending/editing message for user {user_id}: {e}", exc_info=True)
         if chat_id:
              try: await context.bot.send_message(chat_id=chat_id, text=get_text(user_id, 'error_generic', lang=lang), parse_mode=parse_mode)
              except Exception as send_e: log.error(f"Failed to send fallback error msg after TelegramError to user {user_id}: {send_e}")
    except Exception as e:
        log.error(f"Unexpected error in reply_or_edit_text for user {user_id}: {e}", exc_info=True)
        if chat_id:
            try: await context.bot.send_message(chat_id=chat_id, text=get_text(user_id, 'error_generic', lang=lang), parse_mode=parse_mode)
            except Exception as send_e: log.error(f"Failed to send fallback error msg after unexpected error to user {user_id}: {send_e}")


# --- PTB Generic Error Handler ---
async def error_handler(update: object, context: CallbackContext) -> None:
    """Log Errors caused by Updates and notify user."""
    log.error(f"Exception while handling an update:", exc_info=context.error)

    # Attempt to notify the user only if the update context is available
    if isinstance(update, Update) and update.effective_chat:
        user_id, lang = get_user_id_and_lang(update, context)
        # Use a default message as the error might prevent fetching translations properly
        error_message = "An internal error occurred. Please try again later."
        try:
            error_message = get_text(user_id, 'error_generic', lang=lang)
        except Exception:
            pass # Stick to default message if get_text fails

        await reply_or_edit_text(update, context, error_message)

    # Optionally clear conversation data for the user involved
    if hasattr(context, 'user_data'):
         clear_conversation_data(context)


# --- Format Timestamp Helper ---
def format_dt(timestamp: int | None, tz=LITHUANIA_TZ, fmt='%Y-%m-%d %H:%M') -> str:
    """Formats a UTC timestamp into a human-readable string in a specific timezone."""
    if not timestamp: return get_text(0, 'task_value_not_set', lang='en') # Use key for N/A
    try:
        dt_utc = datetime.fromtimestamp(timestamp, UTC_TZ)
        dt_local = dt_utc.astimezone(tz)
        return dt_local.strftime(fmt)
    except (ValueError, TypeError, OSError): # OSError for timestamps too far in past/future
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
    now_ts = int(datetime.now(UTC_TZ).timestamp())
    is_expired = sub_end_ts < now_ts
    end_date = format_dt(sub_end_ts, fmt='%Y-%m-%d') if sub_end_ts else 'N/A'
    expiry_warning = " ⚠️ <b>Expired</b>" if is_expired else ""

    userbot_phones = db.get_client_bots(user_id)
    bot_count = len(userbot_phones)
    parse_mode = ParseMode.HTML # Using HTML for easier formatting

    # Using html.escape for user-provided data like usernames/errors
    menu_text = f"<b>{get_text(user_id, 'client_menu_title', lang=lang, code=html.escape(code))}</b>{expiry_warning}\n"
    menu_text += get_text(user_id, 'client_menu_sub_end', lang=lang, end_date=end_date) + "\n\n"
    menu_text += f"<u>{get_text(user_id, 'client_menu_userbots_title', lang=lang, count=bot_count)}</u>\n"

    if userbot_phones:
        for i, phone in enumerate(userbot_phones, 1):
            bot_db_info = db.find_userbot(phone)
            username = bot_db_info['username'] if bot_db_info else None
            status = bot_db_info['status'].capitalize() if bot_db_info else 'Unknown'
            last_error = bot_db_info['last_error'] if bot_db_info else None
            display_name = html.escape(f"@{username}" if username else phone)
            status_icon = "🟢" if bot_db_info and bot_db_info['status'] == 'active' else \
                          "🟡" if bot_db_info and bot_db_info['status'] not in ['active', 'inactive', 'error'] else \
                          "🔴" if bot_db_info and bot_db_info['status'] == 'error' else "⚪️" # Inactive

            menu_text += f"{i}. {status_icon} {display_name} (<i>Status: {html.escape(status)}</i>)\n"
            if last_error:
                 escaped_error = html.escape(last_error) # Escape potential HTML in error message
                 menu_text += f"  └─ <pre>Error: {escaped_error[:100]}{'...' if len(escaped_error)>100 else ''}</pre>\n"
    else:
        menu_text += get_text(user_id, 'client_menu_no_userbots', lang=lang) + "\n"

    keyboard = [
        [InlineKeyboardButton(get_text(user_id, 'client_menu_button_setup_tasks', lang=lang), callback_data=f"{CALLBACK_CLIENT_PREFIX}select_bot_task")],
        [InlineKeyboardButton(get_text(user_id, 'client_menu_button_manage_folders', lang=lang), callback_data=f"{CALLBACK_CLIENT_PREFIX}manage_folders")],
        [
             InlineKeyboardButton(get_text(user_id, 'client_menu_button_join_groups', lang=lang), callback_data=f"{CALLBACK_CLIENT_PREFIX}select_bot_join"),
             # InlineKeyboardButton(get_text(user_id, 'client_menu_button_view_joined', lang=lang), callback_data=f"{CALLBACK_CLIENT_PREFIX}select_bot_view_joined"), # Simplified - removed separate view joined groups for now
        ],
        [
             # InlineKeyboardButton(get_text(user_id, 'client_menu_button_logs', lang=lang), callback_data=f"{CALLBACK_CLIENT_PREFIX}view_stats"), # Simplified - removed stats button
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
             InlineKeyboardButton(get_text(user_id, 'admin_button_remove_userbot', lang=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}remove_bot_select?page=0") # Add pagination start
        ],
        [InlineKeyboardButton(get_text(user_id, 'admin_button_list_userbots', lang=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}list_bots?page=0")], # Add pagination start
        [InlineKeyboardButton(get_text(user_id, 'admin_button_gen_invite', lang=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}gen_invite_prompt")],
        [InlineKeyboardButton(get_text(user_id, 'admin_button_view_subs', lang=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}view_subs?page=0")], # Add pagination start
        [
             InlineKeyboardButton(get_text(user_id, 'admin_button_extend_sub', lang=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}extend_sub_prompt"),
             InlineKeyboardButton(get_text(user_id, 'admin_button_assign_bots_client', lang=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}assign_bots_prompt")
        ],
        [InlineKeyboardButton(get_text(user_id, 'admin_button_view_logs', lang=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}view_logs?page=0")], # Add pagination start
    ]
    markup = InlineKeyboardMarkup(keyboard)
    return title, markup, parse_mode

# --- Utility for Pagination ---
def build_pagination_buttons(base_callback_data: str, current_page: int, total_items: int, items_per_page: int) -> list:
    buttons = []
    total_pages = math.ceil(total_items / items_per_page)
    if total_pages <= 1:
        return []

    row = []
    if current_page > 0:
        row.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"{base_callback_data}?page={current_page - 1}"))
    # Add page indicator only if there's more than one page
    if total_pages > 1:
         # Use a generic prefix for no-op button
         page_text = get_text(0,'pagination_page',lang='en').format(current=current_page + 1, total=total_pages)
         row.append(InlineKeyboardButton(page_text, callback_data=f"{CALLBACK_GENERIC_PREFIX}noop"))
    if current_page < total_pages - 1:
        row.append(InlineKeyboardButton("Next ➡️", callback_data=f"{base_callback_data}?page={current_page + 1}"))

    if row:
        buttons.append(row)
    return buttons

# --- Command Handlers ---

async def start_command(update: Update, context: CallbackContext) -> str | int:
    """Handles /start: directs users based on status."""
    user_id, lang = get_user_id_and_lang(update, context)
    clear_conversation_data(context) # Clear previous state first
    log.info(f"Start cmd: UserID={user_id}, User={update.effective_user.username}")

    # Check if admin first
    if is_admin(user_id):
        log.info(f"Admin user {user_id} used /start, showing admin menu.")
        return await admin_command(update, context) # Show admin menu directly

    # Check if client
    client_info = db.find_client_by_user_id(user_id)
    if client_info:
        now_ts = int(datetime.now(UTC_TZ).timestamp())
        if client_info['subscription_end'] < now_ts:
            await reply_or_edit_text(update, context, get_text(user_id, 'subscription_expired', lang=lang)) # Use specific key if available
            return ConversationHandler.END
        else:
            return await client_menu(update, context)
    else:
        await reply_or_edit_text(update, context, get_text(user_id, 'welcome', lang=lang))
        return STATE_WAITING_FOR_CODE

async def process_invitation_code(update: Update, context: CallbackContext) -> str | int:
    """Handles the user sending an invitation code."""
    user_id, lang = get_user_id_and_lang(update, context)
    code = update.message.text.strip().lower() # Standardize code to lowercase
    log.info(f"UserID={user_id} submitted code: {code}")

    # Basic format check (e.g., 8 hex characters) - adjust regex if format changes
    if not re.fullmatch(r'[a-f0-9]{8}', code):
        await reply_or_edit_text(update, context, get_text(user_id, 'invalid_code_format', lang=lang))
        return STATE_WAITING_FOR_CODE # Remain in the same state

    success, status_key = db.activate_client(code, user_id)
    text_to_send = get_text(user_id, status_key, lang=lang) # Get text based on activation result

    if success:
        log.info(f"Activated client {user_id} code {code}")
        db.log_event_db("Client Activated", f"Code: {code}", user_id=user_id)
        # Update language in context immediately after successful activation
        context.user_data[CTX_LANG] = db.get_user_language(user_id)
        lang = context.user_data[CTX_LANG]
        await reply_or_edit_text(update, context, text_to_send)
        # Automatically show the client menu after successful activation
        return await client_menu(update, context) # Transition to client menu (ends conversation state)
    else:
        log.warning(f"Failed activation user {user_id} code {code}: {status_key}")
        await reply_or_edit_text(update, context, text_to_send)
        # Determine next state based on error
        if status_key in ["code_not_found", "code_already_used", "subscription_expired"]:
            clear_conversation_data(context)
            return ConversationHandler.END # End conversation on definitive failure
        else:
            # Maybe allow retrying for transient errors? Or just end. Let's end for simplicity.
            clear_conversation_data(context)
            return ConversationHandler.END

async def admin_command(update: Update, context: CallbackContext) -> str | int:
    """Handles the /admin command for authorized admins."""
    user_id, lang = get_user_id_and_lang(update, context)
    clear_conversation_data(context)
    log.info(f"Admin cmd: UserID={user_id}, User={update.effective_user.username}")
    if not is_admin(user_id):
        await reply_or_edit_text(update, context, get_text(user_id, 'unauthorized', lang=lang))
        return ConversationHandler.END

    title, markup, parse_mode = build_admin_menu(user_id, context)
    await reply_or_edit_text(update, context, title, reply_markup=markup, parse_mode=parse_mode)
    # Admin menu is typically the end state unless an action is chosen
    return ConversationHandler.END

async def cancel_command(update: Update, context: CallbackContext) -> int:
    """Generic cancel handler."""
    user_id, lang = get_user_id_and_lang(update, context)
    log.info(f"Cancel cmd: UserID={user_id}")
    current_state = context.user_data.get(ConversationHandler.CURRENT_STATE if hasattr(ConversationHandler, 'CURRENT_STATE') else '_user_data') # Check state
    log.debug(f"Cancel called from state: {current_state}")

    await reply_or_edit_text(update, context, get_text(user_id, 'cancelled', lang=lang))
    clear_conversation_data(context)
    return ConversationHandler.END

async def conversation_fallback(update: Update, context: CallbackContext) -> int:
     """Handles messages not matched in a conversation state."""
     user_id, lang = get_user_id_and_lang(update, context)
     state = context.user_data.get(ConversationHandler.CURRENT_STATE if hasattr(ConversationHandler, 'CURRENT_STATE') else '_user_data') # Check state
     msg_text = update.effective_message.text if update.effective_message else 'Non-text update'
     log.warning(f"Conv fallback: UserID={user_id}. State={state}. Msg='{msg_text[:50]}...'")

     # Check if it's a command that should maybe end the conversation
     if update.message and update.message.text and update.message.text.startswith('/'):
         if update.message.text == '/cancel':
             return await cancel_command(update, context)
         if update.message.text == '/start':
             # Let start command handle redirection
             await reply_or_edit_text(update, context, get_text(user_id, 'state_cleared', lang=lang))
             clear_conversation_data(context)
             return await start_command(update, context)
         if update.message.text == '/admin' and is_admin(user_id):
             await reply_or_edit_text(update, context, get_text(user_id, 'state_cleared', lang=lang))
             clear_conversation_data(context)
             return await admin_command(update, context)

     # If not a recognized command, send fallback message and end
     await reply_or_edit_text(update, context, get_text(user_id, 'conversation_fallback', lang=lang))
     clear_conversation_data(context)
     return ConversationHandler.END

# --- Main Menu & Language ---
async def client_menu(update: Update, context: CallbackContext) -> int:
    """Builds and sends the main client menu."""
    user_id, lang = get_user_id_and_lang(update, context)
    # If called from a callback, make sure context is clean before showing menu
    if update.callback_query:
        clear_conversation_data(context)
        lang = context.user_data[CTX_LANG] # Re-fetch lang

    message, markup, parse_mode = build_client_menu(user_id, context)
    await reply_or_edit_text(update, context, message, reply_markup=markup, parse_mode=parse_mode)
    # Showing the menu is an end state for conversation flows
    return ConversationHandler.END

async def client_ask_select_language(update: Update, context: CallbackContext):
    """Shows language selection buttons."""
    query = update.callback_query
    user_id, lang = get_user_id_and_lang(update, context)
    buttons = []
    row = []
    sorted_languages = sorted(language_names.items(), key=lambda item: item[1]) # Sort by name

    for code, name in sorted_languages:
        row.append(InlineKeyboardButton(name, callback_data=f"{CALLBACK_LANG_PREFIX}{code}"))
        if len(row) >= 2: # Keep rows to max 2 buttons
            buttons.append(row)
            row = []
    if row: # Add remaining button if odd number
        buttons.append(row)

    buttons.append([InlineKeyboardButton(get_text(user_id, 'button_back', lang=lang), callback_data=f"{CALLBACK_CLIENT_PREFIX}back_to_menu")])
    markup = InlineKeyboardMarkup(buttons)
    await reply_or_edit_text(update, context, get_text(user_id, 'select_language', lang=lang), reply_markup=markup)
    return ConversationHandler.END # Remains end state, selection handled by callback

async def set_language_handler(update: Update, context: CallbackContext):
    """Handles language selection callback."""
    query = update.callback_query
    user_id = query.from_user.id
    lang_code = query.data.split(CALLBACK_LANG_PREFIX)[1]
    current_lang = context.user_data.get(CTX_LANG, 'en')

    if lang_code not in language_names:
        await query.answer(get_text(user_id, 'error_invalid_input', lang=current_lang), show_alert=True)
        return ConversationHandler.END

    if db.set_user_language(user_id, lang_code):
         context.user_data[CTX_LANG] = lang_code
         lang = lang_code # Use the new language for the success message
         await reply_or_edit_text(
             update, context,
             get_text(user_id, 'language_set', lang=lang, lang_name=language_names[lang_code]),
             reply_markup=InlineKeyboardMarkup([[ InlineKeyboardButton(get_text(user_id, 'button_main_menu', lang=lang), callback_data=f"{CALLBACK_CLIENT_PREFIX}back_to_menu")]])
         )
    else:
        # Use current language for error message as setting failed
        await query.answer(get_text(user_id, 'language_set_error', lang=current_lang), show_alert=True)

    return ConversationHandler.END # End conversation flow after setting language

# --- Admin Userbot Add Flow (Implemented) ---
async def process_admin_phone(update: Update, context: CallbackContext) -> str | int:
     user_id, lang = get_user_id_and_lang(update, context)
     phone_raw = update.message.text.strip()
     # Simple regex for international format + numbers
     if not re.fullmatch(r'\+\d{9,15}', phone_raw):
          await reply_or_edit_text(update, context, get_text(user_id, 'admin_userbot_invalid_phone', lang=lang))
          return STATE_WAITING_FOR_PHONE # Stay in state

     phone = phone_raw
     context.user_data[CTX_PHONE] = phone
     log.info(f"Admin {user_id} entered phone: {phone}")
     await reply_or_edit_text(update, context, get_text(user_id, 'admin_userbot_prompt_api_id', lang=lang))
     return STATE_WAITING_FOR_API_ID

async def process_admin_api_id(update: Update, context: CallbackContext) -> str | int:
    user_id, lang = get_user_id_and_lang(update, context)
    api_id_str = update.message.text.strip()
    try:
        api_id = int(api_id_str)
        if api_id <= 0: raise ValueError("API ID must be positive")
        context.user_data[CTX_API_ID] = api_id
        log.info(f"Admin {user_id} API ID OK for {context.user_data.get(CTX_PHONE)}")
        await reply_or_edit_text(update, context, get_text(user_id, 'admin_userbot_prompt_api_hash', lang=lang))
        return STATE_WAITING_FOR_API_HASH
    except (ValueError, TypeError):
        await reply_or_edit_text(update, context, get_text(user_id, 'admin_userbot_invalid_api_id', lang=lang))
        return STATE_WAITING_FOR_API_ID # Stay in state

async def process_admin_api_hash(update: Update, context: CallbackContext) -> str | int:
    user_id, lang = get_user_id_and_lang(update, context)
    api_hash = update.message.text.strip()
    # Basic check for non-empty and looks like hex (adjust length check if needed)
    if not api_hash or len(api_hash) < 30 or not re.match('^[a-fA-F0-9]+$', api_hash):
        await reply_or_edit_text(update, context, get_text(user_id, 'admin_userbot_invalid_api_hash', lang=lang))
        return STATE_WAITING_FOR_API_HASH # Stay in state

    context.user_data[CTX_API_HASH] = api_hash
    phone = context.user_data.get(CTX_PHONE)
    api_id = context.user_data.get(CTX_API_ID)

    if not phone or not api_id:
        log.error(f"Admin {user_id} reached API hash step without phone/api_id in context.")
        await reply_or_edit_text(update, context, get_text(user_id, 'session_expired', lang=lang))
        clear_conversation_data(context)
        return ConversationHandler.END

    log.info(f"Admin {user_id} API Hash OK for {phone}. Starting authentication flow.")
    await reply_or_edit_text(update, context, get_text(user_id, 'admin_userbot_auth_connecting', lang=lang, phone=phone))

    try:
        # Run the async function from telethon_utils
        auth_status, auth_data = await telethon_api.start_authentication_flow(phone, api_id, api_hash)
        log.info(f"Authentication start result for {phone}: Status='{auth_status}'")

        if auth_status == 'code_needed':
            context.user_data[CTX_AUTH_DATA] = auth_data # Store data needed for next step
            await reply_or_edit_text(update, context, get_text(user_id, 'admin_userbot_prompt_code', lang=lang, phone=phone))
            return STATE_WAITING_FOR_CODE_USERBOT
        elif auth_status == 'password_needed':
            context.user_data[CTX_AUTH_DATA] = auth_data
            await reply_or_edit_text(update, context, get_text(user_id, 'admin_userbot_prompt_password', lang=lang, phone=phone))
            return STATE_WAITING_FOR_PASSWORD
        elif auth_status == 'already_authorized':
             # This case means the session file already exists and is valid
             log.warning(f"Userbot {phone} is already authorized. Session file likely exists. Ensuring DB record.")
             # Ensure DB record exists or update status
             if not db.find_userbot(phone):
                 session_file_rel = f"{re.sub(r'[^\\d]', '', phone)}.session" # Ensure safe filename
                 db.add_userbot(phone, session_file_rel, api_id, api_hash, 'active')
             else:
                 db.update_userbot_status(phone, 'active') # Mark as active if exists
             # Trigger runtime initialization/check
             asyncio.create_task(telethon_api.get_userbot_runtime_info(phone))
             await reply_or_edit_text(update, context, get_text(user_id, 'admin_userbot_already_auth', lang=lang, display_name=phone))
             clear_conversation_data(context)
             return ConversationHandler.END
        else: # Handle various error cases from start_authentication_flow
            error_msg = auth_data.get('error_message', 'Unknown error')
            log.error(f"Auth start error for {phone}: {error_msg}")
            # Select appropriate user message based on error content
            locals_for_format = {'phone': phone, 'error': error_msg}
            if "flood wait" in error_msg.lower():
                wait_match = re.search(r'(\d+)', error_msg)
                locals_for_format['seconds'] = wait_match.group(1) if wait_match else '?'
                key = 'admin_userbot_auth_error_flood'
            elif "config" in error_msg.lower() or "invalid api" in error_msg.lower() or "invalid cfg" in error_msg.lower():
                key = 'admin_userbot_auth_error_config'
            elif "invalid phone" in error_msg.lower():
                key = 'admin_userbot_auth_error_phone_invalid'
            elif "connection" in error_msg.lower() or "connect failed" in error_msg.lower():
                 key = 'admin_userbot_auth_error_connect'
            else:
                key = 'admin_userbot_auth_error_unknown'

            await reply_or_edit_text(update, context, get_text(user_id, key, lang=lang, **locals_for_format))
            clear_conversation_data(context)
            return ConversationHandler.END

    except Exception as e:
        log.error(f"Exception during start_authentication_flow for {phone}: {e}", exc_info=True)
        await reply_or_edit_text(update, context, get_text(user_id, 'admin_userbot_auth_error_unknown', lang=lang, phone=phone, error=str(e)))
        clear_conversation_data(context)
        return ConversationHandler.END

async def process_admin_userbot_code(update: Update, context: CallbackContext) -> str | int:
    user_id, lang = get_user_id_and_lang(update, context)
    code = update.message.text.strip()
    auth_data = context.user_data.get(CTX_AUTH_DATA)
    phone = context.user_data.get(CTX_PHONE, "N/A")

    if not auth_data:
        await reply_or_edit_text(update, context, get_text(user_id, 'session_expired', lang=lang))
        clear_conversation_data(context)
        return ConversationHandler.END

    # Validate code format (basic - telegram codes are usually digits)
    if not code.isdigit():
         await reply_or_edit_text(update, context, get_text(user_id, 'admin_userbot_auth_error_code_invalid', lang=lang, phone=phone, error="Format incorrect"))
         return STATE_WAITING_FOR_CODE_USERBOT # Stay in state

    await reply_or_edit_text(update, context, get_text(user_id, 'admin_userbot_auth_signing_in', lang=lang, phone=phone))

    try:
        # Call the completion function from telethon_utils
        comp_status, comp_data = await telethon_api.complete_authentication_flow(auth_data, code=code)
        log.info(f"Authentication code complete result for {phone}: Status='{comp_status}'")

        # Clear sensitive auth data from context AFTER the call
        context.user_data.pop(CTX_AUTH_DATA, None)

        if comp_status == 'success':
            phone_num = comp_data.get('phone', phone)
            username = comp_data.get('username')
            display_name = f"@{username}" if username else phone_num
            await reply_or_edit_text(update, context, get_text(user_id, 'admin_userbot_add_success', lang=lang, display_name=display_name))
            # Trigger runtime initialization for the newly added bot
            asyncio.create_task(telethon_api.get_userbot_runtime_info(phone_num))
            clear_conversation_data(context)
            return ConversationHandler.END
        elif comp_status == 'error' and "Password required" in comp_data.get('error_message','').lower():
             # This shouldn't typically happen if start_auth detected password need, but handle defensively
             log.warning(f"Password needed unexpectedly after code for {phone}. Ask user to restart flow.")
             await reply_or_edit_text(update, context, get_text(user_id, 'admin_userbot_auth_error_password_needed_unexpected', lang=lang)) # Need new translation key
             clear_conversation_data(context)
             return ConversationHandler.END
        else: # Handle various error cases from complete_authentication_flow
            error_msg = comp_data.get('error_message', 'Unknown error.')
            log.error(f"Auth code completion error for {phone}: {error_msg}")
            locals_for_format = {'phone': phone, 'error': error_msg}
            if "invalid or expired code" in error_msg.lower() or "invalid" in error_msg.lower() and "code" in error_msg.lower():
                key = 'admin_userbot_auth_error_code_invalid'
            elif "flood wait" in error_msg.lower():
                wait_match = re.search(r'(\d+)', error_msg)
                locals_for_format['seconds'] = wait_match.group(1) if wait_match else '?'
                key = 'admin_userbot_auth_error_flood'
            elif "banned" in error_msg.lower() or "deactivated" in error_msg.lower():
                key = 'admin_userbot_auth_error_account_issue'
            elif "connection" in error_msg.lower():
                key = 'admin_userbot_auth_error_connect'
            else:
                key = 'admin_userbot_auth_error_unknown'

            await reply_or_edit_text(update, context, get_text(user_id, key, lang=lang, **locals_for_format))
            clear_conversation_data(context)
            return ConversationHandler.END

    except Exception as e:
        log.error(f"Exception during complete_authentication_flow (code) for {phone}: {e}", exc_info=True)
        context.user_data.pop(CTX_AUTH_DATA, None) # Ensure auth data is cleared on exception
        await reply_or_edit_text(update, context, get_text(user_id, 'admin_userbot_auth_error_unknown', lang=lang, phone=phone, error=str(e)))
        clear_conversation_data(context)
        return ConversationHandler.END

async def process_admin_userbot_password(update: Update, context: CallbackContext) -> str | int:
    user_id, lang = get_user_id_and_lang(update, context)
    password = update.message.text.strip() # Password itself might have spaces
    auth_data = context.user_data.get(CTX_AUTH_DATA)
    phone = context.user_data.get(CTX_PHONE, "N/A")

    if not auth_data:
        await reply_or_edit_text(update, context, get_text(user_id, 'session_expired', lang=lang))
        clear_conversation_data(context)
        return ConversationHandler.END

    # Basic check: password shouldn't be empty
    if not password:
         await reply_or_edit_text(update, context, get_text(user_id, 'error_invalid_input', lang=lang)) # Generic invalid input
         return STATE_WAITING_FOR_PASSWORD # Stay in state

    await reply_or_edit_text(update, context, get_text(user_id, 'admin_userbot_auth_signing_in', lang=lang, phone=phone))

    try:
        # Call completion function with password
        comp_status, comp_data = await telethon_api.complete_authentication_flow(auth_data, password=password)
        log.info(f"Authentication password complete result for {phone}: Status='{comp_status}'")

        context.user_data.pop(CTX_AUTH_DATA, None) # Clear sensitive data AFTER call

        if comp_status == 'success':
            phone_num = comp_data.get('phone', phone)
            username = comp_data.get('username')
            display_name = f"@{username}" if username else phone_num
            await reply_or_edit_text(update, context, get_text(user_id, 'admin_userbot_add_success', lang=lang, display_name=display_name))
            # Trigger runtime initialization
            asyncio.create_task(telethon_api.get_userbot_runtime_info(phone_num))
            clear_conversation_data(context)
            return ConversationHandler.END
        else: # Handle errors
            error_msg = comp_data.get('error_message', 'Unknown error.')
            log.error(f"Auth password completion error for {phone}: {error_msg}")
            locals_for_format = {'phone': phone, 'error': error_msg}
            if "incorrect password" in error_msg.lower() or "password_hash_invalid" in error_msg.lower():
                key = 'admin_userbot_auth_error_password_invalid'
            elif "flood wait" in error_msg.lower():
                wait_match = re.search(r'(\d+)', error_msg)
                locals_for_format['seconds'] = wait_match.group(1) if wait_match else '?'
                key = 'admin_userbot_auth_error_flood'
            elif "banned" in error_msg.lower() or "deactivated" in error_msg.lower():
                key = 'admin_userbot_auth_error_account_issue'
            elif "connection" in error_msg.lower():
                key = 'admin_userbot_auth_error_connect'
            else:
                key = 'admin_userbot_auth_error_unknown'

            await reply_or_edit_text(update, context, get_text(user_id, key, lang=lang, **locals_for_format))
            clear_conversation_data(context)
            return ConversationHandler.END

    except Exception as e:
        log.error(f"Exception during complete_authentication_flow (password) for {phone}: {e}", exc_info=True)
        context.user_data.pop(CTX_AUTH_DATA, None) # Ensure cleanup
        await reply_or_edit_text(update, context, get_text(user_id, 'admin_userbot_auth_error_unknown', lang=lang, phone=phone, error=str(e)))
        clear_conversation_data(context)
        return ConversationHandler.END


# --- Admin - Other Flows ---

# --- Generate Invite Code ---
async def process_admin_invite_details(update: Update, context: CallbackContext) -> int:
    user_id, lang = get_user_id_and_lang(update, context)
    details_text = update.message.text.strip().lower()
    log.info(f"Admin {user_id} entered invite details: {details_text}")

    # Regex to parse "Xd Yb" format
    match = re.match(r'(\d+)\s*d\s+(\d+)\s*b', details_text)
    if not match:
        await reply_or_edit_text(update, context, get_text(user_id, 'admin_invite_invalid_format', lang=lang))
        return STATE_WAITING_FOR_SUB_DETAILS # Stay in state

    try:
        days = int(match.group(1))
        bots_needed = int(match.group(2))
        if days <= 0 or bots_needed <= 0:
            raise ValueError("Days and bots must be positive.")
    except ValueError:
        await reply_or_edit_text(update, context, get_text(user_id, 'admin_invite_invalid_numbers', lang=lang))
        return STATE_WAITING_FOR_SUB_DETAILS # Stay in state

    log.info(f"Admin {user_id} requesting code for {days} days, {bots_needed} bots.")
    await reply_or_edit_text(update, context, get_text(user_id, 'admin_invite_generating', lang=lang)) # Indicate processing

    # Check availability of unassigned bots (THIS IS OPTIONAL - Original code didn't require assignment here)
    # If you want to pre-assign bots with the code, uncomment and adapt this section
    # available_bots = db.get_unassigned_userbots(limit=bots_needed)
    # if len(available_bots) < bots_needed:
    #     await reply_or_edit_text(update, context, get_text(user_id, 'admin_invite_no_bots_available', lang=lang, needed=bots_needed, available=len(available_bots)))
    #     clear_conversation_data(context)
    #     return ConversationHandler.END

    # Generate unique code (UUID based)
    code = str(uuid.uuid4().hex)[:8] # 8-char hex code

    # Calculate subscription end timestamp
    now = datetime.now(UTC_TZ)
    end_datetime = now + timedelta(days=days)
    sub_end_ts = int(end_datetime.timestamp())

    # Create the invitation record in the database
    if db.create_invitation(code, sub_end_ts):
        end_date_str = format_dt(sub_end_ts, fmt='%Y-%m-%d %H:%M UTC')
        db.log_event_db("Invite Generated", f"Code: {code}, Days: {days}, Bot Count: {bots_needed}", user_id=user_id)
        await reply_or_edit_text(
            update, context,
            get_text(user_id, 'admin_invite_success', lang=lang, code=code, end_date=end_date_str, count=bots_needed)
        )
        # Optionally assign bots here if pre-assignment logic was used
        # db.assign_userbots_to_client(code, available_bots)
    else:
        db.log_event_db("Invite Gen Failed", f"Code: {code} (duplicate?)", user_id=user_id)
        await reply_or_edit_text(update, context, get_text(user_id, 'admin_invite_db_error', lang=lang)) # Or a specific "code exists" error

    clear_conversation_data(context)
    return ConversationHandler.END

# --- Extend Subscription ---
async def process_admin_extend_code(update: Update, context: CallbackContext) -> int:
    user_id, lang = get_user_id_and_lang(update, context)
    code = update.message.text.strip().lower()
    log.info(f"Admin {user_id} entered code to extend: {code}")

    client = db.find_client_by_code(code)
    if not client:
        await reply_or_edit_text(update, context, get_text(user_id, 'admin_extend_invalid_code', lang=lang))
        return STATE_WAITING_FOR_EXTEND_CODE # Stay in state

    context.user_data[CTX_EXTEND_CODE] = code
    end_date_str = format_dt(client['subscription_end'])
    await reply_or_edit_text(
        update, context,
        get_text(user_id, 'admin_extend_prompt_days', lang=lang, code=code, end_date=end_date_str)
    )
    return STATE_WAITING_FOR_EXTEND_DAYS

async def process_admin_extend_days(update: Update, context: CallbackContext) -> int:
    user_id, lang = get_user_id_and_lang(update, context)
    days_str = update.message.text.strip()
    code = context.user_data.get(CTX_EXTEND_CODE)

    if not code:
        await reply_or_edit_text(update, context, get_text(user_id, 'session_expired', lang=lang))
        clear_conversation_data(context)
        return ConversationHandler.END

    try:
        days_to_add = int(days_str)
        if days_to_add <= 0:
            raise ValueError("Days must be positive.")
    except ValueError:
        await reply_or_edit_text(update, context, get_text(user_id, 'admin_extend_invalid_days', lang=lang))
        return STATE_WAITING_FOR_EXTEND_DAYS # Stay in state

    client = db.find_client_by_code(code)
    if not client: # Double check client still exists
        await reply_or_edit_text(update, context, get_text(user_id, 'admin_extend_invalid_code', lang=lang))
        clear_conversation_data(context)
        return ConversationHandler.END

    current_end_ts = client['subscription_end']
    now_ts = int(datetime.now(UTC_TZ).timestamp())

    # If subscription already expired, extend from NOW. Otherwise, extend from current end date.
    start_ts = max(now_ts, current_end_ts)
    start_dt = datetime.fromtimestamp(start_ts, UTC_TZ)
    new_end_dt = start_dt + timedelta(days=days_to_add)
    new_end_ts = int(new_end_dt.timestamp())

    if db.extend_subscription(code, new_end_ts):
        new_end_date_str = format_dt(new_end_ts)
        db.log_event_db("Subscription Extended", f"Code: {code}, Added: {days_to_add} days", user_id=user_id, client_id=client.get('user_id'))
        await reply_or_edit_text(
            update, context,
            get_text(user_id, 'admin_extend_success', lang=lang, code=code, days=days_to_add, new_end_date=new_end_date_str)
        )
    else:
        db.log_event_db("Sub Extend Failed", f"Code: {code}", user_id=user_id)
        await reply_or_edit_text(update, context, get_text(user_id, 'admin_extend_db_error', lang=lang))

    clear_conversation_data(context)
    return ConversationHandler.END

# --- Assign Userbots to Client ---
async def process_admin_add_bots_code(update: Update, context: CallbackContext) -> int:
    user_id, lang = get_user_id_and_lang(update, context)
    code = update.message.text.strip().lower()
    log.info(f"Admin {user_id} entered code to assign bots: {code}")

    client = db.find_client_by_code(code)
    if not client:
        await reply_or_edit_text(update, context, get_text(user_id, 'admin_assignbots_invalid_code', lang=lang))
        return STATE_WAITING_FOR_ADD_USERBOTS_CODE # Stay in state

    context.user_data[CTX_ADD_BOTS_CODE] = code
    # Count currently assigned bots based on userbots table, not the potentially outdated client.dedicated_userbots
    current_bots = db.get_client_bots(client.get('user_id')) if client.get('user_id') else \
                   [b['phone_number'] for b in db.get_all_userbots(assigned_status=True) if b['assigned_client'] == code]
    current_count = len(current_bots)

    await reply_or_edit_text(
        update, context,
        get_text(user_id, 'admin_assignbots_prompt_count', lang=lang, code=code, current_count=current_count)
    )
    return STATE_WAITING_FOR_ADD_USERBOTS_COUNT

async def process_admin_add_bots_count(update: Update, context: CallbackContext) -> int:
    user_id, lang = get_user_id_and_lang(update, context)
    count_str = update.message.text.strip()
    code = context.user_data.get(CTX_ADD_BOTS_CODE)

    if not code:
        await reply_or_edit_text(update, context, get_text(user_id, 'session_expired', lang=lang))
        clear_conversation_data(context)
        return ConversationHandler.END

    try:
        count_to_add = int(count_str)
        if count_to_add <= 0:
            raise ValueError("Count must be positive.")
    except ValueError:
        await reply_or_edit_text(update, context, get_text(user_id, 'admin_assignbots_invalid_count', lang=lang))
        return STATE_WAITING_FOR_ADD_USERBOTS_COUNT # Stay in state

    log.info(f"Admin {user_id} trying to assign {count_to_add} bots to code {code}")

    # Find available (active and unassigned) userbots
    available_bots = db.get_unassigned_userbots(limit=count_to_add)

    if len(available_bots) < count_to_add:
        await reply_or_edit_text(
            update, context,
            get_text(user_id, 'admin_assignbots_no_bots_available', lang=lang, needed=count_to_add, available=len(available_bots))
        )
        clear_conversation_data(context)
        return ConversationHandler.END

    # Assign the bots
    success, message = db.assign_userbots_to_client(code, available_bots) # Pass the list of phones

    if success:
        client_user_id = db.find_client_by_code(code)['user_id']
        db.log_event_db("Userbots Assigned", f"Code: {code}, Count: {len(available_bots)}, Bots: {','.join(available_bots)}", user_id=user_id, client_id=client_user_id)

        # Use different message for partial success
        assigned_count_match = re.search(r"Successfully assigned (\d+) userbots", message)
        if assigned_count_match and int(assigned_count_match.group(1)) == len(available_bots):
             final_message = get_text(user_id, 'admin_assignbots_success', lang=lang, count=len(available_bots), code=code)
        elif assigned_count_match:
             assigned_count = int(assigned_count_match.group(1))
             final_message = get_text(user_id, 'admin_assignbots_partial_success', lang=lang, assigned_count=assigned_count, requested_count=len(available_bots), code=code)
        else: # Fallback if message format changed
             final_message = message # Use the raw message from DB function

        await reply_or_edit_text(update, context, final_message)

        # Initialize runtime for newly assigned bots if they aren't running
        for phone in available_bots:
            asyncio.create_task(telethon_api.get_userbot_runtime_info(phone))
    else:
        db.log_event_db("Bot Assign Failed", f"Code: {code}, Reason: {message}", user_id=user_id)
        fail_message = get_text(user_id, 'admin_assignbots_failed', lang=lang, code=code) + f"\nError: {message}"
        await reply_or_edit_text(update, context, fail_message) # Provide more context on failure

    clear_conversation_data(context)
    return ConversationHandler.END

# --- Folder Management ---
async def client_folder_menu(update: Update, context: CallbackContext) -> int:
    user_id, lang = get_user_id_and_lang(update, context)
    clear_conversation_data(context) # Clear previous state
    lang = context.user_data[CTX_LANG] # Re-fetch lang

    folders = db.get_folders_by_user(user_id)

    text = get_text(user_id, 'folder_menu_title', lang=lang)
    keyboard = []
    # Add create button first
    keyboard.append([InlineKeyboardButton(get_text(user_id, 'folder_menu_create', lang=lang), callback_data=f"{CALLBACK_FOLDER_PREFIX}create_prompt")])

    if folders:
        # Add edit/delete buttons if folders exist
        keyboard.append([InlineKeyboardButton(get_text(user_id, 'folder_menu_edit', lang=lang), callback_data=f"{CALLBACK_FOLDER_PREFIX}select_edit?page=0")])
        keyboard.append([InlineKeyboardButton(get_text(user_id, 'folder_menu_delete', lang=lang), callback_data=f"{CALLBACK_FOLDER_PREFIX}select_delete?page=0")])
    else:
        text += "\n" + get_text(user_id, 'folder_no_folders', lang=lang)

    # Always add back button
    keyboard.append([InlineKeyboardButton(get_text(user_id, 'button_back', lang=lang), callback_data=f"{CALLBACK_CLIENT_PREFIX}back_to_menu")])

    markup = InlineKeyboardMarkup(keyboard)
    await reply_or_edit_text(update, context, text, reply_markup=markup)
    return ConversationHandler.END # Folder menu is an end state

async def process_folder_name(update: Update, context: CallbackContext) -> int:
    user_id, lang = get_user_id_and_lang(update, context)
    folder_name = update.message.text.strip()

    if not folder_name:
        await reply_or_edit_text(update, context, get_text(user_id, 'error_invalid_input', lang=lang))
        return STATE_WAITING_FOR_FOLDER_NAME # Stay in state

    log.info(f"User {user_id} attempting to create folder: {folder_name}")
    folder_id_or_status = db.add_folder(folder_name, user_id)

    if isinstance(folder_id_or_status, int) and folder_id_or_status > 0:
        folder_id = folder_id_or_status
        db.log_event_db("Folder Created", f"Name: {folder_name}, ID: {folder_id}", user_id=user_id)
        await reply_or_edit_text(
            update, context,
            get_text(user_id, 'folder_create_success', lang=lang, name=html.escape(folder_name))
        )
        # Go back to folder menu after creation
        return await client_folder_menu(update, context)
    elif folder_id_or_status is None: # Indicates duplicate name
         await reply_or_edit_text(update, context, get_text(user_id, 'folder_create_error_exists', lang=lang, name=html.escape(folder_name)))
         return STATE_WAITING_FOR_FOLDER_NAME # Allow user to enter a different name
    else: # Database error (returned -1)
        db.log_event_db("Folder Create Failed", f"Name: {folder_name}", user_id=user_id)
        await reply_or_edit_text(update, context, get_text(user_id, 'folder_create_error_db', lang=lang))
        clear_conversation_data(context)
        return ConversationHandler.END

async def client_select_folder_to_edit_or_delete(update: Update, context: CallbackContext, action: str) -> int:
    """Generic function to show folder list for selection (edit/delete)."""
    query = update.callback_query
    user_id, lang = get_user_id_and_lang(update, context)
    _, page_data = query.data.split('?', 1)
    current_page = int(page_data.split('=')[1])

    folders = db.get_folders_by_user(user_id)
    if not folders:
        await query.answer(get_text(user_id, 'folder_no_folders', lang=lang), show_alert=True)
        return await client_folder_menu(update, context) # Go back if no folders

    total_items = len(folders)
    start_index = current_page * ITEMS_PER_PAGE
    end_index = start_index + ITEMS_PER_PAGE
    folders_page = folders[start_index:end_index]

    text_key = 'folder_select_edit' if action == 'edit' else 'folder_select_delete'
    text = get_text(user_id, text_key, lang=lang)
    keyboard = []
    for folder in folders_page:
        # Use html.escape for folder names
        button_text = html.escape(folder['name'])
        callback_action = "edit_selected" if action == 'edit' else "delete_selected" # Adjust callback based on action
        callback_data = f"{CALLBACK_FOLDER_PREFIX}{callback_action}?id={folder['id']}"
        keyboard.append([InlineKeyboardButton(button_text, callback_data=callback_data)])

    base_callback = f"{CALLBACK_FOLDER_PREFIX}select_{action}" # e.g., folder_select_edit
    pagination_buttons = build_pagination_buttons(base_callback, current_page, total_items, ITEMS_PER_PAGE)
    keyboard.extend(pagination_buttons)

    keyboard.append([InlineKeyboardButton(get_text(user_id, 'button_back', lang=lang), callback_data=f"{CALLBACK_FOLDER_PREFIX}back_to_manage")])

    markup = InlineKeyboardMarkup(keyboard)
    await reply_or_edit_text(update, context, text, reply_markup=markup)
    return ConversationHandler.END # Selection is handled by callback

async def client_show_folder_edit_options(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    user_id, lang = get_user_id_and_lang(update, context)
    # Extract folder ID from callback data OR context if returning from another step
    folder_id = context.user_data.get(CTX_FOLDER_ID)
    if not folder_id and query and '?' in query.data:
         try:
             _, params = query.data.split('?', 1)
             folder_id = int(params.split('=')[1])
             context.user_data[CTX_FOLDER_ID] = folder_id # Store it
         except (ValueError, IndexError):
              folder_id = None

    if not folder_id:
        log.error(f"Could not determine folder ID for edit options. User: {user_id}, Callback: {query.data if query else 'N/A'}")
        if query: await query.answer(get_text(user_id, 'error_generic', lang=lang), show_alert=True)
        return await client_folder_menu(update, context)

    folder_name = db.get_folder_name(folder_id)
    if not folder_name:
        await query.answer(get_text(user_id, 'folder_not_found_error', lang=lang), show_alert=True)
        return await client_folder_menu(update, context)

    # Store/update folder name in context
    context.user_data[CTX_FOLDER_NAME] = folder_name

    groups_in_folder = db.get_target_groups_details_by_folder(folder_id)

    text = get_text(user_id, 'folder_edit_title', lang=lang, name=html.escape(folder_name))
    text += "\n" + get_text(user_id, 'folder_edit_groups_intro', lang=lang)
    if groups_in_folder:
        for group in groups_in_folder[:10]: # Show first 10 groups
            link = group['group_link']
            name = group['group_name'] or f"ID: {group['group_id']}" # Use ID if name missing
            text += f"\n- <a href='{html.escape(link)}'>{html.escape(name)}</a>" if link else f"\n- {html.escape(name)}"
        if len(groups_in_folder) > 10:
            text += f"\n... and {len(groups_in_folder) - 10} more."
    else:
        text += "\n" + get_text(user_id, 'folder_edit_no_groups', lang=lang)

    keyboard = [
        # [InlineKeyboardButton(get_text(user_id, 'folder_edit_action_update', lang=lang), callback_data=f"{CALLBACK_FOLDER_PREFIX}edit_update_prompt")], # Update (replace) seems complex, focusing on add/remove
        [InlineKeyboardButton(get_text(user_id, 'folder_edit_action_add', lang=lang), callback_data=f"{CALLBACK_FOLDER_PREFIX}edit_add_prompt")],
        [InlineKeyboardButton(get_text(user_id, 'folder_edit_action_remove', lang=lang), callback_data=f"{CALLBACK_FOLDER_PREFIX}edit_remove_select?page=0")],
        [InlineKeyboardButton(get_text(user_id, 'folder_edit_action_rename', lang=lang), callback_data=f"{CALLBACK_FOLDER_PREFIX}edit_rename_prompt")],
        # [InlineKeyboardButton(get_text(user_id, 'folder_edit_action_delete', lang=lang), callback_data=f"{CALLBACK_FOLDER_PREFIX}delete_confirm")], # Delete is handled via main folder menu
        [InlineKeyboardButton(get_text(user_id, 'button_back', lang=lang), callback_data=f"{CALLBACK_FOLDER_PREFIX}back_to_manage")]
    ]
    markup = InlineKeyboardMarkup(keyboard)
    await reply_or_edit_text(update, context, text, reply_markup=markup, disable_web_page_preview=True)
    # This menu stays within the conversation, waiting for the next action
    return STATE_WAITING_FOR_FOLDER_ACTION

async def process_folder_links(update: Update, context: CallbackContext) -> int:
    user_id, lang = get_user_id_and_lang(update, context)
    folder_id = context.user_data.get(CTX_FOLDER_ID)
    folder_name = context.user_data.get(CTX_FOLDER_NAME)
    # Determine if adding or replacing (if replace is implemented later)
    # action = context.user_data.get(CTX_FOLDER_ACTION, 'add') # Default to add

    if not folder_id or not folder_name:
        await reply_or_edit_text(update, context, get_text(user_id, 'session_expired', lang=lang))
        clear_conversation_data(context)
        return ConversationHandler.END

    links_text = update.message.text
    raw_links = [link.strip() for link in links_text.splitlines() if link.strip()]

    if not raw_links:
        await reply_or_edit_text(update, context, get_text(user_id, 'join_no_links', lang=lang)) # Re-use join message
        # Decide if we stay in state or go back
        # Let's stay in state to allow user to paste correct links
        return STATE_WAITING_FOR_GROUP_LINKS

    await reply_or_edit_text(update, context, get_text(user_id, 'folder_processing_links', lang=lang))

    results = {}
    added_count = 0
    failed_count = 0
    ignored_count = 0

    # Get *any* active client bot to resolve links. Choose one randomly? Or first?
    client_bots = db.get_client_bots(user_id)
    resolver_bot_phone = None
    if client_bots:
         active_client_bots = [b for b in client_bots if (bot_info := db.find_userbot(b)) and bot_info['status'] == 'active']
         if active_client_bots:
              resolver_bot_phone = random.choice(active_client_bots)
              log.info(f"User {user_id} adding folder links. Using bot {resolver_bot_phone} for resolving.")

    link_details = {} # Store resolved {link: {'id': 123, 'name': 'abc'}}
    if resolver_bot_phone:
        try:
            log.debug(f"Calling resolve_links_info for {len(raw_links)} links via bot {resolver_bot_phone}")
            resolved_data = await telethon_api.resolve_links_info(resolver_bot_phone, raw_links)
            link_details.update(resolved_data)
            log.debug(f"Resolved {len(link_details)} links.")
        except Exception as resolve_e:
             log.error(f"Error resolving folder links via bot {resolver_bot_phone}: {resolve_e}")
             # Proceed without resolving if it fails

    for link in raw_links:
        group_id = None
        group_name = None
        reason = None
        status_code = 'failed' # Default status code

        resolved = link_details.get(link)
        if resolved and not resolved.get('error'):
            group_id = resolved.get('id')
            group_name = resolved.get('name')
            # If ID resolved, proceed to add
            if group_id:
                 added = db.add_target_group(group_id, group_name, link, user_id, folder_id)
                 if added: status_code = 'added'; added_count += 1
                 elif added is False: status_code = 'ignored'; ignored_count += 1 # False from DB means duplicate ignored
                 else: status_code = 'failed'; reason = get_text(user_id, 'folder_add_db_error', lang=lang); failed_count += 1 # Should not happen often
            else:
                 # Resolved but no ID? Should not happen with current resolve_links_info
                 status_code = 'failed'; reason = get_text(user_id, 'folder_resolve_error', lang=lang) + " (No ID)"; failed_count += 1
        elif resolved and resolved.get('error'):
             # Resolver returned an error
             status_code = 'failed'; reason = resolved.get('error'); failed_count += 1
        else:
            # Could not resolve link at all
            status_code = 'failed'; reason = get_text(user_id, 'folder_resolve_error', lang=lang); failed_count += 1


        results[link] = {'status': status_code, 'reason': reason}

    # --- Report Results ---
    result_text = get_text(user_id, 'folder_results_title', lang=lang, name=html.escape(folder_name))
    result_text += f"\n(Added: {added_count}, Ignored: {ignored_count}, Failed: {failed_count})\n" # Show counts

    # Limit displayed results to avoid huge messages
    display_limit = 20
    displayed_count = 0
    for link, res in results.items():
        if displayed_count >= display_limit:
            result_text += f"\n...and {len(results) - displayed_count} more."
            break
        status_key = f"folder_results_{res['status']}" # e.g., folder_results_added
        status_text = get_text(user_id, status_key, lang=lang)
        if res['status'] == 'failed' and res['reason']:
             status_text += f" ({html.escape(str(res['reason']))})" # Ensure reason is string
        # Escape the link itself before including it
        result_text += "\n" + get_text(user_id, 'folder_results_line', lang=lang, link=html.escape(link), status=status_text)
        displayed_count += 1

    # Go back to the folder edit menu
    await reply_or_edit_text(update, context, result_text, disable_web_page_preview=True)
    # After processing, return to the edit options menu for that folder
    # We need to pass the folder_id back into the state or function call
    # Option 1: Store folder_id in context (already done) and call function directly
    # Option 2: Use a callback button
    # Let's call the function directly. It will rebuild the menu.
    return await client_show_folder_edit_options(update, context)


async def client_select_groups_to_remove(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    user_id, lang = get_user_id_and_lang(update, context)
    folder_id = context.user_data.get(CTX_FOLDER_ID)
    folder_name = context.user_data.get(CTX_FOLDER_NAME)

    if not folder_id or not folder_name:
        await query.answer(get_text(user_id, 'session_expired', lang=lang), show_alert=True)
        return await client_folder_menu(update, context)

    try:
        _, params = query.data.split('?', 1)
        current_page = int(params.split('=')[1])
    except (ValueError, IndexError):
        log.warning(f"Could not parse page from callback data: {query.data}. Defaulting to page 0.")
        current_page = 0

    groups = db.get_target_groups_details_by_folder(folder_id)
    if not groups:
        await query.answer(get_text(user_id, 'folder_edit_no_groups', lang=lang), show_alert=True)
        # Go back to edit options if no groups to remove
        return await client_show_folder_edit_options(update, context)

    # Get groups selected so far in this session
    selected_ids = set(context.user_data.get(CTX_TARGET_GROUP_IDS_TO_REMOVE, []))

    total_items = len(groups)
    start_index = current_page * ITEMS_PER_PAGE
    end_index = start_index + ITEMS_PER_PAGE
    groups_page = groups[start_index:end_index]

    text = get_text(user_id, 'folder_edit_remove_select', lang=lang, name=html.escape(folder_name))
    keyboard = []
    for group in groups_page:
        db_id = group['id']
        is_selected = db_id in selected_ids
        prefix = "✅ " if is_selected else "➖ "
        link_text = group['group_link'] or f"ID: {group['group_id']}"
        display_text = group['group_name'] or link_text
        button_text = prefix + html.escape(display_text[:40]) + ("..." if len(display_text) > 40 else "") # Truncate long names
        # Callback toggles selection status
        callback_data = f"{CALLBACK_FOLDER_PREFIX}edit_toggle_remove?id={db_id}&page={current_page}"
        keyboard.append([InlineKeyboardButton(button_text, callback_data=callback_data)])

    # Pagination
    base_callback = f"{CALLBACK_FOLDER_PREFIX}edit_remove_select"
    pagination_buttons = build_pagination_buttons(base_callback, current_page, total_items, ITEMS_PER_PAGE)
    keyboard.extend(pagination_buttons)

    # Action buttons (Confirm, Back)
    action_row = []
    if selected_ids: # Show confirm only if something is selected
        action_row.append(InlineKeyboardButton(
            get_text(user_id, 'folder_edit_remove_confirm_title', lang=lang) + f" ({len(selected_ids)})",
            callback_data=f"{CALLBACK_FOLDER_PREFIX}edit_remove_confirm"
        ))
    action_row.append(InlineKeyboardButton(
        get_text(user_id, 'button_back', lang=lang),
        callback_data=f"{CALLBACK_FOLDER_PREFIX}back_to_edit_options" # Go back to the main edit menu
    ))
    keyboard.append(action_row)

    markup = InlineKeyboardMarkup(keyboard)
    await reply_or_edit_text(update, context, text, reply_markup=markup)

    # Stay in this state to allow toggling/pagination/confirmation
    return STATE_FOLDER_EDIT_REMOVE_SELECT


async def client_toggle_group_for_removal(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    user_id, lang = get_user_id_and_lang(update, context)

    try:
        parts = query.data.split('?')[1].split('&')
        group_db_id = int(parts[0].split('=')[1])
        current_page = int(parts[1].split('=')[1])
    except (ValueError, IndexError):
        log.error(f"Could not parse group ID/page from callback: {query.data}")
        await query.answer(get_text(user_id, 'error_generic', lang=lang), show_alert=True)
        return STATE_FOLDER_EDIT_REMOVE_SELECT # Stay in state

    # Initialize set if not present
    if CTX_TARGET_GROUP_IDS_TO_REMOVE not in context.user_data:
        context.user_data[CTX_TARGET_GROUP_IDS_TO_REMOVE] = set()

    # Toggle selection
    if group_db_id in context.user_data[CTX_TARGET_GROUP_IDS_TO_REMOVE]:
        context.user_data[CTX_TARGET_GROUP_IDS_TO_REMOVE].remove(group_db_id)
    else:
        context.user_data[CTX_TARGET_GROUP_IDS_TO_REMOVE].add(group_db_id)

    # Edit the message to reflect the change (re-render the current page)
    # Need to call the function that builds the selection menu again
    # We need to simulate the original callback data for that page
    query.data = f"{CALLBACK_FOLDER_PREFIX}edit_remove_select?page={current_page}" # Modify query data
    return await client_select_groups_to_remove(update, context)


async def client_confirm_remove_selected_groups(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    user_id, lang = get_user_id_and_lang(update, context)
    folder_id = context.user_data.get(CTX_FOLDER_ID)
    folder_name = context.user_data.get(CTX_FOLDER_NAME)
    ids_to_remove = list(context.user_data.get(CTX_TARGET_GROUP_IDS_TO_REMOVE, []))

    if not folder_id or not folder_name or not ids_to_remove:
        await query.answer(get_text(user_id, 'folder_edit_remove_none_selected', lang=lang), show_alert=True)
        # Go back to selection state if nothing was selected somehow
        return STATE_FOLDER_EDIT_REMOVE_SELECT

    removed_count = db.remove_target_groups_by_db_id(ids_to_remove, user_id)

    if removed_count >= 0:
        db.log_event_db("Folder Groups Removed", f"Folder: {folder_name}({folder_id}), Count: {removed_count}, IDs: {ids_to_remove}", user_id=user_id)
        await reply_or_edit_text(
            update, context,
            get_text(user_id, 'folder_edit_remove_success', lang=lang, count=removed_count, name=html.escape(folder_name))
        )
        # Clear selection and return to folder edit options
        context.user_data.pop(CTX_TARGET_GROUP_IDS_TO_REMOVE, None)
        return await client_show_folder_edit_options(update, context)
    else:
        db.log_event_db("Folder Group Remove Failed", f"Folder: {folder_name}({folder_id})", user_id=user_id)
        await reply_or_edit_text(update, context, get_text(user_id, 'folder_edit_remove_error', lang=lang))
        # Stay in selection state on error? Or go back? Let's go back to edit options.
        context.user_data.pop(CTX_TARGET_GROUP_IDS_TO_REMOVE, None)
        return await client_show_folder_edit_options(update, context)


async def process_folder_rename(update: Update, context: CallbackContext) -> int:
    user_id, lang = get_user_id_and_lang(update, context)
    new_name = update.message.text.strip()
    folder_id = context.user_data.get(CTX_FOLDER_ID)
    current_name = context.user_data.get(CTX_FOLDER_NAME)

    if not folder_id or not current_name:
        await reply_or_edit_text(update, context, get_text(user_id, 'session_expired', lang=lang))
        clear_conversation_data(context)
        return ConversationHandler.END

    if not new_name:
        await reply_or_edit_text(update, context, get_text(user_id, 'error_invalid_input', lang=lang))
        return STATE_FOLDER_RENAME_PROMPT # Stay in state

    if new_name == current_name:
        # No change, just go back to edit menu
        return await client_show_folder_edit_options(update, context)

    # Attempt rename using DB function
    success, reason = db.rename_folder(folder_id, user_id, new_name)

    if success:
        db.log_event_db("Folder Renamed", f"ID: {folder_id}, From: {current_name}, To: {new_name}", user_id=user_id)
        await reply_or_edit_text(
            update, context,
            get_text(user_id, 'folder_edit_rename_success', lang=lang, new_name=html.escape(new_name))
        )
        # Update context and return to edit menu
        context.user_data[CTX_FOLDER_NAME] = new_name
        return await client_show_folder_edit_options(update, context)
    else:
        if reason == "name_exists":
             await reply_or_edit_text(update, context, get_text(user_id, 'folder_edit_rename_error_exists', lang=lang, new_name=html.escape(new_name)))
             return STATE_FOLDER_RENAME_PROMPT # Stay in state
        else: # db_error or not_found
             db.log_event_db("Folder Rename Failed", f"ID: {folder_id}, To: {new_name}, Reason: {reason}", user_id=user_id)
             await reply_or_edit_text(update, context, get_text(user_id, 'folder_edit_rename_error_db', lang=lang))
             # Go back to edit menu even on failure
             return await client_show_folder_edit_options(update, context)


async def client_confirm_folder_delete(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    user_id, lang = get_user_id_and_lang(update, context)
    try:
        # Callback data is folder_delete_selected?id=...
        _, params = query.data.split('?', 1)
        folder_id = int(params.split('=')[1])
    except (ValueError, IndexError):
        log.error(f"Could not parse folder ID for delete confirm: {query.data}")
        await query.answer(get_text(user_id, 'error_generic', lang=lang), show_alert=True)
        return await client_folder_menu(update, context)

    folder_name = db.get_folder_name(folder_id)
    if not folder_name:
        await query.answer(get_text(user_id, 'folder_not_found_error', lang=lang), show_alert=True)
        return await client_folder_menu(update, context)

    text = get_text(user_id, 'folder_delete_confirm', lang=lang, name=html.escape(folder_name))
    keyboard = [
        [
            InlineKeyboardButton(get_text(user_id, 'button_yes', lang=lang), callback_data=f"{CALLBACK_FOLDER_PREFIX}delete_confirmed?id={folder_id}"),
            InlineKeyboardButton(get_text(user_id, 'button_no', lang=lang), callback_data=f"{CALLBACK_FOLDER_PREFIX}back_to_manage") # Go back to folder menu
        ]
    ]
    markup = InlineKeyboardMarkup(keyboard)
    await reply_or_edit_text(update, context, text, reply_markup=markup)
    return ConversationHandler.END # Confirmation buttons end the flow


async def client_delete_folder_confirmed(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    user_id, lang = get_user_id_and_lang(update, context)
    try:
        _, params = query.data.split('?', 1)
        folder_id = int(params.split('=')[1])
    except (ValueError, IndexError):
        log.error(f"Could not parse folder ID for delete confirmed: {query.data}")
        await query.answer(get_text(user_id, 'error_generic', lang=lang), show_alert=True)
        return await client_folder_menu(update, context)

    folder_name = db.get_folder_name(folder_id) # Get name for logging before deleting

    if db.delete_folder(folder_id, user_id):
        log.info(f"User {user_id} deleted folder ID {folder_id} (Name: {folder_name})")
        # Log is handled inside db.delete_folder
        await reply_or_edit_text(update, context, get_text(user_id, 'folder_delete_success', lang=lang, name=html.escape(folder_name or '')))
    else:
        log.warning(f"Failed delete folder ID {folder_id} by user {user_id}")
        await reply_or_edit_text(update, context, get_text(user_id, 'folder_delete_error', lang=lang))

    # Return to the main folder menu after deletion attempt
    return await client_folder_menu(update, context)


# --- Join Groups Flow ---
async def client_select_bot_generic(update: Update, context: CallbackContext, action_prefix: str, next_state: str, title_key: str):
    """Generic function to select a bot for an action."""
    query = update.callback_query
    user_id, lang = get_user_id_and_lang(update, context)

    user_bots = db.get_client_bots(user_id)
    if not user_bots:
        await query.answer(get_text(user_id, 'client_menu_no_userbots', lang=lang), show_alert=True)
        return await client_menu(update, context)

    keyboard = []
    text = get_text(user_id, title_key, lang=lang)

    # Option to use all bots (only for joining?)
    if action_prefix == CALLBACK_JOIN_PREFIX:
         keyboard.append([InlineKeyboardButton(
              get_text(user_id, 'join_select_userbot_all', lang=lang),
              callback_data=f"{action_prefix}select_all" # Special callback for all
         )])

    active_bots_count = 0
    all_assigned_bots = []
    for phone in user_bots:
        bot_db_info = db.find_userbot(phone)
        if bot_db_info:
            all_assigned_bots.append(bot_db_info)
            if bot_db_info['status'] == 'active':
                 active_bots_count += 1

    # Add button for each assigned bot
    for bot_db_info in all_assigned_bots:
         phone = bot_db_info['phone_number']
         username = bot_db_info['username']
         display_name = f"@{username}" if username else phone
         status_icon = "🟢" if bot_db_info['status'] == 'active' else "⚪️" if bot_db_info['status'] == 'inactive' else "🔴" # Simplified icons
         button_text = f"{status_icon} {html.escape(display_name)}"
         keyboard.append([InlineKeyboardButton(button_text, callback_data=f"{action_prefix}select_{phone}")])


    if action_prefix == CALLBACK_JOIN_PREFIX and active_bots_count < len(all_assigned_bots) and active_bots_count > 0:
         # Add option for only active bots if some are inactive
         keyboard.append([InlineKeyboardButton(
              get_text(user_id, 'join_select_userbot_active', lang=lang, count=active_bots_count), # Needs translation key
              callback_data=f"{action_prefix}select_active" # Special callback for active
         )])


    keyboard.append([InlineKeyboardButton(get_text(user_id, 'button_back', lang=lang), callback_data=f"{CALLBACK_CLIENT_PREFIX}back_to_menu")])
    markup = InlineKeyboardMarkup(keyboard)

    await reply_or_edit_text(update, context, text, reply_markup=markup)
    # Set the state where we expect the user to *choose* a bot via callback
    return STATE_WAITING_FOR_USERBOT_SELECTION


async def handle_userbot_selection(update: Update, context: CallbackContext, action_prefix: str, next_state: str) -> int:
    """Handles the callback after a user selects a bot (or 'all'/'active')."""
    query = update.callback_query
    user_id, lang = get_user_id_and_lang(update, context)
    data = query.data

    selected_option = data.split(f"{action_prefix}select_")[1] # Gets phone, 'all', or 'active'

    selected_bots = []
    if selected_option == 'all':
        selected_bots = db.get_client_bots(user_id)
        if not selected_bots: # Check if list is empty
             await query.answer(get_text(user_id, 'client_menu_no_userbots', lang=lang), show_alert=True)
             return await client_menu(update, context)
    elif selected_option == 'active':
         all_client_bots = db.get_client_bots(user_id)
         selected_bots = [p for p in all_client_bots if (b := db.find_userbot(p)) and b['status'] == 'active']
         if not selected_bots: # Check if list is empty
              await query.answer(get_text(user_id, 'join_no_active_bots', lang=lang), show_alert=True)
              return await client_menu(update, context) # Go back to main menu
    else: # Specific phone selected
        phone = selected_option
        bot_info = db.find_userbot(phone)
        # Ensure the selected bot actually belongs to the user (security check)
        if not bot_info or phone not in db.get_client_bots(user_id):
            log.warning(f"User {user_id} tried to select unauthorized/invalid bot: {phone}")
            await query.answer(get_text(user_id, 'error_invalid_input', lang=lang), show_alert=True)
            return STATE_WAITING_FOR_USERBOT_SELECTION # Stay in selection state
        # Check if bot is active? Depends on action. For joining, maybe allow inactive? For tasks, likely require active.
        # Let's allow selection, the action function can check status if needed.
        selected_bots = [phone]

    context.user_data[CTX_SELECTED_BOTS] = selected_bots
    log.info(f"User {user_id} selected bot(s): {selected_bots} for action {action_prefix}")

    # Transition to the next state based on the action
    if action_prefix == CALLBACK_JOIN_PREFIX:
        await reply_or_edit_text(update, context, get_text(user_id, 'join_enter_group_links', lang=lang))
        return STATE_WAITING_FOR_GROUP_LINKS
    elif action_prefix == CALLBACK_TASK_PREFIX:
        # Store the single selected bot for task setup
        context.user_data[CTX_TASK_PHONE] = selected_bots[0]
        return await task_show_settings_menu(update, context)
    # Add other actions like view_joined here if needed
    # elif action_prefix == CALLBACK_VIEW_JOINED_PREFIX:
    #     return await client_view_joined_groups(update, context)

    else:
        log.error(f"Unhandled action prefix in handle_userbot_selection: {action_prefix}")
        clear_conversation_data(context)
        return ConversationHandler.END


async def process_join_group_links(update: Update, context: CallbackContext) -> int:
    user_id, lang = get_user_id_and_lang(update, context)
    selected_bots = context.user_data.get(CTX_SELECTED_BOTS)

    if not selected_bots:
        await reply_or_edit_text(update, context, get_text(user_id, 'session_expired', lang=lang))
        clear_conversation_data(context)
        return ConversationHandler.END

    links_text = update.message.text
    raw_links = [link.strip() for link in links_text.splitlines() if link.strip()]

    if not raw_links:
        await reply_or_edit_text(update, context, get_text(user_id, 'join_no_links', lang=lang))
        return STATE_WAITING_FOR_GROUP_LINKS # Stay in state

    await reply_or_edit_text(update, context, get_text(user_id, 'join_processing', lang=lang))

    all_results_text = get_text(user_id, 'join_results_title', lang=lang)
    tasks = []

    # Create an async task for each selected bot
    for phone in selected_bots:
        tasks.append(telethon_api.join_groups_batch(phone, raw_links))

    # Run all join tasks concurrently
    results_list = await asyncio.gather(*tasks, return_exceptions=True)

    # Process results
    for i, result_item in enumerate(results_list):
        phone = selected_bots[i]
        bot_db_info = db.find_userbot(phone)
        bot_display_name = html.escape(f"@{bot_db_info['username']}" if bot_db_info and bot_db_info['username'] else phone)
        all_results_text += "\n" + get_text(user_id, 'join_results_bot_header', lang=lang, display_name=bot_display_name)

        if isinstance(result_item, Exception):
            log.error(f"Join batch task for {phone} raised exception: {result_item}")
            all_results_text += f"\n  -> {get_text(user_id, 'error_generic', lang=lang)} ({html.escape(str(result_item))})"
            continue # Skip to next bot

        error_info, results_dict = result_item
        if error_info and error_info.get("error"):
            log.error(f"Join batch error for {phone}: {error_info['error']}")
            all_results_text += f"\n  -> {get_text(user_id, 'error_generic', lang=lang)} ({html.escape(error_info['error'])})"
            # Continue processing any partial results in results_dict if available

        if not results_dict:
             all_results_text += f"\n  -> ({get_text(user_id, 'error_no_results', lang=lang)})" # Needs key
             continue

        processed_links_count = 0
        for link, (status, detail) in results_dict.items():
             status_key = f"join_results_{status}" # e.g., join_results_success
             # Default status text from key
             status_text = get_text(user_id, status_key, lang=lang) if status_key in translations.get(lang,{}) else status.replace('_',' ').capitalize()

             # Add reason detail for specific statuses
             if isinstance(detail, dict) and detail.get('reason'):
                  reason_key = f"join_results_reason_{detail['reason']}"
                  reason_text = get_text(user_id, reason_key, lang=lang, error=detail.get('error', ''), seconds=detail.get('seconds', ''))
                  if reason_text == reason_key: reason_text = html.escape(str(detail.get('reason'))) # Fallback if reason key missing
                  status_text = get_text(user_id, 'join_results_failed', lang=lang, reason=reason_text) # Embed reason in 'failed' message
             elif status == 'flood_wait' and isinstance(detail, dict):
                  status_text = get_text(user_id, 'join_results_flood_wait', lang=lang, seconds=detail.get('seconds', '?'))


             # Ensure link is escaped for display
             escaped_link = html.escape(link)
             all_results_text += "\n" + get_text(user_id, 'join_results_line', lang=lang, url=escaped_link, status=status_text)
             processed_links_count +=1
             # Prevent excessively long messages
             if len(all_results_text) > 3800:
                 all_results_text += f"\n\n... (message truncated, {len(raw_links) - processed_links_count} links remaining for this bot)"
                 break # Stop adding results for this bot

    # Send the combined results
    # Need a back button to the main menu
    keyboard = [[InlineKeyboardButton(get_text(user_id, 'button_main_menu', lang=lang), callback_data=f"{CALLBACK_CLIENT_PREFIX}back_to_menu")]]
    markup = InlineKeyboardMarkup(keyboard)

    # Send final results, split if necessary (Telegram limit is 4096 chars)
    if len(all_results_text) > 4096:
        log.warning(f"Join results message too long ({len(all_results_text)} chars). Splitting.")
        parts = []
        current_part = ""
        for line in all_results_text.splitlines(keepends=True):
            if len(current_part) + len(line) > 4000: # Leave buffer
                parts.append(current_part)
                current_part = line
            else:
                current_part += line
        parts.append(current_part) # Add the last part

        for i, part in enumerate(parts):
            part_markup = markup if i == len(parts) - 1 else None # Add keyboard to the last part
            await context.bot.send_message(user_id, part, parse_mode=ParseMode.HTML, reply_markup=part_markup, disable_web_page_preview=True)
            if i < len(parts) - 1: await asyncio.sleep(0.5) # Small delay between parts
    else:
        await reply_or_edit_text(update, context, all_results_text, reply_markup=markup, disable_web_page_preview=True)

    clear_conversation_data(context)
    return ConversationHandler.END


# --- View Joined Groups ---
# Simplified: Removing this feature for now to reduce complexity.
# It requires potentially long-running Telethon calls and complex pagination.


# --- Client Stats ---
# Simplified: Removing this feature for now. Stats are basic in DB.
async def client_show_stats(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    user_id, lang = get_user_id_and_lang(update, context)

    stats = db.get_client_stats(user_id) # Reads aggregated stats

    if not stats:
         text = get_text(user_id, 'client_stats_no_data', lang=lang)
    else:
         text = f"<b>{get_text(user_id, 'client_stats_title', lang=lang)}</b>\n\n"
         # Note: These stats might not be super accurate depending on how they are updated
         text += get_text(user_id, 'client_stats_messages', lang=lang, total_sent=stats.get('total_messages_sent', 0)) + "\n"
         # text += get_text(user_id, 'client_stats_forwards', lang=lang, forwards_count=stats.get('forwards_count', 0)) + "\n"
         # 'groups_reached' is unclear, maybe remove or clarify its meaning

    keyboard = [[InlineKeyboardButton(get_text(user_id, 'button_back', lang=lang), callback_data=f"{CALLBACK_CLIENT_PREFIX}back_to_menu")]]
    markup = InlineKeyboardMarkup(keyboard)

    await reply_or_edit_text(update, context, text, reply_markup=markup)
    return ConversationHandler.END


# --- Task Setup ---
async def task_show_settings_menu(update: Update, context: CallbackContext) -> int:
    query = update.callback_query # Can be called initially or after setting something
    user_id, lang = get_user_id_and_lang(update, context)
    phone = context.user_data.get(CTX_TASK_PHONE)

    if not phone:
        log.error(f"Task setup called without phone in context for user {user_id}")
        if query: await query.answer(get_text(user_id, 'session_expired', lang=lang), show_alert=True)
        return await client_menu(update, context) # Go back to main menu

    bot_db_info = db.find_userbot(phone)
    display_name = html.escape(f"@{bot_db_info['username']}" if bot_db_info and bot_db_info['username'] else phone)

    # Load existing settings OR initialize if creating new
    task_settings = db.get_userbot_task_settings(user_id, phone)
    if not task_settings:
        log.info(f"No existing task settings for {user_id}/{phone}. Initializing.")
        task_settings = {} # Start with empty dict, will be populated by DB defaults on save if needed
        context.user_data[CTX_TASK_SETTINGS] = task_settings # Store fresh settings in context
    elif CTX_TASK_SETTINGS not in context.user_data:
         context.user_data[CTX_TASK_SETTINGS] = dict(task_settings) # Load existing to context

    # Use settings from context for display, as they might have been modified in this session
    current_settings = context.user_data.get(CTX_TASK_SETTINGS, {})

    status = current_settings.get('status', 'inactive')
    status_icon_key = f'task_status_icon_{status}'
    status_icon = get_text(user_id, status_icon_key, lang=lang) if status_icon_key in translations.get(lang,{}) else ("🟢" if status == 'active' else "⚪️")
    status_text = get_text(user_id, f'task_status_{status}', lang=lang)

    primary_link_raw = current_settings.get('message_link')
    primary_link = html.escape(primary_link_raw) if primary_link_raw else get_text(user_id, 'task_value_not_set', lang=lang)

    # Fallback link display removed for simplicity
    # fallback_link_raw = current_settings.get('fallback_message_link')
    # fallback_link = html.escape(fallback_link_raw) if fallback_link_raw else get_text(user_id, 'task_value_not_set', lang=lang)

    start_time_ts = current_settings.get('start_time')
    start_time_str = format_dt(start_time_ts, fmt='%H:%M') # Show only time

    interval_min = current_settings.get('repetition_interval')
    interval_str = get_text(user_id, 'task_interval_button', lang=lang, value=f"{interval_min} min") if interval_min else get_text(user_id, 'task_value_not_set', lang=lang)

    target_str = get_text(user_id, 'task_value_not_set', lang=lang)
    if current_settings.get('send_to_all_groups'):
        target_str = get_text(user_id, 'task_value_all_groups', lang=lang)
    elif current_settings.get('folder_id'):
        folder_name = db.get_folder_name(current_settings['folder_id'])
        if folder_name:
            target_str = get_text(user_id, 'task_value_folder', lang=lang, name=html.escape(folder_name))
        else:
            target_str = get_text(user_id, 'task_value_folder', lang=lang, name="ID: " + str(current_settings['folder_id'])) + " (Deleted?)"


    last_run_str = format_dt(current_settings.get('last_run'))
    last_error_raw = current_settings.get('last_error')
    last_error = html.escape(last_error_raw[:100]) if last_error_raw else get_text(user_id, 'task_value_not_set', lang=lang)

    # Build the text message
    text = f"<b>{get_text(user_id, 'task_setup_title', lang=lang, display_name=display_name)}</b>\n\n"
    text += f"{get_text(user_id, 'task_setup_status_line', lang=lang, status_icon=status_icon, status_text=status_text)}\n"
    text += f"{get_text(user_id, 'task_setup_primary_msg', lang=lang, link=primary_link)}\n"
    # text += f"{get_text(user_id, 'task_setup_fallback_msg', lang=lang, link=fallback_link)}\n" # Keep UI simpler
    text += f"{get_text(user_id, 'task_setup_start_time', lang=lang, time=start_time_str)}\n"
    text += f"{get_text(user_id, 'task_setup_interval', lang=lang, interval=interval_str)}\n"
    text += f"{get_text(user_id, 'task_setup_target', lang=lang, target=target_str)}\n\n"
    text += f"{get_text(user_id, 'task_setup_last_run', lang=lang, time=last_run_str)}\n"
    text += f"{get_text(user_id, 'task_setup_last_error', lang=lang, error=last_error)}\n"

    # Build Keyboard
    keyboard = [
        [InlineKeyboardButton(get_text(user_id, 'task_button_set_message', lang=lang), callback_data=f"{CALLBACK_TASK_PREFIX}set_primary_link")],
        [
            InlineKeyboardButton(get_text(user_id, 'task_button_set_time', lang=lang), callback_data=f"{CALLBACK_TASK_PREFIX}set_time"),
            InlineKeyboardButton(get_text(user_id, 'task_button_set_interval', lang=lang), callback_data=f"{CALLBACK_TASK_PREFIX}set_interval"),
        ],
        [InlineKeyboardButton(get_text(user_id, 'task_button_set_target', lang=lang), callback_data=f"{CALLBACK_TASK_PREFIX}set_target_type")],
        [
            InlineKeyboardButton(
                get_text(user_id, 'task_button_deactivate' if status == 'active' else 'task_button_activate', lang=lang),
                callback_data=f"{CALLBACK_TASK_PREFIX}toggle_status"
            ),
            InlineKeyboardButton(get_text(user_id, 'task_button_save', lang=lang), callback_data=f"{CALLBACK_TASK_PREFIX}save")
         ],
        [InlineKeyboardButton(get_text(user_id, 'button_back', lang=lang), callback_data=f"{CALLBACK_TASK_PREFIX}back_to_bot_select")] # Go back to bot selection
    ]
    markup = InlineKeyboardMarkup(keyboard)

    await reply_or_edit_text(update, context, text, reply_markup=markup, disable_web_page_preview=True) # Disable preview for cleaner look
    return STATE_TASK_SETUP # Stay in task setup state


async def task_prompt_set_link(update: Update, context: CallbackContext, link_type: str) -> int:
    """Asks user to send primary or fallback link."""
    query = update.callback_query
    user_id, lang = get_user_id_and_lang(update, context)

    prompt_key = 'task_prompt_primary_link' if link_type == 'primary' else 'task_prompt_fallback_link'
    next_state = STATE_WAITING_FOR_PRIMARY_MESSAGE_LINK if link_type == 'primary' else STATE_WAITING_FOR_FALLBACK_MESSAGE_LINK

    text = get_text(user_id, prompt_key, lang=lang)
    # Simple back button for link prompts
    keyboard = [[InlineKeyboardButton(get_text(user_id, 'button_back', lang=lang), callback_data=f"{CALLBACK_TASK_PREFIX}back_to_task_menu")]]
    markup = InlineKeyboardMarkup(keyboard)

    await reply_or_edit_text(update, context, text, reply_markup=markup)
    return next_state


async def process_task_link(update: Update, context: CallbackContext, link_type: str) -> int:
    """Processes the message link sent by the user."""
    user_id, lang = get_user_id_and_lang(update, context)
    phone = context.user_data.get(CTX_TASK_PHONE)
    task_settings = context.user_data.get(CTX_TASK_SETTINGS)

    if not phone or task_settings is None:
        await reply_or_edit_text(update, context, get_text(user_id, 'session_expired', lang=lang))
        clear_conversation_data(context)
        return ConversationHandler.END

    link_text = update.message.text.strip()

    # Handle skipping fallback link
    if link_type == 'fallback' and link_text.lower() == 'skip':
        task_settings['fallback_message_link'] = None
        await reply_or_edit_text(update, context, get_text(user_id, 'task_set_skipped_fallback', lang=lang))
        # Return to task menu
        return await task_show_settings_menu(update, context)

    # Validate the link format roughly
    link_parsed_type, _ = telethon_api.parse_telegram_url_simple(link_text)
    if link_parsed_type != "message_link":
        await reply_or_edit_text(update, context, get_text(user_id, 'task_error_invalid_link', lang=lang))
        # Stay in the current link waiting state
        return STATE_WAITING_FOR_PRIMARY_MESSAGE_LINK if link_type == 'primary' else STATE_WAITING_FOR_FALLBACK_MESSAGE_LINK

    # Optional: Add a check here using the userbot to see if the message is accessible
    try:
         await reply_or_edit_text(update, context, get_text(user_id, 'task_verifying_link', lang=lang)) # Needs key
         accessible = await telethon_api.check_message_link_access(phone, link_text)
         if not accessible:
              await reply_or_edit_text(update, context, get_text(user_id, 'task_error_link_unreachable', lang=lang, bot_phone=phone))
              return STATE_WAITING_FOR_PRIMARY_MESSAGE_LINK if link_type == 'primary' else STATE_WAITING_FOR_FALLBACK_MESSAGE_LINK
         else:
              log.info(f"User {user_id} link {link_text} verified successfully by bot {phone}")
              # Proceed to store link below
    except Exception as e:
         log.error(f"Error checking link access {phone} -> {link_text}: {e}")
         await reply_or_edit_text(update, context, get_text(user_id, 'error_telegram_api', lang=lang, error=str(e)))
         return STATE_WAITING_FOR_PRIMARY_MESSAGE_LINK if link_type == 'primary' else STATE_WAITING_FOR_FALLBACK_MESSAGE_LINK

    # Store the link
    if link_type == 'primary':
        task_settings['message_link'] = link_text
        await reply_or_edit_text(update, context, get_text(user_id, 'task_set_success_msg', lang=lang))
        # Ask for fallback link next (commented out for simpler UI)
        # return await task_prompt_set_link(update, context, 'fallback')
        # Go back to menu instead
        return await task_show_settings_menu(update, context)
    else: # Fallback
        task_settings['fallback_message_link'] = link_text
        await reply_or_edit_text(update, context, get_text(user_id, 'task_set_success_fallback', lang=lang))
        # Return to task menu
        return await task_show_settings_menu(update, context)

async def task_prompt_start_time(update: Update, context: CallbackContext) -> int:
     """Asks user for start time."""
     query = update.callback_query
     user_id, lang = get_user_id_and_lang(update, context)
     # Provide timezone info in prompt
     local_tz_name = LITHUANIA_TZ.zone if hasattr(LITHUANIA_TZ, 'zone') else 'Europe/Vilnius'
     text = get_text(user_id, 'task_prompt_start_time', lang=lang, timezone_name=local_tz_name)
     keyboard = [[InlineKeyboardButton(get_text(user_id, 'button_back', lang=lang), callback_data=f"{CALLBACK_TASK_PREFIX}back_to_task_menu")]]
     markup = InlineKeyboardMarkup(keyboard)
     await reply_or_edit_text(update, context, text, reply_markup=markup)
     return STATE_WAITING_FOR_START_TIME

async def process_task_start_time(update: Update, context: CallbackContext) -> int:
    """Processes the start time HH:MM input."""
    user_id, lang = get_user_id_and_lang(update, context)
    time_str = update.message.text.strip()
    task_settings = context.user_data.get(CTX_TASK_SETTINGS)

    if task_settings is None:
        await reply_or_edit_text(update, context, get_text(user_id, 'session_expired', lang=lang))
        clear_conversation_data(context)
        return ConversationHandler.END

    try:
        # Parse HH:MM format
        hour, minute = map(int, time_str.split(':'))
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            raise ValueError("Invalid hour/minute range")

        # Convert local time (Lithuania) HH:MM to next upcoming UTC timestamp
        now_local = datetime.now(LITHUANIA_TZ)
        # Create a time object first, then combine with date
        input_time = datetime.strptime(f"{hour:02}:{minute:02}", "%H:%M").time()
        # Combine with today's date initially
        target_local_dt = LITHUANIA_TZ.localize(datetime.combine(now_local.date(), input_time))

        # If the target time today is already past, aim for tomorrow
        # Use localized comparison
        if target_local_dt <= now_local:
            target_local_dt += timedelta(days=1)

        # Convert the target local datetime to UTC timestamp
        target_utc = target_local_dt.astimezone(UTC_TZ)
        start_timestamp = int(target_utc.timestamp())

        task_settings['start_time'] = start_timestamp
        log.info(f"User {user_id} set task start time: {time_str} LT -> {start_timestamp} UTC")
        await reply_or_edit_text(
            update, context,
            get_text(user_id, 'task_set_success_time', lang=lang, time=time_str)
        )
        # Return to task menu
        return await task_show_settings_menu(update, context)

    except (ValueError, TypeError):
        await reply_or_edit_text(update, context, get_text(user_id, 'task_error_invalid_time', lang=lang))
        return STATE_WAITING_FOR_START_TIME # Stay in state


async def task_select_interval(update: Update, context: CallbackContext) -> int:
    """Shows interval selection buttons."""
    query = update.callback_query
    user_id, lang = get_user_id_and_lang(update, context)

    # Define common intervals (in minutes)
    intervals = [5, 10, 15, 30, 60, 120, 180, 240, 360, 720, 1440] # 5m to 1d
    keyboard = []
    row = []
    for minutes in intervals:
        if minutes < 60:
            label = f"{minutes} min"
        elif minutes % (60*24) == 0:
             label = f"{minutes // (60*24)} d"
        else:
            label = f"{minutes // 60} h"

        button_text = get_text(user_id, 'task_interval_button', lang=lang, value=label)
        row.append(InlineKeyboardButton(button_text, callback_data=f"{CALLBACK_INTERVAL_PREFIX}{minutes}"))
        if len(row) >= 3:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)

    keyboard.append([InlineKeyboardButton(get_text(user_id, 'button_back', lang=lang), callback_data=f"{CALLBACK_TASK_PREFIX}back_to_task_menu")])
    markup = InlineKeyboardMarkup(keyboard)

    await reply_or_edit_text(update, context, get_text(user_id, 'task_select_interval_title', lang=lang), reply_markup=markup)
    # Callback handler will process the selection, stay in task setup state technically
    return STATE_TASK_SETUP


async def process_interval_callback(update: Update, context: CallbackContext) -> int:
    """Handles interval selection callback."""
    query = update.callback_query
    user_id, lang = get_user_id_and_lang(update, context)
    task_settings = context.user_data.get(CTX_TASK_SETTINGS)

    if task_settings is None:
        await query.answer(get_text(user_id, 'session_expired', lang=lang), show_alert=True)
        return await client_menu(update, context)

    try:
        interval_minutes = int(query.data.split(CALLBACK_INTERVAL_PREFIX)[1])
        if interval_minutes <= 0: raise ValueError("Interval must be positive")
    except (ValueError, IndexError):
        log.error(f"Invalid interval callback data: {query.data}")
        await query.answer(get_text(user_id, 'error_invalid_input', lang=lang), show_alert=True)
        return STATE_TASK_SETUP # Remain in task setup

    task_settings['repetition_interval'] = interval_minutes
    log.info(f"User {user_id} set task interval to {interval_minutes} minutes.")

    # Format interval for display
    if interval_minutes < 60: interval_str = f"{interval_minutes} min"
    elif interval_minutes % (60*24) == 0: interval_str = f"{interval_minutes // (60*24)} d"
    else: interval_str = f"{interval_minutes // 60} h"
    display_value = get_text(user_id, 'task_interval_button', lang=lang, value=interval_str)

    # No need for separate success message, just update menu
    # await reply_or_edit_text(update, context, get_text(user_id, 'task_set_success_interval', lang=lang, interval=display_value))

    # Return to task menu showing updated settings
    return await task_show_settings_menu(update, context)


async def task_select_target_type(update: Update, context: CallbackContext) -> int:
    """Shows buttons to choose target: Folder or All Groups."""
    query = update.callback_query
    user_id, lang = get_user_id_and_lang(update, context)

    keyboard = [
        [InlineKeyboardButton(get_text(user_id, 'task_button_target_folder', lang=lang), callback_data=f"{CALLBACK_TASK_PREFIX}select_folder_target?page=0")],
        [InlineKeyboardButton(get_text(user_id, 'task_button_target_all', lang=lang), callback_data=f"{CALLBACK_TASK_PREFIX}set_target_all")],
        [InlineKeyboardButton(get_text(user_id, 'button_back', lang=lang), callback_data=f"{CALLBACK_TASK_PREFIX}back_to_task_menu")]
    ]
    markup = InlineKeyboardMarkup(keyboard)
    await reply_or_edit_text(update, context, get_text(user_id, 'task_select_target_title', lang=lang), reply_markup=markup)
    return STATE_TASK_SETUP # Selection handled by callbacks


async def task_select_folder_for_target(update: Update, context: CallbackContext) -> int:
    """Shows list of folders to select as target."""
    query = update.callback_query
    user_id, lang = get_user_id_and_lang(update, context)

    try:
        _, params = query.data.split('?', 1)
        current_page = int(params.split('=')[1])
    except (ValueError, IndexError):
        current_page = 0

    folders = db.get_folders_by_user(user_id)
    if not folders:
        await query.answer(get_text(user_id, 'task_error_no_folders', lang=lang), show_alert=True)
        # Go back to target type selection if no folders exist
        return await task_select_target_type(update, context)

    total_items = len(folders)
    start_index = current_page * ITEMS_PER_PAGE
    end_index = start_index + ITEMS_PER_PAGE
    folders_page = folders[start_index:end_index]

    text = get_text(user_id, 'task_select_folder_title', lang=lang)
    keyboard = []
    for folder in folders_page:
        button_text = html.escape(folder['name'])
        callback_data = f"{CALLBACK_TASK_PREFIX}set_target_folder?id={folder['id']}"
        keyboard.append([InlineKeyboardButton(button_text, callback_data=callback_data)])

    base_callback = f"{CALLBACK_TASK_PREFIX}select_folder_target"
    pagination_buttons = build_pagination_buttons(base_callback, current_page, total_items, ITEMS_PER_PAGE)
    keyboard.extend(pagination_buttons)

    keyboard.append([InlineKeyboardButton(get_text(user_id, 'button_back', lang=lang), callback_data=f"{CALLBACK_TASK_PREFIX}back_to_target_type")])

    markup = InlineKeyboardMarkup(keyboard)
    await reply_or_edit_text(update, context, text, reply_markup=markup)
    return STATE_SELECT_TARGET_GROUPS # Specific state for selecting folder


async def task_set_target(update: Update, context: CallbackContext, target_type: str) -> int:
    """Sets the target type (all or specific folder)."""
    query = update.callback_query
    user_id, lang = get_user_id_and_lang(update, context)
    task_settings = context.user_data.get(CTX_TASK_SETTINGS)

    if task_settings is None:
        await query.answer(get_text(user_id, 'session_expired', lang=lang), show_alert=True)
        return await client_menu(update, context)

    success_text = ""
    if target_type == 'all':
        task_settings['send_to_all_groups'] = 1
        task_settings['folder_id'] = None
        success_text = get_text(user_id, 'task_set_success_target_all', lang=lang)
        log.info(f"User {user_id} set task target to all groups.")
    elif target_type == 'folder':
        try:
            _, params = query.data.split('?', 1)
            folder_id = int(params.split('=')[1])
        except (ValueError, IndexError):
            log.error(f"Could not parse folder ID from target callback: {query.data}")
            await query.answer(get_text(user_id, 'error_generic', lang=lang), show_alert=True)
            return STATE_TASK_SETUP # Go back to main task menu

        folder_name = db.get_folder_name(folder_id)
        if not folder_name:
            await query.answer(get_text(user_id, 'folder_not_found_error', lang=lang), show_alert=True) # Need key
            return STATE_TASK_SETUP # Go back to main task menu

        task_settings['send_to_all_groups'] = 0
        task_settings['folder_id'] = folder_id
        success_text = get_text(user_id, 'task_set_success_target_folder', lang=lang, name=html.escape(folder_name))
        log.info(f"User {user_id} set task target to folder {folder_name} ({folder_id}).")
    else:
        log.error(f"Invalid target_type '{target_type}' in task_set_target")
        return STATE_TASK_SETUP

    # Don't send separate success message, just update the menu
    # await reply_or_edit_text(update, context, success_text)

    # Return to task menu to show updated settings
    return await task_show_settings_menu(update, context)


async def task_toggle_status(update: Update, context: CallbackContext) -> int:
    """Toggles the task status between active and inactive."""
    query = update.callback_query
    user_id, lang = get_user_id_and_lang(update, context)
    task_settings = context.user_data.get(CTX_TASK_SETTINGS)

    if task_settings is None:
        await query.answer(get_text(user_id, 'session_expired', lang=lang), show_alert=True)
        return await client_menu(update, context)

    current_status = task_settings.get('status', 'inactive')
    new_status = 'inactive' if current_status == 'active' else 'active'

    # Add validation before activating: Check required fields
    if new_status == 'active':
        missing_fields = []
        if not task_settings.get('message_link'):
            missing_fields.append(get_text(user_id, 'task_required_message', lang=lang))
        if not task_settings.get('start_time'):
             missing_fields.append(get_text(user_id, 'task_required_start_time', lang=lang))
        if not task_settings.get('repetition_interval'):
             missing_fields.append(get_text(user_id, 'task_required_interval', lang=lang))
        if not task_settings.get('folder_id') and not task_settings.get('send_to_all_groups'):
             missing_fields.append(get_text(user_id, 'task_required_target', lang=lang))

        if missing_fields:
            missing_str = ", ".join(missing_fields)
            await query.answer(get_text(user_id, 'task_save_validation_fail', lang=lang, missing=missing_str), show_alert=True)
            # Do not change status, stay in menu
            return STATE_TASK_SETUP

    task_settings['status'] = new_status
    log.info(f"User {user_id} toggled task status to {new_status}.")
    status_text = get_text(user_id, f'task_status_{new_status}', lang=lang)
    # await query.answer(get_text(user_id, 'task_status_toggled_success', lang=lang, status=status_text)) # Answer silently

    # Update the menu display
    return await task_show_settings_menu(update, context)


async def task_save_settings(update: Update, context: CallbackContext) -> int:
    """Saves the current task settings from context to the database."""
    query = update.callback_query
    user_id, lang = get_user_id_and_lang(update, context)
    phone = context.user_data.get(CTX_TASK_PHONE)
    settings_to_save = context.user_data.get(CTX_TASK_SETTINGS)

    if not phone or settings_to_save is None:
        await query.answer(get_text(user_id, 'session_expired', lang=lang), show_alert=True)
        return await client_menu(update, context)

    # Add validation before saving (especially if status is active)
    if settings_to_save.get('status') == 'active':
        missing_fields = []
        if not settings_to_save.get('message_link'):
            missing_fields.append(get_text(user_id, 'task_required_message', lang=lang))
        if not settings_to_save.get('start_time'):
             missing_fields.append(get_text(user_id, 'task_required_start_time', lang=lang))
        if not settings_to_save.get('repetition_interval'):
             missing_fields.append(get_text(user_id, 'task_required_interval', lang=lang))
        if not settings_to_save.get('folder_id') and not settings_to_save.get('send_to_all_groups'):
             missing_fields.append(get_text(user_id, 'task_required_target', lang=lang))

        if missing_fields:
            missing_str = ", ".join(missing_fields)
            await query.answer(get_text(user_id, 'task_save_validation_fail', lang=lang, missing=missing_str), show_alert=True)
            # Do not save, stay in menu
            return STATE_TASK_SETUP

    # Clear last error when manually saving
    settings_to_save['last_error'] = None

    if db.save_userbot_task_settings(user_id, phone, settings_to_save):
        db.log_event_db("Task Settings Saved", f"User: {user_id}, Bot: {phone}, Status: {settings_to_save.get('status')}", user_id=user_id, userbot_phone=phone)
        bot_db_info = db.find_userbot(phone)
        display_name = html.escape(f"@{bot_db_info['username']}" if bot_db_info and bot_db_info['username'] else phone)
        await reply_or_edit_text(update, context, get_text(user_id, 'task_save_success', lang=lang, display_name=display_name))
        # Successfully saved, clear context and return to main client menu
        clear_conversation_data(context)
        return await client_menu(update, context)
    else:
        db.log_event_db("Task Save Failed", f"User: {user_id}, Bot: {phone}", user_id=user_id, userbot_phone=phone)
        await reply_or_edit_text(update, context, get_text(user_id, 'task_save_error', lang=lang))
        # Stay in task setup menu on save error
        return STATE_TASK_SETUP


# Helper to build simple back button markup for task setup steps
def task_back_button_markup(user_id, context):
    lang = context.user_data.get(CTX_LANG, 'en')
    return InlineKeyboardMarkup([[
        InlineKeyboardButton(get_text(user_id, 'button_back', lang=lang), callback_data=f"{CALLBACK_TASK_PREFIX}back_to_task_menu")
    ]])


# --- Admin Userbot List/Remove/Log Views ---

async def admin_list_userbots(update: Update, context: CallbackContext):
    query = update.callback_query
    user_id, lang = get_user_id_and_lang(update, context)
    try:
        _, params = query.data.split('?', 1)
        current_page = int(params.split('=')[1])
    except (ValueError, IndexError): current_page = 0

    all_bots = db.get_all_userbots() # Fetches all, assigned or not
    if not all_bots:
        text = get_text(user_id, 'admin_userbot_list_no_bots', lang=lang)
        markup = InlineKeyboardMarkup([[InlineKeyboardButton(get_text(user_id, 'button_back', lang=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}back_to_menu")]])
        await reply_or_edit_text(update, context, text, reply_markup=markup)
        return ConversationHandler.END

    total_items = len(all_bots)
    start_index = current_page * ITEMS_PER_PAGE
    end_index = start_index + ITEMS_PER_PAGE
    bots_page = all_bots[start_index:end_index]

    text = f"<b>{get_text(user_id, 'admin_userbot_list_title', lang=lang)}</b> (Page {current_page + 1}/{math.ceil(total_items / ITEMS_PER_PAGE)})\n\n"
    for bot in bots_page:
        phone = bot['phone_number']
        username = bot['username']
        status = bot['status']
        assigned_client_code = bot['assigned_client'] or get_text(user_id, 'admin_userbot_list_unassigned', lang=lang)
        last_error = bot['last_error']

        display_name = f"@{username}" if username else phone
        # Get icon based on status
        status_icon_key = f'admin_userbot_list_status_icon_{status}'
        icon_fallback = { # Define fallbacks if key missing
            'active': "🟢", 'inactive': "⚪️", 'error': "🔴", 'connecting': "🔌",
            'needs_code': "🔢", 'needs_password': "🔒", 'authenticating': "⏳",
            'initializing': "⚙️"}.get(status, "❓")
        status_icon = get_text(user_id, status_icon_key, lang=lang) if status_icon_key in translations.get(lang, {}) else icon_fallback

        text += get_text(
            user_id, 'admin_userbot_list_line', lang=lang,
            status_icon=status_icon,
            display_name=html.escape(display_name),
            phone=html.escape(phone),
            client_code=html.escape(assigned_client_code),
            status=html.escape(status.capitalize())
        ) + "\n"
        if last_error:
             error_text = html.escape(last_error)
             text += get_text(user_id, 'admin_userbot_list_error_line', lang=lang, error=error_text[:150]) + "\n"
        # Add spacing between entries? Optional.
        # text += "\n"

    keyboard = []
    base_callback = f"{CALLBACK_ADMIN_PREFIX}list_bots"
    pagination_buttons = build_pagination_buttons(base_callback, current_page, total_items, ITEMS_PER_PAGE)
    keyboard.extend(pagination_buttons)
    keyboard.append([InlineKeyboardButton(get_text(user_id, 'button_back', lang=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}back_to_menu")])
    markup = InlineKeyboardMarkup(keyboard)

    await reply_or_edit_text(update, context, text, reply_markup=markup, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    return ConversationHandler.END


async def admin_select_userbot_to_remove(update: Update, context: CallbackContext):
    query = update.callback_query
    user_id, lang = get_user_id_and_lang(update, context)
    try:
        _, params = query.data.split('?', 1)
        current_page = int(params.split('=')[1])
    except (ValueError, IndexError): current_page = 0

    all_bots = db.get_all_userbots()
    if not all_bots:
        text = get_text(user_id, 'admin_userbot_no_bots_to_remove', lang=lang)
        markup = InlineKeyboardMarkup([[InlineKeyboardButton(get_text(user_id, 'button_back', lang=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}back_to_menu")]])
        await reply_or_edit_text(update, context, text, reply_markup=markup)
        return ConversationHandler.END

    total_items = len(all_bots)
    start_index = current_page * ITEMS_PER_PAGE
    end_index = start_index + ITEMS_PER_PAGE
    bots_page = all_bots[start_index:end_index]

    text = get_text(user_id, 'admin_userbot_select_remove', lang=lang)
    keyboard = []
    for bot in bots_page:
        phone = bot['phone_number']
        username = bot['username']
        display_name = f"@{username}" if username else phone
        button_text = f"🗑️ {html.escape(display_name)}"
        # Pass phone directly, assuming '+' won't break callbacks (PTB usually handles this)
        keyboard.append([InlineKeyboardButton(button_text, callback_data=f"{CALLBACK_ADMIN_PREFIX}remove_bot_confirm_{phone}")])

    base_callback = f"{CALLBACK_ADMIN_PREFIX}remove_bot_select"
    pagination_buttons = build_pagination_buttons(base_callback, current_page, total_items, ITEMS_PER_PAGE)
    keyboard.extend(pagination_buttons)
    keyboard.append([InlineKeyboardButton(get_text(user_id, 'button_back', lang=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}back_to_menu")])
    markup = InlineKeyboardMarkup(keyboard)

    await reply_or_edit_text(update, context, text, reply_markup=markup)
    return ConversationHandler.END # Selection handled by callback


async def admin_confirm_remove_userbot(update: Update, context: CallbackContext):
     """Sends the confirmation message before actually removing."""
     query = update.callback_query
     user_id, lang = get_user_id_and_lang(update, context)
     try:
         phone_to_remove = query.data.split(f"{CALLBACK_ADMIN_PREFIX}remove_bot_confirm_")[1]
     except IndexError:
         log.error(f"Could not parse phone from remove confirm callback: {query.data}")
         await query.answer(get_text(user_id, 'error_generic', lang=lang), show_alert=True)
         return await admin_command(update, context) # Go back to admin menu

     bot_info = db.find_userbot(phone_to_remove)
     if not bot_info:
          await query.answer(get_text(user_id, 'admin_userbot_not_found', lang=lang), show_alert=True) # Need key
          return await admin_command(update, context)

     username = bot_info['username']
     display_name = html.escape(f"@{username}" if username else phone_to_remove)

     text = get_text(user_id, 'admin_userbot_remove_confirm_text', lang=lang, display_name=display_name)
     keyboard = [[
         InlineKeyboardButton(get_text(user_id, 'button_yes', lang=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}remove_bot_confirmed_{phone_to_remove}"),
         InlineKeyboardButton(get_text(user_id, 'button_no', lang=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}back_to_menu")
     ]]
     markup = InlineKeyboardMarkup(keyboard)
     await reply_or_edit_text(update, context, text, reply_markup=markup)
     return ConversationHandler.END


async def admin_remove_userbot_confirmed(update: Update, context: CallbackContext):
    """Actually removes the userbot after confirmation."""
    query = update.callback_query
    user_id, lang = get_user_id_and_lang(update, context)
    try:
        phone_to_remove = query.data.split(f"{CALLBACK_ADMIN_PREFIX}remove_bot_confirmed_")[1]
    except IndexError:
        log.error(f"Could not parse phone from remove confirmed callback: {query.data}")
        await query.answer(get_text(user_id, 'error_generic', lang=lang), show_alert=True)
        return await admin_command(update, context)

    bot_info = db.find_userbot(phone_to_remove) # Get info for display before removing
    display_name = "N/A"
    if bot_info:
        display_name = html.escape(f"@{bot_info['username']}" if bot_info['username'] else phone_to_remove)

    log.info(f"Admin {user_id} confirmed removal of userbot {phone_to_remove}")

    # Stop runtime first
    stopped = telethon_api.stop_userbot_runtime(phone_to_remove)
    log.info(f"Runtime stop for {phone_to_remove}: {'Success' if stopped else 'Not running/Failed'}")

    # Remove from DB
    if db.remove_userbot(phone_to_remove):
        db.log_event_db("Userbot Removed", f"Phone: {phone_to_remove}", user_id=user_id, userbot_phone=phone_to_remove)
        await reply_or_edit_text(update, context, get_text(user_id, 'admin_userbot_remove_success', lang=lang, display_name=display_name))
    else:
        # Log is handled in db.remove_userbot if it doesn't exist
        await reply_or_edit_text(update, context, get_text(user_id, 'admin_userbot_remove_error', lang=lang))

    # Return to admin menu
    return await admin_command(update, context)


async def admin_view_subscriptions(update: Update, context: CallbackContext):
    query = update.callback_query
    user_id, lang = get_user_id_and_lang(update, context)
    try:
        _, params = query.data.split('?', 1)
        current_page = int(params.split('=')[1])
    except (ValueError, IndexError): current_page = 0

    subs = db.get_all_subscriptions() # Fetches activated clients
    if not subs:
        text = get_text(user_id, 'admin_subs_none', lang=lang)
        markup = InlineKeyboardMarkup([[InlineKeyboardButton(get_text(user_id, 'button_back', lang=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}back_to_menu")]])
        await reply_or_edit_text(update, context, text, reply_markup=markup)
        return ConversationHandler.END

    total_items = len(subs)
    start_index = current_page * ITEMS_PER_PAGE
    end_index = start_index + ITEMS_PER_PAGE
    subs_page = subs[start_index:end_index]

    text = f"<b>{get_text(user_id, 'admin_subs_title', lang=lang)}</b> (Page {current_page + 1}/{math.ceil(total_items / ITEMS_PER_PAGE)})\n\n"
    for sub in subs_page:
        client_user_id = sub['user_id']
        # Attempt to get user info (might fail if bot hasn't interacted)
        user_link = f"ID: `{client_user_id}`"
        if client_user_id:
             try:
                 # Use simpler tg://user link format
                 user_link = f"<a href='tg://user?id={client_user_id}'>{client_user_id}</a>"
             except Exception as e:
                 log.debug(f"Could not create user link for {client_user_id}: {e}")
                 user_link = f"ID: `{client_user_id}` (Inactive?)"


        end_date = format_dt(sub['subscription_end'])
        code = sub['invitation_code']
        bot_count = sub['bot_count'] # Count from the JOIN in the query

        text += get_text(
            user_id, 'admin_subs_line', lang=lang,
            user_link=user_link, # Already formatted with HTML
            code=html.escape(code),
            end_date=end_date,
            bot_count=bot_count
        ) + "\n\n"

    keyboard = []
    base_callback = f"{CALLBACK_ADMIN_PREFIX}view_subs"
    pagination_buttons = build_pagination_buttons(base_callback, current_page, total_items, ITEMS_PER_PAGE)
    keyboard.extend(pagination_buttons)
    keyboard.append([InlineKeyboardButton(get_text(user_id, 'button_back', lang=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}back_to_menu")])
    markup = InlineKeyboardMarkup(keyboard)

    await reply_or_edit_text(update, context, text, reply_markup=markup, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    return ConversationHandler.END


async def admin_view_system_logs(update: Update, context: CallbackContext):
    query = update.callback_query
    user_id, lang = get_user_id_and_lang(update, context)
    limit = 25 # How many logs to show per view
    try:
        _, params = query.data.split('?', 1)
        current_page = int(params.split('=')[1]) # Page currently ignored, just show latest N
    except (ValueError, IndexError): current_page = 0

    logs = db.get_recent_logs(limit=limit)
    if not logs:
        text = get_text(user_id, 'admin_logs_none', lang=lang)
        markup = InlineKeyboardMarkup([[InlineKeyboardButton(get_text(user_id, 'button_back', lang=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}back_to_menu")]])
        await reply_or_edit_text(update, context, text, reply_markup=markup)
        return ConversationHandler.END

    text = f"<b>{get_text(user_id, 'admin_logs_title', lang=lang, limit=limit)}</b>\n\n"
    for log_entry in logs:
        ts = log_entry['timestamp']
        event = log_entry['event']
        log_user_id = log_entry['user_id']
        log_bot_phone = log_entry['userbot_phone']
        details = log_entry['details']

        time_str = format_dt(ts) # Format timestamp

        user_str = get_text(user_id, 'admin_logs_user_none', lang=lang)
        if log_user_id:
            if is_admin(log_user_id):
                 user_str = get_text(user_id, 'admin_logs_user_admin', lang=lang) + f" ({log_user_id})"
            else:
                 user_str = f"{log_user_id}" # Just show client ID

        bot_str = html.escape(log_bot_phone) if log_bot_phone else get_text(user_id, 'admin_logs_bot_none', lang=lang)
        details_str = html.escape(details[:100]) if details else "" # Limit details length

        text += get_text(
            user_id, 'admin_logs_line', lang=lang,
            time=time_str,
            event=html.escape(event),
            user=user_str, # User ID might be linkable if we fetch user info, but keep simple
            bot=bot_str,
            details=details_str
        ) + "\n"

    # No pagination for simple log view
    keyboard = [[InlineKeyboardButton(get_text(user_id, 'button_back', lang=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}back_to_menu")]]
    markup = InlineKeyboardMarkup(keyboard)

    await reply_or_edit_text(update, context, text, reply_markup=markup, parse_mode=ParseMode.HTML)
    return ConversationHandler.END

# --- Callback Routers ---
async def handle_client_callback(update: Update, context: CallbackContext) -> str | int | None:
    query = update.callback_query
    user_id, lang = get_user_id_and_lang(update, context)
    data = query.data

    # Check subscription status for client actions
    client_info = db.find_client_by_user_id(user_id)
    if not client_info or client_info['subscription_end'] < int(datetime.now(UTC_TZ).timestamp()):
        log.warning(f"Expired/Invalid client {user_id} tried action: {data}")
        await query.answer(get_text(user_id, 'subscription_expired', lang=lang), show_alert=True) # Need key
        clear_conversation_data(context)
        return ConversationHandler.END # Exit silently if expired/invalid

    action = data.split(CALLBACK_CLIENT_PREFIX)[1].split('?')[0] # Remove query params for routing

    log.debug(f"Client Callback Route: Action='{action}', Data='{data}'")

    if action == "select_bot_task":
        return await client_select_bot_generic(update, context, CALLBACK_TASK_PREFIX, STATE_TASK_SETUP, 'task_select_userbot')
    elif action == "manage_folders":
        return await client_folder_menu(update, context)
    elif action == "select_bot_join":
        return await client_select_bot_generic(update, context, CALLBACK_JOIN_PREFIX, STATE_WAITING_FOR_GROUP_LINKS, 'join_select_userbot')
    # elif action == "select_bot_view_joined": # Removed feature
    #    return await client_select_bot_generic(update, context, CALLBACK_VIEW_JOINED_PREFIX, STATE_WAITING_FOR_USERBOT_SELECTION, 'view_joined_select_bot')
    elif action == "view_stats":
         return await client_show_stats(update, context)
    elif action == "language":
        return await client_ask_select_language(update, context)
    elif action == "back_to_menu":
        clear_conversation_data(context) # Clear state before showing menu
        return await client_menu(update, context)
    # Add specific handlers for view_joined if re-enabled
    # elif action.startswith("view_joined_"):
    #    return await client_view_joined_groups(update, context)
    else:
        log.warning(f"Unhandled CLIENT CB: Action='{action}', Data='{data}'")
        await query.answer("Action not recognized.", show_alert=True) # Generic feedback

    return None # Return None or END state if action handled without state change


async def handle_admin_callback(update: Update, context: CallbackContext) -> str | int | None:
    query = update.callback_query
    user_id, lang = get_user_id_and_lang(update, context)

    if not is_admin(user_id):
        await query.answer(get_text(user_id, 'unauthorized', lang=lang), show_alert=True)
        return ConversationHandler.END # Exit silently

    data = query.data
    action = data.split(CALLBACK_ADMIN_PREFIX)[1].split('?')[0] # Remove query params for routing
    # Further strip specific action suffixes if needed for cleaner routing
    if action.startswith("remove_bot_confirm_"): action = "remove_bot_confirm"
    elif action.startswith("remove_bot_confirmed_"): action = "remove_bot_confirmed"

    log.debug(f"Admin Callback Route: Action='{action}', Data='{data}'")

    if action == "add_bot_prompt":
        await reply_or_edit_text(update, context, get_text(user_id, 'admin_userbot_prompt_phone', lang=lang))
        return STATE_WAITING_FOR_PHONE
    elif action == "remove_bot_select":
        return await admin_select_userbot_to_remove(update, context)
    elif action == "list_bots":
        return await admin_list_userbots(update, context)
    elif action == "gen_invite_prompt":
        await reply_or_edit_text(update, context, get_text(user_id, 'admin_invite_prompt_details', lang=lang))
        return STATE_WAITING_FOR_SUB_DETAILS
    elif action == "view_subs":
        return await admin_view_subscriptions(update, context)
    elif action == "extend_sub_prompt":
        await reply_or_edit_text(update, context, get_text(user_id, 'admin_extend_prompt_code', lang=lang))
        return STATE_WAITING_FOR_EXTEND_CODE
    elif action == "assign_bots_prompt":
        await reply_or_edit_text(update, context, get_text(user_id, 'admin_assignbots_prompt_code', lang=lang))
        return STATE_WAITING_FOR_ADD_USERBOTS_CODE
    elif action == "view_logs":
        return await admin_view_system_logs(update, context)
    elif action == "remove_bot_confirm": # Route based on stripped action
         return await admin_confirm_remove_userbot(update, context)
    elif action == "remove_bot_confirmed": # Route based on stripped action
         return await admin_remove_userbot_confirmed(update, context)
    elif action == "back_to_menu":
         clear_conversation_data(context)
         return await admin_command(update, context)
    else:
        log.warning(f"Unhandled ADMIN CB: Action='{action}', Data='{data}'")
        await query.answer("Action not recognized.", show_alert=True) # Generic feedback

    return None


async def handle_folder_callback(update: Update, context: CallbackContext) -> str | int | None:
    query = update.callback_query
    user_id, lang = get_user_id_and_lang(update, context)
    data = query.data
    action = data.split(CALLBACK_FOLDER_PREFIX)[1].split('?')[0] # Base action
    log.debug(f"Folder Callback Route: Action='{action}', Data='{data}'")

    if action == "create_prompt":
        await reply_or_edit_text(update, context, get_text(user_id, 'folder_create_prompt', lang=lang))
        return STATE_WAITING_FOR_FOLDER_NAME
    elif action == "select_edit":
        return await client_select_folder_to_edit_or_delete(update, context, 'edit')
    elif action == "select_delete":
        return await client_select_folder_to_edit_or_delete(update, context, 'delete')
    elif action == "edit_selected":
        return await client_show_folder_edit_options(update, context)
    elif action == "delete_selected": # Renamed from delete_confirm
         return await client_confirm_folder_delete(update, context)
    elif action == "delete_confirmed":
         return await client_delete_folder_confirmed(update, context)
    elif action == "back_to_manage":
        clear_conversation_data(context) # Clear folder context
        return await client_folder_menu(update, context)
    # --- Edit Options ---
    elif action == "edit_add_prompt":
         folder_name = context.user_data.get(CTX_FOLDER_NAME, "this folder")
         await reply_or_edit_text(update, context, get_text(user_id, 'folder_edit_add_prompt', lang=lang, name=html.escape(folder_name)))
         return STATE_WAITING_FOR_GROUP_LINKS # Use the same state as joining
    elif action == "edit_remove_select":
         return await client_select_groups_to_remove(update, context)
    elif action == "edit_toggle_remove": # Handles the toggle action
         return await client_toggle_group_for_removal(update, context)
    elif action == "edit_remove_confirm": # Handles the confirmation button press
         return await client_confirm_remove_selected_groups(update, context)
    elif action == "edit_rename_prompt":
         current_name = context.user_data.get(CTX_FOLDER_NAME, "this folder")
         await reply_or_edit_text(update, context, get_text(user_id, 'folder_edit_rename_prompt', lang=lang, current_name=html.escape(current_name)))
         return STATE_FOLDER_RENAME_PROMPT
    elif action == "back_to_edit_options":
        # Go back to the main edit options for the current folder
        return await client_show_folder_edit_options(update, context)
    else:
        log.warning(f"Unhandled FOLDER CB: Action='{action}', Data='{data}'")
        await query.answer("Action not recognized.", show_alert=True)

    return None


async def handle_task_callback(update: Update, context: CallbackContext) -> str | int | None:
    query = update.callback_query
    user_id, lang = get_user_id_and_lang(update, context)
    data = query.data
    action = data.split(CALLBACK_TASK_PREFIX)[1].split('?')[0] # Base action
    log.debug(f"Task Callback Route: Action='{action}', Data='{data}'")

    # --- Bot Selection ---
    if action.startswith("select_"): # Catches select_<phone>
        return await handle_userbot_selection(update, context, CALLBACK_TASK_PREFIX, STATE_TASK_SETUP)
    elif action == "back_to_bot_select":
         clear_conversation_data(context)
         # Re-call the function to show bot selection
         return await client_select_bot_generic(update, context, CALLBACK_TASK_PREFIX, STATE_TASK_SETUP, 'task_select_userbot')

    # --- Inside Task Setup Menu ---
    elif action == "back_to_task_menu":
         # Should only be called from sub-prompts like setting link/time/interval
         return await task_show_settings_menu(update, context)
    elif action == "set_primary_link":
         return await task_prompt_set_link(update, context, 'primary')
    # Fallback link setting is started after primary link is set
    elif action == "set_time":
         return await task_prompt_start_time(update, context)
    elif action == "set_interval":
         return await task_select_interval(update, context)
    elif action == "set_target_type":
         return await task_select_target_type(update, context)
    elif action == "select_folder_target": # Comes from target type selection
         return await task_select_folder_for_target(update, context)
    elif action == "set_target_all": # Comes from target type selection
         return await task_set_target(update, context, 'all')
    elif action == "set_target_folder": # Comes from folder selection
         return await task_set_target(update, context, 'folder')
    elif action == "back_to_target_type": # Back button from folder selection
         return await task_select_target_type(update, context)
    elif action == "toggle_status":
         return await task_toggle_status(update, context)
    elif action == "save":
         return await task_save_settings(update, context)
    else:
        log.warning(f"Unhandled TASK CB: Action='{action}', Data='{data}'")
        await query.answer("Action not recognized.", show_alert=True)

    return None


async def handle_join_callback(update: Update, context: CallbackContext) -> str | int | None:
    query = update.callback_query
    user_id, lang = get_user_id_and_lang(update, context)
    data = query.data
    log.debug(f"Join Callback Route: Data='{data}'")

    if data.startswith(CALLBACK_JOIN_PREFIX + "select_"):
        return await handle_userbot_selection(update, context, CALLBACK_JOIN_PREFIX, STATE_WAITING_FOR_GROUP_LINKS)
    else:
        log.warning(f"Unhandled JOIN CB: Data='{data}'")
        await query.answer("Action not recognized.", show_alert=True)
    return None

async def handle_language_callback(update: Update, context: CallbackContext) -> str | int | None:
    query = update.callback_query
    data = query.data
    log.debug(f"Language Callback Route: Data='{data}'")

    if data.startswith(CALLBACK_LANG_PREFIX):
        return await set_language_handler(update, context)
    else:
        log.warning(f"Unhandled LANG CB: Data='{data}'")
        await query.answer("Action not recognized.", show_alert=True)
    return None

async def handle_interval_callback(update: Update, context: CallbackContext) -> str | int | None:
     """Handles interval button presses."""
     query = update.callback_query
     # user_id, lang = get_user_id_and_lang(update, context) # Not needed if calling specific handler
     data = query.data
     log.debug(f"Interval Callback Route: Data='{data}'")

     if data.startswith(CALLBACK_INTERVAL_PREFIX):
         return await process_interval_callback(update, context) # Call the specific handler
     else:
         log.warning(f"Unhandled INTERVAL CB: Data='{data}'")
         await query.answer("Action not recognized.", show_alert=True)
     return None


async def handle_generic_callback(update: Update, context: CallbackContext) -> str | int | None:
    query = update.callback_query
    user_id, lang = get_user_id_and_lang(update, context)
    data = query.data
    action = data.split(CALLBACK_GENERIC_PREFIX)[1] if CALLBACK_GENERIC_PREFIX in data else None
    log.debug(f"Generic Callback Route: Action='{action}', Data='{data}'")

    if action == "cancel":
        # Send cancellation message and end
        await reply_or_edit_text(update, context, get_text(user_id, 'cancelled', lang=lang))
        clear_conversation_data(context)
        return ConversationHandler.END
    elif action == "confirm_no":
        # Generic 'No' usually cancels the current flow and returns to a relevant menu
        await reply_or_edit_text(update, context, get_text(user_id, 'cancelled', lang=lang)) # Indicate cancellation
        clear_conversation_data(context)
        # Try to determine context: If admin, go to admin menu, else client menu
        if is_admin(user_id):
            return await admin_command(update, context)
        else:
             # Check if user is a valid client before showing client menu
             client_info = db.find_client_by_user_id(user_id)
             if client_info: return await client_menu(update, context)
             else: return ConversationHandler.END # End if not identifiable
    elif action == "noop": # No operation button (like page number indicator)
         await query.answer() # Just acknowledge the press
         return None # Stay in the same state
    else:
        log.warning(f"Unhandled GENERIC CB: Action='{action}', Data='{data}'")
        await query.answer("Action not recognized.", show_alert=True)
    return None

# --- Main Callback Router ---
async def main_callback_handler(update: Update, context: CallbackContext) -> str | int | None:
    """Handles all Inline Keyboard Button presses by routing based on prefix."""
    query = update.callback_query
    data = query.data
    user_id, lang = get_user_id_and_lang(update, context)

    if not query or not data:
        log.warning("main_callback_handler received update without query or data.")
        return None # Cannot proceed

    log.info(f"CB Route: User={user_id}, Data='{data}'")
    next_state = None

    # Answer immediately (unless specific handlers need to answer with alert)
    # Defer answering for confirmation buttons or potentially slow actions
    # if not ("confirm" in data or "delete" in data or "save" in data or "toggle" in data):
    #     try: await query.answer()
    #     except Exception as e: log.debug(f"Ignoring answer error: {e}")
    # Let specific handlers answer unless it's a simple navigation/display update


    # Route based on prefix
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
            log.warning(f"Unhandled CB prefix: User={user_id}, Data='{data}'")
            await query.answer("Unknown button pressed.", show_alert=True)
            next_state = ConversationHandler.END # End conversation for unknown buttons

        # Default answer if handler didn't answer and it wasn't noop
        if query and not query.answered and CALLBACK_GENERIC_PREFIX+"noop" not in data:
             try: await query.answer()
             except Exception: pass

    except Exception as e:
        log.error(f"Error processing callback data '{data}' for user {user_id}: {e}", exc_info=True)
        try:
             await query.answer(get_text(user_id, 'error_generic', lang=lang), show_alert=True)
        except Exception: pass
        await reply_or_edit_text(update, context, get_text(user_id, 'error_generic', lang=lang))
        clear_conversation_data(context)
        next_state = ConversationHandler.END

    return next_state


# --- Conversation Handler Definition ---
# Combine states logically
admin_auth_states = {
    STATE_WAITING_FOR_PHONE: [MessageHandler(Filters.text & ~Filters.command & Filters.chat_type.private, process_admin_phone)],
    STATE_WAITING_FOR_API_ID: [MessageHandler(Filters.text & ~Filters.command & Filters.chat_type.private, process_admin_api_id)],
    STATE_WAITING_FOR_API_HASH: [MessageHandler(Filters.text & ~Filters.command & Filters.chat_type.private, process_admin_api_hash)],
    STATE_WAITING_FOR_CODE_USERBOT: [MessageHandler(Filters.text & ~Filters.command & Filters.chat_type.private, process_admin_userbot_code)],
    STATE_WAITING_FOR_PASSWORD: [MessageHandler(Filters.text & ~Filters.command & Filters.chat_type.private, process_admin_userbot_password)],
}

admin_manage_states = {
    STATE_WAITING_FOR_SUB_DETAILS: [MessageHandler(Filters.text & ~Filters.command & Filters.chat_type.private, process_admin_invite_details)],
    STATE_WAITING_FOR_EXTEND_CODE: [MessageHandler(Filters.text & ~Filters.command & Filters.chat_type.private, process_admin_extend_code)],
    STATE_WAITING_FOR_EXTEND_DAYS: [MessageHandler(Filters.text & ~Filters.command & Filters.chat_type.private, process_admin_extend_days)],
    STATE_WAITING_FOR_ADD_USERBOTS_CODE: [MessageHandler(Filters.text & ~Filters.command & Filters.chat_type.private, process_admin_add_bots_code)],
    STATE_WAITING_FOR_ADD_USERBOTS_COUNT: [MessageHandler(Filters.text & ~Filters.command & Filters.chat_type.private, process_admin_add_bots_count)],
}

client_folder_states = {
    STATE_WAITING_FOR_FOLDER_NAME: [MessageHandler(Filters.text & ~Filters.command & Filters.chat_type.private, process_folder_name)],
    STATE_WAITING_FOR_FOLDER_ACTION: [CallbackQueryHandler(main_callback_handler)], # Handles button presses within edit menu
    STATE_FOLDER_RENAME_PROMPT: [MessageHandler(Filters.text & ~Filters.command & Filters.chat_type.private, process_folder_rename)],
    STATE_FOLDER_EDIT_REMOVE_SELECT: [CallbackQueryHandler(main_callback_handler)], # Handles selecting groups to remove
    STATE_WAITING_FOR_GROUP_LINKS: [MessageHandler(Filters.text & ~Filters.command & Filters.chat_type.private, process_folder_links)], # Used for adding links to folder
}

client_task_states = {
     STATE_TASK_SETUP: [CallbackQueryHandler(main_callback_handler)], # Main menu for task settings
     STATE_WAITING_FOR_PRIMARY_MESSAGE_LINK: [MessageHandler(Filters.text & ~Filters.command & Filters.chat_type.private, lambda u, c: process_task_link(u, c, 'primary'))],
     STATE_WAITING_FOR_FALLBACK_MESSAGE_LINK: [MessageHandler(Filters.text & ~Filters.command & Filters.chat_type.private, lambda u, c: process_task_link(u, c, 'fallback'))],
     STATE_WAITING_FOR_START_TIME: [MessageHandler(Filters.text & ~Filters.command & Filters.chat_type.private, process_task_start_time)],
     STATE_SELECT_TARGET_GROUPS: [CallbackQueryHandler(main_callback_handler)], # Selecting folder as target
}

client_join_states = {
     # Joins use STATE_WAITING_FOR_GROUP_LINKS but it's also used by folder add.
     # The context (CTX_SELECTED_BOTS vs CTX_FOLDER_ID) determines behavior.
     # Consider separate states if logic diverges significantly.
}

# --- Main Conversation Handler ---
main_conversation = ConversationHandler(
    entry_points=[
        CommandHandler('start', start_command, filters=Filters.chat_type.private),
        CommandHandler('admin', admin_command, filters=Filters.chat_type.private),
        # Add other top-level commands if needed
        # General callback handler for buttons pressed when not in a specific state (e.g., main menu buttons)
        CallbackQueryHandler(main_callback_handler)
    ],
    states={
        STATE_WAITING_FOR_CODE: [MessageHandler(Filters.text & ~Filters.command & Filters.chat_type.private, process_invitation_code)],

        # Admin States
        **admin_auth_states,
        **admin_manage_states,

        # Client States (Folder, Task, Join)
        **client_folder_states, # Includes waiting for group links for folders
        **client_task_states,
        # Join uses STATE_WAITING_FOR_GROUP_LINKS handled in client_folder_states

        # State for Bot Selection (used by multiple flows)
        STATE_WAITING_FOR_USERBOT_SELECTION: [CallbackQueryHandler(main_callback_handler)],

        # Other specific states if needed
        # STATE_WAITING_FOR_LANGUAGE: [CallbackQueryHandler(main_callback_handler)], # Language handled by direct callback

    },
    fallbacks=[
        CommandHandler('cancel', cancel_command, filters=Filters.chat_type.private),
        CommandHandler('start', start_command, filters=Filters.chat_type.private), # Allow restarting
        CommandHandler('admin', admin_command, filters=Filters.chat_type.private & Filters.user(ADMIN_IDS)), # Allow admin cmd
        # Fallback handles unexpected messages/commands within a state
        MessageHandler(Filters.all & Filters.chat_type.private, conversation_fallback),
        # CallbackQueryHandler to catch buttons that might not match the current state logic
        CallbackQueryHandler(main_callback_handler)
    ],
    allow_reentry=True, # Allow entering the conversation again via entry points
    # Optional: Define conversation timeouts
    # conversation_timeout=timedelta(minutes=15).total_seconds(),
    # per_user=True,
    # per_chat=True, # Keep state separate per user in private chat
    # per_message=False,
)

log.info("Handlers module loaded with implemented functions.")
# --- END OF FILE handlers.py ---
```

Please deploy this updated `handlers.py`. Hopefully, this resolves the import error, and the bot should now start without crashing during the import phase.
