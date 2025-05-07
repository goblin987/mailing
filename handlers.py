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
    STATE_WAITING_FOR_TASK_BOT, STATE_WAITING_FOR_TASK_MESSAGE,
    STATE_WAITING_FOR_TASK_SCHEDULE, STATE_WAITING_FOR_TASK_TARGET,
    STATE_ADMIN_TASK_CONFIRM,

    # Callback Prefixes
    CALLBACK_ADMIN_PREFIX, CALLBACK_CLIENT_PREFIX, CALLBACK_TASK_PREFIX,
    CALLBACK_FOLDER_PREFIX, CALLBACK_JOIN_PREFIX, CALLBACK_LANG_PREFIX,
    CALLBACK_INTERVAL_PREFIX, CALLBACK_GENERIC_PREFIX
)
from translations import get_text, language_names, translations
# from utils import get_user_id_and_lang, send_or_edit_message # get_user_id_and_lang is used, send_or_edit_message inlined
from utils import get_user_id_and_lang # Keep this

# Import admin handlers for use as state handlers
from admin_handlers import (
    admin_process_task_message as async_admin_process_task_message, # Renamed to avoid conflict if original was sync
    admin_process_task_schedule as async_admin_process_task_schedule,
    admin_process_task_target as async_admin_process_task_target
)


# --- Constants ---
ITEMS_PER_PAGE = 5 # For pagination in lists

# --- Conversation Context Keys ---
CTX_USER_ID = "_user_id"; CTX_LANG = "_lang"; CTX_PHONE = "phone"; CTX_API_ID = "api_id"
CTX_API_HASH = "api_hash"; CTX_AUTH_DATA = "auth_data"; CTX_INVITE_DETAILS = "invite_details"
CTX_EXTEND_CODE = "extend_code"; CTX_ADD_BOTS_CODE = "add_bots_code"; CTX_FOLDER_ID = "folder_id"
CTX_FOLDER_NAME = "folder_name"; CTX_FOLDER_ACTION = "folder_action"; CTX_SELECTED_BOTS = "selected_bots"
CTX_TARGET_GROUP_IDS_TO_REMOVE = "target_group_ids_to_remove"; CTX_TASK_PHONE = "task_phone"
CTX_TASK_SETTINGS = "task_settings"; CTX_PAGE = "page"; CTX_MESSAGE_ID = "message_id"
CTX_TASK_BOT = "task_bot"; CTX_TASK_MESSAGE = "task_message"; CTX_TASK_SCHEDULE = "task_schedule"; CTX_TASK_TARGET_TYPE = "task_target_type"; CTX_TASK_TARGET_FOLDER = "task_target_folder"


# --- Internal Helper Functions (Moved from utils.py or new) ---
async def _internal_send_or_edit_message(update: Update, context: CallbackContext, text: str, **kwargs):
    """Internal: Send a new message or edit the existing one based on context."""
    # This is a simplified version of send_or_edit_message from utils.py
    # In a real scenario, you'd ensure utils.py doesn't create circular dependencies
    # or use a more robust messaging utility.
    message = None 
    try:
        chat_id = update.effective_chat.id if update and update.effective_chat else None
        message_id = context.user_data.get(CTX_MESSAGE_ID) # Using the constant

        if not chat_id:
            log.error("[_internal_send_or_edit_message] No chat_id available.")
            return None
        
        if message_id and update.callback_query: # Typically edit only on callback query
            try:
                message = await context.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=message_id,
                    text=text,
                    **kwargs
                )
            except BadRequest as e:
                if 'message to edit not found' in str(e).lower() or \
                   'message_id_invalid' in str(e).lower() or \
                   'message can\'t be edited' in str(e).lower():
                    log.warning(f"Failed to edit message {message_id}, sending new: {e}")
                    context.user_data.pop(CTX_MESSAGE_ID, None)
                    message = await context.bot.send_message(chat_id=chat_id, text=text, **kwargs)
                    if message: context.user_data[CTX_MESSAGE_ID] = message.message_id
                else:
                    raise # Re-raise other BadRequests
            except Exception as e:
                log.warning(f"Generic error editing message {message_id}: {e}. Sending new.")
                context.user_data.pop(CTX_MESSAGE_ID, None)
                message = await context.bot.send_message(chat_id=chat_id, text=text, **kwargs)
                if message: context.user_data[CTX_MESSAGE_ID] = message.message_id
        else:
            # If it's a new command or message_id was cleared, send a new one
            # And delete previous message_id from context to avoid trying to edit a potentially unrelated old message
            context.user_data.pop(CTX_MESSAGE_ID, None)
            message = await context.bot.send_message(chat_id=chat_id, text=text, **kwargs)
            if message:
                context.user_data[CTX_MESSAGE_ID] = message.message_id
        
        return message
        
    except Exception as e:
        log.error(f"[_internal_send_or_edit_message] Unexpected error: {e}", exc_info=True)
        return None

async def _internal_show_menu_async(update: Update, context: CallbackContext, menu_builder_func):
    """Internal: Asynchronously shows a menu using the provided menu builder function."""
    user_id, lang = get_user_id_and_lang(update, context)
    title, markup, parse_mode = menu_builder_func(user_id, context) # menu_builder_func must be synchronous
    await _internal_send_or_edit_message(update, context, title, reply_markup=markup, parse_mode=parse_mode)


def simple_async_test(update: Update, context: CallbackContext, message: str):
    log.info(f"simple_async_test: ENTERED! Message: '{message}', ChatID: {update.effective_chat.id if update and update.effective_chat else 'N/A'}")
    try:
        if update and update.effective_chat:
            context.bot.send_message(chat_id=update.effective_chat.id, text=f"Async test successful: {message}")
            log.info(f"simple_async_test: Message sent via context.bot.send_message!")
        else:
            log.error("simple_async_test: Update or effective_chat is None.")
    except Exception as e:
        log.error(f"simple_async_test: EXCEPTION - {e}", exc_info=True)
    log.info("simple_async_test: EXITED!")


def clear_conversation_data(context: CallbackContext):
    user_id = context.user_data.get(CTX_USER_ID)
    lang = context.user_data.get(CTX_LANG)
    
    keys_to_keep = {CTX_USER_ID, CTX_LANG, CTX_MESSAGE_ID} # Keep message_id for send_or_edit
    
    # Iterate over a copy of keys if modifying the dictionary
    for key in list(context.user_data.keys()):
        if key not in keys_to_keep:
            context.user_data.pop(key, None)
    
    # Restore essential keys if they were somehow popped (shouldn't happen with above logic)
    if user_id is not None and CTX_USER_ID not in context.user_data :
        context.user_data[CTX_USER_ID] = user_id
    if lang is not None and CTX_LANG not in context.user_data:
        context.user_data[CTX_LANG] = lang
        
    log.debug(f"Cleared volatile conversation user_data for user {user_id or 'N/A'}")

# --- Synchronous Error Handler (for testing) ---
def sync_error_handler(update: object, context: CallbackContext) -> None:
    """Log the error (synchronous version for testing)."""
    log.error(msg="[sync_error_handler] Exception while handling an update:", exc_info=context.error)
    # Optionally, try sending a *synchronous* simple message if possible, but focus on logging.
    # Avoid async calls here.
    if isinstance(update, Update) and update.effective_chat:
        try:
            # This might still fail depending on context state, but it's sync
            # context.bot.send_message(update.effective_chat.id, "An error occurred.")
            pass # Let's just log for now to minimize complexity
        except Exception as e:
            log.error(f"[sync_error_handler] Failed to send sync error message: {e}")

# --- Original Async Error Handler (keep for reference or potential revert) ---
async def async_error_handler(update: object, context: CallbackContext) -> None:
    """Log the error and send a telegram message to notify the developer."""
    log.error(msg="[async_error_handler] Exception while handling an update:", exc_info=context.error)
    
    user_id = None
    chat_id = None
    try:
        if isinstance(update, Update):
            if update.effective_user:
                user_id = update.effective_user.id
            if update.effective_chat:
                chat_id = update.effective_chat.id
        
        if not chat_id and user_id:
            chat_id = user_id # Send to user's PM if chat_id isn't available (e.g. callback query error)
            
        if chat_id:
            log.info(f"[async_error_handler] Attempting to send generic error to user {user_id}, chat {chat_id}")
            # Determine language safely
            current_lang = 'en'
            if context and hasattr(context, 'user_data') and isinstance(context.user_data, dict):
                current_lang = context.user_data.get(CTX_LANG, 'en')
            
            error_message = get_text(user_id, 'error_generic', lang_override=current_lang) # Use lang_override
            await context.bot.send_message(chat_id=chat_id, text=error_message)
    except Exception as e:
        log.error(f"[async_error_handler] Failed to send async error message: {e}", exc_info=True)


def format_dt(timestamp: int | None, tz=LITHUANIA_TZ, fmt='%Y-%m-%d %H:%M') -> str:
    if not timestamp: return get_text(0, 'task_value_not_set', lang_override='en') 
    try: 
        dt_utc = datetime.fromtimestamp(timestamp, UTC_TZ)
        dt_local = dt_utc.astimezone(tz)
        return dt_local.strftime(fmt)
    except (ValueError, TypeError, OSError) as e: 
        log.warning(f"Could not format invalid timestamp: {timestamp}. Error: {e}")
        return "Invalid Date"

def build_client_menu(user_id, context: CallbackContext): # This is synchronous
    lang = context.user_data.get(CTX_LANG) # get_user_id_and_lang should have set this
    if user_id and not lang: 
        lang = db.get_user_language(user_id)
    lang = lang or 'en'

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

def build_admin_menu(user_id, context: CallbackContext): # This is synchronous
    lang = context.user_data.get(CTX_LANG) 
    if user_id and not lang: lang = db.get_user_language(user_id) 
    lang = lang or 'en'

    title = f"<b>{get_text(user_id, 'admin_panel_title', lang_override=lang)}</b>"
    parse_mode = ParseMode.HTML
    keyboard = [
        [
            InlineKeyboardButton(get_text(user_id, 'admin_button_add_userbot', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}add_bot_prompt"),
            InlineKeyboardButton(get_text(user_id, 'admin_button_remove_userbot', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}remove_bot_select?page=0")
        ],
        [InlineKeyboardButton(get_text(user_id, 'admin_button_list_userbots', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}list_bots?page=0")],
        [
            InlineKeyboardButton(get_text(user_id, 'admin_button_manage_tasks', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}manage_tasks"), # manage_tasks will lead to admin_task_menu
            InlineKeyboardButton(get_text(user_id, 'admin_button_view_tasks', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}view_tasks?page=0")
        ],
        [InlineKeyboardButton(get_text(user_id, 'admin_button_gen_invite', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}gen_invite_prompt")],
        [InlineKeyboardButton(get_text(user_id, 'admin_button_view_subs', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}view_subs?page=0")],
        [
            InlineKeyboardButton(get_text(user_id, 'admin_button_extend_sub', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}extend_sub_prompt"),
            InlineKeyboardButton(get_text(user_id, 'admin_button_assign_bots_client', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}assign_bots_prompt")
        ],
        [InlineKeyboardButton(get_text(user_id, 'admin_button_view_logs', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}view_logs?page=0")],
    ]
    markup = InlineKeyboardMarkup(keyboard)
    return title, markup, parse_mode

def build_pagination_buttons(base_callback_data: str, current_page: int, total_items: int, items_per_page: int, lang: str = 'en') -> list:
    buttons = [];
    if total_items <= items_per_page: return []
    total_pages = math.ceil(total_items / items_per_page); row = []
    if current_page > 0: prev_text = get_text(0, 'pagination_prev', lang_override=lang); row.append(InlineKeyboardButton(prev_text, callback_data=f"{base_callback_data}?page={current_page - 1}"))
    if total_pages > 1: page_text = get_text(0,'pagination_page',lang_override=lang).format(current=current_page + 1, total=total_pages); row.append(InlineKeyboardButton(page_text, callback_data=f"{CALLBACK_GENERIC_PREFIX}noop"))
    if current_page < total_pages - 1: next_text = get_text(0, 'pagination_next', lang_override=lang); row.append(InlineKeyboardButton(next_text, callback_data=f"{base_callback_data}?page={current_page + 1}"))
    if row: buttons.append(row)
    return buttons

# **MODIFIED HERE for simple_async_test**
async def start_command(update: Update, context: CallbackContext) -> int:
    """Handle /start command."""
    try:
        user_id, lang = get_user_id_and_lang(update, context)
        log.info(f"Start command received from user {user_id}")
        
        # We are in a conversation, so clear only volatile data
        # clear_conversation_data will keep CTX_USER_ID, CTX_LANG, CTX_MESSAGE_ID
        clear_conversation_data(context) 
        
        # Ensure user_id and lang are set in context if not already (get_user_id_and_lang does this)
        context.user_data[CTX_USER_ID] = user_id 
        context.user_data[CTX_LANG] = lang
        
        if is_admin(user_id):
            menu_text, markup, parse_mode = build_admin_menu(user_id, context)
            await _internal_send_or_edit_message(
                update, context,
                menu_text,
                reply_markup=markup,
                parse_mode=parse_mode
            )
            return ConversationHandler.END # End conversation if admin menu shown
        
        client = db.find_client_by_user_id(user_id)
        if client:
            menu_text, markup, parse_mode = build_client_menu(user_id, context)
            await _internal_send_or_edit_message(
                update, context,
                menu_text,
                reply_markup=markup,
                parse_mode=parse_mode
            )
            return ConversationHandler.END # End conversation if client menu shown
        
        # If not admin and not an existing client, ask for code
        await _internal_send_or_edit_message(
            update, context,
            get_text(user_id, 'ask_invitation_code', lang_override=lang), # use lang_override for safety
            parse_mode=ParseMode.HTML
        )
        return STATE_WAITING_FOR_CODE # Transition to wait for code
        
    except Exception as e:
        log.error(f"Error in start_command: {e}", exc_info=True)
        # Try to get user_id and lang again for error message
        user_id_err, lang_err = get_user_id_and_lang(update, context) 
        try:
            await _internal_send_or_edit_message(
                update, context,
                get_text(user_id_err, 'error_generic', lang_override=lang_err),
                parse_mode=ParseMode.HTML
            )
        except Exception as send_err:
            log.error(f"Failed to send error message in start_command handler: {send_err}")
        return ConversationHandler.END # Always end conversation on unhandled error

async def process_invitation_code(update: Update, context: CallbackContext) -> str | int:
    user_id, lang = get_user_id_and_lang(update, context)
    code_input = update.message.text.strip() # No .lower() for invitation codes
    log.info(f"Processing invitation code '{code_input}' for user {user_id}")
    
    client_info_db = db.find_client_by_user_id(user_id)
    if client_info_db:
        now_ts = int(datetime.now(UTC_TZ).timestamp())
        if client_info_db['subscription_end'] > now_ts:
            await _internal_send_or_edit_message(update, context, get_text(user_id, 'user_already_active', lang_override=lang))
            return await client_menu(update, context) # Show menu and end

    success, reason_or_client_data = db.activate_client(code_input, user_id)

    if success:
        # activation_success is the only one that implies data. Others are error strings
        if reason_or_client_data == "activation_success":
            # Re-fetch client to get updated sub_end
            client_data = db.find_client_by_code(code_input) 
            if client_data:
                await _internal_send_or_edit_message(
                    update, context,
                    get_text(user_id, 'activation_success', lang_override=lang) # Simplified message
                )
                return await client_menu(update, context) # Show menu and end
            else: # Should not happen if activation was successful
                await _internal_send_or_edit_message(update, context, get_text(user_id, 'activation_error', lang_override=lang))
                return STATE_WAITING_FOR_CODE # Ask for code again
        elif reason_or_client_data == "already_active": # User used their own code again
             await _internal_send_or_edit_message(update, context, get_text(user_id, 'already_active', lang_override=lang))
             return await client_menu(update, context)
        else: # Should be an error string from activate_client if success was True but not "activation_success"
             log.error(f"activate_client returned True but unexpected reason: {reason_or_client_data}")
             await _internal_send_or_edit_message(update, context, get_text(user_id, 'activation_error', lang_override=lang))
             return STATE_WAITING_FOR_CODE

    else: # success is False, reason_or_client_data is an error key
        error_key = reason_or_client_data
        # Map db error keys to translation keys
        translation_map = {
            "user_already_active": "user_already_active",
            "code_not_found": "code_not_found",
            "code_already_used": "code_already_used",
            "subscription_expired": "subscription_expired", # or code_expired
            "activation_error": "activation_error",
            "activation_db_error": "activation_db_error",
        }
        message_key = translation_map.get(error_key, 'activation_error') # Default to generic activation error
        await _internal_send_or_edit_message(update, context, get_text(user_id, message_key, lang_override=lang))
        return STATE_WAITING_FOR_CODE # Ask for code again

# **MODIFIED HERE for simple_async_test**
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
            await _internal_send_or_edit_message(
                update, context,
                get_text(user_id, 'not_admin', lang_override=lang), # Corrected key
                parse_mode=ParseMode.HTML
            )
            return ConversationHandler.END # End if not admin
    
        menu_text, markup, parse_mode = build_admin_menu(user_id, context)
        await _internal_send_or_edit_message(
            update, context,
            menu_text,
            reply_markup=markup,
            parse_mode=parse_mode
        )
        return ConversationHandler.END # End conversation after showing admin menu

    except Exception as e:
        log.error(f"Error in admin_command: {e}", exc_info=True)
        user_id_err, lang_err = get_user_id_and_lang(update, context)
        try:
             await _internal_send_or_edit_message(
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
        user_id, lang = get_user_id_and_lang(update, context) # Use helper
        
        # Send cancellation message
        # Use _internal_send_or_edit_message as it handles message_id
        await _internal_send_or_edit_message(
            update, context,
            get_text(user_id, 'cancelled', lang_override=lang),
            parse_mode=ParseMode.HTML
        )
        # Clear conversation data AFTER sending message, so message_id might be used
        clear_conversation_data(context)

        # Determine where to go after cancel
        if is_admin(user_id):
             return await admin_command(update, context)
        client = db.find_client_by_user_id(user_id)
        if client:
             return await client_menu(update, context)
        
        return ConversationHandler.END # Default end
        
    except Exception as e:
        log.error(f"Error in cancel_command: {e}", exc_info=True)
        await async_error_handler(update, context) # Use the async error handler
        return ConversationHandler.END

async def conversation_fallback(update: Update, context: CallbackContext) -> int:
    """Fallback handler for unexpected messages during a conversation."""
    try:
        user_id, lang = get_user_id_and_lang(update, context)
        
        log.warning(f"Conversation fallback triggered for user {user_id}. Update: {update.to_json()}")
        
        await _internal_send_or_edit_message(
            update, context,
            get_text(user_id, 'session_expired', lang_override=lang), # Using 'session_expired' might be more appropriate
            parse_mode=ParseMode.HTML
        )
        clear_conversation_data(context)
        return ConversationHandler.END # Always end the conversation on a fallback
        
    except Exception as e:
        log.error(f"Error in conversation_fallback: {e}", exc_info=True)
        await async_error_handler(update, context)
        return ConversationHandler.END

async def client_menu(update: Update, context: CallbackContext) -> int:
    user_id, lang = get_user_id_and_lang(update, context)
    # clear_conversation_data(context) # Don't clear here if it's a return point
    log.info(f"Client menu: UserID={user_id}, Lang={lang}")
    
    client_info = db.find_client_by_user_id(user_id)
    if not client_info:
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'unauthorized', lang_override=lang))
        return ConversationHandler.END
    
    now_ts = int(datetime.now(UTC_TZ).timestamp())
    if client_info['subscription_end'] < now_ts:
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'subscription_expired', lang_override=lang))
        return ConversationHandler.END
    
    await _internal_show_menu_async(update, context, build_client_menu)
    return ConversationHandler.END

async def client_ask_select_language(update: Update, context: CallbackContext) -> int:
    user_id, lang = get_user_id_and_lang(update, context)
    log.info(f"client_ask_select_language: User {user_id}, current lang {lang}")
    buttons = []
    row = []
    sorted_languages = sorted(language_names.items(), key=lambda item: item[1])
    for code, name in sorted_languages: 
        row.append(InlineKeyboardButton(name, callback_data=f"{CALLBACK_LANG_PREFIX}{code}"))
        if len(row) >= 2: 
            buttons.append(row)
            row = []
    if row: 
        buttons.append(row)
    
    buttons.append([InlineKeyboardButton(get_text(user_id, 'button_back', lang_override=lang), callback_data=f"{CALLBACK_CLIENT_PREFIX}back_to_menu")])
    markup = InlineKeyboardMarkup(buttons)
    await _internal_send_or_edit_message(update, context, get_text(user_id, 'select_language', lang_override=lang), reply_markup=markup)
    # This is called via callback, so it should end or return a state if it expects further interaction
    # For language selection via callback, the callback handler itself (set_language_handler) will end.
    # This function just displays the menu, so no specific state return needed here if called by a CB that expects menu display.
    # If it were an entry point to a conversation for language selection via text, it would return a state.
    # Since it's triggered by a client callback that then calls this, the main_callback_handler's return is what matters.
    return ConversationHandler.END # Or None if the callback handler manages the end

async def set_language_handler(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    await query.answer() # Answer callback quickly
    user_id = query.from_user.id
    lang_code = query.data.split(CALLBACK_LANG_PREFIX)[1]
    current_lang_for_cb_answer = context.user_data.get(CTX_LANG, 'en') 
    log.info(f"set_language_handler: User {user_id} trying to set lang to {lang_code}. Current context lang for CB: {current_lang_for_cb_answer}")

    if lang_code not in language_names:
        log.warning(f"set_language_handler: User {user_id} selected invalid lang_code {lang_code}")
        # No need to answer query again if already answered.
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'error_invalid_input', lang_override=current_lang_for_cb_answer))
        return ConversationHandler.END

    if db.set_user_language(user_id, lang_code):
         context.user_data[CTX_LANG] = lang_code 
         log.info(f"set_language_handler: User {user_id} language set to {lang_code} in DB and context.")
         
         success_text = get_text(user_id, 'language_set', lang_override=lang_code, lang_name=language_names[lang_code])
         keyboard = [[ InlineKeyboardButton(get_text(user_id, 'button_main_menu', lang_override=lang_code), callback_data=f"{CALLBACK_CLIENT_PREFIX}back_to_menu")]]
         markup = InlineKeyboardMarkup(keyboard)
         await _internal_send_or_edit_message(update, context, success_text, reply_markup=markup)
    else:
        log.error(f"set_language_handler: Failed to set language in DB for user {user_id} to {lang_code}")
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'language_set_error', lang_override=current_lang_for_cb_answer))
    
    return ConversationHandler.END 

async def process_admin_phone(update: Update, context: CallbackContext) -> str | int:
    user_id, lang = get_user_id_and_lang(update, context)
    phone = update.message.text.strip()
    log.info(f"process_admin_phone: Processing phone {phone} for user {user_id}")

    if not re.match(r"^\+[1-9]\d{1,14}$", phone): # Basic international phone format validation
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_invalid_phone', lang_override=lang))
        return STATE_WAITING_FOR_PHONE
    
    context.user_data[CTX_PHONE] = phone
    await _internal_send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_prompt_api_id', lang_override=lang))
    return STATE_WAITING_FOR_API_ID


async def process_admin_api_id(update: Update, context: CallbackContext) -> str | int:
    user_id, lang = get_user_id_and_lang(update, context); api_id_str = update.message.text.strip()
    try:
        api_id = int(api_id_str)
        if api_id <= 0:
            raise ValueError("API ID must be positive")
        context.user_data[CTX_API_ID] = api_id
        log.info(f"Admin {user_id} API ID OK for {context.user_data.get(CTX_PHONE)}")
    except (ValueError, TypeError):
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_invalid_api_id', lang_override=lang))
        return STATE_WAITING_FOR_API_ID
    await _internal_send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_prompt_api_hash', lang_override=lang))
    return STATE_WAITING_FOR_API_HASH

async def process_admin_api_hash(update: Update, context: CallbackContext) -> str | int:
    user_id, lang = get_user_id_and_lang(update, context); api_hash = update.message.text.strip()
    if not api_hash or len(api_hash) < 30 or not re.match('^[a-fA-F0-9]+$', api_hash): 
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_invalid_api_hash', lang_override=lang))
        return STATE_WAITING_FOR_API_HASH
    
    context.user_data[CTX_API_HASH] = api_hash
    phone = context.user_data.get(CTX_PHONE)
    api_id = context.user_data.get(CTX_API_ID)

    if not phone or not api_id: 
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'session_expired', lang_override=lang))
        clear_conversation_data(context); return ConversationHandler.END
    
    log.info(f"Admin {user_id} API Hash OK for {phone}. Starting authentication flow.")
    await _internal_send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_auth_connecting', lang_override=lang, phone=html.escape(phone)))
    
    try:
        # telethon_api.start_authentication_flow is async
        auth_status, auth_data = await telethon_api.start_authentication_flow(phone, api_id, api_hash)
        log.info(f"Authentication start result for {phone}: Status='{auth_status}'")

        if auth_status == 'code_needed': 
            context.user_data[CTX_AUTH_DATA] = auth_data
            await _internal_send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_prompt_code', lang_override=lang, phone=html.escape(phone)))
            return STATE_WAITING_FOR_CODE_USERBOT
        elif auth_status == 'password_needed': 
            context.user_data[CTX_AUTH_DATA] = auth_data
            await _internal_send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_prompt_password', lang_override=lang, phone=html.escape(phone)))
            return STATE_WAITING_FOR_PASSWORD
        elif auth_status == 'already_authorized': # This case might be handled by Telethon itself
             log.warning(f"Userbot {phone} is already authorized (Telethon check).");
             # Ensure DB record exists or is updated
             if not db.find_userbot(phone): 
                 safe_phone_part = re.sub(r'[^\d]', '', phone)
                 session_file_rel = f"{safe_phone_part or f'unknown_{random.randint(1000,9999)}'}.session"
                 db.add_userbot(phone, session_file_rel, api_id, api_hash, 'active')
             else: 
                 db.update_userbot_status(phone, 'active')
             
             # Schedule get_userbot_runtime_info to ensure it's running and get username
             context.dispatcher.run_async(telethon_api.get_userbot_runtime_info, phone) # This is not async, correct usage
             
             await _internal_send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_already_auth', lang_override=lang, display_name=html.escape(phone)))
             clear_conversation_data(context); return ConversationHandler.END
        else: # Error case
            error_msg = auth_data.get('error_message', 'Unknown error during auth start')
            log.error(f"Auth start error for {phone}: {error_msg}")
            
            locals_for_format = {'phone': html.escape(phone), 'error': html.escape(error_msg)}
            key = 'admin_userbot_auth_error_unknown' # Default error key
            if "flood wait" in error_msg.lower(): 
                key = 'admin_userbot_auth_error_flood'
                seconds_match = re.search(r'\d+', error_msg)
                locals_for_format['seconds'] = seconds_match.group(0) if seconds_match else '?'
            elif "config" in error_msg.lower() or "invalid api" in error_msg.lower(): 
                key = 'admin_userbot_auth_error_config'
            elif "invalid phone" in error_msg.lower(): 
                key = 'admin_userbot_auth_error_phone_invalid'
            elif "connection" in error_msg.lower() or "timeout" in error_msg.lower():
                key = 'admin_userbot_auth_error_connect'
            
            await _internal_send_or_edit_message(update, context, get_text(user_id, key, lang_override=lang, **locals_for_format))
            clear_conversation_data(context); return ConversationHandler.END

    except Exception as e: 
        log.error(f"Exception during start_authentication_flow for {phone}: {e}", exc_info=True)
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_auth_error_unknown', lang_override=lang, phone=html.escape(phone), error=html.escape(str(e))))
        clear_conversation_data(context); return ConversationHandler.END

async def process_admin_userbot_code(update: Update, context: CallbackContext) -> str | int:
    user_id, lang = get_user_id_and_lang(update, context)
    auth_data = context.user_data.get(CTX_AUTH_DATA)
    phone_num = context.user_data.get(CTX_PHONE) # Get phone from initial entry

    if not auth_data or not phone_num:
        log.error(f"process_admin_userbot_code: Missing auth_data or phone_num for user {user_id}")
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'session_expired', lang_override=lang))
        clear_conversation_data(context)
        return ConversationHandler.END
    
    code = update.message.text.strip()
    log.info(f"process_admin_userbot_code: Processing code for phone {phone_num}")
    
    try:
        # complete_authentication_flow is async
        status, result_data = await telethon_api.complete_authentication_flow(auth_data, code=code)
        
        if status == 'success':
            final_phone = result_data.get('phone', phone_num)
            username = result_data.get('username')
            display_name = f"@{username}" if username else final_phone
            log.info(f"Code accepted for {final_phone}. Authentication successful.")
            
            # Trigger runtime info update for the newly authed bot
            context.dispatcher.run_async(telethon_api.get_userbot_runtime_info, final_phone) # Not async, correct PTB usage

            await _internal_send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_auth_success', lang_override=lang, display_name=html.escape(display_name)))
            clear_conversation_data(context)
            return ConversationHandler.END
        elif status == 'password_needed': # Should not happen here if flow is correct, but handle defensively
            log.warning(f"Password unexpectedly needed after code submission for {phone_num}.")
            # Potentially update auth_data if result_data contains new state for password
            context.user_data[CTX_AUTH_DATA] = result_data # Assuming result_data is the new auth_data
            await _internal_send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_prompt_password', lang_override=lang, phone=html.escape(phone_num)))
            return STATE_WAITING_FOR_PASSWORD
        else: # Error
            error_msg = result_data.get('error_message', "Unknown error during code submission.")
            log.warning(f"Code submission failed for {phone_num}: {error_msg}")
            
            error_key = 'admin_userbot_auth_error_code_invalid' # Default
            if "flood wait" in error_msg.lower(): error_key = 'admin_userbot_auth_error_flood'
            
            await _internal_send_or_edit_message(update, context, get_text(user_id, error_key, lang_override=lang, phone=html.escape(phone_num), error=html.escape(error_msg), seconds=re.search(r'\d+', error_msg).group(0) if "flood" in error_key else 'N/A'))
            # If error is not code invalid, maybe end conversation
            if error_key != 'admin_userbot_auth_error_code_invalid':
                clear_conversation_data(context)
                return ConversationHandler.END
            return STATE_WAITING_FOR_CODE_USERBOT # Re-ask for code if it was invalid

    except Exception as e:
        log.error(f"process_admin_userbot_code: Exception submitting code for {phone_num}: {e}", exc_info=True)
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_auth_error_unknown', lang_override=lang, phone=html.escape(phone_num), error=html.escape(str(e))))
        clear_conversation_data(context)
        return ConversationHandler.END

async def process_admin_userbot_password(update: Update, context: CallbackContext) -> str | int:
    user_id, lang = get_user_id_and_lang(update, context)
    auth_data = context.user_data.get(CTX_AUTH_DATA)
    phone_num = context.user_data.get(CTX_PHONE)

    if not auth_data or not phone_num:
        log.error(f"process_admin_userbot_password: Missing auth_data or phone_num for user {user_id}")
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'session_expired', lang_override=lang))
        clear_conversation_data(context)
        return ConversationHandler.END
    
    password = update.message.text.strip()
    log.info(f"process_admin_userbot_password: Processing 2FA password for phone {phone_num}")
    
    try:
        # complete_authentication_flow is async
        status, result_data = await telethon_api.complete_authentication_flow(auth_data, password=password)

        if status == 'success':
            final_phone = result_data.get('phone', phone_num)
            username = result_data.get('username')
            display_name = f"@{username}" if username else final_phone
            log.info(f"Password accepted for {final_phone}. Authentication successful.")

            context.dispatcher.run_async(telethon_api.get_userbot_runtime_info, final_phone)

            await _internal_send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_auth_success', lang_override=lang, display_name=html.escape(display_name)))
            clear_conversation_data(context)
            return ConversationHandler.END
        else: # Error
            error_msg = result_data.get('error_message', "Unknown error during password submission.")
            log.warning(f"Password submission failed for {phone_num}: {error_msg}")

            error_key = 'admin_userbot_auth_error_password_invalid' # Default
            if "flood wait" in error_msg.lower(): error_key = 'admin_userbot_auth_error_flood'
            
            await _internal_send_or_edit_message(update, context, get_text(user_id, error_key, lang_override=lang, phone=html.escape(phone_num), error=html.escape(error_msg), seconds=re.search(r'\d+', error_msg).group(0) if "flood" in error_key else 'N/A'))
            
            if error_key != 'admin_userbot_auth_error_password_invalid':
                clear_conversation_data(context)
                return ConversationHandler.END
            return STATE_WAITING_FOR_PASSWORD # Re-ask for password

    except Exception as e:
        log.error(f"process_admin_userbot_password: Exception submitting password for {phone_num}: {e}", exc_info=True)
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_auth_error_unknown', lang_override=lang, phone=html.escape(phone_num), error=html.escape(str(e))))
        clear_conversation_data(context)
        return ConversationHandler.END

async def process_admin_invite_details(update: Update, context: CallbackContext) -> int:
    user_id, lang = get_user_id_and_lang(update, context)
    
    if not is_admin(user_id): # Should be redundant if conv entry is admin-only
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'unauthorized', lang_override=lang))
        return ConversationHandler.END
    
    try:
        days_str = update.message.text.strip()
        days = int(days_str)
        if days <= 0:
            await _internal_send_or_edit_message(update, context, get_text(user_id, 'admin_invite_invalid_days', lang_override=lang))
            return STATE_WAITING_FOR_SUB_DETAILS
        
        invite_code = db.generate_invite_code() # This is synchronous
        if invite_code:
            # db.create_invitation is synchronous (used to be store_invite_code)
            end_datetime = datetime.now(UTC_TZ) + timedelta(days=days)
            sub_end_ts = int(end_datetime.timestamp())
            if db.create_invitation(invite_code, sub_end_ts):
                db.log_event_db("Invite Code Generated", f"Code: {invite_code}, Days: {days}", user_id=user_id)
                await _internal_send_or_edit_message(
                    update, context,
                    get_text(user_id, 'admin_invite_generated', lang_override=lang, code=invite_code, days=days)
                )
            else: # create_invitation failed (e.g. duplicate code, though generate_invite_code should prevent this)
                db.log_event_db("Invite Code Store Failed", f"Code: {invite_code}, Days: {days}", user_id=user_id)
                await _internal_send_or_edit_message(update, context, get_text(user_id, 'admin_invite_db_error', lang_override=lang))
        else: # generate_invite_code failed
            await _internal_send_or_edit_message(update, context, get_text(user_id, 'admin_invite_db_error', lang_override=lang)) # Or a more specific error
    except ValueError:
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'admin_invite_invalid_days', lang_override=lang))
        return STATE_WAITING_FOR_SUB_DETAILS
    except Exception as e:
        log.error(f"Error processing admin invite details: {e}", exc_info=True)
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'error_generic', lang_override=lang))
        # Don't return state here, let it fall through to END
    
    clear_conversation_data(context)
    return ConversationHandler.END

async def process_admin_extend_code(update: Update, context: CallbackContext) -> int:
    user_id, lang = get_user_id_and_lang(update, context); code = update.message.text.strip() # No .lower() generally
    client = db.find_client_by_code(code)
    
    if not client:
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'admin_extend_invalid_code', lang_override=lang))
        return STATE_WAITING_FOR_EXTEND_CODE
    
    context.user_data[CTX_EXTEND_CODE] = code
    end_date_str = format_dt(client['subscription_end'])
    await _internal_send_or_edit_message(update, context, get_text(user_id, 'admin_extend_prompt_days', lang_override=lang, code=html.escape(code), end_date=end_date_str))
    return STATE_WAITING_FOR_EXTEND_DAYS

async def process_admin_extend_days(update: Update, context: CallbackContext) -> int:
    user_id, lang = get_user_id_and_lang(update, context); days_str = update.message.text.strip(); code = context.user_data.get(CTX_EXTEND_CODE)
    if not code: 
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'session_expired', lang_override=lang))
        clear_conversation_data(context); return ConversationHandler.END
    try:
        days_to_add = int(days_str)
        if days_to_add <= 0: raise ValueError("Days must be positive")
    except (ValueError, AssertionError):
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'admin_extend_invalid_days', lang_override=lang))
        return STATE_WAITING_FOR_EXTEND_DAYS
    
    client = db.find_client_by_code(code) # Re-fetch to be safe
    if not client: 
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'admin_extend_invalid_code', lang_override=lang))
        clear_conversation_data(context); return ConversationHandler.END

    current_end_ts = client['subscription_end']; now_ts = int(datetime.now(UTC_TZ).timestamp())
    # If sub already expired, extend from now. Otherwise, from current end.
    start_ts = max(now_ts, current_end_ts) 
    start_dt = datetime.fromtimestamp(start_ts, UTC_TZ); new_end_dt = start_dt + timedelta(days=days_to_add); new_end_ts = int(new_end_dt.timestamp())
    
    if db.extend_subscription(code, new_end_ts): 
        new_end_date_str = format_dt(new_end_ts)
        client_user_id_for_log = client.get('user_id') # Might be None if not activated
        db.log_event_db("Subscription Extended", f"Code: {code}, Added: {days_to_add} days, New End: {new_end_date_str}", user_id=user_id, details=f"Client UserID: {client_user_id_for_log}")
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'admin_extend_success', lang_override=lang, code=html.escape(code), days=days_to_add, new_end_date=new_end_date_str))
    else: 
        db.log_event_db("Sub Extend Failed", f"Code: {code}", user_id=user_id)
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'admin_extend_db_error', lang_override=lang))
    
    clear_conversation_data(context); return ConversationHandler.END

async def process_admin_add_bots_code(update: Update, context: CallbackContext) -> int:
    user_id, lang = get_user_id_and_lang(update, context); code = update.message.text.strip(); # No .lower()
    client = db.find_client_by_code(code)
    if not client: 
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'admin_assignbots_invalid_code', lang_override=lang))
        return STATE_WAITING_FOR_ADD_USERBOTS_CODE
    
    context.user_data[CTX_ADD_BOTS_CODE] = code
    # Get count of bots already assigned to THIS client's code
    current_bots_for_client = db.get_all_userbots(assigned_status=True) # Get all assigned bots
    current_count = 0
    if current_bots_for_client:
        current_count = sum(1 for b in current_bots_for_client if b['assigned_client'] == code)
        
    await _internal_send_or_edit_message(update, context, get_text(user_id, 'admin_assignbots_prompt_count', lang_override=lang, code=html.escape(code), current_count=current_count))
    return STATE_WAITING_FOR_ADD_USERBOTS_COUNT

async def process_admin_add_bots_count(update: Update, context: CallbackContext) -> int:
    user_id, lang = get_user_id_and_lang(update, context); count_str = update.message.text.strip(); code = context.user_data.get(CTX_ADD_BOTS_CODE)
    if not code: 
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'session_expired', lang_override=lang))
        clear_conversation_data(context); return ConversationHandler.END
    try:
        count_to_add = int(count_str)
        if count_to_add <= 0: raise ValueError("Count must be positive")
    except (ValueError, AssertionError):
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'admin_assignbots_invalid_count', lang_override=lang))
        return STATE_WAITING_FOR_ADD_USERBOTS_COUNT
    
    # Get *genuinely* unassigned and active bots
    available_bots_phones = db.get_unassigned_userbots(limit=count_to_add) # This already filters by status='active'
    
    if len(available_bots_phones) < count_to_add: 
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'admin_assignbots_no_bots_available', lang_override=lang, needed=count_to_add, available=len(available_bots_phones)))
        # Don't return state here, let it fall through to end or offer retry via buttons if desired
        clear_conversation_data(context); return ConversationHandler.END # End for now
    
    # Assign the exact number requested from the available list
    bots_to_actually_assign = available_bots_phones[:count_to_add]
    success, message_from_db = db.assign_userbots_to_client(code, bots_to_actually_assign)
    
    client_user_id_for_log = db.find_client_by_code(code)['user_id'] if db.find_client_by_code(code) else None # For logging
    
    if success:
        assigned_count_match = re.search(r"Successfully assigned (\d+)", message_from_db)
        actually_assigned_in_db = int(assigned_count_match.group(1)) if assigned_count_match else 0
        
        final_message_key = 'admin_assignbots_success'
        format_params = {'count': actually_assigned_in_db, 'code': html.escape(code)}

        if actually_assigned_in_db != len(bots_to_actually_assign) or "Failed:" in message_from_db :
            final_message_key = 'admin_assignbots_partial_success'
            format_params = {
                'assigned_count': actually_assigned_in_db, 
                'requested_count': len(bots_to_actually_assign), 
                'code': html.escape(code)
            }
        
        response_text = get_text(user_id, final_message_key, lang_override=lang, **format_params)
        if "Failed:" in message_from_db: # Add details if there were failures
            response_text += f"\nDetails: {html.escape(message_from_db)}"

        await _internal_send_or_edit_message(update, context, response_text)
        db.log_event_db("Userbots Assigned", f"Code: {code}, Requested: {count_to_add}, AssignedPhones: {bots_to_actually_assign}, DB_Msg: {message_from_db}", user_id=user_id, details=f"ClientUID: {client_user_id_for_log}")
        
        for phone in bots_to_actually_assign: 
            context.dispatcher.run_async(telethon_api.get_userbot_runtime_info, phone) # Update runtime status
    else: # DB function returned False
        db.log_event_db("Bot Assign Failed Overall", f"Code: {code}, Reason: {message_from_db}", user_id=user_id, details=f"ClientUID: {client_user_id_for_log}")
        fail_message = get_text(user_id, 'admin_assignbots_failed', lang_override=lang, code=html.escape(code)) + f"\nError: {html.escape(message_from_db)}"
        await _internal_send_or_edit_message(update, context, fail_message)
        
    clear_conversation_data(context); return ConversationHandler.END

async def client_folder_menu(update: Update, context: CallbackContext) -> int:
    user_id, lang = get_user_id_and_lang(update, context)
    # clear_conversation_data(context) # Usually called by the function leading here, or on exit
    log.info(f"client_folder_menu: UserID={user_id}, Lang={lang}")
    
    # Refresh bot statuses if needed
    # available_bots = db.get_client_bots(user_id)
    # if available_bots:
    #     for phone in available_bots:
    #         context.dispatcher.run_async(telethon_api.get_userbot_runtime_info, phone) # Not async
    
    await _internal_show_menu_async(update, context, build_folder_menu)
    return ConversationHandler.END # Folder menu is usually an endpoint, further actions via CB

def build_folder_menu(user_id, context: CallbackContext): # Synchronous
    lang = context.user_data.get(CTX_LANG) 
    if user_id and not lang: lang = db.get_user_language(user_id)
    lang = lang or 'en'

    folders = db.get_folders_by_user(user_id)
    text = get_text(user_id, 'folder_menu_title', lang_override=lang)
    keyboard = []
    keyboard.append([InlineKeyboardButton(get_text(user_id, 'folder_menu_create', lang_override=lang), callback_data=f"{CALLBACK_FOLDER_PREFIX}create_prompt")])
    if folders: 
        keyboard.append([InlineKeyboardButton(get_text(user_id, 'folder_menu_edit', lang_override=lang), callback_data=f"{CALLBACK_FOLDER_PREFIX}select_edit?page=0")])
        keyboard.append([InlineKeyboardButton(get_text(user_id, 'folder_menu_delete', lang_override=lang), callback_data=f"{CALLBACK_FOLDER_PREFIX}select_delete?page=0")])
    else: 
        text += "\n" + get_text(user_id, 'folder_no_folders', lang_override=lang)
    keyboard.append([InlineKeyboardButton(get_text(user_id, 'button_back', lang_override=lang), callback_data=f"{CALLBACK_CLIENT_PREFIX}back_to_menu")])
    markup = InlineKeyboardMarkup(keyboard)
    return text, markup, ParseMode.HTML

async def process_folder_name(update: Update, context: CallbackContext) -> int:
    user_id, lang = get_user_id_and_lang(update, context); folder_name = update.message.text.strip()
    if not folder_name: 
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'error_invalid_input', lang_override=lang))
        return STATE_WAITING_FOR_FOLDER_NAME
    
    log.info(f"User {user_id} attempting to create folder: {folder_name}")
    folder_id_or_status = db.add_folder(folder_name, user_id) # Synchronous

    if isinstance(folder_id_or_status, int) and folder_id_or_status > 0: 
        folder_id = folder_id_or_status
        db.log_event_db("Folder Created", f"Name: {folder_name}, ID: {folder_id}", user_id=user_id)
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'folder_create_success', lang_override=lang, name=html.escape(folder_name)))
        return await client_folder_menu(update, context) # Show folder menu and end this path
    elif folder_id_or_status is None: # Duplicate
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'folder_create_error_exists', lang_override=lang, name=html.escape(folder_name)))
        return STATE_WAITING_FOR_FOLDER_NAME # Re-ask
    else: # DB error (-1)
        db.log_event_db("Folder Create Failed", f"Name: {folder_name}, Reason: DB Error", user_id=user_id)
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'folder_create_error_db', lang_override=lang))
        clear_conversation_data(context); return ConversationHandler.END

async def client_select_folder_to_edit_or_delete(update: Update, context: CallbackContext, action: str) -> int:
    query = update.callback_query; await query.answer()
    user_id, lang = get_user_id_and_lang(update, context);
    try:
        current_page = 0
        if query and query.data and '?' in query.data:
            _, page_data = query.data.split('?', 1)
            current_page = int(page_data.split('=')[1])
    except (ValueError, IndexError, AttributeError): current_page = 0
    
    folders = db.get_folders_by_user(user_id)
    if not folders:
        # No query.answer() here as it was done above.
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'folder_no_folders', lang_override=lang), reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(get_text(user_id,'button_back',lang_override=lang), callback_data=f"{CALLBACK_FOLDER_PREFIX}back_to_manage")]]))
        return ConversationHandler.END # Or return to folder menu state if that's desired
    
    total_items = len(folders); start_index = current_page * ITEMS_PER_PAGE; end_index = start_index + ITEMS_PER_PAGE; folders_page = folders[start_index:end_index]
    text_key = 'folder_select_edit' if action == 'edit' else 'folder_select_delete'; text = get_text(user_id, text_key, lang_override=lang); keyboard = []
    for folder in folders_page: 
        button_text = html.escape(folder['name']); 
        callback_action_prefix = "edit_selected" if action == 'edit' else "delete_selected_prompt" # Changed for delete
        callback_data = f"{CALLBACK_FOLDER_PREFIX}{callback_action_prefix}?id={folder['id']}"; 
        keyboard.append([InlineKeyboardButton(button_text, callback_data=callback_data)])
    
    base_callback = f"{CALLBACK_FOLDER_PREFIX}select_{action}"; pagination_buttons = build_pagination_buttons(base_callback, current_page, total_items, ITEMS_PER_PAGE, lang=lang); keyboard.extend(pagination_buttons)
    keyboard.append([InlineKeyboardButton(get_text(user_id, 'button_back', lang_override=lang), callback_data=f"{CALLBACK_FOLDER_PREFIX}back_to_manage")]); markup = InlineKeyboardMarkup(keyboard)
    
    await _internal_send_or_edit_message(update, context, text, reply_markup=markup)
    return STATE_WAITING_FOR_FOLDER_SELECTION # State to wait for folder choice via CB

async def client_show_folder_edit_options(update: Update, context: CallbackContext) -> int:
    query = update.callback_query; await query.answer()
    user_id, lang = get_user_id_and_lang(update, context); folder_id = context.user_data.get(CTX_FOLDER_ID)
    
    if not folder_id and query and query.data and '?id=' in query.data:
         try: folder_id = int(query.data.split('?id=')[1]); context.user_data[CTX_FOLDER_ID] = folder_id
         except (ValueError, IndexError): folder_id = None
    
    if not folder_id:
        log.error(f"Could not determine folder ID for edit options. User: {user_id}, Callback: {query.data if query else 'N/A'}")
        # query already answered
        return await client_folder_menu(update, context) # Go back to main folder menu
        
    folder_name = db.get_folder_name(folder_id)
    if not folder_name:
        # query already answered
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'folder_not_found_error', lang_override=lang))
        clear_conversation_data(context); return await client_folder_menu(update, context)

    context.user_data[CTX_FOLDER_NAME] = folder_name; groups_in_folder = db.get_target_groups_details_by_folder(folder_id)
    text = get_text(user_id, 'folder_edit_title', lang_override=lang, name=html.escape(folder_name)) + "\n" + get_text(user_id, 'folder_edit_groups_intro', lang_override=lang)
    if groups_in_folder:
        display_limit = 10
        for i, group in enumerate(groups_in_folder):
            if i >= display_limit: text += f"\n... and {len(groups_in_folder) - display_limit} more."; break
            link = group['group_link']; name = group['group_name'] or f"ID: {group['group_id']}"; escaped_name = html.escape(name)
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
    await _internal_send_or_edit_message(update, context, text, reply_markup=markup, disable_web_page_preview=True)
    return STATE_WAITING_FOR_FOLDER_ACTION # Waiting for CB related to folder editing

async def process_folder_links(update: Update, context: CallbackContext) -> int:
    user_id, lang = get_user_id_and_lang(update, context); folder_id = context.user_data.get(CTX_FOLDER_ID); folder_name = context.user_data.get(CTX_FOLDER_NAME)
    if not folder_id or not folder_name: 
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'session_expired', lang_override=lang))
        clear_conversation_data(context); return ConversationHandler.END
    
    links_text = update.message.text; raw_links = [link.strip() for link in links_text.splitlines() if link.strip()]
    if not raw_links: 
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'join_no_links', lang_override=lang)) # Re-use join_no_links
        return STATE_WAITING_FOR_GROUP_LINKS # Re-ask for links
    
    await _internal_send_or_edit_message(update, context, get_text(user_id, 'folder_processing_links', lang_override=lang))
    
    results = {}; added_count = 0; failed_count = 0; ignored_count = 0
    client_bots = db.get_client_bots(user_id); resolver_bot_phone = None
    if client_bots: 
        active_client_bots = [b_phone for b_phone in client_bots if (bot_info := db.find_userbot(b_phone)) and bot_info['status'] == 'active']
        if active_client_bots: resolver_bot_phone = random.choice(active_client_bots)
    
    log.info(f"User {user_id} adding links to folder '{folder_name}'. Using bot {resolver_bot_phone or 'None'} for resolution.")
    
    link_details = {};
    if resolver_bot_phone:
        try: 
            resolved_data = await telethon_api.resolve_links_info(resolver_bot_phone, raw_links) # This is async
            link_details.update(resolved_data)
            log.debug(f"Resolved {len(link_details)}/{len(raw_links)} links via bot {resolver_bot_phone}.")
        except Exception as resolve_e: 
            log.error(f"Error resolving folder links via bot {resolver_bot_phone}: {resolve_e}")
            # Proceed without resolved info, or inform user of partial failure

    for link in raw_links:
        group_id = None; group_name_resolved = None; reason = None; status_code = 'failed'
        resolved = link_details.get(link)
        
        if resolved and not resolved.get('error'):
            group_id = resolved.get('id'); group_name_resolved = resolved.get('name')
            if group_id:
                 # add_target_group is synchronous
                 added_status = db.add_target_group(group_id, group_name_resolved, link, user_id, folder_id)
                 if added_status is True: status_code = 'added'; added_count += 1
                 elif added_status is None: status_code = 'ignored'; ignored_count += 1; reason = 'Duplicate in folder or unresolvable' # Generic ignored
                 else: status_code = 'failed'; reason = get_text(user_id, 'folder_add_db_error', lang_override=lang); failed_count += 1
            else: # No ID from resolver
                 status_code = 'failed'; reason = get_text(user_id, 'folder_resolve_error', lang_override=lang) + " (No ID from resolver)"
                 failed_count += 1
        elif resolved and resolved.get('error'): # Resolver returned an error
            status_code = 'failed'; reason = resolved.get('error')
            failed_count += 1
        else: # Link not in resolved_data or no resolver bot
            status_code = 'failed'; reason = get_text(user_id, 'folder_resolve_error', lang_override=lang) + " (No resolver or not in results)"
            failed_count += 1
        results[link] = {'status': status_code, 'reason': reason}

    result_text = get_text(user_id, 'folder_results_title', lang_override=lang, name=html.escape(folder_name)) + f"\n(Added: {added_count}, Ignored: {ignored_count}, Failed: {failed_count})\n"
    display_limit = 20; displayed_count = 0
    for link, res in results.items():
        if displayed_count >= display_limit: 
            result_text += f"\n...and {len(results) - displayed_count} more."
            break
        status_key = f"folder_results_{res['status']}"; 
        status_text_template = get_text(user_id, status_key, lang_override=lang) # Get template
        
        # Format status text based on reason
        if res['status'] != 'added' and res['reason']:
            # If the template already takes a reason, use it. Otherwise, append.
            if "{reason}" in status_text_template:
                status_text = status_text_template.format(reason=html.escape(str(res['reason'])))
            else:
                status_text = status_text_template + f" ({html.escape(str(res['reason']))})"
        else:
            status_text = status_text_template # Use as is if no reason or added

        result_text += "\n" + get_text(user_id, 'folder_results_line', lang_override=lang, link=html.escape(link), status=status_text)
        displayed_count += 1
        
    await _internal_send_or_edit_message(update, context, result_text, disable_web_page_preview=True)
    # After processing, go back to the folder edit options menu
    # This requires client_show_folder_edit_options to handle being called from a message handler context
    # For simplicity, we'll assume the callback handler for folder editing will be re-triggered by user.
    # Or, we can directly call it:
    context.user_data.pop(CTX_TARGET_GROUP_IDS_TO_REMOVE, None) # Clear any selection from remove flow
    return await client_show_folder_edit_options(update, context) # Return to edit options

async def client_select_groups_to_remove(update: Update, context: CallbackContext) -> int:
    query = update.callback_query; await query.answer()
    user_id, lang = get_user_id_and_lang(update, context); folder_id = context.user_data.get(CTX_FOLDER_ID); folder_name = context.user_data.get(CTX_FOLDER_NAME)
    if not folder_id or not folder_name: 
        # await query.answer(get_text(user_id, 'session_expired', lang_override=lang), show_alert=True) # Answered
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'session_expired', lang_override=lang))
        return await client_folder_menu(update, context)
    
    try:
        current_page = 0
        if query and query.data and '?page=' in query.data: # Check for page param
            current_page = int(query.data.split('?page=')[1])
    except (ValueError, IndexError, AttributeError): current_page = 0
    
    groups = db.get_target_groups_details_by_folder(folder_id) # Synchronous
    if not groups: 
        # await query.answer(get_text(user_id, 'folder_edit_no_groups', lang_override=lang), show_alert=True) # Answered
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'folder_edit_no_groups', lang_override=lang))
        return await client_show_folder_edit_options(update, context) # Back to edit options
        
    selected_ids = set(context.user_data.get(CTX_TARGET_GROUP_IDS_TO_REMOVE, [])); 
    total_items = len(groups); start_index = current_page * ITEMS_PER_PAGE
    end_index = start_index + ITEMS_PER_PAGE; groups_page = groups[start_index:end_index]
    
    text = get_text(user_id, 'folder_edit_remove_select', lang_override=lang, name=html.escape(folder_name)); keyboard = []
    for group in groups_page:
        db_id = group['id']; is_selected = db_id in selected_ids; prefix = "✅ " if is_selected else "➖ "
        link_text = group['group_link'] or f"ID: {group['group_id']}"
        display_text = group['group_name'] or link_text; max_len = 40
        truncated_text = display_text[:max_len] + ("..." if len(display_text) > max_len else "")
        button_text = prefix + html.escape(truncated_text)
        callback_data = f"{CALLBACK_FOLDER_PREFIX}edit_toggle_remove?id={db_id}&page={current_page}"
        keyboard.append([InlineKeyboardButton(button_text, callback_data=callback_data)])
        
    base_callback = f"{CALLBACK_FOLDER_PREFIX}edit_remove_select"; # For pagination
    pagination_buttons = build_pagination_buttons(base_callback, current_page, total_items, ITEMS_PER_PAGE, lang=lang); keyboard.extend(pagination_buttons)
    
    action_row = [];
    if selected_ids: 
        confirm_text = get_text(user_id, 'folder_edit_remove_confirm_title', lang_override=lang) + f" ({len(selected_ids)})"
        action_row.append(InlineKeyboardButton(confirm_text, callback_data=f"{CALLBACK_FOLDER_PREFIX}edit_remove_confirm"))
    action_row.append(InlineKeyboardButton(get_text(user_id, 'button_back', lang_override=lang), callback_data=f"{CALLBACK_FOLDER_PREFIX}back_to_edit_options"))
    keyboard.append(action_row)
    
    markup = InlineKeyboardMarkup(keyboard)
    await _internal_send_or_edit_message(update, context, text, reply_markup=markup)
    return STATE_FOLDER_EDIT_REMOVE_SELECT # Stay in this state for toggling/confirming

async def client_toggle_group_for_removal(update: Update, context: CallbackContext) -> int:
    query = update.callback_query; await query.answer() # Answer first
    user_id, lang = get_user_id_and_lang(update, context)
    try:
        params_str = query.data.split('?', 1)[1]; params = dict(qc.split('=') for qc in params_str.split('&'))
        group_db_id = int(params['id']); current_page = int(params['page'])
    except (ValueError, IndexError, KeyError): 
        log.error(f"Could not parse group ID/page from callback: {query.data}")
        # query answered
        await _internal_send_or_edit_message(update,context, get_text(user_id, 'error_generic', lang_override=lang))
        return STATE_FOLDER_EDIT_REMOVE_SELECT # Or end
        
    if CTX_TARGET_GROUP_IDS_TO_REMOVE not in context.user_data: 
        context.user_data[CTX_TARGET_GROUP_IDS_TO_REMOVE] = set()
        
    if group_db_id in context.user_data[CTX_TARGET_GROUP_IDS_TO_REMOVE]: 
        context.user_data[CTX_TARGET_GROUP_IDS_TO_REMOVE].remove(group_db_id)
    else: 
        context.user_data[CTX_TARGET_GROUP_IDS_TO_REMOVE].add(group_db_id)
    
    # Re-render the selection menu
    # To do this, we need to pass the current page back to client_select_groups_to_remove
    # We can modify query.data temporarily or pass page via context.
    # For simplicity, let client_select_groups_to_remove re-parse page from query.data if it exists.
    # The callback_data used to call this toggle handler *already* has the page.
    # So, when we call client_select_groups_to_remove again, it will use that page.
    return await client_select_groups_to_remove(update, context)

async def client_confirm_remove_selected_groups(update: Update, context: CallbackContext) -> int:
    query = update.callback_query; await query.answer()
    user_id, lang = get_user_id_and_lang(update, context); folder_id = context.user_data.get(CTX_FOLDER_ID); folder_name = context.user_data.get(CTX_FOLDER_NAME); ids_to_remove = list(context.user_data.get(CTX_TARGET_GROUP_IDS_TO_REMOVE, []))
    
    if not folder_id or not folder_name: 
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'session_expired', lang_override=lang))
        return await client_folder_menu(update, context)
        
    if not ids_to_remove: 
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'folder_edit_remove_none_selected', lang_override=lang))
        # query.data needs to be set for client_select_groups_to_remove to get page 0
        # Or handle it inside client_select_groups_to_remove if query.data is not for pagination
        # For now, just return to edit options
        return await client_show_folder_edit_options(update, context)

    removed_count = db.remove_target_groups_by_db_id(ids_to_remove, user_id) # Synchronous
    
    if removed_count >= 0: 
        db.log_event_db("Folder Groups Removed", f"Folder: {folder_name}({folder_id}), Count: {removed_count}, IDs: {ids_to_remove}", user_id=user_id)
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'folder_edit_remove_success', lang_override=lang, count=removed_count, name=html.escape(folder_name)))
    else: 
        db.log_event_db("Folder Group Remove Failed", f"Folder: {folder_name}({folder_id}), DB Error", user_id=user_id)
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'folder_edit_remove_error', lang_override=lang))
        
    context.user_data.pop(CTX_TARGET_GROUP_IDS_TO_REMOVE, None)
    return await client_show_folder_edit_options(update, context) # Back to edit options menu

async def process_folder_rename(update: Update, context: CallbackContext) -> int:
    user_id, lang = get_user_id_and_lang(update, context); new_name = update.message.text.strip(); folder_id = context.user_data.get(CTX_FOLDER_ID); current_name = context.user_data.get(CTX_FOLDER_NAME)
    if not folder_id or not current_name: 
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'session_expired', lang_override=lang))
        clear_conversation_data(context); return ConversationHandler.END
    
    if not new_name: 
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'error_invalid_input', lang_override=lang))
        return STATE_FOLDER_RENAME_PROMPT # Re-ask
        
    if new_name == current_name: # No change
        return await client_show_folder_edit_options(update, context) 
        
    success, reason = db.rename_folder(folder_id, user_id, new_name) # Synchronous
    
    if success: 
        db.log_event_db("Folder Renamed", f"ID: {folder_id}, From: {current_name}, To: {new_name}", user_id=user_id)
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'folder_edit_rename_success', lang_override=lang, new_name=html.escape(new_name)))
        context.user_data[CTX_FOLDER_NAME] = new_name # Update context
        return await client_show_folder_edit_options(update, context)
    else:
        if reason == "name_exists": 
            await _internal_send_or_edit_message(update, context, get_text(user_id, 'folder_edit_rename_error_exists', lang_override=lang, new_name=html.escape(new_name)))
            return STATE_FOLDER_RENAME_PROMPT # Re-ask
        else: # db_error or not_found_or_unauthorized
            db.log_event_db("Folder Rename Failed", f"ID: {folder_id}, To: {new_name}, Reason: {reason}", user_id=user_id)
            await _internal_send_or_edit_message(update, context, get_text(user_id, 'folder_edit_rename_error_db', lang_override=lang)) # Generic DB error message
            return await client_show_folder_edit_options(update, context) # Back to edit options

async def client_confirm_folder_delete_prompt(update: Update, context: CallbackContext) -> int: # Renamed for clarity
    query = update.callback_query; await query.answer()
    user_id, lang = get_user_id_and_lang(update, context)
    try: 
        folder_id = int(query.data.split('?id=')[1])
    except (ValueError, IndexError, AttributeError): 
        log.error(f"Could not parse folder ID for delete confirm: {query.data}")
        await _internal_send_or_edit_message(update,context, get_text(user_id, 'error_generic', lang_override=lang))
        return await client_folder_menu(update, context)
        
    folder_name = db.get_folder_name(folder_id) # Sync
    if not folder_name: 
        await _internal_send_or_edit_message(update,context, get_text(user_id, 'folder_not_found_error', lang_override=lang))
        return await client_folder_menu(update, context)
        
    text = get_text(user_id, 'folder_delete_confirm', lang_override=lang, name=html.escape(folder_name))
    keyboard = [[ 
        InlineKeyboardButton(get_text(user_id, 'button_yes', lang_override=lang), callback_data=f"{CALLBACK_FOLDER_PREFIX}delete_confirmed_execute?id={folder_id}"), 
        InlineKeyboardButton(get_text(user_id, 'button_no', lang_override=lang), callback_data=f"{CALLBACK_FOLDER_PREFIX}back_to_manage") 
    ]]
    markup = InlineKeyboardMarkup(keyboard)
    await _internal_send_or_edit_message(update, context, text, reply_markup=markup)
    return STATE_WAITING_FOR_FOLDER_SELECTION # Waiting for Yes/No callback

async def client_delete_folder_confirmed_execute(update: Update, context: CallbackContext) -> int: # Renamed
    query = update.callback_query; await query.answer()
    user_id, lang = get_user_id_and_lang(update, context)
    try: 
        folder_id = int(query.data.split('?id=')[1])
    except (ValueError, IndexError, AttributeError): 
        log.error(f"Could not parse folder ID for delete confirmed execute: {query.data}")
        await _internal_send_or_edit_message(update,context, get_text(user_id, 'error_generic', lang_override=lang))
        return await client_folder_menu(update, context)
        
    folder_name_before_delete = db.get_folder_name(folder_id) # Get name for logging before delete
    
    if db.delete_folder(folder_id, user_id): # Sync
        log.info(f"User {user_id} deleted folder ID {folder_id} (Name: {folder_name_before_delete or 'N/A'})")
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'folder_delete_success', lang_override=lang, name=html.escape(folder_name_before_delete or 'Unknown')))
    else: 
        log.warning(f"Failed delete folder ID {folder_id} by user {user_id}")
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'folder_delete_error', lang_override=lang))
        
    return await client_folder_menu(update, context) # Back to main folder menu

async def client_select_bot_generic(update: Update, context: CallbackContext, action_prefix: str, next_state_on_message: str, title_key: str) -> int | None:
    # This function displays a bot selection menu. Actual selection is handled by a callback.
    query = update.callback_query
    if query: await query.answer() # Answer if called from CB

    user_id, lang = get_user_id_and_lang(update, context)
    # clear_conversation_data(context) # Don't clear if just displaying a menu within a flow
    log.info(f"client_select_bot_generic: User {user_id}, ActionPrefix {action_prefix}, NextState (for msg handler) {next_state_on_message}, TitleKey {title_key}")
    
    available_bots = db.get_client_bots(user_id) # Sync
    
    # Async update bot statuses (fire and forget, or await if critical for display)
    # if available_bots:
    #     for phone in available_bots:
    #         context.dispatcher.run_async(telethon_api.get_userbot_runtime_info, phone) 
    
    keyboard = []
    active_bots_for_selection = []
    for phone in available_bots:
        bot_info = db.find_userbot(phone) # Sync
        if not bot_info or bot_info['status'] != 'active':
            continue # Only show active bots for selection
        active_bots_for_selection.append(bot_info)
        
        username = bot_info.get('username', phone)
        display_name = f"@{username}" if username else phone
        keyboard.append([InlineKeyboardButton(html.escape(display_name), callback_data=f"{action_prefix}select_{phone}")])
    
    if not active_bots_for_selection:
        no_active_bots_key = 'join_no_active_bots' if action_prefix == CALLBACK_JOIN_PREFIX else 'task_error_no_active_bots' # Need a task specific key
        if 'task_error_no_active_bots' not in translations.get(lang, {}): no_active_bots_key = 'join_no_active_bots' # Fallback
        await _internal_send_or_edit_message(update, context, get_text(user_id, no_active_bots_key, lang_override=lang))
        return ConversationHandler.END # Or return to client_menu


    # Add "All Active" option for join, but not for task setup (task is per-bot)
    if action_prefix == CALLBACK_JOIN_PREFIX and len(active_bots_for_selection) > 1 :
         keyboard.insert(0, [InlineKeyboardButton(get_text(user_id, 'join_select_userbot_active', lang_override=lang, count=len(active_bots_for_selection)), callback_data=f"{action_prefix}select_active")])


    keyboard.append([InlineKeyboardButton(get_text(user_id, 'button_back', lang_override=lang), callback_data=f"{CALLBACK_CLIENT_PREFIX}back_to_menu")])
    markup = InlineKeyboardMarkup(keyboard)
    
    await _internal_send_or_edit_message(
        update, context,
        get_text(user_id, title_key, lang_override=lang),
        reply_markup=markup
    )
    # This function sets up a menu. The next state is determined by the *callback handler*
    # or if it expects direct message input (next_state_on_message).
    # If selection is via callback, this function itself doesn't transition.
    # If we expect a message after this (e.g. if no buttons were clicked), then next_state_on_message is used.
    # For bot selection, it's always a callback. So this should ideally return None or END.
    # The ConversationHandler's states dict will define what happens on CB.
    return STATE_WAITING_FOR_USERBOT_SELECTION # State waiting for the callback from this menu

async def handle_userbot_selection_callback(update: Update, context: CallbackContext, action_prefix: str) -> int | None:
    query = update.callback_query; await query.answer()
    user_id, lang = get_user_id_and_lang(update, context); data = query.data
    
    selected_option_part = data.split(f"{action_prefix}select_")[1] # e.g., "active" or a phone number
    
    selected_bots_phones = []
    if selected_option_part == 'active': # For JOIN action
        all_client_bots = db.get_client_bots(user_id)
        selected_bots_phones = [p for p in all_client_bots if (b := db.find_userbot(p)) and b['status'] == 'active']
        if not selected_bots_phones: 
            await _internal_send_or_edit_message(update, context, get_text(user_id, 'join_no_active_bots', lang_override=lang))
            return await client_select_bot_generic(update, context, CALLBACK_JOIN_PREFIX, STATE_WAITING_FOR_GROUP_LINKS, 'join_select_userbot')
    else: # Specific phone number selected
        phone = selected_option_part
        bot_info = db.find_userbot(phone)
        client_owns_bot = phone in db.get_client_bots(user_id)
        if not bot_info or not client_owns_bot or bot_info['status'] != 'active': 
            log.warning(f"User {user_id} selected unauthorized/invalid/inactive bot: {phone} for prefix {action_prefix}")
            await _internal_send_or_edit_message(update, context, get_text(user_id, 'error_invalid_input', lang_override=lang)) # Or more specific error
            # Reshow selection menu
            title_key = 'join_select_userbot' if action_prefix == CALLBACK_JOIN_PREFIX else 'task_select_userbot'
            next_state_for_msg = STATE_WAITING_FOR_GROUP_LINKS if action_prefix == CALLBACK_JOIN_PREFIX else STATE_TASK_SETUP
            return await client_select_bot_generic(update, context, action_prefix, next_state_for_msg, title_key)
        selected_bots_phones = [phone]

    context.user_data[CTX_SELECTED_BOTS] = selected_bots_phones
    log.info(f"User {user_id} selected bot(s): {selected_bots_phones} for action prefix {action_prefix}")

    if action_prefix == CALLBACK_JOIN_PREFIX: 
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'join_enter_group_links', lang_override=lang))
        return STATE_WAITING_FOR_GROUP_LINKS # Expect links message
    elif action_prefix == CALLBACK_TASK_PREFIX: 
        context.user_data[CTX_TASK_PHONE] = selected_bots_phones[0] # Task setup is per-bot
        return await task_show_settings_menu(update, context) # Go to task settings
    else: 
        log.error(f"Unhandled action prefix in handle_userbot_selection_callback: {action_prefix}")
        clear_conversation_data(context); return ConversationHandler.END

async def process_join_group_links(update: Update, context: CallbackContext) -> int:
    user_id, lang = get_user_id_and_lang(update, context); selected_bots = context.user_data.get(CTX_SELECTED_BOTS)
    if not selected_bots: 
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'session_expired', lang_override=lang))
        clear_conversation_data(context); return ConversationHandler.END
        
    links_text = update.message.text; raw_links = [link.strip() for link in links_text.splitlines() if link.strip()]
    if not raw_links: 
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'join_no_links', lang_override=lang))
        return STATE_WAITING_FOR_GROUP_LINKS # Re-ask for links
        
    await _internal_send_or_edit_message(update, context, get_text(user_id, 'join_processing', lang_override=lang))
    
    all_results_text = get_text(user_id, 'join_results_title', lang_override=lang); 
    
    # Create asyncio tasks for each bot's batch join
    join_tasks = []
    for phone in selected_bots: 
        join_tasks.append(telethon_api.join_groups_batch(phone, raw_links)) # This is async
    
    # Gather results
    results_list_from_gather = await asyncio.gather(*join_tasks, return_exceptions=True)
    
    for i, result_item in enumerate(results_list_from_gather):
        phone = selected_bots[i]; bot_db_info = db.find_userbot(phone)
        bot_display_name = html.escape(f"@{bot_db_info['username']}" if bot_db_info and bot_db_info['username'] else phone)
        all_results_text += "\n" + get_text(user_id, 'join_results_bot_header', lang_override=lang, display_name=bot_display_name)
        
        if isinstance(result_item, Exception): 
            log.error(f"Join batch task for {phone} raised exception: {result_item}", exc_info=True)
            all_results_text += f"\n  -> {get_text(user_id, 'error_generic', lang_override=lang)} ({html.escape(str(result_item))})"
            continue
            
        # result_item is (error_info, results_dict) from join_groups_batch
        error_info, results_dict_for_bot = result_item
        
        if error_info and error_info.get("error"): 
            error_message_detail = error_info['error']
            log.error(f"Join batch error for {phone}: {error_message_detail}")
            generic_error_text = get_text(user_id, 'error_generic', lang_override=lang)
            all_results_text += f"\n  -> {generic_error_text} ({html.escape(error_message_detail)})"
            continue
            
        if not results_dict_for_bot: 
            all_results_text += f"\n  -> ({get_text(user_id, 'error_no_results', lang_override=lang)})"
            continue
            
        processed_links_count = 0
        for link, (status_code, detail_dict_or_str) in results_dict_for_bot.items():
             status_key_from_join = f"join_results_{status_code}"
             status_text = get_text(user_id, status_key_from_join, lang_override=lang) # Initial status text

             # Handle detailed reasons for non-success cases
             if status_code not in ['success', 'already_member'] and isinstance(detail_dict_or_str, dict):
                  reason_code = detail_dict_or_str.get('reason')
                  error_detail = detail_dict_or_str.get('error')
                  seconds_detail = detail_dict_or_str.get('seconds')
                  
                  reason_text_parts = []
                  if reason_code:
                      reason_key_from_join = f"join_results_reason_{reason_code}"
                      reason_base_text = get_text(user_id, reason_key_from_join, lang_override=lang)
                      if reason_base_text != reason_key_from_join: # Key exists
                          try:
                              reason_text_parts.append(reason_base_text.format(error=html.escape(str(error_detail or '')), seconds=seconds_detail or ''))
                          except KeyError: # Template might not use all params
                              reason_text_parts.append(reason_base_text)
                      else: # Key doesn't exist, use reason_code itself
                          reason_text_parts.append(html.escape(str(reason_code)))
                  
                  # Append raw error if not already part of formatted reason and reason_code was generic
                  if error_detail and (not reason_code or reason_code in ['internal_error', 'batch_error']):
                      if not any(str(error_detail) in part for part in reason_text_parts):
                           reason_text_parts.append(f"({html.escape(str(error_detail))})")
                  
                  if reason_text_parts:
                      # Re-format the status_text if it's a "failed" type to include reason
                      if status_key_from_join == 'join_results_failed':
                           status_text = get_text(user_id, 'join_results_failed', lang_override=lang, reason=", ".join(reason_text_parts))
                      else: # For other statuses like flood_wait, append if not already specific
                           status_text += " (" + ", ".join(reason_text_parts) + ")"

             elif status_code == 'flood_wait' and isinstance(detail_dict_or_str, dict) and detail_dict_or_str.get('seconds'):
                  status_text = get_text(user_id, 'join_results_flood_wait', lang_override=lang, seconds=detail_dict_or_str.get('seconds'))


             escaped_link = html.escape(link)
             all_results_text += "\n" + get_text(user_id, 'join_results_line', lang_override=lang, url=escaped_link, status=status_text)
             processed_links_count +=1
             if len(all_results_text) > 3800: 
                 all_results_text += f"\n\n... (message truncated, {len(raw_links) - processed_links_count} links remaining for this bot)"
                 break
                 
    keyboard = [[InlineKeyboardButton(get_text(user_id, 'button_main_menu', lang_override=lang), callback_data=f"{CALLBACK_CLIENT_PREFIX}back_to_menu")]]; markup = InlineKeyboardMarkup(keyboard)
    
    # Handle potentially very long messages
    if len(all_results_text) > 4096:
        log.warning(f"Join results message too long ({len(all_results_text)} chars). Splitting.")
        parts = []; current_part = ""
        for line in all_results_text.splitlines(keepends=True):
            if len(current_part) + len(line) > 4000: # Split conservatively
                parts.append(current_part); current_part = line
            else: current_part += line
        if current_part: parts.append(current_part)
        
        for i, part_text in enumerate(parts):
            part_markup = markup if i == len(parts) - 1 else None
            try:
                await context.bot.send_message(user_id, part_text, parse_mode=ParseMode.HTML, reply_markup=part_markup, disable_web_page_preview=True)
                if i < len(parts) - 1: await asyncio.sleep(0.5) # Brief pause between parts
            except Exception as send_e:
                log.error(f"Error sending split join results part {i+1}: {send_e}")
                await context.bot.send_message(user_id, get_text(user_id, 'error_generic', lang_override=lang)) # Inform user of send failure
                break 
    else: 
        await _internal_send_or_edit_message(update, context, all_results_text, reply_markup=markup, disable_web_page_preview=True)
        
    clear_conversation_data(context); return ConversationHandler.END

async def client_show_stats(update: Update, context: CallbackContext) -> int:
    query = update.callback_query; await query.answer()
    user_id, lang = get_user_id_and_lang(update, context); stats = db.get_client_stats(user_id) # Sync
    if not stats: 
        text = get_text(user_id, 'client_stats_no_data', lang_override=lang)
    else: 
        text = f"<b>{get_text(user_id, 'client_stats_title', lang_override=lang)}</b>\n\n"
        text += get_text(user_id, 'client_stats_messages', lang_override=lang, total_sent=stats.get('total_messages_sent', 0)) + "\n"
        text += get_text(user_id, 'client_stats_forwards', lang_override=lang, forwards_count=stats.get('forwards_count', 0)) + "\n"

    keyboard = [[InlineKeyboardButton(get_text(user_id, 'button_back', lang_override=lang), callback_data=f"{CALLBACK_CLIENT_PREFIX}back_to_menu")]]; markup = InlineKeyboardMarkup(keyboard)
    await _internal_send_or_edit_message(update, context, text, reply_markup=markup, parse_mode=ParseMode.HTML)
    return ConversationHandler.END # Stats display is an endpoint

async def task_show_settings_menu(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    if query: await query.answer() # Answer if called from CB

    user_id, lang = get_user_id_and_lang(update, context); phone = context.user_data.get(CTX_TASK_PHONE)
    
    if not phone: # Attempt to recover phone if called directly after selection
        if query and query.data and f"{CALLBACK_TASK_PREFIX}select_" in query.data:
             try: phone = query.data.split(f"{CALLBACK_TASK_PREFIX}select_")[1]; context.user_data[CTX_TASK_PHONE] = phone
             except IndexError: phone = None
        if not phone: # Still no phone
             log.error(f"Task setup called without phone for user {user_id}. CB Data: {query.data if query else 'N/A'}")
             await _internal_send_or_edit_message(update, context, get_text(user_id, 'session_expired', lang_override=lang))
             return await client_menu(update, context) # Back to main client menu
             
    bot_db_info = db.find_userbot(phone); 
    display_name = html.escape(f"@{bot_db_info['username']}" if bot_db_info and bot_db_info['username'] else phone)
    
    # Load or initialize task settings in context.user_data
    if CTX_TASK_SETTINGS not in context.user_data or context.user_data.get(CTX_TASK_SETTINGS, {}).get('_phone_marker_') != phone :
        task_settings_db = db.get_userbot_task_settings(user_id, phone) # Sync
        if task_settings_db:
            context.user_data[CTX_TASK_SETTINGS] = dict(task_settings_db)
        else: # No settings in DB, initialize empty
            context.user_data[CTX_TASK_SETTINGS] = {}
        context.user_data[CTX_TASK_SETTINGS]['_phone_marker_'] = phone # Mark for which bot these settings are

    current_settings = context.user_data.get(CTX_TASK_SETTINGS, {})
    
    status = current_settings.get('status', 'inactive')
    status_icon_key = f'task_status_icon_{status}'; 
    status_icon = get_text(user_id, status_icon_key, lang_override=lang) if status_icon_key in translations.get(lang,{}) else ("🟢" if status == 'active' else "⚪️")
    status_text = get_text(user_id, f'task_status_{status}', lang_override=lang)
    
    primary_link_raw = current_settings.get('message_link')
    primary_link = html.escape(primary_link_raw) if primary_link_raw else get_text(user_id, 'task_value_not_set', lang_override=lang)
    fallback_link_raw = current_settings.get('fallback_message_link')
    fallback_link = html.escape(fallback_link_raw) if fallback_link_raw else get_text(user_id, 'task_value_not_set', lang_override=lang)
    
    start_time_ts = current_settings.get('start_time'); start_time_str = format_dt(start_time_ts, fmt='%H:%M') # Uses local TZ by default
    
    interval_min = current_settings.get('repetition_interval'); interval_str = get_text(user_id, 'task_value_not_set', lang_override=lang)
    if interval_min:
         if interval_min < 60: interval_disp = f"{interval_min} min"
         elif interval_min % (60*24) == 0: interval_disp = f"{interval_min // (60*24)} d" # Days
         elif interval_min % 60 == 0: interval_disp = f"{interval_min // 60} h" # Hours
         else: interval_disp = f"{interval_min // 60} h {interval_min % 60} min" # Hours and minutes
         interval_str = get_text(user_id, 'task_interval_button', lang_override=lang, value=interval_disp) # Re-use button text format
         
    target_str = get_text(user_id, 'task_value_not_set', lang_override=lang)
    if current_settings.get('send_to_all_groups'): 
        target_str = get_text(user_id, 'task_value_all_groups', lang_override=lang)
    elif current_settings.get('folder_id'):
        folder_id_val = current_settings['folder_id']; folder_name = db.get_folder_name(folder_id_val)
        if folder_name: target_str = get_text(user_id, 'task_value_folder', lang_override=lang, name=html.escape(folder_name))
        else: target_str = get_text(user_id, 'task_value_folder', lang_override=lang, name=f"ID: {folder_id_val}") + " (Deleted?)"
        
    last_run_str = format_dt(current_settings.get('last_run')); # UTC display
    last_error_raw = current_settings.get('last_error')
    last_error = html.escape(last_error_raw[:100]) + ('...' if last_error_raw and len(last_error_raw) > 100 else '') if last_error_raw else get_text(user_id, 'task_value_not_set', lang_override=lang)
    
    text = f"<b>{get_text(user_id, 'task_setup_title', lang_override=lang, display_name=display_name)}</b>\n\n"
    text += f"{get_text(user_id, 'task_setup_status_line', lang_override=lang, status_icon=status_icon, status_text=status_text)}\n"
    text += f"{get_text(user_id, 'task_setup_primary_msg', lang_override=lang, link=primary_link)}\n"
    # text += f"{get_text(user_id, 'task_setup_fallback_msg', lang_override=lang, link=fallback_link)}\n" # Fallback less common, maybe hide if not set
    text += f"{get_text(user_id, 'task_setup_start_time', lang_override=lang, time=start_time_str)}\n"
    text += f"{get_text(user_id, 'task_setup_interval', lang_override=lang, interval=interval_str)}\n"
    text += f"{get_text(user_id, 'task_setup_target', lang_override=lang, target=target_str)}\n\n"
    text += f"{get_text(user_id, 'task_setup_last_run', lang_override=lang, time=last_run_str)}\n"
    text += f"{get_text(user_id, 'task_setup_last_error', lang_override=lang, error=last_error)}\n"
    
    keyboard = [ 
        [InlineKeyboardButton(get_text(user_id, 'task_button_set_message', lang_override=lang), callback_data=f"{CALLBACK_TASK_PREFIX}set_primary_link")], 
        [InlineKeyboardButton(get_text(user_id, 'task_button_set_time', lang_override=lang), callback_data=f"{CALLBACK_TASK_PREFIX}set_time"), 
         InlineKeyboardButton(get_text(user_id, 'task_button_set_interval', lang_override=lang), callback_data=f"{CALLBACK_TASK_PREFIX}set_interval")], 
        [InlineKeyboardButton(get_text(user_id, 'task_button_set_target', lang_override=lang), callback_data=f"{CALLBACK_TASK_PREFIX}set_target_type")], 
        [InlineKeyboardButton(get_text(user_id, 'task_button_deactivate' if status == 'active' else 'task_button_activate', lang_override=lang), callback_data=f"{CALLBACK_TASK_PREFIX}toggle_status"), 
         InlineKeyboardButton(get_text(user_id, 'task_button_save', lang_override=lang), callback_data=f"{CALLBACK_TASK_PREFIX}save")], 
        [InlineKeyboardButton(get_text(user_id, 'button_back', lang_override=lang), callback_data=f"{CALLBACK_TASK_PREFIX}back_to_bot_select")] 
    ]
    markup = InlineKeyboardMarkup(keyboard)
    await _internal_send_or_edit_message(update, context, text, reply_markup=markup, disable_web_page_preview=True)
    return STATE_TASK_SETUP # Main state for task configuration, actions via CB

async def task_prompt_set_link(update: Update, context: CallbackContext, link_type: str) -> int:
    query = update.callback_query; await query.answer()
    user_id, lang = get_user_id_and_lang(update, context)
    prompt_key = 'task_prompt_primary_link' if link_type == 'primary' else 'task_prompt_fallback_link'
    next_state = STATE_WAITING_FOR_PRIMARY_MESSAGE_LINK if link_type == 'primary' else STATE_WAITING_FOR_FALLBACK_MESSAGE_LINK
    text = get_text(user_id, prompt_key, lang_override=lang)
    keyboard = [[InlineKeyboardButton(get_text(user_id, 'button_back', lang_override=lang), callback_data=f"{CALLBACK_TASK_PREFIX}back_to_task_menu")]]
    markup = InlineKeyboardMarkup(keyboard)
    await _internal_send_or_edit_message(update, context, text, reply_markup=markup)
    return next_state # Wait for message with link

async def process_task_link(update: Update, context: CallbackContext, link_type: str) -> int:
    user_id, lang = get_user_id_and_lang(update, context); phone = context.user_data.get(CTX_TASK_PHONE); task_settings = context.user_data.get(CTX_TASK_SETTINGS)
    if not phone or task_settings is None: 
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'session_expired', lang_override=lang))
        clear_conversation_data(context); return ConversationHandler.END
        
    link_text = update.message.text.strip()
    expected_next_state = STATE_WAITING_FOR_PRIMARY_MESSAGE_LINK if link_type == 'primary' else STATE_WAITING_FOR_FALLBACK_MESSAGE_LINK
    
    if link_type == 'fallback' and link_text.lower() == 'skip': 
        task_settings['fallback_message_link'] = None
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'task_set_skipped_fallback', lang_override=lang))
        return await task_show_settings_menu(update, context) # Back to settings menu
        
    link_parsed_type, _ = telethon_api.parse_telegram_url_simple(link_text) # Sync
    if link_parsed_type != "message_link": 
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'task_error_invalid_link', lang_override=lang))
        return expected_next_state # Re-ask for link
        
    await _internal_send_or_edit_message(update, context, get_text(user_id, 'task_verifying_link', lang_override=lang))
    link_verified = False
    try:
        log.info(f"Verifying link access for {link_text} via bot {phone}..."); 
        accessible = await telethon_api.check_message_link_access(phone, link_text) # Async
        if not accessible: 
            log.warning(f"Link {link_text} not accessible by bot {phone}.")
            await _internal_send_or_edit_message(update, context, get_text(user_id, 'task_error_link_unreachable', lang_override=lang, bot_phone=html.escape(phone)))
            return expected_next_state
        else: 
            log.info(f"User {user_id} link {link_text} verified successfully by bot {phone}.")
            link_verified = True
    except Exception as e: 
        log.error(f"Error checking link access {phone} -> {link_text}: {e}", exc_info=True)
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'error_telegram_api', lang_override=lang, error=html.escape(str(e))))
        return expected_next_state # Re-ask or allow user to go back
        
    if link_verified:
        success_msg_key = 'task_set_success_msg' if link_type == 'primary' else 'task_set_success_fallback'
        if link_type == 'primary': task_settings['message_link'] = link_text
        else: task_settings['fallback_message_link'] = link_text
        
        await _internal_send_or_edit_message(update, context, get_text(user_id, success_msg_key, lang_override=lang))
        return await task_show_settings_menu(update, context) # Back to settings menu
    else: # Should have been caught by specific errors above, but as a fallback
        log.error(f"Link verification failed unexpectedly for {link_text}.")
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'error_generic', lang_override=lang))
        return expected_next_state

async def task_prompt_start_time(update: Update, context: CallbackContext) -> int:
     query = update.callback_query; await query.answer()
     user_id, lang = get_user_id_and_lang(update, context)
     local_tz_name = LITHUANIA_TZ.zone if hasattr(LITHUANIA_TZ, 'zone') else str(LITHUANIA_TZ) # Ensure string
     text = get_text(user_id, 'task_prompt_start_time', lang_override=lang, timezone_name=local_tz_name)
     keyboard = [[InlineKeyboardButton(get_text(user_id, 'button_back', lang_override=lang), callback_data=f"{CALLBACK_TASK_PREFIX}back_to_task_menu")]]
     markup = InlineKeyboardMarkup(keyboard)
     await _internal_send_or_edit_message(update, context, text, reply_markup=markup)
     return STATE_WAITING_FOR_START_TIME

async def process_task_start_time(update: Update, context: CallbackContext) -> int:
    user_id, lang = get_user_id_and_lang(update, context); time_str = update.message.text.strip(); task_settings = context.user_data.get(CTX_TASK_SETTINGS)
    if task_settings is None: 
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'session_expired', lang_override=lang))
        clear_conversation_data(context); return ConversationHandler.END
        
    try:
        hour, minute = map(int, time_str.split(':'))
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            raise ValueError("Invalid hour/minute range")
    except (ValueError, TypeError):
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'task_error_invalid_time', lang_override=lang))
        return STATE_WAITING_FOR_START_TIME
        
    try:
        now_local = datetime.now(LITHUANIA_TZ)
        # Create a datetime object for today at the specified HH:MM in local timezone
        input_time_obj = datetime.strptime(f"{hour:02d}:{minute:02d}", "%H:%M").time()
        target_local_dt_naive = datetime.combine(now_local.date(), input_time_obj)
        target_local_dt = LITHUANIA_TZ.localize(target_local_dt_naive) # Make it timezone-aware
        
        # If this time is already past for today, schedule for tomorrow
        if target_local_dt <= now_local: 
            target_local_dt += timedelta(days=1)
            
        target_utc = target_local_dt.astimezone(UTC_TZ); # Convert to UTC
        start_timestamp = int(target_utc.timestamp())
        
        task_settings['start_time'] = start_timestamp
        log.info(f"User {user_id} set task start time: {time_str} LT -> {start_timestamp} UTC ({target_utc.strftime('%Y-%m-%d %H:%M:%S %Z')})")
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'task_set_success_time', lang_override=lang, time=time_str))
        return await task_show_settings_menu(update, context)
    except Exception as e: 
        log.error(f"Error converting start time '{time_str}' for user {user_id}: {e}", exc_info=True)
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'error_generic', lang_override=lang))
        return STATE_WAITING_FOR_START_TIME # Re-ask

async def task_select_interval(update: Update, context: CallbackContext) -> int:
    query = update.callback_query; await query.answer()
    user_id, lang = get_user_id_and_lang(update, context)
    # Intervals in minutes
    intervals = [5, 10, 15, 30, 60, 120, 180, 240, 360, 720, 1440] # 1 day = 1440 min
    keyboard = []; row = []
    for minutes in intervals:
        if minutes < 60: label = f"{minutes} min"
        elif minutes % (60*24) == 0: label = f"{minutes // (60*24)} d"
        elif minutes % 60 == 0: label = f"{minutes // 60} h"
        else: label = f"{minutes // 60} h {minutes % 60} min"
        button_text = get_text(user_id, 'task_interval_button', lang_override=lang, value=label)
        row.append(InlineKeyboardButton(button_text, callback_data=f"{CALLBACK_INTERVAL_PREFIX}{minutes}"))
        if len(row) >= 3: keyboard.append(row); row = []
    if row: keyboard.append(row)
    
    keyboard.append([InlineKeyboardButton(get_text(user_id, 'button_back', lang_override=lang), callback_data=f"{CALLBACK_TASK_PREFIX}back_to_task_menu")])
    markup = InlineKeyboardMarkup(keyboard)
    await _internal_send_or_edit_message(update, context, get_text(user_id, 'task_select_interval_title', lang_override=lang), reply_markup=markup)
    return STATE_TASK_SETUP # Return to main task setup state, selection via CB

async def process_interval_callback(update: Update, context: CallbackContext) -> int:
    query = update.callback_query; await query.answer()
    user_id, lang = get_user_id_and_lang(update, context); task_settings = context.user_data.get(CTX_TASK_SETTINGS)
    if task_settings is None: 
        # await query.answer(get_text(user_id, 'session_expired', lang_override=lang), show_alert=True) # Answered
        await _internal_send_or_edit_message(update,context, get_text(user_id, 'session_expired', lang_override=lang))
        return await client_menu(update, context) # Back to main client menu
        
    try: 
        interval_minutes = int(query.data.split(CALLBACK_INTERVAL_PREFIX)[1])
    except (ValueError, IndexError, AssertionError): 
        log.error(f"Invalid interval callback data: {query.data}")
        # await query.answer(get_text(user_id, 'error_invalid_input', lang_override=lang), show_alert=True) # Answered
        await _internal_send_or_edit_message(update,context, get_text(user_id, 'error_invalid_input', lang_override=lang))
        return STATE_TASK_SETUP # Back to task menu
        
    task_settings['repetition_interval'] = interval_minutes
    log.info(f"User {user_id} set task interval to {interval_minutes} minutes (unsaved).")
    
    # No confirmation message needed here, just update the main task menu display
    return await task_show_settings_menu(update, context)

async def task_select_target_type(update: Update, context: CallbackContext) -> int:
    query = update.callback_query; await query.answer()
    user_id, lang = get_user_id_and_lang(update, context)
    keyboard = [ 
        [InlineKeyboardButton(get_text(user_id, 'task_button_target_folder', lang_override=lang), callback_data=f"{CALLBACK_TASK_PREFIX}select_folder_target?page=0")], 
        [InlineKeyboardButton(get_text(user_id, 'task_button_target_all', lang_override=lang), callback_data=f"{CALLBACK_TASK_PREFIX}set_target_all")], 
        [InlineKeyboardButton(get_text(user_id, 'button_back', lang_override=lang), callback_data=f"{CALLBACK_TASK_PREFIX}back_to_task_menu")] 
    ]
    markup = InlineKeyboardMarkup(keyboard)
    await _internal_send_or_edit_message(update, context, get_text(user_id, 'task_select_target_title', lang_override=lang), reply_markup=markup)
    return STATE_TASK_SETUP # Waiting for CB for target type

async def task_select_folder_for_target(update: Update, context: CallbackContext) -> int:
    query = update.callback_query; await query.answer()
    user_id, lang = get_user_id_and_lang(update, context)
    try:
        current_page = 0
        if query and query.data and '?page=' in query.data:
            current_page = int(query.data.split('?page=')[1])
    except (ValueError, IndexError, AttributeError): current_page = 0
    
    folders = db.get_folders_by_user(user_id) # Sync
    if not folders: 
        # await query.answer(get_text(user_id, 'task_error_no_folders', lang_override=lang), show_alert=True) # Answered
        await _internal_send_or_edit_message(update,context, get_text(user_id, 'task_error_no_folders', lang_override=lang))
        return await task_select_target_type(update, context) # Back to target type selection
        
    total_items = len(folders); start_index = current_page * ITEMS_PER_PAGE; end_index = start_index + ITEMS_PER_PAGE; folders_page = folders[start_index:end_index]
    
    text = get_text(user_id, 'task_select_folder_title', lang_override=lang); keyboard = []
    for folder in folders_page: 
        button_text = html.escape(folder['name'])
        callback_data = f"{CALLBACK_TASK_PREFIX}set_target_folder?id={folder['id']}"
        keyboard.append([InlineKeyboardButton(button_text, callback_data=callback_data)])
        
    base_callback = f"{CALLBACK_TASK_PREFIX}select_folder_target" # For pagination
    pagination_buttons = build_pagination_buttons(base_callback, current_page, total_items, ITEMS_PER_PAGE, lang=lang); keyboard.extend(pagination_buttons)
    keyboard.append([InlineKeyboardButton(get_text(user_id, 'button_back', lang_override=lang), callback_data=f"{CALLBACK_TASK_PREFIX}back_to_target_type")])
    markup = InlineKeyboardMarkup(keyboard)
    
    await _internal_send_or_edit_message(update, context, text, reply_markup=markup)
    return STATE_TASK_SETUP # Waiting for CB for folder selection (was STATE_SELECT_TARGET_GROUPS, but TASK_SETUP is more general for task config menu)

async def task_set_target(update: Update, context: CallbackContext, target_type_from_cb: str) -> int:
    query = update.callback_query; await query.answer()
    user_id, lang = get_user_id_and_lang(update, context); task_settings = context.user_data.get(CTX_TASK_SETTINGS)
    if task_settings is None: 
        # await query.answer(get_text(user_id, 'session_expired', lang_override=lang), show_alert=True) # Answered
        await _internal_send_or_edit_message(update,context, get_text(user_id, 'session_expired', lang_override=lang))
        return await client_menu(update, context) # Back to main client menu
        
    if target_type_from_cb == 'all': 
        task_settings['send_to_all_groups'] = 1 # True
        task_settings['folder_id'] = None # Clear folder_id
        log.info(f"User {user_id} set task target to all groups (unsaved).")
    elif target_type_from_cb == 'folder':
        try: 
            folder_id = int(query.data.split('?id=')[1])
        except (ValueError, IndexError, AttributeError): 
            log.error(f"Could not parse folder ID from callback: {query.data}")
            # await query.answer(get_text(user_id, 'error_generic', lang_override=lang), show_alert=True) # Answered
            await _internal_send_or_edit_message(update,context, get_text(user_id, 'error_generic', lang_override=lang))
            return STATE_TASK_SETUP # Back to task menu
            
        folder_name = db.get_folder_name(folder_id) # Sync
        if not folder_name: 
            # await query.answer(get_text(user_id, 'folder_not_found_error', lang_override=lang), show_alert=True) # Answered
            await _internal_send_or_edit_message(update,context, get_text(user_id, 'folder_not_found_error', lang_override=lang))
            return STATE_TASK_SETUP # Back to task menu
            
        task_settings['send_to_all_groups'] = 0 # False
        task_settings['folder_id'] = folder_id
        log.info(f"User {user_id} set task target to folder {folder_name} ({folder_id}) (unsaved).")
    else: 
        log.error(f"Invalid target_type '{target_type_from_cb}' in task_set_target.")
        return STATE_TASK_SETUP # Back to task menu
        
    # No specific success message here, main menu will reflect change
    return await task_show_settings_menu(update, context)

async def task_toggle_status(update: Update, context: CallbackContext) -> int:
    query = update.callback_query; await query.answer()
    user_id, lang = get_user_id_and_lang(update, context); task_settings = context.user_data.get(CTX_TASK_SETTINGS)
    if task_settings is None: 
        # await query.answer(get_text(user_id, 'session_expired', lang_override=lang), show_alert=True) # Answered
        await _internal_send_or_edit_message(update,context, get_text(user_id, 'session_expired', lang_override=lang))
        return await client_menu(update, context)
        
    current_status = task_settings.get('status', 'inactive'); 
    new_status = 'inactive' if current_status == 'active' else 'active'
    
    # Validation before activating
    if new_status == 'active':
        missing_fields = []
        if not task_settings.get('message_link'): missing_fields.append(get_text(user_id, 'task_required_message', lang_override=lang))
        if not task_settings.get('start_time'): missing_fields.append(get_text(user_id, 'task_required_start_time', lang_override=lang))
        if not task_settings.get('repetition_interval'): missing_fields.append(get_text(user_id, 'task_required_interval', lang_override=lang))
        if not task_settings.get('folder_id') and not task_settings.get('send_to_all_groups'): 
            missing_fields.append(get_text(user_id, 'task_required_target', lang_override=lang))
            
        if missing_fields: 
            missing_str = ", ".join(missing_fields)
            # query already answered, send as message
            await _internal_send_or_edit_message(update, context, get_text(user_id, 'task_save_validation_fail', lang_override=lang, missing=missing_str))
            # Don't change status, re-show menu
            return await task_show_settings_menu(update, context)
            
    task_settings['status'] = new_status
    log.info(f"User {user_id} toggled task status to {new_status} (unsaved).")
    # No specific success message, main menu reflects change
    return await task_show_settings_menu(update, context)

async def task_save_settings(update: Update, context: CallbackContext) -> int:
    query = update.callback_query; await query.answer()
    user_id, lang = get_user_id_and_lang(update, context); phone = context.user_data.get(CTX_TASK_PHONE); settings_to_save = context.user_data.get(CTX_TASK_SETTINGS)
    
    if not phone or settings_to_save is None: 
        # await query.answer(get_text(user_id, 'session_expired', lang_override=lang), show_alert=True) # Answered
        await _internal_send_or_edit_message(update,context, get_text(user_id, 'session_expired', lang_override=lang))
        return await client_menu(update, context)
        
    # Final validation if trying to save as active
    if settings_to_save.get('status') == 'active':
        missing_fields = []
        if not settings_to_save.get('message_link'): missing_fields.append(get_text(user_id, 'task_required_message', lang_override=lang))
        if not settings_to_save.get('start_time'): missing_fields.append(get_text(user_id, 'task_required_start_time', lang_override=lang))
        if not settings_to_save.get('repetition_interval'): missing_fields.append(get_text(user_id, 'task_required_interval', lang_override=lang))
        if not settings_to_save.get('folder_id') and not settings_to_save.get('send_to_all_groups'): 
            missing_fields.append(get_text(user_id, 'task_required_target', lang_override=lang))
            
        if missing_fields: 
            missing_str = ", ".join(missing_fields)
            await _internal_send_or_edit_message(update, context, get_text(user_id, 'task_save_validation_fail', lang_override=lang, missing=missing_str))
            return STATE_TASK_SETUP # Stay in task menu to fix
            
    # Clear last_error on manual save as per DB function's ON CONFLICT clause
    settings_to_save['last_error'] = None 
    
    if db.save_userbot_task_settings(user_id, phone, settings_to_save): # Sync
        db.log_event_db("Task Settings Saved", f"User: {user_id}, Bot: {phone}, Status: {settings_to_save.get('status')}", user_id=user_id, userbot_phone=phone)
        bot_db_info = db.find_userbot(phone)
        display_name = html.escape(f"@{bot_db_info['username']}" if bot_db_info and bot_db_info['username'] else phone)
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'task_save_success', lang_override=lang, display_name=display_name))
        clear_conversation_data(context)
        return await client_menu(update, context) # Back to main client menu
    else: 
        db.log_event_db("Task Save Failed", f"User: {user_id}, Bot: {phone}, DB Error", user_id=user_id, userbot_phone=phone)
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'task_save_error', lang_override=lang))
        return STATE_TASK_SETUP # Stay in task menu

async def admin_list_userbots(update: Update, context: CallbackContext) -> int:
    query = update.callback_query; await query.answer()
    user_id, lang = get_user_id_and_lang(update, context)
    try:
        current_page = 0
        if query and query.data and '?page=' in query.data:
            current_page = int(query.data.split('?page=')[1])
    except (ValueError, IndexError, AttributeError): current_page = 0
    
    all_bots = db.get_all_userbots() # Sync
    if not all_bots:
        text = get_text(user_id, 'admin_userbot_list_no_bots', lang_override=lang)
        markup = InlineKeyboardMarkup([[InlineKeyboardButton(get_text(user_id, 'button_back', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}back_to_menu")]])
        await _internal_send_or_edit_message(update, context, text, reply_markup=markup)
        return ConversationHandler.END # To admin menu usually
        
    total_items = len(all_bots); start_index = current_page * ITEMS_PER_PAGE; end_index = start_index + ITEMS_PER_PAGE; bots_page = all_bots[start_index:end_index]
    
    text = f"<b>{get_text(user_id, 'admin_userbot_list_title', lang_override=lang)}</b> (Page {current_page + 1}/{math.ceil(total_items / ITEMS_PER_PAGE)})\n\n"
    for bot in bots_page:
        phone = bot['phone_number']; username = bot['username']; status = bot['status']
        assigned_client_code = bot['assigned_client'] or get_text(user_id, 'admin_userbot_list_unassigned', lang_override=lang)
        last_error = bot['last_error']; display_name = f"@{username}" if username else phone
        
        status_icon_key = f'admin_userbot_list_status_icon_{status}'
        icon_fallback = {'active': "🟢", 'inactive': "⚪️", 'error': "🔴", 'connecting': "🔌", 'needs_code': "🔢", 'needs_password': "🔒", 'authenticating': "⏳", 'initializing': "⚙️"}.get(status, "❓")
        status_icon = get_text(user_id, status_icon_key, lang_override=lang) if status_icon_key in translations.get(lang, {}) else icon_fallback
        
        text += get_text(user_id, 'admin_userbot_list_line', lang_override=lang, status_icon=status_icon, display_name=html.escape(display_name), phone=html.escape(phone), client_code=html.escape(assigned_client_code), status=html.escape(status.capitalize())) + "\n"
        if last_error: 
            error_text = html.escape(last_error)
            text += get_text(user_id, 'admin_userbot_list_error_line', lang_override=lang, error=error_text[:150] + ("..." if len(error_text)>150 else "")) + "\n"
            
    keyboard = []; base_callback = f"{CALLBACK_ADMIN_PREFIX}list_bots"
    pagination_buttons = build_pagination_buttons(base_callback, current_page, total_items, ITEMS_PER_PAGE, lang=lang); keyboard.extend(pagination_buttons)
    keyboard.append([InlineKeyboardButton(get_text(user_id, 'button_back', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}back_to_menu")])
    markup = InlineKeyboardMarkup(keyboard)
    
    await _internal_send_or_edit_message(update, context, text, reply_markup=markup, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    return ConversationHandler.END # Display only

async def admin_select_userbot_to_remove(update: Update, context: CallbackContext) -> int:
    query = update.callback_query; await query.answer()
    user_id, lang = get_user_id_and_lang(update, context)
    try:
        current_page = 0
        if query and query.data and '?page=' in query.data:
            current_page = int(query.data.split('?page=')[1])
    except (ValueError, IndexError, AttributeError): current_page = 0
        
    all_bots = db.get_all_userbots() # Sync
    if not all_bots: 
        text = get_text(user_id, 'admin_userbot_no_bots_to_remove', lang_override=lang)
        markup = InlineKeyboardMarkup([[InlineKeyboardButton(get_text(user_id, 'button_back', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}back_to_menu")]])
        await _internal_send_or_edit_message(update, context, text, reply_markup=markup)
        return ConversationHandler.END
        
    total_items = len(all_bots); start_index = current_page * ITEMS_PER_PAGE; end_index = start_index + ITEMS_PER_PAGE; bots_page = all_bots[start_index:end_index]
    
    text = get_text(user_id, 'admin_userbot_select_remove', lang_override=lang); keyboard = []
    for bot in bots_page: 
        phone = bot['phone_number']; username = bot['username']
        display_name = f"@{username}" if username else phone
        button_text = f"🗑️ {html.escape(display_name)}"
        keyboard.append([InlineKeyboardButton(button_text, callback_data=f"{CALLBACK_ADMIN_PREFIX}remove_bot_confirm_prompt_{phone}")]) # Changed CB
        
    base_callback = f"{CALLBACK_ADMIN_PREFIX}remove_bot_select"
    pagination_buttons = build_pagination_buttons(base_callback, current_page, total_items, ITEMS_PER_PAGE, lang=lang); keyboard.extend(pagination_buttons)
    keyboard.append([InlineKeyboardButton(get_text(user_id, 'button_back', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}back_to_menu")])
    markup = InlineKeyboardMarkup(keyboard)
    
    await _internal_send_or_edit_message(update, context, text, reply_markup=markup)
    return STATE_ADMIN_CONFIRM_USERBOT_RESET # State for waiting for CB from this menu

async def admin_confirm_remove_userbot_prompt(update: Update, context: CallbackContext) -> int: # Renamed for clarity
     query = update.callback_query; await query.answer()
     user_id, lang = get_user_id_and_lang(update, context);
     phone_to_remove = None
     try:
         phone_to_remove = query.data.split(f"{CALLBACK_ADMIN_PREFIX}remove_bot_confirm_prompt_")[1] # From CB
     except IndexError:
         log.error(f"Could not parse phone from remove confirm prompt callback: {query.data}")
         await _internal_send_or_edit_message(update,context, get_text(user_id, 'error_generic', lang_override=lang))
         return await admin_command(update, context) # Back to admin menu

     bot_info = db.find_userbot(phone_to_remove) # Sync
     if not bot_info:
         await _internal_send_or_edit_message(update,context, get_text(user_id, 'admin_userbot_not_found', lang_override=lang))
         return await admin_command(update, context) 

     username = bot_info['username']; display_name = html.escape(f"@{username}" if username else phone_to_remove)
     text = get_text(user_id, 'admin_userbot_remove_confirm_text', lang_override=lang, display_name=display_name)
     keyboard = [[ 
        InlineKeyboardButton(get_text(user_id, 'button_yes', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}remove_bot_confirmed_execute_{phone_to_remove}"), # New CB for execution
        InlineKeyboardButton(get_text(user_id, 'button_no', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}back_to_menu") 
     ]]; 
     markup = InlineKeyboardMarkup(keyboard)
     await _internal_send_or_edit_message(update, context, text, reply_markup=markup)
     return STATE_ADMIN_CONFIRM_USERBOT_RESET # Waiting for Yes/No CB

async def admin_remove_userbot_confirmed_execute(update: Update, context: CallbackContext) -> int: # Renamed
    query = update.callback_query; await query.answer()
    user_id, lang = get_user_id_and_lang(update, context);
    phone_to_remove = None
    try:
        phone_to_remove = query.data.split(f"{CALLBACK_ADMIN_PREFIX}remove_bot_confirmed_execute_")[1] # From CB
    except IndexError:
        log.error(f"Could not parse phone from remove confirmed execute callback: {query.data}");
        await _internal_send_or_edit_message(update,context, get_text(user_id, 'error_generic', lang_override=lang))
        return await admin_command(update, context) 

    bot_info = db.find_userbot(phone_to_remove) # Sync
    display_name = "N/A";
    if bot_info: display_name = html.escape(f"@{bot_info['username']}" if bot_info['username'] else phone_to_remove)
    
    log.info(f"Admin {user_id} confirmed removal of userbot {phone_to_remove}")
    
    # Stop runtime first (this is synchronous but internally schedules async tasks)
    stopped = telethon_api.stop_userbot_runtime(phone_to_remove)
    log.info(f"Runtime stop request for {phone_to_remove}: {'Successful' if stopped else 'Not running/Failed'}")
    
    # Then remove from DB (sync)
    if db.remove_userbot(phone_to_remove):
        log.info(f"Attempting to remove session files for {phone_to_remove}...")
        telethon_api.delete_session_files_for_phone(phone_to_remove) # Sync (file ops)
        db.log_event_db("Userbot Removed", f"Phone: {phone_to_remove}", user_id=user_id, userbot_phone=phone_to_remove)
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_remove_success', lang_override=lang, display_name=display_name))
    else: # Bot not found in DB or DB delete failed
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_remove_error', lang_override=lang)) # Or more specific if bot not found
        
    return await admin_command(update, context) # Back to admin menu

async def admin_view_subscriptions(update: Update, context: CallbackContext) -> int:
    query = update.callback_query; await query.answer()
    user_id, lang = get_user_id_and_lang(update, context)
    try:
        current_page = 0
        if query and query.data and '?page=' in query.data:
            current_page = int(query.data.split('?page=')[1])
    except (ValueError, IndexError, AttributeError): current_page = 0
        
    subs = db.get_all_subscriptions() # Sync
    if not subs:
        text = get_text(user_id, 'admin_subs_none', lang_override=lang)
        markup = InlineKeyboardMarkup([[InlineKeyboardButton(get_text(user_id, 'button_back', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}back_to_menu")]])
        await _internal_send_or_edit_message(update, context, text, reply_markup=markup)
        return ConversationHandler.END 
        
    total_items = len(subs); start_index = current_page * ITEMS_PER_PAGE; end_index = start_index + ITEMS_PER_PAGE; subs_page = subs[start_index:end_index]
    
    text = f"<b>{get_text(user_id, 'admin_subs_title', lang_override=lang)}</b> (Page {current_page + 1}/{math.ceil(total_items / ITEMS_PER_PAGE)})\n\n"
    for sub in subs_page:
        client_user_id = sub['user_id']; 
        user_link = get_text(user_id, 'admin_subs_no_user', lang_override=lang) # Default if no user_id
        if client_user_id:
             try: user_link = f"<a href='tg://user?id={client_user_id}'>{client_user_id}</a>"
             except Exception as e: log.debug(f"Could not create user link for {client_user_id}: {e}"); user_link = f"ID: `{client_user_id}`"
             
        end_date = format_dt(sub['subscription_end']); code = sub['invitation_code']; bot_count = sub['bot_count']
        text += get_text(user_id, 'admin_subs_line', lang_override=lang, user_link=user_link, code=html.escape(code), end_date=end_date, bot_count=bot_count) + "\n\n"
        
    keyboard = []; base_callback = f"{CALLBACK_ADMIN_PREFIX}view_subs"
    pagination_buttons = build_pagination_buttons(base_callback, current_page, total_items, ITEMS_PER_PAGE, lang=lang); keyboard.extend(pagination_buttons)
    keyboard.append([InlineKeyboardButton(get_text(user_id, 'button_back', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}back_to_menu")])
    markup = InlineKeyboardMarkup(keyboard)
    
    await _internal_send_or_edit_message(update, context, text, reply_markup=markup, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    return ConversationHandler.END

async def admin_view_system_logs(update: Update, context: CallbackContext) -> int:
    query = update.callback_query; await query.answer()
    user_id, lang = get_user_id_and_lang(update, context); limit = 25
    
    logs_from_db = db.get_recent_logs(limit=limit) # Sync
    if not logs_from_db: 
        text = get_text(user_id, 'admin_logs_none', lang_override=lang)
        markup = InlineKeyboardMarkup([[InlineKeyboardButton(get_text(user_id, 'button_back', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}back_to_menu")]])
        await _internal_send_or_edit_message(update, context, text, reply_markup=markup)
        return ConversationHandler.END
        
    text = f"<b>{get_text(user_id, 'admin_logs_title', lang_override=lang, limit=limit)}</b>\n\n"
    for log_entry in logs_from_db:
        ts = log_entry['timestamp']; event = log_entry['event']; log_user_id = log_entry['user_id']
        log_bot_phone = log_entry['userbot_phone']; details = log_entry['details']; time_str = format_dt(ts)
        
        user_str = get_text(user_id, 'admin_logs_user_none', lang_override=lang) # Default "System"
        if log_user_id: 
            user_str = get_text(user_id, 'admin_logs_user_admin', lang_override=lang) + f" ({log_user_id})" if is_admin(log_user_id) else f"Client ({log_user_id})"
            
        bot_str = html.escape(log_bot_phone) if log_bot_phone else get_text(user_id, 'admin_logs_bot_none', lang_override=lang)
        details_str = html.escape(details[:100]) + ('...' if details and len(details)>100 else '') if details else ""
        
        text += get_text(user_id, 'admin_logs_line', lang_override=lang, time=time_str, event=html.escape(event), user=user_str, bot=bot_str, details=details_str) + "\n"
        
    keyboard = [[InlineKeyboardButton(get_text(user_id, 'button_back', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}back_to_menu")]]
    markup = InlineKeyboardMarkup(keyboard)
    
    await _internal_send_or_edit_message(update, context, text, reply_markup=markup, parse_mode=ParseMode.HTML)
    return ConversationHandler.END

# --- Callback Query Routers ---

async def handle_client_callback(update: Update, context: CallbackContext) -> str | int | None:
    query = update.callback_query # No answer here, sub-handlers will answer or this will answer at end.
    user_id, lang = get_user_id_and_lang(update, context); data = query.data
    client_info = db.find_client_by_user_id(user_id)
    
    log.info(f"handle_client_callback: User {user_id}, Data {data}, Lang {lang}")

    if not client_info or client_info['subscription_end'] < int(datetime.now(UTC_TZ).timestamp()):
        log.warning(f"Expired/Invalid client {user_id} tried action: {data}")
        await query.answer(get_text(user_id, 'subscription_expired', lang_override=lang), show_alert=True)
        clear_conversation_data(context); return ConversationHandler.END
        
    action = data.split(CALLBACK_CLIENT_PREFIX)[1].split('?')[0] # Get action part before query params
    log.debug(f"Client Callback Route: Action='{action}', Data='{data}'")

    # Route to specific async client action handlers
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
        clear_conversation_data(context)
        return await client_menu(update, context)
    else: 
        log.warning(f"Unhandled CLIENT CB: Action='{action}', Data='{data}'")
        await query.answer(get_text(user_id, 'error_invalid_action', lang_override=lang, default_text="Action not recognized."), show_alert=True)
        return ConversationHandler.END # Or None if it's part of a sub-conversation


async def handle_admin_callback(update: Update, context: CallbackContext) -> str | int | None:
    query = update.callback_query # Answer in sub-handlers or at the end
    user_id, lang = get_user_id_and_lang(update, context); data = query.data

    if not is_admin(user_id):
        await query.answer(get_text(user_id, 'unauthorized', lang_override=lang), show_alert=True)
        return ConversationHandler.END

    action_full = data.split(CALLBACK_ADMIN_PREFIX)[1] # e.g., "add_bot_prompt" or "list_bots?page=0"
    action_main = action_full.split('?')[0] # e.g., "add_bot_prompt" or "list_bots"
    log.info(f"handle_admin_callback: User {user_id}, ActionFull '{action_full}', ActionMain '{action_main}'")

    if action_main == "back_to_menu":
        await query.answer()
        return await admin_command(update, context) # Re-show admin menu
    elif action_main == "add_bot_prompt":
        await query.answer()
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'admin_userbot_prompt_phone', lang_override=lang))
        return STATE_WAITING_FOR_PHONE
    elif action_main == "remove_bot_select":
        # await query.answer() # Done in admin_select_userbot_to_remove
        return await admin_select_userbot_to_remove(update, context)
    elif action_main.startswith("remove_bot_confirm_prompt_"): # e.g. remove_bot_confirm_prompt_+12345
        # await query.answer() # Done in admin_confirm_remove_userbot_prompt
        return await admin_confirm_remove_userbot_prompt(update, context)
    elif action_main.startswith("remove_bot_confirmed_execute_"):
        # await query.answer() # Done in admin_remove_userbot_confirmed_execute
        return await admin_remove_userbot_confirmed_execute(update, context)
    elif action_main == "list_bots":
        # await query.answer() # Done in admin_list_userbots
        return await admin_list_userbots(update, context)
    elif action_main == "gen_invite_prompt":
        await query.answer()
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'admin_invite_prompt_details', lang_override=lang))
        return STATE_WAITING_FOR_SUB_DETAILS
    elif action_main == "view_subs":
        # await query.answer() # Done in admin_view_subscriptions
        return await admin_view_subscriptions(update, context)
    elif action_main == "extend_sub_prompt":
        await query.answer()
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'admin_extend_prompt_code', lang_override=lang))
        return STATE_WAITING_FOR_EXTEND_CODE
    elif action_main == "assign_bots_prompt":
        await query.answer()
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'admin_assignbots_prompt_code', lang_override=lang))
        return STATE_WAITING_FOR_ADD_USERBOTS_CODE
    elif action_main == "view_logs":
        # await query.answer() # Done in admin_view_system_logs
        return await admin_view_system_logs(update, context)
    elif action_main == "manage_tasks": # Leads to admin task sub-menu
        # await query.answer() # Done in admin_task_menu
        return await admin_task_menu(update, context)
    elif action_main == "view_tasks": # Lists admin tasks
        # await query.answer() # Done in admin_view_tasks
        return await admin_view_tasks(update, context) # Shows list, then ENDs or goes to task_options
    elif action_main == "create_task": # Starts admin task creation flow
        # await query.answer() # Done in admin_create_task_start
        return await admin_create_task_start(update, context) # Returns STATE_WAITING_FOR_TASK_BOT
    elif action_main.startswith("task_bot_"): # Admin selected a bot for a new task
        await query.answer()
        context.user_data[CTX_TASK_BOT] = action_main.replace("task_bot_", "")
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'admin_task_enter_message', lang_override=lang))
        return STATE_ADMIN_TASK_MESSAGE # Waiting for task message (text input)
    # Note: task_schedule, task_target_all, task_target_folder, task_folder_
    #       task_confirm, task_options_, toggle_task_, delete_task_
    #       are handled by the more detailed logic in admin_handlers section below
    #       if they are distinct admin task management states.
    #       The current structure implies these are routed through the imported async_admin_process functions
    #       which means they should be MessageHandlers, not callback handlers here.
    #       If they are meant to be callbacks, they need their own `elif action_main == "task_schedule_..."` blocks.
    #       For now, assuming they are MessageHandlers.
    
    else:
        await query.answer(get_text(user_id, 'error_invalid_action', lang_override=lang, default_text="Admin action not recognized."), show_alert=True)
        return ConversationHandler.END

async def handle_folder_callback(update: Update, context: CallbackContext) -> str | int | None:
    query = update.callback_query # Answer in sub-handlers or at the end
    user_id, lang = get_user_id_and_lang(update, context); data = query.data
    action = data.split(CALLBACK_FOLDER_PREFIX)[1].split('?')[0]
    
    log.info(f"handle_folder_callback: User {user_id}, Data {data}, Action {action}, Lang {lang}")

    if action == "create_prompt": 
        await query.answer()
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'folder_create_prompt', lang_override=lang))
        return STATE_WAITING_FOR_FOLDER_NAME
    elif action == "select_edit": 
        return await client_select_folder_to_edit_or_delete(update, context, 'edit')
    elif action == "select_delete": 
        return await client_select_folder_to_edit_or_delete(update, context, 'delete')
    elif action == "edit_selected": # Folder selected for edit
        return await client_show_folder_edit_options(update, context)
    elif action == "delete_selected_prompt": # Folder selected for delete, show prompt
        return await client_confirm_folder_delete_prompt(update, context)
    elif action == "delete_confirmed_execute": # Yes on delete confirmation
        return await client_delete_folder_confirmed_execute(update, context)
    elif action == "back_to_manage": 
        await query.answer()
        clear_conversation_data(context) # Clear folder-specific stuff
        return await client_folder_menu(update, context)
    elif action == "edit_add_prompt": 
        await query.answer()
        folder_name = context.user_data.get(CTX_FOLDER_NAME, get_text(user_id, "this_folder", lang_override=lang, default_text="this folder"))
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'folder_edit_add_prompt', lang_override=lang, name=html.escape(folder_name)))
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
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'folder_edit_rename_prompt', lang_override=lang, current_name=html.escape(current_name)))
        return STATE_FOLDER_RENAME_PROMPT
    elif action == "back_to_edit_options": 
        await query.answer()
        context.user_data.pop(CTX_TARGET_GROUP_IDS_TO_REMOVE, None) # Clear selections
        return await client_show_folder_edit_options(update, context)
    else: 
        log.warning(f"Unhandled FOLDER CB: Action='{action}', Data='{data}'")
        await query.answer(get_text(user_id, 'error_invalid_action', lang_override=lang, default_text="Folder action not recognized."), show_alert=True)
        return ConversationHandler.END

async def handle_task_callback(update: Update, context: CallbackContext) -> str | int | None:
    query = update.callback_query # Answer in sub-handlers or at the end
    user_id, lang = get_user_id_and_lang(update, context); data = query.data
    action = data.split(CALLBACK_TASK_PREFIX)[1].split('?')[0]
    
    log.info(f"handle_task_callback: User {user_id}, Data {data}, Action {action}, Lang {lang}")

    if action.startswith("select_"): # e.g. task_select_PHONE_NUMBER
        return await handle_userbot_selection_callback(update, context, CALLBACK_TASK_PREFIX)
    elif action == "back_to_bot_select": 
        await query.answer()
        clear_conversation_data(context) # Clear task-specific settings
        return await client_select_bot_generic(update, context, CALLBACK_TASK_PREFIX, STATE_TASK_SETUP, 'task_select_userbot')
    elif action == "back_to_task_menu": 
        # await query.answer() # Done in task_show_settings_menu
        return await task_show_settings_menu(update, context)
    elif action == "set_primary_link": 
        return await task_prompt_set_link(update, context, 'primary')
    # Fallback link option removed for brevity, can be re-added if needed:
    # elif action == "set_fallback_link": 
    #     return await task_prompt_set_link(update, context, 'fallback')
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
    elif action == "set_target_folder": # ?id=FOLDER_ID
        return await task_set_target(update, context, 'folder')
    elif action == "back_to_target_type": 
        # await query.answer() # Done in task_select_target_type
        return await task_select_target_type(update, context)
    elif action == "toggle_status": 
        return await task_toggle_status(update, context)
    elif action == "save": 
        return await task_save_settings(update, context)
    else: 
        log.warning(f"Unhandled TASK CB: Action='{action}', Data='{data}'")
        await query.answer(get_text(user_id, 'error_invalid_action', lang_override=lang, default_text="Task action not recognized."), show_alert=True)
        return ConversationHandler.END

async def handle_join_callback(update: Update, context: CallbackContext) -> str | int | None:
    query = update.callback_query # Answer in sub-handlers or at the end
    user_id, lang = get_user_id_and_lang(update, context); data = query.data
    
    log.info(f"handle_join_callback: User {user_id}, Data {data}, Lang {lang}")

    if data.startswith(CALLBACK_JOIN_PREFIX + "select_"): # e.g., join_select_active or join_select_PHONE
        return await handle_userbot_selection_callback(update, context, CALLBACK_JOIN_PREFIX)
    else: 
        log.warning(f"Unhandled JOIN CB: Data='{data}'")
        await query.answer(get_text(user_id, 'error_invalid_action', lang_override=lang, default_text="Join action not recognized."), show_alert=True)
        return ConversationHandler.END

async def handle_language_callback(update: Update, context: CallbackContext) -> str | int | None:
    query = update.callback_query # Answer in sub-handlers or at the end
    user_id, lang = get_user_id_and_lang(update,context); data = query.data
    
    log.info(f"handle_language_callback: User {user_id}, Data {data}, Lang {lang}")

    if data.startswith(CALLBACK_LANG_PREFIX): # e.g. lang_lt
        return await set_language_handler(update, context)
    else: 
        log.warning(f"Unhandled LANG CB: Data='{data}'")
        await query.answer(get_text(user_id, 'error_invalid_action', lang_override=lang, default_text="Language action not recognized."), show_alert=True)
        return ConversationHandler.END

async def handle_interval_callback(update: Update, context: CallbackContext) -> str | int | None:
     query = update.callback_query # Answer in sub-handlers or at the end
     user_id, lang = get_user_id_and_lang(update,context); data = query.data
     
     log.info(f"handle_interval_callback: User {user_id}, Data {data}, Lang {lang}")

     if data.startswith(CALLBACK_INTERVAL_PREFIX): # e.g. interval_30
         return await process_interval_callback(update, context)
     else: 
         log.warning(f"Unhandled INTERVAL CB: Data='{data}'")
         await query.answer(get_text(user_id, 'error_invalid_action', lang_override=lang, default_text="Interval action not recognized."), show_alert=True)
         return ConversationHandler.END

async def handle_generic_callback(update: Update, context: CallbackContext) -> str | int | None:
    query = update.callback_query; await query.answer() # Answer generic callbacks immediately
    user_id, lang = get_user_id_and_lang(update, context); data = query.data
    action = data.split(CALLBACK_GENERIC_PREFIX)[1] if CALLBACK_GENERIC_PREFIX in data else "unknown"
    
    log.info(f"handle_generic_callback: User {user_id}, Data {data}, Action {action}, Lang {lang}")

    if action == "cancel" or action == "confirm_no": # General cancel or "No" to a confirmation
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'cancelled', lang_override=lang))
        clear_conversation_data(context)
        # Decide where to go back to
        if is_admin(user_id): return await admin_command(update, context)
        elif db.find_client_by_user_id(user_id): return await client_menu(update, context)
        else: return ConversationHandler.END # If no specific menu, just end
    elif action == "noop":
        # query already answered
        return None # Do nothing, stay in current state or let PTB handle it
    else:
        log.warning(f"Unhandled GENERIC CB: Action='{action}', Data='{data}'")
        # query already answered
        await _internal_send_or_edit_message(update,context,get_text(user_id, 'error_invalid_action', lang_override=lang, default_text="Generic action not recognized."))
        return ConversationHandler.END


async def main_callback_handler(update: Update, context: CallbackContext) -> str | int | None:
    """Master router for all callback queries."""
    query = update.callback_query
    # DO NOT answer query here. Let sub-handlers do it for better UX (e.g. alerts vs silent answer)
    
    user_id = query.from_user.id # Get user_id early for logging and auth checks
    data = query.data
    # lang will be fetched by get_user_id_and_lang in sub-handlers

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
        
        # If sub-handler didn't answer and didn't raise an error, answer silently
        if not query._answered:
            try: await query.answer()
            except Exception: pass # Ignore if already answered or other error

        return next_state

    except Exception as e:
        log.error(f"Error in main_callback_handler for data '{data}': {e}", exc_info=True)
        if not query._answered:
            try: await query.answer(get_text(user_id, 'error_generic', lang_override=context.user_data.get(CTX_LANG,'en')), show_alert=True)
            except Exception: pass
        return ConversationHandler.END


# --- Main Conversation Handler Setup ---
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
        STATE_WAITING_FOR_GROUP_LINKS: [MessageHandler(Filters.text & ~Filters.command, process_folder_links)], # Used by folder add
        STATE_FOLDER_RENAME_PROMPT: [MessageHandler(Filters.text & ~Filters.command, process_folder_rename)],
        
        STATE_WAITING_FOR_PRIMARY_MESSAGE_LINK: [MessageHandler(Filters.text & ~Filters.command, lambda u, c: process_task_link(u, c, 'primary'))],
        STATE_WAITING_FOR_FALLBACK_MESSAGE_LINK: [MessageHandler(Filters.text & ~Filters.command, lambda u, c: process_task_link(u, c, 'fallback'))],
        STATE_WAITING_FOR_START_TIME: [MessageHandler(Filters.text & ~Filters.command, process_task_start_time)],

        # Admin Task Creation (Text Input States) - using imported async handlers
        STATE_ADMIN_TASK_MESSAGE: [MessageHandler(Filters.text & ~Filters.command, async_admin_process_task_message)],
        STATE_ADMIN_TASK_SCHEDULE: [MessageHandler(Filters.text & ~Filters.command, async_admin_process_task_schedule)],
        STATE_ADMIN_TASK_TARGET: [MessageHandler(Filters.text & ~Filters.command, async_admin_process_task_target)],

        # States that are primarily navigated via Callbacks (but might have a fallback message handler)
        STATE_TASK_SETUP: [CallbackQueryHandler(main_callback_handler)], # Main task menu
        STATE_WAITING_FOR_FOLDER_SELECTION: [CallbackQueryHandler(main_callback_handler)], # For folder edit/delete choice
        STATE_WAITING_FOR_USERBOT_SELECTION: [CallbackQueryHandler(main_callback_handler)], # For bot choice (join/task)
        STATE_WAITING_FOR_FOLDER_ACTION: [CallbackQueryHandler(main_callback_handler)], # After folder selected for edit
        STATE_FOLDER_EDIT_REMOVE_SELECT: [CallbackQueryHandler(main_callback_handler)], # Selecting groups in folder to remove
        STATE_ADMIN_CONFIRM_USERBOT_RESET: [CallbackQueryHandler(main_callback_handler)], # Confirming bot removal

        # Fallback for any message in these specific states if not handled by CBs (e.g. user types text instead of clicking button)
        # Add MessageHandler(Filters.text, conversation_fallback_message_handler) to these if needed
    },
    fallbacks=[
        CommandHandler('cancel', cancel_command),
        CallbackQueryHandler(main_callback_handler), # Catch-all for callbacks not tied to a specific state's MessageHandler
        MessageHandler(Filters.all, conversation_fallback) # Fallback for any unhandled message
    ],
    name="main_conversation",
    persistent=False, 
    allow_reentry=True
)

log.info("Handlers module loaded with corrected sync/async definitions.")

# --- Admin Task Management Handlers (specific to admin task creation/editing flow) ---
# These functions are called by admin callbacks or handle message inputs for admin tasks.

async def admin_task_menu(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    if query: await query.answer()
    user_id, lang = get_user_id_and_lang(update, context)
    # No admin check needed here if entry to this is already admin-protected
    
    keyboard = [
        [InlineKeyboardButton(get_text(user_id, 'admin_task_view', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}view_tasks?page=0")], # Ensure CB has page
        [InlineKeyboardButton(get_text(user_id, 'admin_task_create', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}create_task")],
        [InlineKeyboardButton(get_text(user_id, 'button_back', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}back_to_menu")]
    ]
    markup = InlineKeyboardMarkup(keyboard)
    
    await _internal_send_or_edit_message(
        update, context,
        get_text(user_id, 'admin_task_menu_title', lang_override=lang),
        reply_markup=markup
    )
    return ConversationHandler.END # This menu is an endpoint, actions via CB

async def admin_view_tasks(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    if query: await query.answer()
    user_id, lang = get_user_id_and_lang(update, context)

    try:
        current_page = 0
        if query and query.data and '?page=' in query.data:
            current_page = int(query.data.split('?page=')[1])
    except (ValueError, IndexError, AttributeError): current_page = 0
    
    tasks, total_tasks = db.get_admin_tasks(page=current_page, per_page=ITEMS_PER_PAGE) # Assuming get_admin_tasks returns (list, total_count)
    
    keyboard = []
    text = f"<b>{get_text(user_id, 'admin_task_list_title', lang_override=lang)}</b>"
    
    if tasks:
        text += f" (Page {current_page + 1}/{math.ceil(total_tasks / ITEMS_PER_PAGE)})\n\n"
        for task in tasks:
            status_icon = "🟢" if task['status'] == 'active' else "⚪️" # Use status from DB
            # Display basic info: Bot phone, target, schedule
            task_info_line = f"{status_icon} Bot: {html.escape(task['userbot_phone'])} -> Target: {html.escape(task['target'])}"
            if task['schedule']:
                task_info_line += f" | Schedule: <code>{html.escape(task['schedule'])}</code>"

            keyboard.append([
                InlineKeyboardButton(
                    task_info_line,
                    callback_data=f"{CALLBACK_ADMIN_PREFIX}task_options_{task['id']}"
                )
            ])
    else:
        text += "\n" + get_text(user_id, 'admin_task_list_empty', lang_override=lang)
    
    pagination_buttons = build_pagination_buttons(f"{CALLBACK_ADMIN_PREFIX}view_tasks", current_page, total_tasks, ITEMS_PER_PAGE, lang)
    keyboard.extend(pagination_buttons)
    keyboard.append([InlineKeyboardButton(get_text(user_id, 'button_back', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}manage_tasks")]) # Back to manage_tasks menu
    markup = InlineKeyboardMarkup(keyboard)
    
    await _internal_send_or_edit_message(update, context, text, reply_markup=markup, parse_mode=ParseMode.HTML)
    return ConversationHandler.END # Display only, options via CB

async def admin_create_task_start(update: Update, context: CallbackContext) -> int:
    # This is typically called by a CB that has already been answered.
    user_id, lang = get_user_id_and_lang(update, context)
    
    clear_conversation_data(context) # Start fresh for new task
    # Re-set essential context after clearing
    context.user_data[CTX_USER_ID] = user_id
    context.user_data[CTX_LANG] = lang
    return await admin_select_task_bot(update, context) # Transition to bot selection

async def admin_select_task_bot(update: Update, context: CallbackContext) -> int:
    # Called by admin_create_task_start, no query to answer usually.
    user_id, lang = get_user_id_and_lang(update, context)
    
    keyboard = []
    # Get all bots, filter for active ones
    all_bots_db = db.get_all_userbots() 
    active_bots = [bot for bot in all_bots_db if bot['status'] == 'active']
    
    if not active_bots:
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'admin_task_no_bots', lang_override=lang),
                                           reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(get_text(user_id,'button_back',lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}manage_tasks")]]))
        return ConversationHandler.END
    
    for bot in active_bots:
        display_name = f"@{bot['username']}" if bot['username'] else bot['phone_number']
        keyboard.append([InlineKeyboardButton(html.escape(display_name), callback_data=f"{CALLBACK_ADMIN_PREFIX}task_bot_{bot['phone_number']}")])
    
    keyboard.append([InlineKeyboardButton(get_text(user_id, 'button_back', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}manage_tasks")])
    markup = InlineKeyboardMarkup(keyboard)
    
    await _internal_send_or_edit_message(
        update, context,
        get_text(user_id, 'admin_task_select_bot', lang_override=lang),
        reply_markup=markup
    )
    # STATE_WAITING_FOR_TASK_BOT isn't strictly needed if selection is via CB
    # The callback handler (handle_admin_callback) will catch "task_bot_PHONE"
    # and then transition to STATE_ADMIN_TASK_MESSAGE.
    # This state is more for if we expected a text message for bot selection.
    # For now, let's keep it as a conceptual state the CB leads out of.
    return STATE_ADMIN_TASK_MESSAGE # Or simply return None/END and let CB handle it. For safety, having a state is fine.

# admin_process_task_message, schedule, target are now imported as async
# from admin_handlers.py. They will be used as MessageHandlers for states:
# STATE_ADMIN_TASK_MESSAGE, STATE_ADMIN_TASK_SCHEDULE, STATE_ADMIN_TASK_TARGET

async def admin_task_options(update: Update, context: CallbackContext) -> int: # task_id passed in CB data
    query = update.callback_query; await query.answer()
    user_id, lang = get_user_id_and_lang(update, context)
    
    task_id = None
    try: task_id = int(query.data.split(f"{CALLBACK_ADMIN_PREFIX}task_options_")[1])
    except (IndexError, ValueError):
        log.error(f"Failed to parse task_id from CB: {query.data}")
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'error_generic', lang_override=lang))
        return await admin_view_tasks(update, context) # Go back to list

    task = db.get_admin_task(task_id) # Sync
    if not task:
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'admin_task_not_found', lang_override=lang)) # Use specific key
        return await admin_view_tasks(update, context)
    
    status_icon = "🟢" if task['status'] == 'active' else "⚪️"
    toggle_text_key = 'admin_task_deactivate' if task['status'] == 'active' else 'admin_task_activate'
    toggle_text = get_text(user_id, toggle_text_key, lang_override=lang)
    
    keyboard = [
        [InlineKeyboardButton(toggle_text, callback_data=f"{CALLBACK_ADMIN_PREFIX}toggle_task_status_{task_id}")], # New CB
        [InlineKeyboardButton(get_text(user_id, 'admin_task_delete_button', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}delete_task_confirm_{task_id}")], # New CB for confirm
        [InlineKeyboardButton(get_text(user_id, 'button_back', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}view_tasks?page=0")] # Back to task list
    ]
    markup = InlineKeyboardMarkup(keyboard)
    
    # Format task details for display
    details_text = f"<b>Task #{task_id} Details</b>\n"
    details_text += f"Status: {status_icon} {html.escape(task['status'].capitalize())}\n"
    details_text += f"Bot: {html.escape(task['userbot_phone'])}\n"
    details_text += f"Message: <pre>{html.escape(task['message'][:100])}{'...' if len(task['message']) > 100 else ''}</pre>\n"
    details_text += f"Schedule: <code>{html.escape(task['schedule'])}</code>\n"
    details_text += f"Target: {html.escape(task['target'])}\n"
    details_text += f"Last Run: {format_dt(task['last_run']) if task['last_run'] else 'Never'}\n"
    details_text += f"Next Run Estimate: {format_dt(task['next_run']) if task['next_run'] else 'Not Scheduled'}\n"
    
    await _internal_send_or_edit_message(update, context, details_text, reply_markup=markup, parse_mode=ParseMode.HTML)
    return ConversationHandler.END # Display options, further actions via CB

async def admin_toggle_task_status(update: Update, context: CallbackContext) -> int:
    query = update.callback_query; await query.answer()
    user_id, lang = get_user_id_and_lang(update, context)
    
    task_id = None
    try: task_id = int(query.data.split(f"{CALLBACK_ADMIN_PREFIX}toggle_task_status_")[1])
    except (IndexError, ValueError):
        log.error(f"Failed to parse task_id for toggle status: {query.data}")
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'error_generic', lang_override=lang))
        return await admin_view_tasks(update, context)

    if db.toggle_admin_task_status(task_id): # Sync
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'admin_task_toggled', lang_override=lang))
        # Re-show task options for the same task
        query.data = f"{CALLBACK_ADMIN_PREFIX}task_options_{task_id}" # Modify query data to go back to options
        return await admin_task_options(update, context)
    else:
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'admin_task_error', lang_override=lang))
        return await admin_view_tasks(update, context) # Back to list on error

async def admin_delete_task_confirm(update: Update, context: CallbackContext) -> int:
    query = update.callback_query; await query.answer()
    user_id, lang = get_user_id_and_lang(update, context)
    
    task_id = None
    try: task_id = int(query.data.split(f"{CALLBACK_ADMIN_PREFIX}delete_task_confirm_")[1])
    except (IndexError, ValueError):
        log.error(f"Failed to parse task_id for delete confirm: {query.data}")
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'error_generic', lang_override=lang))
        return await admin_view_tasks(update, context)
    
    task = db.get_admin_task(task_id)
    if not task:
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'admin_task_not_found', lang_override=lang))
        return await admin_view_tasks(update, context)

    confirm_text = f"Are you sure you want to delete Task #{task_id}?\nBot: {task['userbot_phone']}\nTarget: {task['target']}"
    keyboard = [
        [InlineKeyboardButton(get_text(user_id, 'button_yes', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}delete_task_execute_{task_id}")],
        [InlineKeyboardButton(get_text(user_id, 'button_no', lang_override=lang), callback_data=f"{CALLBACK_ADMIN_PREFIX}task_options_{task_id}")] # Back to options
    ]
    markup = InlineKeyboardMarkup(keyboard)
    await _internal_send_or_edit_message(update, context, confirm_text, reply_markup=markup)
    return ConversationHandler.END # Waiting for CB (Yes/No)

async def admin_delete_task_execute(update: Update, context: CallbackContext) -> int:
    query = update.callback_query; await query.answer()
    user_id, lang = get_user_id_and_lang(update, context)
    
    task_id = None
    try: task_id = int(query.data.split(f"{CALLBACK_ADMIN_PREFIX}delete_task_execute_")[1])
    except (IndexError, ValueError):
        log.error(f"Failed to parse task_id for delete execute: {query.data}")
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'error_generic', lang_override=lang))
        return await admin_view_tasks(update, context)

    if db.delete_admin_task(task_id): # Sync
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'admin_task_deleted', lang_override=lang))
    else:
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'admin_task_error', lang_override=lang))
    
    return await admin_view_tasks(update, context) # Return to task list

# Helper function to handle errors in command handlers (already defined, kept for reference)
async def handle_command_error(update: Update, context: CallbackContext, error: Exception) -> int:
    """Helper function to handle errors in command handlers."""
    log.error(f"Error in command handler: {error}", exc_info=True)
    await async_error_handler(update, context) # Use the async error handler
    return ConversationHandler.END

# admin_confirm_task and admin_save_task for admin task creation via text input states
# (These are called by async_admin_process_task_target if it goes through a confirmation step)
# Assuming async_admin_process_task_target directly creates the task for now as per its current structure.
# If a separate confirmation step is added via CB, these handlers would be used.
