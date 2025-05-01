# handlers.py
import re
import uuid
from datetime import datetime, timedelta
import asyncio
import time
import random

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
    # States
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
    STATE_FOLDER_RENAME_PROMPT, STATE_ADMIN_CONFIRM_USERBOT_RESET,
    # Callback Prefixes
    CALLBACK_ADMIN_PREFIX, CALLBACK_CLIENT_PREFIX, CALLBACK_TASK_PREFIX,
    CALLBACK_FOLDER_PREFIX, CALLBACK_JOIN_PREFIX, CALLBACK_LANG_PREFIX,
    CALLBACK_REMOVE_PREFIX, CALLBACK_INTERVAL_PREFIX, CALLBACK_GENERIC_PREFIX
)
from translations import get_text, language_names, translations # Import full dict if needed

# --- Conversation Context Keys ---
# Using constants for context keys improves readability and reduces typos
CTX_USER_ID = "_user_id"
CTX_LANG = "_lang"
CTX_PHONE = "phone"
CTX_API_ID = "api_id"
CTX_API_HASH = "api_hash"
CTX_AUTH_DATA = "auth_data" # Stores temp data from start_authentication_flow
CTX_INVITE_DETAILS = "invite_details" # Tuple (days, bots)
CTX_EXTEND_CODE = "extend_code"
CTX_ADD_BOTS_CODE = "add_bots_code"
CTX_FOLDER_ID = "folder_id" # Used in folder management AND task setup target
CTX_FOLDER_NAME = "folder_name" # Used for display/prompts
CTX_FOLDER_ACTION = "folder_action" # e.g., 'add', 'update', 'remove_select'
CTX_SELECTED_BOTS = "selected_bots" # Phone number(s) selected for an action
CTX_TARGET_GROUP_IDS = "target_group_ids_to_remove" # For folder remove step
CTX_TASK_PHONE = "task_phone" # Phone of the bot whose task is being configured
CTX_TASK_SETTINGS = "task_settings" # Dictionary holding current task setup state

# --- Helper Functions ---

def clear_conversation_data(context: CallbackContext):
    """Clears sensitive or state-specific keys from user_data."""
    # List all context keys defined above, excluding persistent ones like CTX_USER_ID, CTX_LANG
    keys_to_clear = [
        CTX_PHONE, CTX_API_ID, CTX_API_HASH, CTX_AUTH_DATA, CTX_INVITE_DETAILS,
        CTX_EXTEND_CODE, CTX_ADD_BOTS_CODE, CTX_FOLDER_ID, CTX_FOLDER_NAME,
        CTX_FOLDER_ACTION, CTX_SELECTED_BOTS, CTX_TARGET_GROUP_IDS,
        CTX_TASK_PHONE, CTX_TASK_SETTINGS
    ]
    for key in keys_to_clear:
        context.user_data.pop(key, None)
    log.debug(f"Cleared volatile conversation user_data for user {context.user_data.get(CTX_USER_ID, 'N/A')}")

def get_user_id_and_lang(update: Update, context: CallbackContext) -> tuple:
     """Gets user ID and language, storing them in context if missing."""
     user_id = context.user_data.get(CTX_USER_ID)
     lang = context.user_data.get(CTX_LANG)
     if not user_id and update.effective_user:
          user_id = update.effective_user.id
          context.user_data[CTX_USER_ID] = user_id
     if user_id and not lang:
          lang = db.get_user_language(user_id)
          context.user_data[CTX_LANG] = lang
     elif not lang:
          lang = 'en' # Default if no user_id either
     return user_id, lang

def reply_or_edit_text(update: Update, context: CallbackContext, text: str, **kwargs):
     """Safely replies or edits a message, handling potential errors."""
     user_id, lang = get_user_id_and_lang(update, context)
     try:
          if update.callback_query:
               update.callback_query.edit_message_text(text=text, **kwargs)
          elif update.message:
               update.message.reply_text(text=text, **kwargs)
          else:
               # Cannot reply or edit, maybe log or try sending new message?
               log.warning(f"Cannot reply_or_edit_text for update type: {type(update)}")
               if user_id: context.bot.send_message(chat_id=user_id, text=text, **kwargs)
     except BadRequest as e:
          # Handle common errors like "message is not modified" or "message to edit not found"
          if "message is not modified" in str(e).lower():
               log.debug(f"Ignoring 'message is not modified' error for user {user_id}.")
               # Answer callback query if it wasn't answered yet (e.g., if it was a button toggle)
               if update.callback_query and not update.callback_query.answered:
                    update.callback_query.answer()
          elif "message to edit not found" in str(e).lower() or "chat not found" in str(e).lower():
                log.warning(f"Failed to edit message for user {user_id} (maybe deleted or chat issue): {e}")
                # Try sending as a new message instead
                if user_id: context.bot.send_message(chat_id=user_id, text=text, **kwargs)
          else:
                log.error(f"Error sending/editing message for user {user_id}: {e}", exc_info=True)
                # Try sending a generic error message as a fallback
                try:
                    context.bot.send_message(chat_id=user_id, text=get_text(user_id, 'error_generic', lang=lang))
                except Exception as send_e:
                     log.error(f"Failed to send fallback error message to user {user_id}: {send_e}")
     except Exception as e:
          log.error(f"Unexpected error in reply_or_edit_text for user {user_id}: {e}", exc_info=True)
          # Try sending a generic error message
          try:
               context.bot.send_message(chat_id=user_id, text=get_text(user_id, 'error_generic', lang=lang))
          except Exception as send_e:
               log.error(f"Failed to send fallback error message after unexpected error to user {user_id}: {send_e}")


# --- PTB Generic Error Handler ---
def error_handler(update: object, context: CallbackContext) -> None:
    """Log Errors caused by Updates and notify user."""
    log.error(f"Exception while handling an update:", exc_info=context.error)

    # Store traceback maybe? Be careful with size.
    # context.bot_data.setdefault('errors', []).append(traceback.format_exc())

    if isinstance(update, Update) and update.effective_message:
        user_id, lang = get_user_id_and_lang(update, context)
        # Send generic error message to the user
        reply_or_edit_text(update, context, get_text(user_id, 'error_generic', lang=lang))
        # End any active conversation safely
        # How to know if in conversation? Check state? Risky. Assume END is safe.
        clear_conversation_data(context)
        # We don't return ConversationHandler.END here as this is not a state handler.

# --- Command Handlers ---

# /start Command (Entry Point)
def start_command(update: Update, context: CallbackContext) -> str | int:
    """Handles the /start command: checks user status and directs them."""
    user = update.effective_user
    user_id, lang = get_user_id_and_lang(update, context) # Stores user_id/lang if new
    clear_conversation_data(context) # Always clear previous state on /start
    log.info(f"Start command received from UserID={user_id}, Username={user.username}")

    # Check if user is an active client
    client_info = db.find_client_by_user_id(user_id)
    if client_info:
        # Check subscription validity
        now_ts = int(datetime.now(UTC_TZ).timestamp())
        if client_info['subscription_end'] < now_ts:
            log.warning(f"Client {user_id} tried to start but subscription ended {format_dt(client_info['subscription_end'])}.")
            reply_or_edit_text(update, context, get_text(user_id, 'activation_expired', lang=lang))
            return ConversationHandler.END # End convo, expired
        else:
            log.info(f"User {user_id} is an active client. Building client menu.")
            return client_menu(update, context) # Show client menu (ends conversation)
    else:
        # User is not an active client, prompt for invitation code
        log.info(f"User {user_id} is not an active client. Prompting for invitation code.")
        reply_or_edit_text(update, context, get_text(user_id, 'welcome', lang=lang))
        return STATE_WAITING_FOR_CODE # Transition to code waiting state

# Handles the invitation code submission
def process_invitation_code(update: Update, context: CallbackContext) -> str | int:
    """Handles the user sending an invitation code."""
    user_id, lang = get_user_id_and_lang(update, context)
    code = update.message.text.strip()
    log.info(f"UserID={user_id} submitted invitation code: {code}")

    # Basic format check (e.g., uuid4[:8] is 8 hex chars)
    if not re.fullmatch(r'[a-f0-9]{8}', code, re.IGNORECASE):
        reply_or_edit_text(update, context, get_text(user_id, 'invalid_code_format', lang=lang))
        return STATE_WAITING_FOR_CODE # Stay in this state

    # Attempt activation
    success, status_key = db.activate_client(code, user_id)

    if success:
        log.info(f"Successfully activated client {user_id} with code {code}")
        db.log_event_db("Client Activated", f"Code: {code}", user_id=user_id)
        # Update language cache after activation potentially sets it
        context.user_data[CTX_LANG] = db.get_user_language(user_id)
        lang = context.user_data[CTX_LANG]
        reply_or_edit_text(update, context, get_text(user_id, 'activation_success', lang=lang))
        # Show client menu directly after success
        return client_menu(update, context)
    else:
        log.warning(f"Failed activation for user {user_id} with code {code}: Reason={status_key}")
        reply_or_edit_text(update, context, get_text(user_id, status_key, lang=lang)) # Show specific error
        # Should they get another chance? Or end conversation? End for now.
        clear_conversation_data(context)
        return ConversationHandler.END

# /admin Command (Entry Point for Admins)
def admin_command(update: Update, context: CallbackContext) -> str | int:
    """Handles the /admin command for authorized administrators."""
    user = update.effective_user
    user_id, lang = get_user_id_and_lang(update, context)
    clear_conversation_data(context)
    log.info(f"Admin command received from UserID={user_id}, Username={user.username}")

    if not is_admin(user_id):
        log.warning(f"Unauthorized admin access attempt by UserID={user_id}")
        reply_or_edit_text(update, context, get_text(user_id, 'unauthorized', lang=lang))
        return ConversationHandler.END # End if not admin

    # Build Admin Panel Keyboard
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
        # Maybe add folder management access for admins?
        # [InlineKeyboardButton(get_text(user_id, 'admin_button_manage_folders'), callback_data=f"{CALLBACK_ADMIN_PREFIX}manage_folders")],
    ]
    markup = InlineKeyboardMarkup(keyboard)
    reply_or_edit_text(update, context, get_text(user_id, 'admin_panel_title', lang=lang), reply_markup=markup)
    return ConversationHandler.END # Admin panel itself is stateless for now

# /cancel command handler
def cancel_command(update: Update, context: CallbackContext) -> int:
    """Generic cancel handler to exit conversations."""
    user = update.effective_user
    user_id, lang = get_user_id_and_lang(update, context)
    log.info(f"Cancel command received from UserID={user_id}")

    # Check if currently in a state that needs specific cleanup? Difficult to track robustly.
    # Rely on clear_conversation_data for general cleanup.
    clear_conversation_data(context)
    reply_or_edit_text(update, context, get_text(user_id, 'cancelled', lang=lang))

    # Attempt to determine if they were client or admin to show appropriate menu
    if is_admin(user_id):
         # Should ideally re-show admin panel. But return END for now.
         pass # Maybe send a message "Admin panel accessible via /admin"
    else:
        client_info = db.find_client_by_user_id(user_id)
        if client_info:
            # Re-show client menu? Requires checking subscription etc.
            # For simplicity, just end. User can use /start.
             pass # Maybe send "Client menu accessible via /start"

    return ConversationHandler.END

# --- Fallback Handler ---
def conversation_fallback(update: Update, context: CallbackContext) -> int:
     """Handles messages that don't match any state handlers in a conversation."""
     user = update.effective_user
     user_id, lang = get_user_id_and_lang(update, context)
     current_state = context.user_data.get(ConversationHandler.CURRENT_STATE) # Get current state if needed

     log.warning(f"Conversation fallback triggered for UserID={user_id}. Message: '{update.message.text[:50]}...' in state {current_state}")

     reply_or_edit_text(update, context, get_text(user_id, 'conversation_fallback', lang=lang))
     clear_conversation_data(context)
     return ConversationHandler.END


# --- Main Menu Builder (Called by /start or callbacks) ---
def client_menu(update: Update, context: CallbackContext) -> int:
    """Builds and sends the main client menu."""
    user_id, lang = get_user_id_and_lang(update, context)

    # If called via callback, answer it
    if update.callback_query:
        try:
            update.callback_query.answer()
        except BadRequest as e: # Handle if query expired etc.
            log.debug(f"Failed to answer client_menu callback query: {e}")

    # Build menu content
    message, markup = build_client_menu(user_id, context) # Uses helper from handlers_helpers.py (or define here)

    # Send/Edit the message
    reply_or_edit_text(update, context, message, reply_markup=markup, parse_mode=ParseMode.MARKDOWN_V2)

    clear_conversation_data(context) # Ensure clean state after showing menu
    return ConversationHandler.END # End the conversation


# --- Inline Button Handlers ---
# A single handler routes callbacks based on prefixes

def main_callback_handler(update: Update, context: CallbackContext) -> str | int | None:
    """Handles all Inline Keyboard Button presses."""
    query = update.callback_query
    user = update.effective_user
    user_id, lang = get_user_id_and_lang(update, context)
    data = query.data

    log.info(f"Callback received: UserID={user_id}, Data='{data}'")

    # --- Route based on prefix ---
    if data.startswith(CALLBACK_CLIENT_PREFIX):
        return handle_client_callback(update, context)
    elif data.startswith(CALLBACK_ADMIN_PREFIX):
         # Add admin check inside the handler
        return handle_admin_callback(update, context)
    elif data.startswith(CALLBACK_FOLDER_PREFIX):
         # Handles folder creation, selection, editing actions
        return handle_folder_callback(update, context)
    elif data.startswith(CALLBACK_TASK_PREFIX):
        # Handles task setup selections
        return handle_task_callback(update, context)
    elif data.startswith(CALLBACK_JOIN_PREFIX):
         # Handles userbot selection for joining
        return handle_join_callback(update, context)
    elif data.startswith(CALLBACK_LANG_PREFIX):
         # Handles language selection
         return handle_language_callback(update, context)
    elif data.startswith(CALLBACK_GENERIC_PREFIX):
        # Handles simple generic actions like 'cancel', 'confirm_yes/no' if needed
        return handle_generic_callback(update, context)
    else:
        log.warning(f"Unhandled callback data prefix from UserID={user_id}: '{data}'")
        query.answer("Unknown action.", show_alert=True)
        return ConversationHandler.END


# --- Specific Callback Routers (called by main_callback_handler) ---

def handle_client_callback(update: Update, context: CallbackContext) -> str | int | None:
    """Handles callbacks starting with CALLBACK_CLIENT_PREFIX."""
    query = update.callback_query
    user_id, lang = get_user_id_and_lang(update, context)
    data = query.data
    # No need to answer query again here if main_callback_handler does it?
    # query.answer() # Answer here ensures it's always answered for this prefix

    # Basic check: Is user still a valid client?
    client_info = db.find_client_by_user_id(user_id)
    if not client_info or client_info['subscription_end'] < int(datetime.now(UTC_TZ).timestamp()):
         query.answer(get_text(user_id, 'session_expired', lang=lang), show_alert=True)
         # Try to remove the expired message maybe?
         try: query.delete_message()
         except: pass
         return ConversationHandler.END

    action = data.split(CALLBACK_CLIENT_PREFIX)[1]

    if action == "setup_tasks":
        query.answer()
        return client_select_bot_for_task(update, context)
    elif action == "manage_folders":
        query.answer()
        return client_folder_menu(update, context)
    elif action == "join_groups":
        query.answer()
        return client_select_bot_for_join(update, context)
    elif action == "view_joined":
        query.answer()
        return client_select_bot_for_view_joined(update, context)
    elif action == "view_logs": # Renamed from view_stats
        query.answer()
        return client_show_stats(update, context)
    elif action == "language":
        query.answer()
        return client_ask_select_language(update, context)
    elif action == "back_to_menu":
        query.answer()
        return client_menu(update, context)
    elif data.startswith(CALLBACK_CLIENT_PREFIX + "view_joined_"): # Specific handler
        query.answer()
        return client_view_joined_groups(update, context)
    else:
        log.warning(f"Unhandled CLIENT callback action: '{action}'")
        query.answer(get_text(user_id, 'not_implemented', lang=lang))
        return None # Stay in current state or return END? Maybe None to indicate unhandled within handler.


def handle_admin_callback(update: Update, context: CallbackContext) -> str | int | None:
     """Handles callbacks starting with CALLBACK_ADMIN_PREFIX."""
     query = update.callback_query
     user_id, lang = get_user_id_and_lang(update, context)

     if not is_admin(user_id):
          query.answer(get_text(user_id, 'unauthorized', lang=lang), show_alert=True)
          return ConversationHandler.END

     data = query.data
     action = data.split(CALLBACK_ADMIN_PREFIX)[1]

     # --- Route Admin Actions ---
     if action == "add_bot_prompt":
          query.answer()
          reply_or_edit_text(update, context, get_text(user_id, 'admin_userbot_prompt_phone', lang=lang))
          return STATE_WAITING_FOR_PHONE
     elif action == "remove_bot_select":
          query.answer()
          return admin_select_userbot_to_remove(update, context)
     elif action == "list_bots":
          query.answer()
          return admin_list_userbots(update, context)
     elif action == "gen_invite_prompt":
          query.answer()
          reply_or_edit_text(update, context, get_text(user_id, 'admin_invite_prompt_details', lang=lang))
          return STATE_WAITING_FOR_SUB_DETAILS
     elif action == "view_subs":
          query.answer()
          return admin_view_subscriptions(update, context)
     elif action == "extend_sub_prompt":
          query.answer()
          reply_or_edit_text(update, context, get_text(user_id, 'admin_extend_prompt_code', lang=lang))
          return STATE_WAITING_FOR_EXTEND_CODE
     elif action == "assign_bots_prompt":
          query.answer()
          reply_or_edit_text(update, context, get_text(user_id, 'admin_assignbots_prompt_code', lang=lang))
          return STATE_WAITING_FOR_ADD_USERBOTS_CODE
     elif action == "view_logs":
          query.answer()
          return admin_view_system_logs(update, context)
     # --- Specific Action Callbacks ---
     elif data.startswith(CALLBACK_ADMIN_PREFIX + "remove_bot_confirm_"):
          query.answer()
          return admin_remove_userbot_confirmed(update, context)
     elif data.startswith(CALLBACK_GENERIC_PREFIX + "confirm_no"): # Generic 'No' button used in admin confirm
          query.answer(get_text(user_id, 'cancelled', lang=lang))
          # Return to admin panel after cancelling remove confirm?
          return admin_command(update, context) # Re-show panel
     else:
          log.warning(f"Unhandled ADMIN callback action: '{action}'")
          query.answer(get_text(user_id, 'not_implemented', lang=lang))
          return None


def handle_folder_callback(update: Update, context: CallbackContext) -> str | int | None:
     """Handles callbacks starting with CALLBACK_FOLDER_PREFIX."""
     query = update.callback_query
     user_id, lang = get_user_id_and_lang(update, context)
     data = query.data
     # query.answer() # Answer within specific handlers

     action = data.split(CALLBACK_FOLDER_PREFIX)[1] if CALLBACK_FOLDER_PREFIX in data else None

     # --- Route Folder Actions ---
     # Folder Menu Options
     if action == "create_prompt":
          query.answer()
          reply_or_edit_text(update, context, get_text(user_id, 'folder_create_prompt', lang=lang))
          return STATE_WAITING_FOR_FOLDER_NAME
     elif action == "edit_select":
          query.answer()
          return client_select_folder_to_edit(update, context)
     # Add "delete_select" handler later if needed

     # Selecting a folder for edit
     elif data.startswith(CALLBACK_FOLDER_PREFIX + "edit_folder_"):
          query.answer()
          folder_id = int(data.split("_")[-1])
          context.user_data[CTX_FOLDER_ID] = folder_id
          return client_show_folder_edit_options(update, context)

     # Folder edit options
     elif action == "edit_action_add":
          query.answer()
          return client_prompt_folder_add_links(update, context)
     elif action == "edit_action_update":
          query.answer()
          return client_prompt_folder_update_links(update, context)
     elif action == "edit_action_remove_select":
         query.answer()
         return client_select_groups_to_remove(update, context)
     elif action == "edit_action_rename":
          query.answer()
          return client_prompt_folder_rename(update, context)
     elif action == "edit_action_delete_confirm": # Confirmation step
          query.answer()
          return client_confirm_folder_delete(update, context)

     # Remove group action
     elif data.startswith(CALLBACK_FOLDER_PREFIX + "remove_group_confirm"): # Confirm removing groups
          query.answer()
          return client_remove_selected_groups(update, context)
     elif data.startswith(CALLBACK_FOLDER_PREFIX + "remove_groups_page_"): # Pagination for remove list
         query.answer()
         page = int(data.split("_")[-1])
         return client_select_groups_to_remove(update, context, page=page)
     elif data.startswith(CALLBACK_FOLDER_PREFIX + "toggle_remove_"): # Toggle group selection
         query.answer()
         db_id = int(data.split("_")[-1])
         page = context.user_data.get('_remove_page', 0)
         # Toggle selection logic
         selected = context.user_data.get(CTX_TARGET_GROUP_IDS, set())
         if db_id in selected: selected.remove(db_id)
         else: selected.add(db_id)
         context.user_data[CTX_TARGET_GROUP_IDS] = selected
         # Re-render the selection message
         return client_select_groups_to_remove(update, context, page=page)

     # Delete folder confirmation
     elif data.startswith(CALLBACK_GENERIC_PREFIX + "confirm_yes_delete_folder_"):
          query.answer()
          folder_id = int(data.split("_")[-1])
          context.user_data[CTX_FOLDER_ID] = folder_id # Ensure context has correct ID
          return client_delete_folder_confirmed(update, context)

     # Back buttons within folder flow
     elif action == "back_to_folder_edit":
         query.answer()
         # Needs folder_id in context
         return client_show_folder_edit_options(update, context)
     elif action == "back_to_folder_menu":
          query.answer()
          return client_folder_menu(update, context)

     else:
          log.warning(f"Unhandled FOLDER callback action: '{action}' / '{data}'")
          query.answer(get_text(user_id, 'not_implemented', lang=lang))
          return None


def handle_task_callback(update: Update, context: CallbackContext) -> str | int | None:
     """Handles callbacks related to task setup (CALLBACK_TASK_PREFIX)."""
     query = update.callback_query
     user_id, lang = get_user_id_and_lang(update, context)
     data = query.data
     # Ensure task phone context exists if needed
     task_phone = context.user_data.get(CTX_TASK_PHONE)

     # Callback for selecting userbot to configure task for
     if data.startswith(CALLBACK_TASK_PREFIX + "select_"):
          query.answer()
          phone = data.split(CALLBACK_TASK_PREFIX + "select_")[1]
          context.user_data[CTX_TASK_PHONE] = phone
          context.user_data[CTX_TASK_SETTINGS] = db.get_userbot_task_settings(user_id, phone) or {} # Load or init empty dict
          return task_show_settings_menu(update, context)

     # --- Task Setup Menu Actions ---
     # Ensure task_phone is set before proceeding with these
     if not task_phone:
          query.answer(get_text(user_id, 'session_expired', lang=lang), show_alert=True)
          return ConversationHandler.END

     action = data.split(CALLBACK_TASK_PREFIX)[1] if CALLBACK_TASK_PREFIX in data else None

     if action == "set_message":
          query.answer()
          reply_or_edit_text(update, context, get_text(user_id, 'task_prompt_primary_link', lang=lang),
                             reply_markup=task_back_button_markup(user_id, context))
          return STATE_WAITING_FOR_PRIMARY_MESSAGE_LINK
     elif action == "set_time":
          query.answer()
          reply_or_edit_text(update, context, get_text(user_id, 'task_prompt_start_time', lang=lang),
                             reply_markup=task_back_button_markup(user_id, context))
          return STATE_WAITING_FOR_START_TIME
     elif action == "set_interval":
          query.answer()
          return task_select_interval(update, context)
     elif action == "set_target":
          query.answer()
          return task_select_target_type(update, context)
     elif action == "toggle_status":
          query.answer()
          return task_toggle_status(update, context)
     elif action == "save":
          query.answer()
          return task_save_settings(update, context)
     elif action == "cancel":
          query.answer()
          clear_conversation_data(context) # Clear task setup state
          reply_or_edit_text(update, context, get_text(user_id, 'task_cancel_confirm', lang=lang))
          return client_menu(update, context) # Back to main client menu

     # --- Task Target Selection Callbacks ---
     elif action == "target_all_groups":
          query.answer()
          # Update context, then show main task menu again
          settings = context.user_data.get(CTX_TASK_SETTINGS, {})
          settings['send_to_all_groups'] = 1
          settings['folder_id'] = None # Mutually exclusive
          context.user_data[CTX_TASK_SETTINGS] = settings
          reply_or_edit_text(update, context, get_text(user_id, 'task_set_success_target_all', lang=lang))
          time.sleep(1) # Pause briefly to show confirmation
          return task_show_settings_menu(update, context)
     elif action == "target_select_folder":
          query.answer()
          return task_select_folder_for_target(update, context)
     elif data.startswith(CALLBACK_TASK_PREFIX + "set_folder_target_"):
          query.answer()
          folder_id = int(data.split("_")[-1])
          # Update context, then show main task menu again
          settings = context.user_data.get(CTX_TASK_SETTINGS, {})
          settings['folder_id'] = folder_id
          settings['send_to_all_groups'] = 0 # Mutually exclusive
          context.user_data[CTX_TASK_SETTINGS] = settings
          folder_name = db.get_folder_name(folder_id) or f"ID {folder_id}"
          reply_or_edit_text(update, context, get_text(user_id, 'task_set_success_target_folder', lang=lang, folder_name=folder_name))
          time.sleep(1) # Pause briefly
          return task_show_settings_menu(update, context)

     # Back buttons within task setup
     elif action == "back_to_task_menu":
          query.answer()
          return task_show_settings_menu(update, context)
     elif action == "back_to_target_type":
          query.answer()
          return task_select_target_type(update, context)

     # Interval selection handled by handle_interval_callback
     elif data.startswith(CALLBACK_INTERVAL_PREFIX):
          return handle_interval_callback(update, context)

     else:
          log.warning(f"Unhandled TASK callback action: '{action}' / '{data}'")
          query.answer(get_text(user_id, 'not_implemented', lang=lang))
          return None

def handle_join_callback(update: Update, context: CallbackContext) -> str | int | None:
     """Handles callbacks starting with CALLBACK_JOIN_PREFIX."""
     query = update.callback_query
     user_id, lang = get_user_id_and_lang(update, context)
     data = query.data

     if data.startswith(CALLBACK_JOIN_PREFIX + "select_"):
          query.answer()
          return handle_userbot_selection_for_join(update, context)
     else:
          log.warning(f"Unhandled JOIN callback action: '{data}'")
          query.answer(get_text(user_id, 'not_implemented', lang=lang))
          return None

def handle_language_callback(update: Update, context: CallbackContext) -> str | int | None:
     """Handles language selection callbacks (CALLBACK_LANG_PREFIX)."""
     query = update.callback_query
     user_id, lang = get_user_id_and_lang(update, context)
     data = query.data

     if data.startswith(CALLBACK_LANG_PREFIX):
          # query.answer() # Answer within specific handler
          return set_language_handler(update, context)
     else:
          log.warning(f"Unhandled LANG callback action: '{data}'")
          query.answer(get_text(user_id, 'not_implemented', lang=lang))
          return None

def handle_generic_callback(update: Update, context: CallbackContext) -> str | int | None:
     """Handles simple generic callbacks like cancel, back, yes/no."""
     query = update.callback_query
     user_id, lang = get_user_id_and_lang(update, context)
     data = query.data

     action = data.split(CALLBACK_GENERIC_PREFIX)[1] if CALLBACK_GENERIC_PREFIX in data else None

     if action == "cancel":
          query.answer(get_text(user_id, 'cancelled', lang=lang))
          clear_conversation_data(context)
          # Need context to decide where to go back to (client menu? admin menu?)
          # Best is often to just end.
          # Let's try removing the message for cancellation.
          try: query.delete_message()
          except: pass
          return ConversationHandler.END
     # Add other generic handlers like confirm_yes/no if needed by flows

     else:
          log.warning(f"Unhandled GENERIC callback action: '{action}' / '{data}'")
          query.answer(get_text(user_id, 'not_implemented', lang=lang))
          return None


# --- Message Handlers (Within Conversations) ---

# Add functions here for handling text messages in specific states:
# - process_admin_phone
# - process_admin_api_id
# - process_admin_api_hash
# - process_admin_userbot_code
# - process_admin_userbot_password
# - process_admin_invite_details
# - process_admin_extend_code
# - process_admin_extend_days
# - process_admin_add_bots_code
# - process_admin_add_bots_count
# - process_folder_name
# - process_folder_add_links / process_folder_update_links
# - process_folder_rename
# - process_join_group_links # Used by client join flow
# - process_task_primary_link
# - process_task_fallback_link
# - process_task_start_time


# --- Example Message Handler: Waiting for Admin Phone ---
def process_admin_phone(update: Update, context: CallbackContext) -> str | int:
     """Handles admin entering a phone number for a new userbot."""
     user_id, lang = get_user_id_and_lang(update, context)
     phone_raw = update.message.text.strip()
     # Basic validation (starts with +, digits, length)
     if not re.fullmatch(r'\+\d{9,15}', phone_raw):
          reply_or_edit_text(update, context, get_text(user_id, 'admin_userbot_invalid_phone', lang=lang))
          return STATE_WAITING_FOR_PHONE # Ask again

     phone = phone_raw
     context.user_data[CTX_PHONE] = phone
     log.info(f"Admin {user_id} entered phone: {phone}")

     # Check if bot already exists
     existing_bot = db.find_userbot(phone)
     if existing_bot:
          reply_or_edit_text(update, context, get_text(user_id, 'admin_userbot_already_exists', lang=lang, phone=phone))
          # Ask for confirmation before proceeding? Leads to another state.
          # For now, just proceed to API ID/Hash entry, which will trigger re-auth.
          # Later: Add confirm state STATE_ADMIN_CONFIRM_USERBOT_RESET

     # Ask for API ID
     reply_or_edit_text(update, context, get_text(user_id, 'admin_userbot_prompt_api_id', lang=lang))
     return STATE_WAITING_FOR_API_ID

# --- Placeholder implementations for other state handlers ---
# Replace these with actual logic based on the original script

# TODO: Implement process_admin_api_id
# TODO: Implement process_admin_api_hash -> calls telethon_api.start_authentication_flow
# TODO: Implement process_admin_userbot_code -> calls telethon_api.complete_authentication_flow
# TODO: Implement process_admin_userbot_password -> calls telethon_api.complete_authentication_flow
# TODO: Implement process_admin_invite_details
# TODO: Implement process_admin_extend_code
# TODO: Implement process_admin_extend_days
# TODO: Implement process_admin_add_bots_code
# TODO: Implement process_admin_add_bots_count
# TODO: Implement process_folder_name -> Adds folder, asks for links? Or just returns?
# TODO: Implement process_folder_links (handle add/update based on CTX_FOLDER_ACTION) -> calls db add/remove group funcs
# TODO: Implement process_folder_rename
# TODO: Implement process_join_group_links -> calls telethon_api.join_groups_batch
# TODO: Implement process_task_primary_link -> Validates link? Stores in context. Asks fallback.
# TODO: Implement process_task_fallback_link -> Stores in context. Back to menu.
# TODO: Implement process_task_start_time -> Parses time, stores ts in context. Back to menu.


# --- Conversation Handler Setup ---
# Define your conversation handler(s) here, mapping commands/messages/callbacks to functions/states

# Example structure for a single main conversation handler
main_conversation = ConversationHandler(
    entry_points=[
        CommandHandler('start', start_command),
        CommandHandler('admin', admin_command),
        # CallbackQueryHandler to handle buttons pressed when NOT in a conversation state
        # Needs careful pattern matching to avoid hijacking stateful callbacks
        # CallbackQueryHandler(main_callback_handler, pattern=f"^(?!" + "|".join([CALLBACK_TASK_PREFIX, CALLBACK_FOLDER_PREFIX,...]) + ")") # Complex regex
        # Simpler: Let main_callback_handler be the fallback for non-state callbacks if needed?
        # Or handle ALL callbacks via main_callback_handler and have it return None if state should persist.
        CallbackQueryHandler(main_callback_handler) # Route ALL callbacks here initially
    ],
    states={
        # Initial Activation State
        STATE_WAITING_FOR_CODE: [MessageHandler(Filters.text & ~Filters.command, process_invitation_code)],

        # Admin Add Userbot Flow
        STATE_WAITING_FOR_PHONE: [MessageHandler(Filters.text & ~Filters.command, process_admin_phone)],
        STATE_WAITING_FOR_API_ID: [MessageHandler(Filters.text & ~Filters.command, process_admin_api_id)], # Needs implementation
        STATE_WAITING_FOR_API_HASH: [MessageHandler(Filters.text & ~Filters.command, process_admin_api_hash)], # Needs implementation
        STATE_WAITING_FOR_CODE_USERBOT: [MessageHandler(Filters.text & ~Filters.command, process_admin_userbot_code)], # Needs implementation
        STATE_WAITING_FOR_PASSWORD: [MessageHandler(Filters.text & ~Filters.command, process_admin_userbot_password)], # Needs implementation

        # Admin Invite/Subscription Flows
        STATE_WAITING_FOR_SUB_DETAILS: [MessageHandler(Filters.text & ~Filters.command, process_admin_invite_details)], # Needs implementation
        STATE_WAITING_FOR_EXTEND_CODE: [MessageHandler(Filters.text & ~Filters.command, process_admin_extend_code)], # Needs implementation
        STATE_WAITING_FOR_EXTEND_DAYS: [MessageHandler(Filters.text & ~Filters.command, process_admin_extend_days)], # Needs implementation
        STATE_WAITING_FOR_ADD_USERBOTS_CODE: [MessageHandler(Filters.text & ~Filters.command, process_admin_add_bots_code)], # Needs implementation
        STATE_WAITING_FOR_ADD_USERBOTS_COUNT: [MessageHandler(Filters.text & ~Filters.command, process_admin_add_bots_count)], # Needs implementation

        # Client Join Groups Flow
        STATE_WAITING_FOR_USERBOT_SELECTION: [CallbackQueryHandler(main_callback_handler)], # Reuse main handler
        STATE_WAITING_FOR_GROUP_LINKS: [MessageHandler(Filters.text & ~Filters.command, process_join_group_links)], # Needs implementation

        # Client Folder Management Flow
        STATE_WAITING_FOR_FOLDER_ACTION: [CallbackQueryHandler(main_callback_handler)], # Select create/edit/delete
        STATE_WAITING_FOR_FOLDER_NAME: [MessageHandler(Filters.text & ~Filters.command, process_folder_name)], # Needs implementation
        STATE_WAITING_FOR_FOLDER_SELECTION: [CallbackQueryHandler(main_callback_handler)], # Select folder for edit/delete
        # STATE_WAITING_FOR_GROUP_LINKS -> Reuse for folder add/update actions (triggered by callback)
        STATE_FOLDER_EDIT_REMOVE_SELECT: [CallbackQueryHandler(main_callback_handler)], # Handle group removal UI
        STATE_FOLDER_RENAME_PROMPT: [MessageHandler(Filters.text & ~Filters.command, process_folder_rename)], # Needs implementation

        # Client Task Setup Flow
        # STATE_WAITING_FOR_USERBOT_SELECTION -> Handled by initial task callback
        STATE_TASK_SETUP: [CallbackQueryHandler(main_callback_handler)], # Handles button presses in task menu
        STATE_WAITING_FOR_PRIMARY_MESSAGE_LINK: [MessageHandler(Filters.text & ~Filters.command, process_task_primary_link)], # Needs implementation
        STATE_WAITING_FOR_FALLBACK_MESSAGE_LINK: [MessageHandler(Filters.text & ~Filters.command, process_task_fallback_link)], # Needs implementation
        STATE_WAITING_FOR_START_TIME: [MessageHandler(Filters.text & ~Filters.command, process_task_start_time)], # Needs implementation
        STATE_SELECT_TARGET_GROUPS: [CallbackQueryHandler(main_callback_handler)], # Handles selecting all/folder for task
        # STATE_WAITING_FOR_FOLDER_SELECTION -> Reuse for task folder target

    },
    fallbacks=[
        CommandHandler('cancel', cancel_command), # Allow cancellation
        # CallbackQueryHandler(main_callback_handler), # Let main handler route fallbacks too? Might conflict.
        # MessageHandler(Filters.text & ~Filters.command, conversation_fallback) # Catch unexpected text
    ],
    # Optional: Add conversation timeout
    # conversation_timeout=timedelta(minutes=30).total_seconds(),
    # Optional: Make conversation persistent across bot restarts (requires pickling context)
    # persistent=True, name="main_conversation_persistence" # Needs careful handling of context data types
)


log.info("Handlers module loaded.")

# Define handler implementations below this line...
# ... (implement all the process_* and client/admin action functions) ...
# Make sure they correctly return the next state or ConversationHandler.END
# Use reply_or_edit_text for sending messages/updating callbacks.
# Use clear_conversation_data before returning ConversationHandler.END from states.
# Use get_user_id_and_lang at the start of handlers.