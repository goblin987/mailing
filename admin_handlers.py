# --- START OF FILE admin_handlers.py ---

import html
import re # Added for basic validation if needed

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackContext, ConversationHandler

# Import shared constants and DB from their respective modules
from config import (
    log, ADMIN_IDS, is_admin,
    STATE_ADMIN_TASK_MESSAGE, STATE_ADMIN_TASK_SCHEDULE, STATE_ADMIN_TASK_TARGET,
    STATE_ADMIN_TASK_CONFIRM, # Keep if confirmation step is used
    CALLBACK_ADMIN_PREFIX,
    # Import necessary context keys
    CTX_TASK_BOT, CTX_TASK_MESSAGE, CTX_TASK_SCHEDULE, CTX_TASK_TARGET,
    CTX_TASK_TARGET_TYPE, CTX_TASK_TARGET_FOLDER
)
import database as db
from translations import get_text

# Import shared helpers from utils.py
from utils import (
    get_user_id_and_lang,
    send_or_edit_message,
    clear_conversation_data
)
# DO NOT import anything directly from handlers.py here

# --- Admin Task Creation State Handlers ---

async def admin_process_task_message(update: Update, context: CallbackContext) -> int:
    """Handles receiving the message/link for an admin task."""
    user_id, lang = get_user_id_and_lang(update, context)
    # No admin check needed if entry point is already admin protected

    message_text = update.message.text
    # Basic validation: check if message is empty
    if not message_text or message_text.isspace():
        await send_or_edit_message(update, context, get_text(user_id, 'admin_task_invalid_link', lang_override=lang)) # Use a relevant error message key
        return STATE_ADMIN_TASK_MESSAGE # Re-ask

    context.user_data[CTX_TASK_MESSAGE] = message_text
    log.info(f"Admin Task: Stored message for user {user_id}: {message_text[:50]}...")

    # Ask for the schedule
    await send_or_edit_message(update, context, get_text(user_id, 'admin_task_enter_schedule', lang_override=lang))
    return STATE_ADMIN_TASK_SCHEDULE

async def admin_process_task_schedule(update: Update, context: CallbackContext) -> int:
    """Handles receiving the schedule (cron string) for an admin task."""
    user_id, lang = get_user_id_and_lang(update, context)

    schedule_text = update.message.text.strip()
    # TODO: Add robust Cron format validation here using a library if possible
    # Example basic check (very rudimentary):
    parts = schedule_text.split()
    if len(parts) != 5:
        await send_or_edit_message(update, context, get_text(user_id, 'admin_task_invalid_schedule', lang_override=lang))
        return STATE_ADMIN_TASK_SCHEDULE # Re-ask

    context.user_data[CTX_TASK_SCHEDULE] = schedule_text
    log.info(f"Admin Task: Stored schedule for user {user_id}: {schedule_text}")

    # Ask for the target
    await send_or_edit_message(update, context, get_text(user_id, 'admin_task_enter_target', lang_override=lang))
    return STATE_ADMIN_TASK_TARGET

async def admin_process_task_target(update: Update, context: CallbackContext) -> int:
    """Handles receiving the target, creates the task, and ends the flow."""
    user_id, lang = get_user_id_and_lang(update, context)

    target_text = update.message.text.strip()
    # TODO: Add target validation (e.g., check if it's a valid ID or @username format)
    if not target_text:
         await send_or_edit_message(update, context, get_text(user_id, 'admin_task_invalid_target', lang_override=lang))
         return STATE_ADMIN_TASK_TARGET # Re-ask

    # We should have all pieces now: Bot, Message, Schedule, Target
    bot_phone = context.user_data.get(CTX_TASK_BOT)
    message = context.user_data.get(CTX_TASK_MESSAGE)
    schedule = context.user_data.get(CTX_TASK_SCHEDULE)

    if not all([bot_phone, message, schedule]):
        log.error(f"Admin Task Creation: Missing context data for user {user_id}. Bot: {bot_phone}, Msg: {message}, Sched: {schedule}")
        await send_or_edit_message(update, context, get_text(user_id, 'session_expired', lang_override=lang))
        clear_conversation_data(context) # Clear potentially partial data
        return ConversationHandler.END

    # Create the task in DB
    task_id = db.create_admin_task(
        userbot_phone=bot_phone,
        message=message, # Store the message content/link directly
        schedule=schedule, # Store cron string
        target=target_text, # Store target string (username or ID)
        created_by=user_id
        # Status defaults to inactive in DB schema
    )

    if task_id:
        await send_or_edit_message(update, context, get_text(user_id, 'admin_task_created', lang_override=lang))
        log.info(f"Admin task {task_id} created by user {user_id}.")
        db.log_event_db("Admin Task Created", f"TaskID: {task_id}, Target: {target_text}", user_id=user_id, userbot_phone=bot_phone)

    else:
        await send_or_edit_message(update, context, get_text(user_id, 'admin_task_error', lang_override=lang))
        db.log_event_db("Admin Task Creation Failed", f"Target: {target_text}", user_id=user_id, userbot_phone=bot_phone)

    # Always clear specific task data and end the conversation flow here
    context.user_data.pop(CTX_TASK_BOT, None)
    context.user_data.pop(CTX_TASK_MESSAGE, None)
    context.user_data.pop(CTX_TASK_SCHEDULE, None)
    context.user_data.pop(CTX_TASK_TARGET, None) # Clear target as well

    # Don't return admin_command - just end this specific flow.
    # The user can navigate back to the main admin menu if needed.
    return ConversationHandler.END
# --- END OF FILE admin_handlers.py ---
