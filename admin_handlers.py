# --- START OF FILE admin_handlers.py ---

import html

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackContext, ConversationHandler

from config import (
    log, ADMIN_IDS, is_admin,
    STATE_ADMIN_TASK_MESSAGE, STATE_ADMIN_TASK_SCHEDULE, STATE_ADMIN_TASK_TARGET,
    STATE_ADMIN_TASK_CONFIRM, # Added if confirmation step needed
    CALLBACK_ADMIN_PREFIX
)
import database as db
from translations import get_text
# Import context keys from handlers (assuming they are defined there)
# If they are defined in config, import from config instead.
from handlers import (
    CTX_TASK_BOT, CTX_TASK_MESSAGE, CTX_TASK_SCHEDULE, CTX_TASK_TARGET,
    CTX_TASK_TARGET_TYPE, CTX_TASK_TARGET_FOLDER,
    _internal_send_or_edit_message, # Use the internal helper from handlers
    get_user_id_and_lang, # Use the helper from handlers/utils
    clear_conversation_data, # Use helper
    admin_command # Import to potentially return to admin menu state
)

# IMPORTANT: These functions are now ASYNC as handlers.py expects them to be.

async def admin_process_task_message(update: Update, context: CallbackContext) -> int:
    user_id, lang = get_user_id_and_lang(update, context)
    # No need for admin check if entry point is already admin protected
    
    # Store the message in context using the standard key
    message_text = update.message.text
    context.user_data[CTX_TASK_MESSAGE] = message_text
    log.info(f"Admin Task: Stored message for user {user_id}: {message_text[:50]}...")
    
    # Ask for the schedule (Assuming schedule selection is next via text/CB)
    # For simplicity, let's assume schedule is entered via text next
    await _internal_send_or_edit_message(update, context, get_text(user_id, 'admin_task_enter_schedule', lang_override=lang))
    return STATE_ADMIN_TASK_SCHEDULE

async def admin_process_task_schedule(update: Update, context: CallbackContext) -> int:
    user_id, lang = get_user_id_and_lang(update, context)
    
    schedule_text = update.message.text.strip()
    # TODO: Add robust Cron format validation here
    if not schedule_text: # Basic check
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'admin_task_invalid_schedule', lang_override=lang))
        return STATE_ADMIN_TASK_SCHEDULE # Re-ask

    # Store the schedule in context
    context.user_data[CTX_TASK_SCHEDULE] = schedule_text
    log.info(f"Admin Task: Stored schedule for user {user_id}: {schedule_text}")
    
    # Ask for the target
    await _internal_send_or_edit_message(update, context, get_text(user_id, 'admin_task_enter_target', lang_override=lang))
    return STATE_ADMIN_TASK_TARGET

async def admin_process_task_target(update: Update, context: CallbackContext) -> int:
    user_id, lang = get_user_id_and_lang(update, context)
    
    target_text = update.message.text.strip()
    # TODO: Add target validation (e.g., check if it's a valid ID or @username format)
    if not target_text:
         await _internal_send_or_edit_message(update, context, get_text(user_id, 'admin_task_invalid_target', lang_override=lang))
         return STATE_ADMIN_TASK_TARGET # Re-ask

    # We have all pieces: Bot, Message, Schedule, Target
    bot_phone = context.user_data.get(CTX_TASK_BOT)
    message = context.user_data.get(CTX_TASK_MESSAGE)
    schedule = context.user_data.get(CTX_TASK_SCHEDULE)
    
    if not all([bot_phone, message, schedule]):
        log.error(f"Admin Task Creation: Missing context data for user {user_id}. Bot: {bot_phone}, Msg: {message}, Sched: {schedule}")
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'session_expired', lang_override=lang))
        clear_conversation_data(context)
        return ConversationHandler.END

    # Create the task in DB
    task_id = db.create_admin_task(
        userbot_phone=bot_phone,
        message=message, # Store the message content directly
        schedule=schedule, # Store cron string
        target=target_text, # Store target string
        created_by=user_id
    )
    
    if task_id:
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'admin_task_created', lang_override=lang))
        log.info(f"Admin task {task_id} created by user {user_id}.")
        db.log_event_db("Admin Task Created", f"TaskID: {task_id}, Target: {target_text}", user_id=user_id, userbot_phone=bot_phone)

        # Clear specific task data from context
        context.user_data.pop(CTX_TASK_BOT, None)
        context.user_data.pop(CTX_TASK_MESSAGE, None)
        context.user_data.pop(CTX_TASK_SCHEDULE, None)
        # Keep user_id, lang, message_id
        
        # Return to admin menu
        return await admin_command(update, context) # This correctly shows the admin menu and ends the conversation.
    else:
        await _internal_send_or_edit_message(update, context, get_text(user_id, 'admin_task_error', lang_override=lang))
        db.log_event_db("Admin Task Creation Failed", f"Target: {target_text}", user_id=user_id, userbot_phone=bot_phone)
        # Don't clear data on error? Or clear? Let's clear for now.
        context.user_data.pop(CTX_TASK_BOT, None)
        context.user_data.pop(CTX_TASK_MESSAGE, None)
        context.user_data.pop(CTX_TASK_SCHEDULE, None)
        return ConversationHandler.END # End conversation on error
# --- END OF FILE admin_handlers.py ---
