from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackContext, ConversationHandler

from config import (
    log, ADMIN_IDS, is_admin,
    STATE_ADMIN_TASK_MESSAGE, STATE_ADMIN_TASK_SCHEDULE, STATE_ADMIN_TASK_TARGET,
    CALLBACK_ADMIN_PREFIX
)
import database as db
from translations import get_text
from utils import get_user_id_and_lang, send_or_edit_message

def admin_process_task_message(update: Update, context: CallbackContext) -> int:
    user_id, lang = get_user_id_and_lang(update, context)
    if not is_admin(user_id):
        send_or_edit_message(update, context, get_text(user_id, 'unauthorized', lang=lang))
        return ConversationHandler.END
    
    # Store the message in context
    context.user_data['task_message'] = update.message.text
    
    # Ask for the schedule
    send_or_edit_message(update, context, get_text(user_id, 'admin_task_enter_schedule', lang=lang))
    return STATE_ADMIN_TASK_SCHEDULE

def admin_process_task_schedule(update: Update, context: CallbackContext) -> int:
    user_id, lang = get_user_id_and_lang(update, context)
    if not is_admin(user_id):
        send_or_edit_message(update, context, get_text(user_id, 'unauthorized', lang=lang))
        return ConversationHandler.END
    
    schedule = update.message.text.strip()
    # TODO: Validate cron format
    
    # Store the schedule in context
    context.user_data['task_schedule'] = schedule
    
    # Ask for the target
    send_or_edit_message(update, context, get_text(user_id, 'admin_task_enter_target', lang=lang))
    return STATE_ADMIN_TASK_TARGET

def admin_process_task_target(update: Update, context: CallbackContext) -> int:
    user_id, lang = get_user_id_and_lang(update, context)
    if not is_admin(user_id):
        send_or_edit_message(update, context, get_text(user_id, 'unauthorized', lang=lang))
        return ConversationHandler.END
    
    target = update.message.text.strip()
    # TODO: Validate target format
    
    # Create the task
    task_id = db.create_admin_task(
        userbot_phone=context.user_data['task_userbot_phone'],
        message=context.user_data['task_message'],
        schedule=context.user_data['task_schedule'],
        target=target,
        created_by=user_id
    )
    
    if task_id:
        send_or_edit_message(update, context, get_text(user_id, 'admin_task_created', lang=lang))
        # Clear task data
        context.user_data.pop('task_userbot_phone', None)
        context.user_data.pop('task_message', None)
        context.user_data.pop('task_schedule', None)
        # Return to admin menu
        from handlers import admin_command  # Import here to avoid circular import
        return admin_command(update, context)
    else:
        send_or_edit_message(update, context, get_text(user_id, 'admin_task_error', lang=lang))
        return ConversationHandler.END 
