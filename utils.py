from telegram import Update, ParseMode
from telegram.ext import CallbackContext
from telegram.error import BadRequest, RetryAfter

import html
import time
import asyncio

from config import log
import database as db
from translations import get_text

def get_user_id_and_lang(update: Update, context: CallbackContext) -> tuple[int | None, str]:
    user_id = context.user_data.get('_user_id')
    lang = context.user_data.get('_lang')

    if user_id is None:
        if update and update.effective_user:
            user_id = update.effective_user.id
            context.user_data['_user_id'] = user_id 
    
    if lang is None: 
        if user_id is not None: 
            lang = db.get_user_language(user_id)
            context.user_data['_lang'] = lang 
        else:
            lang = 'en'
    
    final_lang = lang if lang is not None else 'en'

    if user_id is not None and context.user_data.get('_lang') != final_lang:
        context.user_data['_lang'] = final_lang
            
    return user_id, final_lang

async def _show_menu_async(update: Update, context: CallbackContext, menu_builder_func):
    """
    Asynchronously shows a menu using the provided menu builder function.
    """
    user_id, lang = get_user_id_and_lang(update, context)
    title, markup, parse_mode = menu_builder_func(user_id, context)
    await send_or_edit_message(update, context, title, reply_markup=markup, parse_mode=parse_mode)

async def send_or_edit_message(update: Update, context: CallbackContext, text: str, **kwargs):
    """Send a new message or edit the existing one based on context."""
    log.debug(f"[send_or_edit_message] ENTERED. Target Text: {text[:50]}... Kwargs: {kwargs}")
    message = None # Initialize message variable
    try:
        chat_id = update.effective_chat.id if update and update.effective_chat else None
        message_id = context.user_data.get('_message_id') if context and context.user_data else None
        log.debug(f"[send_or_edit_message] ChatID: {chat_id}, Existing MessageID: {message_id}")
        
        if not chat_id:
            log.error("[send_or_edit_message] No chat_id available. Cannot send/edit.")
            return None
        
        if message_id:
            log.debug(f"[send_or_edit_message] Attempting to EDIT message {message_id} in chat {chat_id}")
            try:
                message = await context.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=message_id,
                    text=text,
                    **kwargs
                )
                log.info(f"[send_or_edit_message] Successfully EDITED message {message_id}")
                return message
            except BadRequest as e:
                log.warning(f"[send_or_edit_message] BadRequest editing message {message_id}: {e}. Will try sending new.")
                # Clear invalid message_id
                if 'message to edit not found' in str(e) or 'message identifier not specified' in str(e):
                     if context and context.user_data: context.user_data.pop('_message_id', None)
                message_id = None # Force sending new message
            except Exception as e:
                log.warning(f"[send_or_edit_message] Generic error editing message {message_id}: {e}. Will try sending new.")
                if context and context.user_data: context.user_data.pop('_message_id', None)
                message_id = None # Force sending new message
        
        # If no message_id or editing failed, send a new message
        if not message:
             log.debug(f"[send_or_edit_message] Attempting to SEND new message to chat {chat_id}")
             try:
                 message = await context.bot.send_message(
                     chat_id=chat_id,
                     text=text,
                     **kwargs
                 )
                 log.info(f"[send_or_edit_message] Successfully SENT new message. ID: {message.message_id if message else 'N/A'}")
                 # Store new message ID for potential future edits
                 if message and context and context.user_data is not None:
                     context.user_data['_message_id'] = message.message_id
                     log.debug(f"[send_or_edit_message] Stored new message ID {message.message_id} in context.")
             except Exception as send_e:
                  log.error(f"[send_or_edit_message] Failed to SEND new message: {send_e}", exc_info=True)
                  # Don't try to send another error message from here, avoid loops.
                  return None # Indicate failure
        
        return message # Return the sent or edited message object, or None if sending failed
        
    except Exception as e:
        log.error(f"[send_or_edit_message] Unexpected error OUTSIDE send/edit block: {e}", exc_info=True)
        return None 
