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
    try:
        # Get the chat ID and message ID from context if available
        chat_id = update.effective_chat.id if update and update.effective_chat else None
        message_id = context.user_data.get('_message_id') if context and context.user_data else None
        
        if not chat_id:
            log.error("No chat_id available for sending/editing message")
            return None
        
        # Try to edit existing message if we have a message_id
        if message_id:
            try:
                message = await context.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=message_id,
                    text=text,
                    **kwargs
                )
                return message
            except Exception as e:
                log.warning(f"Could not edit message {message_id}: {e}")
                # Fall through to sending new message
        
        # Send new message
        message = await context.bot.send_message(
            chat_id=chat_id,
            text=text,
            **kwargs
        )
        
        # Store message ID in context for future edits
        if context and context.user_data is not None:
            context.user_data['_message_id'] = message.message_id
        
        return message
        
    except Exception as e:
        log.error(f"Error in send_or_edit_message: {e}", exc_info=True)
        # Try to send error message without any fancy features
        try:
            if chat_id:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text="Error sending message. Please try again."
                )
        except:
            pass
        return None 
