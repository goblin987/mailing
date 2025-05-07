from telegram import Update, ParseMode
from telegram.ext import CallbackContext
from telegram.error import BadRequest, RetryAfter

import html
import time

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

def send_or_edit_message(update: Update, context: CallbackContext, text: str, **kwargs):
    user_id, lang = get_user_id_and_lang(update, context)
    log.info(f"send_or_edit_message: START - User: {user_id}, Lang: {lang}, Text: '{html.escape(text[:70])}...', Kwargs: {kwargs}")

    parse_mode = kwargs.get('parse_mode', ParseMode.HTML)
    kwargs['parse_mode'] = parse_mode

    chat_id = None
    if update and update.effective_chat:
        chat_id = update.effective_chat.id
    elif user_id: 
        chat_id = user_id
    
    log.debug(f"send_or_edit_message: Determined chat_id: {chat_id}")

    if not chat_id:
        log.error(f"send_or_edit_message: CRITICAL - Cannot determine chat_id. User ID: {user_id}. Update provided: {update is not None}")
        return

    message_id_to_edit = None
    if update.callback_query:
        message_id_to_edit = update.callback_query.message.message_id
        # Answer callback query to remove loading state
        try:
            update.callback_query.answer()
        except Exception as e:
            log.warning(f"Failed to answer callback query: {e}")

    log.debug(f"send_or_edit_message: Final message_id_to_edit (for editing attempts): {message_id_to_edit}")

    try:
        if message_id_to_edit:
            # Try to edit existing message
            log.info(f"send_or_edit_message: Attempting to EDIT message {message_id_to_edit} in chat {chat_id}")
            try:
                result = context.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=message_id_to_edit,
                    text=text,
                    **kwargs
                )
                log.info(f"send_or_edit_message: Successfully EDITED message {message_id_to_edit}")
                return result
            except BadRequest as e:
                if "message is not modified" in str(e).lower():
                    log.debug(f"Message {message_id_to_edit} not modified (content unchanged)")
                    return None
                log.warning(f"Failed to edit message {message_id_to_edit}: {e}")
                # Fall through to sending new message
        
        # If we have an incoming message to reply to, use reply
        if update.message:
            log.info(f"send_or_edit_message: Attempting to REPLY to incoming message {update.message.message_id} in chat {chat_id}")
            result = context.bot.send_message(
                chat_id=chat_id,
                text=text,
                reply_to_message_id=update.message.message_id,
                **kwargs
            )
            log.info(f"send_or_edit_message: Successfully REPLIED with new message {result.message_id}")
            return result
        
        # Otherwise send a new message
        log.info(f"send_or_edit_message: Attempting to SEND new message to chat {chat_id}")
        result = context.bot.send_message(
            chat_id=chat_id,
            text=text,
            **kwargs
        )
        log.info(f"send_or_edit_message: Successfully SENT new message {result.message_id}")
        return result

    except RetryAfter as e:
        log.warning(f"Rate limit hit when sending message to {chat_id}: {e}")
        time.sleep(e.retry_after)
        return send_or_edit_message(update, context, text, **kwargs)  # Retry once after waiting
    except Exception as e:
        log.error(f"Error sending/editing message to {chat_id}: {e}", exc_info=True)
        return None 

async def _show_menu_async(update: Update, context: CallbackContext, menu_builder_func):
    """
    Asynchronously shows a menu using the provided menu builder function.
    """
    user_id, lang = get_user_id_and_lang(update, context)
    title, markup, parse_mode = menu_builder_func(user_id, context)
    await _send_or_edit_message(update, context, title, reply_markup=markup, parse_mode=parse_mode)

async def _send_or_edit_message(update: Update, context: CallbackContext, text: str, **kwargs):
    """
    Asynchronous version of send_or_edit_message.
    """
    return await context.bot.run_async(send_or_edit_message, update, context, text, **kwargs) 
