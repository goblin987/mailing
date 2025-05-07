# --- START OF FILE utils.py ---

from telegram import Update, ParseMode
from telegram.ext import CallbackContext
from telegram.error import BadRequest, RetryAfter

import html
import time
import asyncio

# Import necessary items from config and database
from config import log, CTX_USER_ID, CTX_LANG, CTX_MESSAGE_ID
import database as db
# Import translations function - ensure translations.py doesn't import utils to avoid circularity
from translations import get_text


def get_user_id_and_lang(update: Update, context: CallbackContext) -> tuple[int | None, str]:
    """
    Retrieves user_id and language code from context or DB.
    Sets them in context.user_data if found.
    Defaults language to 'en'.
    """
    user_id = None
    lang = None

    if context and hasattr(context, 'user_data') and isinstance(context.user_data, dict):
        user_id = context.user_data.get(CTX_USER_ID)
        lang = context.user_data.get(CTX_LANG)

    # If user_id not in context, try getting it from update
    if user_id is None and update and update.effective_user:
        user_id = update.effective_user.id
        if context and hasattr(context, 'user_data') and isinstance(context.user_data, dict):
            context.user_data[CTX_USER_ID] = user_id # Store it

    # If lang not in context, try getting from DB using user_id
    if lang is None and user_id is not None:
        try:
            lang = db.get_user_language(user_id) # Fetch from DB
            if context and hasattr(context, 'user_data') and isinstance(context.user_data, dict):
                context.user_data[CTX_LANG] = lang # Store it
        except Exception as e:
            log.error(f"Failed to get language for user {user_id} from DB in get_user_id_and_lang: {e}")
            lang = 'en' # Default on DB error

    # Default lang to 'en' if still None
    final_lang = lang if lang is not None else 'en'

    # Ensure context has the final determined language (might be redundant but safe)
    if context and hasattr(context, 'user_data') and isinstance(context.user_data, dict) and context.user_data.get(CTX_LANG) != final_lang:
         context.user_data[CTX_LANG] = final_lang

    return user_id, final_lang


def clear_conversation_data(context: CallbackContext):
    """
    Clears volatile conversation data from context.user_data,
    preserving essential keys like user_id, lang, and message_id.
    """
    if not context or not hasattr(context, 'user_data') or not isinstance(context.user_data, dict):
        log.warning("Attempted to clear conversation data with invalid context.")
        return

    user_id = context.user_data.get(CTX_USER_ID)
    lang = context.user_data.get(CTX_LANG)
    message_id = context.user_data.get(CTX_MESSAGE_ID) # Preserve message_id

    keys_to_keep = {CTX_USER_ID, CTX_LANG, CTX_MESSAGE_ID}

    # Iterate over a copy of keys because we're modifying the dictionary
    for key in list(context.user_data.keys()):
        if key not in keys_to_keep:
            context.user_data.pop(key, None)

    # Restore essential keys if they were somehow popped (shouldn't happen with above logic)
    if user_id is not None and CTX_USER_ID not in context.user_data :
        context.user_data[CTX_USER_ID] = user_id
    if lang is not None and CTX_LANG not in context.user_data:
        context.user_data[CTX_LANG] = lang
    if message_id is not None and CTX_MESSAGE_ID not in context.user_data:
        context.user_data[CTX_MESSAGE_ID] = message_id

    log.debug(f"Cleared volatile conversation user_data for user {user_id or 'N/A'}")


async def send_or_edit_message(update: Update, context: CallbackContext, text: str, **kwargs):
    """
    Send a new message or edit the existing one identified by context.user_data['_message_id'].
    Stores the new message_id if a new message is sent.
    Handles common errors like message not found for editing.
    """
    message = None
    chat_id = None
    message_id = None

    try:
        if update and update.effective_chat:
            chat_id = update.effective_chat.id
        if context and hasattr(context, 'user_data') and isinstance(context.user_data, dict):
            message_id = context.user_data.get(CTX_MESSAGE_ID)

        if not chat_id:
            log.error("[send_or_edit_message] No chat_id available.")
            return None

        parse_mode = kwargs.get('parse_mode', ParseMode.HTML) # Default to HTML
        reply_markup = kwargs.get('reply_markup', None)
        disable_web_page_preview=kwargs.get('disable_web_page_preview', True) # Default to true

        # Try editing if conditions are met
        can_edit = False
        if message_id and chat_id:
            # Prefer editing if it's a callback query update
            if update and update.callback_query:
                can_edit = True
            # Or if it's the same message triggering the edit (less common)
            # elif update and update.effective_message and update.effective_message.message_id == message_id:
            #    can_edit = True

        if can_edit:
            log.debug(f"[send_or_edit_message] Attempting to EDIT message {message_id} in chat {chat_id}")
            try:
                message = await context.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=message_id,
                    text=text,
                    parse_mode=parse_mode,
                    reply_markup=reply_markup,
                    disable_web_page_preview=disable_web_page_preview
                    # **kwargs # Pass other kwargs if needed, but explicit is safer
                )
                log.info(f"[send_or_edit_message] Successfully EDITED message {message_id}")
                # message_id remains the same, no need to update context
                return message
            except BadRequest as e:
                # Handle specific errors where editing isn't possible
                if 'message to edit not found' in str(e).lower() or \
                   'message_id_invalid' in str(e).lower() or \
                   'message can\'t be edited' in str(e).lower() or \
                   'chat not found' in str(e).lower():
                    log.warning(f"[send_or_edit_message] Failed to edit message {message_id} (will send new): {e}")
                    if context and hasattr(context, 'user_data') and isinstance(context.user_data, dict):
                        context.user_data.pop(CTX_MESSAGE_ID, None) # Clear invalid message_id
                    message = None # Ensure we send new
                else:
                    log.error(f"[send_or_edit_message] Unhandled BadRequest editing message {message_id}: {e}", exc_info=True)
                    raise # Re-raise other BadRequests
            except RetryAfter as e:
                 log.warning(f"[send_or_edit_message] RetryAfter editing message {message_id}: {e.retry_after}s. Sleeping.")
                 await asyncio.sleep(e.retry_after + 0.5)
                 # Retry editing (or fall through to sending) - for simplicity, let's fall through
                 if context and hasattr(context, 'user_data') and isinstance(context.user_data, dict):
                     context.user_data.pop(CTX_MESSAGE_ID, None)
                 message = None
            except Exception as e:
                log.warning(f"[send_or_edit_message] Generic error editing message {message_id} (will send new): {e}")
                if context and hasattr(context, 'user_data') and isinstance(context.user_data, dict):
                    context.user_data.pop(CTX_MESSAGE_ID, None)
                message = None # Ensure we send new

        # Send a new message if editing wasn't attempted or failed and was recoverable
        if message is None:
             log.debug(f"[send_or_edit_message] Attempting to SEND new message to chat {chat_id}")
             # Clear any previous message_id from context before sending new
             if context and hasattr(context, 'user_data') and isinstance(context.user_data, dict):
                 context.user_data.pop(CTX_MESSAGE_ID, None)
             try:
                 message = await context.bot.send_message(
                     chat_id=chat_id,
                     text=text,
                     parse_mode=parse_mode,
                     reply_markup=reply_markup,
                     disable_web_page_preview=disable_web_page_preview
                     # **kwargs
                 )
                 log.info(f"[send_or_edit_message] Successfully SENT new message. ID: {message.message_id if message else 'N/A'}")
                 # Store new message ID for potential future edits
                 if message and context and hasattr(context, 'user_data') and isinstance(context.user_data, dict):
                     context.user_data[CTX_MESSAGE_ID] = message.message_id
                     log.debug(f"[send_or_edit_message] Stored new message ID {message.message_id} in context.")
             except RetryAfter as e:
                 log.error(f"[send_or_edit_message] RetryAfter sending new message: {e.retry_after}s. NOT RETRYING.")
                 # Consider implementing retry logic if essential
                 return None
             except Exception as send_e:
                  log.error(f"[send_or_edit_message] Failed to SEND new message: {send_e}", exc_info=True)
                  return None # Indicate failure

        return message # Return the sent or edited message object, or None if sending failed

    except Exception as e:
        log.error(f"[send_or_edit_message] Unexpected error: {e}", exc_info=True)
        return None

# --- END OF FILE utils.py ---
