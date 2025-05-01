# telethon_utils.py
import asyncio
import threading
import os
import random
import re
import time
from datetime import datetime
import sqlite3 # Import sqlite3 for catching its specific errors if needed in this file

from telethon import TelegramClient
# Specific imports for clarity and potentially better type hinting
from telethon.tl.types import (
    PeerChannel, PeerChat, PeerUser, InputPeerChannel, InputPeerChat, InputPeerUser,
    Channel, User as TelethonUser, Chat as TelethonChat
)
# Corrected error imports based on Telethon v1.36.0 / common usage
from telethon.errors import (
    SessionPasswordNeededError, FloodWaitError, ChatSendMediaForbiddenError,
    UserNotParticipantError, ChatAdminRequiredError, # UserBannedInChatError REMOVED
    PhoneNumberInvalidError, PhoneCodeInvalidError, PhoneCodeExpiredError,
    PasswordHashInvalidError, ApiIdInvalidError, AuthKeyError, # TelethonConnectionError REMOVED
    UserDeactivatedBanError, UsernameNotOccupiedError, ChannelPrivateError, InviteHashExpiredError, InviteHashInvalidError,
    MessageIdInvalidError, PeerIdInvalidError, UserBlockedError, ChatWriteForbiddenError
    # Note: We will catch the standard Python 'ConnectionError' for network issues.
)
# Import RPC functions used
from telethon.tl.functions.channels import JoinChannelRequest, GetFullChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest, GetMessagesRequest
from telethon.tl.functions.account import GetPasswordRequest

import database as db
from config import (
    SESSION_DIR, CLIENT_TIMEOUT, CHECK_TASKS_INTERVAL, UTC_TZ, log # Use logger from config
)
from translations import get_text # Only for very specific internal errors if needed

# --- Userbot Runtime Management ---
_userbots = {}
_userbots_lock = threading.Lock()
_stop_event = threading.Event()

# --- Helper Functions ---

def _get_session_path(phone):
    """Returns the absolute path for a userbot's session file."""
    safe_phone = re.sub(r'[^\d\+]', '', phone)
    return os.path.join(SESSION_DIR, f"{safe_phone}.session")

async def _create_telethon_client_instance(session_path, api_id, api_hash, loop):
    """Creates and returns a configured Telethon client instance."""
    log.debug(f"Creating TelethonClient for session: {session_path}")
    client = TelegramClient(
        session_path,
        api_id,
        api_hash,
        timeout=CLIENT_TIMEOUT,
        loop=loop,
        device_model="PC 64bit",
        system_version="Linux",
        app_version="1.0.0"
    )
    return client

def _run_loop(loop, phone_for_log):
    """Target function for the userbot's dedicated event loop thread."""
    asyncio.set_event_loop(loop)
    thread_name = f"UserBotLoop-{phone_for_log}"
    threading.current_thread().name = thread_name
    log.info(f"Event loop thread started: {thread_name}")
    try:
        loop.run_forever()
    except Exception as e:
        log.error(f"Exception in event loop for {phone_for_log}: {e}", exc_info=True)
    finally:
        log.info(f"Event loop stopping for {phone_for_log}...")
        try:
             if loop.is_running():
                  loop.run_until_complete(loop.shutdown_asyncgens())
             loop.close()
             log.info(f"Event loop closed cleanly for {phone_for_log}.")
        except Exception as close_e:
             log.error(f"Error closing event loop for {phone_for_log}: {close_e}")

async def _safe_connect(client: TelegramClient, phone: str) -> bool:
    """Connects the client if not connected, handling common errors and updating DB status."""
    if client.is_connected():
        return True

    log.info(f"Connecting userbot {phone}...")
    db.update_userbot_status(phone, 'connecting')
    try:
        await client.connect()
        if await client.is_user_authorized():
             log.info(f"Userbot {phone} connected and authorized.")
             return True
        else:
             log.error(f"Userbot {phone} connected but NOT authorized. Session invalid?")
             db.update_userbot_status(phone, 'error', last_error="Session invalid - Not Authorized")
             await _safe_disconnect(client, phone, update_db=False)
             return False
    except AuthKeyError:
        log.error(f"Authentication key error for {phone}. Session invalid. Deleting.")
        db.update_userbot_status(phone, 'error', last_error="Invalid session (AuthKeyError)")
        await _delete_session_file(phone)
        return False
    except (ConnectionError, asyncio.TimeoutError, OSError) as e: # Catch built-in ConnectionError now
        log.error(f"Connection failed for {phone}: {e}")
        db.update_userbot_status(phone, 'error', last_error=f"Connection Error: {type(e).__name__}")
        return False
    except Exception as e:
        log.exception(f"Unexpected error connecting userbot {phone}: {e}", exc_info=True)
        db.update_userbot_status(phone, 'error', last_error=f"Unexpected connect error: {type(e).__name__}")
        return False

async def _safe_disconnect(client: TelegramClient, phone: str, update_db: bool = True):
    """Disconnects the client gracefully."""
    if client and client.is_connected():
        log.info(f"Disconnecting userbot {phone}...")
        try:
            await client.disconnect()
            log.info(f"Userbot {phone} disconnected.")
        except Exception as e:
            log.error(f"Error during graceful disconnect for userbot {phone}: {e}")

async def _delete_session_file(phone):
     """Deletes the session file and its journal."""
     session_path = _get_session_path(phone)
     journal_path = f"{session_path}-journal"
     deleted = False
     try:
         if os.path.exists(session_path):
             os.remove(session_path)
             log.info(f"Deleted session file: {session_path}")
             deleted = True
         if os.path.exists(journal_path):
             os.remove(journal_path)
             log.info(f"Deleted session journal file: {journal_path}")
     except OSError as e:
         log.error(f"Failed to delete session file(s) for {phone}: {e}")
     return deleted

# --- Public Userbot Runtime Functions ---

def get_userbot_runtime_info(phone_number):
     """
     Gets the runtime info (client, loop, lock, thread) for a userbot.
     Initializes runtime if it doesn't exist for a bot found in the DB.
     Returns the runtime dict or None if initialization fails or bot not in DB.
     """
     with _userbots_lock:
        if phone_number in _userbots:
            bot_info = _userbots[phone_number]
            if bot_info.get('thread') and bot_info['thread'].is_alive():
                log.debug(f"Returning existing runtime for {phone_number}")
                return bot_info
            else:
                log.warning(f"Thread for userbot {phone_number} found dead. Cleaning up.")
                loop = bot_info.get('loop')
                if loop and loop.is_running():
                     loop.call_soon_threadsafe(loop.stop)
                del _userbots[phone_number]

        log.info(f"Attempting to initialize runtime for userbot {phone_number}...")
        userbot_db = db.find_userbot(phone_number)
        if not userbot_db:
            log.error(f"Userbot {phone_number} not found in database for runtime initialization.")
            return None

        if userbot_db['status'] == 'inactive':
            log.warning(f"Skipping runtime initialization for inactive userbot {phone_number}.")
            return None

        session_file = _get_session_path(phone_number)
        api_id = userbot_db['api_id']
        api_hash = userbot_db['api_hash']

        try:
            loop = asyncio.new_event_loop()
            lock = asyncio.Lock()

            async def _create_client_task():
                return await _create_telethon_client_instance(session_file, api_id, api_hash, loop)
            future = asyncio.run_coroutine_threadsafe(_create_client_task(), loop)

            thread = threading.Thread(target=_run_loop, args=(loop, phone_number), daemon=True)
            thread.start()

            client = future.result(timeout=CLIENT_TIMEOUT)
            log.info(f"Telethon client created for {phone_number}")

            bot_info = {'client': client, 'loop': loop, 'lock': lock, 'thread': thread}
            _userbots[phone_number] = bot_info
            db.update_userbot_status(phone_number, 'initializing')

            async def _initial_connect_check():
                 connected = await _safe_connect(client, phone_number)
                 if connected:
                      log.info(f"Initial connection check successful for {phone_number}")
                      try:
                          me = await client.get_me()
                          db.update_userbot_status(phone_number, 'active', username=me.username if me else None)
                      except Exception as get_me_err:
                           log.error(f"Failed to get_me after connect for {phone_number}: {get_me_err}")
                           db.update_userbot_status(phone_number, 'error', last_error=f"get_me failed: {type(get_me_err).__name__}")
                 else:
                      log.warning(f"Initial connection check failed for {phone_number}. Status updated by _safe_connect.")

            asyncio.run_coroutine_threadsafe(_initial_connect_check(), loop)

            log.info(f"Runtime initialized for userbot {phone_number}. Thread started.")
            return bot_info

        except Exception as e:
            log.critical(f"CRITICAL: Failed to initialize runtime for userbot {phone_number}: {e}", exc_info=True)
            db.update_userbot_status(phone_number, 'error', last_error=f"Runtime init failed: {type(e).__name__}")
            if 'loop' in locals() and loop.is_running(): loop.call_soon_threadsafe(loop.stop)
            if 'thread' in locals() and thread.is_alive(): thread.join(timeout=2)
            if phone_number in _userbots: del _userbots[phone_number]
            return None

def stop_userbot_runtime(phone_number):
    """Stops the event loop and disconnects a specific userbot."""
    with _userbots_lock:
         bot_info = _userbots.pop(phone_number, None)

    if bot_info:
        log.info(f"Stopping runtime for userbot {phone_number}...")
        client = bot_info.get('client')
        loop = bot_info.get('loop')
        thread = bot_info.get('thread')

        if client and loop and loop.is_running():
            future = asyncio.run_coroutine_threadsafe(_safe_disconnect(client, phone_number), loop)
            try: future.result(timeout=CLIENT_TIMEOUT / 2)
            except Exception as e: log.warning(f"Error during disconnect future for {phone_number} on stop: {e}")

        if loop and loop.is_running(): loop.call_soon_threadsafe(loop.stop)

        if thread and thread.is_alive():
            log.debug(f"Waiting for thread {thread.name} to finish...")
            thread.join(timeout=5)
            if thread.is_alive(): log.warning(f"Thread {thread.name} did not stop gracefully.")
            else: log.info(f"Thread {thread.name} stopped.")
        return True
    else:
        log.warning(f"Tried to stop runtime for {phone_number}, but it wasn't running.")
        return False


# --- Authentication Flow ---

async def start_authentication_flow(phone, api_id, api_hash):
    """
    Initiates the Telethon authentication process using a TEMPORARY client/loop.
    Returns: Tuple (status, data) as described before.
    """
    session_file = _get_session_path(phone)
    await _delete_session_file(phone) # Ensure clean state

    temp_loop = asyncio.new_event_loop()
    temp_client = None
    auth_result_status = 'error'
    auth_result_data = "Initialization failed"

    try:
        temp_client = await _create_telethon_client_instance(session_file, api_id, api_hash, temp_loop)
        log.info(f"Attempting connection for authentication: {phone}")
        code_request_info = await temp_client.send_code_request(phone)
        log.info(f"Code request sent to {phone}. Phone code hash obtained.")
        auth_result_status = 'code_needed'
        auth_result_data = {'client': temp_client, 'loop': temp_loop, 'phone_code_hash': code_request_info.phone_code_hash}

    except SessionPasswordNeededError:
         log.warning(f"Password needed immediately for {phone} (code step skipped).")
         try:
              if not temp_client: temp_client = await _create_telethon_client_instance(session_file, api_id, api_hash, temp_loop)
              if not temp_client.is_connected(): await temp_client.connect()
              pwd_state = await temp_client(GetPasswordRequest())
              log.info(f"Password required for {phone}. Hint: {getattr(pwd_state, 'hint', 'None')}")
              auth_result_status = 'password_needed'
              auth_result_data = {'client': temp_client, 'loop': temp_loop, 'pwd_state': pwd_state}
         except Exception as pwd_err: log.error(f"Error getting password state for {phone}: {pwd_err}"); auth_result_status = 'error'; auth_result_data = f"Failed to get password details: {pwd_err}"
    except FloodWaitError as e: log.warning(f"Flood wait code request {phone}: {e.seconds}s"); auth_result_status = 'error'; auth_result_data = f"Flood wait: Try again in {e.seconds} seconds."
    except (PhoneNumberInvalidError, ApiIdInvalidError, ApiIdPublishedFloodError) as e: log.error(f"Config/Phone error code request {phone}: {e}"); auth_result_status = 'error'; auth_result_data = f"Invalid config/phone: {e}"
    except AuthKeyError: log.error(f"AuthKeyError initial code request {phone}."); auth_result_status = 'error'; auth_result_data = "Auth key error. Please try again."
    except (ConnectionError, asyncio.TimeoutError, OSError) as e: log.error(f"Connection issue code request {phone}: {e}"); auth_result_status = 'error'; auth_result_data = f"Connection failed: {e}" # Catch built-in ConnectionError
    except Exception as e: log.exception(f"Unexpected error auth start {phone}: {e}"); auth_result_status = 'error'; auth_result_data = f"Unexpected error: {e}"

    if auth_result_status == 'error':
        await _safe_disconnect(temp_client, phone, update_db=False)
        if temp_loop and not temp_loop.is_closed():
            try: temp_loop.close(); log.debug("Temp auth loop closed on error.")
            except Exception as loop_e: log.error(f"Error closing temp loop for {phone}: {loop_e}")
        auth_result_data = {'error_message': str(auth_result_data)}

    return auth_result_status, auth_result_data

async def complete_authentication_flow(auth_data, code=None, password=None):
    """
    Completes Telethon auth using temp client/loop from start_authentication_flow.
    Returns: Tuple (status, data) as described before.
    """
    temp_client = auth_data.get('client')
    temp_loop = auth_data.get('loop')
    phone = "UnknownPhone"
    final_status = 'error'
    final_data = "Unknown auth failure"

    if not temp_client or not temp_loop: return 'error', {'error_message': "Invalid auth data (client/loop missing)."}

    me_object = None
    try:
        if code:
            phone_code_hash = auth_data.get('phone_code_hash')
            if not phone_code_hash: return 'error', {'error_message': "Internal Error: Missing phone_code_hash."}
            log.info(f"Attempting code sign-in...")
            me_object = await temp_client.sign_in(code=code, phone_code_hash=phone_code_hash)
        elif password:
            log.info(f"Attempting password sign-in...")
            me_object = await temp_client.sign_in(password=password)
        else: return 'error', {'error_message': "No code or password provided."}

        if me_object and await temp_client.is_user_authorized():
            phone = me_object.phone; username = me_object.username
            session_file_path = temp_client.session.filename
            api_id = temp_client.api_id; api_hash = temp_client.api_hash
            session_file_rel = os.path.relpath(session_file_path, SESSION_DIR)

            log.info(f"Auth successful for {phone} (@{username}) via temp client.")
            db_add_ok = db.add_userbot(phone, session_file_rel, api_id, api_hash, 'active', username, None, None)
            if db_add_ok: final_status = 'success'; final_data = {'phone': phone, 'username': username}; log.info(f"Userbot {phone} info saved to DB."); get_userbot_runtime_info(phone) # Init persistent runtime
            else: final_status = 'error'; final_data = "Auth success, but DB save failed."; log.error(final_data)
        else: final_status = 'error'; final_data = "Sign-in completed but authorization failed."; log.error(f"Auth check failed for {phone}")

    except SessionPasswordNeededError: final_status = 'error'; final_data = "Password required. Restart process."
    except (PhoneCodeInvalidError, PhoneCodeExpiredError): final_status = 'error'; final_data = "Invalid or expired code."
    except PasswordHashInvalidError: final_status = 'error'; final_data = "Incorrect password."
    except FloodWaitError as e: final_status = 'error'; final_data = f"Flood wait: Try again in {e.seconds} seconds."
    except AuthKeyError: final_status = 'error'; final_data = "Auth key error. Session corrupt?"; await _delete_session_file(phone)
    except UserDeactivatedBanError as e: final_status = 'error'; final_data = f"Account issue: Banned/Deactivated."; db.update_userbot_status(phone, 'error', last_error="Account Banned/Deactivated")
    except (ConnectionError, asyncio.TimeoutError, OSError) as e: final_status = 'error'; final_data = f"Connection failed: {e}" # Catch built-in ConnectionError
    except Exception as e: log.exception(f"Unexpected error auth completion {phone}: {e}"); final_status = 'error'; final_data = f"Unexpected error: {e}"

    log.debug(f"Cleaning up temporary auth resources for {phone} (Status: {final_status}).")
    await _safe_disconnect(temp_client, phone, update_db=False)
    if temp_loop and not temp_loop.is_closed():
        try: temp_loop.close(); log.debug(f"Temp auth loop {phone} closed.")
        except Exception as loop_e: log.error(f"Error closing temp loop {phone}: {loop_e}")

    if final_status == 'error' and isinstance(final_data, str): final_data = {'error_message': final_data} # Standardize error

    return final_status, final_data


# --- Other Telethon Actions ---

def parse_telegram_url_simple(url: str) -> tuple:
    """Simplified URL parsing for join flow. Returns (type, identifier)."""
    url = url.strip()
    if match := re.match(r"https?://t\.me/\+([\w\d_-]+)/?", url): return "private_join", match.group(1)
    if match := re.match(r"https?://t\.me/joinchat/([\w\d_-]+)/?", url): return "private_join", match.group(1)
    if match := re.match(r"https?://t\.me/([\w\d_]{5,})/?$", url): return "public", match.group(1)
    if "/c/" in url or re.match(r'https?://t\.me/\w+/\d+', url): return "message_link", url
    return "unknown", url

def _format_entity_detail(entity) -> dict:
    """Formats Telethon entity into a standard detail dictionary."""
    if not entity: return {}
    return {
        "id": entity.id,
        "name": getattr(entity, 'title', getattr(entity, 'username', f"ID {entity.id}")),
        "username": getattr(entity, 'username', None)
    }

async def join_groups_batch(phone, urls):
    """
    Manages joining multiple Telegram groups/channels for a specified userbot.
    Returns: Tuple (error_info, results_dict)
    """
    runtime_info = get_userbot_runtime_info(phone)
    if not runtime_info: return {"error": "Userbot runtime not available."}, {}
    client, loop, lock = runtime_info['client'], runtime_info['loop'], runtime_info['lock']
    bot_display = phone

    async def _join_batch_task():
        results = {}; nonlocal bot_display
        async with lock:
            try:
                if not await _safe_connect(client, phone): raise ConnectionError("Failed connection")
                try: me = await client.get_me(); bot_display = f"@{me.username}" if me.username else phone
                except: pass
                log.info(f"{bot_display}: Start join {len(urls)} URLs.")
                for i, url in enumerate(urls):
                    if not url: continue
                    status, detail = "failed", "Unknown"; start_time = time.monotonic(); entity = None
                    try:
                        link_type, identifier = parse_telegram_url_simple(url)
                        if link_type == "unknown": raise ValueError("Unrecognized URL")
                        if link_type == "message_link": raise ValueError("Cannot join msg link")

                        if link_type == "private_join":
                            log.info(f"{bot_display}: Join invite: {identifier}"); updates = await client(ImportChatInviteRequest(identifier))
                            if updates and updates.chats: entity = updates.chats[0]; status, detail = "success", _format_entity_detail(entity)
                            else: raise InviteHashInvalidError("Import empty.")
                        elif link_type == "public":
                            log.info(f"{bot_display}: Resolve public: {identifier}"); entity = await client.get_entity(identifier)
                            if not isinstance(entity, (Channel, TelethonChat)): raise ValueError("Link->user")
                            log.info(f"{bot_display}: Join public: {getattr(entity,'title','N/A')}"); await client(JoinChannelRequest(entity)); status, detail = "success", _format_entity_detail(entity)
                        else: raise ValueError("Unsupported link type")

                    except (InviteHashExpiredError, InviteHashInvalidError): status, detail = "failed", {"reason": "invalid_invite"}
                    except ChannelPrivateError: status, detail = "failed", {"reason": "private"}
                    except ChatWriteForbiddenError: status, detail = "failed", {"reason": "banned_or_restricted"}; log.warning(f"{bot_display}: ChatWriteForbidden {url}")
                    except UserNotParticipantError: status, detail = "already_member", _format_entity_detail(entity) if entity else {"reason": "already_member_unresolved"}
                    except ChatAdminRequiredError: status, detail = "pending", {"reason": "admin_approval"}
                    except FloodWaitError as e: wait = min(e.seconds + random.uniform(1, 3), 90); log.warning(f"{bot_display}: Flood join {url} ({e.seconds}s). Wait {wait:.1f}s."); await asyncio.sleep(wait); status, detail = "flood_wait", {"seconds": e.seconds}
                    except ValueError as e: status, detail = "failed", {"reason": f"invalid_link_or_resolve: {e}"}
                    except (AuthKeyError, ConnectionError, asyncio.TimeoutError, OSError) as e: log.error(f"{bot_display}: Conn/Auth err join {url}: {e}. Abort."); db.update_userbot_status(phone, 'error', f"Conn/Auth Err: {type(e).__name__}"); raise ConnectionError(f"Conn/Auth fail: {e}") # Catch built-in ConnErr
                    except UserDeactivatedBanError as e: log.critical(f"{bot_display}: BANNED join {url}: {e}. Abort."); db.update_userbot_status(phone, 'error', "Account Banned"); raise e
                    except Exception as e: log.exception(f"{bot_display}: Unexpected err join {url}: {e}"); status, detail = "failed", {"reason": f"internal_error: {e}"}

                    results[url] = (status, detail); log.info(f"{bot_display}: Result {url}: {status} ({(time.monotonic()-start_time):.2f}s)")
                    if i < len(urls) - 1: delay = max(0.5, 3.0 + random.uniform(-1.0, 1.0)); await asyncio.sleep(delay)

            except ConnectionError as e: return {"error": f"Connection Error: {e}"}, results
            except UserDeactivatedBanError as e: return {"error": "Userbot Account Banned/Deactivated"}, results
            except Exception as e: log.exception(f"{bot_display}: Error in _join_batch_task: {e}"); return {"error": f"Batch Error: {e}"}, results
            log.info(f"{bot_display}: Finished join batch task."); return {}, results # No fatal error

    if not loop or loop.is_closed(): return {"error": "Userbot event loop unavailable."}, {}
    future = asyncio.run_coroutine_threadsafe(_join_batch_task(), loop)
    try:
        timeout = (len(urls) * (CLIENT_TIMEOUT / 2 + 5)) + 30; error_info, results_dict = future.result(timeout=timeout); return error_info, results_dict
    except asyncio.TimeoutError: log.error(f"Timeout join batch {phone}."); final_results = locals().get('results_dict', {}); [final_results.setdefault(url, ("failed", {"reason": "batch_timeout"})) for url in urls if url not in final_results]; return {"error": "Batch join timeout."}, final_results
    except Exception as e: log.exception(f"Error join batch future {phone}: {e}"); final_results = locals().get('results_dict', {}); [final_results.setdefault(url, ("failed", {"reason": f"internal_error: {e}"})) for url in urls if url not in final_results]; error_info = {"error": str(e)} if not locals().get('error_info') else locals()['error_info']; return error_info, final_results

# --- Get Joined Groups ---
async def get_joined_chats_telethon(phone):
    """Retrieves joined chats (groups/channels) for a userbot."""
    runtime_info = get_userbot_runtime_info(phone)
    if not runtime_info: return None, {"error": "Userbot runtime not available."}
    client, loop, lock = runtime_info['client'], runtime_info['loop'], runtime_info['lock']
    bot_display = phone

    async def _get_dialogs_task():
        dialog_list = []; error_msg = None; nonlocal bot_display
        async with lock:
            try:
                if not await _safe_connect(client, phone): raise ConnectionError("Failed connection")
                try: me = await client.get_me(); bot_display = f"@{me.username}" if me.username else phone
                except: pass
                log.info(f"{bot_display}: Fetching dialogs (limit 500)..."); dialog_count = 0
                async for dialog in client.iter_dialogs(limit=500):
                    dialog_count += 1;
                    if dialog.is_group or dialog.is_channel:
                        entity = dialog.entity; link = None; id_part = None; username = getattr(entity, 'username', None)
                        if username: link = f"https://t.me/{username}"
                        elif isinstance(entity, Channel): id_part_str = str(entity.id);
                        if id_part_str.startswith('-100'): id_part = id_part_str[4:]; link = f"https://t.me/c/{id_part}/1"
                        dialog_list.append({ 'id': entity.id, 'name': getattr(entity, 'title', f'ID: {entity.id}'), 'username': username, 'link': link, 'type': 'channel' if dialog.is_channel else 'group'})
                log.info(f"{bot_display}: Fetched {dialog_count} dialogs, found {len(dialog_list)} groups/channels.")
            except ConnectionError as e: error_msg = f"Connection Error: {e}" # Catch built-in ConnectionError
            except AuthKeyError: error_msg = "Invalid session."; db.update_userbot_status(phone, 'error', "Invalid session")
            except UserDeactivatedBanError: error_msg = "Account Banned/Deactivated."; db.update_userbot_status(phone, 'error', error_msg)
            except FloodWaitError as e: error_msg = f"Flood wait ({e.seconds}s)"
            except Exception as e: log.exception(f"{bot_display}: Error fetching dialogs: {e}"); error_msg = f"Unexpected error: {e}"
            return dialog_list, {"error": error_msg} if error_msg else {}

    if not loop or loop.is_closed(): return None, {"error": "Userbot loop unavailable."}
    future = asyncio.run_coroutine_threadsafe(_get_dialogs_task(), loop)
    try: dialogs, error_info = future.result(timeout=120); return dialogs, error_info
    except asyncio.TimeoutError: return None, {"error": "Timeout fetching dialogs."}
    except Exception as e: return None, {"error": f"Internal task error: {e}"}


# --- Message Link Parsing ---
async def get_message_entity_by_link(client, link):
    """Parses message links and returns (source_chat_entity, message_id). Raises errors."""
    link = link.strip(); log.debug(f"Parsing message link: {link}")
    private_match = re.match(r'https?://t\.me/c/(\d+)/(\d+)', link)
    public_match = re.match(r'https?://t\.me/([\w\d_]+)/(\d+)', link)
    chat_identifier = None; message_id = None; link_type = 'unknown'

    if private_match: link_type = 'private'; chat_identifier = int(f"-100{private_match.group(1)}"); message_id = int(private_match.group(2))
    elif public_match: link_type = 'public'; chat_identifier = public_match.group(1); message_id = int(public_match.group(2))
    else: raise ValueError("Invalid message link format.")
    log.debug(f"Parsed link: Type={link_type}, Identifier='{chat_identifier}', MsgID={message_id}")

    try:
        if not client.is_connected(): raise ConnectionError("Client not connected") # Use built-in ConnectionError
        chat_entity = await client.get_entity(chat_identifier)
        if not chat_entity: raise ValueError(f"Could not resolve entity '{chat_identifier}'")
        log.debug(f"Resolved chat entity: ID={chat_entity.id}, Type={type(chat_entity)}")
        log.info(f"Parsed message link: ChatID {chat_entity.id}, MsgID {message_id}")
        return chat_entity, message_id
    except MessageIdInvalidError as e: log.error(f"MsgID invalid link {link}: {e}"); raise ValueError(f"Message ID {message_id} invalid.") from e
    except (ValueError, TypeError) as e: log.error(f"Entity resolve error link {link}: {e}"); raise ValueError(f"Could not resolve source chat: {e}") from e


# --- Forwarding Logic ---
async def _forward_single_message(client, target_peer, source_chat_entity, message_id, fallback_chat_entity=None, fallback_message_id=None):
    """Forwards single message, handles errors, returns (bool_success, error_msg_or_None)."""
    phone = getattr(client.session.auth_key, 'phone', 'Unknown')
    target_id = getattr(target_peer, 'id', str(target_peer))
    log.debug(f"[{phone}] Attempt FWD -> Target={target_id} from Chat={source_chat_entity.id}/Msg={message_id}")
    ok, err_reason = False, None
    try:
        await client.forward_messages(target_peer, message_id, source_chat_entity)
        log.info(f"[{phone}] -> FWD SUCCESS to Target={target_id}"); ok = True
    except ChatSendMediaForbiddenError:
        log.warning(f"[{phone}] -> Media forbidden Target={target_id}. Fallback?"); err_reason = "Media forbidden"
        if fallback_chat_entity and fallback_message_id:
            try: await client.forward_messages(target_peer, fallback_message_id, fallback_chat_entity); log.info(f"[{phone}] -> Fallback SUCCESS Target={target_id}"); ok = True; err_reason += " (Used Fallback)"
            except ChatSendMediaForbiddenError: err_reason += " (Fallback Forbidden Too)"; log.error(f"[{phone}] -> Fallback FAIL Target={target_id}: {err_reason}")
            except Exception as fb_e: err_reason += f" (Fallback Failed: {type(fb_e).__name__})"; log.exception(f"[{phone}] -> Fallback FAIL Target={target_id}: {fb_e}", exc_info=False)
        else: err_reason += " (No Fallback)"; log.warning(f"[{phone}] -> FWD FAIL Target={target_id}: {err_reason}")
    except FloodWaitError as e: err_reason = f"Flood Wait ({e.seconds}s)"; log.warning(f"[{phone}] -> {err_reason} Target={target_id}. Wait {e.seconds}s."); await asyncio.sleep(e.seconds + random.uniform(0.5, 1.5))
    except (ChatWriteForbiddenError, UserBlockedError): err_reason = "Permission denied"; log.warning(f"[{phone}] -> Permission denied Target={target_id}.") # Combined some permission errors
    except UserNotParticipantError: err_reason = "Not in group"; log.warning(f"[{phone}] -> Not participant Target={target_id}.")
    except (PeerIdInvalidError, ChannelPrivateError): err_reason = "Target invalid/private"; log.warning(f"[{phone}] -> Target={target_id} invalid/private.")
    except MessageIdInvalidError: err_reason = "Source msg invalid"; log.error(f"[{phone}] -> Source {source_chat_entity.id}/{message_id} invalid."); raise ValueError(err_reason) # Critical
    except (AuthKeyError, ConnectionError, asyncio.TimeoutError, OSError) as e: err_reason = f"Conn/Sess Error: {type(e).__name__}"; log.error(f"[{phone}] -> {err_reason} Target={target_id}: {e}"); raise ConnectionError(err_reason) from e # Critical, use built-in ConnectionError
    except UserDeactivatedBanError as e: err_reason = "Account Banned"; log.critical(f"[{phone}] -> BANNED: {e}. ABORT."); db.update_userbot_status(phone, 'error', err_reason); raise e # Critical
    except Exception as e: err_reason = f"Unexpected FWD err: {type(e).__name__}"; log.exception(f"[{phone}] -> Unexpected err Target={target_id}: {e}", exc_info=True)
    return ok, err_reason

# --- Main Task Execution Logic ---
async def _execute_single_task(instance, task_info):
    """Core logic for executing one forwarding task."""
    client = instance["client"]; lock = instance["lock"]; phone = task_info['userbot_phone']; client_id = task_info['client_id']
    run_ts = int(datetime.now(UTC_TZ).timestamp()); log.info(f"[{phone}] Task triggered client {client_id}. Lock...")
    success_count, target_count, final_err = 0, 0, None
    try:
        async with lock:
            log.info(f"[{phone}] Lock acquire. Task execute.")
            if not await _safe_connect(client, phone): raise ConnectionError("Task connect fail.") # Use built-in ConnectionError

            primary_link = task_info['message_link']; fallback_link = task_info['fallback_message_link']
            try: source_chat, source_msg_id = await get_message_entity_by_link(client, primary_link); log.info(f"[{phone}] Primary msg ok.")
            except (ValueError, MessageIdInvalidError) as e: raise ValueError(f"Primary link fail '{primary_link}': {e}") from e
            fb_chat, fb_msg_id = None, None
            if fallback_link:
                try: fb_chat, fb_msg_id = await get_message_entity_by_link(client, fallback_link); log.info(f"[{phone}] Fallback msg ok.")
                except Exception as fb_err: log.warning(f"[{phone}] Fallback link resolve fail '{fallback_link}', skip: {fb_err}")

            target_ids = []; folder_name = "N/A"
            if task_info['send_to_all_groups']:
                log.info(f"[{phone}] Target: All groups..."); dialogs, err = await get_joined_chats_telethon(phone)
                if err: raise ConnectionError(f"Task fail get targets: {err.get('error')}") # Use built-in ConnectionError
                target_ids = [d['id'] for d in dialogs if d.get('id')]; log.info(f"[{phone}] Target {len(target_ids)} dialogs.")
            else:
                folder_id = task_info['folder_id']
                if folder_id: folder_name = db.get_folder_name(folder_id) or f"ID {folder_id}"; log.info(f"[{phone}] Target: Folder '{folder_name}'"); target_ids = db.get_target_groups_by_folder(folder_id); log.info(f"[{phone}] Target {len(target_ids)} DB groups.")
                else: raise ValueError("Task config error: No target.")

            target_count = len(target_ids)
            if target_count == 0: log.warning(f"[{phone}] No target groups for task. Finish.")
            else:
                log.info(f"[{phone}] Start FWD loop {target_count} targets...")
                processed = set(); delay = max(1.0, min(5.0, 30 / target_count))
                for target_id in target_ids:
                    if _stop_event.is_set(): log.info(f"[{phone}] Shutdown signal stop task run."); final_err = "Shutdown"; break
                    if not target_id or target_id in processed: continue
                    try:
                        target_peer = await client.get_entity(target_id)
                        if not target_peer: raise ValueError("Resolve target peer fail")
                        fwd_ok, fwd_err = await _forward_single_message(client, target_peer, source_chat, source_msg_id, fb_chat, fb_msg_id)
                        if fwd_ok: success_count += 1
                        elif fwd_err and final_err is None and "Flood Wait" not in fwd_err and "Connection Error" not in fwd_err and "Conn/Sess Error" not in fwd_err: final_err = f"Target {target_id}: {fwd_err}"
                        processed.add(target_id)
                    except FloodWaitError as e_fwd_loop: final_err = f"Flood Wait ({e_fwd_loop.seconds}s)"; log.warning(f"[{phone}] Outer loop flood. Abort run."); break
                    except ConnectionError as e_conn_loop: final_err = str(e_conn_loop); log.critical(f"[{phone}] Conn/Auth error loop. Abort."); raise # Use built-in ConnectionError
                    except UserDeactivatedBanError as e_ban_loop: final_err = "Account Banned"; log.critical(f"[{phone}] BANNED loop. Abort."); raise
                    except ValueError as e_val_loop: final_err = f"Config/Resolve err: {e_val_loop}"; log.error(f"[{phone}] {final_err}. Abort run."); break
                    except Exception as e_loop: final_err = f"Unexpected loop err: {e_loop}"; log.exception(f"[{phone}] {final_err}"); break
                    await asyncio.sleep(delay + random.uniform(0, 0.5))
    except (ValueError, ConnectionError, UserDeactivatedBanError) as critical_e: log.error(f"[{phone}] Task CRITICAL FAIL: {critical_e}"); final_err = final_err or str(critical_e) # Use built-in ConnectionError
    except Exception as outer_e: log.exception(f"[{phone}] Task unexpected exception: {outer_e}"); final_err = f"Unexpected task error: {outer_e}"
    finally: log.info(f"[{phone}] Task finish. Sent: {success_count}/{target_count}. Error: {final_err}. Release lock."); db.update_task_after_run(client_id, phone, run_ts, success_count, error=final_err)

# --- Background Task Scheduler ---
async def run_check_tasks_periodically():
    """Periodically checks DB for due tasks and schedules them."""
    log.info("Background task checker service started."); await asyncio.sleep(15)
    while not _stop_event.is_set():
        start_t = datetime.now(UTC_TZ); log.info(f"Task Check Cycle Start: {start_t.isoformat()}"); current_ts = int(start_t.timestamp()); scheduled = 0
        try:
            tasks = db.get_active_tasks_to_run(current_ts); log.info(f"Found {len(tasks)} tasks due.")
            if tasks:
                for task in tasks:
                     if _stop_event.is_set(): break
                     phone = task['userbot_phone']; client_id = task['client_id']
                     runtime = get_userbot_runtime_info(phone)
                     if not runtime: log.warning(f"Skip task {client_id}/{phone}: Runtime unavailable."); db.update_task_after_run(client_id, phone, current_ts, 0, "Skipped - Runtime fail"); continue
                     log.debug(f"Schedule task {client_id}/{phone} in loop."); asyncio.run_coroutine_threadsafe(_execute_single_task(runtime, task), runtime["loop"]); scheduled += 1; await asyncio.sleep(random.uniform(0.05, 0.2))
        except sqlite3.Error as db_e: log.exception(f"DB error task check: {db_e}")
        except Exception as e: log.exception(f"Unexpected error task check loop: {e}")
        if _stop_event.is_set(): break
        elapsed = (datetime.now(UTC_TZ) - start_t).total_seconds(); wait = max(5.0, CHECK_TASKS_INTERVAL - elapsed); log.info(f"Task Check Cycle End. Scheduled: {scheduled}. Elapsed: {elapsed:.2f}s. Next check in {wait:.2f}s."); await asyncio.sleep(wait)
    log.info("Background task checker stopped.")

# --- Initialization and Shutdown ---
def initialize_all_userbots():
    """Initializes runtime for all potentially active bots on startup."""
    log.info("Initializing userbot runtimes..."); all_bots = db.get_all_userbots(); init_count = 0
    for bot in all_bots:
        if bot['status'] != 'inactive': phone = bot['phone_number']; log.debug(f"Init runtime {phone} (Status: {bot['status']})..."); info = get_userbot_runtime_info(phone);
        if info: init_count += 1
    log.info(f"Finished runtime init. Attempted/Verified {init_count} runtimes.")

def shutdown_telethon():
    """Signals tasks to stop and disconnects userbots."""
    log.info("Initiating Telethon shutdown..."); _stop_event.set()
    with _userbots_lock: phones = list(_userbots.keys()); log.info(f"Stopping {len(phones)} userbot runtimes...")
    if not phones: log.info("No active runtimes."); return
    # Stop runtime sequentially might be safer
    for phone in phones: stop_userbot_runtime(phone)
    log.info("Telethon shutdown sequence finished.")

log.info("Telethon Utils module loaded.")
