# --- START OF FILE telethon_utils.py ---

import asyncio
import threading
import os
import random
import re
import time
from datetime import datetime
import sqlite3
import html # For escaping in logs/errors if needed

from telethon import TelegramClient, functions, types, errors
from telethon.tl.types import (
    PeerChannel, PeerChat, PeerUser, InputPeerChannel, InputPeerChat, InputPeerUser,
    Channel, User as TelethonUser, Chat as TelethonChat, Message
)
# Import specific errors to catch them explicitly
from telethon.errors import (
    # Auth errors
    SessionPasswordNeededError, FloodWaitError, PhoneNumberInvalidError, PhoneCodeInvalidError,
    PhoneCodeExpiredError, PasswordHashInvalidError, ApiIdInvalidError, AuthKeyError,
    UserDeactivatedBanError, PhonePasswordFloodError, RpcCallFailError, FreshChangePhoneForbiddenError,
    # Joining/Entity errors
    UsernameNotOccupiedError, ChannelPrivateError, InviteHashExpiredError, InviteHashInvalidError,
    UserAlreadyParticipantError, UserNotMutualContactError, UsersTooMuchError, ChatAdminRequiredError,
    UserNotParticipantError, # Added this explicitly
    # Sending/Forwarding errors
    ChatSendMediaForbiddenError, UserIsBlockedError, ChatWriteForbiddenError, MessageIdInvalidError,
    PeerIdInvalidError, InputUserDeactivatedError, YouBlockedUserError, MsgIdInvalidError,
    # Other common errors
    TimeoutError as AsyncTimeoutError, # Distinguish from built-in TimeoutError
    UserPrivacyRestrictedError, ChatRestrictedError, ChatNotModifiedError,
)
from telethon.tl.functions.channels import JoinChannelRequest, GetFullChannelRequest, GetMessagesRequest as GetChannelMessagesRequest
from telethon.tl.functions.messages import ImportChatInviteRequest, GetMessagesRequest, GetHistoryRequest, CheckChatInviteRequest
from telethon.tl.functions.account import GetPasswordRequest
from telethon.sessions import StringSession, SQLiteSession # SQLiteSession is used

import database as db
from config import (
    SESSION_DIR, CLIENT_TIMEOUT, CHECK_TASKS_INTERVAL, UTC_TZ, log
)

# --- Userbot Runtime Management ---
_userbots = {} # {phone: {'client': obj, 'loop': obj, 'lock': obj, 'thread': obj}}
_userbots_lock = threading.Lock()
_stop_event = threading.Event() # Global stop signal for background tasks

# --- Helper Functions ---

def _get_session_path(phone):
    """Generates the full path for the session file."""
    # Ensure phone number is filename-safe (remove '+', maybe other chars if needed)
    safe_phone = re.sub(r'[^\d]', '', phone) # Keep only digits
    if not safe_phone: # Handle potential empty string
        safe_phone = f"invalid_phone_{random.randint(1000, 9999)}"
    return os.path.join(SESSION_DIR, f"{safe_phone}.session")

async def _create_telethon_client_instance(session_path, api_id, api_hash, loop):
    """Creates a Telethon client instance."""
    log.debug(f"Creating TelethonClient instance for session: {session_path}")
    # Using SQLiteSession by default, seems more robust than StringSession for file storage
    session = SQLiteSession(session_path)
    client = TelegramClient(
        session,
        api_id,
        api_hash,
        timeout=CLIENT_TIMEOUT,
        loop=loop,
        # Device info can help avoid session issues sometimes
        device_model="PC 64bit",
        system_version="Linux",
        app_version="1.0.0", # Custom app version
        lang_code="en", # Use English for system messages from Telegram
        system_lang_code="en"
    )
    return client

def _run_loop(loop, phone_for_log):
    """Runs the event loop for a specific userbot thread."""
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
            # Clean up tasks before closing loop
            tasks = asyncio.all_tasks(loop=loop)
            for task in tasks:
                if not task.done():
                    task.cancel()
            # Wait for tasks to cancel (optional, adds delay but cleaner)
            # loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True)) # Can be complex

            if loop.is_running():
                loop.run_until_complete(loop.shutdown_asyncgens())
            loop.close()
            log.info(f"Event loop closed cleanly for {phone_for_log}.")
        except Exception as close_e:
            log.error(f"Error closing event loop for {phone_for_log}: {close_e}")

async def _safe_connect(client: TelegramClient, phone: str) -> bool:
    """Connects the client safely, handling common errors."""
    if client.is_connected():
        # Verify authorization even if connected
        try:
            if await client.is_user_authorized():
                return True
            else:
                log.warning(f"Userbot {phone} connected but not authorized. Session might be revoked.")
                db.update_userbot_status(phone, 'error', last_error="Session Revoked?")
                await _safe_disconnect(client, phone, update_db=False)
                return False
        except Exception as auth_check_e:
             log.error(f"Error checking authorization for {phone} while connected: {auth_check_e}")
             # Assume connection is problematic
             await _safe_disconnect(client, phone, update_db=False)
             return False

    log.info(f"Connecting userbot {phone}...")
    db.update_userbot_status(phone, 'connecting')
    try:
        await client.connect()
        if await client.is_user_authorized():
            log.info(f"Userbot {phone} connected and authorized.")
            # Update status to 'active' only after successful get_me later
            return True
        else:
            log.error(f"Userbot {phone} connection attempt failed: Not authorized after connect.")
            db.update_userbot_status(phone, 'error', last_error="Session invalid - Not Authorized")
            await _safe_disconnect(client, phone, update_db=False) # Disconnect but keep DB status as error
            # Potentially delete session file here if consistently not authorized
            # await delete_session_files_for_phone(phone) # Use the public function name
            return False
    except AuthKeyError as e:
        log.error(f"Auth key error for {phone}. Session invalid. Deleting session file.", exc_info=True)
        db.update_userbot_status(phone, 'error', last_error="Invalid session (AuthKeyError)")
        await delete_session_files_for_phone(phone) # Use the public function name
        return False
    except (ConnectionError, AsyncTimeoutError, OSError, RpcCallFailError) as e:
        log.error(f"Connection failed for {phone}: {type(e).__name__} - {e}")
        db.update_userbot_status(phone, 'error', last_error=f"Connection Error: {type(e).__name__}")
        return False
    except Exception as e:
        log.exception(f"Unexpected error connecting userbot {phone}: {e}", exc_info=True)
        db.update_userbot_status(phone, 'error', last_error=f"Unexpected connect error: {type(e).__name__}")
        return False

async def _safe_disconnect(client: TelegramClient, phone: str, update_db: bool = True):
    """Disconnects the client safely."""
    if client and client.is_connected():
        log.info(f"Disconnecting userbot {phone}...")
        try:
            await client.disconnect()
            log.info(f"Userbot {phone} disconnected.")
            # Optionally update DB status on disconnect if needed
            # if update_db: db.update_userbot_status(phone, 'inactive', last_error='Disconnected')
        except Exception as e:
            log.error(f"Error during graceful disconnect for userbot {phone}: {e}")
    elif client:
         log.debug(f"Userbot {phone} already disconnected.")

def delete_session_files_for_phone(phone): # Renamed from _delete_session_file
    """Deletes the session file and its journal-related files."""
    session_path = _get_session_path(phone)
    journal_path = f"{session_path}-journal" # For SQLiteSession
    wal_path = f"{session_path}-wal"         # Write-ahead log
    shm_path = f"{session_path}-shm"         # Shared memory

    deleted_count = 0
    for path in [session_path, journal_path, wal_path, shm_path]:
        try:
            if os.path.exists(path):
                os.remove(path)
                log.info(f"Deleted session-related file: {path}")
                deleted_count += 1
        except OSError as e:
            log.error(f"Failed to delete session file {path} for {phone}: {e}")
    return deleted_count > 0


# --- Public Userbot Runtime Functions ---
async def get_userbot_runtime_info_async(phone: str) -> dict:
    """Get runtime information for a userbot."""
    try:
        runtime = get_userbot_runtime_info(phone)
        if not runtime:
            log.warning(f"get_userbot_runtime_info_async: No runtime found for {phone}")
            return None
        
        client = runtime['client']
        if not client:
            log.warning(f"get_userbot_runtime_info_async: No client found for {phone}")
            return None
        
        try:
            me = await client.get_me()
            if me:
                return {
                    'id': me.id,
                    'username': me.username,
                    'phone': phone,
                    'connected': client.is_connected()
                }
            else:
                log.warning(f"get_userbot_runtime_info_async: get_me returned None for {phone}")
                return None
        except Exception as e:
            log.error(f"get_userbot_runtime_info_async: Error getting user info for {phone}: {e}", exc_info=True)
            return None
    except Exception as e:
        log.error(f"get_userbot_runtime_info_async: Error getting runtime info for {phone}: {e}", exc_info=True)
        return None

async def submit_userbot_code_async(phone: str, code: str) -> dict:
    """Submit verification code for userbot authentication."""
    try:
        runtime = get_runtime(phone)
        if not runtime:
            log.warning(f"submit_userbot_code_async: No runtime found for {phone}")
            return {'success': False, 'error': 'No runtime found'}
        
        result = await runtime.submit_code(code)
        log.info(f"submit_userbot_code_async: Code submission result for {phone}: {result}")
        return result
    except Exception as e:
        log.error(f"submit_userbot_code_async: Error submitting code for {phone}: {e}", exc_info=True)
        return {'success': False, 'error': str(e)}

async def submit_userbot_password_async(phone: str, password: str) -> dict:
    """Submit 2FA password for userbot authentication."""
    try:
        runtime = get_runtime(phone)
        if not runtime:
            log.warning(f"submit_userbot_password_async: No runtime found for {phone}")
            return {'success': False, 'error': 'No runtime found'}
        
        result = await runtime.submit_password(password)
        log.info(f"submit_userbot_password_async: Password submission result for {phone}: {result}")
        return result
    except Exception as e:
        log.error(f"submit_userbot_password_async: Error submitting password for {phone}: {e}", exc_info=True)
        return {'success': False, 'error': str(e)}

def get_userbot_runtime_info(phone_number):
    """Gets or initializes the runtime environment (client, loop, thread) for a userbot."""
    with _userbots_lock:
        # Check if already running and thread is alive
        if phone_number in _userbots:
            bot_info = _userbots[phone_number]
            thread = bot_info.get('thread')
            if thread and thread.is_alive():
                log.debug(f"Returning existing, live runtime for {phone_number}")
                return bot_info
            else:
                log.warning(f"Thread for userbot {phone_number} found dead or missing. Cleaning up existing entry.")
                loop = bot_info.get('loop')
                if loop and loop.is_running():
                    # Stop the loop from the correct thread
                    loop.call_soon_threadsafe(loop.stop)
                    # Wait briefly for thread to potentially exit (though join is better)
                    if thread: thread.join(timeout=1)
                del _userbots[phone_number] # Remove stale entry

        log.info(f"Attempting to initialize runtime for userbot {phone_number}...")
        userbot_db = db.find_userbot(phone_number)
        if not userbot_db:
            log.error(f"Cannot initialize runtime: Userbot {phone_number} not found in DB.")
            return None
        # Don't initialize if marked inactive in DB
        if userbot_db['status'] == 'inactive':
            log.warning(f"Skipping runtime initialization for inactive userbot {phone_number}.")
            return None

        session_file = _get_session_path(phone_number)
        api_id = userbot_db['api_id']
        api_hash = userbot_db['api_hash']

        if not api_id or not api_hash:
            log.error(f"Cannot initialize runtime for {phone_number}: Missing API ID/Hash in DB.")
            db.update_userbot_status(phone_number, 'error', last_error="Missing API credentials")
            return None

        # Create new runtime environment
        client = None # Initialize client to None
        loop = None # Initialize loop to None
        thread = None # Initialize thread to None
        try:
            loop = asyncio.new_event_loop()
            lock = asyncio.Lock() # Per-bot lock for operations

            # Create client in the new loop's thread
            async def _create_client_task():
                return await _create_telethon_client_instance(session_file, api_id, api_hash, loop)

            # Start the event loop thread
            thread = threading.Thread(target=_run_loop, args=(loop, phone_number), daemon=True)
            thread.start()

            # Run client creation in the loop's thread and wait for result
            future = asyncio.run_coroutine_threadsafe(_create_client_task(), loop)
            client = future.result(timeout=CLIENT_TIMEOUT) # Wait for client creation
            log.info(f"Telethon client created for {phone_number}")

            bot_info = {'client': client, 'loop': loop, 'lock': lock, 'thread': thread}
            _userbots[phone_number] = bot_info
            db.update_userbot_status(phone_number, 'initializing') # Mark as initializing

            # Schedule initial connection check and get_me in the loop
            async def _initial_connect_check():
                 log.debug(f"Running initial connect check for {phone_number}")
                 connected = await _safe_connect(client, phone_number)
                 if connected:
                     log.info(f"Initial connection successful for {phone_number}. Getting user info...")
                     try:
                         me = await client.get_me()
                         if me:
                              log.info(f"Userbot {phone_number} identified as @{me.username} (ID: {me.id})")
                              db.update_userbot_status(phone_number, 'active', username=me.username, last_error=None) # Mark active
                         else:
                              log.error(f"get_me returned None for {phone_number} after connect.")
                              db.update_userbot_status(phone_number, 'error', last_error="get_me failed (None)")
                     except UserDeactivatedBanError as e:
                          log.critical(f"Userbot {phone_number} is BANNED/DEACTIVATED during init check: {e}")
                          db.update_userbot_status(phone_number, 'error', last_error="Account Banned/Deactivated")
                          await _safe_disconnect(client, phone_number, update_db=False)
                          stop_userbot_runtime(phone_number) # Stop the runtime if banned
                     except AuthKeyError as e:
                           log.error(f"AuthKeyError during get_me for {phone_number}: {e}. Session invalid.")
                           db.update_userbot_status(phone_number, 'error', last_error="Invalid session (AuthKeyError)")
                           await delete_session_files_for_phone(phone_number)
                           await _safe_disconnect(client, phone_number, update_db=False)
                           stop_userbot_runtime(phone_number)
                     except Exception as get_me_err:
                         log.error(f"Failed get_me for {phone_number} after connect: {get_me_err}", exc_info=True)
                         db.update_userbot_status(phone_number, 'error', last_error=f"get_me failed: {type(get_me_err).__name__}")
                 else:
                     log.warning(f"Initial connection failed for {phone_number}. Status updated by _safe_connect.")

            asyncio.run_coroutine_threadsafe(_initial_connect_check(), loop)

            log.info(f"Runtime initialization process started for userbot {phone_number}.")
            return bot_info
        except Exception as e:
            log.critical(f"CRITICAL runtime initialization failed for {phone_number}: {e}", exc_info=True)
            db.update_userbot_status(phone_number, 'error', last_error=f"Runtime init fail: {type(e).__name__}")
            # Clean up partially created resources
            if loop and loop.is_running():
                loop.call_soon_threadsafe(loop.stop)
            if thread and thread.is_alive():
                thread.join(timeout=2)
            if phone_number in _userbots:
                del _userbots[phone_number]
            return None

def stop_userbot_runtime(phone_number):
    """Stops the runtime environment for a specific userbot."""
    with _userbots_lock:
        bot_info = _userbots.pop(phone_number, None) # Remove from dict atomically

    if bot_info:
        log.info(f"Stopping runtime for userbot {phone_number}...")
        client, loop, thread = bot_info.get('client'), bot_info.get('loop'), bot_info.get('thread')

        # Disconnect client in its own loop
        if client and loop and loop.is_running():
            future = asyncio.run_coroutine_threadsafe(_safe_disconnect(client, phone_number, update_db=False), loop)
            try:
                future.result(timeout=CLIENT_TIMEOUT / 2) # Wait for disconnect
            except (AsyncTimeoutError, Exception) as e:
                log.warning(f"Error or timeout during disconnect future for {phone_number} on stop: {e}")

        # Stop the event loop
        if loop and loop.is_running():
            loop.call_soon_threadsafe(loop.stop)

        # Wait for the thread to finish
        if thread and thread.is_alive():
            log.debug(f"Waiting for thread {thread.name} to join...")
            thread.join(timeout=5) # Wait max 5 seconds for thread cleanup
            if thread.is_alive():
                log.warning(f"Thread {thread.name} did not stop gracefully after 5 seconds.")
            else:
                 log.info(f"Thread {thread.name} stopped.")
        else:
             log.debug(f"Thread for {phone_number} was not found or already stopped.")

        log.info(f"Runtime stopped for {phone_number}")
        return True
    else:
        log.warning(f"Attempted to stop runtime for {phone_number}, but it was not found in the active runtimes list.")
        return False

# --- Authentication Flow ---
async def start_authentication_flow(phone, api_id, api_hash):
    """Initiates the Telethon authentication flow."""
    session_file = _get_session_path(phone)
    await delete_session_files_for_phone(phone) # Ensure clean start
    log.info(f"Starting authentication flow for {phone}...")

    # Create a temporary loop and client instance ONLY for this auth attempt
    temp_loop = asyncio.new_event_loop()
    temp_client = None
    auth_thread = None # Initialize auth_thread
    status = 'error'
    data = {'error_message': "Initialization failed"} # Default error

    try:
        # Run client creation and connection within the temp loop
        async def _auth_start_task():
            nonlocal temp_client # Allow modification
            temp_client = await _create_telethon_client_instance(session_file, api_id, api_hash, temp_loop)
            log.info(f"Temporary client created for {phone} auth.")
            await temp_client.connect() # Connect first
            log.info(f"Requesting phone code for {phone}...")
            # This might raise SessionPasswordNeededError directly
            code_info = await temp_client.send_code_request(phone)
            log.info(f"Phone code request sent successfully for {phone}.")
            # Return necessary data for code step
            return 'code_needed', {'phone_code_hash': code_info.phone_code_hash}

        # Start the temp loop in a separate thread briefly for this async task
        auth_thread = threading.Thread(target=temp_loop.run_forever, daemon=True)
        auth_thread.start()
        future = asyncio.run_coroutine_threadsafe(_auth_start_task(), temp_loop)
        status, result_data = future.result(timeout=CLIENT_TIMEOUT * 2) # Allow longer timeout for code request
        data = result_data # Store phone_code_hash

    except SessionPasswordNeededError:
        log.warning(f"Password needed for {phone} during initial code request.")
        status = 'password_needed'
        # We need the client instance, try to keep it alive if possible
        if not temp_client: # Should exist from _auth_start_task attempt
            log.error("Temporary client is None after SessionPasswordNeededError.")
            data = {'error_message': "Internal error: Client lost"}
            status = 'error'
        else:
             # Fetch password hint if needed (can be done within the same client session)
             async def _get_pwd_hint():
                 return await temp_client(GetPasswordRequest())
             future = asyncio.run_coroutine_threadsafe(_get_pwd_hint(), temp_loop)
             try:
                 pwd_state = future.result(timeout=CLIENT_TIMEOUT)
                 data = {'pwd_state': pwd_state} # Store password state for completion step
                 log.info(f"Password required for {phone}. Hint: {getattr(pwd_state, 'hint', 'None')}")
             except Exception as pwd_e:
                  log.error(f"Failed to get password hint for {phone}: {pwd_e}")
                  status = 'error'
                  data = {'error_message': f"Failed to get password state: {pwd_e}"}
    except FloodWaitError as e:
        log.warning(f"Flood wait during code request for {phone}: {e.seconds}s")
        status = 'error'
        data = {'error_message': f"Flood wait: {e.seconds}s"}
    except (PhoneNumberInvalidError, ApiIdInvalidError) as e:
        log.error(f"Invalid configuration or phone number for {phone}: {e}")
        status = 'error'
        data = {'error_message': f"Invalid config/phone: {e}"}
    except AuthKeyError as e:
        log.error(f"AuthKeyError during initial connection for {phone}: {e}")
        status = 'error'
        data = {'error_message': "Authentication key error (session likely invalid)."}
    except (ConnectionError, AsyncTimeoutError, OSError, RpcCallFailError) as e:
        log.error(f"Connection error during code request for {phone}: {type(e).__name__} - {e}")
        status = 'error'
        data = {'error_message': f"Connection failed: {e}"}
    except Exception as e:
        log.exception(f"Unexpected error during authentication start for {phone}: {e}")
        status = 'error'
        data = {'error_message': f"Unexpected error: {e}"}

    # --- Cleanup or Prepare for Next Step ---
    if status == 'error':
        log.warning(f"Authentication start failed for {phone}. Cleaning up temporary resources.")
        if temp_client:
            if temp_loop and temp_loop.is_running():
                asyncio.run_coroutine_threadsafe(_safe_disconnect(temp_client, phone, update_db=False), temp_loop)
            else:
                try: asyncio.run(temp_client.disconnect())
                except: pass
        if temp_loop and temp_loop.is_running():
            temp_loop.call_soon_threadsafe(temp_loop.stop)
        if auth_thread and auth_thread.is_alive():
            auth_thread.join(timeout=2)
        if 'error_message' not in data: data = {'error_message': 'Unknown failure'}
    else:
        data['client'] = temp_client
        data['loop'] = temp_loop
        data['thread'] = auth_thread
        log.info(f"Authentication flow for {phone} requires next step: {status}")

    return status, data

async def complete_authentication_flow(auth_data, code=None, password=None):
    """Completes the Telethon authentication flow using code or password."""
    temp_client = auth_data.get('client')
    temp_loop = auth_data.get('loop')
    auth_thread = auth_data.get('thread')
    phone = "Unknown"
    status = 'error'
    data = {'error_message': "Initialization failed"}

    if not temp_client or not temp_loop:
        log.error("Complete authentication called with invalid auth_data (missing client or loop).")
        data = {'error_message': "Internal Error: Session data missing."}
        if temp_loop and temp_loop.is_running(): temp_loop.call_soon_threadsafe(temp_loop.stop)
        if auth_thread and auth_thread.is_alive(): auth_thread.join(timeout=1)
        return status, data

    log.info(f"Attempting to complete authentication for phone (number TBD)...")
    me = None
    try:
        async def _auth_complete_task():
            nonlocal me, phone
            if code:
                phone_code_hash = auth_data.get('phone_code_hash')
                if not phone_code_hash: raise ValueError("Missing phone_code_hash for code login.")
                log.info(f"Signing in with phone code...")
                me = await temp_client.sign_in(code=code, phone_code_hash=phone_code_hash)
            elif password:
                log.info(f"Signing in with password...")
                me = await temp_client.sign_in(password=password)
            else:
                raise ValueError("No code or password provided for sign in.")

            if me and await temp_client.is_user_authorized():
                phone = me.phone
                username = me.username
                log.info(f"Authentication successful for {phone} (@{username})")
                session_path = temp_client.session.filename
                session_rel = os.path.relpath(session_path, SESSION_DIR) if SESSION_DIR in session_path else os.path.basename(session_path)
                api_id = temp_client.api_id
                api_hash = temp_client.api_hash
                db_ok = db.add_userbot(phone, session_rel, api_id, api_hash, 'active', username, assigned_client=None, last_error=None)
                if db_ok:
                    log.info(f"Userbot {phone} saved to database successfully.")
                    return 'success', {'phone': phone, 'username': username}
                else:
                    log.error(f"Authentication successful for {phone}, but failed to save to database.")
                    return 'error', {'error_message': "DB save failed after successful auth."}
            else:
                log.error(f"Sign-in attempt completed but user is not authorized. Phone: {phone if me else 'Unknown'}")
                return 'error', {'error_message': "Sign-in failed: Not authorized (Incorrect password or other issue?)."}

        future = asyncio.run_coroutine_threadsafe(_auth_complete_task(), temp_loop)
        status, data = future.result(timeout=CLIENT_TIMEOUT * 2)

    except SessionPasswordNeededError:
        status = 'error'; data = {'error_message': "Password required unexpectedly. Restart flow."}
    except (PhoneCodeInvalidError, PhoneCodeExpiredError):
        status = 'error'; data = {'error_message': "Invalid or expired verification code."}
    except PasswordHashInvalidError:
        status = 'error'; data = {'error_message': "Incorrect password."}
    except (PhonePasswordFloodError, FloodWaitError) as e:
         wait_seconds = getattr(e, 'seconds', '?'); status = 'error'; data = {'error_message': f"Flood wait: {wait_seconds}s"}
    except AuthKeyError as e:
        status = 'error'; data = {'error_message': "Authentication key error (session likely invalid)."}
        known_phone = getattr(temp_client.session.auth_key, 'phone', phone or "unknown_auth_complete")
        await delete_session_files_for_phone(known_phone)
    except UserDeactivatedBanError as e:
        known_phone = getattr(me, 'phone', phone or "Unknown"); status = 'error'; data = {'error_message': "Account banned or deactivated."}
        if known_phone != "Unknown": db.update_userbot_status(known_phone, 'error', last_error="Account Banned/Deactivated")
    except (ConnectionError, AsyncTimeoutError, OSError, RpcCallFailError) as e:
        known_phone = getattr(me, 'phone', phone or "Unknown"); status = 'error'; data = {'error_message': f"Connection failed: {e}"}
    except Exception as e:
        known_phone = getattr(me, 'phone', phone or "Unknown"); status = 'error'; data = {'error_message': f"Unexpected error: {e}"}
        log.exception(f"Unexpected error during authentication completion for {known_phone}: {e}")

    known_phone_for_log = getattr(me, 'phone', phone or "unknown_auth_complete")
    log.debug(f"Cleaning up temporary auth resources for {known_phone_for_log} (Status: {status}).")
    if temp_client:
        if temp_loop and temp_loop.is_running():
            future = asyncio.run_coroutine_threadsafe(_safe_disconnect(temp_client, known_phone_for_log, update_db=False), temp_loop)
            try: future.result(timeout=5)
            except Exception: pass
        else:
            try: asyncio.run(temp_client.disconnect())
            except Exception: pass
    if temp_loop and temp_loop.is_running():
        temp_loop.call_soon_threadsafe(temp_loop.stop)
    if auth_thread and auth_thread.is_alive():
        auth_thread.join(timeout=2)

    if status == 'error' and isinstance(data, str): data = {'error_message': data}
    elif status == 'error' and ('error_message' not in data or not data['error_message']):
         data['error_message'] = 'Unknown failure reason.'

    return status, data

# --- Other Telethon Actions ---
def parse_telegram_url_simple(url: str) -> tuple[str, str | int]:
    """
    Parses various Telegram URL formats.
    Returns tuple: (link_type, identifier)
    link_type: 'public', 'private_join', 'message_link', 'username', 'unknown'
    identifier: username, hash, full_url (for message), number (for ID)
    """
    url = url.strip()
    if "/c/" in url or re.match(r'https?://t\.me/[\w\d_]{5,}/\d+', url):
        return "message_link", url
    if match := re.match(r"https?://t\.me/\+([\w\d_-]+)/?", url):
        return "private_join", match.group(1)
    if match := re.match(r"https?://t\.me/joinchat/([\w\d_-]+)/?", url):
        return "private_join", match.group(1)
    if match := re.match(r"https?://t\.me/([\w\d_]{5,})/?$", url):
        return "public", match.group(1)
    if match := re.match(r"@([\w\d_]{5,})/?$", url):
        return "username", match.group(1)
    return "unknown", url

def _format_entity_detail(entity) -> dict | None:
    """Formats Telethon entity into a standard dictionary."""
    if not entity: return None
    entity_type = 'unknown'
    if isinstance(entity, TelethonUser): entity_type = 'user'
    elif isinstance(entity, TelethonChat): entity_type = 'group'
    elif isinstance(entity, Channel): entity_type = 'channel'

    name = getattr(entity, 'title', None) or \
           getattr(entity, 'username', None) or \
           getattr(entity, 'first_name', '') + (' ' + getattr(entity, 'last_name', '') if getattr(entity, 'last_name', None) else '') or \
           f"ID {entity.id}"
    name = name.strip()

    return {
        "id": entity.id,
        "name": name,
        "username": getattr(entity, 'username', None),
        "type": entity_type
        }

async def resolve_links_info(phone: str, urls: list[str]) -> dict[str, dict]:
    runtime_info = get_userbot_runtime_info(phone)
    if not runtime_info:
        return {url: {"error": "Userbot offline"} for url in urls}

    client, loop, lock = runtime_info['client'], runtime_info['loop'], runtime_info['lock']
    bot_display = phone

    async def _resolve_batch_task():
        results = {}
        nonlocal bot_display
        async with lock:
            try:
                if not await _safe_connect(client, phone):
                    raise ConnectionError("Failed connection for link resolution")
                try:
                    me = await client.get_me()
                    bot_display = f"@{me.username}" if me and me.username else phone
                except Exception: pass

                log.info(f"[{bot_display}] Resolving {len(urls)} URLs...")
                for i, url in enumerate(urls):
                    if not url or not isinstance(url, str):
                        results[url] = {"error": "Invalid input URL"}; continue
                    start_t = time.monotonic(); resolved_info = {"error": "Resolution failed"}
                    try:
                        link_type, identifier = parse_telegram_url_simple(url)
                        log.debug(f"[{bot_display}] Parsing '{url}': Type={link_type}, ID='{identifier}'")
                        if link_type == "unknown": resolved_info = {"error": "Unrecognized URL format"}
                        elif link_type == "message_link": resolved_info = {"error": "Cannot resolve message link for entity info"}
                        elif link_type == "private_join":
                            try:
                                 invite_info = await client(CheckChatInviteRequest(hash=identifier))
                                 if hasattr(invite_info, 'chat') and invite_info.chat:
                                     entity_detail = _format_entity_detail(invite_info.chat)
                                     if entity_detail: resolved_info = entity_detail
                                     else: resolved_info = {"error": "Could not format chat from invite"}
                                 elif hasattr(invite_info, 'title'):
                                      resolved_info = {"name": invite_info.title, "type": "channel" if invite_info.channel else "group", "id": None}
                                 else: resolved_info = {"error": "Invite valid but no chat info"}
                            except InviteHashInvalidError: resolved_info = {"error": "Invalid invite link"}
                            except InviteHashExpiredError: resolved_info = {"error": "Expired invite link"}
                            except UserAlreadyParticipantError: resolved_info = {"error": "Already participant (cannot get info from invite)"}
                            except Exception as invite_e: resolved_info = {"error": f"Invite check error: {type(invite_e).__name__}"}
                        elif link_type == "public" or link_type == "username":
                            try:
                                entity = await client.get_entity(identifier)
                                entity_detail = _format_entity_detail(entity)
                                if entity_detail: resolved_info = entity_detail
                                else: resolved_info = {"error": "Could not format resolved entity"}
                            except ValueError as e: resolved_info = {"error": f"Cannot resolve '{identifier}': Not found or accessible"}
                            except UserPrivacyRestrictedError: resolved_info = {"error": "Cannot resolve user (privacy settings)"}
                            except Exception as resolve_e: resolved_info = {"error": f"Resolution error: {type(resolve_e).__name__}"}
                        results[url] = resolved_info
                        log.debug(f"[{bot_display}] Result '{url}': {resolved_info.get('name', resolved_info.get('error', 'OK'))} ({(time.monotonic()-start_t):.2f}s)")
                    except Exception as inner_e:
                        log.exception(f"[{bot_display}] Unexpected error processing URL '{url}': {inner_e}")
                        results[url] = {"error": f"Internal processing error: {type(inner_e).__name__}"}
                    if i < len(urls) - 1: await asyncio.sleep(random.uniform(0.3, 0.8))
            except ConnectionError as e:
                for u in urls: results.setdefault(u, {"error": "Userbot connection failed"})
            except AuthKeyError:
                 db.update_userbot_status(phone, 'error', "Invalid session (AuthKeyError)")
                 for u in urls: results.setdefault(u, {"error": "Userbot session invalid"})
            except UserDeactivatedBanError:
                  db.update_userbot_status(phone, 'error', "Account Banned/Deactivated")
                  for u in urls: results.setdefault(u, {"error": "Userbot account banned"})
            except Exception as e:
                log.exception(f"[{bot_display}] Error in _resolve_batch_task: {e}")
                for u in urls: results.setdefault(u, {"error": f"Batch Error: {e}"})
            return results

    if not loop or loop.is_closed(): return {url: {"error": "Userbot loop offline"} for url in urls}
    future = asyncio.run_coroutine_threadsafe(_resolve_batch_task(), loop)
    try:
        timeout = (len(urls) * 5) + CLIENT_TIMEOUT
        return future.result(timeout=timeout)
    except AsyncTimeoutError: return {url: {"error": "Resolution timeout"} for url in urls}
    except Exception as e: return {url: {"error": f"Internal task error: {e}"} for url in urls}

async def join_groups_batch(phone, urls):
    runtime_info = get_userbot_runtime_info(phone)
    if not runtime_info: return {"error": "Userbot runtime unavailable."}, {}

    client, loop, lock = runtime_info['client'], runtime_info['loop'], runtime_info['lock']
    bot_display = phone

    async def _join_batch_task():
        results = {}; fatal_error_info = {}; nonlocal bot_display
        async with lock:
            try:
                if not await _safe_connect(client, phone): raise ConnectionError("Failed connection for joining groups")
                try: me = await client.get_me(); bot_display = f"@{me.username}" if me and me.username else phone
                except Exception: pass
                log.info(f"[{bot_display}] Starting join batch for {len(urls)} URLs.")
                for i, url in enumerate(urls):
                    if not url or not isinstance(url, str): continue
                    status, detail = "failed", {"reason": "Unknown"}; start_t = time.monotonic(); entity = None
                    try:
                        link_type, identifier = parse_telegram_url_simple(url)
                        log.debug(f"[{bot_display}] Parsing '{url}': Type={link_type}, ID='{identifier}'")
                        if link_type == "unknown": raise ValueError("Unrecognized URL format")
                        if link_type == "message_link": raise ValueError("Cannot join a message link")
                        if link_type == "private_join":
                            updates = await client(ImportChatInviteRequest(identifier))
                            if updates and updates.chats: entity = updates.chats[0]; status, detail = "success", _format_entity_detail(entity)
                            else: raise InviteHashInvalidError("ImportChatInvite failed or returned no chat.")
                        elif link_type == "public" or link_type == "username":
                             try:
                                 entity = await client.get_entity(identifier)
                                 if not isinstance(entity, (Channel, TelethonChat)): raise ValueError("Identifier points to a user.")
                                 await client(JoinChannelRequest(entity)); status, detail = "success", _format_entity_detail(entity)
                             except ValueError as e: status, detail = "failed", {"reason": "invalid_link_or_resolve", "error": str(e)}
                             except UserAlreadyParticipantError:
                                  if not entity: entity = await client.get_entity(identifier)
                                  status, detail = "already_member", _format_entity_detail(entity)
                        else: raise ValueError(f"Unsupported link type for joining: {link_type}")
                    except (InviteHashExpiredError, InviteHashInvalidError): status, detail = "failed", {"reason": "invalid_invite"}
                    except ChannelPrivateError: status, detail = "failed", {"reason": "private"}
                    except UserAlreadyParticipantError:
                          status = "already_member"
                          if link_type == 'public' or link_type == 'username':
                               try: detail = _format_entity_detail(await client.get_entity(identifier))
                               except Exception: detail = {"reason": "already_member_unresolved"}
                          else: detail = {"reason": "already_member_private_link"}
                    except UsersTooMuchError: status, detail = "failed", {"reason": "chat_full"}
                    except ChatAdminRequiredError: status, detail = "pending", {"reason": "admin_approval"}
                    except ChatWriteForbiddenError: status, detail = "failed", {"reason": "banned_or_restricted"}
                    except FloodWaitError as e:
                        wait = min(e.seconds + random.uniform(1, 3), 90); status, detail = "flood_wait", {"seconds": e.seconds}
                        results[url] = (status, detail); await asyncio.sleep(wait); continue
                    except ValueError as e: status, detail = "failed", {"reason": "invalid_link_or_resolve", "error": str(e)}
                    except AuthKeyError as e: db.update_userbot_status(phone, 'error', last_error="Invalid session (AuthKeyError)"); raise
                    except UserDeactivatedBanError as e: db.update_userbot_status(phone, 'error', last_error="Account Banned/Deactivated"); raise
                    except (ConnectionError, AsyncTimeoutError, OSError, RpcCallFailError) as e:
                         db.update_userbot_status(phone, 'error', last_error=f"Connection Error: {type(e).__name__}"); raise ConnectionError(f"Connection error during join: {e}") from e
                    except Exception as e: status, detail = "failed", {"reason": "internal_error", "error": str(e)}; log.exception(f"[{bot_display}] Unexpected error joining {url}: {e}")
                    results[url] = (status, detail); log.info(f"[{bot_display}] Result '{url}': {status} ({(time.monotonic()-start_t):.2f}s)")
                    if i < len(urls) - 1: await asyncio.sleep(max(0.5, 3.0 + random.uniform(-1.0, 1.0)))
            except (ConnectionError, AuthKeyError, UserDeactivatedBanError) as batch_e:
                 error_type = "Connection Error" if isinstance(batch_e, ConnectionError) else "Session Invalid" if isinstance(batch_e, AuthKeyError) else "Account Banned/Deactivated"
                 fatal_error_info = {"error": f"{error_type}: {batch_e}"}
            except Exception as e: fatal_error_info = {"error": f"Unexpected Batch Error: {e}"}
            return fatal_error_info, results

    if not loop or loop.is_closed(): return {"error": "Userbot event loop unavailable."}, {}
    future = asyncio.run_coroutine_threadsafe(_join_batch_task(), loop)
    try:
        timeout = CLIENT_TIMEOUT + (len(urls) * 15) + 30
        error_info, results_dict = future.result(timeout=timeout)
        final_results = {}
        for url in urls:
             if url in results_dict: final_results[url] = results_dict[url]
             else:
                 fail_reason = error_info.get("error", "Batch failed early") if error_info else "Batch failed early"
                 final_results[url] = ("failed", {"reason": "batch_error", "error": fail_reason})
        return error_info, final_results
    except AsyncTimeoutError: return {"error": "Batch join timeout."}, {url: ("failed", {"reason": "batch_timeout"}) for url in urls}
    except Exception as e: return {"error": f"Internal task error: {e}"}, {url: ("failed", {"reason": f"internal_error: {e}"}) for url in urls}

async def get_joined_chats_telethon(phone):
    runtime_info = get_userbot_runtime_info(phone)
    if not runtime_info: return None, {"error": "Userbot runtime not available."}

    client, loop, lock = runtime_info['client'], runtime_info['loop'], runtime_info['lock']
    bot_display = phone

    async def _get_dialogs_task():
        dialog_list = []; error_msg = None; nonlocal bot_display
        async with lock:
            try:
                if not await _safe_connect(client, phone): raise ConnectionError("Failed connection for fetching dialogs")
                try: me = await client.get_me(); bot_display = f"@{me.username}" if me and me.username else phone
                except Exception: pass
                log.info(f"[{bot_display}] Fetching dialogs (limit 500)...")
                dialog_count = 0
                async for dialog in client.iter_dialogs(limit=500, ignore_pinned=True, ignore_migrated=True):
                    dialog_count += 1; entity = dialog.entity
                    if entity and (dialog.is_group or dialog.is_channel):
                         details = _format_entity_detail(entity)
                         if details: dialog_list.append(details)
                log.info(f"[{bot_display}] Fetched {dialog_count} dialogs, found {len(dialog_list)} groups/channels.")
            except ConnectionError as e: error_msg = f"Connection Error: {e}"
            except AuthKeyError: error_msg = "Invalid session."; db.update_userbot_status(phone, 'error', error_msg)
            except UserDeactivatedBanError: error_msg = "Account Banned/Deactivated."; db.update_userbot_status(phone, 'error', error_msg)
            except FloodWaitError as e: error_msg = f"Flood wait ({e.seconds}s)"
            except Exception as e: error_msg = f"Unexpected error: {e}"; log.exception(f"[{bot_display}] Error fetching dialogs: {e}")
            return dialog_list, {"error": error_msg} if error_msg else {}

    if not loop or loop.is_closed(): return None, {"error": "Userbot loop unavailable."}
    future = asyncio.run_coroutine_threadsafe(_get_dialogs_task(), loop)
    try:
        dialogs, error_info = future.result(timeout=180)
        return dialogs, error_info
    except AsyncTimeoutError: return None, {"error": "Timeout fetching dialogs."}
    except Exception as e: return None, {"error": f"Internal task error: {e}"}

async def get_message_entity_by_link(client: TelegramClient, link: str) -> tuple[Channel | TelethonChat, int]:
    link = link.strip(); log.debug(f"Parsing and resolving message link: {link}")
    private_match = re.match(r'https?://t\.me/c/(\d+)/(\d+)', link)
    public_match = re.match(r'https?://t\.me/([\w\d_]+)/(\d+)', link)
    chat_identifier = None; message_id = None; link_type = 'unknown'

    if private_match:
        link_type = 'private'; chat_identifier = int(f"-100{private_match.group(1)}"); message_id = int(private_match.group(2))
    elif public_match:
        link_type = 'public'; chat_identifier = public_match.group(1); message_id = int(public_match.group(2))
    else: raise ValueError(f"Invalid or unsupported message link format: {link}")
    log.debug(f"Parsed link: Type={link_type}, Identifier='{chat_identifier}', MsgID={message_id}")
    try:
        if not client.is_connected(): raise ConnectionError("Client not connected for resolving link")
        chat_entity = await client.get_entity(chat_identifier)
        if not chat_entity: raise ValueError(f"Could not resolve entity for identifier '{chat_identifier}'")
        if not isinstance(chat_entity, (Channel, TelethonChat)): raise ValueError(f"Link identifier '{chat_identifier}' points to a user.")
        log.info(f"Resolved message link: ChatID={chat_entity.id} ('{getattr(chat_entity, 'title', 'N/A')}'), MsgID={message_id}")
        return chat_entity, message_id
    except MessageIdInvalidError as e: raise ValueError(f"Message ID {message_id} is invalid or inaccessible.") from e
    except (ValueError, TypeError) as e: raise ValueError(f"Could not resolve source chat: {e}") from e
    except ConnectionError as e: raise

async def check_message_link_access(phone: str, link: str) -> bool:
    runtime_info = get_userbot_runtime_info(phone)
    if not runtime_info: return False

    client, loop, lock = runtime_info['client'], runtime_info['loop'], runtime_info['lock']
    async def _check_access_task():
        async with lock:
            try:
                if not await _safe_connect(client, phone): raise ConnectionError("Connection failed for link access check")
                chat_entity, message_id = await get_message_entity_by_link(client, link)
                if isinstance(chat_entity, Channel): request = GetChannelMessagesRequest(channel=chat_entity, id=[message_id])
                else: request = GetMessagesRequest(id=[message_id])
                messages_obj = await client(request)
                if messages_obj and hasattr(messages_obj, 'messages') and messages_obj.messages: return True
                elif messages_obj and hasattr(messages_obj, 'chats') and not hasattr(messages_obj, 'messages'): return False
                else: return False
            except (ValueError, MessageIdInvalidError) as e: return False
            except (AuthKeyError, UserDeactivatedBanError, ConnectionError) as e:
                  if isinstance(e, AuthKeyError): db.update_userbot_status(phone, 'error', "Invalid session")
                  if isinstance(e, UserDeactivatedBanError): db.update_userbot_status(phone, 'error', "Account Banned")
                  return False
            except Exception as e: return False

    if not loop or loop.is_closed(): return False
    future = asyncio.run_coroutine_threadsafe(_check_access_task(), loop)
    try: return future.result(timeout=CLIENT_TIMEOUT * 2)
    except AsyncTimeoutError: return False
    except Exception as e: return False

async def _forward_single_message(client, target_peer, source_chat_entity, message_id, fallback_chat_entity=None, fallback_message_id=None):
    phone = "UnknownPhone"; ok, err_reason = False, None
    target_id_str = getattr(target_peer, 'id', str(target_peer)); source_id_str = getattr(source_chat_entity, 'id', str(source_chat_entity))
    log.debug(f"[{phone}] Attempt FWD: Source={source_id_str}/{message_id} -> Target={target_id_str}")
    try:
        await client.forward_messages(entity=target_peer, messages=message_id, from_peer=source_chat_entity)
        ok = True
    except ChatSendMediaForbiddenError:
        err_reason = "Media forbidden"
        if fallback_chat_entity and fallback_message_id:
            try:
                await client.forward_messages(entity=target_peer, messages=fallback_message_id, from_peer=fallback_chat_entity)
                ok = True; err_reason += " (Used Fallback)"
            except ChatSendMediaForbiddenError: err_reason += " (Fallback Forbidden Too)"
            except FloodWaitError as fb_e: err_reason += f" (Fallback Flood Wait {fb_e.seconds}s)"; await asyncio.sleep(fb_e.seconds + random.uniform(0.5, 1.5))
            except Exception as fb_e: err_reason += f" (Fallback Failed: {type(fb_e).__name__})"
        else: err_reason += " (No Fallback)"
    except FloodWaitError as e: err_reason = f"Flood Wait ({e.seconds}s)"; await asyncio.sleep(e.seconds + random.uniform(0.5, 1.5))
    except (ChatWriteForbiddenError, UserIsBlockedError, YouBlockedUserError, UserPrivacyRestrictedError, ChatRestrictedError, UserNotParticipantError): err_reason = "Permission denied/Not in group"
    except (PeerIdInvalidError, ChannelPrivateError, InputUserDeactivatedError): err_reason = "Target invalid/private/deactivated"
    except (MessageIdInvalidError, MsgIdInvalidError) as e: err_reason = "Source message invalid/deleted"; raise ValueError(err_reason) from e
    except AuthKeyError as e: err_reason = f"Session Error: {type(e).__name__}"; db.update_userbot_status(phone, 'error', last_error="Invalid session (AuthKeyError)"); raise ConnectionError(err_reason) from e
    except UserDeactivatedBanError as e: err_reason = "Account Banned"; db.update_userbot_status(phone, 'error', last_error=err_reason); raise ConnectionError(err_reason) from e
    except (ConnectionError, AsyncTimeoutError, OSError, RpcCallFailError) as e: err_reason = f"Connection/RPC Error: {type(e).__name__}"; db.update_userbot_status(phone, 'error', last_error=err_reason); raise ConnectionError(err_reason) from e
    except Exception as e: err_reason = f"Unexpected FWD err: {type(e).__name__}"; log.exception(f"[{phone}] -> UNEXPECTED FWD FAIL Target={target_id_str}: {e}", exc_info=True)
    if ok: log.info(f"[{phone}] -> FWD SUCCESS to Target={target_id_str}")
    else: log.warning(f"[{phone}] -> FWD FAILED Target={target_id_str}: {err_reason}")
    return ok, err_reason

async def _execute_single_task(instance, task_info):
    client = instance["client"]; lock = instance["lock"]; phone = task_info['userbot_phone']; client_id = task_info['client_id']; task_key = f"{client_id}_{phone}"
    run_ts = int(datetime.now(UTC_TZ).timestamp()); success_count, target_count, permanent_error = 0, 0, None
    try:
        async with lock:
            if _stop_event.is_set(): return
            if not await _safe_connect(client, phone): raise ConnectionError("Task connection failed.")
            primary_link = task_info.get('message_link')
            if not primary_link: raise ValueError("Task config error: Missing primary message link.")
            try: source_chat, source_msg_id = await get_message_entity_by_link(client, primary_link)
            except (ValueError, MessageIdInvalidError, ConnectionError) as e: raise ValueError(f"Primary link invalid: {e}") from e
            fb_chat, fb_msg_id = None, None
            if task_info.get('fallback_message_link'):
                try: fb_chat, fb_msg_id = await get_message_entity_by_link(client, task_info['fallback_message_link'])
                except Exception as fb_err: log.warning(f"[{task_key}] Failed fallback link resolve: {fb_err}")
            target_ids = []; folder_name = "N/A"
            if task_info.get('send_to_all_groups'):
                dialogs, err_info = await get_joined_chats_telethon(phone)
                if err_info and err_info.get('error'): raise ConnectionError(f"Failed to get target groups: {err_info['error']}")
                if dialogs: target_ids = [d['id'] for d in dialogs if d and d.get('id')]
            else:
                folder_id = task_info.get('folder_id')
                if folder_id:
                    folder_name_db = db.get_folder_name(folder_id)
                    if folder_name_db is None: raise ValueError("Target folder deleted or inaccessible.")
                    folder_name = folder_name_db or f"ID {folder_id}"
                    target_ids = db.get_target_groups_by_folder(folder_id)
                else: raise ValueError("Task config error: No target specified.")
            target_count = len(target_ids)
            if target_count > 0:
                processed = set(); base_delay = max(0.8, min(5.0, 60.0 / target_count))
                for i, target_id in enumerate(target_ids):
                    if _stop_event.is_set(): permanent_error = "Shutdown"; break
                    if not target_id or target_id in processed: continue
                    try:
                        target_peer = await client.get_entity(target_id)
                        if not target_peer:
                             if permanent_error is None: permanent_error = f"Target {target_id}: Not Found"
                             continue
                        fwd_ok, fwd_err = await _forward_single_message(client, target_peer, source_chat, source_msg_id, fb_chat, fb_msg_id)
                        processed.add(target_id)
                        if fwd_ok: success_count += 1
                        elif fwd_err and permanent_error is None and "Flood Wait" not in fwd_err and "Connection/RPC Error" not in fwd_err:
                            permanent_error = f"Target {target_id}: {fwd_err}"
                    except (ValueError, ConnectionError) as e: permanent_error = str(e); raise
                    except Exception as loop_e:
                         if permanent_error is None: permanent_error = f"Target {target_id}: Unexpected - {type(loop_e).__name__}"
                         log.exception(f"[{task_key}] Unexpected error in forward loop target {target_id}: {loop_e}"); break
                    await asyncio.sleep(max(0.2, base_delay + random.uniform(-0.3, 0.3)))
    except (ValueError, ConnectionError) as critical_e: permanent_error = permanent_error or str(critical_e)
    except Exception as outer_e: permanent_error = permanent_error or f"Unexpected: {type(outer_e).__name__}"; log.exception(f"[{task_key}] Outer task error: {outer_e}")
    finally:
        log.info(f"[{task_key}] Task finished. Sent: {success_count}/{target_count}. Error: {permanent_error}.")
        db.update_task_after_run(client_id, phone, run_ts, success_count, error=permanent_error)

async def run_check_tasks_periodically():
    log.info("Background task checker service starting...")
    await asyncio.sleep(15)
    while not _stop_event.is_set():
        start_t = datetime.now(UTC_TZ); current_ts = int(start_t.timestamp()); scheduled_count = 0; tasks_found = 0
        try:
            tasks_to_run = db.get_active_tasks_to_run(current_ts); tasks_found = len(tasks_to_run)
            if tasks_to_run:
                tasks_by_bot = {}
                for task in tasks_to_run: tasks_by_bot.setdefault(task['userbot_phone'], []).append(task)
                for phone, bot_tasks in tasks_by_bot.items():
                    if _stop_event.is_set(): break
                    runtime = get_userbot_runtime_info(phone)
                    if not runtime or not runtime['loop'].is_running():
                        for task in bot_tasks: db.update_task_after_run(task['client_id'], phone, current_ts, 0, error="Userbot Runtime Unavailable")
                        continue
                    for task in bot_tasks:
                         if _stop_event.is_set(): break
                         runtime['loop'].create_task(_execute_single_task(runtime, task)); scheduled_count += 1
                    if not _stop_event.is_set(): await asyncio.sleep(random.uniform(0.05, 0.2))
        except sqlite3.Error as db_e: log.exception(f"DB error in task check: {db_e}")
        except Exception as e: log.exception(f"Unexpected error in task check: {e}")
        if _stop_event.is_set(): break
        elapsed = (datetime.now(UTC_TZ) - start_t).total_seconds(); wait_time = max(5.0, CHECK_TASKS_INTERVAL - elapsed)
        log.info(f"Task Check Cycle End. Found: {tasks_found}. Scheduled: {scheduled_count}. Elapsed: {elapsed:.2f}s. Next: {wait_time:.2f}s.")
        try:
            await asyncio.wait_for(_stop_event.wait(), timeout=wait_time)
            break # Stop event was set
        except AsyncTimeoutError: pass # Timeout, continue
        except Exception as wait_e: log.error(f"Task check wait error: {wait_e}")
    log.info("Background task checker service stopped.")

async def initialize_all_userbots():
    """Initialize all non-inactive userbots from the database."""
    log.info("Starting initialization of all userbots...")
    
    try:
        # Get all non-inactive userbots from database
        userbots = db.get_all_userbots(exclude_status=['inactive'])
        if not userbots:
            log.info("No active userbots found in database.")
            return
        
        # Initialize each userbot
        for userbot in userbots:
            phone = userbot['phone']
            api_id = userbot['api_id']
            api_hash = userbot['api_hash']
            
            try:
                # Create new event loop for this userbot
                loop = asyncio.new_event_loop()
                
                # Create session path
                session_path = _get_session_path(phone)
                
                # Create client instance
                client = await _create_telethon_client_instance(session_path, api_id, api_hash, loop)
                
                # Store runtime info
                with _userbots_lock:
                    _userbots[phone] = {
                        'client': client,
                        'loop': loop,
                        'lock': threading.Lock(),
                        'thread': None  # Will be set when thread starts
                    }
                
                # Start client in its own thread
                thread = threading.Thread(
                    target=_run_loop,
                    args=(loop, phone),
                    name=f"UserBotThread-{phone}",
                    daemon=True
                )
                thread.start()
                
                # Store thread reference
                _userbots[phone]['thread'] = thread
                
                # Initial connection attempt
                try:
                    # Run connect in the client's event loop
                    future = asyncio.run_coroutine_threadsafe(_safe_connect(client, phone), loop)
                    connected = future.result(timeout=30)  # 30 second timeout
                    
                    if connected:
                        # Get user info
                        me_future = asyncio.run_coroutine_threadsafe(client.get_me(), loop)
                        me = me_future.result(timeout=30)
                        
                        if me:
                            # Update database with user info
                            db.update_userbot(phone, {
                                'username': me.username,
                                'user_id': me.id,
                                'status': 'active',
                                'last_error': None
                            })
                            log.info(f"Userbot {phone} initialized successfully.")
                        else:
                            log.error(f"Could not get user info for {phone}")
                            db.update_userbot_status(phone, 'error', "Failed to get user info")
                    else:
                        log.error(f"Could not connect userbot {phone}")
                        # Status already updated by _safe_connect
                
                except asyncio.TimeoutError:
                    log.error(f"Timeout while initializing userbot {phone}")
                    db.update_userbot_status(phone, 'error', "Initialization timeout")
                except Exception as e:
                    log.error(f"Error during userbot {phone} initialization: {e}", exc_info=True)
                    db.update_userbot_status(phone, 'error', f"Init error: {type(e).__name__}")
            
            except Exception as e:
                log.error(f"Failed to initialize userbot {phone}: {e}", exc_info=True)
                db.update_userbot_status(phone, 'error', f"Setup error: {type(e).__name__}")
                continue
    
    except Exception as e:
        log.error(f"Error during userbot initialization process: {e}", exc_info=True)
        return False
    
    return True

def shutdown_telethon():
    if _stop_event.is_set(): return
    log.info("Initiating Telethon shutdown sequence...")
    _stop_event.set()
    with _userbots_lock: phones = list(_userbots.keys())
    if not phones: time.sleep(1); return
    stopped_count = 0
    for phone in phones:
        if stop_userbot_runtime(phone): stopped_count += 1
    log.info(f"Telethon shutdown sequence finished. Stopped {stopped_count}/{len(phones)} runtimes.")
    time.sleep(1)

log.info("Telethon Utils module loaded.")
# --- END OF FILE telethon_utils.py ---
