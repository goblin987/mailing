# telethon_utils.py
import asyncio
import threading
import os
import random
import re
import time
from datetime import datetime

from telethon import TelegramClient
from telethon.tl.types import PeerChannel, PeerChat, PeerUser, InputPeerChannel, InputPeerChat, InputPeerUser, Channel, User as TelethonUser, Chat as TelethonChat # More specific imports
from telethon.errors import (
    SessionPasswordNeededError, FloodWaitError, ChatSendMediaForbiddenError,
    UserNotParticipantError, ChatAdminRequiredError, UserBannedInChatError,
    PhoneNumberInvalidError, PhoneCodeInvalidError, PhoneCodeExpiredError,
    PasswordHashInvalidError, ApiIdInvalidError, AuthKeyError, ConnectionError as TelethonConnectionError,
    UserDeactivatedBanError, UsernameNotOccupiedError, ChannelPrivateError, InviteHashExpiredError, InviteHashInvalidError,
    MessageIdInvalidError, PeerIdInvalidError, UserBlockedError, ChatWriteForbiddenError
)
from telethon.tl.functions.channels import JoinChannelRequest, GetFullChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest, GetMessagesRequest
from telethon.tl.functions.account import GetPasswordRequest

import database as db
from config import (
    SESSION_DIR, CLIENT_TIMEOUT, CHECK_TASKS_INTERVAL, UTC_TZ, log # Use logger from config
)
from translations import get_text # Only for very specific internal errors if needed

# --- Userbot Runtime Management ---
# Stores active Telethon client instances and their associated resources
# Structure: { 'phone_number': {'client': obj, 'loop': obj, 'lock': obj, 'thread': obj} }
_userbots = {}
_userbots_lock = threading.Lock() # Protects access to the _userbots dictionary

# Event to signal background threads (like the task checker) to stop gracefully
_stop_event = threading.Event()

# --- Helper Functions ---

def _get_session_path(phone):
    """Returns the absolute path for a userbot's session file."""
    safe_phone = re.sub(r'[^\d\+]', '', phone) # Basic sanitization for filename
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
        # Use system details that are less likely to cause suspicion if possible
        device_model="PC 64bit",
        system_version="Linux", # Render.com likely runs Linux
        app_version="1.0.0" # Your bot's version
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
        # Ensure loop cleanup when thread exits (run_forever ends or is stopped)
        log.info(f"Event loop stopping for {phone_for_log}...")
        try:
             # Run pending tasks and shutdown async generators
             if loop.is_running():
                  loop.run_until_complete(loop.shutdown_asyncgens())
             loop.close()
             log.info(f"Event loop closed cleanly for {phone_for_log}.")
        except Exception as close_e:
             log.error(f"Error closing event loop for {phone_for_log}: {close_e}")

async def _safe_connect(client: TelegramClient, phone: str) -> bool:
    """Connects the client if not connected, handling common errors and updating DB status."""
    if client.is_connected():
        # Optional: Add a periodic is_user_authorized check here? Might be too slow.
        # For now, assume connected means likely authorized unless specific errors occur.
        return True

    log.info(f"Connecting userbot {phone}...")
    db.update_userbot_status(phone, 'connecting')
    try:
        await client.connect()
        # Verify authorization after connection attempt
        if await client.is_user_authorized():
             log.info(f"Userbot {phone} connected and authorized.")
             # No need to set status to 'active' here, actions should update status based on outcome.
             return True
        else:
             # This means connected but not authorized, session likely invalid
             log.error(f"Userbot {phone} connected but NOT authorized. Session invalid?")
             db.update_userbot_status(phone, 'error', last_error="Session invalid - Not Authorized")
             # Disconnect? It's already disconnected implicitly if auth failed, but good practice:
             await _safe_disconnect(client, phone, update_db=False) # Don't update DB again here
             return False # Treat as failure
    except AuthKeyError:
        log.error(f"Authentication key error for {phone}. Session invalid. Deleting.")
        db.update_userbot_status(phone, 'error', last_error="Invalid session (AuthKeyError)")
        await _delete_session_file(phone) # Delete the bad session
        return False
    except (TelethonConnectionError, asyncio.TimeoutError, OSError) as e: # Catch more connection issues
        log.error(f"Connection failed for {phone}: {e}")
        db.update_userbot_status(phone, 'error', last_error=f"Connection Error: {e}")
        return False
    except Exception as e: # Catch unexpected errors during connection
        log.exception(f"Unexpected error connecting userbot {phone}: {e}", exc_info=True)
        db.update_userbot_status(phone, 'error', last_error=f"Unexpected connect error: {e}")
        return False

async def _safe_disconnect(client: TelegramClient, phone: str, update_db: bool = True):
    """Disconnects the client gracefully."""
    if client and client.is_connected():
        log.info(f"Disconnecting userbot {phone}...")
        try:
            await client.disconnect()
            log.info(f"Userbot {phone} disconnected.")
            if update_db:
                 # Update status? Maybe only if error occurred? Optional.
                 # db.update_userbot_status(phone, 'inactive') # Or keep last status?
                 pass
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
        # Check if already running and thread is alive
        if phone_number in _userbots:
            bot_info = _userbots[phone_number]
            if bot_info.get('thread') and bot_info['thread'].is_alive():
                log.debug(f"Returning existing runtime for {phone_number}")
                return bot_info
            else:
                log.warning(f"Thread for userbot {phone_number} found dead. Cleaning up.")
                # Clean up potentially problematic state
                loop = bot_info.get('loop')
                if loop and loop.is_running():
                     loop.call_soon_threadsafe(loop.stop)
                # Don't join thread here, assume it's dead already
                del _userbots[phone_number]
                # Continue to initialize fresh below

        # If not in memory or thread died, try to initialize from DB
        log.info(f"Attempting to initialize runtime for userbot {phone_number}...")
        userbot_db = db.find_userbot(phone_number)
        if not userbot_db:
            log.error(f"Userbot {phone_number} not found in database for runtime initialization.")
            return None

        # Do not initialize if bot is marked inactive (unless forced?)
        if userbot_db['status'] == 'inactive':
            log.warning(f"Skipping runtime initialization for inactive userbot {phone_number}.")
            return None

        # Proceed with initialization
        session_file = _get_session_path(phone_number)
        api_id = userbot_db['api_id']
        api_hash = userbot_db['api_hash']

        try:
            # 1. Create event loop
            loop = asyncio.new_event_loop()
            # 2. Create asyncio lock associated with this loop
            lock = asyncio.Lock() # loop param deprecated

            # 3. Create client instance (run coroutine in the new loop)
            # We need to run this *before* starting the thread to catch immediate client creation errors.
            # Create a temporary task to run the async function in the new loop.
            async def _create_client_task():
                return await _create_telethon_client_instance(session_file, api_id, api_hash, loop)
            future = asyncio.run_coroutine_threadsafe(_create_client_task(), loop)

            # 4. Start the thread AFTER initiating client creation
            thread = threading.Thread(target=_run_loop, args=(loop, phone_number), daemon=True)
            thread.start()

            # 5. Wait for client creation result (should be quick)
            client = future.result(timeout=CLIENT_TIMEOUT)
            log.info(f"Telethon client created for {phone_number}")

            # 6. Store runtime info
            bot_info = {'client': client, 'loop': loop, 'lock': lock, 'thread': thread}
            _userbots[phone_number] = bot_info
            db.update_userbot_status(phone_number, 'initializing') # Update status

            # 7. Optional: Run initial connection check async
            async def _initial_connect_check():
                 connected = await _safe_connect(client, phone_number)
                 if connected:
                      log.info(f"Initial connection check successful for {phone_number}")
                      me = await client.get_me()
                      db.update_userbot_status(phone_number, 'active', username=me.username if me else None)
                 else:
                      log.warning(f"Initial connection check failed for {phone_number}. Status updated by _safe_connect.")
                 # No need to disconnect after check, keep alive

            asyncio.run_coroutine_threadsafe(_initial_connect_check(), loop)

            log.info(f"Runtime initialized for userbot {phone_number}. Thread started.")
            return bot_info

        except Exception as e:
            log.critical(f"CRITICAL: Failed to initialize runtime for userbot {phone_number}: {e}", exc_info=True)
            db.update_userbot_status(phone_number, 'error', last_error=f"Runtime init failed: {e}")
            # Clean up resources if initialization failed partially
            if 'loop' in locals() and loop.is_running():
                loop.call_soon_threadsafe(loop.stop)
            if 'thread' in locals() and thread.is_alive():
                thread.join(timeout=2)
            # Remove partial entry if added
            if phone_number in _userbots:
                 del _userbots[phone_number]
            return None

def stop_userbot_runtime(phone_number):
    """Stops the event loop and disconnects a specific userbot."""
    with _userbots_lock:
         bot_info = _userbots.pop(phone_number, None) # Remove from active dict

    if bot_info:
        log.info(f"Stopping runtime for userbot {phone_number}...")
        client = bot_info.get('client')
        loop = bot_info.get('loop')
        thread = bot_info.get('thread')

        # Disconnect client safely (run in its own loop)
        if client and loop and loop.is_running():
            future = asyncio.run_coroutine_threadsafe(_safe_disconnect(client, phone_number), loop)
            try:
                future.result(timeout=CLIENT_TIMEOUT / 2) # Shorter timeout on stop
            except Exception as e:
                 log.warning(f"Error during disconnect future for {phone_number} on stop: {e}")

        # Stop the event loop
        if loop and loop.is_running():
            loop.call_soon_threadsafe(loop.stop)

        # Wait for the thread to finish
        if thread and thread.is_alive():
            log.debug(f"Waiting for thread {thread.name} to finish...")
            thread.join(timeout=5)
            if thread.is_alive():
                log.warning(f"Thread {thread.name} did not stop gracefully.")
            else:
                log.info(f"Thread {thread.name} stopped.")
        return True
    else:
        log.warning(f"Tried to stop runtime for {phone_number}, but it wasn't running.")
        return False


# --- Authentication Flow ---

async def start_authentication_flow(phone, api_id, api_hash):
    """
    Initiates the Telethon authentication process using a TEMPORARY client/loop.
    Does NOT store the runtime in the main _userbots dict yet.

    Returns:
        Tuple (status, data):
        - status: 'code_needed', 'password_needed', 'already_authorized', 'error'
        - data: Dict containing temp client, loop, and potentially phone_code_hash or pwd_state if auth proceeds.
                Or an error message string if status is 'error'.
    """
    session_file = _get_session_path(phone)
    # Ensure clean state: delete existing session before starting fresh auth
    await _delete_session_file(phone)

    # Use a temporary loop just for this auth sequence
    temp_loop = asyncio.new_event_loop()
    temp_client = None
    auth_result_status = 'error'
    auth_result_data = "Initialization failed"

    try:
        temp_client = await _create_telethon_client_instance(session_file, api_id, api_hash, temp_loop)
        log.info(f"Attempting connection for authentication: {phone}")

        # Try connecting first (optional but good practice before code request)
        # await temp_client.connect() # send_code_request handles connect

        # --- Code Request ---
        code_request_info = await temp_client.send_code_request(phone)
        log.info(f"Code request sent to {phone}. Phone code hash obtained.")
        auth_result_status = 'code_needed'
        auth_result_data = {'client': temp_client, 'loop': temp_loop, 'phone_code_hash': code_request_info.phone_code_hash}

    except SessionPasswordNeededError:
         # This indicates 2FA is enabled AND Telegram skipped the code step
         log.warning(f"Password needed immediately for {phone} (code step skipped by Telegram).")
         try:
              if not temp_client.is_connected(): await temp_client.connect() # Ensure connection
              pwd_state = await temp_client(GetPasswordRequest())
              log.info(f"Password required for {phone}. Hint: {getattr(pwd_state, 'hint', 'None')}")
              auth_result_status = 'password_needed'
              auth_result_data = {'client': temp_client, 'loop': temp_loop, 'pwd_state': pwd_state}
         except Exception as pwd_err:
              log.error(f"Error getting password state for {phone}: {pwd_err}")
              auth_result_status = 'error'
              auth_result_data = f"Failed to get password details: {pwd_err}"
    except FloodWaitError as e:
        log.warning(f"Flood wait during code request for {phone}: {e.seconds}s")
        auth_result_status = 'error'
        auth_result_data = f"Flood wait: Try again in {e.seconds} seconds."
    except (PhoneNumberInvalidError, ApiIdInvalidError, ApiIdPublishedFloodError) as e:
         log.error(f"Configuration/Phone error during code request for {phone}: {e}")
         auth_result_status = 'error'
         auth_result_data = f"Invalid configuration or phone number: {e}"
    except AuthKeyError:
         # Should not happen with deleted session, but handle defensively
         log.error(f"AuthKeyError during initial code request for {phone}. Session potentially corrupt?")
         auth_result_status = 'error'
         auth_result_data = "Authentication key error. Please try again."
    except (TelethonConnectionError, asyncio.TimeoutError, OSError) as e:
         log.error(f"Connection issue during code request for {phone}: {e}")
         auth_result_status = 'error'
         auth_result_data = f"Connection failed: {e}"
    except Exception as e:
        log.exception(f"Unexpected error during authentication start for {phone}: {e}", exc_info=True)
        auth_result_status = 'error'
        auth_result_data = f"An unexpected error occurred: {e}"

    # --- Cleanup or Keep Alive ---
    if auth_result_status == 'error':
        # If an error occurred, ensure the temporary client is disconnected and loop stopped.
        await _safe_disconnect(temp_client, phone, update_db=False) # Don't update DB for temp client
        if temp_loop and not temp_loop.is_closed():
            # Run loop briefly to allow disconnect task to finish if needed
            async def _stop_temp_loop():
                 temp_loop.stop()
            asyncio.run_coroutine_threadsafe(_stop_temp_loop(), temp_loop)
            # Need a thread to run this loop's cleanup? This gets complicated.
            # Simplest might be just closing here, though not ideal.
            try:
                 temp_loop.close()
                 log.debug("Temporary auth loop closed on error.")
            except Exception as loop_e:
                 log.error(f"Error closing temporary auth loop for {phone}: {loop_e}")
        auth_result_data = {'error_message': str(auth_result_data)} # Ensure data is just the message

    # If code_needed or password_needed, the client and loop are kept alive in auth_result_data
    # The calling handler MUST ensure cleanup later using complete_authentication_flow.
    return auth_result_status, auth_result_data


async def complete_authentication_flow(auth_data, code=None, password=None):
    """
    Completes the Telethon authentication using code or password with the temporary client/loop.
    If successful, updates DB and initializes the persistent runtime instance.

    Args:
        auth_data: The dictionary received from start_authentication_flow ('client', 'loop', 'hash'/'state').
        code: The verification code entered by the user.
        password: The 2FA password entered by the user.

    Returns:
        Tuple (status, data):
        - status: 'success', 'error'
        - data: Dict {'phone': ..., 'username': ...} if success, or error message string if error.
    """
    temp_client = auth_data.get('client')
    temp_loop = auth_data.get('loop')
    phone = "UnknownPhone" # Try to get phone later from 'me' object
    final_status = 'error'
    final_data = "Unknown authentication failure"

    if not temp_client or not temp_loop:
        return 'error', "Invalid authentication data provided (client or loop missing)."

    # Assign phone if possible (though might not be available until success)
    # if temp_client.session and temp_client.session.auth_key: phone = temp_client.session.auth_key.phone

    me_object = None
    try:
        if code:
            phone_code_hash = auth_data.get('phone_code_hash')
            if not phone_code_hash:
                return 'error', "Internal Error: Missing phone_code_hash for code sign-in."
            log.info(f"Attempting code sign-in for temporary auth...")
            me_object = await temp_client.sign_in(code=code, phone_code_hash=phone_code_hash)
        elif password:
            # We might need the password state (e.g., pwd_state.srp_id) from start_auth if using SRP
            # However, client.sign_in(password=...) often handles this internally if called sequentially.
            log.info(f"Attempting password sign-in for temporary auth...")
            me_object = await temp_client.sign_in(password=password)
        else:
            return 'error', "No code or password provided for completion step."

        # Check authorization status after sign_in attempt
        if me_object and await temp_client.is_user_authorized():
            phone = me_object.phone # Get correct phone number
            username = me_object.username
            session_file_path = temp_client.session.filename
            api_id = temp_client.api_id
            api_hash = temp_client.api_hash
            session_file_rel = os.path.relpath(session_file_path, SESSION_DIR) # Store relative path

            log.info(f"Authentication successful for {phone} (@{username}) via temporary client.")

            # Persist the authenticated session and bot info to DB
            db_add_ok = db.add_userbot(
                phone=phone,
                session_file_rel=session_file_rel,
                api_id=api_id,
                api_hash=api_hash,
                status='active', # Mark as active now
                username=username,
                assigned_client=None, # Not assigned to client during initial add
                last_error=None # Clear any previous error
            )

            if db_add_ok:
                final_status = 'success'
                final_data = {'phone': phone, 'username': username}
                log.info(f"Userbot {phone} info saved to database.")
                # Now, initialize the persistent runtime instance in the background
                # It might already exist if admin updated an existing bot, get_userbot_runtime_info handles this.
                get_userbot_runtime_info(phone) # This will create/update the entry in _userbots
            else:
                 final_status = 'error'
                 final_data = "Authentication succeeded, but failed to save userbot to database."
                 log.error(final_data)
                 # Critical error - session is likely valid but DB failed.
        else:
            # Should not happen if sign_in doesn't raise error, but handle defensively.
             log.error(f"Sign-in call completed for {phone} but client.is_user_authorized() is False.")
             final_status = 'error'
             final_data = "Sign-in completed but authorization failed."

    # --- Specific Error Handling for sign_in ---
    except SessionPasswordNeededError:
         # This means the user provided a CODE, but 2FA password is REQUIRED next.
         log.info(f"Password needed for {phone} after code entry.")
         # We cannot complete here, the admin panel needs to transition to ask for password.
         # This function should ideally not be called with CODE if password is required.
         # If it happens, signal back clearly.
         final_status = 'error' # Treat as error *in this completion step*
         final_data = "Password required. Please provide the password." # Caller needs to handle this state change.
    except (PhoneCodeInvalidError, PhoneCodeExpiredError):
         log.warning(f"Invalid/Expired code provided for {phone}.")
         final_status = 'error'
         final_data = "Invalid or expired code."
    except PasswordHashInvalidError:
         log.warning(f"Incorrect password provided for {phone}.")
         final_status = 'error'
         final_data = "Incorrect password."
    except FloodWaitError as e:
        log.warning(f"Flood wait during sign-in completion for {phone}: {e.seconds}s")
        final_status = 'error'
        final_data = f"Flood wait: Try again in {e.seconds} seconds."
    except AuthKeyError:
         # Should not happen if connect worked initially, but could occur
         log.error(f"AuthKeyError during sign-in completion for {phone}. Session corrupt?")
         final_status = 'error'
         final_data = "Authentication key error. Session might be corrupt."
         await _delete_session_file(phone) # Delete potentially corrupt session
    except UserDeactivatedBanError as e:
         log.error(f"Account issue during sign-in for {phone}: Account Banned/Deactivated. {e}")
         db.update_userbot_status(phone, 'error', last_error="Account Banned/Deactivated") # Mark in DB
         final_status = 'error'
         final_data = f"Account issue: Banned or Deactivated."
    except (TelethonConnectionError, asyncio.TimeoutError, OSError) as e:
         log.error(f"Connection issue during sign-in completion for {phone}: {e}")
         final_status = 'error'
         final_data = f"Connection failed during sign-in: {e}"
    except Exception as e:
        log.exception(f"Unexpected error during authentication completion for {phone}: {e}", exc_info=True)
        final_status = 'error'
        final_data = f"An unexpected error occurred: {e}"

    # --- Cleanup Temporary Resources ---
    log.debug(f"Cleaning up temporary auth resources for {phone} (Status: {final_status}).")
    await _safe_disconnect(temp_client, phone, update_db=False)
    if temp_loop and not temp_loop.is_closed():
        try:
            # Schedule loop stop and wait briefly for it to process
            async def _stop_temp(): temp_loop.stop()
            asyncio.run_coroutine_threadsafe(_stop_temp(), temp_loop)
            # Give loop a moment to potentially close connections from disconnect
            # Running the loop briefly in a temp thread is too complex here.
            # Closing directly might leave resources hanging but is simplest for temp loop.
            temp_loop.close()
            log.debug(f"Temporary auth loop for {phone} closed.")
        except Exception as loop_e:
             log.error(f"Error closing temporary auth loop for {phone}: {loop_e}")

    return final_status, final_data


# --- Other Telethon Actions (Joining, Forwarding etc.) ---

# (Add refined versions of join_groups_batch, get_joined_chats_telethon,
#  run_scheduled_tasks, _execute_single_task, get_message_entity_by_link etc. here,
#  using the persistent runtime instances obtained via get_userbot_runtime_info)

async def join_groups_batch(phone, urls):
    """
    Manages joining multiple Telegram groups/channels for a specified userbot.

    Args:
        phone: The phone number of the userbot.
        urls: A list of Telegram group/channel URLs to join.

    Returns:
        A tuple: (error_info, results_dict)
        - error_info: Dictionary with an 'error' key if a fatal error occurred, else {}.
        - results_dict: Dictionary {url: (status, detail_dict_or_msg)}
          - status: 'success', 'already_member', 'pending', 'failed', 'flood_wait'
          - detail_dict_or_msg: Dict {'id': ..., 'name': ...} on success/already_member,
                                string error message/reason otherwise.
    """
    runtime_info = get_userbot_runtime_info(phone)
    if not runtime_info:
        return {"error": "Userbot runtime not available."}, {}

    client = runtime_info['client']
    loop = runtime_info['loop']
    lock = runtime_info['lock']
    bot_display = phone # Placeholder, updated if possible

    async def _join_batch_task():
        results = {}
        nonlocal bot_display
        async with lock: # Ensure thread safety for this bot's client
            try:
                # 1. Ensure Connection and Authorization
                if not await _safe_connect(client, phone):
                    raise ConnectionError("Failed to connect userbot for joining.")
                # Update display name
                try: me = await client.get_me(); bot_display = f"@{me.username}" if me.username else phone
                except: pass

                # 2. Process URLs one by one
                log.info(f"{bot_display}: Starting join process for {len(urls)} URLs.")
                for url in urls:
                    if not url: continue
                    status, detail = "failed", "Unknown processing error"
                    start_time = time.monotonic()
                    try:
                        log.debug(f"{bot_display}: Processing URL: {url}")
                        # a. Parse and resolve URL type
                        link_type, identifier = parse_telegram_url_simple(url) # Basic parsing first

                        if link_type == "unknown":
                            raise ValueError("Invalid or unrecognized URL format")

                        # b. Handle Invite Links (implicitly joins)
                        entity = None
                        if link_type == "private_join":
                            log.info(f"{bot_display}: Attempting join via invite hash: {identifier}")
                            updates = await client(ImportChatInviteRequest(identifier))
                            if updates and updates.chats:
                                entity = updates.chats[0]
                                status, detail = "success", _format_entity_detail(entity)
                            else:
                                raise InviteHashInvalidError("Import request returned no chat info.")

                        # c. Handle Public Links / IDs
                        elif link_type == "public":
                            log.info(f"{bot_display}: Resolving public identifier: {identifier}")
                            try:
                                entity = await client.get_entity(identifier)
                            except UsernameNotOccupiedError:
                                raise ValueError(f"Public username/link '{identifier}' not found.")
                            except ValueError: # Catch other get_entity value errors
                                 raise ValueError(f"Could not resolve public link '{identifier}'.")

                            if not isinstance(entity, (Channel, TelethonChat)): # Only join channels/chats
                                 raise ValueError("Link points to a user, not a group/channel.")

                            log.info(f"{bot_display}: Attempting JoinChannel for {getattr(entity,'title','N/A')} ({entity.id})")
                            await client(JoinChannelRequest(entity))
                            status, detail = "success", _format_entity_detail(entity)

                        # Should not happen if parse_telegram_url_simple worked, but defensive check
                        else:
                            raise ValueError("Unsupported link type for joining.")

                    # --- Error Handling for single URL ---
                    except (InviteHashExpiredError, InviteHashInvalidError):
                        status, detail = "failed", {"reason": "invalid_invite"}
                    except ChannelPrivateError: # Cannot join private channel via public link maybe
                        status, detail = "failed", {"reason": "private"}
                    except UserBannedInChatError:
                        status, detail = "failed", {"reason": "banned"}
                    except UserNotParticipantError: # Join request likely successful if this happens after JoinChannel
                        status, detail = "already_member", _format_entity_detail(entity) if entity else {"reason": "already_member_unresolved"}
                    except ChatAdminRequiredError: # For channels requiring admin approval
                        status, detail = "pending", {"reason": "admin_approval"}
                    except FloodWaitError as e:
                        wait_time = min(e.seconds + random.uniform(1, 3), 90) # Cap wait
                        log.warning(f"{bot_display}: Flood wait ({e.seconds}s) joining {url}. Sleeping {wait_time:.1f}s.")
                        await asyncio.sleep(wait_time)
                        status, detail = "flood_wait", {"seconds": e.seconds}
                        # Continue processing next URL after waiting
                    except ValueError as e: # Catch resolution/parsing errors
                        status, detail = "failed", {"reason": f"invalid_link: {e}"}
                    except (AuthKeyError, TelethonConnectionError, asyncio.TimeoutError, OSError) as e:
                        # Fatal connection error, abort the whole batch for this bot
                        log.error(f"{bot_display}: Connection error during join for {url}: {e}. Aborting batch.")
                        db.update_userbot_status(phone, 'error', last_error=f"Connection Error: {e}")
                        raise ConnectionError(f"Connection failed: {e}") # Propagate to stop batch
                    except UserDeactivatedBanError as e:
                         log.critical(f"{bot_display}: Account banned/deactivated during join: {e}. Aborting.")
                         db.update_userbot_status(phone, 'error', last_error="Account Banned/Deactivated")
                         raise e # Propagate fatal error
                    except Exception as e:
                        log.exception(f"{bot_display}: Unexpected error joining {url}: {e}", exc_info=True)
                        status, detail = "failed", {"reason": f"internal_error: {e}"}

                    results[url] = (status, detail)
                    log.info(f"{bot_display}: Result for {url}: {status} (took {time.monotonic()-start_time:.2f}s)")

                    # d. Delay before next URL (crucial!)
                    base_delay = 3.0 # Minimum seconds
                    jitter = 2.0   # Random seconds to add/subtract
                    await asyncio.sleep(max(0.5, base_delay + random.uniform(-jitter/2, jitter)))

            except ConnectionError as e: # Catch propagated connection error
                 return {"error": f"Connection Error: {e}"}, results
            except UserDeactivatedBanError as e: # Catch propagated ban error
                 return {"error": "Userbot Account Banned/Deactivated"}, results
            except Exception as e: # Catch unexpected errors in the outer loop
                 log.exception(f"{bot_display}: Unexpected error in _join_batch_task: {e}", exc_info=True)
                 return {"error": f"Unexpected Batch Error: {e}"}, results

            # Batch completed without fatal error for this bot
            log.info(f"{bot_display}: Finished join batch task.")
            return {}, results # No fatal error, return results

    # --- Run the batch task in the userbot's loop ---
    if not loop or loop.is_closed():
         log.error(f"Cannot run join batch for {phone}: Event loop is not available.")
         return {"error": "Userbot event loop unavailable"}, {}

    future = asyncio.run_coroutine_threadsafe(_join_batch_task(), loop)
    try:
        # Calculate a reasonable timeout
        timeout = (len(urls) * (CLIENT_TIMEOUT / 2 + 5)) + 30 # Base + per-url time
        error_info, results_dict = future.result(timeout=timeout)
        return error_info, results_dict
    except asyncio.TimeoutError:
        log.error(f"Timeout waiting for join batch result for {phone}.")
        # Mark remaining unprocessed URLs as timeout
        processed_urls = results_dict.keys() if 'results_dict' in locals() else []
        final_results = locals().get('results_dict', {})
        for url in urls:
            if url not in processed_urls: final_results[url] = ("failed", {"reason": "batch_timeout"})
        return {"error": "Batch join operation timed out."}, final_results
    except Exception as e:
        # Error retrieving result from future (e.g., exception propagated from task)
        log.exception(f"Exception waiting for join batch future result for {phone}: {e}", exc_info=True)
        final_results = locals().get('results_dict', {})
        for url in urls:
            if url not in final_results: final_results[url] = ("failed", {"reason": f"internal_error: {e}"})
        # If the error itself was the useful info:
        error_info = {"error": str(e)} if not locals().get('error_info') else locals()['error_info']
        return error_info, final_results


def parse_telegram_url_simple(url: str) -> tuple:
    """Simplified URL parsing for join flow. Returns (type, identifier)."""
    url = url.strip()
    # New private join link: https://t.me/+HASH
    if match := re.match(r"https?://t\.me/\+([\w\d_-]+)/?", url):
        return "private_join", match.group(1)
    # Old private join link: https://t.me/joinchat/HASH
    if match := re.match(r"https?://t\.me/joinchat/([\w\d_-]+)/?", url):
        return "private_join", match.group(1)
    # Public link: https://t.me/username_or_channelname
    if match := re.match(r"https?://t\.me/([\w\d_]{5,})/?$", url): # Min 5 chars for username/channel
        return "public", match.group(1)
    # Message link (treat as invalid for joining groups)
    if "/c/" in url or re.match(r'https?://t\.me/\w+/\d+', url):
        return "message_link", url # Mark as message link type
    return "unknown", url # Unrecognized format

def _format_entity_detail(entity) -> dict:
    """Formats Telethon entity into a standard detail dictionary."""
    if not entity: return {}
    return {
        "id": entity.id,
        "name": getattr(entity, 'title', getattr(entity, 'username', f"ID {entity.id}")),
        "username": getattr(entity, 'username', None)
    }

# --- Get Joined Groups ---
async def get_joined_chats_telethon(phone):
    """Retrieves joined chats (groups/channels) for a userbot."""
    runtime_info = get_userbot_runtime_info(phone)
    if not runtime_info:
        return None, {"error": "Userbot runtime not available."}

    client = runtime_info['client']
    loop = runtime_info['loop']
    lock = runtime_info['lock']
    bot_display = phone # Placeholder

    async def _get_dialogs_task():
        dialog_list = []
        error_msg = None
        nonlocal bot_display
        async with lock:
            try:
                if not await _safe_connect(client, phone):
                    raise ConnectionError("Failed to connect userbot.")
                try: me = await client.get_me(); bot_display = f"@{me.username}" if me.username else phone
                except: pass

                log.info(f"{bot_display}: Fetching dialogs (limit 500)...")
                dialog_count = 0
                # Use iter_dialogs for potentially large number of chats
                async for dialog in client.iter_dialogs(limit=500): # Limit to avoid excessive load?
                    dialog_count += 1
                    if dialog.is_group or dialog.is_channel:
                        entity = dialog.entity
                        link = None
                        id_part = None
                        if getattr(entity, 'username', None):
                            link = f"https://t.me/{entity.username}"
                        elif isinstance(entity, Channel): # Check if it's a Channel object
                             # Create 'c' link if possible (usually for supergroups/channels)
                             id_part_str = str(entity.id)
                             if id_part_str.startswith('-100'): # Standard format for channels
                                 id_part = id_part_str[4:]
                                 link = f"https://t.me/c/{id_part}/1" # Link to first message guess

                        group_info = {
                             'id': entity.id,
                             'name': getattr(entity, 'title', f'ID: {entity.id}'),
                             'username': getattr(entity, 'username', None),
                             'link': link, # May be None
                             'type': 'channel' if dialog.is_channel else 'group'
                        }
                        dialog_list.append(group_info)
                log.info(f"{bot_display}: Fetched {dialog_count} dialogs, found {len(dialog_list)} groups/channels.")

            except ConnectionError as e:
                error_msg = f"Connection Error: {e}"
            except AuthKeyError:
                 error_msg = "Invalid session."
                 db.update_userbot_status(phone, 'error', last_error="Invalid session (AuthKeyError)")
            except UserDeactivatedBanError:
                 error_msg = "Account Banned/Deactivated."
                 db.update_userbot_status(phone, 'error', last_error=error_msg)
            except FloodWaitError as e:
                 error_msg = f"Flood wait ({e.seconds}s)"
            except Exception as e:
                 log.exception(f"{bot_display}: Error fetching dialogs: {e}", exc_info=True)
                 error_msg = f"Unexpected error fetching groups: {e}"
            # Keep client connected
            return dialog_list, {"error": error_msg} if error_msg else {}

    # --- Run task in userbot's loop ---
    if not loop or loop.is_closed():
        return None, {"error": "Userbot event loop unavailable."}
    future = asyncio.run_coroutine_threadsafe(_get_dialogs_task(), loop)
    try:
        dialogs, error_info = future.result(timeout=120) # Timeout for fetching dialogs
        return dialogs, error_info # error_info is {} if no error
    except asyncio.TimeoutError:
        return None, {"error": "Operation timed out."}
    except Exception as e:
        return None, {"error": f"Internal task error: {e}"}


# --- Message Link Parsing ---
async def get_message_entity_by_link(client, link):
    """
    Parses Telegram message links and returns source chat entity and message ID.
    Handles connection and basic validation. Requires connected client.
    Raises ValueError, ConnectionError, FloodWaitError etc. on failure.
    """
    link = link.strip()
    log.debug(f"Attempting to parse message link: {link}")

    # 1. Simple Regex Parsing
    private_match = re.match(r'https?://t\.me/c/(\d+)/(\d+)', link)
    public_match = re.match(r'https?://t\.me/([\w\d_]+)/(\d+)', link) # Allows username/public channel

    chat_identifier = None
    message_id = None
    link_type = 'unknown'

    if private_match:
        link_type = 'private'
        # Use negative ID format standard in Telethon for channels/supergroups
        chat_identifier = int(f"-100{private_match.group(1)}")
        message_id = int(private_match.group(2))
    elif public_match:
        link_type = 'public'
        chat_identifier = public_match.group(1) # Username or public channel name
        message_id = int(public_match.group(2))
    else:
        raise ValueError("Invalid message link format.")

    log.debug(f"Parsed link: Type={link_type}, Identifier='{chat_identifier}', MsgID={message_id}")

    # 2. Resolve Entity and Verify Message (Requires connection)
    try:
        # Connection should be handled by caller (_execute_single_task)
        if not client.is_connected():
            raise ConnectionError("Client not connected for message link resolution.")

        chat_entity = await client.get_entity(chat_identifier)
        if not chat_entity:
             # Should raise ValueError from get_entity if not found, but double-check
             raise ValueError(f"Could not resolve chat entity '{chat_identifier}'.")

        log.debug(f"Resolved chat entity: ID={chat_entity.id}, Type={type(chat_entity)}")

        # 3. Basic Message ID Validation (Optional but good)
        # This confirms the message *exists*, doesn't guarantee forwardability
        # Disabled for now to save API calls, rely on forward errors.
        # log.debug(f"Verifying message {message_id} in chat {chat_entity.id}...")
        # msg_check = await client.get_messages(chat_entity, ids=message_id)
        # if not msg_check:
        #      raise MessageIdInvalidError(f"Message {message_id} not found in chat.")
        # log.debug("Message verified.")

        return chat_entity, message_id

    except MessageIdInvalidError as e: # Catch specific error if verification enabled
         log.error(f"Message ID validation failed for link {link}: {e}")
         raise ValueError(f"Message ID {message_id} appears invalid.") from e
    except (ValueError, TypeError) as e: # From get_entity or ID conversion
         log.error(f"Failed to resolve entity '{chat_identifier}' from link {link}: {e}")
         raise ValueError(f"Could not resolve source chat from link: {e}") from e
    # Let other errors like FloodWaitError, ConnectionError propagate up


# --- Forwarding Logic ---
async def _forward_single_message(client, target_peer, source_chat_entity, message_id, fallback_chat_entity=None, fallback_message_id=None):
    """
    Forwards a single message, handling errors and optional fallback.
    Returns: (bool_success, error_message_or_None)
    """
    phone = getattr(client.session.auth_key, 'phone', 'UnknownBot') # Get phone for logs if possible
    target_id = getattr(target_peer, 'id', str(target_peer)) # For logging
    log.debug(f"[{phone}] Attempting forward to TargetID={target_id} from ChatID={source_chat_entity.id}/MsgID={message_id}")
    forward_ok = False
    error_reason = None

    try:
        await client.forward_messages(target_peer, message_id, source_chat_entity)
        log.info(f"[{phone}] -> Forward SUCCESS to TargetID={target_id}")
        forward_ok = True
    except ChatSendMediaForbiddenError:
        log.warning(f"[{phone}] -> Media forbidden in TargetID={target_id}. Trying fallback...")
        if fallback_chat_entity and fallback_message_id:
            try:
                await client.forward_messages(target_peer, fallback_message_id, fallback_chat_entity)
                log.info(f"[{phone}] -> Fallback SUCCESS to TargetID={target_id}")
                forward_ok = True # Consider fallback success as overall success
                error_reason = "Used fallback" # Indicate fallback was used
            except ChatSendMediaForbiddenError:
                error_reason = "Media forbidden (Primary & Fallback)"
                log.error(f"[{phone}] -> Fallback FAILED for TargetID={target_id}: {error_reason}")
            except Exception as fb_e:
                error_reason = f"Fallback failed: {type(fb_e).__name__}"
                log.exception(f"[{phone}] -> Fallback FAILED for TargetID={target_id}: {fb_e}", exc_info=False)
        else:
            error_reason = "Media forbidden (No fallback)"
            log.warning(f"[{phone}] -> Forward FAILED for TargetID={target_id}: {error_reason}")
    except FloodWaitError as e:
        error_reason = f"Flood Wait ({e.seconds}s)"
        log.warning(f"[{phone}] -> Flood wait sending to TargetID={target_id}. Waiting {e.seconds}s.")
        await asyncio.sleep(e.seconds + random.uniform(0.5, 1.5)) # Wait and continue next target
    except (ChatWriteForbiddenError, UserBannedInChatError, UserBlockedError): # Permissions issues
         error_reason = "Permission denied (Banned/Blocked/WriteForbidden)"
         log.warning(f"[{phone}] -> Permission denied forwarding to TargetID={target_id}.")
    except UserNotParticipantError: # Bot not in target chat
         error_reason = "Not in group/channel"
         log.warning(f"[{phone}] -> Not participant in TargetID={target_id}.")
         # Maybe attempt rejoin here? Or let user handle. For now, just log fail.
    except (PeerIdInvalidError, ChannelPrivateError): # Target chat doesn't exist or inaccessible
         error_reason = "Target chat invalid/private"
         log.warning(f"[{phone}] -> Target chat TargetID={target_id} is invalid or private.")
    except MessageIdInvalidError: # Source message deleted?
         error_reason = "Source message invalid/deleted"
         log.error(f"[{phone}] -> Source message ChatID={source_chat_entity.id}/MsgID={message_id} is invalid.")
         # This is critical for the task, maybe stop the whole run? Raise specific error?
         raise ValueError(error_reason) # Propagate to stop task run
    except (AuthKeyError, TelethonConnectionError, asyncio.TimeoutError, OSError) as e:
        error_reason = f"Connection/Session Error: {type(e).__name__}"
        log.error(f"[{phone}] -> Connection/Session error forwarding to TargetID={target_id}: {e}")
        # This is critical, stop the task run by re-raising
        raise ConnectionError(error_reason) from e
    except UserDeactivatedBanError as e:
         error_reason = "Account Banned/Deactivated"
         log.critical(f"[{phone}] -> Userbot account issue forwarding: {e}. ABORTING TASK.")
         db.update_userbot_status(phone, 'error', last_error=error_reason)
         raise e # Propagate fatal error
    except Exception as e:
         error_reason = f"Unexpected error: {type(e).__name__}"
         log.exception(f"[{phone}] -> Unexpected error forwarding to TargetID={target_id}: {e}", exc_info=True)

    return forward_ok, error_reason

# --- Main Task Execution Logic ---
async def _execute_single_task(instance, task_info):
    """The core logic for executing one forwarding task instance."""
    client = instance["client"]
    lock = instance["lock"] # Use the bot's specific lock
    phone = task_info['userbot_phone']
    client_id = task_info['client_id']
    run_timestamp = int(datetime.now(UTC_TZ).timestamp())
    log.info(f"[{phone}] Task triggered for client {client_id}. Acquiring lock...")

    task_success_count = 0
    task_target_count = 0
    final_task_error = None # Store first critical error for DB update

    try:
        async with lock: # Ensures only one critical action per bot at a time
            log.info(f"[{phone}] Lock acquired. Starting task execution.")

            # 1. Ensure Connection and Authorization
            if not await _safe_connect(client, phone):
                raise ConnectionError("Task failed: Could not connect userbot.")

            # 2. Resolve Message Links
            primary_link = task_info['message_link']
            fallback_link = task_info['fallback_message_link']
            try:
                source_chat, source_msg_id = await get_message_entity_by_link(client, primary_link)
                log.info(f"[{phone}] Resolved primary message: Chat={getattr(source_chat, 'id', 'N/A')}, MsgID={source_msg_id}")
            except (ValueError, MessageIdInvalidError) as e:
                raise ValueError(f"Task failed: Primary message link invalid '{primary_link}'. Error: {e}") from e

            fb_chat, fb_msg_id = None, None
            if fallback_link:
                try:
                    fb_chat, fb_msg_id = await get_message_entity_by_link(client, fallback_link)
                    log.info(f"[{phone}] Resolved fallback message: Chat={getattr(fb_chat, 'id', 'N/A')}, MsgID={fb_msg_id}")
                except Exception as fb_err:
                    log.warning(f"[{phone}] Failed to resolve fallback link '{fallback_link}', proceeding without: {fb_err}")

            # 3. Determine Target Groups
            target_ids_to_process = []
            folder_name = "N/A"
            if task_info['send_to_all_groups']:
                log.info(f"[{phone}] Targeting all joined groups/channels...")
                # Fetching dialogs live can be slow/rate-limited. Consider caching?
                dialogs, err_info = await get_joined_chats_telethon(phone)
                if err_info: raise ConnectionError(f"Task failed: Could not get target groups: {err_info.get('error')}")
                target_ids_to_process = [d['id'] for d in dialogs if d.get('id')]
                log.info(f"[{phone}] Targeting {len(target_ids_to_process)} groups/channels from dialogs.")
            else:
                folder_id = task_info['folder_id']
                if folder_id:
                    folder_name = db.get_folder_name(folder_id) or f"ID {folder_id}"
                    log.info(f"[{phone}] Targeting folder '{folder_name}' (ID: {folder_id})")
                    target_ids_to_process = db.get_target_groups_by_folder(folder_id)
                    log.info(f"[{phone}] Targeting {len(target_ids_to_process)} groups from folder DB.")
                    if target_ids_to_process and folder_name == f"ID {folder_id}": # Folder name wasn't found -> deleted?
                         log.warning(f"[{phone}] Task targets folder ID {folder_id}, but folder seems deleted. Task might fail.")
                         # Task should fail gracefully in forwarding loop if folder_id became NULL in settings table due to cascade
                else:
                    # Task is active but has neither folder nor send_to_all -> invalid state
                    raise ValueError("Task configuration error: No target folder and not set to send to all.")

            task_target_count = len(target_ids_to_process)
            if task_target_count == 0:
                log.warning(f"[{phone}] No target groups identified for task. Finishing run.")
                # Treat as success with 0 messages sent
            else:
                log.info(f"[{phone}] Starting forward loop for {task_target_count} targets...")
                processed_targets = set() # Avoid duplicate forwards if ID appears twice somehow
                delay_between_forwards = max(1.0, 30 / task_target_count) # Simple dynamic delay (e.g., 30s total / targets) capped min 1s
                delay_between_forwards = min(delay_between_forwards, 5.0) # Cap max delay too

                for target_id in target_ids_to_process:
                     if _stop_event.is_set(): # Check if shutdown was requested
                          log.info(f"[{phone}] Shutdown signal received, stopping task run early.")
                          final_task_error = "Shutdown requested"
                          break
                     if not target_id or target_id in processed_targets: continue # Skip invalid/duplicate IDs

                     try:
                         # Resolve target ID to peer object
                         # Use client.get_input_entity to get InputPeer types if needed,
                         # but client.forward_messages often accepts IDs or Peer objects.
                         target_peer = await client.get_entity(target_id)
                         if not target_peer: raise ValueError("Could not resolve target peer")

                         # Execute forward with error handling and optional fallback
                         forward_success, fwd_err_msg = await _forward_single_message(
                              client, target_peer, source_chat, source_msg_id, fb_chat, fb_msg_id
                         )

                         if forward_success:
                             task_success_count += 1
                         elif fwd_err_msg and final_task_error is None:
                             # Store the first significant error message encountered in this run
                              if "Flood Wait" not in fwd_err_msg and "Connection/Session Error" not in fwd_err_msg:
                                   final_task_error = f"Target {target_id}: {fwd_err_msg}"

                         processed_targets.add(target_id)

                     except FloodWaitError as e_outer: # Should be caught inside _forward, but handle here too
                          final_task_error = f"Flood Wait ({e_outer.seconds}s)"
                          log.warning(f"[{phone}] Outer loop flood wait. Aborting current task run.")
                          break # Stop processing more targets for this run
                     except ConnectionError as e_outer: # Propagated critical errors
                          final_task_error = str(e_outer)
                          log.critical(f"[{phone}] Connection/Auth error during task. Aborting run.")
                          raise # Re-raise to be caught by outer try/except
                     except UserDeactivatedBanError as e_outer:
                          final_task_error = "Account Banned/Deactivated"
                          log.critical(f"[{phone}] Userbot banned/deactivated. Aborting run.")
                          raise # Re-raise fatal error
                     except ValueError as e_outer: # E.g., source message invalid or target resolve failed
                          final_task_error = f"Config/Resolve error: {e_outer}"
                          log.error(f"[{phone}] {final_task_error}. Aborting run.")
                          break # Stop processing more targets for this run
                     except Exception as e_outer:
                          final_task_error = f"Unexpected task loop error: {e_outer}"
                          log.exception(f"[{phone}] {final_task_error}", exc_info=True)
                          # Maybe continue to next target? For safety, let's abort run.
                          break

                     # Wait before next forward
                     await asyncio.sleep(delay_between_forwards + random.uniform(0, 0.5)) # Add jitter

    # --- Handle Task Completion or Failure ---
    except (ValueError, ConnectionError, UserDeactivatedBanError) as critical_error:
        # Catch errors raised explicitly to stop the task run
        log.error(f"[{phone}] Task execution CRITICAL FAILURE: {critical_error}")
        final_task_error = final_task_error or str(critical_error) # Ensure error is recorded
    except Exception as outer_exception:
        # Catch any other unexpected errors during the locked operation
        log.exception(f"[{phone}] Unexpected exception during task execution: {outer_exception}", exc_info=True)
        final_task_error = f"Unexpected error: {outer_exception}"
    finally:
        # Update the database regardless of success or failure
        log.info(f"[{phone}] Task finished. Sent: {task_success_count}/{task_target_count}. Error: {final_task_error}")
        db.update_task_after_run(client_id, phone, run_timestamp, task_success_count, error=final_task_error)
        log.info(f"[{phone}] Lock released.")
        # Lock released by async with block ending


# --- Background Task Scheduler ---
async def run_check_tasks_periodically():
    """The main async function that periodically checks and schedules tasks."""
    log.info("Background task checker service started.")
    await asyncio.sleep(15) # Initial delay before first check cycle

    while not _stop_event.is_set():
        start_check_time = datetime.now(UTC_TZ)
        log.info(f"Task Check Cycle Started at {start_check_time.isoformat()}")
        current_ts = int(start_check_time.timestamp())
        scheduled_count = 0

        try:
            tasks_to_run = db.get_active_tasks_to_run(current_ts)
            log.info(f"Found {len(tasks_to_run)} tasks due for execution.")

            if tasks_to_run:
                for task in tasks_to_run:
                     if _stop_event.is_set(): break # Check stop event before scheduling next task

                     phone = task['userbot_phone']
                     client_id = task['client_id']

                     # Get/Initialize the runtime instance for the bot needed for the task
                     runtime_info = get_userbot_runtime_info(phone)
                     if not runtime_info:
                           log.warning(f"Skipping task for bot {phone}, client {client_id}: Runtime not available or failed initialization.")
                           # Record the skipped run with an error?
                           db.update_task_after_run(client_id, phone, current_ts, 0, error="Skipped - Userbot runtime unavailable")
                           continue

                     # Schedule the actual task execution coroutine in the bot's loop
                     log.debug(f"Scheduling task execution for bot {phone}, client {client_id} in its loop.")
                     asyncio.run_coroutine_threadsafe(
                         _execute_single_task(runtime_info, task),
                         runtime_info["loop"]
                     )
                     scheduled_count += 1
                     # Add a small delay between *scheduling* tasks for different bots to prevent thundering herd?
                     await asyncio.sleep(random.uniform(0.05, 0.2))

        except sqlite3.Error as db_e:
            log.exception(f"Database error during task check cycle: {db_e}", exc_info=True)
        except Exception as e:
             log.exception(f"Unexpected error in task checking loop: {e}", exc_info=True)

        if _stop_event.is_set(): break # Exit loop immediately if stop signal received

        # Calculate wait time for the next cycle
        elapsed_time = (datetime.now(UTC_TZ) - start_check_time).total_seconds()
        wait_time = max(5.0, CHECK_TASKS_INTERVAL - elapsed_time) # Minimum 5s wait
        log.info(f"Task Check Cycle Finished. Scheduled: {scheduled_count}. Elapsed: {elapsed_time:.2f}s. Next check in {wait_time:.2f}s.")
        await asyncio.sleep(wait_time)

    log.info("Background task checker stopped.")


# --- Initialization and Shutdown ---

def initialize_all_userbots():
    """Loads all non-inactive userbots from DB and initializes their runtime on startup."""
    log.info("Initializing runtime for all potentially active userbots from database...")
    all_bots = db.get_all_userbots(assigned_status=None) # Get all bots
    initialized_count = 0
    for bot_data in all_bots:
        if bot_data['status'] != 'inactive': # Attempt to initialize if not explicitly inactive
             phone = bot_data['phone_number']
             log.debug(f"Initializing runtime for {phone} (Status: {bot_data['status']})...")
             # get_userbot_runtime_info handles the creation/check logic
             info = get_userbot_runtime_info(phone)
             if info:
                 initialized_count += 1
        # else: # Log inactive skips if needed
             # log.debug(f"Skipping inactive userbot: {bot_data['phone_number']}")

    log.info(f"Finished initial runtime initialization. Attempted/Verified {initialized_count} userbot runtimes.")


def start_background_tasks():
    """Starts the run_check_tasks_periodically loop in the main asyncio loop."""
    log.info("Starting background task checking service...")
    # Get the main event loop (should be running from main.py)
    try:
        loop = asyncio.get_running_loop()
        # Create the task in the main loop
        loop.create_task(run_check_tasks_periodically())
        log.info("Background task checker scheduled in main event loop.")
    except RuntimeError:
        log.critical("CRITICAL: No running event loop found to schedule background tasks.")
        # This indicates a problem with the main bot setup
    except Exception as e:
        log.critical(f"CRITICAL: Failed to schedule background tasks: {e}", exc_info=True)


def shutdown_telethon():
    """Signals background tasks to stop and disconnects all active userbots."""
    log.info("Initiating Telethon shutdown sequence...")
    _stop_event.set() # Signal the background task loop to stop checking

    # Create a copy of keys to avoid modification issues during iteration
    with _userbots_lock:
        active_phones = list(_userbots.keys())

    if not active_phones:
        log.info("No active userbot runtimes to shut down.")
        return

    log.info(f"Stopping runtime for {len(active_phones)} userbots...")
    # Use a list to track shutdown tasks if needed for waiting
    # shutdown_tasks = []
    for phone in active_phones:
        # stop_userbot_runtime handles removing from _userbots dict
        if stop_userbot_runtime(phone):
            log.debug(f"Successfully initiated stop for {phone}.")
        # else: log already warned if stop failed

    # Optionally: Wait for all threads to actually finish?
    # This might delay main bot shutdown significantly. For now, we just signal stop and join briefly.
    log.info("Telethon shutdown sequence initiated.")

# Log on import completion
log.info("Telethon Utils module loaded.")