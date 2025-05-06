# --- START OF FILE database.py ---

# database.py
import sqlite3
import threading
from datetime import datetime, timedelta
import os
from config import DB_PATH, UTC_TZ, SESSION_DIR, log # Import constants and logger
import uuid

# Use a reentrant lock to allow the same thread to acquire the lock multiple times if needed
# Useful if one DB function calls another within the same thread.
db_lock = threading.RLock()
_connection = None # Internal variable to hold the connection, managed by _get_db_connection

def _get_db_connection():
    """Establishes and returns a database connection, creating the DB file if needed."""
    global _connection
    # Use thread-local storage or ensure connection is robustly handled per thread if not using check_same_thread=False
    # For simplicity with PTB's threading model, check_same_thread=False with external locking (db_lock) is common.

    # Check if connection exists and is usable
    if _connection:
        try:
            # Simple check to see if connection is still valid
            _connection.execute("SELECT 1")
            return _connection
        except (sqlite3.ProgrammingError, sqlite3.OperationalError) as e:
            log.warning(f"Database connection test failed ({e}), attempting to reconnect.")
            _connection = None # Force reconnect

    # If no valid connection, establish a new one
    if _connection is None:
        try:
            log.info(f"Attempting to connect to database: {DB_PATH}")
            # Ensure the directory exists before connecting
            db_dir = os.path.dirname(DB_PATH)
            if not os.path.exists(db_dir):
                 log.info(f"Database directory '{db_dir}' not found, creating.")
                 os.makedirs(db_dir, exist_ok=True)

            # check_same_thread=False is needed because PTB handlers run in different threads.
            # We use db_lock for protecting access across threads.
            # isolation_level=None means autocommit mode for single statements.
            # Use explicit BEGIN/COMMIT/ROLLBACK for multi-statement transactions.
            _connection = sqlite3.connect(
                DB_PATH,
                check_same_thread=False,
                timeout=15, # Wait up to 15 seconds if DB is locked
                isolation_level=None # Autocommit mode
                )
            _connection.row_factory = sqlite3.Row # Access columns by name (e.g., row['username'])
            # Enable Write-Ahead Logging for better concurrency
            _connection.execute("PRAGMA journal_mode=WAL;")
            # Enforce foreign key constraints
            _connection.execute("PRAGMA foreign_keys = ON;")
            # Wait up to 10 seconds if the database is locked by another connection before failing
            _connection.execute("PRAGMA busy_timeout = 10000;")
            log.info(f"Database connection established successfully: {DB_PATH}")
            # Initialize schema if needed right after connection
            # init_db() # Moved initialization call to the bottom of the file
        except sqlite3.Error as e:
            log.critical(f"CRITICAL: Database connection failed: {e}", exc_info=True)
            # Exit if DB connection fails? Or allow bot to run without DB? Critical seems appropriate.
            raise RuntimeError(f"Failed to connect to the database: {e}") from e
    return _connection

def init_db():
    """Initializes the database schema if tables don't exist."""
    conn = _get_db_connection() # Ensure connection is established
    # Define the database schema with Foreign Key constraints and indices
    # Schema remains largely the same, validated for consistency.
    schema = '''
        PRAGMA foreign_keys = ON; -- Ensure FKs are checked

        CREATE TABLE IF NOT EXISTS clients (
            invitation_code TEXT PRIMARY KEY NOT NULL,
            user_id INTEGER UNIQUE, -- Telegram User ID, can be NULL until activated
            subscription_end INTEGER NOT NULL, -- Unix timestamp (UTC)
            dedicated_userbots TEXT, -- Comma-separated phone numbers initially assigned (Can normalize later if needed) - CONSIDER REMOVING/IGNORING
            -- Aggregated stats - Updated by update_task_after_run
            forwards_count INTEGER DEFAULT 0 NOT NULL, -- Counts task runs that sent >= 1 message
            groups_reached INTEGER DEFAULT 0 NOT NULL, -- This column is hard to update reliably, maybe deprecate?
            total_messages_sent INTEGER DEFAULT 0 NOT NULL, -- Sum of messages sent by tasks
            language TEXT DEFAULT 'en' NOT NULL -- User's preferred language
        );

        CREATE TABLE IF NOT EXISTS userbots (
            phone_number TEXT PRIMARY KEY NOT NULL, -- Using phone as PK simplifies lookups
            session_file TEXT NOT NULL UNIQUE, -- Path relative to SESSION_DIR
            status TEXT CHECK(status IN ('active', 'inactive', 'authenticating', 'error', 'connecting', 'needs_code', 'needs_password', 'initializing')) DEFAULT 'inactive' NOT NULL,
            assigned_client TEXT, -- Client's invitation_code, NULL if unassigned
            api_id INTEGER NOT NULL,
            api_hash TEXT NOT NULL,
            username TEXT, -- Telegram @username, nullable
            last_error TEXT, -- Store last known significant error message
            FOREIGN KEY (assigned_client) REFERENCES clients(invitation_code) ON DELETE SET NULL -- If client deleted, unassign bot
        );

        CREATE TABLE IF NOT EXISTS folders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            created_by INTEGER NOT NULL, -- Client's user_id, FK to clients(user_id)
            UNIQUE(name, created_by), -- Folder names must be unique per user
            FOREIGN KEY (created_by) REFERENCES clients(user_id) ON DELETE CASCADE -- If client deleted, remove their folders
        );

        CREATE TABLE IF NOT EXISTS target_groups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_id INTEGER NOT NULL, -- Telegram's group/channel ID (MUST be known to add)
            group_name TEXT, -- Store for display purposes, can be updated
            group_link TEXT UNIQUE, -- Store original link if available, maybe unique? Or allow multiple links to same group? Let's make it optional unique for reference.
            added_by INTEGER NOT NULL, -- Client's user_id, FK to clients(user_id)
            folder_id INTEGER NOT NULL, -- FK to folders(id)
            -- A user shouldn't add the same group ID to the same folder multiple times
            UNIQUE(group_id, added_by, folder_id),
            FOREIGN KEY (added_by) REFERENCES clients(user_id) ON DELETE CASCADE, -- If client deleted, remove their groups
            FOREIGN KEY (folder_id) REFERENCES folders(id) ON DELETE CASCADE -- If folder deleted, remove groups within it
        );
        -- Removed UNIQUE constraint on group_link as it might be null or change.

        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp INTEGER NOT NULL, -- Unix timestamp (UTC)
            event TEXT NOT NULL, -- Type of event (e.g., 'Client Activated', 'Task Run', 'Join Failed')
            user_id INTEGER, -- Client User ID or Admin ID involved
            userbot_phone TEXT, -- Involved userbot phone number
            details TEXT -- Additional information/error message
        );
        -- Removed client_id from logs for simplicity.

        CREATE TABLE IF NOT EXISTS userbot_settings (
            client_id INTEGER NOT NULL, -- Client's user_id, FK to clients(user_id)
            userbot_phone TEXT NOT NULL, -- FK to userbots(phone_number)
            message_link TEXT, -- Link to the primary message to forward
            fallback_message_link TEXT, -- Link to the fallback message (optional)
            start_time INTEGER, -- Unix timestamp (UTC) for the first run *after* this time
            repetition_interval INTEGER, -- Interval in minutes
            status TEXT CHECK(status IN ('active', 'inactive')) DEFAULT 'inactive' NOT NULL,
            folder_id INTEGER, -- Target folder ID, NULL if send_to_all_groups is true
            send_to_all_groups INTEGER DEFAULT 0 CHECK(send_to_all_groups IN (0, 1)), -- Boolean (0=false, 1=true)
            last_run INTEGER, -- Unix timestamp (UTC) of the last successful run start (or attempt?) - Use start time.
            last_error TEXT, -- Store last error related to this specific task run (cleared on successful run/manual save)
            messages_sent_count INTEGER DEFAULT 0 NOT NULL, -- Track messages sent by this task specifically
            -- Composite primary key ensures one settings entry per user-bot pair
            PRIMARY KEY (client_id, userbot_phone),
            FOREIGN KEY (client_id) REFERENCES clients(user_id) ON DELETE CASCADE, -- If client deleted, remove their tasks
            FOREIGN KEY (userbot_phone) REFERENCES userbots(phone_number) ON DELETE CASCADE, -- If bot deleted, remove its tasks
            -- If folder deleted, set folder_id to NULL (task should ideally become inactive or error - handle in task runner)
            FOREIGN KEY (folder_id) REFERENCES folders(id) ON DELETE SET NULL
        );

        CREATE TABLE IF NOT EXISTS admin_tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            userbot_phone TEXT NOT NULL,
            message TEXT NOT NULL,
            schedule TEXT NOT NULL, -- Cron format
            target TEXT NOT NULL, -- Group username or ID
            status TEXT CHECK(status IN ('active', 'inactive')) DEFAULT 'inactive' NOT NULL,
            last_run INTEGER, -- Unix timestamp of last run
            next_run INTEGER, -- Unix timestamp of next scheduled run
            created_by INTEGER NOT NULL, -- Admin user ID
            created_at INTEGER NOT NULL, -- Unix timestamp
            FOREIGN KEY (userbot_phone) REFERENCES userbots(phone_number) ON DELETE CASCADE,
            FOREIGN KEY (created_by) REFERENCES clients(user_id)
        );
        CREATE INDEX IF NOT EXISTS idx_admin_tasks_status ON admin_tasks (status);
        CREATE INDEX IF NOT EXISTS idx_admin_tasks_next_run ON admin_tasks (next_run);

        -- Create Indexes for faster lookups on frequently queried columns
        CREATE INDEX IF NOT EXISTS idx_clients_user_id ON clients (user_id);
        CREATE INDEX IF NOT EXISTS idx_userbots_assigned_client ON userbots (assigned_client);
        CREATE INDEX IF NOT EXISTS idx_userbots_status ON userbots (status); -- For finding available bots
        CREATE INDEX IF NOT EXISTS idx_folders_created_by ON folders (created_by);
        CREATE INDEX IF NOT EXISTS idx_target_groups_folder_id ON target_groups (folder_id);
        CREATE INDEX IF NOT EXISTS idx_target_groups_added_by ON target_groups (added_by);
        -- Removed index on group_id as uniqueness constraint covers it with folder_id/added_by
        -- CREATE INDEX IF NOT EXISTS idx_target_groups_group_id ON target_groups (group_id);
        CREATE INDEX IF NOT EXISTS idx_logs_timestamp ON logs (timestamp DESC); -- For fetching recent logs
        -- Index to efficiently find active tasks due to run
        CREATE INDEX IF NOT EXISTS idx_userbot_settings_active ON userbot_settings (status, start_time, repetition_interval, last_run) WHERE status = 'active';
        CREATE INDEX IF NOT EXISTS idx_userbot_settings_folder_id ON userbot_settings (folder_id);
        CREATE INDEX IF NOT EXISTS idx_userbot_settings_client_id ON userbot_settings (client_id);
    '''
    try:
        with db_lock:
            # Use executescript for multi-statement schema definition
            conn.executescript(schema)
        log.info("Database schema initialized/verified successfully.")
    except sqlite3.Error as e:
        log.critical(f"CRITICAL: Database schema initialization failed: {e}", exc_info=True)
        raise

def close_db():
    """Closes the database connection."""
    global _connection
    if _connection:
        log.info("Attempting to close database connection...")
        try:
            # Ensure WAL checkpoint before closing if needed (usually handled by SQLite on close)
            # with db_lock:
            #    _connection.execute("PRAGMA wal_checkpoint(TRUNCATE);")
            _connection.close()
            _connection = None
            log.info("Database connection closed successfully.")
        except sqlite3.Error as e:
            log.error(f"Error closing database connection: {e}")

# --- Logging Function (to DB) ---
def log_event_db(event, details="", user_id=None, userbot_phone=None):
    """Logs an event to the 'logs' table in the database."""
    timestamp = int(datetime.now(UTC_TZ).timestamp())
    sql = "INSERT INTO logs (timestamp, event, user_id, userbot_phone, details) VALUES (?, ?, ?, ?, ?)"
    try:
        conn = _get_db_connection()
        with db_lock:
             # Execute directly since autocommit is on
            conn.execute(sql, (timestamp, event, user_id, userbot_phone, str(details)))
        # Also log to standard logger for immediate visibility/Render logs
        log.debug(f"DBLog-{event}: User={user_id}, Bot={userbot_phone}, Details={details[:100]}...")
    except sqlite3.Error as e:
        # Log DB logging failure to standard logger, but don't crash the app
        log.error(f"CRITICAL: Failed to log event to DB: {e} - Event: {event}, Details: {details[:100]}...")
    except Exception as e:
         log.error(f"Unexpected error logging event to DB: {e}", exc_info=True)


# --- Client Functions ---
def find_client_by_user_id(user_id):
    """Retrieves client data by Telegram User ID."""
    sql = "SELECT * FROM clients WHERE user_id = ?"
    try:
        conn = _get_db_connection()
        with db_lock:
            cursor = conn.cursor()
            cursor.execute(sql, (user_id,))
            return cursor.fetchone() # Returns a Row object or None
    except sqlite3.Error as e:
        log.error(f"DB Error finding client by user_id {user_id}: {e}")
        return None

def find_client_by_code(code):
    """Retrieves client data by invitation code."""
    sql = "SELECT * FROM clients WHERE invitation_code = ?"
    try:
        conn = _get_db_connection()
        with db_lock:
            cursor = conn.cursor()
            cursor.execute(sql, (code,))
            return cursor.fetchone()
    except sqlite3.Error as e:
        log.error(f"DB Error finding client by code {code}: {e}")
        return None

def activate_client(code, user_id):
    """Attempts to activate a client by assigning a user_id to an invitation code."""
    sql_check_user = "SELECT invitation_code FROM clients WHERE user_id = ?"
    sql_check_code = "SELECT user_id, subscription_end FROM clients WHERE invitation_code = ?"
    sql_update = "UPDATE clients SET user_id = ? WHERE invitation_code = ? AND user_id IS NULL"
    try:
        conn = _get_db_connection()
        with db_lock:
            cursor = conn.cursor()
            # Use a transaction for consistency checks
            cursor.execute("BEGIN")
            try:
                # 1. Check if this user_id is already assigned to ANY code
                cursor.execute(sql_check_user, (user_id,))
                existing_user_code = cursor.fetchone()
                if existing_user_code and existing_user_code['invitation_code'] != code:
                    log.warning(f"Activation attempt failed: User {user_id} already linked to code {existing_user_code['invitation_code']}")
                    cursor.execute("ROLLBACK")
                    return False, "user_already_active"

                # 2. Check the specific code
                cursor.execute(sql_check_code, (code,))
                code_data = cursor.fetchone()
                if not code_data:
                    cursor.execute("ROLLBACK")
                    return False, "code_not_found"
                if code_data['user_id'] is not None:
                    if code_data['user_id'] == user_id:
                         cursor.execute("ROLLBACK") # No change needed
                         return True, "already_active" # Already activated with this code
                    else:
                         cursor.execute("ROLLBACK")
                         return False, "code_already_used" # Used by someone else

                # 3. Check expiration
                now_ts = int(datetime.now(UTC_TZ).timestamp())
                if code_data['subscription_end'] < now_ts:
                    cursor.execute("ROLLBACK")
                    # Use subscription_expired key for consistency
                    return False, "subscription_expired"

                # 4. Attempt update (Atomic check via WHERE user_id IS NULL)
                cursor.execute(sql_update, (user_id, code))
                updated_rows = cursor.rowcount
                if updated_rows > 0:
                    log.info(f"Client activated: Code={code}, UserID={user_id}")
                    cursor.execute("COMMIT")
                    return True, "activation_success"
                else:
                    # Should not happen if previous checks passed, but indicates a potential race condition or logic flaw
                    log.error(f"Activation update failed unexpectedly for Code={code}, UserID={user_id}.")
                    cursor.execute("ROLLBACK")
                    return False, "activation_error" # Generic activation error
            except sqlite3.Error as tx_e:
                 log.error(f"DB Tx Error activating client {code} user {user_id}: {tx_e}", exc_info=True)
                 cursor.execute("ROLLBACK")
                 return False, "activation_db_error"

    except sqlite3.Error as e:
        log.error(f"DB Connection Error activating client code {code} for user {user_id}: {e}", exc_info=True)
        return False, "activation_db_error"

def get_user_language(user_id):
    """Gets the user's preferred language code, defaults to 'en'."""
    client = find_client_by_user_id(user_id)
    return client['language'] if client and client['language'] else 'en'

def set_user_language(user_id, lang):
    """Sets the user's preferred language."""
    sql = "UPDATE clients SET language = ? WHERE user_id = ?"
    try:
        conn = _get_db_connection()
        with db_lock:
            cursor = conn.cursor()
            cursor.execute(sql, (lang, user_id))
            if cursor.rowcount > 0:
                # No need for separate log_event_db here, log happens elsewhere if needed
                return True
            else:
                log.warning(f"Attempted to set language for non-existent/inactive user_id {user_id}")
                return False
    except sqlite3.Error as e:
        log.error(f"DB Error setting language for user {user_id}: {e}")
        return False

def create_invitation(code, sub_end_ts):
    """Creates a new client invitation code record."""
    # Removed dedicated_userbots assignment here. Assign bots separately.
    sql = "INSERT INTO clients (invitation_code, subscription_end) VALUES (?, ?)"
    try:
        conn = _get_db_connection()
        with db_lock:
            conn.execute(sql, (code, sub_end_ts))
        return True
    except sqlite3.IntegrityError:
        log.warning(f"Attempted to insert duplicate invitation code: {code}")
        return False # Code already exists
    except sqlite3.Error as e:
        log.error(f"DB Error creating invitation {code}: {e}")
        return False

def extend_subscription(code, new_end_ts):
    """Updates the subscription end timestamp for a client."""
    sql = "UPDATE clients SET subscription_end = ? WHERE invitation_code = ?"
    try:
        conn = _get_db_connection()
        with db_lock:
            cursor = conn.cursor()
            cursor.execute(sql, (new_end_ts, code))
            if cursor.rowcount > 0:
                log.info(f"Extended subscription for code {code} to {datetime.fromtimestamp(new_end_ts, UTC_TZ)}")
                return True
            else:
                log.warning(f"Tried to extend subscription for non-existent code: {code}")
                return False
    except sqlite3.Error as e:
        log.error(f"DB Error extending subscription for code {code}: {e}")
        return False

def get_all_subscriptions():
    """Fetches details of all activated client subscriptions."""
    # Query relies on JOIN to count bots, which is efficient.
    sql = """
        SELECT c.user_id, c.invitation_code, c.subscription_end,
               COUNT(u.phone_number) as bot_count
        FROM clients c
        LEFT JOIN userbots u ON c.invitation_code = u.assigned_client
        WHERE c.user_id IS NOT NULL -- Only activated clients
        GROUP BY c.invitation_code
        ORDER BY c.subscription_end ASC
    """
    # Removed c.dedicated_userbots as it's potentially unreliable
    try:
        conn = _get_db_connection()
        with db_lock:
            cursor = conn.cursor()
            cursor.execute(sql)
            return cursor.fetchall()
    except sqlite3.Error as e:
        log.error(f"DB Error fetching subscriptions: {e}")
        return []

# --- Userbot Functions ---
def find_userbot(phone):
    """Retrieves userbot data by phone number."""
    sql = "SELECT * FROM userbots WHERE phone_number = ?"
    try:
        conn = _get_db_connection()
        with db_lock:
            cursor = conn.cursor()
            cursor.execute(sql, (phone,))
            return cursor.fetchone()
    except sqlite3.Error as e:
        log.error(f"DB Error finding userbot {phone}: {e}")
        return None

def get_all_userbots(assigned_status=None):
    """Fetches all userbots, optionally filtering by assigned status."""
    # assigned_status: True (assigned), False (unassigned), None (all)
    sql = "SELECT u.*, c.user_id as client_user_id FROM userbots u LEFT JOIN clients c ON u.assigned_client = c.invitation_code"
    params = []
    if assigned_status is True:
        sql += " WHERE u.assigned_client IS NOT NULL"
    elif assigned_status is False:
        sql += " WHERE u.assigned_client IS NULL"
    sql += " ORDER BY u.assigned_client NULLS FIRST, u.phone_number" # Show unassigned first
    try:
        conn = _get_db_connection()
        with db_lock:
            cursor = conn.cursor()
            cursor.execute(sql, params)
            return cursor.fetchall()
    except sqlite3.Error as e:
        log.error(f"DB Error fetching userbots (assigned={assigned_status}): {e}")
        return []

def get_client_bots(user_id):
    """Gets the phone numbers of bots currently assigned to a client's active code."""
    client = find_client_by_user_id(user_id)
    if not client: return []

    # Find bots assigned to THIS client's code using the assigned_client column
    sql = "SELECT phone_number FROM userbots WHERE assigned_client = ?"
    try:
        conn = _get_db_connection()
        with db_lock:
             cursor = conn.cursor()
             cursor.execute(sql, (client['invitation_code'],))
             return [row['phone_number'] for row in cursor.fetchall()]
    except sqlite3.Error as e:
         log.error(f"DB Error getting bots for client {user_id} (code {client['invitation_code']}): {e}")
         return []

def add_userbot(phone, session_file_rel, api_id, api_hash, status='inactive', username=None, assigned_client=None, last_error=None):
    """Adds a new userbot or updates an existing one."""
    # Using ON CONFLICT to handle updates cleanly
    sql = """
        INSERT INTO userbots (
            phone_number, session_file, status, api_id, api_hash, username, assigned_client, last_error
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(phone_number) DO UPDATE SET
            session_file=excluded.session_file,
            status=excluded.status,
            api_id=excluded.api_id,
            api_hash=excluded.api_hash,
            username=excluded.username,
            assigned_client=excluded.assigned_client,
            last_error=excluded.last_error
        WHERE phone_number = excluded.phone_number; -- Explicit WHERE clause for clarity
    """
    try:
        conn = _get_db_connection()
        with db_lock:
            conn.execute(sql, (phone, session_file_rel, status, api_id, api_hash, username, assigned_client, last_error))
        log.info(f"Userbot {phone} added/updated. Status: {status}, Assigned: {assigned_client}")
        return True
    except sqlite3.Error as e:
        log.error(f"DB Error adding/updating userbot {phone}: {e}")
        return False

def update_userbot_status(phone, status, username=None, last_error=None):
    """Updates the status, optionally username and last_error for a userbot."""
    # Use COALESCE to update username only if provided, keep existing otherwise
    # Clear last_error if None is passed, otherwise update it.
    sql = """
        UPDATE userbots
        SET status = ?,
            username = COALESCE(?, username),
            last_error = ?
        WHERE phone_number = ?
    """
    try:
        conn = _get_db_connection()
        updated = False # Initialize updated flag
        with db_lock:
            cursor = conn.cursor()
            cursor.execute(sql, (status, username, last_error, phone))
            updated = cursor.rowcount > 0

        # Avoid logging excessively frequent status changes like 'connecting'
        if status not in ['connecting', 'initializing']:
            if updated: log.info(f"Updated status for userbot {phone} to {status}. Error: {last_error}")
            else: log.warning(f"Attempted to update status for non-existent userbot {phone}?")
        else:
            if updated: log.debug(f"Userbot {phone} status set to {status}.")
        return updated
    except sqlite3.Error as e:
        log.error(f"DB Error updating status for userbot {phone}: {e}")
        return False

def assign_userbots_to_client(code, phones_to_assign: list):
    """Assigns a list of userbots to a client's invitation code."""
    if not phones_to_assign:
        log.warning(f"assign_userbots_to_client called with empty list for code {code}.")
        return True, "No bots provided to assign." # Not an error, just nothing to do

    sql_check_client = "SELECT invitation_code FROM clients WHERE invitation_code = ?"
    # Ensure bot exists and is unassigned before assigning
    sql_update_bot = "UPDATE userbots SET assigned_client = ? WHERE phone_number = ? AND assigned_client IS NULL"
    # Removed update to client.dedicated_userbots as it's unreliable

    updated_count = 0
    failed_phones = []
    try:
        conn = _get_db_connection()
        with db_lock:
            cursor = conn.cursor()
            # Check if client code is valid
            cursor.execute(sql_check_client, (code,))
            if not cursor.fetchone():
                 log.error(f"Cannot assign bots: Client code '{code}' not found.")
                 return False, "Client code not found."

            # Use transaction for bulk update
            cursor.execute("BEGIN")
            try:
                for phone in phones_to_assign:
                     cursor.execute(sql_update_bot, (code, phone))
                     if cursor.rowcount > 0:
                         updated_count += 1
                     else:
                         # Check why update failed (already assigned elsewhere or bot doesn't exist?)
                         cursor.execute("SELECT assigned_client FROM userbots WHERE phone_number = ?", (phone,))
                         bot_info = cursor.fetchone()
                         if bot_info and bot_info['assigned_client'] is not None:
                              log.warning(f"Could not assign bot {phone} to {code}: Already assigned to {bot_info['assigned_client']}")
                              failed_phones.append(f"{phone} (already assigned)")
                         elif not bot_info: # Bot not found in userbots table
                              log.warning(f"Could not assign bot {phone} to {code}: Bot not found.")
                              failed_phones.append(f"{phone} (not found)")
                         else: # Bot exists but other update condition failed (e.g. assigned_client was not NULL but update failed)
                              log.warning(f"Could not assign bot {phone} to {code}: Bot found but update failed (unexpected).")
                              failed_phones.append(f"{phone} (update error)")


                cursor.execute("COMMIT") # Commit successful assignments

                log.info(f"Assigned {updated_count}/{len(phones_to_assign)} bots to client {code}.")
                if updated_count != len(phones_to_assign):
                    msg = f"Successfully assigned {updated_count} userbots. Failed: {', '.join(failed_phones)}"
                    log.warning(f"Some bots requested for assignment to {code} were unavailable or already assigned: {failed_phones}")
                    return True, msg # Partial success is still success
                else:
                    return True, f"Successfully assigned {updated_count} userbots."

            except sqlite3.Error as tx_e:
                 log.error(f"DB Tx Error assigning bots to client {code}: {tx_e}", exc_info=True)
                 cursor.execute("ROLLBACK")
                 return False, "Database transaction error during assignment."

    except sqlite3.Error as e:
        log.error(f"DB Connection Error assigning bots to client {code}: {e}", exc_info=True)
        # No rollback needed if transaction wasn't started
        return False, "Database connection error during assignment."


def remove_userbot(phone):
    """Removes a userbot record from the database."""
    sql = "DELETE FROM userbots WHERE phone_number = ?"
    # Session file path and deletion logic removed from here.
    # It will be handled by the caller in handlers.py, which uses telethon_utils.delete_session_files_for_phone.

    try:
        conn = _get_db_connection()
        deleted_rows = 0
        with db_lock:
            cursor = conn.cursor()
            # Using a try-except block for the execute itself within the lock
            try:
                cursor.execute(sql, (phone,))
                deleted_rows = cursor.rowcount
            except sqlite3.Error as db_exec_e:
                 log.error(f"DB Error executing remove userbot {phone}: {db_exec_e}", exc_info=True)
                 return False # DB execution failed

        if deleted_rows > 0:
            log.info(f"Removed userbot {phone} from database.")
            # Session file deletion is now handled by the caller.
            return True
        else:
            log.warning(f"Attempted to remove userbot {phone}, but it was not found in the database.")
            return False # Bot wasn't in DB
    except sqlite3.Error as e:
        log.error(f"DB Connection/General Error removing userbot {phone}: {e}", exc_info=True)
        return False


def get_unassigned_userbots(limit):
    """Gets a list of phone numbers for active, unassigned userbots."""
    # Ensure we only get bots that are genuinely 'active' (not error, connecting etc)
    sql = "SELECT phone_number FROM userbots WHERE assigned_client IS NULL AND status = 'active' ORDER BY phone_number LIMIT ?"
    try:
        conn = _get_db_connection()
        with db_lock:
            cursor = conn.cursor()
            cursor.execute(sql, (limit,))
            return [row['phone_number'] for row in cursor.fetchall()]
    except sqlite3.Error as e:
        log.error(f"DB Error fetching unassigned active userbots: {e}")
        return []


# --- Folder Functions ---
def add_folder(name, user_id):
    """Adds a new folder for a client."""
    sql = "INSERT INTO folders (name, created_by) VALUES (?, ?)"
    try:
        conn = _get_db_connection()
        with db_lock:
            cursor = conn.cursor()
            cursor.execute(sql, (name, user_id))
            folder_id = cursor.lastrowid
            log.info(f"Folder '{name}' (ID: {folder_id}) created for user {user_id}.")
            return folder_id
    except sqlite3.IntegrityError: # Handles UNIQUE constraint violation
         log.warning(f"Attempt to create duplicate folder name '{name}' for user {user_id}.")
         return None # Indicate duplicate
    except sqlite3.Error as e:
        log.error(f"DB Error adding folder '{name}' for user {user_id}: {e}")
        return -1 # Indicate general error (use distinct value from None)

def get_folders_by_user(user_id):
    """Retrieves all folders created by a specific user."""
    sql = "SELECT id, name FROM folders WHERE created_by = ? ORDER BY name ASC"
    try:
        conn = _get_db_connection()
        with db_lock:
            cursor = conn.cursor()
            cursor.execute(sql, (user_id,))
            return cursor.fetchall() # List of Row objects
    except sqlite3.Error as e:
        log.error(f"DB Error fetching folders for user {user_id}: {e}")
        return []

def get_folder_name(folder_id):
    """Retrieves the name of a folder by its ID."""
    sql = "SELECT name FROM folders WHERE id = ?"
    try:
        conn = _get_db_connection()
        with db_lock:
            cursor = conn.cursor()
            cursor.execute(sql, (folder_id,))
            result = cursor.fetchone()
            return result['name'] if result else None
    except sqlite3.Error as e:
        log.error(f"DB Error fetching folder name for ID {folder_id}: {e}")
        return None

def delete_folder(folder_id, user_id):
    """Deletes a folder and its associated target groups (via CASCADE)."""
    sql = "DELETE FROM folders WHERE id = ? AND created_by = ?"
    try:
        conn = _get_db_connection()
        deleted_rows = 0
        with db_lock:
             # CASCADE handles associated target_groups, single statement is safe with autocommit
             cursor = conn.cursor()
             cursor.execute(sql, (folder_id, user_id))
             deleted_rows = cursor.rowcount

        if deleted_rows > 0:
             log.info(f"Deleted folder ID {folder_id} for user {user_id}.")
             log_event_db("Folder Deleted", f"Folder ID: {folder_id}", user_id=user_id)
             return True
        else:
             # Could be non-existent folder or user trying to delete someone else's
             log.warning(f"Attempt to delete non-existent or unauthorized folder ID {folder_id} by user {user_id}.")
             return False
    except sqlite3.Error as e:
        # Could be OperationalError if DB is locked, etc.
        log.error(f"DB Error deleting folder ID {folder_id} for user {user_id}: {e}")
        return False

def rename_folder(folder_id, user_id, new_name):
    """Renames a folder for a specific user, checking for name uniqueness."""
    # Check uniqueness constraint first (handled by UNIQUE index, but explicit check is clearer)
    sql_check = "SELECT 1 FROM folders WHERE name = ? AND created_by = ? AND id != ?"
    sql_update = "UPDATE folders SET name = ? WHERE id = ? AND created_by = ?"
    try:
        conn = _get_db_connection()
        with db_lock:
            cursor = conn.cursor()
            # Use transaction for check and update
            cursor.execute("BEGIN")
            try:
                 # Check if the new name already exists for this user (excluding the current folder)
                 cursor.execute(sql_check, (new_name, user_id, folder_id))
                 if cursor.fetchone():
                      log.warning(f"Rename failed: Folder name '{new_name}' already exists for user {user_id}.")
                      cursor.execute("ROLLBACK")
                      return False, "name_exists" # Indicate specific error

                 # If name is unique, perform the update
                 cursor.execute(sql_update, (new_name, folder_id, user_id))
                 if cursor.rowcount > 0:
                      cursor.execute("COMMIT")
                      log.info(f"Renamed folder ID {folder_id} to '{new_name}' for user {user_id}.")
                      return True, "success"
                 else:
                      # Folder ID not found or didn't belong to user
                      log.warning(f"Rename failed: Folder ID {folder_id} not found or not owned by user {user_id}.")
                      cursor.execute("ROLLBACK")
                      return False, "not_found_or_unauthorized"

            except sqlite3.Error as tx_e:
                 log.error(f"DB Tx Error renaming folder {folder_id} for user {user_id}: {tx_e}", exc_info=True)
                 cursor.execute("ROLLBACK")
                 return False, "db_error"

    except sqlite3.Error as e:
        log.error(f"DB Connection Error renaming folder {folder_id} for user {user_id}: {e}", exc_info=True)
        return False, "db_error"


# --- Target Group Functions ---
def add_target_group(group_id, group_name, group_link, user_id, folder_id):
    """
    Adds a target group to a user's folder. Requires group_id.
    Ignores duplicates based on (group_id, added_by, folder_id).
    """
    # Require group_id to add a group reliably
    if group_id is None:
        log.warning(f"Attempted to add target group without ID to folder {folder_id} by user {user_id}. Link: {group_link}")
        return False # Indicates error/invalid input to caller

    sql = """
        INSERT INTO target_groups (group_id, group_name, group_link, added_by, folder_id)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(group_id, added_by, folder_id) DO NOTHING
    """
    # ON CONFLICT(group_id, added_by, folder_id) DO UPDATE SET
    #     group_name = excluded.group_name,  -- Update name/link if already exists
    #     group_link = excluded.group_link;
    # Decided DO NOTHING is safer, avoids overwriting potentially better existing data.

    try:
        conn = _get_db_connection()
        with db_lock:
            cursor = conn.cursor()
            cursor.execute(sql, (group_id, group_name, group_link, user_id, folder_id))
            if cursor.rowcount > 0:
                 log.info(f"Added group ID {group_id} ('{group_name}') to folder {folder_id} for user {user_id}.")
                 return True # Inserted successfully
            else:
                 # This means ON CONFLICT happened (duplicate)
                 log.debug(f"Group ID {group_id} already exists in folder {folder_id} for user {user_id}. Ignored.")
                 return None # Changed from False to None to specifically indicate duplicate/ignored
    except sqlite3.IntegrityError as fk_e:
         # This likely means the folder_id or added_by (client user_id) is invalid/deleted
         log.error(f"DB Integrity Error adding target group {group_id} to folder {folder_id} for user {user_id}: {fk_e}. Foreign key constraint likely failed.")
         return False # Indicates DB error
    except sqlite3.Error as e:
        log.error(f"DB Error adding target group {group_id} to folder {folder_id} for user {user_id}: {e}")
        return False # Indicates DB error

def get_target_groups_by_folder(folder_id):
    """Gets a list of group IDs belonging to a specific folder."""
    sql = "SELECT group_id FROM target_groups WHERE folder_id = ?"
    try:
        conn = _get_db_connection()
        with db_lock:
            cursor = conn.cursor()
            cursor.execute(sql, (folder_id,))
            return [row['group_id'] for row in cursor.fetchall()]
    except sqlite3.Error as e:
        log.error(f"DB Error fetching group IDs for folder {folder_id}: {e}")
        return []

def get_target_groups_details_by_folder(folder_id):
     """Gets details (id, group_id, name, link) of groups in a folder."""
     sql = "SELECT id, group_id, group_name, group_link FROM target_groups WHERE folder_id = ? ORDER BY group_name ASC, id ASC"
     try:
         conn = _get_db_connection()
         with db_lock:
             cursor = conn.cursor()
             cursor.execute(sql, (folder_id,))
             return cursor.fetchall() # List of Row objects
     except sqlite3.Error as e:
         log.error(f"DB Error fetching group details for folder {folder_id}: {e}")
         return []

def remove_target_groups_by_db_id(db_ids_to_remove: list[int], user_id: int) -> int:
    """Removes target groups using their database primary key IDs, ensuring user owns them."""
    if not db_ids_to_remove:
        return 0
    # Ensure IDs are integers
    try:
        safe_ids = tuple(int(id_) for id_ in db_ids_to_remove)
    except (ValueError, TypeError):
        log.error(f"Invalid non-integer ID provided for group removal by user {user_id}.")
        return -1 # Indicate error due to bad input

    placeholders = ','.join('?' * len(safe_ids))
    # Added check for added_by to prevent users deleting others' entries via crafted callbacks
    sql = f"DELETE FROM target_groups WHERE id IN ({placeholders}) AND added_by = ?"
    params = safe_ids + (user_id,)
    try:
        conn = _get_db_connection()
        deleted_count = -1
        with db_lock:
             # Use transaction for atomicity, though single delete is often atomic anyway
             cursor = conn.cursor()
             cursor.execute("BEGIN")
             try:
                 cursor.execute(sql, params)
                 deleted_count = cursor.rowcount
                 cursor.execute("COMMIT")
                 if deleted_count > 0:
                     log.info(f"User {user_id} removed {deleted_count} target groups by DB ID.")
                     # log_event_db("Groups Removed", f"Count: {deleted_count}, IDs: {safe_ids}", user_id=user_id) # Logged by caller maybe
                 elif deleted_count == 0:
                      log.warning(f"User {user_id} tried to remove target group IDs, but none matched or belonged to them: {safe_ids}")

             except sqlite3.Error as tx_e:
                  log.error(f"DB Tx Error removing target groups by DB ID for user {user_id}: {tx_e}", exc_info=True)
                  cursor.execute("ROLLBACK")
                  deleted_count = -1 # Indicate error

        return deleted_count
    except sqlite3.Error as e:
        log.error(f"DB Connection Error removing target groups by DB ID for user {user_id}: {e}", exc_info=True)
        return -1 # Indicate error

# (remove_all_target_groups_from_folder seems less used now, but keep for potential future use)
def remove_all_target_groups_from_folder(folder_id, user_id):
    """Removes all target groups associated with a specific folder for a user."""
    sql = "DELETE FROM target_groups WHERE folder_id = ? AND added_by = ?"
    try:
        conn = _get_db_connection()
        deleted_count = -1
        with db_lock:
            cursor = conn.cursor()
            cursor.execute(sql, (folder_id, user_id))
            deleted_count = cursor.rowcount
        log.info(f"User {user_id} cleared {deleted_count} groups from folder {folder_id}.")
        if deleted_count > 0:
            log_event_db("Folder Groups Cleared", f"Folder ID: {folder_id}, Count: {deleted_count}", user_id=user_id)
        return deleted_count
    except sqlite3.Error as e:
        log.error(f"DB Error clearing folder {folder_id} for user {user_id}: {e}")
        return -1 # Indicate error

# --- Userbot Task Settings Functions ---
def get_userbot_task_settings(client_id, userbot_phone):
    """Retrieves task settings for a specific userbot and client."""
    sql = "SELECT * FROM userbot_settings WHERE client_id = ? AND userbot_phone = ?"
    try:
        conn = _get_db_connection()
        with db_lock:
            cursor = conn.cursor()
            cursor.execute(sql, (client_id, userbot_phone))
            return cursor.fetchone() # Row object or None
    except sqlite3.Error as e:
        log.error(f"DB Error getting task settings for user {client_id}, bot {userbot_phone}: {e}")
        return None

def save_userbot_task_settings(client_id, userbot_phone, settings: dict):
    """Saves or updates task settings for a userbot using ON CONFLICT."""
    # Ensure required fields are present with defaults if necessary
    # Map input dictionary keys to DB columns carefully
    params = {
        'client_id': client_id,
        'userbot_phone': userbot_phone,
        'message_link': settings.get('message_link'), # Handles None
        'fallback_message_link': settings.get('fallback_message_link'), # Handles None
        'start_time': settings.get('start_time'), # Store as Unix TS UTC, handles None
        'repetition_interval': settings.get('repetition_interval'), # Store as minutes, handles None
        'status': settings.get('status', 'inactive'), # Default to inactive
        'folder_id': settings.get('folder_id'), # Can be NULL
        'send_to_all_groups': int(settings.get('send_to_all_groups', 0)), # Ensure integer 0 or 1
        'last_run': settings.get('last_run'), # Usually managed internally
        'last_error': settings.get('last_error'), # Usually managed internally or cleared on save
        # messages_sent_count is updated by update_task_after_run, don't overwrite here unless intended
        # If you want to reset it on save, include it:
        # 'messages_sent_count': settings.get('messages_sent_count', 0)
    }

    # Use named placeholders for clarity with ON CONFLICT
    # Update only fields that are typically set manually via the UI
    sql = """
        INSERT INTO userbot_settings (
            client_id, userbot_phone, message_link, fallback_message_link, start_time,
            repetition_interval, status, folder_id, send_to_all_groups, last_run, last_error, messages_sent_count
        ) VALUES (
            :client_id, :userbot_phone, :message_link, :fallback_message_link, :start_time,
            :repetition_interval, :status, :folder_id, :send_to_all_groups, :last_run, :last_error,
            COALESCE((SELECT messages_sent_count FROM userbot_settings WHERE client_id = :client_id AND userbot_phone = :userbot_phone), 0) -- Preserve existing count on INSERT
        )
        ON CONFLICT(client_id, userbot_phone) DO UPDATE SET
            message_link=excluded.message_link,
            fallback_message_link=excluded.fallback_message_link,
            start_time=excluded.start_time,
            repetition_interval=excluded.repetition_interval,
            status=excluded.status,
            folder_id=excluded.folder_id,
            send_to_all_groups=excluded.send_to_all_groups,
            -- last_run should NOT be updated here, only by the task runner
            -- last_error should be cleared on manual save
            last_error = NULL -- Clear error on successful manual save
            -- messages_sent_count should NOT be updated here
        WHERE client_id = excluded.client_id AND userbot_phone = excluded.userbot_phone;
    """
    try:
        conn = _get_db_connection()
        with db_lock:
            conn.execute(sql, params)
        log.info(f"Saved task settings for user {client_id}, bot {userbot_phone}. Status: {params['status']}")
        return True
    except sqlite3.Error as e:
        log.error(f"DB Error saving task settings for user {client_id}, bot {userbot_phone}: {e}", exc_info=True)
        return False

def get_active_tasks_to_run(current_time_ts):
    """
    Finds tasks that are:
    1. Active (task status)
    2. Belong to an active userbot (userbot status)
    3. Belong to a client with an active subscription
    4. Have a start time in the past
    5. Have a valid repetition interval
    6. Have a primary message link
    7. Are due based on last_run + interval comparison
    """
    sql = """
        SELECT s.*, u.session_file, u.api_id, u.api_hash
        FROM userbot_settings s
        JOIN userbots u ON s.userbot_phone = u.phone_number
        JOIN clients c ON s.client_id = c.user_id
        WHERE
            s.status = 'active'                          -- 1. Task must be active
            AND u.status = 'active'                      -- 2. Userbot must be active
            AND c.subscription_end > ?                   -- 3. Client subscription must be valid (current_time_ts)
            AND s.start_time IS NOT NULL                 -- 4. Start time must be set
            AND s.start_time <= ?                        --    Start time must be in the past (current_time_ts)
            AND s.repetition_interval IS NOT NULL        -- 5. Interval must be set
            AND s.repetition_interval > 0                --    Interval must be positive
            AND s.message_link IS NOT NULL               -- 6. Must have a message to send
            -- 7. Check if due based on last run and interval:
            --    (last_run IS NULL means it should run if start_time is past)
            --    OR (last_run + interval_seconds <= current_time)
            AND (s.last_run IS NULL OR (s.last_run + (s.repetition_interval * 60)) <= ?) -- (current_time_ts)
    """
    # Note: This query doesn't explicitly check if the target folder (s.folder_id) still exists
    # if send_to_all_groups is False. That check is better handled in the task execution logic
    # (_execute_single_task in telethon_utils) to avoid complex subqueries here.
    try:
        conn = _get_db_connection()
        with db_lock:
            cursor = conn.cursor()
            # Pass current_time_ts three times for the placeholders
            cursor.execute(sql, (current_time_ts, current_time_ts, current_time_ts))
            return cursor.fetchall() # List of Row objects for due tasks
    except sqlite3.Error as e:
        log.error(f"DB Error fetching active tasks to run: {e}", exc_info=True)
        return []

def update_task_after_run(client_id, userbot_phone, run_start_time_ts, messages_sent_increment=0, error=None):
    """
    Updates task's last_run time and last_error after execution attempt.
    Also increments aggregate client stats if messages were sent.
    """
    # Update task settings: set last_run, clear/set last_error, increment task-specific count
    sql_task_update = """
        UPDATE userbot_settings
        SET last_run = ?,             -- run_start_time_ts
            last_error = ?,           -- error (can be None to clear)
            messages_sent_count = messages_sent_count + ? -- messages_sent_increment
        WHERE client_id = ? AND userbot_phone = ?
    """
    # Update aggregate client stats only if messages were successfully sent
    sql_client_update = """
        UPDATE clients
        SET total_messages_sent = total_messages_sent + ?, -- messages_sent_increment
            forwards_count = forwards_count + 1 -- Count this run as one operation if >0 messages sent
        WHERE user_id = ?
    """
    try:
        conn = _get_db_connection()
        with db_lock:
            cursor = conn.cursor()
            # Use transaction to update task and client stats together
            cursor.execute("BEGIN")
            try:
                # Update the task specific record
                cursor.execute(sql_task_update, (run_start_time_ts, error, messages_sent_increment, client_id, userbot_phone))

                # Update aggregate client stats only if messages were actually sent in this run
                if messages_sent_increment > 0:
                    cursor.execute(sql_client_update, (messages_sent_increment, client_id))

                cursor.execute("COMMIT") # Commit both updates

                log.debug(f"Updated task after run for client {client_id}, bot {userbot_phone}. Sent: {messages_sent_increment}, Error: {error}")
                return True

            except sqlite3.Error as tx_e:
                 log.error(f"DB Tx Error updating task after run for user {client_id}, bot {userbot_phone}: {tx_e}", exc_info=True)
                 cursor.execute("ROLLBACK")
                 return False

    except sqlite3.Error as e:
        log.error(f"DB Connection Error updating task after run for user {client_id}, bot {userbot_phone}: {e}", exc_info=True)
        return False

# --- Logs ---
def get_recent_logs(limit=25):
    """Retrieves the most recent log entries from the database."""
    # Ordering by timestamp DESC, then ID DESC as fallback for same timestamp
    sql = "SELECT timestamp, event, user_id, userbot_phone, details FROM logs ORDER BY timestamp DESC, id DESC LIMIT ?"
    try:
        conn = _get_db_connection()
        with db_lock:
            cursor = conn.cursor()
            cursor.execute(sql, (limit,))
            return cursor.fetchall()
    except sqlite3.Error as e:
        log.error(f"DB Error fetching recent logs: {e}")
        return []

# --- Client Stats ---
def get_client_stats(user_id):
    """Retrieves aggregate statistics for a specific client from the clients table."""
    client = find_client_by_user_id(user_id)
    if client:
        # Return stats directly from the columns
        return {
            "total_messages_sent": client.get('total_messages_sent', 0),
            "groups_reached": client.get('groups_reached', 0), # Note: This stat might be inaccurate/deprecated
            "forwards_count": client.get('forwards_count', 0)
        }
    return None # Return None if client not found

def generate_invite_code():
    """Generates a unique 8-character invite code."""
    try:
        conn = _get_db_connection()
        while True:
            code = str(uuid.uuid4().hex)[:8]  # Generate 8-character code
            with db_lock:
                cursor = conn.cursor()
                # Check if code already exists
                cursor.execute("SELECT 1 FROM clients WHERE invitation_code = ?", (code,))
                if not cursor.fetchone():
                    return code
    except sqlite3.Error as e:
        log.error(f"DB Error generating invite code: {e}")
        return None

def store_invite_code(code, days):
    """Stores a new invite code with subscription duration."""
    try:
        conn = _get_db_connection()
        end_datetime = datetime.now(UTC_TZ) + timedelta(days=days)
        sub_end_ts = int(end_datetime.timestamp())
        
        with db_lock:
            cursor = conn.cursor()
            cursor.execute("INSERT INTO clients (invitation_code, subscription_end) VALUES (?, ?)",
                         (code, sub_end_ts))
            log.info(f"Successfully stored invite code {code} with {days} days duration")
            return True
    except sqlite3.Error as e:
        log.error(f"DB Error storing invite code: {e}")
        return False

# --- Admin Task Functions ---
def create_admin_task(userbot_phone: str, message: str, schedule: str, target: str, created_by: int) -> int | None:
    """Creates a new admin task and returns its ID."""
    sql = """
    INSERT INTO admin_tasks (userbot_phone, message, schedule, target, created_by, created_at, status)
    VALUES (?, ?, ?, ?, ?, ?, 'inactive')
    """
    try:
        conn = _get_db_connection()
        with db_lock:
            cursor = conn.cursor()
            now_ts = int(datetime.now(UTC_TZ).timestamp())
            cursor.execute(sql, (userbot_phone, message, schedule, target, created_by, now_ts))
            return cursor.lastrowid
    except sqlite3.Error as e:
        log.error(f"DB Error creating admin task: {e}")
        return None

def get_admin_tasks(page: int = 0, per_page: int = 10) -> tuple[list, int]:
    """Returns a tuple of (tasks list, total count)."""
    sql_count = "SELECT COUNT(*) FROM admin_tasks"
    sql_tasks = """
    SELECT t.*, u.status as userbot_status 
    FROM admin_tasks t 
    LEFT JOIN userbots u ON t.userbot_phone = u.phone_number 
    ORDER BY t.id DESC LIMIT ? OFFSET ?
    """
    try:
        conn = _get_db_connection()
        with db_lock:
            cursor = conn.cursor()
            cursor.execute(sql_count)
            total = cursor.fetchone()[0]
            
            cursor.execute(sql_tasks, (per_page, page * per_page))
            tasks = cursor.fetchall()
            return list(tasks), total
    except sqlite3.Error as e:
        log.error(f"DB Error getting admin tasks: {e}")
        return [], 0

def get_admin_task(task_id: int) -> dict | None:
    """Gets a single admin task by ID."""
    sql = """
    SELECT t.*, u.status as userbot_status 
    FROM admin_tasks t 
    LEFT JOIN userbots u ON t.userbot_phone = u.phone_number 
    WHERE t.id = ?
    """
    try:
        conn = _get_db_connection()
        with db_lock:
            cursor = conn.cursor()
            cursor.execute(sql, (task_id,))
            task = cursor.fetchone()
            return dict(task) if task else None
    except sqlite3.Error as e:
        log.error(f"DB Error getting admin task {task_id}: {e}")
        return None

def update_admin_task(task_id: int, updates: dict) -> bool:
    """Updates an admin task. Updates should be a dict of column:value pairs."""
    allowed_fields = {'message', 'schedule', 'target', 'status'}
    update_fields = {k: v for k, v in updates.items() if k in allowed_fields}
    
    if not update_fields:
        return False
        
    sql = f"""
    UPDATE admin_tasks 
    SET {', '.join(f'{k} = ?' for k in update_fields.keys())}
    WHERE id = ?
    """
    try:
        conn = _get_db_connection()
        with db_lock:
            cursor = conn.cursor()
            cursor.execute(sql, (*update_fields.values(), task_id))
            return cursor.rowcount > 0
    except sqlite3.Error as e:
        log.error(f"DB Error updating admin task {task_id}: {e}")
        return False

def delete_admin_task(task_id: int) -> bool:
    """Deletes an admin task."""
    sql = "DELETE FROM admin_tasks WHERE id = ?"
    try:
        conn = _get_db_connection()
        with db_lock:
            cursor = conn.cursor()
            cursor.execute(sql, (task_id,))
            return cursor.rowcount > 0
    except sqlite3.Error as e:
        log.error(f"DB Error deleting admin task {task_id}: {e}")
        return False

def toggle_admin_task_status(task_id: int) -> bool:
    """Toggles an admin task's status between active and inactive."""
    sql = """
    UPDATE admin_tasks 
    SET status = CASE WHEN status = 'active' THEN 'inactive' ELSE 'active' END 
    WHERE id = ?
    """
    try:
        conn = _get_db_connection()
        with db_lock:
            cursor = conn.cursor()
            cursor.execute(sql, (task_id,))
            return cursor.rowcount > 0
    except sqlite3.Error as e:
        log.error(f"DB Error toggling admin task {task_id}: {e}")
        return False

def get_active_admin_tasks_to_run(current_time_ts: int) -> list:
    """Returns active admin tasks that are due to run."""
    sql = """
    SELECT t.*, u.status as userbot_status 
    FROM admin_tasks t 
    JOIN userbots u ON t.userbot_phone = u.phone_number 
    WHERE t.status = 'active' 
    AND u.status = 'active'
    AND (t.next_run IS NULL OR t.next_run <= ?)
    """
    try:
        conn = _get_db_connection()
        with db_lock:
            cursor = conn.cursor()
            cursor.execute(sql, (current_time_ts,))
            return cursor.fetchall()
    except sqlite3.Error as e:
        log.error(f"DB Error getting active admin tasks: {e}")
        return []

def update_admin_task_run(task_id: int, last_run: int, next_run: int, error: str = None) -> bool:
    """Updates the last run and next run times for an admin task."""
    sql = """
    UPDATE admin_tasks 
    SET last_run = ?, next_run = ?, status = CASE WHEN ? IS NOT NULL THEN 'inactive' ELSE status END
    WHERE id = ?
    """
    try:
        conn = _get_db_connection()
        with db_lock:
            cursor = conn.cursor()
            cursor.execute(sql, (last_run, next_run, error, task_id))
            return cursor.rowcount > 0
    except sqlite3.Error as e:
        log.error(f"DB Error updating admin task run {task_id}: {e}")
        return False

# --- Initialize DB on Import ---
# Ensure schema is checked/created when the module is first imported.
try:
     init_db()
     log.info("Database module initialized and schema verified.")
except Exception as e:
     # Log critical error if DB init fails on startup
     log.critical(f"FATAL: Failed to initialize database on module load: {e}", exc_info=True)
     # Depending on requirements, might want to sys.exit(1) here if DB is absolutely essential
     # For now, just log the critical failure. The bot might crash later if DB is needed.

# --- END OF FILE database.py ---
