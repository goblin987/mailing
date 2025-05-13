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
            raise RuntimeError(f"Failed to connect to the database: {e}") from e
    return _connection

def init_db():
    """Initializes the database schema if tables don't exist."""
    conn = _get_db_connection() # Ensure connection is established
    # Define the database schema with Foreign Key constraints and indices
    schema = '''
        PRAGMA foreign_keys = ON; -- Ensure FKs are checked

        CREATE TABLE IF NOT EXISTS clients (
            invitation_code TEXT PRIMARY KEY NOT NULL,
            user_id INTEGER UNIQUE, -- Telegram User ID, can be NULL until activated
            subscription_end INTEGER NOT NULL, -- Unix timestamp (UTC)
            dedicated_userbots TEXT, -- DEPRECATED/IGNORED: Comma-separated phone numbers initially assigned
            -- Aggregated stats - Updated by update_task_after_run
            forwards_count INTEGER DEFAULT 0 NOT NULL, -- Counts task runs that sent >= 1 message
            groups_reached INTEGER DEFAULT 0 NOT NULL, -- DEPRECATED/IGNORED: This column is hard to update reliably
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
            group_link TEXT, -- Store original link if available
            added_by INTEGER NOT NULL, -- Client's user_id, FK to clients(user_id)
            folder_id INTEGER NOT NULL, -- FK to folders(id)
            -- A user shouldn't add the same group ID to the same folder multiple times
            UNIQUE(group_id, added_by, folder_id),
            FOREIGN KEY (added_by) REFERENCES clients(user_id) ON DELETE CASCADE, -- If client deleted, remove their groups
            FOREIGN KEY (folder_id) REFERENCES folders(id) ON DELETE CASCADE -- If folder deleted, remove groups within it
        );

        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp INTEGER NOT NULL, -- Unix timestamp (UTC)
            event TEXT NOT NULL, -- Type of event (e.g., 'Client Activated', 'Task Run', 'Join Failed')
            user_id INTEGER, -- Client User ID or Admin ID involved
            userbot_phone TEXT, -- Involved userbot phone number
            details TEXT -- Additional information/error message
        );

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
            last_run INTEGER, -- Unix timestamp (UTC) of the last successful run start
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
            message TEXT NOT NULL, -- Could be message text or link
            schedule TEXT NOT NULL, -- Cron format or simple interval string? Using cron for now.
            target TEXT NOT NULL, -- Group username, ID, or link
            status TEXT CHECK(status IN ('active', 'inactive')) DEFAULT 'inactive' NOT NULL,
            last_run INTEGER, -- Unix timestamp of last run
            next_run INTEGER, -- Unix timestamp of next scheduled run
            created_by INTEGER NOT NULL, -- Admin user ID (FK to clients.user_id)
            created_at INTEGER NOT NULL, -- Unix timestamp
            FOREIGN KEY (userbot_phone) REFERENCES userbots(phone_number) ON DELETE CASCADE,
            FOREIGN KEY (created_by) REFERENCES clients(user_id)
        );
        CREATE INDEX IF NOT EXISTS idx_admin_tasks_status ON admin_tasks (status);
        CREATE INDEX IF NOT EXISTS idx_admin_tasks_next_run ON admin_tasks (next_run);

        -- Create Indexes for faster lookups
        CREATE INDEX IF NOT EXISTS idx_clients_user_id ON clients (user_id);
        CREATE INDEX IF NOT EXISTS idx_userbots_assigned_client ON userbots (assigned_client);
        CREATE INDEX IF NOT EXISTS idx_userbots_status ON userbots (status);
        CREATE INDEX IF NOT EXISTS idx_folders_created_by ON folders (created_by);
        CREATE INDEX IF NOT EXISTS idx_target_groups_folder_id ON target_groups (folder_id);
        CREATE INDEX IF NOT EXISTS idx_target_groups_added_by ON target_groups (added_by);
        CREATE INDEX IF NOT EXISTS idx_logs_timestamp ON logs (timestamp DESC);
        CREATE INDEX IF NOT EXISTS idx_userbot_settings_active ON userbot_settings (status, start_time, repetition_interval, last_run) WHERE status = 'active';
        CREATE INDEX IF NOT EXISTS idx_userbot_settings_folder_id ON userbot_settings (folder_id);
        CREATE INDEX IF NOT EXISTS idx_userbot_settings_client_id ON userbot_settings (client_id);
    '''
    try:
        with db_lock:
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
            # PRAGMA wal_checkpoint removed - SQLite handles this on close usually
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
            conn.execute(sql, (timestamp, event, user_id, userbot_phone, str(details)))
        log.debug(f"DBLog-{event}: User={user_id}, Bot={userbot_phone}, Details={details[:100]}...")
    except sqlite3.Error as e:
        log.error(f"CRITICAL: Failed to log event to DB: {e} - Event: {event}, Details: {details[:100]}...")
    except Exception as e:
         log.error(f"Unexpected error logging event to DB: {e}", exc_info=True)


# --- Client Functions ---
def find_client_by_user_id(user_id):
    """Find a client by their Telegram user ID."""
    try:
        with db_lock:
            conn = _get_db_connection()
            cursor = conn.execute(
                "SELECT * FROM clients WHERE user_id = ?",
                (user_id,)
            )
            result = cursor.fetchone()
            return dict(result) if result else None
    except sqlite3.Error as e:
        log.error(f"Database error in find_client_by_user_id: {e}", exc_info=True)
        return None

def find_client_by_code(code):
    """Find a client by their invitation code."""
    try:
        with db_lock:
            conn = _get_db_connection()
            cursor = conn.execute(
                "SELECT * FROM clients WHERE invitation_code = ?",
                (code,)
            )
            result = cursor.fetchone()
            return dict(result) if result else None
    except sqlite3.Error as e:
        log.error(f"Database error in find_client_by_code: {e}", exc_info=True)
        return None

def activate_client(code, user_id):
    """Activate a client's account with their invitation code."""
    try:
        with db_lock:
            conn = _get_db_connection()
            # Start transaction
            conn.execute("BEGIN")
            try:
                # Check if code exists and is not used
                cursor = conn.execute(
                    "SELECT user_id FROM clients WHERE invitation_code = ?",
                    (code,)
                )
                result = cursor.fetchone()
                if not result:
                    conn.execute("ROLLBACK")
                    return False

                # Update the client record
                conn.execute(
                    "UPDATE clients SET user_id = ? WHERE invitation_code = ?",
                    (user_id, code)
                )
                # Commit transaction
                conn.execute("COMMIT")
                return True
            except sqlite3.Error as e:
                conn.execute("ROLLBACK")
                log.error(f"Database error in activate_client transaction: {e}", exc_info=True)
                return False
    except sqlite3.Error as e:
        log.error(f"Database error in activate_client: {e}", exc_info=True)
        return False

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
            return cursor.rowcount > 0
    except sqlite3.Error as e:
        log.error(f"DB Error setting language for user {user_id}: {e}")
        return False

def create_invitation(code, sub_end_ts):
    """Creates a new client invitation code record."""
    sql = "INSERT INTO clients (invitation_code, subscription_end) VALUES (?, ?)"
    try:
        conn = _get_db_connection()
        with db_lock:
            conn.execute(sql, (code, sub_end_ts))
        return True
    except sqlite3.IntegrityError:
        log.warning(f"Attempted to insert duplicate invitation code: {code}")
        return False
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
    sql = """
        SELECT c.user_id, c.invitation_code, c.subscription_end,
               COUNT(u.phone_number) as bot_count
        FROM clients c
        LEFT JOIN userbots u ON c.invitation_code = u.assigned_client
        WHERE c.user_id IS NOT NULL
        GROUP BY c.invitation_code
        ORDER BY c.subscription_end ASC
    """
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

def get_all_userbots(assigned_status=None, exclude_status=None):
    """Retrieves all userbots, optionally filtered by assigned status and excluded statuses."""
    try:
        conn = _get_db_connection()
        with db_lock:
            cursor = conn.cursor()
            sql = "SELECT * FROM userbots"
            params = []
            conditions = []

            if assigned_status is not None:
                conditions.append("assigned_client IS NOT NULL" if assigned_status else "assigned_client IS NULL")

            if exclude_status and isinstance(exclude_status, list) and exclude_status:
                placeholders = ','.join('?' * len(exclude_status))
                conditions.append(f"status NOT IN ({placeholders})")
                params.extend(exclude_status)

            if conditions:
                sql += " WHERE " + " AND ".join(conditions)

            sql += " ORDER BY phone_number ASC" # Added default sorting

            cursor.execute(sql, params)
            return cursor.fetchall()
    except sqlite3.Error as e:
        log.error(f"DB Error getting all userbots: {e}")
        return []

def get_client_bots(user_id):
    """Gets the phone numbers of bots currently assigned to a client's active code."""
    client = find_client_by_user_id(user_id)
    if not client: return []
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
        WHERE phone_number = excluded.phone_number;
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
    sql = """
        UPDATE userbots
        SET status = ?,
            username = COALESCE(?, username),
            last_error = ?
        WHERE phone_number = ?
    """
    try:
        conn = _get_db_connection()
        updated = False
        with db_lock:
            cursor = conn.cursor()
            cursor.execute(sql, (status, username, last_error, phone))
            updated = cursor.rowcount > 0
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
    if not phones_to_assign: log.warning(f"assign_userbots_to_client called with empty list for code {code}."); return True, "No bots provided to assign."
    sql_check_client = "SELECT invitation_code FROM clients WHERE invitation_code = ?"; sql_update_bot = "UPDATE userbots SET assigned_client = ? WHERE phone_number = ? AND assigned_client IS NULL"
    updated_count = 0; failed_phones = []; success = False; msg = "Assignment process failed."
    try:
        conn = _get_db_connection();
        with db_lock:
            cursor = conn.cursor(); cursor.execute(sql_check_client, (code,));
            if not cursor.fetchone(): log.error(f"Cannot assign bots: Client code '{code}' not found."); return False, "Client code not found."
            cursor.execute("BEGIN");
            try:
                for phone in phones_to_assign:
                     cursor.execute(sql_update_bot, (code, phone));
                     if cursor.rowcount > 0: updated_count += 1
                     else:
                         cursor.execute("SELECT assigned_client FROM userbots WHERE phone_number = ?", (phone,)); bot_info = cursor.fetchone()
                         if bot_info and bot_info['assigned_client'] is not None: log.warning(f"Could not assign bot {phone} to {code}: Already assigned to {bot_info['assigned_client']}"); failed_phones.append(f"{phone} (already assigned)")
                         elif not bot_info: log.warning(f"Could not assign bot {phone} to {code}: Bot not found."); failed_phones.append(f"{phone} (not found)")
                         else: log.warning(f"Could not assign bot {phone} to {code}: Bot found but update failed (unexpected)."); failed_phones.append(f"{phone} (update error)")
                cursor.execute("COMMIT"); success = True
                log.info(f"Assigned {updated_count}/{len(phones_to_assign)} bots to client {code}.")
                if updated_count != len(phones_to_assign): msg = f"Successfully assigned {updated_count} userbots. Failed: {', '.join(failed_phones)}"; log.warning(f"Some bots requested for assignment to {code} were unavailable or already assigned: {failed_phones}")
                else: msg = f"Successfully assigned {updated_count} userbots."
            except sqlite3.Error as tx_e: log.error(f"DB Tx Error assigning bots to client {code}: {tx_e}", exc_info=True); cursor.execute("ROLLBACK"); success = False; msg = "Database transaction error during assignment."
    except sqlite3.Error as e: log.error(f"DB Connection Error assigning bots to client {code}: {e}", exc_info=True); success = False; msg = "Database connection error during assignment."
    return success, msg

def remove_userbot(phone):
    """Removes a userbot record from the database."""
    sql = "DELETE FROM userbots WHERE phone_number = ?"
    try:
        conn = _get_db_connection(); deleted_rows = 0
        with db_lock:
            cursor = conn.cursor()
            try: cursor.execute(sql, (phone,)); deleted_rows = cursor.rowcount
            except sqlite3.Error as db_exec_e: log.error(f"DB Error executing remove userbot {phone}: {db_exec_e}", exc_info=True); return False
        if deleted_rows > 0: log.info(f"Removed userbot {phone} from database."); return True
        else: log.warning(f"Attempted to remove userbot {phone}, but it was not found in the database."); return False
    except sqlite3.Error as e: log.error(f"DB Connection/General Error removing userbot {phone}: {e}", exc_info=True); return False

def get_unassigned_userbots(limit):
    """Gets a list of phone numbers for active, unassigned userbots."""
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
    except sqlite3.IntegrityError:
         log.warning(f"Attempt to create duplicate folder name '{name}' for user {user_id}.")
         return None # Indicate duplicate
    except sqlite3.Error as e:
        log.error(f"DB Error adding folder '{name}' for user {user_id}: {e}")
        return False # Indicate general error

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
        conn = _get_db_connection(); deleted_rows = 0
        with db_lock:
             cursor = conn.cursor(); cursor.execute(sql, (folder_id, user_id)); deleted_rows = cursor.rowcount
        if deleted_rows > 0: log.info(f"Deleted folder ID {folder_id} for user {user_id}."); log_event_db("Folder Deleted", f"Folder ID: {folder_id}", user_id=user_id); return True
        else: log.warning(f"Attempt to delete non-existent or unauthorized folder ID {folder_id} by user {user_id}."); return False
    except sqlite3.Error as e: log.error(f"DB Error deleting folder ID {folder_id} for user {user_id}: {e}"); return False

def rename_folder(folder_id, user_id, new_name):
    """Renames a folder for a specific user, checking for name uniqueness."""
    sql_check = "SELECT 1 FROM folders WHERE name = ? AND created_by = ? AND id != ?"
    sql_update = "UPDATE folders SET name = ? WHERE id = ? AND created_by = ?"
    try:
        conn = _get_db_connection()
        with db_lock:
            cursor = conn.cursor(); cursor.execute("BEGIN")
            try:
                 cursor.execute(sql_check, (new_name, user_id, folder_id))
                 if cursor.fetchone(): log.warning(f"Rename failed: Folder name '{new_name}' already exists for user {user_id}."); cursor.execute("ROLLBACK"); return False, "name_exists"
                 cursor.execute(sql_update, (new_name, folder_id, user_id))
                 if cursor.rowcount > 0: cursor.execute("COMMIT"); log.info(f"Renamed folder ID {folder_id} to '{new_name}' for user {user_id}."); return True, "success"
                 else: log.warning(f"Rename failed: Folder ID {folder_id} not found or not owned by user {user_id}."); cursor.execute("ROLLBACK"); return False, "not_found_or_unauthorized"
            except sqlite3.Error as tx_e: log.error(f"DB Tx Error renaming folder {folder_id} for user {user_id}: {tx_e}", exc_info=True); cursor.execute("ROLLBACK"); return False, "db_error"
    except sqlite3.Error as e: log.error(f"DB Connection Error renaming folder {folder_id} for user {user_id}: {e}", exc_info=True); return False, "db_error"


# --- Target Group Functions ---
def add_target_group(group_id, group_name, group_link, user_id, folder_id):
    """Adds a target group to a user's folder."""
    if group_id is None: log.warning(f"Attempted to add target group without ID to folder {folder_id} by user {user_id}. Link: {group_link}"); return False
    sql = """
        INSERT INTO target_groups (group_id, group_name, group_link, added_by, folder_id)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(group_id, added_by, folder_id) DO NOTHING
    """
    try:
        conn = _get_db_connection()
        with db_lock:
            cursor = conn.cursor()
            cursor.execute(sql, (group_id, group_name, group_link, user_id, folder_id))
            if cursor.rowcount > 0: log.info(f"Added group ID {group_id} ('{group_name}') to folder {folder_id} for user {user_id}."); return True
            else: log.debug(f"Group ID {group_id} already exists in folder {folder_id} for user {user_id}. Ignored."); return None # Indicate duplicate/ignored
    except sqlite3.IntegrityError as fk_e: log.error(f"DB Integrity Error adding target group {group_id} to folder {folder_id} for user {user_id}: {fk_e}. FK constraint likely failed."); return False
    except sqlite3.Error as e: log.error(f"DB Error adding target group {group_id} to folder {folder_id} for user {user_id}: {e}"); return False

def get_target_groups_by_folder(folder_id):
    """Gets a list of group IDs belonging to a specific folder."""
    sql = "SELECT group_id FROM target_groups WHERE folder_id = ?"
    try:
        conn = _get_db_connection()
        with db_lock:
            cursor = conn.cursor(); cursor.execute(sql, (folder_id,)); return [row['group_id'] for row in cursor.fetchall()]
    except sqlite3.Error as e: log.error(f"DB Error fetching group IDs for folder {folder_id}: {e}"); return []

def get_target_groups_details_by_folder(folder_id):
     """Gets details (id, group_id, name, link) of groups in a folder."""
     sql = "SELECT id, group_id, group_name, group_link FROM target_groups WHERE folder_id = ? ORDER BY group_name ASC, id ASC"
     try:
         conn = _get_db_connection()
         with db_lock: cursor = conn.cursor(); cursor.execute(sql, (folder_id,)); return cursor.fetchall()
     except sqlite3.Error as e: log.error(f"DB Error fetching group details for folder {folder_id}: {e}"); return []

def remove_target_groups_by_db_id(db_ids_to_remove: list[int], user_id: int) -> int:
    """Removes target groups using their database primary key IDs, ensuring user owns them."""
    if not db_ids_to_remove: return 0
    try: safe_ids = tuple(int(id_) for id_ in db_ids_to_remove)
    except (ValueError, TypeError): log.error(f"Invalid non-integer ID provided for group removal by user {user_id}."); return -1
    placeholders = ','.join('?' * len(safe_ids)); sql = f"DELETE FROM target_groups WHERE id IN ({placeholders}) AND added_by = ?"; params = safe_ids + (user_id,)
    try:
        conn = _get_db_connection(); deleted_count = -1
        with db_lock:
             cursor = conn.cursor(); cursor.execute("BEGIN")
             try:
                 cursor.execute(sql, params); deleted_count = cursor.rowcount; cursor.execute("COMMIT")
                 if deleted_count > 0: log.info(f"User {user_id} removed {deleted_count} target groups by DB ID.")
                 elif deleted_count == 0: log.warning(f"User {user_id} tried to remove target group IDs, but none matched or belonged to them: {safe_ids}")
             except sqlite3.Error as tx_e: log.error(f"DB Tx Error removing target groups by DB ID for user {user_id}: {tx_e}", exc_info=True); cursor.execute("ROLLBACK"); deleted_count = -1
        return deleted_count
    except sqlite3.Error as e: log.error(f"DB Connection Error removing target groups by DB ID for user {user_id}: {e}", exc_info=True); return -1

def remove_all_target_groups_from_folder(folder_id, user_id):
    """Removes all target groups associated with a specific folder for a user."""
    sql = "DELETE FROM target_groups WHERE folder_id = ? AND added_by = ?"
    try:
        conn = _get_db_connection(); deleted_count = -1
        with db_lock: cursor = conn.cursor(); cursor.execute(sql, (folder_id, user_id)); deleted_count = cursor.rowcount
        log.info(f"User {user_id} cleared {deleted_count} groups from folder {folder_id}.")
        if deleted_count > 0: log_event_db("Folder Groups Cleared", f"Folder ID: {folder_id}, Count: {deleted_count}", user_id=user_id)
        return deleted_count
    except sqlite3.Error as e: log.error(f"DB Error clearing folder {folder_id} for user {user_id}: {e}"); return -1

# --- Userbot Task Settings Functions ---
def get_userbot_task_settings(client_id, userbot_phone):
    """Retrieves task settings for a specific userbot and client."""
    sql = "SELECT * FROM userbot_settings WHERE client_id = ? AND userbot_phone = ?"
    try:
        conn = _get_db_connection()
        with db_lock: cursor = conn.cursor(); cursor.execute(sql, (client_id, userbot_phone)); return cursor.fetchone()
    except sqlite3.Error as e: log.error(f"DB Error getting task settings for user {client_id}, bot {userbot_phone}: {e}"); return None

def save_userbot_task_settings(client_id, userbot_phone, settings: dict):
    """Saves or updates task settings for a userbot using ON CONFLICT."""
    params = {
        'client_id': client_id, 'userbot_phone': userbot_phone,
        'message_link': settings.get('message_link'), 'fallback_message_link': settings.get('fallback_message_link'),
        'start_time': settings.get('start_time'), 'repetition_interval': settings.get('repetition_interval'),
        'status': settings.get('status', 'inactive'), 'folder_id': settings.get('folder_id'),
        'send_to_all_groups': int(settings.get('send_to_all_groups', 0)),
        'last_run': settings.get('last_run'), 'last_error': settings.get('last_error'),
    }
    sql = """
        INSERT INTO userbot_settings (
            client_id, userbot_phone, message_link, fallback_message_link, start_time,
            repetition_interval, status, folder_id, send_to_all_groups, last_run, last_error, messages_sent_count
        ) VALUES (
            :client_id, :userbot_phone, :message_link, :fallback_message_link, :start_time,
            :repetition_interval, :status, :folder_id, :send_to_all_groups, :last_run, :last_error,
            COALESCE((SELECT messages_sent_count FROM userbot_settings WHERE client_id = :client_id AND userbot_phone = :userbot_phone), 0)
        )
        ON CONFLICT(client_id, userbot_phone) DO UPDATE SET
            message_link=excluded.message_link,
            fallback_message_link=excluded.fallback_message_link,
            start_time=excluded.start_time,
            repetition_interval=excluded.repetition_interval,
            status=excluded.status,
            folder_id=excluded.folder_id,
            send_to_all_groups=excluded.send_to_all_groups,
            last_error = NULL -- Clear error on successful manual save
        WHERE client_id = excluded.client_id AND userbot_phone = excluded.userbot_phone;
    """
    try:
        conn = _get_db_connection()
        with db_lock: conn.execute(sql, params)
        log.info(f"Saved task settings for user {client_id}, bot {userbot_phone}. Status: {params['status']}")
        return True
    except sqlite3.Error as e: log.error(f"DB Error saving task settings for user {client_id}, bot {userbot_phone}: {e}", exc_info=True); return False

def get_active_tasks_to_run(current_time_ts):
    """Finds tasks that are due to run based on various criteria."""
    sql = """
        SELECT s.*, u.session_file, u.api_id, u.api_hash
        FROM userbot_settings s
        JOIN userbots u ON s.userbot_phone = u.phone_number
        JOIN clients c ON s.client_id = c.user_id
        WHERE
            s.status = 'active' AND u.status = 'active' AND c.subscription_end > ? AND s.start_time IS NOT NULL AND
            s.start_time <= ? AND s.repetition_interval IS NOT NULL AND s.repetition_interval > 0 AND
            s.message_link IS NOT NULL AND (s.last_run IS NULL OR (s.last_run + (s.repetition_interval * 60)) <= ?)
    """
    try:
        conn = _get_db_connection()
        with db_lock:
            cursor = conn.cursor()
            cursor.execute(sql, (current_time_ts, current_time_ts, current_time_ts))
            return cursor.fetchall()
    except sqlite3.Error as e: log.error(f"DB Error fetching active tasks to run: {e}", exc_info=True); return []

def update_task_after_run(client_id, userbot_phone, run_start_time_ts, messages_sent_increment=0, error=None):
    """Updates task's last_run time and stats after execution attempt."""
    sql_task_update = """
        UPDATE userbot_settings SET last_run = ?, last_error = ?, messages_sent_count = messages_sent_count + ?
        WHERE client_id = ? AND userbot_phone = ?"""
    sql_client_update = """
        UPDATE clients SET total_messages_sent = total_messages_sent + ?, forwards_count = forwards_count + 1
        WHERE user_id = ?"""
    try:
        conn = _get_db_connection()
        with db_lock:
            cursor = conn.cursor(); cursor.execute("BEGIN")
            try:
                cursor.execute(sql_task_update, (run_start_time_ts, error, messages_sent_increment, client_id, userbot_phone))
                if messages_sent_increment > 0: cursor.execute(sql_client_update, (messages_sent_increment, client_id))
                cursor.execute("COMMIT");
                log.debug(f"Updated task after run for client {client_id}, bot {userbot_phone}. Sent: {messages_sent_increment}, Error: {error}")
                return True
            except sqlite3.Error as tx_e: log.error(f"DB Tx Error updating task after run for user {client_id}, bot {userbot_phone}: {tx_e}", exc_info=True); cursor.execute("ROLLBACK"); return False
    except sqlite3.Error as e: log.error(f"DB Connection Error updating task after run for user {client_id}, bot {userbot_phone}: {e}", exc_info=True); return False

# --- Logs ---
def get_recent_logs(limit=25):
    """Retrieves the most recent log entries from the database."""
    sql = "SELECT timestamp, event, user_id, userbot_phone, details FROM logs ORDER BY timestamp DESC, id DESC LIMIT ?"
    try:
        conn = _get_db_connection()
        with db_lock: cursor = conn.cursor(); cursor.execute(sql, (limit,)); return cursor.fetchall()
    except sqlite3.Error as e: log.error(f"DB Error fetching recent logs: {e}"); return []

# --- Client Stats ---
def get_client_stats(user_id):
    """Retrieves aggregate statistics for a specific client from the clients table."""
    client = find_client_by_user_id(user_id)
    if client: return {"total_messages_sent": client.get('total_messages_sent', 0), "groups_reached": client.get('groups_reached', 0), "forwards_count": client.get('forwards_count', 0)}
    return None

# --- Invite Code Functions ---
def generate_invite_code():
    """Generates a unique 8-character invite code."""
    try:
        conn = _get_db_connection()
        while True:
            code = str(uuid.uuid4().hex)[:8]
            with db_lock:
                cursor = conn.cursor(); cursor.execute("SELECT 1 FROM clients WHERE invitation_code = ?", (code,))
                if not cursor.fetchone(): return code
    except sqlite3.Error as e: log.error(f"DB Error generating invite code: {e}"); return None

def store_invite_code(code, days):
    """Stores a new invite code with subscription duration."""
    # This seems deprecated by create_invitation, but kept for potential direct use.
    try:
        conn = _get_db_connection(); end_datetime = datetime.now(UTC_TZ) + timedelta(days=days); sub_end_ts = int(end_datetime.timestamp())
        with db_lock: cursor = conn.cursor(); cursor.execute("INSERT INTO clients (invitation_code, subscription_end) VALUES (?, ?)", (code, sub_end_ts));
        log.info(f"Successfully stored invite code {code} with {days} days duration")
        return True
    except sqlite3.Error as e: log.error(f"DB Error storing invite code: {e}"); return False

# --- Admin Task Functions ---
def create_admin_task(userbot_phone: str, message: str, schedule: str, target: str, created_by: int) -> int | None:
    """Creates a new admin task and returns its ID."""
    sql = "INSERT INTO admin_tasks (userbot_phone, message, schedule, target, created_by, created_at, status) VALUES (?, ?, ?, ?, ?, ?, 'inactive')"
    try:
        conn = _get_db_connection()
        with db_lock: cursor = conn.cursor(); now_ts = int(datetime.now(UTC_TZ).timestamp()); cursor.execute(sql, (userbot_phone, message, schedule, target, created_by, now_ts)); return cursor.lastrowid
    except sqlite3.Error as e: log.error(f"DB Error creating admin task: {e}"); return None

def get_admin_tasks(page: int = 0, per_page: int = 10) -> tuple[list, int]:
    """Returns a tuple of (tasks list, total count)."""
    sql_count = "SELECT COUNT(*) FROM admin_tasks"; sql_tasks = "SELECT t.*, u.status as userbot_status FROM admin_tasks t LEFT JOIN userbots u ON t.userbot_phone = u.phone_number ORDER BY t.id DESC LIMIT ? OFFSET ?"
    try:
        conn = _get_db_connection()
        with db_lock:
            cursor = conn.cursor(); cursor.execute(sql_count); total = cursor.fetchone()[0]
            cursor.execute(sql_tasks, (per_page, page * per_page)); tasks = cursor.fetchall(); return list(tasks), total
    except sqlite3.Error as e: log.error(f"DB Error getting admin tasks: {e}"); return [], 0

def get_admin_task(task_id: int) -> dict | None:
    """Gets a single admin task by ID."""
    sql = "SELECT t.*, u.status as userbot_status FROM admin_tasks t LEFT JOIN userbots u ON t.userbot_phone = u.phone_number WHERE t.id = ?"
    try:
        conn = _get_db_connection()
        with db_lock: cursor = conn.cursor(); cursor.execute(sql, (task_id,)); task = cursor.fetchone(); return dict(task) if task else None
    except sqlite3.Error as e: log.error(f"DB Error getting admin task {task_id}: {e}"); return None

def update_admin_task(task_id: int, updates: dict) -> bool:
    """Updates an admin task. Updates should be a dict of column:value pairs."""
    allowed_fields = {'message', 'schedule', 'target', 'status'}; update_fields = {k: v for k, v in updates.items() if k in allowed_fields}
    if not update_fields: return False
    sql = f"UPDATE admin_tasks SET {', '.join(f'{k} = ?' for k in update_fields.keys())} WHERE id = ?"
    try:
        conn = _get_db_connection()
        with db_lock: cursor = conn.cursor(); cursor.execute(sql, (*update_fields.values(), task_id)); return cursor.rowcount > 0
    except sqlite3.Error as e: log.error(f"DB Error updating admin task {task_id}: {e}"); return False

def delete_admin_task(task_id: int) -> bool:
    """Deletes an admin task."""
    sql = "DELETE FROM admin_tasks WHERE id = ?"
    try:
        conn = _get_db_connection()
        with db_lock: cursor = conn.cursor(); cursor.execute(sql, (task_id,)); return cursor.rowcount > 0
    except sqlite3.Error as e: log.error(f"DB Error deleting admin task {task_id}: {e}"); return False

def toggle_admin_task_status(task_id: int) -> bool:
    """Toggles an admin task's status between active and inactive."""
    sql = "UPDATE admin_tasks SET status = CASE WHEN status = 'active' THEN 'inactive' ELSE 'active' END WHERE id = ?"
    try:
        conn = _get_db_connection()
        with db_lock: cursor = conn.cursor(); cursor.execute(sql, (task_id,)); return cursor.rowcount > 0
    except sqlite3.Error as e: log.error(f"DB Error toggling admin task {task_id}: {e}"); return False

def get_active_admin_tasks_to_run(current_time_ts: int) -> list:
    """Returns active admin tasks that are due to run."""
    sql = """
    SELECT t.*, u.status as userbot_status FROM admin_tasks t JOIN userbots u ON t.userbot_phone = u.phone_number
    WHERE t.status = 'active' AND u.status = 'active' AND (t.next_run IS NULL OR t.next_run <= ?)"""
    try:
        conn = _get_db_connection()
        with db_lock: cursor = conn.cursor(); cursor.execute(sql, (current_time_ts,)); return cursor.fetchall()
    except sqlite3.Error as e: log.error(f"DB Error getting active admin tasks: {e}"); return []

def update_admin_task_run(task_id: int, last_run: int, next_run: int, error: str = None) -> bool:
    """Updates the last run and next run times for an admin task."""
    # Also sets status to inactive if an error occurred during the run
    sql = "UPDATE admin_tasks SET last_run = ?, next_run = ?, status = CASE WHEN ? IS NOT NULL THEN 'inactive' ELSE status END WHERE id = ?"
    try:
        conn = _get_db_connection()
        with db_lock: cursor = conn.cursor(); cursor.execute(sql, (last_run, next_run, error, task_id)); return cursor.rowcount > 0
    except sqlite3.Error as e: log.error(f"DB Error updating admin task run {task_id}: {e}"); return False

# --- Initialize DB on Import ---
try:
     init_db()
     log.info("Database module initialized and schema verified.")
except Exception as e:
     log.critical(f"FATAL: Failed to initialize database on module load: {e}", exc_info=True)
     # Depending on requirements, might want to sys.exit(1) here if DB is absolutely essential

# --- END OF FILE database.py ---

# Add this after init_db() function
def init_test_data():
    """Initialize test data for development."""
    try:
        with db_lock:
            conn = _get_db_connection()
            # Start transaction
            conn.execute("BEGIN")
            try:
                # Add a test invitation code
                test_code = "12345678"
                test_end_date = int((datetime.now(UTC_TZ) + timedelta(days=30)).timestamp())
                
                # Check if test code already exists
                cursor = conn.execute(
                    "SELECT invitation_code FROM clients WHERE invitation_code = ?",
                    (test_code,)
                )
                if not cursor.fetchone():
                    conn.execute(
                        "INSERT INTO clients (invitation_code, subscription_end, language) VALUES (?, ?, ?)",
                        (test_code, test_end_date, 'en')
                    )
                    log.info(f"Added test invitation code: {test_code}")
                
                # Commit transaction
                conn.execute("COMMIT")
                return True
            except sqlite3.Error as e:
                conn.execute("ROLLBACK")
                log.error(f"Database error in init_test_data transaction: {e}", exc_info=True)
                return False
    except sqlite3.Error as e:
        log.error(f"Database error in init_test_data: {e}", exc_info=True)
        return False

# Call init_test_data after database initialization
init_db()
init_test_data()
