# database.py
import sqlite3
import threading
from datetime import datetime
import os
from config import DB_PATH, UTC_TZ, SESSION_DIR, log # Import constants and logger

db_lock = threading.RLock()
_connection = None # Internal variable to hold the connection

def _get_db_connection():
    """Establishes and returns a database connection, creating if needed."""
    global _connection
    if _connection is None:
        try:
            # check_same_thread=False is needed because handlers run in different threads.
            # We use db_lock for protecting access across threads.
            # isolation_level=None means autocommit mode, simplifying single statements.
            # Use explicit BEGIN/COMMIT/ROLLBACK for multi-statement transactions.
            _connection = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=15, isolation_level=None)
            _connection.row_factory = sqlite3.Row # Access columns by name (e.g., row['username'])
            # Enable Write-Ahead Logging for better concurrency
            _connection.execute("PRAGMA journal_mode=WAL;")
            # Enforce foreign key constraints
            _connection.execute("PRAGMA foreign_keys = ON;")
            # Wait up to 10 seconds if the database is locked by another connection
            _connection.execute("PRAGMA busy_timeout = 10000;")
            log.info(f"Database connection established: {DB_PATH}")
        except sqlite3.Error as e:
            log.critical(f"CRITICAL: Database connection failed: {e}", exc_info=True)
            raise # Fail fast if DB connection fails
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
            dedicated_userbots TEXT, -- Comma-separated phone numbers initially assigned (Can normalize later if needed)
            -- Aggregated stats - consider if per-task stats are sufficient
            forwards_count INTEGER DEFAULT 0 NOT NULL,
            groups_reached INTEGER DEFAULT 0 NOT NULL,
            total_messages_sent INTEGER DEFAULT 0 NOT NULL,
            language TEXT DEFAULT 'en' NOT NULL
        );

        CREATE TABLE IF NOT EXISTS userbots (
            phone_number TEXT PRIMARY KEY NOT NULL, -- Using phone as PK simplifies lookups
            session_file TEXT NOT NULL UNIQUE, -- Path relative to SESSION_DIR? Or store full path? Store relative.
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
            group_id INTEGER NOT NULL, -- Telegram's group/channel ID (can be negative)
            group_name TEXT, -- Store for display purposes
            group_link TEXT, -- Store original link if available, for reference
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
            last_error TEXT, -- Store last error related to this specific task run
            messages_sent_count INTEGER DEFAULT 0 NOT NULL, -- Track messages sent by this task specifically
            -- Composite primary key
            PRIMARY KEY (client_id, userbot_phone),
            FOREIGN KEY (client_id) REFERENCES clients(user_id) ON DELETE CASCADE, -- If client deleted, remove their tasks
            FOREIGN KEY (userbot_phone) REFERENCES userbots(phone_number) ON DELETE CASCADE, -- If bot deleted, remove its tasks
            -- If folder deleted, set folder_id to NULL (task should ideally become inactive or error)
            FOREIGN KEY (folder_id) REFERENCES folders(id) ON DELETE SET NULL
        );

        -- Create Indexes for faster lookups on frequently queried columns
        CREATE INDEX IF NOT EXISTS idx_clients_user_id ON clients (user_id);
        CREATE INDEX IF NOT EXISTS idx_userbots_assigned_client ON userbots (assigned_client);
        CREATE INDEX IF NOT EXISTS idx_userbots_status ON userbots (status); -- For finding available bots
        CREATE INDEX IF NOT EXISTS idx_folders_created_by ON folders (created_by);
        CREATE INDEX IF NOT EXISTS idx_target_groups_folder_id ON target_groups (folder_id);
        CREATE INDEX IF NOT EXISTS idx_target_groups_added_by ON target_groups (added_by);
        CREATE INDEX IF NOT EXISTS idx_target_groups_group_id ON target_groups (group_id);
        CREATE INDEX IF NOT EXISTS idx_logs_timestamp ON logs (timestamp DESC); -- For fetching recent logs
        -- Index to efficiently find active tasks due to run
        CREATE INDEX IF NOT EXISTS idx_userbot_settings_active ON userbot_settings (status, start_time, last_run) WHERE status = 'active';
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
        try:
            _connection.close()
            _connection = None
            log.info("Database connection closed.")
        except sqlite3.Error as e:
            log.error(f"Error closing database: {e}")

# --- Logging Function (to DB) ---
def log_event_db(event, details="", user_id=None, userbot_phone=None):
    """Logs an event to the 'logs' table in the database."""
    timestamp = int(datetime.now(UTC_TZ).timestamp())
    sql = "INSERT INTO logs (timestamp, event, user_id, userbot_phone, details) VALUES (?, ?, ?, ?, ?)"
    try:
        conn = _get_db_connection()
        with db_lock:
             # Execute directly (autocommit is on)
            conn.execute(sql, (timestamp, event, user_id, userbot_phone, str(details)))
        # Also log to standard logger for immediate visibility/Render logs
        log.debug(f"DBLog-{event}: User={user_id} Bot={userbot_phone} Details={details}")
    except sqlite3.Error as e:
        # Log DB logging failure to standard logger, but don't crash the app
        log.error(f"CRITICAL: Failed to log event to DB: {e} - Event: {event}, Details: {details}")

# --- Helper Function for Transactions ---
def _execute_transaction(cursor, sql_statements):
    """Executes multiple SQL statements within a transaction."""
    try:
        cursor.execute("BEGIN")
        for sql, params in sql_statements:
            cursor.execute(sql, params)
        cursor.execute("COMMIT")
        return True
    except sqlite3.Error as e:
        log.error(f"Transaction failed: {e}. Rolling back.")
        cursor.execute("ROLLBACK")
        return False

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
            # 1. Check if this user_id is already assigned to ANY code
            cursor.execute(sql_check_user, (user_id,))
            existing_user_code = cursor.fetchone()
            if existing_user_code and existing_user_code['invitation_code'] != code:
                log.warning(f"Activation attempt failed: User {user_id} already linked to code {existing_user_code['invitation_code']}")
                return False, "user_already_active" # Key for translation

            # 2. Check the specific code
            cursor.execute(sql_check_code, (code,))
            code_data = cursor.fetchone()
            if not code_data:
                return False, "code_not_found"
            if code_data['user_id'] is not None:
                if code_data['user_id'] == user_id:
                     return True, "already_active" # Already activated with this code
                else:
                     return False, "code_already_used" # Used by someone else

            # 3. Check expiration
            now_ts = int(datetime.now(UTC_TZ).timestamp())
            if code_data['subscription_end'] < now_ts:
                return False, "activation_expired"

            # 4. Attempt update (Atomic check via WHERE user_id IS NULL)
            cursor.execute(sql_update, (user_id, code))
            updated_rows = cursor.rowcount
            if updated_rows > 0:
                log.info(f"Client activated: Code={code}, UserID={user_id}")
                return True, "activation_success"
            else:
                # Should not happen if previous checks passed, but indicates a potential race condition or logic flaw
                log.error(f"Activation update failed unexpectedly for Code={code}, UserID={user_id}.")
                return False, "activation_error" # Generic activation error
    except sqlite3.Error as e:
        log.error(f"DB Error activating client code {code} for user {user_id}: {e}", exc_info=True)
        return False, "activation_db_error"

def get_user_language(user_id):
    """Gets the user's preferred language code, defaults to 'en'."""
    client = find_client_by_user_id(user_id)
    # Ensure language is never None, fallback to 'en'
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
                log_event_db("Language Set", f"Lang: {lang}", user_id=user_id)
                return True
            else:
                # User might not exist, although called from authorized context usually
                log.warning(f"Attempted to set language for non-existent user_id {user_id}")
                return False
    except sqlite3.Error as e:
        log.error(f"DB Error setting language for user {user_id}: {e}")
        return False

def create_invitation(code, sub_end_ts):
    """Creates a new client invitation code record (userbots assigned later)."""
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
        SELECT c.user_id, c.invitation_code, c.subscription_end, c.dedicated_userbots,
               COUNT(DISTINCT u.phone_number) as bot_count
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

def get_all_userbots(assigned_status=None):
    """Fetches all userbots, optionally filtering by assigned status."""
    # assigned_status can be True (assigned), False (unassigned), None (all)
    sql = "SELECT u.*, c.user_id as client_user_id FROM userbots u LEFT JOIN clients c ON u.assigned_client = c.invitation_code"
    params = []
    if assigned_status is True:
        sql += " WHERE u.assigned_client IS NOT NULL"
    elif assigned_status is False:
        sql += " WHERE u.assigned_client IS NULL"
    sql += " ORDER BY u.assigned_client, u.phone_number"
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
    # Find bots assigned to THIS client's code
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
    """
    try:
        conn = _get_db_connection()
        with db_lock:
            conn.execute(sql, (phone, session_file_rel, status, api_id, api_hash, username, assigned_client, last_error))
        log.info(f"Userbot {phone} added/updated. Status: {status}")
        return True
    except sqlite3.Error as e:
        log.error(f"DB Error adding/updating userbot {phone}: {e}")
        return False

def update_userbot_status(phone, status, username=None, last_error=None):
    """Updates the status, optionally username and last_error for a userbot."""
    # Use COALESCE to update username only if provided, keep existing otherwise
    sql = """
        UPDATE userbots
        SET status = ?,
            username = COALESCE(?, username),
            last_error = ?
        WHERE phone_number = ?
    """
    try:
        conn = _get_db_connection()
        with db_lock:
            cursor = conn.cursor()
            cursor.execute(sql, (status, username, last_error, phone))
        # Avoid logging excessively frequent status changes like 'connecting'
        if status not in ['connecting', 'initializing']:
            log.info(f"Updated status for userbot {phone} to {status}. Error: {last_error}")
        else:
            log.debug(f"Userbot {phone} status set to {status}.")
        return cursor.rowcount > 0
    except sqlite3.Error as e:
        log.error(f"DB Error updating status for userbot {phone}: {e}")
        return False

def assign_userbots_to_client(code, phones_to_assign):
    """Assigns a list of userbots to a client's invitation code."""
    sql_check_client = "SELECT invitation_code FROM clients WHERE invitation_code = ?"
    # Ensure bot exists and is unassigned before assigning
    sql_update_bot = "UPDATE userbots SET assigned_client = ? WHERE phone_number = ? AND assigned_client IS NULL"
    # Update client's dedicated_userbots (This field becomes redundant if we query assignments directly)
    sql_get_current_client_bots = "SELECT dedicated_userbots FROM clients WHERE invitation_code = ?"
    sql_update_client_bots_list = "UPDATE clients SET dedicated_userbots = ? WHERE invitation_code = ?"

    updated_count = 0
    try:
        conn = _get_db_connection()
        with db_lock:
            cursor = conn.cursor()
            # Check if client code is valid
            cursor.execute(sql_check_client, (code,))
            if not cursor.fetchone():
                 log.error(f"Cannot assign bots: Client code '{code}' not found.")
                 return False, "Client code not found."

            # Use transaction
            cursor.execute("BEGIN")
            actually_assigned_phones = []
            for phone in phones_to_assign:
                 cursor.execute(sql_update_bot, (code, phone))
                 if cursor.rowcount > 0:
                     actually_assigned_phones.append(phone)
                     updated_count += 1
                 else:
                     # Check why update failed (already assigned elsewhere or bot doesn't exist?)
                     cursor.execute("SELECT assigned_client FROM userbots WHERE phone_number = ?", (phone,))
                     bot_info = cursor.fetchone()
                     if bot_info and bot_info['assigned_client'] is not None:
                          log.warning(f"Could not assign bot {phone} to {code}: Already assigned to {bot_info['assigned_client']}")
                     else:
                          log.warning(f"Could not assign bot {phone} to {code}: Bot not found or error.")


            # Update the redundant list in clients table (optional, consider removing this field)
            if updated_count > 0:
                 cursor.execute(sql_get_current_client_bots, (code,))
                 current_list_str = cursor.fetchone()['dedicated_userbots']
                 current_list = current_list_str.split(',') if current_list_str else []
                 new_full_list = sorted(list(set(current_list + actually_assigned_phones)))
                 cursor.execute(sql_update_client_bots_list, (",".join(new_full_list), code))

            cursor.execute("COMMIT")

            log.info(f"Assigned {updated_count}/{len(phones_to_assign)} bots to client {code}.")
            if updated_count != len(phones_to_assign):
                log.warning(f"Some bots requested for assignment to {code} were unavailable or already assigned.")
            return True, f"Successfully assigned {updated_count} userbots."
    except sqlite3.Error as e:
        log.error(f"DB Error assigning bots to client {code}: {e}", exc_info=True)
        with db_lock:
             if conn.in_transaction: conn.execute("ROLLBACK")
        return False, "Database error during assignment."


def remove_userbot(phone):
    """Removes a userbot record from the database."""
    sql = "DELETE FROM userbots WHERE phone_number = ?"
    session_file_rel = f"{phone}.session" # Assuming standard naming
    session_path = os.path.join(SESSION_DIR, session_file_rel)
    try:
        conn = _get_db_connection()
        with db_lock:
            cursor = conn.cursor()
            # Use transaction? Not strictly needed for single delete but good practice
            cursor.execute("BEGIN")
            cursor.execute(sql, (phone,))
            deleted_rows = cursor.rowcount
            cursor.execute("COMMIT")

        if deleted_rows > 0:
            log.info(f"Removed userbot {phone} from database.")
            # Attempt to remove session file after successful DB deletion
            try:
                if os.path.exists(session_path):
                    os.remove(session_path)
                    log.info(f"Removed session file {session_path}")
                # Check for journal file too
                journal_path = f"{session_path}-journal"
                if os.path.exists(journal_path):
                    os.remove(journal_path)
            except OSError as e:
                log.error(f"Error removing session file(s) for removed userbot {phone}: {e}")
            log_event_db("Userbot Removed", f"Phone: {phone}", userbot_phone=phone)
            return True
        else:
            log.warning(f"Attempted to remove non-existent userbot {phone}.")
            return False
    except sqlite3.Error as e:
        log.error(f"DB Error removing userbot {phone}: {e}", exc_info=True)
        # Rollback might not be needed with autocommit, but check 'in_transaction' if using explicit txns
        return False

def get_unassigned_userbots(limit):
    """Gets a list of phone numbers for active, unassigned userbots."""
    sql = "SELECT phone_number FROM userbots WHERE assigned_client IS NULL AND status = 'active' LIMIT ?"
    try:
        conn = _get_db_connection()
        with db_lock:
            cursor = conn.cursor()
            cursor.execute(sql, (limit,))
            return [row['phone_number'] for row in cursor.fetchall()]
    except sqlite3.Error as e:
        log.error(f"DB Error fetching unassigned userbots: {e}")
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
            log_event_db("Folder Created", f"Name: {name}, ID: {folder_id}", user_id=user_id)
            return folder_id
    except sqlite3.IntegrityError: # Handles UNIQUE constraint violation
         log.warning(f"Folder '{name}' already exists for user {user_id}.")
         return None # Indicate duplicate
    except sqlite3.Error as e:
        log.error(f"DB Error adding folder '{name}' for user {user_id}: {e}")
        return None # Indicate error

def get_folders_by_user(user_id):
    """Retrieves all folders created by a specific user."""
    sql = "SELECT id, name FROM folders WHERE created_by = ?"
    try:
        conn = _get_db_connection()
        with db_lock:
            cursor = conn.cursor()
            cursor.execute(sql, (user_id,))
            return cursor.fetchall()
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
        with db_lock:
             cursor = conn.cursor()
             # Use transaction for safety, although cascade handles groups
             cursor.execute("BEGIN")
             cursor.execute(sql, (folder_id, user_id))
             deleted_rows = cursor.rowcount
             cursor.execute("COMMIT") # Commit deletion

             if deleted_rows > 0:
                 log.info(f"Deleted folder ID {folder_id} for user {user_id}.")
                 log_event_db("Folder Deleted", f"Folder ID: {folder_id}", user_id=user_id)
                 return True
             else:
                 log.warning(f"Attempt to delete non-existent or unauthorized folder ID {folder_id} by user {user_id}.")
                 return False
    except sqlite3.Error as e:
        log.error(f"DB Error deleting folder ID {folder_id} for user {user_id}: {e}")
        with db_lock:
            if conn.in_transaction: conn.execute("ROLLBACK") # Check if transaction is active before rollback
        return False

# --- Target Group Functions ---
def add_target_group(group_id, group_name, group_link, user_id, folder_id):
    """Adds a target group to a user's folder, ignoring duplicates."""
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
            return cursor.rowcount > 0 # Returns True if inserted, False if conflict/ignored
    except sqlite3.Error as e:
        # Could be FK constraint failure if folder_id or user_id is invalid
        log.error(f"DB Error adding target group {group_id} to folder {folder_id} for user {user_id}: {e}")
        return False

def get_target_groups_by_folder(folder_id):
    """Gets a list of group IDs belonging to a specific folder."""
    sql = "SELECT group_id FROM target_groups WHERE folder_id = ?"
    try:
        conn = _get_db_connection()
        with db_lock:
            cursor = conn.cursor()
            cursor.execute(sql, (folder_id,))
            # Fetch all group IDs for the folder
            return [row['group_id'] for row in cursor.fetchall()]
    except sqlite3.Error as e:
        log.error(f"DB Error fetching group IDs for folder {folder_id}: {e}")
        return []

def get_target_groups_details_by_folder(folder_id):
     """Gets details (id, name, link) of groups in a folder."""
     sql = "SELECT id, group_id, group_name, group_link FROM target_groups WHERE folder_id = ?"
     try:
         conn = _get_db_connection()
         with db_lock:
             cursor = conn.cursor()
             cursor.execute(sql, (folder_id,))
             return cursor.fetchall()
     except sqlite3.Error as e:
         log.error(f"DB Error fetching group details for folder {folder_id}: {e}")
         return []

def remove_target_groups_by_db_id(db_ids_to_remove, user_id):
    """Removes target groups using their database primary key IDs."""
    if not db_ids_to_remove:
        return 0
    # Ensure IDs are integers
    safe_ids = tuple(int(id_) for id_ in db_ids_to_remove)
    placeholders = ','.join('?' * len(safe_ids))
    # Added check for added_by to prevent users deleting others' entries
    sql = f"DELETE FROM target_groups WHERE id IN ({placeholders}) AND added_by = ?"
    params = safe_ids + (user_id,)
    try:
        conn = _get_db_connection()
        with db_lock:
             cursor = conn.cursor()
             cursor.execute("BEGIN") # Use transaction for bulk delete
             cursor.execute(sql, params)
             deleted_count = cursor.rowcount
             cursor.execute("COMMIT")
             if deleted_count > 0:
                 log.info(f"User {user_id} removed {deleted_count} target groups by DB ID.")
                 log_event_db("Groups Removed", f"Count: {deleted_count}, IDs: {safe_ids}", user_id=user_id)
             return deleted_count
    except sqlite3.Error as e:
        log.error(f"DB Error removing target groups by DB ID for user {user_id}: {e}")
        with db_lock:
            if conn.in_transaction: conn.execute("ROLLBACK")
        return -1 # Indicate error

def remove_all_target_groups_from_folder(folder_id, user_id):
    """Removes all target groups associated with a specific folder for a user."""
    sql = "DELETE FROM target_groups WHERE folder_id = ? AND added_by = ?"
    try:
        conn = _get_db_connection()
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
            return cursor.fetchone()
    except sqlite3.Error as e:
        log.error(f"DB Error getting task settings for user {client_id}, bot {userbot_phone}: {e}")
        return None

def save_userbot_task_settings(client_id, userbot_phone, settings):
    """Saves or updates task settings for a userbot. Input 'settings' is a dictionary."""
    # Ensure required fields are present with defaults if necessary
    params = {
        'client_id': client_id,
        'userbot_phone': userbot_phone,
        'message_link': settings.get('message_link'),
        'fallback_message_link': settings.get('fallback_message_link'),
        'start_time': settings.get('start_time'), # Store as Unix TS UTC
        'repetition_interval': settings.get('repetition_interval'), # Store as minutes
        'status': settings.get('status', 'inactive'), # Default to inactive
        'folder_id': settings.get('folder_id'), # Can be NULL
        'send_to_all_groups': settings.get('send_to_all_groups', 0), # Default to 0 (False)
        'last_run': settings.get('last_run'), # Usually managed internally, not by user directly
        'last_error': settings.get('last_error'), # Usually managed internally
        'messages_sent_count': settings.get('messages_sent_count', 0) # Allow direct set? Or only increment? Increment usually better.
    }

    # Use named placeholders for clarity with ON CONFLICT
    sql = """
        INSERT INTO userbot_settings (
            client_id, userbot_phone, message_link, fallback_message_link, start_time,
            repetition_interval, status, folder_id, send_to_all_groups, last_run, last_error, messages_sent_count
        ) VALUES (:client_id, :userbot_phone, :message_link, :fallback_message_link, :start_time,
                  :repetition_interval, :status, :folder_id, :send_to_all_groups, :last_run, :last_error, :messages_sent_count)
        ON CONFLICT(client_id, userbot_phone) DO UPDATE SET
            message_link=excluded.message_link,
            fallback_message_link=excluded.fallback_message_link,
            start_time=excluded.start_time,
            repetition_interval=excluded.repetition_interval,
            status=excluded.status,
            folder_id=excluded.folder_id,
            send_to_all_groups=excluded.send_to_all_groups,
            last_run=excluded.last_run, -- Let update overwrite last_run? Careful.
            last_error=excluded.last_error, -- Usually clear error on save
            messages_sent_count=excluded.messages_sent_count -- Allow reset/set
    """
    try:
        conn = _get_db_connection()
        with db_lock:
            conn.execute(sql, params)
        log.info(f"Saved task settings for user {client_id}, bot {userbot_phone}. Status: {params['status']}")
        return True
    except sqlite3.Error as e:
        log.error(f"DB Error saving task settings for user {client_id}, bot {userbot_phone}: {e}")
        return False

def get_active_tasks_to_run(current_time_ts):
    """Finds active, due tasks for active userbots with active client subscriptions."""
    sql = """
        SELECT s.*, u.session_file, u.api_id, u.api_hash
        FROM userbot_settings s
        JOIN userbots u ON s.userbot_phone = u.phone_number
        JOIN clients c ON s.client_id = c.user_id
        WHERE s.status = 'active'                          -- Task must be active
          AND u.status = 'active'                          -- Userbot must be active
          AND c.subscription_end > ?                   -- Client subscription must be valid
          AND s.start_time IS NOT NULL AND s.start_time <= ? -- Task start time must be in the past
          AND s.repetition_interval IS NOT NULL AND s.repetition_interval > 0 -- Must have valid interval
          AND s.message_link IS NOT NULL                   -- Must have a message to send
          -- Check if due based on last run and interval
          AND (s.last_run IS NULL OR (s.last_run + (s.repetition_interval * 60)) <= ?)
          -- Check if folder exists or sending to all groups (avoids tasks with deleted folders and send_to_all=0)
          -- AND (s.send_to_all_groups = 1 OR s.folder_id IS NULL OR EXISTS (SELECT 1 FROM folders WHERE id = s.folder_id)) -- Subquery might be slow, handled in task runner instead
    """
    try:
        conn = _get_db_connection()
        with db_lock:
            cursor = conn.cursor()
            # current_time_ts is used 3 times in the query
            cursor.execute(sql, (current_time_ts, current_time_ts, current_time_ts))
            return cursor.fetchall()
    except sqlite3.Error as e:
        log.error(f"DB Error fetching active tasks: {e}", exc_info=True)
        return []

def update_task_after_run(client_id, userbot_phone, run_time_ts, messages_sent_increment=0, error=None):
    """Updates task's last_run time, optionally clears/sets error, and increments message count."""
    sql = """
        UPDATE userbot_settings
        SET last_run = ?,
            last_error = ?, -- Clears error if error=None
            messages_sent_count = messages_sent_count + ?
        WHERE client_id = ? AND userbot_phone = ?
    """
    # Also update aggregate client stats? Could become inconsistent. Maybe calculate stats on demand?
    sql_client_update = """
        UPDATE clients
        SET total_messages_sent = total_messages_sent + ?,
            forwards_count = forwards_count + 1 -- Count each run as one 'forward operation'
        WHERE user_id = ?
    """
    try:
        conn = _get_db_connection()
        with db_lock:
            cursor = conn.cursor()
            cursor.execute("BEGIN") # Transaction for multiple updates
            cursor.execute(sql, (run_time_ts, error, messages_sent_increment, client_id, userbot_phone))
            # Update aggregate stats only if successful forwards occurred
            if messages_sent_increment > 0:
                cursor.execute(sql_client_update, (messages_sent_increment, client_id))
            cursor.execute("COMMIT")
        log.debug(f"Updated task after run for client {client_id}, bot {userbot_phone}. Sent: {messages_sent_increment}, Error: {error}")
        return True
    except sqlite3.Error as e:
        log.error(f"DB Error updating task after run for user {client_id}, bot {userbot_phone}: {e}")
        with db_lock:
             if conn.in_transaction: conn.execute("ROLLBACK")
        return False

# --- Logs ---
def get_recent_logs(limit=25):
    """Retrieves the most recent log entries from the database."""
    # Ordering by ID DESC assumes IDs are sequential, timestamp is safer but potentially slower if not indexed well
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

def get_client_stats(user_id):
    """Retrieves aggregate statistics for a specific client."""
    # This reads from the aggregated columns, alternative is calculating on the fly from tasks/logs
    client = find_client_by_user_id(user_id)
    if client:
        return {
            "total_messages_sent": client['total_messages_sent'],
            "groups_reached": client['groups_reached'], # Note: Clarity of 'groups_reached' might be low
            "forwards_count": client['forwards_count']
        }
    return None

# Initialize DB on import (creates connection and runs schema checks/creation)
init_db()
log.info("Database module initialized.")