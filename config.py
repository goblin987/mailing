# --- START OF FILE config.py ---

# config.py
import os
import logging
import logging.handlers # For RotatingFileHandler
import types
import sys
import filetype # Ensure this is imported
import pytz
from dotenv import load_dotenv

# Load .env file if it exists (for local development)
load_dotenv()

# --- Constants ---
CLIENT_TIMEOUT = 30  # Telethon client timeout in seconds
CHECK_TASKS_INTERVAL = 60  # How often to check for due tasks, in seconds (1 minute)
DB_FILENAME = 'telegram_bot.db'
SESSION_SUBDIR = 'sessions'
LOG_FILENAME = 'bot.log'
MAX_LOG_BYTES = 10 * 1024 * 1024  # Max log file size (10MB)
LOG_BACKUP_COUNT = 5  # Number of old log files to keep
ITEMS_PER_PAGE = 5 # For pagination in lists

# Note: The 'groups_reached' stat in the DB might be unreliable/deprecated.

# --- Compatibility Shim for imghdr ---
_imghdr_compat_logger = None # Will be assigned after logging is fully set up

if sys.version_info >= (3, 11): # If Python 3.11 or newer
    # Forcibly use the filetype-based shim
    print("INFO: Python 3.11+ detected, using 'filetype' based compatibility shim for 'imghdr'.")
    imghdr_module = types.ModuleType('imghdr')
    def what_shim(file, h=None): # Renamed to avoid conflict if system imghdr is later imported
        """Basic file type check using 'filetype' as a replacement for imghdr.what."""
        global _imghdr_compat_logger
        try:
            buf = None
            if hasattr(file, 'read'):
                start_pos = file.tell()
                buf = file.read(261)
                file.seek(start_pos)
            elif isinstance(file, str):
                if not os.path.exists(file): return None
                with open(file, 'rb') as f:
                    buf = f.read(261)
            elif isinstance(file, bytes):
                 buf = file[:261]
            else:
                return None

            if not buf: return None
            kind = filetype.guess(buf)
            if kind and kind.mime.startswith('image/'):
                 # Return common extensions recognized by older libraries
                 ext = kind.extension.lower()
                 if ext == 'jpg': return 'jpeg'
                 if ext in ['png', 'gif', 'bmp', 'tiff', 'webp']: return ext
                 return None # If not a common type imghdr might recognize
            return None
        except Exception as e:
             if _imghdr_compat_logger: # Use logger only if it has been initialized
                  _imghdr_compat_logger.warning(f"Error during 'what' compatibility check: {e}")
             else:
                  print(f"WARNING: Error during 'what' compatibility check: {e}", file=sys.stderr)
             return None
    imghdr_module.what = what_shim
    sys.modules['imghdr'] = imghdr_module
else:
    # For older Python versions, try importing system imghdr first
    try:
        import imghdr
        print("DEBUG: Using system 'imghdr' module.")
    except ImportError:
        print("DEBUG: System 'imghdr' not found (Python < 3.11), creating compatibility shim.")
        imghdr_module = types.ModuleType('imghdr')
        # Duplicating the shim logic here for older Pythons if system imghdr is missing
        def what_shim_old(file, h=None): # Renamed to avoid conflict
            global _imghdr_compat_logger
            try:
                buf = None
                if hasattr(file, 'read'):
                    start_pos = file.tell(); buf = file.read(261); file.seek(start_pos)
                elif isinstance(file, str):
                    if not os.path.exists(file): return None
                    with open(file, 'rb') as f: buf = f.read(261)
                elif isinstance(file, bytes): buf = file[:261]
                else: return None
                if not buf: return None
                kind = filetype.guess(buf)
                if kind and kind.mime.startswith('image/'):
                     ext = kind.extension.lower()
                     if ext == 'jpg': return 'jpeg'
                     if ext in ['png', 'gif', 'bmp', 'tiff', 'webp']: return ext
                     return None
                return None
            except Exception as e:
                 if _imghdr_compat_logger: _imghdr_compat_logger.warning(f"Error during 'what' (old Python shim) check: {e}")
                 else: print(f"WARNING: Error during 'what' (old Python shim) check: {e}", file=sys.stderr)
                 return None
        imghdr_module.what = what_shim_old
        sys.modules['imghdr'] = imghdr_module


# --- Data Directory Setup ---
DATA_DIR_BASE = os.environ.get('RENDER_DISK_PATH', os.environ.get('DATA_DIR', './data'))
DATA_DIR = os.path.abspath(DATA_DIR_BASE)
DB_PATH = os.path.join(DATA_DIR, DB_FILENAME)
SESSION_DIR = os.path.join(DATA_DIR, SESSION_SUBDIR)


# --- Environment Variable Loading & Validation ---
def load_env_var(name, required=True, cast_func=str, default=None):
    """Loads an environment variable, raises ValueError if required and missing."""
    value = os.environ.get(name)
    if value is None:
        if required and default is None:
            raise ValueError(f"CRITICAL: Required environment variable '{name}' is not set.")
        value = default
    else:
        if cast_func:
            try:
                return cast_func(value)
            except ValueError as e:
                raise ValueError(f"Environment variable '{name}' ('{value}') has invalid type for {cast_func.__name__}: {e}") from e
    return value

try:
    API_ID_CLIENT = load_env_var('API_ID', required=True, cast_func=int) # Renamed to avoid conflict if userbot uses different creds
    API_HASH_CLIENT = load_env_var('API_HASH', required=True, cast_func=str) # Renamed
    BOT_TOKEN = load_env_var('BOT_TOKEN', required=True, cast_func=str)
    admin_ids_str = load_env_var('ADMIN_IDS', required=False, cast_func=str, default='')
    ADMIN_IDS = [int(id_.strip()) for id_ in admin_ids_str.split(',') if id_.strip().isdigit()]

    # Added default API ID/Hash for userbots if not provided per-bot (less secure, optional)
    DEFAULT_USERBOT_API_ID = load_env_var('DEFAULT_USERBOT_API_ID', required=False, cast_func=int, default=API_ID_CLIENT)
    DEFAULT_USERBOT_API_HASH = load_env_var('DEFAULT_USERBOT_API_HASH', required=False, cast_func=str, default=API_HASH_CLIENT)


except ValueError as e:
    print(f"CRITICAL: Configuration Error loading environment variables - {e}")
    sys.stderr.write(f"CRITICAL: Configuration Error loading environment variables - {e}\n")
    sys.exit(1)

# --- Logging Setup ---
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - [%(name)s:%(lineno)d] - %(message)s')
log_file_path = os.path.join(DATA_DIR, LOG_FILENAME)

try:
    if not os.path.isdir(DATA_DIR):
        print(f"Data directory '{DATA_DIR}' not found. Attempting to create...")
        try:
             os.makedirs(DATA_DIR, exist_ok=True)
             print(f"Created data directory: {DATA_DIR}")
        except OSError as e:
             print(f"CRITICAL: Failed to create data directory '{DATA_DIR}': {e}. Check permissions and RENDER_DISK_PATH.", file=sys.stderr)
             sys.exit(1)
    if not os.access(DATA_DIR, os.W_OK):
         print(f"WARNING: Data directory '{DATA_DIR}' is not writable! File logging and database operations might fail.", file=sys.stderr)
except Exception as e:
    print(f"CRITICAL: Error during data directory check/creation: {e}", file=sys.stderr)
    sys.exit(1)

file_handler = None
try:
    file_handler = logging.handlers.RotatingFileHandler(
        log_file_path, maxBytes=MAX_LOG_BYTES, backupCount=LOG_BACKUP_COUNT, encoding='utf-8'
    )
    file_handler.setFormatter(log_formatter)
    file_handler.setLevel(logging.INFO)
except PermissionError:
     print(f"Warning: Permission denied writing log file to {log_file_path}. File logging disabled.", file=sys.stderr)
     file_handler = None
except Exception as e:
    print(f"Warning: Could not set up file logging to {log_file_path}: {e}", file=sys.stderr)
    file_handler = None

stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(log_formatter)
log_level_str = os.environ.get('LOG_LEVEL', 'DEBUG').upper()
log_level = getattr(logging, log_level_str, logging.DEBUG)
stream_handler.setLevel(log_level)

handlers_list = [stream_handler]
if file_handler:
    handlers_list.append(file_handler)

logging.basicConfig(
    level=logging.DEBUG,
    handlers=handlers_list,
    format='%(asctime)s - %(levelname)s - [%(name)s:%(lineno)d] - %(message)s'
)
log = logging.getLogger(__name__)
_imghdr_compat_logger = logging.getLogger('imghdr_compat')

log.info(f"Logging configured. Stream level: {log_level_str}. File logging: {'Enabled' if file_handler else 'Disabled'}")
if not ADMIN_IDS and admin_ids_str:
    log.warning(f"ADMIN_IDS provided ('{admin_ids_str}') but contained no valid integer IDs.")
elif not ADMIN_IDS:
     log.warning("ADMIN_IDS environment variable is not set or empty. Admin features will be disabled.")


# --- Ensure Session Subdirectory Exists ---
try:
    if not os.path.isdir(DATA_DIR):
        log.critical(f"CRITICAL: Data directory '{DATA_DIR}' does not exist or is not a directory! Check Render disk mount and RENDER_DISK_PATH env var.")
        sys.exit(1)
    if not os.path.exists(SESSION_DIR):
        log.info(f"Session directory '{SESSION_DIR}' not found. Attempting to create...")
        os.makedirs(SESSION_DIR, exist_ok=True)
        log.info(f"Created session directory: {SESSION_DIR}")
    elif not os.path.isdir(SESSION_DIR):
         log.critical(f"CRITICAL: Path '{SESSION_DIR}' exists but is not a directory!")
         sys.exit(1)
    else: log.info(f"Session directory '{SESSION_DIR}' already exists.")
    if not os.access(SESSION_DIR, os.W_OK): log.warning(f"Session directory '{SESSION_DIR}' might not be writable! Session file creation may fail.")
    if not os.access(DATA_DIR, os.W_OK): log.warning(f"Data directory '{DATA_DIR}' might not be writable! Database file creation/write may fail.")
except OSError as e: log.critical(f"CRITICAL: OSError ensuring directory '{SESSION_DIR}' exists/writable: {e}", exc_info=True); sys.exit(1)
except Exception as e: log.critical(f"CRITICAL: Unexpected error setting up directories: {e}", exc_info=True); sys.exit(1)

log.info("--- Bot Configuration Summary ---")
log.info(f"Data Directory: {DATA_DIR}")
log.info(f"Session Directory: {SESSION_DIR}")
log.info(f"Database Path: {DB_PATH}")
log.info(f"Log File Path: {log_file_path if file_handler else 'Disabled'}")
log.info(f"Admin IDs: {ADMIN_IDS if ADMIN_IDS else 'Not Configured'}")


# --- Timezones ---
try:
    LITHUANIA_TZ = pytz.timezone('Europe/Vilnius')
    UTC_TZ = pytz.utc
    log.info(f"Timezones loaded: LT={LITHUANIA_TZ}, UTC={UTC_TZ}")
except pytz.UnknownTimeZoneError as e:
    log.critical(f"CRITICAL: Unknown timezone specified: {e}")
    sys.exit(1)

# --- Conversation States ---
(
    STATE_WAITING_FOR_COMMAND, STATE_WAITING_FOR_ADMIN_COMMAND,
    STATE_WAITING_FOR_CODE, STATE_WAITING_FOR_PHONE, STATE_WAITING_FOR_API_ID,
    STATE_WAITING_FOR_API_HASH, STATE_WAITING_FOR_CODE_USERBOT,
    STATE_WAITING_FOR_PASSWORD, STATE_WAITING_FOR_SUB_DETAILS,
    STATE_WAITING_FOR_FOLDER_CHOICE,
    STATE_WAITING_FOR_FOLDER_NAME,
    STATE_WAITING_FOR_FOLDER_SELECTION,
    STATE_TASK_SETUP,
    STATE_WAITING_FOR_LANGUAGE,
    STATE_WAITING_FOR_EXTEND_CODE,
    STATE_WAITING_FOR_EXTEND_DAYS, STATE_WAITING_FOR_ADD_USERBOTS_CODE,
    STATE_WAITING_FOR_ADD_USERBOTS_COUNT, STATE_SELECT_TARGET_GROUPS,
    STATE_WAITING_FOR_USERBOT_SELECTION,
    STATE_WAITING_FOR_GROUP_LINKS,
    STATE_WAITING_FOR_FOLDER_ACTION,
    STATE_WAITING_FOR_PRIMARY_MESSAGE_LINK,
    STATE_WAITING_FOR_FALLBACK_MESSAGE_LINK,
    STATE_FOLDER_EDIT_REMOVE_SELECT,
    STATE_FOLDER_RENAME_PROMPT,
    STATE_ADMIN_CONFIRM_USERBOT_RESET,
    STATE_WAITING_FOR_START_TIME,
    STATE_ADMIN_TASK_MESSAGE,
    STATE_ADMIN_TASK_SCHEDULE,
    STATE_ADMIN_TASK_TARGET,
    STATE_WAITING_FOR_TASK_BOT, # Note: CTX_TASK_BOT is used for admin task creation flow, this state might be redundant or for client flow
    STATE_WAITING_FOR_TASK_MESSAGE, # Client flow
    STATE_WAITING_FOR_TASK_SCHEDULE, # Client flow (not used if interval buttons)
    STATE_WAITING_FOR_TASK_TARGET, # Client flow (not used if target type buttons)
    STATE_ADMIN_TASK_CONFIRM # Admin task confirmation (if added)
) = map(str, range(36))  # 36 states total, ensure this covers all unique states

# --- Conversation Context Keys ---
CTX_USER_ID = "_user_id"
CTX_LANG = "_lang"
CTX_PHONE = "phone" # Used in admin userbot add
CTX_API_ID = "api_id" # Used in admin userbot add
CTX_API_HASH = "api_hash" # Used in admin userbot add
CTX_AUTH_DATA = "auth_data" # Used in admin userbot add (Telethon state)
CTX_INVITE_DETAILS = "invite_details" # (Potentially for admin invite creation, not currently used in states)
CTX_EXTEND_CODE = "extend_code" # Admin extend subscription
CTX_ADD_BOTS_CODE = "add_bots_code" # Admin assign bots to client
CTX_FOLDER_ID = "folder_id" # Client folder management
CTX_FOLDER_NAME = "folder_name" # Client folder management
CTX_FOLDER_ACTION = "folder_action" # (Potentially for multi-step folder ops, not currently used)
CTX_SELECTED_BOTS = "selected_bots" # Client join groups/task setup
CTX_TARGET_GROUP_IDS_TO_REMOVE = "target_group_ids_to_remove" # Client folder group removal
CTX_TASK_PHONE = "task_phone" # Client task setup specific bot
CTX_TASK_SETTINGS = "task_settings" # Client task setup temporary settings
CTX_PAGE = "page" # For pagination in general
CTX_MESSAGE_ID = "message_id" # For send_or_edit_message

# Admin Task Creation Context Keys (used by admin_handlers.py)
CTX_TASK_BOT = "task_bot" # Phone of the bot for the admin task
CTX_TASK_MESSAGE = "task_message" # Message/link for the admin task
CTX_TASK_SCHEDULE = "task_schedule" # Cron schedule for the admin task
CTX_TASK_TARGET = "task_target" # Target for the admin task (ID/username)
CTX_TASK_TARGET_TYPE = "task_target_type" # (Not explicitly used by admin_handlers.py shown)
CTX_TASK_TARGET_FOLDER = "task_target_folder" # (Not explicitly used by admin_handlers.py shown)

# --- Callback Data Prefixes ---
CALLBACK_ADMIN_PREFIX = "admin_"
CALLBACK_CLIENT_PREFIX = "client_"
CALLBACK_TASK_PREFIX = "task_" # Used for client-side task setup AND admin task management
CALLBACK_FOLDER_PREFIX = "folder_"
CALLBACK_JOIN_PREFIX = "join_"
CALLBACK_LANG_PREFIX = "lang_"
CALLBACK_INTERVAL_PREFIX = "interval_"
CALLBACK_GENERIC_PREFIX = "generic_" # For things like no-op pagination buttons

# --- Utility Functions (stubs removed, real ones in respective modules) ---
def is_admin(user_id: int) -> bool:
    """Checks if a given user ID is in the ADMIN_IDS list."""
    return isinstance(user_id, int) and ADMIN_IDS and user_id in ADMIN_IDS

log.info("Configuration loaded successfully.")

# --- END OF FILE config.py ---
