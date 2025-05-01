# config.py
import os
import logging
import logging.handlers # For RotatingFileHandler
import types
import sys
import filetype
import pytz
from dotenv import load_dotenv

# Load .env file if it exists (for local development)
load_dotenv()

# --- Constants ---
CLIENT_TIMEOUT = 30  # Telethon client timeout in seconds
CHECK_TASKS_INTERVAL = 60  # How often to check for due tasks, in seconds
DB_FILENAME = 'telegram_bot.db'
SESSION_SUBDIR = 'sessions'
LOG_FILENAME = 'bot.log'
MAX_LOG_BYTES = 10 * 1024 * 1024  # Max log file size (e.g., 10MB)
LOG_BACKUP_COUNT = 5  # Number of old log files to keep

# --- Compatibility Shim ---
# Fake 'imghdr' module for Python 3.11+ compatibility where imghdr is deprecated
try:
    import imghdr
    log_imghdr = logging.getLogger('imghdr_compat')
    log_imghdr.debug("Using system 'imghdr' module.")
except ImportError:
    log_imghdr = logging.getLogger('imghdr_compat')
    log_imghdr.debug("System 'imghdr' not found, creating compatibility shim.")
    imghdr_module = types.ModuleType('imghdr')
    def what(file, h=None):
        """Basic file type check using 'filetype' as a replacement for imghdr.what."""
        try:
            # Read a small chunk for type detection
            if hasattr(file, 'read'):
                start_pos = file.tell() # Remember position
                buf = file.read(32)
                file.seek(start_pos) # Reset position
            elif isinstance(file, str):
                if not os.path.exists(file): return None
                with open(file, 'rb') as f:
                    buf = f.read(32)
            elif isinstance(file, bytes):
                 buf = file[:32]
            else:
                return None # Unsupported type

            if not buf: return None

            # Use filetype library
            kind = filetype.guess(buf)
            return kind.extension if kind else None # Return extension like imghdr did
        except Exception as e:
             log_imghdr.warning(f"Error during 'what' compatibility check: {e}")
             return None
    imghdr_module.what = what
    sys.modules['imghdr'] = imghdr_module
    log_imghdr.info("Using 'filetype' based compatibility shim for 'imghdr'.")


# --- Data Directory Setup ---
# Use RENDER_DISK_PATH (persistent disk on Render) if set, otherwise DATA_DIR, otherwise default './data'
DATA_DIR_BASE = os.environ.get('RENDER_DISK_PATH', os.environ.get('DATA_DIR', './data'))
DATA_DIR = os.path.abspath(DATA_DIR_BASE)
DB_PATH = os.path.join(DATA_DIR, DB_FILENAME)
SESSION_DIR = os.path.join(DATA_DIR, SESSION_SUBDIR)


# --- Environment Variable Loading & Validation ---
# Defined early so logging setup can use it if needed
def load_env_var(name, required=True, cast=str, default=None):
    """Loads an environment variable, raises ValueError if required and missing."""
    value = os.environ.get(name)
    if value is None:
        if required and default is None:
            raise ValueError(f"Required environment variable '{name}' is not set.")
        value = default # Use default if not required or default is provided
    elif cast: # Cast only if value is not None
        try:
            return cast(value)
        except ValueError as e:
            raise ValueError(f"Environment variable '{name}' ('{value}') has invalid type for {cast.__name__}: {e}") from e
    return value # Return value as is (can be None if not required and no default)

try:
    API_ID = load_env_var('API_ID', required=True, cast=int)
    API_HASH = load_env_var('API_HASH', required=True, cast=str)
    BOT_TOKEN = load_env_var('BOT_TOKEN', required=True, cast=str)
    admin_ids_str = load_env_var('ADMIN_IDS', required=False, cast=str, default='')
    # Ensure IDs are integers and handle potential whitespace
    ADMIN_IDS = [int(id_.strip()) for id_ in admin_ids_str.split(',') if id_.strip()]
except ValueError as e:
    # Use basic print here as logging might not be fully configured yet
    print(f"CRITICAL: Configuration Error loading environment variables - {e}")
    sys.exit(1)

# --- Logging Setup ---
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - [%(name)s:%(lineno)d] - %(message)s')
log_file_path = os.path.join(DATA_DIR, LOG_FILENAME)

# File Handler (optional but recommended for persistence beyond Render's console log limits)
file_handler = None
try:
    # Use RotatingFileHandler to prevent logs from growing indefinitely
    file_handler = logging.handlers.RotatingFileHandler(
        log_file_path, maxBytes=MAX_LOG_BYTES, backupCount=LOG_BACKUP_COUNT, encoding='utf-8'
    )
    file_handler.setFormatter(log_formatter)
    file_handler.setLevel(logging.INFO) # Log INFO level and above to file
except PermissionError:
     # Logging not set up yet, use print
     print(f"Warning: Permission denied writing log file to {log_file_path}. File logging disabled.")
     file_handler = None
except Exception as e:
    print(f"Warning: Could not set up file logging to {log_file_path}: {e}")
    file_handler = None

# Stream Handler (for console output, captured by Render)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(log_formatter)
stream_handler.setLevel(logging.DEBUG) # Log DEBUG level and above to console/Render

# Configure root logger
handlers_list = [stream_handler]
if file_handler:
    handlers_list.append(file_handler)

logging.basicConfig(
    level=logging.DEBUG, # Set root logger level to lowest (DEBUG) to allow handlers to filter
    handlers=handlers_list
)
log = logging.getLogger(__name__) # Use this logger in other modules: from config import log

# --- Ensure Data Directories Exist (AFTER Logging is setup) ---
try:
    # DATA_DIR (/data on Render) must already exist as the mount point provided by Render.
    if not os.path.isdir(DATA_DIR):
        log.critical(f"CRITICAL: Data directory '{DATA_DIR}' does not exist or is not a directory! Check Render disk mount and RENDER_DISK_PATH env var.")
        sys.exit(1)

    # Explicitly create the 'sessions' subdirectory if it doesn't exist.
    if not os.path.exists(SESSION_DIR):
        log.info(f"Session directory '{SESSION_DIR}' not found. Attempting to create...")
        os.makedirs(SESSION_DIR) # This was the line failing before
        log.info(f"Created session directory: {SESSION_DIR}")
    elif not os.path.isdir(SESSION_DIR):
         # Handle case where SESSION_DIR exists but is a file
         log.critical(f"CRITICAL: Path '{SESSION_DIR}' exists but is not a directory!")
         sys.exit(1)
    else:
        log.info(f"Session directory '{SESSION_DIR}' already exists.")

    # Optional: Check writability - maybe less critical if makedirs worked
    if not os.access(SESSION_DIR, os.W_OK):
         log.warning(f"Session directory '{SESSION_DIR}' might not be writable! Session file creation may fail.")
    if not os.access(DATA_DIR, os.W_OK):
         log.warning(f"Data directory '{DATA_DIR}' might not be writable! Database file creation/write may fail.")

except OSError as e:
    # If os.makedirs(SESSION_DIR) fails with Permission Denied here, it's definitely a disk permission issue.
    log.critical(f"CRITICAL: OSError ensuring directory '{SESSION_DIR}' exists/writable: {e}", exc_info=True)
    sys.exit(1)
except Exception as e:
    # Catch any other unexpected errors during setup
    log.critical(f"CRITICAL: Unexpected error setting up directories: {e}", exc_info=True)
    sys.exit(1)

# Log basic config info AFTER directory setup and logging is confirmed working
log.info("--- Bot Starting ---")
log.info(f"Data Directory: {DATA_DIR}")
log.info(f"Session Directory: {SESSION_DIR}")
log.info(f"Database Path: {DB_PATH}")
log.info(f"Log File Path: {log_file_path if file_handler else 'Disabled'}")
log.info(f"Admin IDs: {ADMIN_IDS}")


# --- Timezones ---
try:
    LITHUANIA_TZ = pytz.timezone('Europe/Vilnius')
    UTC_TZ = pytz.utc
except pytz.UnknownTimeZoneError as e:
    log.critical(f"CRITICAL: Unknown timezone specified: {e}")
    sys.exit(1)

# --- Conversation States ---
# Using string constants for states can be clearer for debugging
(
    STATE_WAITING_FOR_CODE, STATE_WAITING_FOR_PHONE, STATE_WAITING_FOR_API_ID,
    STATE_WAITING_FOR_API_HASH, STATE_WAITING_FOR_CODE_USERBOT,
    STATE_WAITING_FOR_PASSWORD, STATE_WAITING_FOR_SUB_DETAILS,
    STATE_WAITING_FOR_GROUP_URLS, # Possibly deprecated by folder system
    STATE_WAITING_FOR_MESSAGE_LINK, # Possibly split into primary/fallback?
    STATE_WAITING_FOR_START_TIME,
    STATE_WAITING_FOR_FOLDER_CHOICE, STATE_WAITING_FOR_FOLDER_NAME,
    STATE_WAITING_FOR_FOLDER_SELECTION, STATE_TASK_SETUP,
    STATE_WAITING_FOR_LANGUAGE, STATE_WAITING_FOR_EXTEND_CODE,
    STATE_WAITING_FOR_EXTEND_DAYS, STATE_WAITING_FOR_ADD_USERBOTS_CODE,
    STATE_WAITING_FOR_ADD_USERBOTS_COUNT, STATE_SELECT_TARGET_GROUPS,
    STATE_WAITING_FOR_USERBOT_SELECTION, STATE_WAITING_FOR_GROUP_LINKS, # Used by join & folder edit
    STATE_WAITING_FOR_FOLDER_ACTION, STATE_WAITING_FOR_PRIMARY_MESSAGE_LINK,
    STATE_WAITING_FOR_FALLBACK_MESSAGE_LINK,
    STATE_FOLDER_EDIT_REMOVE_SELECT, # State for selecting groups to remove from folder
    STATE_FOLDER_RENAME_PROMPT, # State waiting for new folder name
    STATE_ADMIN_CONFIRM_USERBOT_RESET # State for confirming userbot reset
) = map(str, range(28)) # Ensure this range covers all defined states

# --- Callback Data Prefixes ---
# Using prefixes helps route callbacks efficiently in a single handler function
CALLBACK_ADMIN_PREFIX = "admin_"
CALLBACK_CLIENT_PREFIX = "client_"
CALLBACK_TASK_PREFIX = "task_"
CALLBACK_FOLDER_PREFIX = "folder_"
CALLBACK_JOIN_PREFIX = "join_"
CALLBACK_LANG_PREFIX = "lang_"
CALLBACK_REMOVE_PREFIX = "remove_"
CALLBACK_INTERVAL_PREFIX = "interval_"
CALLBACK_GENERIC_PREFIX = "generic_" # For simple actions like back buttons, confirmation


# --- Utility Functions ---
def is_admin(user_id):
    """Checks if a given user ID is in the ADMIN_IDS list."""
    return user_id in ADMIN_IDS

log.info("Configuration loaded.")
