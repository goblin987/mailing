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
                 return 'jpeg' if kind.extension == 'jpg' else kind.extension
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
                     return 'jpeg' if kind.extension == 'jpg' else kind.extension
                return None
            except Exception as e:
                 if _imghdr_compat_logger: _imghdr_compat_logger.warning(f"Error during 'what' (old Python shim) check: {e}")
                 else: print(f"WARNING: Error during 'what' (old Python shim) check: {e}", file=sys.stderr)
                 return None
        imghdr_module.what = what_shim_old
        sys.modules['imghdr'] = imghdr_module


# --- Data Directory Setup ---
# Use RENDER_DISK_PATH (persistent disk on Render) if set, otherwise DATA_DIR, otherwise default './data'
# Ensure DATA_DIR_BASE is treated as the root for persistent storage.
DATA_DIR_BASE = os.environ.get('RENDER_DISK_PATH', os.environ.get('DATA_DIR', './data'))
DATA_DIR = os.path.abspath(DATA_DIR_BASE)
DB_PATH = os.path.join(DATA_DIR, DB_FILENAME)
SESSION_DIR = os.path.join(DATA_DIR, SESSION_SUBDIR)


# --- Environment Variable Loading & Validation ---
# Defined early so logging setup can use it if needed
# ** REMOVED log calls from this function **
def load_env_var(name, required=True, cast_func=str, default=None):
    """Loads an environment variable, raises ValueError if required and missing."""
    value = os.environ.get(name)
    if value is None:
        if required and default is None:
            raise ValueError(f"CRITICAL: Required environment variable '{name}' is not set.")
        value = default # Use default if not required or default is provided
        # print(f"DEBUG: Env var '{name}' not set, using default: '{default}'") # Optional: Use print for debugging before logging is up
    else: # Value exists
        # print(f"DEBUG: Env var '{name}' found.") # Optional: Use print for debugging before logging is up
        if cast_func: # Cast only if value exists and cast_func is provided
            try:
                return cast_func(value)
            except ValueError as e:
                raise ValueError(f"Environment variable '{name}' ('{value}') has invalid type for {cast_func.__name__}: {e}") from e
    return value # Return value (could be None if not required and no default)

try:
    API_ID = load_env_var('API_ID', required=True, cast_func=int)
    API_HASH = load_env_var('API_HASH', required=True, cast_func=str)
    BOT_TOKEN = load_env_var('BOT_TOKEN', required=True, cast_func=str)
    admin_ids_str = load_env_var('ADMIN_IDS', required=False, cast_func=str, default='')
    # Ensure IDs are integers and handle potential whitespace/empty strings
    ADMIN_IDS = [int(id_.strip()) for id_ in admin_ids_str.split(',') if id_.strip().isdigit()]


except ValueError as e:
    # Use basic print here as logging might not be fully configured yet
    print(f"CRITICAL: Configuration Error loading environment variables - {e}")
    # Log to stderr as well, which Render might capture better initially
    sys.stderr.write(f"CRITICAL: Configuration Error loading environment variables - {e}\n")
    sys.exit(1) # Exit immediately on critical config errors

# --- Logging Setup ---
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - [%(name)s:%(lineno)d] - %(message)s')
log_file_path = os.path.join(DATA_DIR, LOG_FILENAME)

# Ensure data directory exists before setting up file handler
try:
    # DATA_DIR (/data on Render) must already exist or be creatable.
    # If running on Render, RENDER_DISK_PATH should point to the mounted disk.
    if not os.path.isdir(DATA_DIR):
        print(f"Data directory '{DATA_DIR}' not found. Attempting to create...")
        try:
             os.makedirs(DATA_DIR, exist_ok=True)
             print(f"Created data directory: {DATA_DIR}")
        except OSError as e:
             print(f"CRITICAL: Failed to create data directory '{DATA_DIR}': {e}. Check permissions and RENDER_DISK_PATH.", file=sys.stderr)
             sys.exit(1)

    # Optional: Check writability early (logging might not be fully set up)
    if not os.access(DATA_DIR, os.W_OK):
         print(f"WARNING: Data directory '{DATA_DIR}' is not writable! File logging and database operations might fail.", file=sys.stderr)

except Exception as e:
    print(f"CRITICAL: Error during data directory check/creation: {e}", file=sys.stderr)
    sys.exit(1)


# Now setup logging handlers
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
     print(f"Warning: Permission denied writing log file to {log_file_path}. File logging disabled.", file=sys.stderr)
     file_handler = None
except Exception as e:
    print(f"Warning: Could not set up file logging to {log_file_path}: {e}", file=sys.stderr)
    file_handler = None

# Stream Handler (for console output, captured by Render)
stream_handler = logging.StreamHandler(sys.stdout) # Explicitly use stdout
stream_handler.setFormatter(log_formatter)
# Set console level based on an environment variable? Default to DEBUG.
log_level_str = os.environ.get('LOG_LEVEL', 'DEBUG').upper()
log_level = getattr(logging, log_level_str, logging.DEBUG)
stream_handler.setLevel(log_level)

# Configure root logger
handlers_list = [stream_handler]
if file_handler:
    handlers_list.append(file_handler)

logging.basicConfig(
    level=logging.DEBUG, # Set root logger level to lowest (DEBUG) to allow handlers to filter effectively
    handlers=handlers_list,
    format='%(asctime)s - %(levelname)s - [%(name)s:%(lineno)d] - %(message)s' # BasicConfig needs format too if setting here
)
# --- Define the main log object HERE ---
log = logging.getLogger(__name__) # Use __name__ (config) for the logger name for this file
# --- Assign logger to imghdr shim now that it exists ---
_imghdr_compat_logger = logging.getLogger('imghdr_compat')

# --- Log initial config status AFTER logging is set up ---
log.info(f"Logging configured. Stream level: {log_level_str}. File logging: {'Enabled' if file_handler else 'Disabled'}")
if not ADMIN_IDS and admin_ids_str: # Log if input was given but non were valid ints
    log.warning(f"ADMIN_IDS provided ('{admin_ids_str}') but contained no valid integer IDs.")
elif not ADMIN_IDS:
     log.warning("ADMIN_IDS environment variable is not set or empty. Admin features will be disabled.")


# --- Ensure Session Subdirectory Exists (AFTER Logging is setup) ---
try:
    # DATA_DIR check repeated here now that logging is configured
    if not os.path.isdir(DATA_DIR):
        log.critical(f"CRITICAL: Data directory '{DATA_DIR}' does not exist or is not a directory! Check Render disk mount and RENDER_DISK_PATH env var.")
        sys.exit(1)

    # Explicitly create the 'sessions' subdirectory if it doesn't exist.
    if not os.path.exists(SESSION_DIR):
        log.info(f"Session directory '{SESSION_DIR}' not found. Attempting to create...")
        os.makedirs(SESSION_DIR, exist_ok=True) # exist_ok=True prevents error if dir exists
        log.info(f"Created session directory: {SESSION_DIR}")
    elif not os.path.isdir(SESSION_DIR):
         # Handle case where SESSION_DIR exists but is a file
         log.critical(f"CRITICAL: Path '{SESSION_DIR}' exists but is not a directory!")
         sys.exit(1)
    else:
        log.info(f"Session directory '{SESSION_DIR}' already exists.")

    # Check writability (optional but good diagnostic)
    if not os.access(SESSION_DIR, os.W_OK):
         log.warning(f"Session directory '{SESSION_DIR}' might not be writable! Session file creation may fail.")
    if not os.access(DATA_DIR, os.W_OK):
         log.warning(f"Data directory '{DATA_DIR}' might not be writable! Database file creation/write may fail.")

except OSError as e:
    # If os.makedirs fails with Permission Denied here, it's likely a disk permission issue.
    log.critical(f"CRITICAL: OSError ensuring directory '{SESSION_DIR}' exists/writable: {e}", exc_info=True)
    sys.exit(1)
except Exception as e:
    # Catch any other unexpected errors during setup
    log.critical(f"CRITICAL: Unexpected error setting up directories: {e}", exc_info=True)
    sys.exit(1)

# Log basic config info AFTER directory setup and logging is confirmed working
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
# Using string constants for states can be clearer for debugging and persistence
# There are 33 state variables listed below.
(
    STATE_WAITING_FOR_CODE, STATE_WAITING_FOR_PHONE, STATE_WAITING_FOR_API_ID,
    STATE_WAITING_FOR_API_HASH, STATE_WAITING_FOR_CODE_USERBOT,
    STATE_WAITING_FOR_PASSWORD, STATE_WAITING_FOR_SUB_DETAILS,
    STATE_WAITING_FOR_FOLDER_CHOICE, # Possibly deprecated
    STATE_WAITING_FOR_FOLDER_NAME,
    STATE_WAITING_FOR_FOLDER_SELECTION, # Used for edit/delete choice
    STATE_TASK_SETUP, # Main task config state
    STATE_WAITING_FOR_LANGUAGE, # Not really used as state, handled by callback
    STATE_WAITING_FOR_EXTEND_CODE,
    STATE_WAITING_FOR_EXTEND_DAYS, STATE_WAITING_FOR_ADD_USERBOTS_CODE,
    STATE_WAITING_FOR_ADD_USERBOTS_COUNT, STATE_SELECT_TARGET_GROUPS, # Selecting folder for task target
    STATE_WAITING_FOR_USERBOT_SELECTION, # Used by join, task setup etc.
    STATE_WAITING_FOR_GROUP_LINKS, # Used by join & folder add
    STATE_WAITING_FOR_FOLDER_ACTION, # State after selecting folder to edit
    STATE_WAITING_FOR_PRIMARY_MESSAGE_LINK,
    STATE_WAITING_FOR_FALLBACK_MESSAGE_LINK,
    STATE_FOLDER_EDIT_REMOVE_SELECT, # State for selecting groups to remove from folder
    STATE_FOLDER_RENAME_PROMPT, # State waiting for new folder name
    STATE_ADMIN_CONFIRM_USERBOT_RESET, # State for confirming userbot reset (Not currently used)
    STATE_WAITING_FOR_START_TIME, # State for task start time input
    STATE_ADMIN_TASK_MESSAGE, # State for entering admin task message
    STATE_ADMIN_TASK_SCHEDULE, # State for entering admin task schedule
    STATE_ADMIN_TASK_TARGET, # State for entering admin task target
    STATE_WAITING_FOR_TASK_BOT, # State for selecting bot for task
    STATE_WAITING_FOR_TASK_MESSAGE, # State for entering task message
    STATE_WAITING_FOR_TASK_SCHEDULE, # State for selecting task schedule
    STATE_WAITING_FOR_TASK_TARGET, # State for selecting task target
    STATE_ADMIN_TASK_CONFIRM # State for confirming admin task setup
    # Add any new states here if needed
) = map(str, range(33)) # Updated count to 33 for new states

# --- Callback Data Prefixes ---
# Using prefixes helps route callbacks efficiently in a single handler function
CALLBACK_ADMIN_PREFIX = "admin_"
CALLBACK_CLIENT_PREFIX = "client_"
CALLBACK_TASK_PREFIX = "task_"
CALLBACK_FOLDER_PREFIX = "folder_"
CALLBACK_JOIN_PREFIX = "join_"
CALLBACK_LANG_PREFIX = "lang_"
# CALLBACK_REMOVE_PREFIX = "remove_" # Not used as a standalone prefix? Actions included in other prefixes.
CALLBACK_INTERVAL_PREFIX = "interval_"
CALLBACK_GENERIC_PREFIX = "generic_" # For simple actions like back buttons, confirmation, no-op


# --- Utility Functions ---
def is_admin(user_id: int) -> bool:
    """Checks if a given user ID is in the ADMIN_IDS list."""
    # Ensure ADMIN_IDS exists and user_id is an integer before checking
    return isinstance(user_id, int) and ADMIN_IDS and user_id in ADMIN_IDS

log.info("Configuration loaded successfully.")
# --- END OF FILE config.py ---
