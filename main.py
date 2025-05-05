# --- START OF FILE main.py ---

# main.py
import signal
import sys
import asyncio
import threading
import time # For potential delays if needed

# Import configurations and modules
# Ensure config is imported first if other modules rely on its side effects (like logging setup)
from config import BOT_TOKEN, log, ADMIN_IDS
import database as db
import telethon_utils as telethon_api
import handlers # Import handlers module (which contains main_conversation, error_handler)

from telegram.ext import Updater, Dispatcher

# --- Global Variables ---
# Flag to indicate if shutdown is in progress to prevent duplicate handling
_shutdown_in_progress = False
# Global updater instance for access in the shutdown handler
updater: Updater | None = None
# Store the checker thread to potentially join it on shutdown
checker_thread: threading.Thread | None = None

# --- Shutdown Handler ---
def shutdown(signum, frame):
    """Handles shutdown signals (SIGTERM, SIGINT) for graceful exit."""
    global _shutdown_in_progress, updater, checker_thread
    if _shutdown_in_progress:
        log.warning("Shutdown already in progress, ignoring signal.")
        return
    _shutdown_in_progress = True

    log.info(f"Received shutdown signal {signum}. Initiating graceful shutdown...")

    # 1. Stop Telethon background tasks (task checker) and disconnect clients
    log.info("Requesting Telethon components shutdown...")
    # This signals the checker loop to stop and stops/disconnects userbots
    telethon_api.shutdown_telethon()

    # Optional: Wait for checker thread to finish after signaling stop
    if checker_thread and checker_thread.is_alive():
         log.info("Waiting for background task checker thread to exit...")
         checker_thread.join(timeout=10) # Wait up to 10 seconds
         if checker_thread.is_alive():
              log.warning("Background task checker thread did not exit gracefully.")

    # 2. Stop the PTB Updater polling
    if updater:
        log.info("Stopping PTB Updater polling...")
        updater.stop()
        # Wait for the updater polling thread to actually stop
        if updater.is_running:
             log.info("Waiting for updater thread to finish...")
             # PTB < 20 way: access updater.job_queue.stop() or similar might be needed?
             # PTB 13.x updater.stop() should handle it. We might need to wait for the thread.
             # Accessing private _thread might be unstable, rely on updater.stop() for now.
             time.sleep(1) # Short sleep to allow threads to potentially close

        log.info("PTB Updater stopped.")
    else:
        log.warning("Updater instance not found during shutdown.")

    # 3. Close Database Connection
    log.info("Closing database connection...")
    db.close_db()

    log.info("Shutdown complete. Exiting.")
    sys.exit(0)

# --- Main Function ---
def main():
    """Main function to initialize and start the bot."""
    global updater, checker_thread # Make variables accessible to shutdown handler

    log.info("--- Starting Telegram Bot ---")
    if not ADMIN_IDS:
         log.critical("CRITICAL: No valid ADMIN_IDS configured in environment variables. Bot may not function correctly. Exiting.")
         sys.exit(1)

    # --- Initialize PTB ---
    log.info("Initializing PTB Updater...")
    try:
        # Consider adding persistence later if needed for conversation states across restarts
        # from telegram.ext import PicklePersistence
        # persistence = PicklePersistence(filename=os.path.join(DATA_DIR, 'bot_persistence.pickle'))
        updater = Updater(token=BOT_TOKEN, use_context=True) # persistence=persistence)
        dp: Dispatcher = updater.dispatcher
        log.info("PTB Updater initialized.")
    except Exception as e:
        log.critical(f"CRITICAL: Failed to initialize PTB Updater: {e}", exc_info=True)
        sys.exit(1)

    # --- Register Handlers ---
    log.info("Registering handlers...")
    # Register the main conversation handler from handlers.py
    # This handles commands (/start, /admin) and callback queries routing through states
    if handlers.main_conversation:
        dp.add_handler(handlers.main_conversation)
        log.info("Main conversation handler registered.")
    else:
        log.critical("CRITICAL: Main conversation handler not found in handlers module!")
        sys.exit(1)

    # Register the error handler
    if handlers.error_handler:
         dp.add_error_handler(handlers.error_handler)
         log.info("Error handler registered.")
    else:
         log.warning("Error handler not found in handlers module!")

    log.info("Handlers registered successfully.")

    # --- Initialize Telethon Userbots ---
    log.info("Initializing Telethon userbot runtimes...")
    try:
        telethon_api.initialize_all_userbots()
        log.info("Telethon userbot initialization process completed.")
    except Exception as e:
        # Log error but continue running - bot might function partially without all userbots
        log.error(f"Error during initial userbot runtime initialization: {e}", exc_info=True)

    # --- Start Background Task Checker ---
    log.info("Starting Telethon background task checker thread...")
    try:
        # Create a dedicated event loop for the checker thread
        checker_loop = asyncio.new_event_loop()
        def run_checker_in_loop():
            asyncio.set_event_loop(checker_loop)
            try:
                # Run the async task checker function until it completes (or is stopped)
                checker_loop.run_until_complete(telethon_api.run_check_tasks_periodically())
            except Exception as task_e:
                 # Log critical errors in the checker loop itself
                 log.critical(f"CRITICAL: Background task checker loop crashed: {task_e}", exc_info=True)
            finally:
                 if not checker_loop.is_closed():
                      log.info("Closing background task checker event loop.")
                      checker_loop.close()

        # Start the checker function in a separate daemon thread
        checker_thread = threading.Thread(target=run_checker_in_loop, name="TaskCheckerThread", daemon=True)
        checker_thread.start()
        log.info("Telethon background task checker thread started.")
    except Exception as e:
        log.critical(f"CRITICAL: Failed to start background task checker thread: {e}", exc_info=True)
        # Consider exiting if background tasks are essential and fail to start
        # sys.exit(1)


    # --- Register Signal Handlers ---
    log.info("Registering shutdown signal handlers (SIGTERM, SIGINT)...")
    try:
        signal.signal(signal.SIGTERM, shutdown) # Standard signal for termination (e.g., Docker, Render)
        signal.signal(signal.SIGINT, shutdown)  # Signal for Ctrl+C
        log.info("Signal handlers registered.")
    except ValueError as e:
         # This can happen on Windows if trying to register SIGTERM outside main thread
         log.warning(f"Could not register all signal handlers (maybe running on Windows?): {e}")
         # Try registering only SIGINT on Windows if possible
         try: signal.signal(signal.SIGINT, shutdown)
         except: pass


    # --- Start Polling ---
    log.info("Starting PTB polling...")
    updater.start_polling()
    log.info("--- Bot is now running ---")
    log.info("Press Ctrl+C or send SIGTERM to stop.")

    # Keep the main thread alive until a shutdown signal is received
    updater.idle()

    # --- Cleanup (after idle() stops, usually via shutdown signal) ---
    log.info("Bot polling loop exited.")
    # Ensure shutdown logic runs even if idle() exits unexpectedly (e.g., error not caught by handler)
    if not _shutdown_in_progress:
         log.warning("Updater exited unexpectedly without a shutdown signal. Initiating manual cleanup...")
         shutdown(0, None) # Manual shutdown call


if __name__ == "__main__":
    # Ensure the script is run directly
    main()
# --- END OF FILE main.py ---
