# main.py
import signal
import sys
import asyncio
import threading

from telegram.ext import Updater, Dispatcher

# Import configurations and modules
from config import BOT_TOKEN, log
import database as db
import telethon_utils as telethon_api
import handlers # Import handlers to register them

# Flag to indicate if shutdown is in progress
_shutdown_in_progress = False

def shutdown(signum, frame):
    """Handles shutdown signals (SIGTERM, SIGINT) for graceful exit."""
    global _shutdown_in_progress
    if _shutdown_in_progress:
        log.warning("Shutdown already in progress, ignoring signal.")
        return
    _shutdown_in_progress = True

    log.info(f"Received shutdown signal {signum}. Initiating graceful shutdown...")

    # 1. Stop Telethon background tasks and disconnect clients
    log.info("Stopping Telethon components...")
    telethon_api.shutdown_telethon() # Signals stop event and handles disconnects/thread joins

    # 2. Stop the PTB Updater
    # Access updater instance (need to make it accessible, e.g., global or passed)
    if 'updater' in globals() and updater:
        log.info("Stopping PTB Updater polling...")
        updater.stop()
        log.info("PTB Updater stopped.")
    else:
        log.warning("Updater instance not found during shutdown.")

    # 3. Close Database Connection
    log.info("Closing database connection...")
    db.close_db()

    log.info("Shutdown complete. Exiting.")
    sys.exit(0)

def main():
    """Main function to start the bot."""
    global updater # Make updater accessible to shutdown function

    log.info("Starting Telegram Bot...")

    # --- Initialize PTB ---
    try:
        updater = Updater(token=BOT_TOKEN, use_context=True)
        dp: Dispatcher = updater.dispatcher
    except Exception as e:
        log.critical(f"CRITICAL: Failed to initialize PTB Updater: {e}", exc_info=True)
        sys.exit(1)

    # --- Register Handlers ---
    log.info("Registering handlers...")
    # Register the main conversation handler from handlers.py
    dp.add_handler(handlers.main_conversation)

    # Register the error handler
    dp.add_handler(handlers.error_handler) # Note: ErrorHandler takes the callback directly

    log.info("Handlers registered.")

    # --- Initialize Telethon Userbots ---
    # Load existing sessions and potentially connect/check auth status
    # Run this in the main thread before starting polling/background tasks
    # This ensures DB is ready and initial bot states are loaded.
    try:
         # This function now iterates bots from DB and initializes runtime if not inactive
        telethon_api.initialize_all_userbots()
    except Exception as e:
        log.error(f"Error during initial userbot initialization: {e}", exc_info=True)
        # Decide if this is critical enough to stop startup? Maybe not, log and continue.

    # --- Start Background Task Checker ---
    # telethon_utils now handles starting its own background task thread internally
    # Ensure telethon_api.start_background_tasks() is implemented correctly if needed,
    # but the current telethon_utils.py starts it automatically via _execute_single_task calls.
    # The run_check_tasks_periodically function needs to be started.
    # Let's create a dedicated thread for the task checker loop from telethon_utils.
    log.info("Starting Telethon background task checker thread...")
    try:
        # The checker loop needs its own event loop to run async tasks in
        checker_loop = asyncio.new_event_loop()
        def run_checker():
            asyncio.set_event_loop(checker_loop)
            checker_loop.run_until_complete(telethon_api.run_check_tasks_periodically())

        checker_thread = threading.Thread(target=run_checker, name="TaskCheckerThread", daemon=True)
        checker_thread.start()
        log.info("Telethon background task checker thread started.")
    except Exception as e:
        log.critical(f"CRITICAL: Failed to start background task checker: {e}", exc_info=True)
        # Depending on importance, might exit here
        # sys.exit(1)


    # --- Register Signal Handlers ---
    signal.signal(signal.SIGTERM, shutdown) # Signal for graceful shutdown (e.g., Docker, Render)
    signal.signal(signal.SIGINT, shutdown)  # Signal for Ctrl+C

    # --- Start Polling ---
    log.info("Starting PTB polling...")
    updater.start_polling()
    log.info("Bot is now running. Press Ctrl+C to stop.")

    # Keep the main thread alive until shutdown signal
    updater.idle()

    # --- Cleanup (should be handled by shutdown function) ---
    # Code here likely won't be reached if using updater.idle() and signal handlers correctly
    log.info("Bot polling ended.")
    # Ensure cleanup runs even if idle() exits unexpectedly
    if not _shutdown_in_progress:
         shutdown(0, None) # Manual shutdown call if idle ends without signal


if __name__ == "__main__":
    main()