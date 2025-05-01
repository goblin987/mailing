# main.py
import signal
import sys
import asyncio
import threading

from telegram.ext import Updater, Dispatcher, Handler # Import base Handler for type checking if needed

# Import configurations and modules
from config import BOT_TOKEN, log
import database as db
import telethon_utils as telethon_api
import handlers # Import handlers module

# Flag to indicate if shutdown is in progress
_shutdown_in_progress = False
updater = None # Define updater in global scope for shutdown handler access

def shutdown(signum, frame):
    """Handles shutdown signals (SIGTERM, SIGINT) for graceful exit."""
    global _shutdown_in_progress, updater
    if _shutdown_in_progress:
        log.warning("Shutdown already in progress, ignoring signal.")
        return
    _shutdown_in_progress = True

    log.info(f"Received shutdown signal {signum}. Initiating graceful shutdown...")

    # 1. Stop Telethon background tasks and disconnect clients
    log.info("Stopping Telethon components...")
    telethon_api.shutdown_telethon() # Signals stop event and handles disconnects/thread joins

    # 2. Stop the PTB Updater
    if updater:
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
        # persistence= # Consider adding persistence later if needed
        updater = Updater(token=BOT_TOKEN, use_context=True)
        dp: Dispatcher = updater.dispatcher
    except Exception as e:
        log.critical(f"CRITICAL: Failed to initialize PTB Updater: {e}", exc_info=True)
        sys.exit(1)

    # --- Register Handlers ---
    log.info("Registering handlers...")
    # Register the main conversation handler from handlers.py
    dp.add_handler(handlers.main_conversation)

    # Register the error handler CORRECTLY using add_error_handler
    dp.add_error_handler(handlers.error_handler)

    log.info("Handlers registered.")

    # --- Initialize Telethon Userbots ---
    try:
        telethon_api.initialize_all_userbots()
    except Exception as e:
        log.error(f"Error during initial userbot initialization: {e}", exc_info=True)

    # --- Start Background Task Checker ---
    log.info("Starting Telethon background task checker thread...")
    try:
        checker_loop = asyncio.new_event_loop()
        def run_checker():
            asyncio.set_event_loop(checker_loop)
            # Ensure the loop eventually completes or handles exceptions properly
            try:
                checker_loop.run_until_complete(telethon_api.run_check_tasks_periodically())
            except Exception as task_e:
                 log.critical(f"CRITICAL: Background task checker loop crashed: {task_e}", exc_info=True)
            finally:
                 if not checker_loop.is_closed(): checker_loop.close()

        checker_thread = threading.Thread(target=run_checker, name="TaskCheckerThread", daemon=True)
        checker_thread.start()
        log.info("Telethon background task checker thread started.")
    except Exception as e:
        log.critical(f"CRITICAL: Failed to start background task checker: {e}", exc_info=True)
        # Consider exiting if background tasks are essential and fail to start
        # sys.exit(1)


    # --- Register Signal Handlers ---
    signal.signal(signal.SIGTERM, shutdown) # For Docker/Render shutdown
    signal.signal(signal.SIGINT, shutdown)  # For Ctrl+C

    # --- Start Polling ---
    log.info("Starting PTB polling...")
    updater.start_polling()
    log.info("Bot is now running. Press Ctrl+C or send SIGTERM to stop.")

    # Keep the main thread alive (listens for signals) until updater.stop() is called
    updater.idle()

    # --- Cleanup (after idle() stops, usually via shutdown signal) ---
    log.info("Bot polling loop exited.")
    # Ensure shutdown logic runs even if idle() exits unexpectedly
    if not _shutdown_in_progress:
         log.warning("Updater exited unexpectedly without shutdown signal. Initiating cleanup.")
         shutdown(0, None) # Manual shutdown call


if __name__ == "__main__":
    main()
