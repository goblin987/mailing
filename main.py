# --- START OF FILE main.py ---

# main.py
import signal
import sys
import asyncio
import threading
import time # For potential delays if needed
import os # Import os for path operations

# Import configurations and modules
# Ensure config is imported first if other modules rely on its side effects (like logging setup)
from config import BOT_TOKEN, log, ADMIN_IDS, DATA_DIR # Import DATA_DIR for persistence if needed
import database as db
import telethon_utils as telethon_api
import handlers # Import handlers module (which contains main_conversation, error_handler)

from telegram.ext import Updater, Dispatcher, PicklePersistence # Import PicklePersistence
from telegram.ext import CommandHandler, MessageHandler, CallbackQueryHandler, Filters # Import CommandHandler, MessageHandler, CallbackQueryHandler, Filters

# --- Global Variables ---
# Flag to indicate if shutdown is in progress to prevent duplicate handling
_shutdown_in_progress = False
# Global updater instance for access in the shutdown handler
updater: Updater | None = None
# Store the checker thread to potentially join it on shutdown
checker_thread: threading.Thread | None = None
# Store the checker loop to safely interact with it
checker_loop: asyncio.AbstractEventLoop | None = None


# --- Shutdown Handler ---
def shutdown(signum, frame):
    """Handles shutdown signals (SIGTERM, SIGINT) for graceful exit."""
    global _shutdown_in_progress, updater, checker_thread, checker_loop
    if _shutdown_in_progress:
        log.warning("Shutdown already in progress, ignoring signal.")
        return
    _shutdown_in_progress = True

    log.info(f"Received shutdown signal {signum}. Initiating graceful shutdown...")

    # 1. Stop Telethon background tasks (task checker) and disconnect clients
    log.info("Requesting Telethon components shutdown...")
    telethon_api.shutdown_telethon() # Signals checker loop and disconnects bots

    # 2. Stop the PTB Updater polling
    if updater:
        log.info("Stopping PTB Updater polling...")
        # Stop accepting new updates and shut down the dispatcher queues.
        if updater.running: # PTB v13 check
            updater.stop()
        
        # Wait for the polling thread to finish. Accessing _thread is internal,
        # but often necessary for a clean exit in PTB v13.
        if hasattr(updater, 'job_queue') and updater.job_queue:
             updater.job_queue.stop() # Stop job queue if exists
        
        # For PTB v13, the dispatcher has a `shutdown` method that can be awaited if in async context,
        # or called if in sync. Since shutdown handler is sync, direct call is okay.
        # However, `updater.stop()` should handle dispatcher shutdown too.
        # The `updater._thread` join is a common pattern for v13.
        if hasattr(updater, '_thread') and updater._thread and updater._thread.is_alive():
             log.info("Waiting for updater thread to finish...")
             updater._thread.join(timeout=10) # Increased timeout
             if updater._thread.is_alive():
                  log.warning("Updater thread did not exit gracefully.")

        log.info("PTB Updater stopped.")
    else:
        log.warning("Updater instance not found during shutdown.")

    # 3. Close Database Connection
    log.info("Closing database connection...")
    db.close_db()

    log.info("Shutdown complete. Exiting.")
    # Force exit if threads are hanging
    # sys.exit(0) # Prefer sys.exit for cleaner exit if possible
    os._exit(0) # Use os._exit for a more forceful exit if clean sys.exit fails

# --- Main Function ---
async def main():
    """Main function to initialize and start the bot."""
    global updater, checker_thread, checker_loop

    log.info("--- Starting Telegram Bot ---")
    if not ADMIN_IDS:
        log.critical("CRITICAL: No valid ADMIN_IDS configured in environment variables. Bot may not function correctly. Exiting.")
        sys.exit(1)

    # --- Initialize PTB ---
    log.info("Initializing PTB Updater...")
    try:
        updater = Updater(token=BOT_TOKEN, use_context=True, persistence=None)
        log.info("PTB Updater initialized WITHOUT persistence.")
    except Exception as e:
        log.critical(f"CRITICAL: Failed to initialize PTB Updater: {e}", exc_info=True)
        sys.exit(1)

    dp: Dispatcher = updater.dispatcher

    # --- Register Handlers ---
    log.info("Registering handlers...")

    if handlers.main_conversation:
        dp.add_handler(handlers.main_conversation)
        log.info("Main conversation handler registered.")
    else:
        log.critical("CRITICAL: Main conversation handler not found in handlers module!")
        sys.exit(1)

    # Register the error handler
    dp.add_error_handler(handlers.async_error_handler)
    log.info("Error handler registered.")

    # --- Initialize Telethon Userbots ---
    log.info("Initializing Telethon userbot runtimes...")
    try:
        await telethon_api.initialize_all_userbots()
        log.info("Telethon userbot initialization process requested.")
    except Exception as e:
        log.error(f"Error during initial userbot runtime initialization request: {e}", exc_info=True)

    # --- Start Background Task Checker ---
    log.info("Starting Telethon background task checker thread...")
    try:
        checker_loop = asyncio.new_event_loop()
        def run_checker_in_loop():
            asyncio.set_event_loop(checker_loop)
            try:
                if asyncio.iscoroutinefunction(telethon_api.run_check_tasks_periodically):
                    checker_loop.run_until_complete(telethon_api.run_check_tasks_periodically())
                else:
                    telethon_api.run_check_tasks_periodically()
            except asyncio.CancelledError:
                log.info("Background task checker loop cancelled.")
            except Exception as task_e:
                log.critical(f"CRITICAL: Background task checker loop crashed: {task_e}", exc_info=True)
            finally:
                if not checker_loop.is_closed():
                    checker_loop.close()
                    log.info("Checker loop closed.")

        checker_thread = threading.Thread(target=run_checker_in_loop, name="TaskCheckerThread", daemon=True)
        checker_thread.start()
        log.info("Telethon background task checker thread started.")
    except Exception as e:
        log.critical(f"CRITICAL: Failed to start background task checker thread: {e}", exc_info=True)

    # --- Register Signal Handlers ---
    log.info("Registering shutdown signal handlers...")
    try:
        signal.signal(signal.SIGTERM, shutdown)
        signal.signal(signal.SIGINT, shutdown)
        log.info("Signal handlers registered.")
    except ValueError as e:
        log.warning(f"Could not register all signal handlers: {e}")
        try:
            signal.signal(signal.SIGINT, shutdown)
        except Exception:
            pass

    # --- Start Polling ---
    log.info("Starting PTB polling...")
    updater.start_polling()
    log.info("--- Bot is now running ---")
    log.info("Press Ctrl+C or send SIGTERM to stop.")

    try:
        await asyncio.get_event_loop().create_future()  # Keep the event loop running
    except asyncio.CancelledError:
        pass
    except KeyboardInterrupt:
        if not _shutdown_in_progress:
            shutdown(signal.SIGINT, None)
    except Exception as e:
        log.error(f"Main loop exited with an exception: {e}", exc_info=True)
        if not _shutdown_in_progress:
            shutdown(0, None)

    log.info("Bot polling loop exited.")
    if not _shutdown_in_progress:
        log.warning("Bot exited unexpectedly. Initiating manual cleanup...")
        shutdown(0, None)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Received keyboard interrupt, shutting down...")
    except Exception as e:
        log.critical(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
# --- END OF FILE main.py ---
