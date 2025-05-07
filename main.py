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
        updater.stop()
        # Wait for the polling thread to finish. Accessing _thread is internal,
        # but often necessary for a clean exit in PTB v13.
        if hasattr(updater, 'job_queue') and updater.job_queue:
             updater.job_queue.stop() # Stop job queue if exists
        if hasattr(updater, '_thread') and updater._thread and updater._thread.is_alive():
             log.info("Waiting for updater thread to finish...")
             updater._thread.join(timeout=5)
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
    os._exit(0) # Use os._exit for a more forceful exit if clean sys.exit fails

# --- Main Function ---
def main():
    """Main function to initialize and start the bot."""
    global updater, checker_thread, checker_loop # Make variables accessible to shutdown handler

    log.info("--- Starting Telegram Bot ---")
    if not ADMIN_IDS:
         log.critical("CRITICAL: No valid ADMIN_IDS configured in environment variables. Bot may not function correctly. Exiting.")
         sys.exit(1)

    # --- Initialize PTB ---
    log.info("Initializing PTB Updater...")
    try:
        # Enable persistence
        persistence_path = os.path.join(DATA_DIR, 'bot_persistence.pickle')
        log.info(f"Using persistence file: {persistence_path}")
        
        try:
            persistence = PicklePersistence(filename=persistence_path)
            updater = Updater(token=BOT_TOKEN, use_context=True, persistence=persistence)
            log.info("PTB Updater initialized with persistence.")
        except (EOFError, TypeError) as e:
            log.warning(f"Failed to load persistence file, it may be corrupted: {e}")
            # Backup the old file if it exists
            if os.path.exists(persistence_path):
                backup_path = persistence_path + '.backup'
                try:
                    os.rename(persistence_path, backup_path)
                    log.info(f"Backed up corrupted persistence file to {backup_path}")
                except Exception as backup_err:
                    log.error(f"Failed to backup persistence file: {backup_err}")
                    # If we can't backup, try to remove it
                    try:
                        os.remove(persistence_path)
                        log.info("Removed corrupted persistence file")
                    except Exception as rm_err:
                        log.error(f"Failed to remove persistence file: {rm_err}")
            
            # Create new persistence instance
            persistence = PicklePersistence(filename=persistence_path)
            updater = Updater(token=BOT_TOKEN, use_context=True, persistence=persistence)
            log.info("PTB Updater initialized with fresh persistence file.")
            
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

    if handlers.error_handler:
        # Register error handler
        dp.add_error_handler(lambda u, c: asyncio.create_task(handlers.error_handler(u, c)))
        log.info("Error handler registered.")
    else:
        log.warning("Error handler not found in handlers module!")

    log.info("Handlers registered successfully.")

    # --- Initialize Telethon Userbots ---
    log.info("Initializing Telethon userbot runtimes...")
    try:
        # This function now attempts to start runtimes for all non-inactive bots in DB
        telethon_api.initialize_all_userbots()
        log.info("Telethon userbot initialization process requested.")
    except Exception as e:
        # Log error but continue running - bot might function partially without all userbots
        log.error(f"Error during initial userbot runtime initialization request: {e}", exc_info=True)

    # --- Start Background Task Checker ---
    log.info("Starting Telethon background task checker thread...")
    try:
        # Create and store the event loop for the checker thread
        checker_loop = asyncio.new_event_loop()
        def run_checker_in_loop():
            asyncio.set_event_loop(checker_loop)
            try:
                checker_loop.run_until_complete(telethon_api.run_check_tasks_periodically())
            except asyncio.CancelledError:
                 log.info("Background task checker loop cancelled.")
            except Exception as task_e:
                 log.critical(f"CRITICAL: Background task checker loop crashed: {task_e}", exc_info=True)
            finally:
                 if not checker_loop.is_closed():
                      log.info("Closing background task checker event loop.")
                      # Additional cleanup before closing
                      try:
                           loop_tasks = asyncio.all_tasks(loop=checker_loop)
                           for task in loop_tasks:
                               if not task.done(): task.cancel()
                           # Wait briefly for tasks to cancel
                           checker_loop.run_until_complete(asyncio.gather(*loop_tasks, return_exceptions=True))
                           checker_loop.run_until_complete(checker_loop.shutdown_asyncgens())
                      except Exception as close_err:
                           log.error(f"Error during checker loop final cleanup: {close_err}")
                      finally:
                            checker_loop.close()
                      log.info("Checker loop closed.")

        # Start the checker function in a separate daemon thread
        checker_thread = threading.Thread(target=run_checker_in_loop, name="TaskCheckerThread", daemon=True)
        checker_thread.start()
        log.info("Telethon background task checker thread started.")
    except Exception as e:
        log.critical(f"CRITICAL: Failed to start background task checker thread: {e}", exc_info=True)
        # Don't exit, but log critically. Bot may run without background tasks.


    # --- Register Signal Handlers ---
    log.info("Registering shutdown signal handlers (SIGTERM, SIGINT)...")
    try:
        signal.signal(signal.SIGTERM, shutdown) # Standard signal for termination
        signal.signal(signal.SIGINT, shutdown)  # Signal for Ctrl+C
        log.info("Signal handlers registered.")
    except ValueError as e:
         # This can happen on Windows if trying to register SIGTERM outside main thread
         log.warning(f"Could not register all signal handlers (maybe running on Windows?): {e}")
         # Try registering only SIGINT on Windows if possible
         try: signal.signal(signal.SIGINT, shutdown)
         except Exception: pass


    # --- Start Polling ---
    log.info("Starting PTB polling...")
    updater.start_polling()
    log.info("--- Bot is now running ---")
    log.info("Press Ctrl+C or send SIGTERM to stop.")

    # Keep the main thread alive until a shutdown signal is received
    updater.idle()

    # --- Cleanup (after idle() stops, usually via shutdown signal) ---
    log.info("Bot polling loop exited.")
    # Ensure shutdown logic runs even if idle() exits unexpectedly
    if not _shutdown_in_progress:
         log.warning("Updater exited unexpectedly without a shutdown signal. Initiating manual cleanup...")
         shutdown(0, None) # Manual shutdown call


if __name__ == "__main__":
    # Ensure the script is run directly
    main()
# --- END OF FILE main.py ---
