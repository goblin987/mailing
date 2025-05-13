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

async def async_shutdown():
    """Async shutdown handler for graceful exit."""
    global _shutdown_in_progress, updater, checker_thread, checker_loop
    if _shutdown_in_progress:
        log.warning("Shutdown already in progress, ignoring signal.")
        return
    _shutdown_in_progress = True

    log.info("Initiating graceful shutdown...")

    # 1. Stop Telethon background tasks (task checker) and disconnect clients
    log.info("Requesting Telethon components shutdown...")
    telethon_api.shutdown_telethon() # Signals checker loop and disconnects bots

    # 2. Stop the PTB Updater polling
    if updater:
        log.info("Stopping PTB Updater polling...")
        # Stop accepting new updates and shut down the dispatcher queues.
        if updater.running: # PTB v13 check
            updater.stop()
        
        # Wait for the polling thread to finish
        if hasattr(updater, 'job_queue') and updater.job_queue:
             updater.job_queue.stop() # Stop job queue if exists
        
        if hasattr(updater, '_thread') and updater._thread and updater._thread.is_alive():
             log.info("Waiting for updater thread to finish...")
             updater._thread.join(timeout=10)
             if updater._thread.is_alive():
                  log.warning("Updater thread did not exit gracefully.")

        log.info("PTB Updater stopped.")
    else:
        log.warning("Updater instance not found during shutdown.")

    # 3. Close Database Connection
    log.info("Closing database connection...")
    db.close_db()

    log.info("Shutdown complete.")

def signal_handler(signum, frame):
    """Signal handler for system signals."""
    log.info(f"Received signal {signum}. Initiating shutdown...")
    asyncio.run(async_shutdown())
    sys.exit(0)

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
        updater = Updater(token=BOT_TOKEN, use_context=True, workers=4)  # Added workers parameter
        log.info("PTB Updater initialized with 4 workers.")
    except Exception as e:
        log.critical(f"CRITICAL: Failed to initialize PTB Updater: {e}", exc_info=True)
        sys.exit(1)

    dp: Dispatcher = updater.dispatcher
    dp.use_context = True

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
        # Skip background task checker if function doesn't exist
        if hasattr(telethon_api, 'run_check_tasks_periodically'):
            checker_thread = threading.Thread(target=telethon_api.run_check_tasks_periodically, daemon=True)
            checker_thread.start()
            log.info("Telethon background task checker thread started.")
        else:
            log.warning("Background task checker not available in telethon_utils, skipping...")
    except Exception as e:
        log.error(f"Error starting background task checker thread: {e}", exc_info=True)

    # --- Register Signal Handlers ---
    log.info("Registering shutdown signal handlers...")
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, signal_handler)
    log.info("Signal handlers registered.")

    # --- Start Bot ---
    log.info("Starting PTB polling...")
    try:
        updater.start_polling(drop_pending_updates=True)
        log.info("--- Bot is now running ---")
        log.info("Press Ctrl+C or send SIGTERM to stop.")
        
        # Keep the main task running
        while True:
            await asyncio.sleep(1)
            
    except Exception as e:
        log.error(f"Error in main polling loop: {e}", exc_info=True)
        await async_shutdown()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Received KeyboardInterrupt, initiating shutdown...")
        asyncio.run(async_shutdown())
    except Exception as e:
        log.error(f"Fatal error in main: {e}", exc_info=True)
        try:
            asyncio.run(async_shutdown())
        except:
            pass
# --- END OF FILE main.py ---
