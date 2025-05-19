import signal
import sys
import asyncio
import threading
import time # For potential delays if needed
import os # Import os for path operations
import logging

from telegram.ext import Updater, PicklePersistence # Using Updater from v13
from telegram.ext import CommandHandler, MessageHandler, CallbackQueryHandler, Filters

from config import BOT_TOKEN, log, ADMIN_IDS, DATA_DIR, CHECK_TASKS_INTERVAL # Added CHECK_TASKS_INTERVAL
import database as db
import telethon_utils as telethon_api
import handlers # Import handlers module

# --- Global Variables ---
_shutdown_in_progress = False
updater_instance: Updater | None = None 
checker_task: asyncio.Task | None = None 

async def run_background_tasks():
    """Run background tasks in a separate asyncio loop."""
    log.info("Background task runner started.")
    while not _shutdown_in_progress: 
        try:
            # Placeholder for any other general background tasks
            # The Telethon task checker is started separately if it has its own loop management
            await asyncio.sleep(CHECK_TASKS_INTERVAL) 
        except asyncio.CancelledError:
            log.info("Background tasks runner cancelled.")
            break
        except Exception as e:
            log.error(f"Error in background tasks runner: {e}", exc_info=True)
            await asyncio.sleep(60) 
    log.info("Background task runner stopped.")


async def async_shutdown_tasks():
    """Gracefully shut down background tasks."""
    global checker_task
    if checker_task and not checker_task.done():
        log.info("Cancelling background task checker...")
        checker_task.cancel()
        try:
            await checker_task
        except asyncio.CancelledError:
            log.info("Background task checker successfully cancelled.")
        except Exception as e:
            log.error(f"Error during background task cancellation: {e}", exc_info=True)
    else:
        log.info("Background task checker not running or already finished.")


async def perform_shutdown():
    """Async shutdown handler for graceful exit."""
    global _shutdown_in_progress, updater_instance

    if _shutdown_in_progress:
        log.warning("Shutdown already in progress, ignoring signal.")
        return
    _shutdown_in_progress = True
    log.info("Initiating graceful shutdown...")

    # 1. Stop background tasks (general ones, Telethon has its own via _stop_event)
    await async_shutdown_tasks()

    # 2. Stop Telethon components
    log.info("Requesting Telethon components shutdown...")
    if hasattr(telethon_api, 'shutdown_telethon'):
        telethon_api.shutdown_telethon() # This sets _stop_event for telethon_utils
    log.info("Telethon components shutdown process initiated.")

    # 3. Stop the PTB Updater
    if updater_instance:
        log.info("Stopping PTB Updater...")
        if hasattr(updater_instance, 'running') and updater_instance.running: 
            updater_instance.stop()
            log.info("PTB Updater.stop() called.")
        elif hasattr(updater_instance, 'is_polling') and updater_instance.is_polling(): # Older PTB check
             updater_instance.stop()
             log.info("PTB Updater.stop() called (is_polling check).")
    else:
        log.warning("Updater instance not found during shutdown.")

    # 4. Close Database Connection
    log.info("Closing database connection...")
    db.close_db()

    log.info("Shutdown complete.")
    try:
        current_loop = asyncio.get_running_loop()
        if current_loop.is_running():
            current_loop.stop()
    except RuntimeError: # Loop not running
        pass


def signal_handler_sync(signum, frame):
    """Signal handler for system signals (runs in main thread)."""
    log.info(f"Received signal {signum}. Initiating shutdown from signal_handler_sync...")
    if not _shutdown_in_progress:
        # Schedule the async shutdown to run in the main event loop if it exists
        try:
            loop = asyncio.get_running_loop()
            if loop.is_running():
                asyncio.create_task(perform_shutdown())
            else: # If no loop is running, try to run perform_shutdown directly (might not work perfectly for all async parts)
                asyncio.run(perform_shutdown())
        except RuntimeError: # No current event loop
             log.warning("No running event loop to schedule perform_shutdown. Attempting direct run.")
             asyncio.run(perform_shutdown())


async def main_async():
    """Main async function to initialize and start the bot."""
    global updater_instance, checker_task

    log.info("--- Starting Telegram Bot (Async Main) ---")
    if not ADMIN_IDS:
        log.critical("CRITICAL: No valid ADMIN_IDS. Exiting.")
        sys.exit(1)

    log.info("Initializing Telethon userbot runtimes...")
    await telethon_api.initialize_all_userbots() 
    log.info("Telethon userbot initialization process completed.")

    if hasattr(telethon_api, 'run_check_tasks_periodically'):
        log.info("Starting Telethon background task checker...")
        asyncio.create_task(telethon_api.run_check_tasks_periodically())
        log.info("Telethon background task checker started.")
    
    # Start other general background tasks if needed
    # checker_task = asyncio.create_task(run_background_tasks())


    updater_instance = Updater(BOT_TOKEN, use_context=True) 
    dp = updater_instance.dispatcher

    conv_handler = handlers.main()
    dp.add_handler(conv_handler)
    log.info("Main conversation handler registered.")

    dp.add_error_handler(handlers.async_error_handler)
    log.info("Error handler registered.")

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        # For SIGINT (Ctrl+C), Python's default handler raises KeyboardInterrupt.
        # We want our graceful shutdown for both.
        # loop.add_signal_handler might be cleaner if not for Windows compatibility / complex setups.
        # Using signal.signal is broadly compatible.
        signal.signal(sig, signal_handler_sync)
    log.info("Signal handlers registered for SIGTERM and SIGINT.")


    updater_instance.start_polling(drop_pending_updates=True)
    log.info("--- Bot is now polling ---")
    log.info("Press Ctrl+C or send SIGTERM to stop.")

    try:
        while not _shutdown_in_progress:
            await asyncio.sleep(1) # Keep the main coroutine alive
    except KeyboardInterrupt: # Handle Ctrl+C if not fully caught by signal handler
        log.info("KeyboardInterrupt received in main_async. Initiating shutdown.")
        if not _shutdown_in_progress:
            await perform_shutdown()
    except asyncio.CancelledError:
        log.info("Main asyncio loop cancelled.")
    finally:
        if not _shutdown_in_progress: # Ensure shutdown runs if loop exited for other reasons
            log.warning("Main loop exited unexpectedly. Performing cleanup...")
            await perform_shutdown()
        log.info("Main asyncio loop finished.")


if __name__ == '__main__':
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        log.info("KeyboardInterrupt received at top level. Shutdown should have been handled.")
    except Exception as e:
        log.critical(f"Fatal error in __main__ execution: {e}", exc_info=True)
    finally:
        log.info("Application exiting.")
