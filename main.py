import signal
import sys
import asyncio
import threading
import time # For potential delays if needed
import os # Import os for path operations
import logging

from telegram.ext import Updater, PicklePersistence # Using Updater from v13
from telegram.ext import CommandHandler, MessageHandler, CallbackQueryHandler, Filters

from config import BOT_TOKEN, log, ADMIN_IDS, DATA_DIR
import database as db
import telethon_utils as telethon_api
import handlers # Import handlers module

# --- Global Variables ---
_shutdown_in_progress = False
updater_instance: Updater | None = None # Renamed to avoid conflict with PTB's internal 'updater'
checker_task: asyncio.Task | None = None # For the background task

async def run_background_tasks():
    """Run background tasks in a separate asyncio loop."""
    log.info("Background task runner started.")
    while not _shutdown_in_progress: # Check global shutdown flag
        try:
            # Check for Telethon userbot tasks (client tasks)
            if hasattr(telethon_api, 'run_check_tasks_periodically'): # Check if function exists
                # This function is long-running, so it should handle its own loop and stop event.
                # We just ensure it's started once. For this example, assuming it's a one-off check.
                # A more robust way is for run_check_tasks_periodically to be a long-running coroutine.
                # The current telethon_utils.run_check_tasks_periodically seems designed to be run once and loop internally.
                pass # It's started by initialize_all_userbots or a dedicated starter.
                     # The current structure of `check_tasks` in `telethon_utils` seems to be a one-shot.
                     # Let's use `run_check_tasks_periodically` if that's the intended looping one.
                     # The provided `telethon_utils.py` has `run_check_tasks_periodically`.

            # Check for Admin tasks (if any scheduled via a cron-like mechanism not shown yet)
            # For now, only client tasks are actively checked by telethon_utils.
            # If admin tasks are also managed by telethon_utils.run_check_tasks_periodically, that's fine.

            await asyncio.sleep(CHECK_TASKS_INTERVAL) # Default interval from config
        except asyncio.CancelledError:
            log.info("Background tasks runner cancelled.")
            break
        except Exception as e:
            log.error(f"Error in background tasks runner: {e}", exc_info=True)
            await asyncio.sleep(60) # Wait longer before retrying on error
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

    # 1. Stop background tasks
    await async_shutdown_tasks()

    # 2. Stop Telethon components
    log.info("Requesting Telethon components shutdown...")
    if hasattr(telethon_api, 'shutdown_telethon'):
        # Assuming shutdown_telethon is synchronous or manages its own async cleanup.
        # If it's async, it should be awaited: await telethon_api.shutdown_telethon()
        telethon_api.shutdown_telethon()
    log.info("Telethon components shutdown requested.")

    # 3. Stop the PTB Updater
    if updater_instance:
        log.info("Stopping PTB Updater...")
        if updater_instance.running: # For PTB v13+
            updater_instance.stop()
            # PTB v13's updater.stop() is synchronous and handles thread joining.
            log.info("PTB Updater.stop() called.")
        # For PTB v13, job_queue is part of Application, not Updater directly.
        # If jobs were added to dispatcher, they are managed there.
        # If using Application class (recommended for v20+), it's app.shutdown().
    else:
        log.warning("Updater instance not found during shutdown.")

    # 4. Close Database Connection
    log.info("Closing database connection...")
    db.close_db()

    log.info("Shutdown complete.")
    # Ensure the main asyncio loop can exit
    current_loop = asyncio.get_running_loop()
    current_loop.stop()


def signal_handler_sync(signum, frame):
    """Signal handler for system signals (runs in main thread)."""
    log.info(f"Received signal {signum}. Initiating shutdown from signal_handler_sync...")
    # Schedule the async shutdown to run in the main event loop
    if not _shutdown_in_progress:
        asyncio.create_task(perform_shutdown())


async def main_async():
    """Main async function to initialize and start the bot."""
    global updater_instance, checker_task

    log.info("--- Starting Telegram Bot (Async Main) ---")
    if not ADMIN_IDS:
        log.critical("CRITICAL: No valid ADMIN_IDS. Exiting.")
        sys.exit(1)

    # Initialize Telethon Userbots first, as they might be needed by handlers or tasks
    log.info("Initializing Telethon userbot runtimes...")
    await telethon_api.initialize_all_userbots() # This should start their loops
    log.info("Telethon userbot initialization process completed.")

    # Start the Telethon task checker if it's designed to run independently
    if hasattr(telethon_api, 'run_check_tasks_periodically'):
        log.info("Starting Telethon background task checker...")
        # This function runs its own loop, so we just start it as a task
        asyncio.create_task(telethon_api.run_check_tasks_periodically())
        log.info("Telethon background task checker started.")
    
    # Start other general background tasks (if any, separate from Telethon's)
    # checker_task = asyncio.create_task(run_background_tasks())


    # Create the Updater and pass it your bot's token
    # No PicklePersistence specified in original problem, so removing for simplicity.
    # If persistence is needed, DATA_DIR can be used for the file path.
    # persistence_path = os.path.join(DATA_DIR, "bot_persistence.pickle")
    # persistence = PicklePersistence(filepath=persistence_path)
    updater_instance = Updater(BOT_TOKEN, use_context=True) # Removed workers, default is fine.
    dp = updater_instance.dispatcher

    # Get the conversation handler from handlers module
    conv_handler = handlers.main()
    dp.add_handler(conv_handler)
    log.info("Main conversation handler registered.")

    # Register the error handler
    dp.add_error_handler(handlers.async_error_handler)
    log.info("Error handler registered.")

    # Register Signal Handlers (for clean shutdown)
    # For asyncio, it's better to handle signals within the loop or use add_signal_handler
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(perform_shutdown()))
    log.info("Signal handlers registered for SIGTERM and SIGINT.")


    # Start the Bot polling
    updater_instance.start_polling(drop_pending_updates=True)
    log.info("--- Bot is now polling ---")
    log.info("Press Ctrl+C or send SIGTERM to stop.")

    # Keep the main thread alive until shutdown is triggered
    try:
        while not _shutdown_in_progress:
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        log.info("Main asyncio loop cancelled.")
    finally:
        if updater_instance and updater_instance.running:
            updater_instance.stop()
        log.info("Main asyncio loop finished.")


if __name__ == '__main__':
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        log.info("KeyboardInterrupt received in __main__.")
        # Shutdown should be handled by the signal handler within the loop
    except Exception as e:
        log.critical(f"Fatal error in __main__ execution: {e}", exc_info=True)
    finally:
        log.info("Application exiting.")
