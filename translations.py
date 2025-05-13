# --- START OF FILE translations.py ---

# translations.py
# Contains all user-facing strings for multi-language support.

import database as db # Needs access to DB to get user language preference
from config import log
import html # For escaping potentially unsafe default values

# --- Language Data ---

language_names = {
    # Code: Display Name (in its own language ideally)
    'en': "English",
    'lt': "Lietuvių",
    'uk': "Українська",
    'pl': "Polski",
    'ru': "Русский",
}

translations = {
    'en': {
        # --- General ---
        'error_generic': "⚙️ An unexpected error occurred. Please try again later or contact support.",
        'error_invalid_input': "⚠️ Invalid input format. Please check and try again.",
        'error_invalid_action': "🚫 Action not recognized or invalid in this context.",
        'error_db': "⚙️ A database error occurred. Please contact support if this persists.",
        'error_telegram_api': "🌐 Telegram API error: {error}. This might be temporary.",
        'error_flood_wait': "⏳ Please wait {seconds} seconds before trying this action again (Telegram limit).",
        'error_timeout': "⏰ The operation timed out. Please try again.",
        'error_no_results': "ℹ️ No results found or action could not be completed.",
        'unauthorized': "🚫 You are not authorized to use this command.",
        'not_admin': "🚫 This command is only available to administrators.",
        'command_in_private_chat': "ℹ️ Please use this command in a private chat with the bot.",
        'success': "✅ Success!",
        'cancelled': "❌ Operation cancelled.",
        'not_implemented': "🚧 This feature is not yet implemented.",
        'session_expired': "🔄 Your previous action timed out or was cancelled. Please start over.",
        'state_cleared': "🔄 Action cancelled. Please restart the operation.",
        'unknown_user': "❓ I don't seem to have your active account details. Please use /start with a valid invitation code.",
        'subscription_expired': "⏳ Your subscription has expired. Please contact support to renew.",
        'subscription_expired_short': "Expired",
        'welcome_new_user': "👋 Welcome to the bot! To get started, you'll need an invitation code.",
        'ask_invitation_code': "Please send your unique invitation code to activate your account (e.g., `a565ae57`). If you are an Admin, use /admin.",
        'admin_welcome': "👑 Welcome to the Admin Panel!",
        'admin_panel_title': "👑 Admin Panel",
        'this_folder': "this folder",

        # --- Admin Buttons ---
        'admin_button_add_userbot': "➕ Add Userbot",
        'admin_button_remove_userbot': "➖ Remove Userbot",
        'admin_button_list_userbots': "🤖 List Userbots",
        'admin_button_manage_tasks': "⚙️ Manage Admin Tasks",
        'admin_button_view_tasks': "👁️ View Admin Tasks",
        'admin_button_gen_invite': "✉️ Generate Invite Code",
        'admin_button_view_subs': "👥 View Subscriptions",
        'admin_button_extend_sub': "⏳ Extend Subscription",
        'admin_button_assign_bots_client': "🔗 Assign Bots to Client",
        'admin_button_view_logs': "📜 View System Logs",

        # --- Buttons ---
        'button_back': "🔙 Back",
        'button_cancel': "❌ Cancel",
        'button_confirm': "✅ Confirm",
        'button_delete': "🗑️ Delete",
        'button_skip': "➡️ Skip",
        'button_yes': "✅ Yes",
        'button_no': "❌ No",
        'button_main_menu': "🏠 Main Menu",
        'button_admin_menu': "👑 Admin Menu",
        'button_retry': "🔄 Retry",

        # --- Pagination ---
        'pagination_prev': "⬅️ Prev",
        'pagination_next': "Next ➡️",
        'pagination_page': "Page {current}/{total}",

        # --- Start & Authentication (Client) ---
        'welcome': "👋 Welcome! Please send your unique invitation code to activate your account (e.g., `a565ae57`). If you are an Admin, use /admin.",
        'invalid_code_format': "⚠️ The code format seems incorrect (should be 8 characters, letters a-f, numbers 0-9). Please check and try again.",
        'code_not_found': "❌ Invalid invitation code. It might be incorrect or expired.",
        'code_expired': "⏳ This invitation code has expired.",
        'code_already_used': "🚫 This invitation code has already been activated by another user.",
        'user_already_active': "⚠️ You already seem to have an active account. Use /start to access the menu.",
        'activation_error': "⚙️ An error occurred during activation. Please double-check the code or contact support.",
        'activation_db_error': "⚙️ A database error occurred during activation. Please contact support.",
        'already_active': "✅ Your account is already active.",
        'activation_success': "✅ Account activated! Welcome aboard. Use /start to see your menu.",

        # --- Client Menu ---
        'client_menu_title': "<b>Client Menu</b> (Code: <code>{code}</code>)",
        'client_menu_sub_end': "Subscription ends: <code>{end_date}</code>",
        'client_menu_userbots_title': "<u>Assigned Userbots ({count}):</u>",
        'client_menu_userbot_line': "{index}. {status_icon} {display_name} (<i>Status: {status}</i>)",
        'client_menu_userbot_error': "  └─ <pre>Error: {error}</pre>",
        'client_menu_no_userbots': "You currently have no userbots assigned.",
        'client_menu_button_setup_tasks': "🚀 Setup Forwarding Tasks",
        'client_menu_button_manage_folders': "📁 Manage Group Folders",
        'client_menu_button_join_groups': "🔗 Join New Groups",
        'client_menu_button_view_joined': "👁️ View Joined Groups (per Bot)",
        'client_menu_button_stats': "📊 View Your Stats",
        'client_menu_button_language': "🌐 Set Language",

        # --- Language Selection ---
        'select_language': "Please select your preferred language:",
        'language_set': "✅ Language set to {lang_name}.",
        'language_set_error': "❌ Failed to set language.",

        # --- Userbot Action Selection (Generic) ---
        'action_select_userbot_title': "Select a Userbot",
        'action_select_userbot_prompt': "Please select the userbot for this action:",

        # --- Userbot Joining Groups ---
        'join_select_userbot': "Which userbot(s) should join the groups?",
        'join_select_userbot_all': "🤖 All Assigned Userbots",
        'join_select_userbot_active': "🟢 Only Active Userbots ({count})",
        'join_enter_group_links': ("Please send the list of group/channel links (one per line).\n\n"
                                   "<i>Examples:</i>\n"
                                   "<code>https://t.me/publicgroup</code>\n"
                                   "<code>https://t.me/joinchat/xyzabc...</code>\n"
                                   "<code>https://t.me/+xyzabc...</code>\n"
                                   "<code>@publicusername</code>"),
        'join_processing': "⏳ Processing links and attempting to join groups... Please wait.",
        'join_results_title': "<b>🔗 Group Join Results:</b>",
        'join_results_bot_header': "\n--- Userbot: {display_name} ---",
        'join_results_line': "<code>{url}</code>: {status}",
        'join_results_success': "✅ Joined",
        'join_results_already_member': "⚪ Already Member",
        'join_results_pending': "⏳ Join Request Pending (Admin Approval?)",
        'join_results_failed': "❌ Failed ({reason})",
        'join_results_flood_wait': "⏳ Flood Wait ({seconds}s)",
        'join_results_reason_invalid_invite': "invalid/expired invite link",
        'join_results_reason_private': "private/inaccessible",
        'join_results_reason_invalid_link_or_resolve': "invalid link or cannot resolve",
        'join_results_reason_chat_full': "group/channel is full",
        'join_results_reason_banned_or_restricted': "banned/restricted",
        'join_results_reason_admin_approval': "admin approval needed",
        'join_results_reason_timeout': "operation timed out",
        'join_results_reason_batch_timeout': "batch operation timed out",
        'join_results_reason_batch_error': "batch error ({error})",
        'join_results_reason_internal_error': "internal error ({error})",
        'join_no_bots': "You have no userbots assigned or available to perform this action.",
        'join_no_active_bots': "You have no <i>active</i> userbots to perform this action.",
        'join_no_links': "⚠️ No valid group links were provided in your message.",

        # --- View Joined Groups ---
        'view_joined_select_bot': "Select a userbot to view its joined groups:",
        'view_joined_fetching': "⏳ Fetching joined groups for {display_name}... This may take a while.",
        'view_joined_title': "<b>👁️ Joined Groups for {display_name}:</b>",
        'view_joined_group_public': "- <a href='{link}'>{name}</a> (<code>@{username}</code>)",
        'view_joined_group_private': "- {name} (<code>Private/ID: {id}</code>)",
        'view_joined_error': "⚙️ Error fetching joined groups for {display_name}: {error}",
        'view_joined_none': "Userbot {display_name} hasn't joined any recognizable groups or channels yet.",

        # --- Client Stats ---
        'client_stats_title': "<b>📊 Your Stats</b>",
        'client_stats_messages': "Total Messages Forwarded (All Tasks): <code>{total_sent}</code>",
        'client_stats_forwards': "Total Forward Operations Run: <code>{forwards_count}</code>",
        'client_stats_no_data': "No statistics available yet.",

        # --- Folder Management ---
        'folder_menu_title': "<b>📁 Manage Folders</b>",
        'folder_menu_create': "➕ Create New Folder",
        'folder_menu_edit': "✏️ Edit Existing Folder",
        'folder_menu_delete': "🗑️ Delete Folder",
        'folder_create_prompt': "Enter the name for the new folder:",
        'folder_create_success': "✅ Folder '<code>{name}</code>' created.",
        'folder_create_error_exists': "⚠️ A folder named '<code>{name}</code>' already exists.",
        'folder_create_error_db': "⚙️ Could not create folder due to a database error.",
        'folder_select_edit': "Select a folder to edit:",
        'folder_select_delete': "Select a folder to delete:",
        'folder_not_found_error': "❌ Folder not found or access denied.",
        'folder_no_folders': "You haven't created any folders yet.",
        'folder_edit_title': "<b>✏️ Editing Folder:</b> <code>{name}</code>",
        'folder_edit_groups_intro': "\nCurrent groups in this folder:",
        'folder_edit_no_groups': "\nThis folder currently has no groups.",
        'folder_edit_group_line': "\n- <a href='{link}'>{name}</a>", # Assumes link is always present
        'folder_edit_group_line_no_link': "\n- {name}", # Fallback if link might be missing
        'folder_edit_action_add': "➕ Add New Links",
        'folder_edit_action_remove': "➖ Remove Groups",
        'folder_edit_action_rename': "📝 Rename Folder",
        'folder_edit_add_prompt': "Send the group/channel links (one per line) to add to folder '<code>{name}</code>'. Use public links (t.me/...), private links (t.me/+...), or usernames (@...).",
        'folder_edit_remove_select': "Select groups to remove from '<code>{name}</code>':\n(Click button to toggle selection)",
        'folder_edit_remove_button': "{prefix}{text}",
        'folder_edit_remove_confirm_title': "Confirm Removal",
        'folder_edit_remove_confirm_text': "Remove {count} selected group(s) from folder '<code>{name}</code>'?", # {count} for number of groups
        'folder_edit_remove_success': "✅ {count} group(s) removed from folder '<code>{name}</code>'.",
        'folder_edit_remove_error': "⚙️ Error removing groups.",
        'folder_edit_remove_none_selected': "ℹ️ No groups were selected for removal.",
        'folder_edit_rename_prompt': "Enter the new name for folder '<code>{current_name}</code>':",
        'folder_edit_rename_success': "✅ Folder renamed to '<code>{new_name}</code>'.",
        'folder_edit_rename_error_exists': "⚠️ A folder named '<code>{new_name}</code>' already exists.",
        'folder_edit_rename_error_db': "⚙️ Database error renaming folder.",
        'folder_delete_confirm_title': "Confirm Delete Folder",
        'folder_delete_confirm': "⚠️ Are you sure you want to delete folder <b>{name}</b> and all groups within it? This cannot be undone.",
        'folder_delete_success': "✅ Folder '<code>{name}</code>' deleted.",
        'folder_delete_error': "⚙️ Failed to delete folder.",
        'folder_processing_links': "⏳ Processing provided group links... (This may take a moment if resolving info)",
        'folder_results_title': "<b>🛠️ Folder Update Results for '<code>{name}</code>':</b>",
        'folder_results_line': "<code>{link}</code>: {status}",
        'folder_results_added': "✅ Added",
        'folder_results_ignored': "⚪ Ignored (duplicate or unresolvable)",
        'folder_results_failed': "❌ Failed ({reason})",
        'folder_link_parse_error': "invalid link format",
        'folder_resolve_error': "could not resolve ID/name",
        'folder_add_db_error': "database error",

        # --- Task Setup ---
        'task_select_userbot': "🚀 Setup Forwarding Task\nSelect a userbot to configure:",
        'task_setup_title': "<b>⚙️ Task Settings for {display_name}</b>",
        'task_setup_status_line': "<code>Status:</code> {status_icon} {status_text}",
        'task_setup_primary_msg': "<code>Primary Msg:</code> {link}",
        'task_setup_fallback_msg': "<code>Fallback Msg:</code> {link}",
        'task_setup_start_time': "<code>Start Time (Local):</code> {time}",
        'task_setup_interval': "<code>Interval:</code> {interval}",
        'task_setup_target': "<code>Target:</code> {target}",
        'task_setup_last_run': "<code>Last Run (UTC):</code> {time}",
        'task_setup_last_error': "<code>Last Error:</code> <pre>{error}</pre>",
        'task_value_not_set': "<i>Not Set</i>",
        'task_value_all_groups': "🌐 All Joined Groups",
        'task_value_folder': "📁 Folder '<code>{name}</code>'",
        'task_status_active': "Active",
        'task_status_inactive': "Inactive",
        'task_status_icon_active': "🟢",
        'task_status_icon_inactive': "⚪️",
        'task_button_set_message': "✉️ Set Message Link(s)",
        'task_button_set_time': "⏰ Set Start Time",
        'task_button_set_interval': "🔁 Set Interval",
        'task_button_set_target': "🎯 Set Target",
        'task_button_toggle_status': "{action} Task", # {action} will be Activate/Deactivate
        'task_button_activate': "▶️ Activate",
        'task_button_deactivate': "⏸️ Deactivate",
        'task_button_save': "💾 Save & Exit",
        'task_prompt_primary_link': ("Send the link to the <b>primary message</b> to be forwarded.\n"
                                     "<i>Example:</i> <code>https://t.me/c/1234567890/123</code> or <code>https://t.me/channel_username/456</code>"),
        'task_prompt_fallback_link': ("Send the link to the <b>fallback message</b> (optional, used if primary fails).\n"
                                      "Send '<code>skip</code>' to not use a fallback message."),
        'task_error_invalid_link': "⚠️ Invalid message link format. Please provide a direct link to a specific message (e.g., `https://t.me/c/123.../456`).",
        'task_error_link_unreachable': "❌ Could not access the message at this link. Ensure the userbot (<code>{bot_phone}</code>) has access to the source chat/channel.",
        'task_verifying_link': "⏳ Verifying link access...",
        'task_set_success_msg': "✅ Primary message link set.",
        'task_set_success_fallback': "✅ Fallback message link set.",
        'task_set_skipped_fallback': "⚪ Fallback message skipped.",
        'task_prompt_start_time': ("Enter the <b>start time</b> for the task in your local time ({timezone_name}) using HH:MM format (e.g., <code>17:30</code> for 5:30 PM).\n"
                                   "The task will first run <i>after</i> this time each day it's due."),
        'task_error_invalid_time': "⚠️ Invalid time format. Use HH:MM (e.g., <code>09:00</code>, <code>23:15</code>).",
        'task_set_success_time': "✅ Start time set to {time} (Local Time).",
        'task_select_interval_title': "Select the repetition interval:",
        'task_interval_button': "Every {value}",
        'task_set_success_interval': "✅ Interval set to {interval}.", # {interval} like "30 min" or "1 h"
        'task_select_target_title': "Choose where to forward the messages:",
        'task_button_target_folder': "📁 Select Folder",
        'task_button_target_all': "🌐 Send to All Groups",
        'task_select_folder_title': "Select a folder for forwarding:",
        'task_error_no_folders': "⚠️ No folders found. Create one via 'Manage Folders' first, or choose 'Send to All Groups'.",
        'task_set_success_target_all': "✅ Target set to: Send to All Joined Groups.",
        'task_set_success_target_folder': "✅ Target set to: Folder '<code>{name}</code>'.",
        'task_status_toggled_success': "✅ Task status set to: <b>{status}</b>.",
        'task_save_success': "✅ Task settings for {display_name} saved.",
        'task_save_error': "⚙️ Failed to save task settings.",
        'task_save_validation_fail': "⚠️ Cannot save/activate task. Missing required settings: {missing}. Please configure them first.",
        'task_required_message': "Primary Message Link",
        'task_required_target': "Target (Folder or All Groups)",
        'task_required_start_time': "Start Time",
        'task_required_interval': "Interval",
        'task_error_no_active_bots': "⚠️ You have no <i>active</i> userbots available to run tasks.",


        # --- Admin Panel (Redundant, using admin_button_* above, but kept for structure if needed) ---
        # 'admin_button_add_userbot': "🤖 Add Userbot", (already defined)
        # ... other admin buttons ...

        # --- Admin Userbot Management ---
        'admin_userbot_prompt_phone': "Enter userbot phone number (international format, e.g., <code>+1234567890</code>):",
        'admin_userbot_prompt_api_id': "Enter API ID:",
        'admin_userbot_prompt_api_hash': "Enter API hash:",
        'admin_userbot_prompt_code': "Enter the verification code sent to <code>{phone}</code> via Telegram:",
        'admin_userbot_prompt_password': "Account <code>{phone}</code> has 2FA enabled. Enter the password:",
        'admin_userbot_invalid_phone': "❌ Invalid phone number format (must start with + and digits).",
        'admin_userbot_invalid_api_id': "❌ API ID must be a positive number.",
        'admin_userbot_invalid_api_hash': "❌ API Hash seems invalid (usually a long hexadecimal string).",
        'admin_userbot_already_exists': "ℹ️ Userbot <code>{phone}</code> already exists in DB. Attempting re-authentication/status check...",
        'admin_userbot_auth_connecting': "⏳ Connecting to Telegram for <code>{phone}</code>...",
        'admin_userbot_auth_sending_code': "⏳ Requesting login code for <code>{phone}</code>...",
        'admin_userbot_auth_code_sent': "✅ Code sent. Please enter it now.", # Unused if start_auth sends directly
        'admin_userbot_auth_signing_in': "⏳ Signing in <code>{phone}</code>...", # Unused if complete_auth handles it
        'admin_userbot_add_success': "✅ Userbot {display_name} added and authenticated!", # More general after successful auth+DB
        'admin_userbot_auth_success': "✅ Userbot {display_name} authenticated!",
        'admin_userbot_already_auth': "✅ Userbot {display_name} is already authorized and active.",
        'admin_userbot_auth_error_connect': "❌ Connection Error for <code>{phone}</code>: {error}",
        'admin_userbot_auth_error_auth': "❌ Authentication Error for <code>{phone}</code>: {error}", # Generic auth error
        'admin_userbot_auth_error_flood': "❌ Flood Wait for <code>{phone}</code>: Try again in {seconds} seconds.",
        'admin_userbot_auth_error_config': "❌ Configuration Error for <code>{phone}</code> (Invalid API ID/Hash?): {error}",
        'admin_userbot_auth_error_phone_invalid': "❌ Telegram rejected the phone number <code>{phone}</code>.",
        'admin_userbot_auth_error_code_invalid': "❌ Invalid or expired verification code for <code>{phone}</code>.",
        'admin_userbot_auth_error_password_invalid': "❌ Incorrect password for <code>{phone}</code>.",
        'admin_userbot_auth_error_password_needed_unexpected': "🔒 Password needed, but wasn't expected. Please restart the process.",
        'admin_userbot_auth_error_account_issue': "❌ Account issue for <code>{phone}</code> (Banned? Deactivated?): {error}",
        'admin_userbot_auth_error_unknown': "❌ An unexpected error occurred during authentication for <code>{phone}</code>: {error}",
        'admin_userbot_select_remove': "Select the userbot to remove:",
        'admin_userbot_no_bots_to_remove': "No userbots have been added yet.",
        'admin_userbot_not_found': "❌ Userbot not found in database.",
        'admin_userbot_remove_confirm_title': "Confirm Removal",
        'admin_userbot_remove_confirm_text': "Are you sure you want to remove userbot {display_name}?\nThis will delete its session and tasks.",
        'admin_userbot_remove_success': "✅ Userbot {display_name} removed.",
        'admin_userbot_remove_error': "⚙️ Failed to remove userbot.",
        'admin_userbot_list_title': "<b>📋 Registered Userbots:</b>",
        'admin_userbot_list_line': "{status_icon} {display_name} (<code>{phone}</code>) | Client: {client_code} | Status: <code>{status}</code>",
        'admin_userbot_list_status_icon_active': "🟢",
        'admin_userbot_list_status_icon_inactive': "⚪️",
        'admin_userbot_list_status_icon_error': "🔴",
        'admin_userbot_list_status_icon_connecting': "🔌",
        'admin_userbot_list_status_icon_needs_code': "🔢",
        'admin_userbot_list_status_icon_needs_password': "🔒",
        'admin_userbot_list_status_icon_authenticating': "⏳",
        'admin_userbot_list_status_icon_initializing': "⚙️",
        'admin_userbot_list_status_icon_unknown': "❓",
        'admin_userbot_list_unassigned': "<i>Unassigned</i>",
        'admin_userbot_list_error_line': "  └─ <pre>Error: {error}</pre>",
        'admin_userbot_list_no_bots': "No userbots have been added yet.",

        # --- Admin Invite Management ---
        'admin_invite_prompt_details': "Enter the number of days for the subscription (e.g., '30' for 30 days):",
        'admin_invite_invalid_days': "⚠️ Please enter a valid positive number of days (e.g., '30').",
        'admin_invite_generated': "✅ Invite code generated:\nCode: <code>{code}</code>\nDuration: {days} days",
        'admin_invite_db_error': "⚙️ Failed to generate invite code. Please try again.",

        # --- Admin Subscription Management ---
        'admin_subs_title': "<b>📄 Active Subscriptions:</b>",
        'admin_subs_line': "User: {user_link} | Code: <code>{code}</code> | Ends: {end_date} | Bots: {bot_count}",
        'admin_subs_no_user': "<i>Not Activated Yet</i>",
        'admin_subs_error': "⚙️ Could not retrieve subscriptions.",
        'admin_subs_none': "No active client subscriptions found.",
        'admin_extend_prompt_code': "Enter the client's invitation code to extend their subscription:",
        'admin_extend_invalid_code': "❌ Invitation code not found.",
        'admin_extend_prompt_days': "Current subscription for code <code>{code}</code> ends: {end_date}.\nEnter number of days to extend:",
        'admin_extend_invalid_days': "❌ Please enter a positive number of days.",
        'admin_extend_success': "✅ Subscription for code <code>{code}</code> extended by {days} days. New end date: {new_end_date}",
        'admin_extend_db_error': "⚙️ Failed to update subscription in database.",
        'admin_assignbots_prompt_code': "Enter the client's invitation code to assign userbots:",
        'admin_assignbots_invalid_code': "❌ Invitation code not found.",
        'admin_assignbots_prompt_count': "Client <code>{code}</code> currently has {current_count} bot(s).\nEnter the number of <i>additional</i> active, unassigned userbots to assign:",
        'admin_assignbots_invalid_count': "❌ Please enter a positive number of userbots.",
        'admin_assignbots_no_bots_available': "❌ Not enough available (active & unassigned) userbots ({needed} required, {available} found).",
        'admin_assignbots_success': "✅ Assigned {count} userbots to client <code>{code}</code>.",
        'admin_assignbots_partial_success': "⚠️ Assigned {assigned_count}/{requested_count} userbots to client <code>{code}</code>. Some may have been already assigned or not found.",
        'admin_assignbots_db_error': "⚙️ Failed to assign userbots in database.",
        'admin_assignbots_failed': "❌ Failed to assign any userbots to client <code>{code}</code>. Check bot availability and logs.",

        # --- Admin Logs ---
        'admin_logs_title': "<b>📜 Recent System Logs (Last {limit}):</b>",
        'admin_logs_line': "<code>{time}</code> | {event} | User: <code>{user}</code> | Bot: <code>{bot}</code> | {details}",
        'admin_logs_user_admin': "Admin",
        'admin_logs_user_none': "System",
        'admin_logs_bot_none': "-",
        'admin_logs_fetch_error': "⚙️ Could not retrieve logs.",
        'admin_logs_none': "No logs recorded yet.",

        # --- Generic Fallback/Error in Conversation ---
        'conversation_fallback': "❓ Unrecognized command or input in the current context. Action cancelled. Please start again using /start or /admin.",
        'internal_error_log': "An internal error occurred in state {state}. User: {user_id}. Error: {error}",

        # --- Admin Task Management ---
        'admin_task_menu_title': '⚙️ Manage Admin Tasks',
        'admin_task_view': '👁️ View Tasks',
        'admin_task_create': '➕ Create New Task',
        'admin_task_list_title': '📋 Admin Task List',
        'admin_task_list_empty': 'No admin tasks configured yet.',
        'admin_task_details': "<b>Task #{task_id} Details</b>\nStatus: {status_icon} {status_text}\nBot: {phone}\nTarget: {target}\nSchedule: {schedule}\nMessage: <pre>{message_preview}</pre>\nLast Run: {last_run}\nNext Run: {next_run}",
        'admin_task_options_title': "Task Options:",
        'admin_task_activate': "▶️ Activate Task",
        'admin_task_deactivate': "⏸️ Deactivate Task",
        'admin_task_delete': "🗑️ Delete Task", # Generic button text
        'admin_task_toggled': "✅ Task status toggled.",
        'admin_task_deleted': "✅ Task deleted.",
        'admin_task_not_found': "❌ Task not found.",
        'admin_task_select_bot': '🤖 Select a userbot for this task:',
        'admin_task_no_bots': '⚠️ No active userbots available. Please add and activate a userbot first.',
        'admin_task_enter_message': 'Enter the message link or text to be posted:',
        'admin_task_enter_schedule': 'Enter the schedule in cron format (e.g., "0 9 * * *" for daily at 9 AM UTC):',
        'admin_task_invalid_schedule': '⚠️ Invalid schedule format. Please use cron format (e.g., "0 9 * * *").',
        'admin_task_enter_target': 'Enter the target group/channel username (e.g., @channelname) or ID (e.g., -100123...):',
        'admin_task_invalid_target': '⚠️ Invalid target format. Please enter a username or ID.',
        'admin_task_invalid_link': '⚠️ Invalid message link/text provided.',
        'admin_task_created': '✅ Admin task created successfully!',
        'admin_task_error': '⚠️ An error occurred while managing the admin task. Please check logs.',
        'admin_task_delete_button': "🗑️ Delete This Task", # More specific button text for confirm
    },
    # --- Lithuanian Translations ---
    'lt': {
        'error_generic': "⚙️ Įvyko netikėta klaida. Bandykite vėliau arba kreipkitės pagalbos.",
        'unauthorized': "🚫 Jūs neturite teisės vykdyti šios komandos.",
        'button_back': "🔙 Atgal",
        'ask_invitation_code': "Norėdami aktyvuoti paskyrą, įveskite savo unikalų pakvietimo kodą (pvz., `a565ae57`). Jei esate administratorius, naudokite /admin.",
        'select_language': "Pasirinkite norimą kalbą:",
        'language_set': "✅ Kalba pakeista į {lang_name}.",
        # ... (ALL OTHER KEYS NEED TO BE TRANSLATED) ...
    },
    # --- Ukrainian Translations ---
    'uk': {
        'error_generic': "⚙️ Сталася неочікувана помилка. Спробуйте пізніше або зверніться до підтримки.",
        'unauthorized': "🚫 У вас немає доступу до цієї команди.",
        'button_back': "🔙 Назад",
        'ask_invitation_code': "Будь ласка, надішліть свій унікальний код запрошення для активації облікового запису (наприклад, `a565ae57`). Якщо ви адміністратор, використовуйте /admin.",
        'select_language': "Будь ласка, оберіть бажану мову:",
        'language_set': "✅ Мову встановлено на {lang_name}.",
        # ... (ALL OTHER KEYS NEED TO BE TRANSLATED) ...
    },
    # --- Polish Translations ---
    'pl': {
        'error_generic': "⚙️ Wystąpił nieoczekiwany błąd. Spróbuj ponownie później lub skontaktuj się z pomocą techniczną.",
        'unauthorized': "🚫 Nie masz uprawnień do użycia tej komendy.",
        'button_back': "🔙 Wstecz",
        'ask_invitation_code': "Aby aktywować konto, wprowadź swój unikalny kod zaproszenia (np. `a565ae57`). Jeśli jesteś administratorem, użyj /admin.",
        'select_language': "Proszę wybrać preferowany język:",
        'language_set': "✅ Język ustawiony na {lang_name}.",
        # ... (ALL OTHER KEYS NEED TO BE TRANSLATED) ...
    },
    # --- Russian Translations ---
    'ru': {
        'error_generic': "⚙️ Произошла непредвиденная ошибка. Пожалуйста, попробуйте позже или свяжитесь со службой поддержки.",
        'unauthorized': "🚫 У вас нет прав для выполнения этой команды.",
        'button_back': "🔙 Назад",
        'ask_invitation_code': "Пожалуйста, отправьте ваш уникальный код приглашения для активации учетной записи (например, `a565ae57`). Если вы администратор, используйте /admin.",
        'select_language': "Пожалуйста, выберите предпочитаемый язык:",
        'language_set': "✅ Язык установлен на {lang_name}.",
        # ... (ALL OTHER KEYS NEED TO BE TRANSLATED) ...
    }
}

# --- Function to Get Text ---
def get_text(user_id, key, lang_override=None, default_text=None, **kwargs):
    """
    Retrieves translated text based on user's language preference or an override.
    Uses English as a fallback if the key is missing in the target language.
    Formats the string with provided kwargs.
    """
    lang_to_use = 'en' # Default language

    if lang_override and lang_override in translations:
        lang_to_use = lang_override
    elif user_id is not None and user_id != 0:
        try:
            user_db_lang = db.get_user_language(user_id)
            if user_db_lang and user_db_lang in translations:
                lang_to_use = user_db_lang
        except Exception as e:
            log.error(f"Failed to get language for user {user_id} from DB: {e}")
            # lang_to_use remains 'en'
    
    if lang_to_use not in translations: # Should not happen if logic above is correct
        log.warning(f"Language '{lang_to_use}' not found in translations. Falling back to 'en'.")
        lang_to_use = 'en'

    selected_lang_dict = translations.get(lang_to_use, {}) # Default to empty dict if lang somehow invalid
    english_dict = translations.get('en', {}) # English fallback always available

    text_template = ""
    if key in selected_lang_dict:
        text_template = selected_lang_dict[key]
    elif key in english_dict:
        text_template = english_dict[key]
        if lang_to_use != 'en': 
            log.debug(f"Translation key '{key}' not found for lang '{lang_to_use}', using English fallback.")
    else:
        log.warning(f"Translation key '{key}' not found in '{lang_to_use}' or English fallback.")
        text_template = default_text if default_text is not None else f"KEY_NOT_FOUND:_{html.escape(key)}"

    # Format the string
    try:
        return text_template.format(**kwargs)
    except KeyError as e_format:
        # This means a placeholder in the template (e.g. {name}) was not provided in kwargs
        log.error(f"Formatting error for key='{key}' lang='{lang_to_use}': Missing placeholder value for '{e_format}'. Kwargs provided: {kwargs}. Template was: '{text_template}'")
        # Return the unformatted template with a warning prefix or just the template
        return f"[FORMAT_ERR] {text_template}" 
    except Exception as e_general_format:
        log.error(f"Unexpected formatting error for key='{key}' lang='{lang_to_use}': {e_general_format}. Template: '{text_template}', Kwargs: {kwargs}")
        return text_template # Return unformatted template

log.info("Translations module loaded with updated keys.")
# --- END OF FILE translations.py ---
