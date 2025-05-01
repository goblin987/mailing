# translations.py
# Contains all user-facing strings for multi-language support.

import database as db # Needs access to DB to get user language preference
from config import log

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
        'error_db': "⚙️ A database error occurred. Please contact support if this persists.",
        'error_telegram_api': "🌐 Telegram API error: {error}. This might be temporary.",
        'error_flood_wait': "⏳ Please wait {seconds} seconds before trying this action again (Telegram limit).",
        'error_timeout': "⏰ The operation timed out. Please try again.",
        'unauthorized': "🚫 You are not authorized to use this command.",
        'command_in_private_chat': "ℹ️ Please use this command in a private chat with the bot.",
        'success': "✅ Success!",
        'cancelled': "❌ Operation cancelled.",
        'not_implemented': "🚧 This feature is not yet implemented.",
        'session_expired': "🔄 Your previous action timed out or was cancelled. Please start over.",
        'state_cleared': "🔄 Action cancelled. Please restart the operation.", # Slightly different message
        'unknown_user': "❓ I don't seem to have your active account details. Please use /start with a valid invitation code.",

        # --- Buttons ---
        'button_back': "🔙 Back",
        'button_cancel': "❌ Cancel",
        'button_confirm': "✅ Confirm",
        'button_delete': "🗑️ Delete",
        'button_skip': "➡️ Skip",
        'button_yes': "✅ Yes",
        'button_no': "❌ No",
        'button_main_menu': "🏠 Main Menu",

        # --- Start & Authentication (Client) ---
        'welcome': "👋 Welcome! Please send your unique invitation code to activate your account (e.g., a565ae57).",
        'invalid_code_format': "⚠️ The code format seems incorrect. Please check and try again.",
        'code_not_found': "❌ Invalid invitation code. It might be incorrect, already used, or expired.",
        'code_expired': "⏳ This invitation code has expired.",
        'code_already_used': "🚫 This invitation code has already been activated by another user.",
        'user_already_active': "⚠️ You already seem to have an active account. Use /start to access the menu.",
        'activation_error': "⚙️ An error occurred during activation. Please double-check the code or contact support.",
        'activation_db_error': "⚙️ A database error occurred during activation. Please contact support.",
        'already_active': "✅ Your account is already active.", # Used when activation is tried again
        'activation_success': "✅ Account activated! Welcome aboard. Use /start again to see your menu.",

        # --- Client Menu ---
        'client_menu_title': "**Client Menu** (Code: `{code}`)",
        'client_menu_sub_end': "Subscription ends: `{end_date}`",
        'client_menu_userbots_title': "Assigned Userbots ({count}):",
        'client_menu_userbot_line': "{index}. {display_name} (`Status: {status}`)", # Added status formatting
        'client_menu_userbot_error': "  └─ `Last Error: {error}`", # Display last known error
        'client_menu_no_userbots': "You currently have no userbots assigned.",
        'client_menu_button_setup_tasks': "🚀 Setup Forwarding Tasks",
        'client_menu_button_manage_folders': "📁 Manage Group Folders",
        'client_menu_button_join_groups': "🔗 Join New Groups",
        'client_menu_button_view_joined': "👁️ View Joined Groups (per Bot)",
        'client_menu_button_logs': "📊 View Your Stats", # Changed label
        'client_menu_button_language': "🌐 Set Language",

        # --- Language Selection ---
        'select_language': "Please select your preferred language:",
        'language_set': "✅ Language set to {lang_name}.",
        'language_set_error': "❌ Failed to set language.",

        # --- Userbot Action Selection (Generic) ---
        'action_select_userbot_title': "Select a Userbot", # Generic title, specific ones below override
        'action_select_userbot_prompt': "Please select the userbot for this action:", # Generic prompt

        # --- Userbot Joining Groups ---
        'join_select_userbot': "Which userbot should join the groups?", # Specific prompt for join
        'join_select_userbot_all': "🤖 All Assigned Userbots",
        'join_enter_group_links': "Please send the list of group links (one per line).\n\n*Examples:*\n`https://t.me/publicgroup`\n`https://t.me/joinchat/xyzabc...`\n`https://t.me/+xyzabc...`",
        'join_processing': "⏳ Processing links and attempting to join groups... Please wait.",
        'join_results_title': "**🔗 Group Join Results:**",
        'join_results_bot_header': "\n--- Userbot: {display_name} ---",
        'join_results_line': "`{url}`: {status}", # Use markdown for URL emphasis
        'join_results_success': "✅ Joined",
        'join_results_already_member': "⚪ Already Member",
        'join_results_pending': "⏳ Join Request Pending",
        'join_results_failed': "❌ Failed ({reason})", # Provide reason
        'join_results_reason_private': "private/inaccessible",
        'join_results_reason_invalid': "invalid link/ID",
        'join_results_reason_banned': "banned",
        'join_results_reason_timeout': "timeout",
        'join_results_reason_flood': "flood wait ({seconds}s)",
        'join_results_reason_internal': "internal error ({error})", # Catch-all for unexpected
        'join_no_bots': "You have no userbots assigned to perform this action.",
        'join_no_links': "⚠️ No valid group links were provided in your message.",

        # --- View Joined Groups ---
        'view_joined_select_bot': "Select a userbot to view its joined groups:",
        'view_joined_fetching': "⏳ Fetching joined groups for {display_name}... This may take a while.",
        'view_joined_title': "**👁️ Joined Groups for {display_name}:**",
        'view_joined_group_public': "- [{name}](https://t.me/{username}) (`@{username}`)", # Markdown link if public
        'view_joined_group_private': "- {name} (`Private/ID: {id}`)",
        'view_joined_error': "⚙️ Error fetching joined groups for {display_name}: {error}",
        'view_joined_none': "Userbot {display_name} hasn't joined any recognizable groups or channels yet.",

        # --- Client Stats ---
        'client_stats_title': "**📊 Your Stats**",
        'client_stats_messages': "Total Messages Forwarded: `{total_sent}`",
        'client_stats_forwards': "Total Forward Operations: `{forwards_count}`", # Number of task runs?
        'client_stats_no_data': "No statistics available yet.",

        # --- Folder Management ---
        'folder_menu_title': "**📁 Manage Folders**",
        'folder_menu_create': "➕ Create New Folder",
        'folder_menu_edit': "✏️ Edit Existing Folder",
        'folder_menu_delete': "🗑️ Delete Folder",
        'folder_create_prompt': "Enter the name for the new folder:",
        'folder_create_success': "✅ Folder '{name}' created.",
        'folder_create_error_exists': "⚠️ A folder with this name already exists.",
        'folder_create_error_db': "⚙️ Could not create folder due to a database error.",
        'folder_select_edit': "Select a folder to edit:",
        'folder_select_delete': "Select a folder to delete:",
        'folder_no_folders': "You haven't created any folders yet.",
        'folder_edit_title': "**✏️ Editing Folder: `{name}`**",
        'folder_edit_groups_intro': "\nCurrent groups in this folder:",
        'folder_edit_no_groups': "\nThis folder currently has no groups.",
        'folder_edit_group_line': "\n- `{link}`", # Group links in code block
        'folder_edit_action_update': "🔄 Replace Group List",
        'folder_edit_action_add': "➕ Add New Links",
        'folder_edit_action_remove': "➖ Remove Groups",
        'folder_edit_action_rename': "📝 Rename Folder",
        'folder_edit_action_delete': "🗑️ Delete This Folder", # Moved delete action here
        'folder_edit_update_prompt': "⚠️ *This will replace all existing groups in this folder.*\nSend the complete new list of group links (one per line) for folder `{name}`:",
        'folder_edit_add_prompt': "Send the additional group links (one per line) to add to folder `{name}`:",
        'folder_edit_remove_select': "Select groups to remove from `{name}`:",
        'folder_edit_remove_button': "{link}", # Show the link on the button
        'folder_edit_remove_confirm_title': "Confirm Removal",
        'folder_edit_remove_confirm_text': "Remove {count} selected group(s) from folder '{name}'?",
        'folder_edit_remove_success': "✅ {count} group(s) removed.",
        'folder_edit_remove_error': "⚙️ Error removing groups.",
        'folder_edit_remove_none_selected': "ℹ️ No groups were selected for removal.",
        'folder_edit_rename_prompt': "Enter the new name for folder `{current_name}`:",
        'folder_edit_rename_success': "✅ Folder renamed to '{new_name}'.",
        'folder_edit_rename_error_exists': "⚠️ A folder named '{new_name}' already exists.",
        'folder_edit_rename_error_db': "⚙️ Database error renaming folder.",
        'folder_delete_confirm_title': "Confirm Delete Folder",
        'folder_delete_confirm': "⚠️ Are you sure you want to delete folder **{name}** and all groups within it? This cannot be undone.",
        'folder_delete_success': "✅ Folder '{name}' deleted.",
        'folder_delete_error': "⚙️ Failed to delete folder.",
        'folder_processing_links': "⏳ Processing provided group links...",
        'folder_results_title': "**🛠️ Folder Update Results for '{name}':**",
        'folder_results_line': "`{link}`: {status}",
        'folder_results_added': "✅ Added",
        'folder_results_ignored': "⚪ Ignored (already exists or invalid ID)",
        'folder_results_failed': "❌ Failed to add (reason: {reason})",
        'folder_link_parse_error': "invalid link format",
        'folder_add_db_error': "database error",

        # --- Task Setup ---
        'task_select_userbot': "🚀 Setup Forwarding Task\nSelect a userbot to configure:",
        'task_setup_title': "**⚙️ Task Settings for {display_name}**",
        'task_setup_status_line': "`Status:` {status_icon} {status_text}",
        'task_setup_primary_msg': "`Primary Msg:` {link}",
        'task_setup_fallback_msg': "`Fallback Msg:` {link}",
        'task_setup_start_time': "`Start Time (Lithuania):` {time}",
        'task_setup_interval': "`Interval:` {interval}",
        'task_setup_target': "`Target:` {target}",
        'task_setup_last_run': "`Last Run:` {time}",
        'task_setup_last_error': "`Last Error:` {error}",
        'task_value_not_set': "Not Set",
        'task_value_all_groups': "All Joined Groups",
        'task_value_folder': "Folder '{name}'",
        'task_status_active': "Active",
        'task_status_inactive': "Inactive",
        'task_status_icon_active': "🟢",
        'task_status_icon_inactive': "⚪️",
        'task_button_set_message': "✉️ Set Message Link(s)",
        'task_button_set_time': "⏰ Set Start Time",
        'task_button_set_interval': "🔁 Set Interval",
        'task_button_set_target': "🎯 Set Target",
        'task_button_toggle_status': "{action} Task", # Format with Activate/Deactivate
        'task_button_activate': "▶️ Activate",
        'task_button_deactivate': "⏸️ Deactivate",
        'task_button_save': "💾 Save", # Removed 'and activate' for clarity
        'task_prompt_primary_link': "Send the link to the **primary message** to be forwarded (e.g., `https://t.me/c/12345/678`).",
        'task_prompt_fallback_link': "Send the link to the **fallback message** (optional, used if primary fails due to media restrictions).\nType 'skip' or send the link.",
        'task_error_invalid_link': "⚠️ Invalid message link format or message not accessible by the userbot. Please provide a direct link to a specific message.",
        'task_error_link_unreachable': "❌ Could not access the message at this link. Ensure the userbot has access to the source chat/channel.",
        'task_set_success_msg': "✅ Primary message link set.",
        'task_set_success_fallback': "✅ Fallback message link set.",
        'task_set_skipped_fallback': "⚪ Fallback message skipped.",
        'task_prompt_start_time': "Enter the **start time** for the task in Lithuanian time (HH:MM format, e.g., 17:30). The task will first run *after* this time each day it's due.",
        'task_error_invalid_time': "⚠️ Invalid time format. Use HH:MM (e.g., 09:00, 23:15).",
        'task_set_success_time': "✅ Start time set to {time} (Lithuanian Time).",
        'task_select_interval_title': "Select the repetition interval:",
        'task_interval_button': "Every {value}", # Generic button text
        'task_set_success_interval': "✅ Interval set to {interval}.",
        'task_select_target_title': "Choose where to forward the messages:",
        'task_button_target_folder': "📁 Select Folder",
        'task_button_target_all': "🌐 Send to All Groups",
        'task_select_folder_title': "Select a folder for forwarding:",
        'task_error_no_folders': "⚠️ No folders found. Create one via 'Manage Folders' first, or choose 'Send to All Groups'.",
        'task_set_success_target_all': "✅ Target set to: Send to All Joined Groups.",
        'task_set_success_target_folder': "✅ Target set to: Folder '{name}'.",
        'task_status_toggled_success': "✅ Task status set to: **{status}**.",
        'task_save_success': "✅ Task settings for {display_name} saved.",
        'task_save_error': "⚙️ Failed to save task settings.",
        'task_save_validation_fail': "⚠️ Cannot save/activate task. Missing required settings: {missing}. Please configure them first.",
        'task_required_message': "Primary Message",
        'task_required_target': "Target Folder or All Groups",
        'task_required_start_time': "Start Time",
        'task_required_interval': "Interval",

        # --- Admin Panel ---
        'admin_panel_title': "**👑 Admin Panel**",
        'admin_button_add_userbot': "🤖 Add Userbot",
        'admin_button_remove_userbot': "🗑️ Remove Userbot",
        'admin_button_list_userbots': "📋 List Userbots",
        'admin_button_gen_invite': "🎟️ Generate Invite Code",
        'admin_button_view_subs': "📄 View Subscriptions",
        'admin_button_view_logs': "📜 View System Logs",
        'admin_button_extend_sub': "⏳ Extend Subscription",
        'admin_button_assign_bots_client': "➕ Assign Userbots to Client",

        # --- Admin Userbot Management ---
        'admin_userbot_prompt_phone': "Enter userbot phone number (international format, e.g., `+1234567890`):",
        'admin_userbot_prompt_api_id': "Enter API ID:",
        'admin_userbot_prompt_api_hash': "Enter API hash:",
        'admin_userbot_prompt_code': "Enter the verification code sent to `{phone}` via Telegram:",
        'admin_userbot_prompt_password': "Account `{phone}` has 2FA enabled. Enter the password:",
        'admin_userbot_invalid_phone': "❌ Invalid phone number format.",
        'admin_userbot_invalid_api_id': "❌ API ID must be a positive number.",
        'admin_userbot_invalid_api_hash': "❌ API Hash seems invalid (usually a long string).",
        'admin_userbot_already_exists': "ℹ️ Userbot `{phone}` already exists. Starting authentication process...",
        'admin_userbot_auth_connecting': "⏳ Connecting to Telegram for `{phone}`...",
        'admin_userbot_auth_sending_code': "⏳ Requesting login code for `{phone}`...",
        'admin_userbot_auth_code_sent': "✅ Code sent. Please enter it now.",
        'admin_userbot_auth_signing_in': "⏳ Signing in `{phone}`...",
        'admin_userbot_add_success': "✅ Userbot {display_name} added and authenticated!",
        'admin_userbot_auth_success': "✅ Userbot {display_name} authenticated!",
        'admin_userbot_already_auth': "✅ Userbot {display_name} is already authorized.",
        'admin_userbot_auth_error_connect': "❌ Connection Error for `{phone}`: {error}",
        'admin_userbot_auth_error_auth': "❌ Authentication Error for `{phone}`: {error}",
        'admin_userbot_auth_error_flood': "❌ Flood Wait for `{phone}`: Try again in {seconds} seconds.",
        'admin_userbot_auth_error_config': "❌ Configuration Error for `{phone}` (Invalid API ID/Hash?): {error}",
        'admin_userbot_auth_error_phone_invalid': "❌ Telegram rejected the phone number `{phone}`.",
        'admin_userbot_auth_error_code_invalid': "❌ Invalid or expired verification code for `{phone}`.",
        'admin_userbot_auth_error_password_invalid': "❌ Incorrect password for `{phone}`.",
        'admin_userbot_auth_error_account_issue': "❌ Account issue for `{phone}` (Banned? Deactivated?): {error}",
        'admin_userbot_auth_error_unknown': "❌ An unexpected error occurred during authentication for `{phone}`: {error}",
        'admin_userbot_select_remove': "Select the userbot to remove:",
        'admin_userbot_no_bots_to_remove': "No userbots have been added yet.",
        'admin_userbot_remove_confirm_title': "Confirm Removal",
        'admin_userbot_remove_confirm_text': "Are you sure you want to remove userbot {display_name}?\nThis will delete its session and tasks.",
        'admin_userbot_remove_success': "✅ Userbot {display_name} removed.",
        'admin_userbot_remove_error': "⚙️ Failed to remove userbot.",
        'admin_userbot_list_title': "**📋 Registered Userbots:**",
        'admin_userbot_list_line': "{status_icon} {display_name} `({phone})` | Client: {client_code} | Status: `{status}`",
        'admin_userbot_list_status_icon_active': "🟢",
        'admin_userbot_list_status_icon_inactive': "⚪️",
        'admin_userbot_list_status_icon_error': "🔴",
        'admin_userbot_list_status_icon_auth': "🟡", # needs_code, needs_password, authenticating
        'admin_userbot_list_unassigned': "`Unassigned`",
        'admin_userbot_list_error_line': "  └─ `Error: {error}`",
        'admin_userbot_list_no_bots': "No userbots have been added yet.",

        # --- Admin Invite Management ---
        'admin_invite_prompt_details': "Enter subscription details:\nFormat: `<days>d <bots>b`\nExample: `30d 2b` (for 30 days, 2 bots)",
        'admin_invite_invalid_format': "❌ Invalid format. Use: `<days>d <bots>b` (e.g., 30d 2b)",
        'admin_invite_invalid_numbers': "❌ Days and bot count must be positive numbers.",
        'admin_invite_no_bots_available': "❌ Not enough available (active & unassigned) userbots ({needed} required, {available} found). Add more userbots first.",
        'admin_invite_generating': "⏳ Generating code...",
        'admin_invite_success': "✅ Invitation code created:\n`{code}`\n(Expires: {end_date}, For: {count} bots)",
        'admin_invite_db_error': "⚙️ Failed to save invitation code to database.",

        # --- Admin Subscription Management ---
        'admin_subs_title': "**📄 Active Subscriptions:**",
        'admin_subs_line': "User: {user_link} | Code: `{code}` | Ends: {end_date} | Bots: {bot_count}",
        'admin_subs_no_user': "`Not Activated Yet`",
        'admin_subs_error': "⚙️ Could not retrieve subscriptions.",
        'admin_subs_none': "No active client subscriptions found.",
        'admin_extend_prompt_code': "Enter the client's activation code to extend their subscription:",
        'admin_extend_invalid_code': "❌ Invitation code not found.",
        'admin_extend_prompt_days': "Current subscription for code `{code}` ends: {end_date}.\nEnter number of days to extend:",
        'admin_extend_invalid_days': "❌ Please enter a positive number of days.",
        'admin_extend_success': "✅ Subscription for code `{code}` extended by {days} days. New end date: {new_end_date}",
        'admin_extend_db_error': "⚙️ Failed to update subscription in database.",
        'admin_assignbots_prompt_code': "Enter the client's activation code to assign userbots:",
        'admin_assignbots_invalid_code': "❌ Invitation code not found.",
        'admin_assignbots_prompt_count': "Client `{code}` currently has {current_count} bot(s).\nEnter the number of *additional* active, unassigned userbots to assign:",
        'admin_assignbots_invalid_count': "❌ Please enter a positive number of userbots.",
        'admin_assignbots_no_bots_available': "❌ Not enough available userbots ({needed} required, {available} found).",
        'admin_assignbots_success': "✅ Assigned {count} userbots to client `{code}`.",
        'admin_assignbots_db_error': "⚙️ Failed to assign userbots in database.",

        # --- Admin Logs ---
        'admin_logs_title': "**📜 Recent System Logs (Last {limit}):**",
        'admin_logs_line': "`{time}` | {event} | User: `{user}` | Bot: `{bot}` | {details}",
        'admin_logs_user_admin': "Admin",
        'admin_logs_user_none': "System",
        'admin_logs_bot_none': "-",
        'admin_logs_fetch_error': "⚙️ Could not retrieve logs.",
        'admin_logs_none': "No logs recorded yet.",

        # --- Generic Fallback/Error in Conversation ---
        'conversation_fallback': "Action cancelled or session expired. Please start again.",
        'internal_error_log': "An internal error occurred in state {state}. User: {user_id}. Error: {error}",

    },
    # --- Lithuanian Translations ---
    'lt': {
        # --- General ---
        'error_generic': "⚙️ Įvyko netikėta klaida. Bandykite dar kartą vėliau arba susisiekite su pagalba.",
        'error_invalid_input': "⚠️ Netinkamas įvesties formatas. Patikrinkite ir bandykite dar kartą.",
        'error_db': "⚙️ Įvyko duomenų bazės klaida. Susisiekite su pagalba, jei tai kartosis.",
        'error_telegram_api': "🌐 Telegram API klaida: {error}. Tai gali būti laikina.",
        'error_flood_wait': "⏳ Palaukite {seconds} sek. prieš bandydami šį veiksmą dar kartą (Telegram limitas).",
        'error_timeout': "⏰ Operacija viršijo laukimo laiką. Bandykite dar kartą.",
        'unauthorized': "🚫 Jūs neturite teisės naudoti šios komandos.",
        'command_in_private_chat': "ℹ️ Prašome naudoti šią komandą privačiame pokalbyje su botu.",
        'success': "✅ Pavyko!",
        'cancelled': "❌ Veiksmas atšauktas.",
        'not_implemented': "🚧 Ši funkcija dar kuriama.",
        'session_expired': "🔄 Jūsų ankstesnis veiksmas pasibaigė arba buvo atšauktas. Pradėkite iš naujo.",
        'state_cleared': "🔄 Veiksmas atšauktas. Pradėkite operaciją iš naujo.",
        'unknown_user': "❓ Atrodo, neturiu jūsų aktyvios paskyros duomenų. Naudokite /start su galiojančiu kvietimo kodu.",

        # --- Buttons ---
        'button_back': "🔙 Atgal",
        'button_cancel': "❌ Atšaukti",
        'button_confirm': "✅ Patvirtinti",
        'button_delete': "🗑️ Ištrinti",
        'button_skip': "➡️ Praleisti",
        'button_yes': "✅ Taip",
        'button_no': "❌ Ne",
        'button_main_menu': "🏠 Pagrindinis Meniu",

        # --- Start & Authentication (Client) ---
        'welcome': "👋 Sveiki! Norėdami aktyvuoti paskyrą, atsiųskite savo unikalų kvietimo kodą (pvz., a565ae57).",
        'invalid_code_format': "⚠️ Kodo formatas atrodo neteisingas. Patikrinkite ir bandykite dar kartą.",
        'code_not_found': "❌ Netinkamas kvietimo kodas. Jis gali būti neteisingas, jau panaudotas arba pasibaigęs.",
        'code_expired': "⏳ Šio kvietimo kodo galiojimas baigėsi.",
        'code_already_used': "🚫 Šį kvietimo kodą jau aktyvavo kitas vartotojas.",
        'user_already_active': "⚠️ Atrodo, kad jau turite aktyvią paskyrą. Naudokite /start, kad pasiektumėte meniu.",
        'activation_error': "⚙️ Aktyvacijos metu įvyko klaida. Patikrinkite kodą arba susisiekite su pagalba.",
        'activation_db_error': "⚙️ Aktyvacijos metu įvyko duomenų bazės klaida. Susisiekite su pagalba.",
        'already_active': "✅ Jūsų paskyra jau aktyvuota.",
        'activation_success': "✅ Paskyra aktyvuota! Sveiki prisijungę. Naudokite /start dar kartą, kad pamatytumėte meniu.",

        # --- Client Menu ---
        'client_menu_title': "**Kliento Meniu** (Kodas: `{code}`)",
        'client_menu_sub_end': "Prenumerata baigiasi: `{end_date}`",
        'client_menu_userbots_title': "Priskirti Vartotojo Botai ({count}):",
        'client_menu_userbot_line': "{index}. {display_name} (`Būsena: {status}`)",
        'client_menu_userbot_error': "  └─ `Paskutinė klaida: {error}`",
        'client_menu_no_userbots': "Šiuo metu jums nėra priskirta jokių vartotojo botų.",
        'client_menu_button_setup_tasks': "🚀 Konfigūruoti Persiuntimo Užduotis",
        'client_menu_button_manage_folders': "📁 Tvarkyti Grupių Aplankus",
        'client_menu_button_join_groups': "🔗 Prisijungti prie Naujų Grupių",
        'client_menu_button_view_joined': "👁️ Peržiūrėti Prisijungtas Grupes",
        'client_menu_button_logs': "📊 Peržiūrėti Statistiką",
        'client_menu_button_language': "🌐 Nustatyti Kalbą",

        # --- Language Selection ---
        'select_language': "Pasirinkite pageidaujamą kalbą:",
        'language_set': "✅ Kalba nustatyta į {lang_name}.",
        'language_set_error': "❌ Nepavyko nustatyti kalbos.",

        # MUST ADD ALL OTHER KEYS FOR LITHUANIAN HERE...

    },
    # --- Other Language Placeholders ---
    'uk': { # Ukrainian
        'welcome': '👋 Ласкаво просимо! Будь ласка, надішліть свій унікальний код запрошення для активації облікового запису (наприклад, a565ae57).',
        # Add ALL other keys
    },
    'pl': { # Polish
        'welcome': '👋 Witamy! Proszę wysłać unikalny kod zaproszenia, aby aktywować konto (np. a565ae57).',
        # Add ALL other keys
    },
    'ru': { # Russian
        'welcome': '👋 Добро пожаловать! Пожалуйста, отправьте ваш уникальный код приглашения для активации аккаунта (например, a565ae57).',
        # Add ALL other keys
    }
}

def get_text(user_id, key, lang_override=None, **kwargs):
    """
    Retrieves translated text based on user's language preference or an override.

    Args:
        user_id: The Telegram user ID to check language preference. Can be 0 for default.
        key: The key for the translation string.
        lang_override: Explicit language code to use (e.g., 'en', 'lt').
        **kwargs: Placeholders to format into the string.

    Returns:
        The translated and formatted string, or the key itself if not found.
    """
    # Determine the language code
    lang = lang_override
    if not lang and user_id is not None and user_id != 0:
        try:
            lang = db.get_user_language(user_id)
        except Exception as e:
            # Log error getting language, default to English
            log.error(f"Failed to get lang for user {user_id}: {e}")
            lang = 'en'
    elif not lang: # Default language if no user or override
        lang = 'en'

    # Fallback logic: Specified Lang -> English -> Key itself
    selected_lang_dict = translations.get(lang)
    if selected_lang_dict and key in selected_lang_dict:
        text = selected_lang_dict[key]
    elif key in translations.get('en', {}): # Fallback to English if key exists there
        text = translations['en'][key]
        # Optional: Log a warning that a translation is missing for 'lang'
        # if lang != 'en': log.debug(f"Translation missing for lang='{lang}', key='{key}'. Using English fallback.")
    else:
        # Key not found anywhere, return the key itself as a fallback
        log.warning(f"Translation key '{key}' not found in language '{lang}' or English fallback.")
        text = key

    # Format the text with provided keyword arguments
    try:
        return text.format(**kwargs)
    except KeyError as e:
        # Log missing placeholder key if formatting fails
        log.error(f"Formatting error: Missing key '{e}' in translation for key '{key}' (lang '{lang}') with args {kwargs}")
        # Return the unformatted text in this case to avoid crashing
        return text
    except Exception as e:
        log.error(f"Unexpected formatting error for key '{key}' (lang '{lang}'): {e}")
        return text # Return unformatted text

log.info("Translations module loaded.")