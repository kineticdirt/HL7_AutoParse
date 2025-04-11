# Necessary imports...
import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import threading
import queue
import sys
import json
import os
import shutil
import time
import re # Import regex for camel case conversion
# import base64 # Not used currently
# import requests # Not used currently
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager
from PIL import Image #, ImageTk # ImageTk not used currently
import google.generativeai as genai
import google.api_core.exceptions
import traceback

# --- Configuration, Globals ---
BASE_URL = "https://hl7-definition.caristix.com/v2/HL7v2.6"
OUTPUT_JSON_FILE = "hl7_definitions_v2.6.json"
# SCREENSHOT_DIR = "screenshots_gui_hybrid_zoom" # No longer used
FALLBACK_HTML_DIR = "fallback_html" # Directory for saving HTML on fallback
API_KEY_FILE = "api_key.txt"
HL7_VERSION = "2.6"
GEMINI_API_KEY = None
GEMINI_MODEL = None
# Global variable to hold the app instance for access in functions
app = None

# --- Gemini API Functions (Unchanged) ---
def load_api_key():
    global GEMINI_API_KEY;
    try:
        try: script_dir = os.path.dirname(os.path.abspath(__file__))
        except NameError: script_dir = os.getcwd()
        key_file_path = os.path.join(script_dir, API_KEY_FILE)
        with open(key_file_path, 'r') as f: GEMINI_API_KEY = f.read().strip()
        if not GEMINI_API_KEY: messagebox.showerror("API Key Error", f"'{API_KEY_FILE}' is empty."); return False
        print("API Key loaded successfully."); return True
    except FileNotFoundError: messagebox.showerror("API Key Error", f"'{API_KEY_FILE}' not found in {script_dir}."); return False
    except Exception as e: messagebox.showerror("API Key Error", f"Error reading API key file: {e}"); return False

def configure_gemini():
    global GEMINI_MODEL;
    if not GEMINI_API_KEY: print("Error: API Key not loaded."); return False
    try:
        genai.configure(api_key=GEMINI_API_KEY)
        # Consider trying a potentially more capable model if flash struggles with zoomed-out text
        # GEMINI_MODEL = genai.GenerativeModel('gemini-1.5-pro-latest')
        GEMINI_MODEL = genai.GenerativeModel('gemini-1.5-flash') # Keep flash for now
        print("Gemini configured successfully."); return True
    except Exception as e: messagebox.showerror("Gemini Config Error", f"Failed to configure Gemini: {e}"); return False

# analyze_html_with_gemini() UNCHANGED from previous version
def analyze_html_with_gemini(html_content, definition_name, definition_type):
    """Analyzes HTML source code with Gemini to extract definitions."""
    global app
    if not GEMINI_MODEL:
        print("Error: Gemini model not configured.")
        return None
    if app and app.stop_event.is_set():
        print(f"  Skip Gemini (HTML): Stop requested for {definition_name}.")
        return None

    print(f"  Analyzing {definition_type} '{definition_name}' HTML with Gemini...")
    max_retries = 3
    retry_delay = 5 # Slightly longer delay might be needed for complex HTML

    prompt = "" # Initialize prompt

    # --- *** CONSTRUCT PROMPT BASED ON DEFINITION TYPE *** ---
    # Base instructions common to both/all types when analyzing HTML
    common_instructions = f"""
        Analyze the provided HTML source code for the HL7 {definition_type} definition page for '{definition_name}', version {HL7_VERSION}.
        Focus on the main data table, likely marked with classes like 'mat-table', 'table-definition', or similar structured `<tr>` and `<td>` elements within the primary content area. Ignore extraneous HTML like headers, footers, scripts, and sidebars.
        Extract the required information based on the {definition_type} type below.
        Generate a JSON object strictly following the specified rules.
        Return ONLY the raw JSON object for '{definition_name}' without any surrounding text or markdown formatting (` ```json ... ``` `).
    """

    if definition_type == 'Tables':
        prompt = common_instructions + f"""
        Specifically, find the table containing 'Value' and 'Description' (or 'Comment') columns.
        Extract the 'Value' and 'Description' for each data row (`<tr>`) within the table body (`<tbody>`).

        JSON Rules:
        1.  The **top-level key MUST be the numeric table ID as a JSON string** (e.g., "{definition_name}").
        2.  The value associated with this key MUST be an **array** of objects.
        3.  Each object in the array represents one row and MUST contain only two keys:
            *   `value`: The exact string from the 'Value' column cell (`<td>`).
            *   `description`: The exact string from the 'Description'/'Comment' column cell (`<td>`).
        4.  **Do NOT include** any other keys.

        Example structure for table "0001":
        {{
          "{definition_name}": [
            {{ "value": "F", "description": "Female", "comment": "..." }},
            {{ "value": "M", "description": "Male", "comment": "..." }},
          ]
        }}
        """
    elif definition_type == 'DataTypes' or definition_type == 'Segments':
        # Determine expected separator based on final user requirement
        separator_value = "." # Always use period now

        prompt = common_instructions + f"""
        Specifically, find the table defining the structure (fields/components). Look for columns like 'FIELD', 'LENGTH', 'DATA TYPE', 'OPTIONALITY', 'REPEATABILITY', 'TABLE'.

        JSON Rules:
        1.  Create a **top-level key which is the {definition_type} name** ('{definition_name}').
        2.  The value associated with this key MUST be an object.
        3.  This object MUST contain:
            *   `separator`: MUST be set to "{separator_value}"
            *   `versions`: An object containing a key for the HL7 version ('{HL7_VERSION}').
        4.  The '{HL7_VERSION}' object MUST contain:
            *   `appliesTo`: Set to 'equalOrGreater'.
            *   `totalFields`: The total count of rows extracted for the 'parts' array.
            *   `length`: The overall length shown near the top of the page content if available (e.g., text like "LENGTH 831"), otherwise -1. Find this value outside the main table if necessary.
            *   `parts`: An **array** of objects, one for each data row (`<tr>`) in the definition table body (`<tbody>`).
        5.  Each object within the 'parts' array represents a field/component and MUST contain:
            *   `name`: The field description (from 'FIELD' or similar column) converted to camelCase (e.g., 'setIdPv1', 'financialClassCode'). Remove any prefix like 'PV1-1'. If the description is just '...', use a generic name like 'fieldN' where N is the row number.
            *   `type`: The exact string from the 'DATA TYPE' column cell (`<td>`).
            *   `length`: The numeric value from the 'LENGTH' column cell (`<td>`). If it's '*' or empty/blank, use -1. Otherwise, use the integer value.
        6.  **Conditionally include** these keys in the part object ONLY if applicable, based on the corresponding column cell (`<td>`) content:
            *   `mandatory`: Set to `true` ONLY if the 'OPTIONALITY' column is 'R', 'C', or 'B'. Omit otherwise.
            *   `repeats`: Set to `true` ONLY if the 'REPEATABILITY' column does not contain a '-' character. Omit otherwise.
            *   `table`: Set to the **numeric table ID as a JSON string** ONLY if the 'TABLE' column contains a numeric value (e.g., "0004", "0125"). Omit if the cell is empty.

        Example structure for a Segment ('PV1') component part:
        {{ "name": "patientClass", "type": "IS", "length": 1, "mandatory": true, "table": "0004" }}
        """
    else:
        print(f"Error: Unknown definition_type '{definition_type}' for Gemini HTML prompt.")
        return None
    # --- END PROMPT CONSTRUCTION ---

    for attempt in range(max_retries):
        if app and app.stop_event.is_set():
            print(f"  Skip Gemini HTML attempt {attempt+1}: Stop requested.")
            return None
        try:
            print(f"  Attempt {attempt + 1} for {definition_name} HTML analysis...")
            # Send HTML content as text
            response = GEMINI_MODEL.generate_content(prompt + "\n\nHTML SOURCE:\n```html\n" + html_content + "\n```")

            # Robust JSON cleaning (same as before)
            json_text = response.text.strip()
            if json_text.startswith("```json"): json_text = json_text[7:]
            elif json_text.startswith("```"): json_text = json_text[3:]
            if json_text.endswith("```"): json_text = json_text[:-3]
            json_text = json_text.strip()

            # Attempt to parse
            parsed_json = json.loads(json_text)
            print(f"  Successfully parsed Gemini HTML response for {definition_name}.")
            return parsed_json
        except json.JSONDecodeError as e:
            print(f"Error: Bad JSON from Gemini HTML analysis for '{definition_name}': {e}")
            err_line, err_col = getattr(e, 'lineno', 'N/A'), getattr(e, 'colno', 'N/A')
            print(f"  Error at line ~{err_line}, column ~{err_col}")
            print(f"  Received Text: ```\n{response.text}\n```") # Log raw response
            if attempt == max_retries - 1:
                print(f"  Max retries reached for Gemini HTML analysis of {definition_name}.")
                return None
            print(f"  Retrying Gemini HTML analysis in {retry_delay}s...")
            time.sleep(retry_delay)
        except (google.api_core.exceptions.ResourceExhausted, google.api_core.exceptions.InternalServerError, google.api_core.exceptions.ServiceUnavailable, google.api_core.exceptions.GatewayTimeout) as e:
             print(f"Warn: Gemini API error attempt {attempt+1} for HTML analysis of '{definition_name}': {e}")
             if attempt < max_retries-1:
                  print(f"  Retrying in {retry_delay}s...")
                  time.sleep(retry_delay)
             else:
                  print(f"Error: Max Gemini retries reached for HTML analysis of '{definition_name}'."); return None
        except Exception as e:
            print(f"Error: Unexpected Gemini HTML analysis error attempt {attempt+1} for '{definition_name}': {e}")
            print(traceback.format_exc())
            # Decide if retry makes sense for unexpected errors, or just fail
            return None # Fail on unexpected errors for now

    return None # Should only be reached if all retries fail

# --- Selenium Functions ---
# setup_driver() UNCHANGED from previous version (includes headless)
def setup_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1200") # Set a reasonable default size for headless
    options.add_argument("--log-level=3")
    options.add_experimental_option('excludeSwitches', ['enable-logging'])

    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.implicitly_wait(3)
        return driver
    except WebDriverException as e:
        error_msg = f"Failed WebDriver init: {e}\n";
        if "net::ERR_INTERNET_DISCONNECTED" in str(e): error_msg += "Please check your internet connection.\n"
        elif "session not created" in str(e) and "version is" in str(e): error_msg += "ChromeDriver version might be incompatible with your Chrome browser. Try clearing the .wdm cache (see log/docs).\n"
        elif "user data directory is already in use" in str(e): error_msg += "Another Chrome process might be using the profile. Close all Chrome instances (including background tasks) and try again.\n"
        else: error_msg += "Check Chrome install/updates/antivirus. Clearing .wdm cache might help.\n"
        messagebox.showerror("WebDriver Error", error_msg); print(f"WebDriver Error:\n{error_msg}"); return None
    except Exception as e:
        messagebox.showerror("WebDriver Error", f"Unexpected WebDriver init error: {e}"); print(f"Unexpected WebDriver init error: {traceback.format_exc()}"); return None

# get_definition_list() UNCHANGED from previous version
def get_definition_list(driver, definition_type, status_queue, stop_event):
    list_url = f"{BASE_URL}/{definition_type}"
    status_queue.put(('status', f"Fetching {definition_type} list from: {list_url}"))
    if stop_event.is_set(): return []
    try: driver.get(list_url); time.sleep(0.2)
    except WebDriverException as e: status_queue.put(('error', f"Navigation error: {list_url}: {e}")); return []

    definitions = []; wait_time_initial = 15; pause_after_scroll = 0.2
    link_pattern_xpath = f"//a[contains(@href, '/{definition_type}/') and not(contains(@href,'#'))]"
    try:
        status_queue.put(('status', f"  Waiting up to {wait_time_initial}s for initial links..."))
        wait = WebDriverWait(driver, wait_time_initial)
        try: wait.until(EC.presence_of_element_located((By.XPATH, link_pattern_xpath))); status_queue.put(('status', "  Initial links detected. Starting scroll loop..."))
        except TimeoutException: status_queue.put(('error', f"Timeout waiting for initial links for {definition_type}.")); return []

        found_hrefs = set(); stale_scroll_count = 0; max_stale_scrolls = 5
        while stale_scroll_count < max_stale_scrolls:
            if stop_event.is_set(): status_queue.put(('warning', f"Stop requested during {definition_type} list scroll.")); break
            previous_href_count = len(found_hrefs); current_links = []
            try:
                WebDriverWait(driver, 3).until(EC.presence_of_element_located((By.XPATH, link_pattern_xpath)))
                current_links = driver.find_elements(By.XPATH, link_pattern_xpath)
            except TimeoutException: status_queue.put(('warning', "  Warn: No links found in current view after scroll/wait (likely end of list)."))
            except Exception as e: status_queue.put(('error', f"  Error finding links during scroll: {e}")); break

            if not current_links: stale_scroll_count += 1; status_queue.put(('status', f"  No links currently visible. Stale count: {stale_scroll_count}/{max_stale_scrolls}"))
            else:
                newly_added_this_pass = 0
                for link in current_links:
                    try:
                        href = link.get_attribute('href')
                        if href and f"/{definition_type}/" in href and href not in found_hrefs:
                            name = href.split('/')[-1].strip()
                            is_valid_name = False; validation_reason = "Unknown"
                            if definition_type == 'Tables': # Validation logic... (identical to previous)
                                clean_name = name
                                if not clean_name: validation_reason = "Name is empty"
                                elif any(char.isdigit() for char in clean_name):
                                     valid_chars = set('0123456789.')
                                     if all(char in valid_chars for char in clean_name):
                                         dot_count = clean_name.count('.')
                                         if dot_count == 0 and clean_name.isdigit(): is_valid_name = True; validation_reason = "Purely numeric"
                                         elif dot_count == 1:
                                             parts = clean_name.split('.');
                                             if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit() and parts[0] and parts[1]: is_valid_name = True; validation_reason = "Numeric with one decimal"
                                             else: validation_reason = "Invalid decimal format"
                                         else: validation_reason = f"Contains valid chars but invalid structure (dots={dot_count})"
                                     else: validation_reason = "Contains invalid characters"
                                else: validation_reason = "Contains no digits"
                                status_queue.put(('debug', f"Checking Table name: '{name}'. Is Valid: {is_valid_name}. Reason: {validation_reason}"))
                            else: # DataTypes/Segments
                                if name.isalnum(): is_valid_name = True; validation_reason = "Is alphanumeric"
                                else: validation_reason = "Is not alphanumeric"

                            if name and name != "#" and is_valid_name: found_hrefs.add(href); newly_added_this_pass += 1
                            elif name and name != "#" and not is_valid_name: status_queue.put(('debug', f"  Skipping '{name}' for type '{definition_type}' because Is Valid = {is_valid_name} (Reason: {validation_reason})"))
                    except StaleElementReferenceException: status_queue.put(('warning', "  Warn: Stale link encountered during scroll check.")); continue
                    except Exception as e: status_queue.put(('warning', f"  Warn: Error processing link attribute: {e}"))

                current_total_hrefs = len(found_hrefs); status_queue.put(('status', f"  Added {newly_added_this_pass} new valid links. Total unique valid: {current_total_hrefs}"))
                if current_total_hrefs == previous_href_count: stale_scroll_count += 1; status_queue.put(('status', f"  Scroll count stable: {stale_scroll_count}/{max_stale_scrolls}"))
                else: stale_scroll_count = 0
                if stale_scroll_count < max_stale_scrolls and current_links:
                    try: driver.execute_script("arguments[0].scrollIntoView(true);", current_links[-1]); status_queue.put(('status', f"  Scrolling last item into view. Pausing {pause_after_scroll}s...")); time.sleep(pause_after_scroll)
                    except StaleElementReferenceException: status_queue.put(('warning', "  Warn: Last element became stale before scroll could execute."))
                    except Exception as e: status_queue.put(('error', f"  Error scrolling last element: {e}")); stale_scroll_count += 1; status_queue.put(('status', f"  Incrementing stale count due to scroll error: {stale_scroll_count}/{max_stale_scrolls}"))

        status_queue.put(('status', "  Finished scroll attempts."))
        # Final name extraction... (identical to previous)
        definitions.clear(); valid_names_extracted = set()
        for href in found_hrefs:
            try:
                name = href.split('/')[-1].strip()
                if name and name != "#":
                    is_final_valid = False
                    if definition_type == 'Tables':
                         clean_name = name
                         if any(char.isdigit() for char in clean_name):
                             valid_chars = set('0123456789.'); dot_count = clean_name.count('.')
                             if all(char in valid_chars for char in clean_name):
                                 if dot_count == 0 and clean_name.isdigit(): is_final_valid = True
                                 elif dot_count == 1:
                                     parts = clean_name.split('.');
                                     if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit() and parts[0] and parts[1]: is_final_valid = True
                    else:
                        if name.isalnum(): is_final_valid = True
                    if is_final_valid: valid_names_extracted.add(name)
                    else: status_queue.put(('warning', f"  Final check failed for name '{name}' from href '{href}' (Type: {definition_type}). Skipping."))
            except Exception as e: status_queue.put(('warning', f"Warn: Error extracting name from final href '{href}': {e}"))
        definitions = sorted(list(valid_names_extracted))
        if not definitions and len(found_hrefs) > 0: status_queue.put(('warning', f"Warning: Collected {len(found_hrefs)} hrefs, but failed to extract valid names."))
        elif not definitions and not stop_event.is_set(): status_queue.put(('warning', f"Warning: No valid {definition_type} definitions found."))

    except TimeoutException: status_queue.put(('error', f"Timeout waiting for initial links for {definition_type}: {list_url}"))
    except WebDriverException as e: status_queue.put(('error', f"WebDriver error during {definition_type} list fetch: {e}"))
    except Exception as e: status_queue.put(('error', f"Unexpected error fetching {definition_type} list: {e}")); status_queue.put(('error', traceback.format_exc()))
    status_queue.put(('status', f"Final count: Found {len(definitions)} unique valid {definition_type}."))
    return definitions

# Helper: Camel Case Conversion (Unchanged)
def convert_to_camel_case(text):
    if not text: return "unknownFieldName"
    text = re.sub(r"^[A-Z0-9]{3}\s*-\s*\d+\s*-\s*", "", text)
    text = re.sub(r"^[A-Z0-9]{3}\s*-\s*\d+\s*", "", text)
    s = re.sub(r"[^a-zA-Z0-9\s]", "", text).strip()
    if not s: return "unknownFieldName"
    s = s.title(); s = s.replace(" ", "")
    return s[0].lower() + s[1:] if s else "unknownFieldName"

# <<<< Modified to be get copy-paste and css >>>>
def scrape_table_details(driver, table_id, status_queue, stop_event):
    """Scrapes Value and Description columns for a Table definition using persistent content-based scrolling."""
    status_queue.put(('debug', f"  Scraping Table {table_id}..."))
    table_data = []
    processed_values = set() # To handle potential duplicates during scroll
    table_locator = (By.XPATH, "//table[contains(@class, 'mat-table') or contains(@class, 'table-definition')]//tbody") # Broader XPath for table body
    row_locator = (By.TAG_NAME, "tr")
    # --- Assuming Value is Column 0 and Description/Comment is Column 1 ---
    # --- If this is wrong for some tables, that's a separate issue ---
    value_col_index = 0
    desc_col_index = 1
    # --- ---
    pause_after_scroll = 0.5 # Keep increased pause time

    # --- Scrolling Logic Variables ---
    stale_content_count = 0
    # *** INCREASED STALE COUNT TOLERANCE for Tables ***
    max_stale_content_scrolls = 10 # Increased further (Adjust if needed)
    scroll_amount = 800 # Pixels to scroll each time
    # --- End Scrolling ---

    try:
        # Wait longer for the initial table body presence
        WebDriverWait(driver, 15).until(EC.presence_of_element_located(table_locator))
        status_queue.put(('debug', f"    Table body located for Table {table_id}."))

        # --- SCROLLING LOOP ---
        while stale_content_count < max_stale_content_scrolls:
            if stop_event.is_set(): raise KeyboardInterrupt("Stop requested during table scroll scrape.")

            tbody = driver.find_element(*table_locator)
            try:
                # Get rows visible in the current state *before* processing them
                current_view_rows = tbody.find_elements(*row_locator)
                if not current_view_rows: # Check if empty list returned
                    status_queue.put(('debug', f"    Found 0 rows in current view for Table {table_id}. Might be empty or loading."))
            except StaleElementReferenceException:
                status_queue.put(('warning', f"    TBody became stale for Table {table_id} while finding rows, retrying scroll/find..."))
                time.sleep(0.3)
                driver.execute_script(f"window.scrollBy(0, {scroll_amount // 4});")
                time.sleep(pause_after_scroll)
                continue # Skip processing this pass

            newly_added_this_pass = 0 # Count for this specific pass

            # Process the rows found in the current view
            for row_index, row in enumerate(current_view_rows):
                value_text = None # Reset for each row
                desc_text = None  # Reset for each row
                row_identifier_for_log = f"view_row_{row_index}" # Generic log id

                try:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if len(cells) > desc_col_index: # Ensure at least value and description columns exist

                        # --- Extract Value with Stale Check ---
                        try:
                            value_text = cells[value_col_index].text.strip()
                            row_identifier_for_log = f"value:'{value_text[:20]}...'" # Update log id if value found
                        except StaleElementReferenceException:
                            status_queue.put(('warning', f"    Stale Value cell encountered in Table {table_id} row ~{row_identifier_for_log}, skipping cell."))
                            continue # Skip this row if value cell is stale

                        # Check uniqueness *before* getting description
                        if value_text and value_text not in processed_values:

                            # --- Extract Description with Stale Check ---
                            try:
                                # Use textContent for potentially more accurate text
                                desc_text = cells[desc_col_index].get_attribute('textContent').strip()
                                # desc_text = cells[desc_col_index].text.strip() # Fallback if textContent fails
                            except StaleElementReferenceException:
                                status_queue.put(('warning', f"    Stale Description cell encountered in Table {table_id} row {row_identifier_for_log}, skipping cell."))
                                continue # Skip this row if description cell is stale
                            except Exception as desc_err:
                                status_queue.put(('warning', f"    Error getting description cell text in Table {table_id} row {row_identifier_for_log}: {desc_err}"))
                                desc_text = "Error extracting description" # Add placeholder on error

                            # Add the successfully extracted pair
                            processed_values.add(value_text)
                            table_data.append({"value": value_text, "description": desc_text or ""}) # Ensure description is not None
                            newly_added_this_pass += 1

                        # Optional logging for skipped rows
                        # elif not value_text: status_queue.put(('debug', f"    Skipping row with empty value in Table {table_id}"))
                        # elif value_text in processed_values: status_queue.put(('debug', f"    Skipping duplicate value '{value_text}' in Table {table_id}"))

                    # else: # Log rows with too few columns
                    #     status_queue.put(('debug', f"    Skipping row with insufficient columns ({len(cells)} <= {desc_col_index}) in Table {table_id}"))

                # Catch errors during cell finding/processing for a single row
                except StaleElementReferenceException:
                    status_queue.put(('warning', f"    Stale row encountered mid-processing in Table {table_id} row ~{row_identifier_for_log}, continuing pass..."))
                    continue # Skip this problematic row
                except Exception as cell_err:
                    status_queue.put(('warning', f"    Error processing cells in Table {table_id} row ~{row_identifier_for_log}: {cell_err}"))
                    continue # Skip this problematic row
            # End of row processing loop for this view

            current_total_rows = len(table_data)
            status_queue.put(('debug', f"    Table {table_id} scroll pass: Found {len(current_view_rows)} rows in view, added {newly_added_this_pass} new unique rows. Total unique: {current_total_rows}"))

            # --- Check if new unique rows were added this pass ---
            if newly_added_this_pass == 0:
                stale_content_count += 1
                status_queue.put(('debug', f"    No new unique rows added for Table {table_id}. Stale count: {stale_content_count}/{max_stale_content_scrolls}"))
            else:
                stale_content_count = 0 # Reset if new content found

            # --- Scroll until max stale count is reached ---
            if stale_content_count < max_stale_content_scrolls:
                driver.execute_script(f"window.scrollBy(0, {scroll_amount});")
                time.sleep(pause_after_scroll / 2)
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                # status_queue.put(('debug', f"    Scrolled to bottom for Table {table_id} (Attempting to trigger load)")) # Can be noisy, enable if needed
                time.sleep(pause_after_scroll)

    # --- Exception Handling ---
    except TimeoutException: status_queue.put(('error', f"  Timeout finding table body for Table {table_id}.")); return None
    except NoSuchElementException: status_queue.put(('error', f"  Could not find table body for Table {table_id}.")); return None
    except KeyboardInterrupt: raise
    except Exception as e: status_queue.put(('error', f"  Unexpected error scraping Table {table_id}: {e}")); status_queue.put(('error', traceback.format_exc())); return None

    # --- Return Logic ---
    if not table_data:
        if not stop_event.is_set():
            status_queue.put(('warning', f"  No data scraped for Table {table_id} (and not stopped)."))
        return None

    status_queue.put(('debug', f"  Finished scraping Table {table_id}. Final row count: {len(table_data)}"))
    return {str(table_id): table_data}

# --- REVISED: process_definition_page  ---
def process_definition_page(driver, definition_type, definition_name, status_queue, stop_event):
    """Attempts direct scraping. If fails, falls back to HTML source + AI."""
    url = f"{BASE_URL}/{definition_type}/{definition_name}"
    status_queue.put(('status', f"Processing {definition_type}: {definition_name} at {url}"))
    if stop_event.is_set(): return None, definition_name

    scraped_data = None
    ai_data = None
    # screenshot_path = None # No longer used
    html_save_path = None # Path for saving HTML on fallback
    final_data_source = "None"
    final_data = None

    # 1. Navigate
    try:
        driver.get(url)
        # Add a small explicit wait *after* navigation for page elements to start loading
        WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.TAG_NAME, "body"))) # Wait for body tag
        time.sleep(0.5) # Extra buffer
    except WebDriverException as nav_err:
        status_queue.put(('error', f"Error navigating to {url}: {nav_err}"))
        return None, definition_name
    except TimeoutException:
        status_queue.put(('warning', f"Timeout waiting for body tag on {url}, proceeding anyway."))
        # Page might be blank or very slow, scraping will likely fail

    # 2. Attempt Direct Scraping (using the improved scraping functions)
    try:
        status_queue.put(('status', f"  Attempting direct scraping..."))
        if definition_type == "Tables":
            scraped_data = scrape_table_details(driver, definition_name, status_queue, stop_event)
        elif definition_type in ["DataTypes", "Segments"]:
            scraped_data = scrape_segment_or_datatype_details(driver, definition_type, definition_name, status_queue, stop_event)
        else:
            status_queue.put(('warning', f"  Unsupported type for scraping: {definition_type}"))

        # Basic validation of scraped data structure
        if scraped_data and isinstance(scraped_data, dict) and scraped_data:
            data_key = next(iter(scraped_data))
            data_value = scraped_data[data_key]
            valid_scrape = False
            if definition_type == "Tables" and isinstance(data_value, list):
                valid_scrape = True
            elif definition_type in ["DataTypes", "Segments"] and isinstance(data_value, dict) and "versions" in data_value:
                 valid_scrape = True

            if valid_scrape:
                status_queue.put(('status', f"  Direct scraping successful."))
                final_data_source = "Scraping"
                final_data = scraped_data
            else:
                status_queue.put(('warning', f"  Direct scraping result failed basic validation. Proceeding to AI fallback."))
                scraped_data = None # Ensure fallback happens
        elif scraped_data is None:
             # Check if stopped before logging warning
             if not stop_event.is_set():
                  status_queue.put(('warning', f"  Direct scraping function returned None. Proceeding to AI fallback."))
        else:
            status_queue.put(('warning', f"  Unexpected scraping result type: {type(scraped_data)}. Proceeding to AI fallback."))
            scraped_data = None # Ensure fallback happens

    except KeyboardInterrupt:
        status_queue.put(('warning', "Stop requested during scraping attempt."))
        return None, definition_name
    except Exception as scrape_err:
        status_queue.put(('warning', f"  Direct scraping failed: {scrape_err}. Proceeding to AI fallback."))
        status_queue.put(('debug', traceback.format_exc()))
        scraped_data = None # Ensure fallback happens

    # 3. Fallback to HTML Source and AI Analysis (if scraping failed and not stopped)
    if final_data is None and not stop_event.is_set():
        status_queue.put(('status', f"  Falling back to HTML Source + AI Analysis..."))
        try:
            # --- Get HTML Source ---
            status_queue.put(('status', "    Getting page source..."))
            html_content = driver.page_source
            if not html_content:
                 raise ValueError("Failed to retrieve page source.")
            status_queue.put(('status', f"    Retrieved page source ({len(html_content)} bytes)."))

            # --- Save HTML for debugging (Optional but recommended) ---
            try:
                script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
                html_full_dir = os.path.join(script_dir, FALLBACK_HTML_DIR)
                if not os.path.exists(html_full_dir):
                    os.makedirs(html_full_dir)
                html_filename = f"{definition_type}_{definition_name}_fallback.html"
                html_save_path = os.path.join(html_full_dir, html_filename)
                with open(html_save_path, 'w', encoding='utf-8') as f:
                    f.write(html_content)
                status_queue.put(('debug', f"    Saved fallback HTML to: {html_filename}"))
            except Exception as save_err:
                status_queue.put(('warning', f"    Could not save fallback HTML for {definition_name}: {save_err}"))
            # --- End Save HTML ---

            if stop_event.is_set(): raise KeyboardInterrupt("Stop requested before AI HTML analysis.")

            # --- AI Analysis of HTML ---
            ai_data = analyze_html_with_gemini(html_content, definition_name, definition_type)

            if ai_data:
                final_data_source = "AI Fallback (HTML)"
                final_data = ai_data
                # Add validation for AI data structure here if needed
            else:
                status_queue.put(('error', f"    AI HTML Analysis failed for {definition_name}."))

        except KeyboardInterrupt:
            status_queue.put(('warning', "Stop requested during AI fallback."))
            return None, definition_name
        except WebDriverException as wd_err:
            status_queue.put(('error', f"WebDriver error during AI fallback (getting source) on page {url}: {wd_err}"))
            return None, definition_name
        except Exception as e:
            status_queue.put(('error', f"Error during AI HTML fallback processing page {url}: {e}"))
            status_queue.put(('error', traceback.format_exc()))
            return None, definition_name
        # No finally block needed here as we didn't change zoom

    # 4. Log final source and return result
    status_queue.put(('status', f"  Finished processing {definition_name}. Source: {final_data_source}"))
    time.sleep(0.1) # Keep small delay
    return final_data, definition_name
# --- END REVISED process_definition_page ---

# clear_fallback_html_folder() UNCHANGED
def clear_fallback_html_folder(status_queue):
    """Clears the directory used for saving fallback HTML files."""
    script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
    dir_path = os.path.join(script_dir, FALLBACK_HTML_DIR) # Use the new directory name
    if os.path.exists(dir_path):
        status_queue.put(('status', f"Cleaning up fallback HTML directory: {dir_path}"))
        try:
            # Add safety check: only delete if it's the expected directory name
            if os.path.basename(dir_path) == FALLBACK_HTML_DIR and os.path.isdir(dir_path):
                shutil.rmtree(dir_path)
                os.makedirs(dir_path) # Recreate empty directory
                status_queue.put(('status', "Fallback HTML directory cleared and recreated."))
            elif not os.path.isdir(dir_path):
                 status_queue.put(('error', f"Path exists but is not a directory: {dir_path}"))
            else:
                 status_queue.put(('warning', f"Safety check failed: Path name '{os.path.basename(dir_path)}' does not match expected '{FALLBACK_HTML_DIR}'. Directory NOT deleted."))
        except OSError as e:
            status_queue.put(('error', f"Error clearing fallback HTML directory {dir_path}: {e}"))
        except Exception as e:
            status_queue.put(('error', f"Unexpected error clearing fallback HTML directory: {e}"))
    else:
        status_queue.put(('status', "Fallback HTML directory does not exist, nothing to clear."))

# load_existing_definitions() UNCHANGED
def load_existing_definitions(output_file, status_queue):
    script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
    file_path = os.path.join(script_dir, output_file)
    default_structure = {"tables": {}, "dataTypes": {}, "HL7": {}}
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if "tables" not in data: data["tables"] = {}
                if "dataTypes" not in data: data["dataTypes"] = {}
                if "HL7" not in data: data["HL7"] = {}
                status_queue.put(('status', f"Loaded {len(data.get('tables', {}))} tables and {len(data.get('dataTypes', {}))} dataTypes/segments from cache."))
                return data
        except json.JSONDecodeError as e: status_queue.put(('error', f"Error decoding existing JSON file '{output_file}': {e}. Starting fresh.")); return default_structure
        except Exception as e: status_queue.put(('error', f"Error reading existing JSON file '{output_file}': {e}. Starting fresh.")); return default_structure
    else: status_queue.put(('status', "No existing JSON file found. Starting fresh.")); return default_structure

# item_exists_in_cache() UNCHANGED
def item_exists_in_cache(definition_type, item_name, cache_dict):
    if not cache_dict: return False
    try:
        if definition_type == "Tables": return str(item_name) in cache_dict.get("tables", {})
        elif definition_type in ["DataTypes", "Segments"]: return item_name in cache_dict.get("dataTypes", {})
        else: return False
    except Exception: return False

# <<<< NOTE: This is the ONE CORRECT definition of scrape_segment_or_datatype_details >>>>
def scrape_segment_or_datatype_details(driver, definition_type, definition_name, status_queue, stop_event):
    """Scrapes details for Segment or DataType definitions using more persistent content-based scrolling."""
    status_queue.put(('debug', f"  Scraping {definition_type} {definition_name}..."))
    parts_data = []
    processed_row_identifiers = set() # Use first column (e.g., "PV1-1") as identifier

    # --- SELECTORS AND COLUMN INDICES (Verify these) ---
    table_locator = (By.XPATH, "//table[contains(@class, 'table-definition') and contains(@class, 'table')]//tbody")
    row_locator = (By.TAG_NAME, "tr")
    seq_col_index = 0
    desc_col_index = 1
    type_col_index = 2
    len_col_index = 3
    opt_col_index = 4
    repeat_col_index = 5
    table_col_index = 6
    # --- ---

    overall_length = -1
    pause_after_scroll = 0.5 # Keep increased pause time

    # --- Scrolling Logic Variables ---
    stale_content_count = 0
    # *** INCREASED STALE COUNT TOLERANCE ***
    max_stale_content_scrolls = 8 # Increased from 4 to 8 (Adjust further if needed, e.g., 10 or 12)
    scroll_amount = 800 # Pixels to scroll each time
    # --- End Scrolling ---

    try:
        # --- Try to get overall length (remains the same) ---
        try:
             length_element = driver.find_element(By.XPATH, "//div[contains(@class,'DefinitionPage_definitionContent')]//span[contains(text(),'Length:')]/following-sibling::span")
             length_text = length_element.text.strip()
             if length_text.isdigit(): overall_length = int(length_text); status_queue.put(('debug', f"    Found overall length: {overall_length}"))
             else: status_queue.put(('debug', f"    Found length text '{length_text}', couldn't parse as number."))
        except NoSuchElementException: status_queue.put(('debug', "    Overall length element not found."))
        except Exception as len_err: status_queue.put(('warning', f"    Error getting overall length: {len_err}"))
        # --- End Overall Length ---

        WebDriverWait(driver, 10).until(EC.presence_of_element_located(table_locator))

        # --- SCROLLING LOOP ---
        while stale_content_count < max_stale_content_scrolls:
            if stop_event.is_set(): raise KeyboardInterrupt("Stop requested during detail scroll scrape.")

            # Find rows currently in view
            tbody = driver.find_element(*table_locator)
            try:
                # Get rows visible in the current state *before* processing them
                current_view_rows = tbody.find_elements(*row_locator)
            except StaleElementReferenceException:
                status_queue.put(('warning', f"    TBody became stale for {definition_name} while finding rows, retrying scroll/find..."))
                time.sleep(0.2) # Optional pause
                driver.execute_script(f"window.scrollBy(0, {scroll_amount // 4});") # Gentle scroll
                time.sleep(pause_after_scroll)
                continue # Skip processing this pass

            newly_added_count = 0 # Reset for this pass

            # Process the rows found in the current view
            for row in current_view_rows:
                # Initialize part dictionary at the start of row processing
                part = {}
                row_identifier = None
                table_text = "" # Ensure table_text is reset for each row

                try:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    # Check if row has enough cells, including the table column index
                    if len(cells) > table_col_index: # More precise check

                        row_identifier = cells[seq_col_index].text.strip()
                        if not row_identifier: continue # Skip rows without identifier

                        # Check uniqueness *before* full processing
                        if row_identifier in processed_row_identifiers: continue

                        # --- Process data only if unique ---
                        processed_row_identifiers.add(row_identifier)

                        # Extract Data reliably
                        desc_text = cells[desc_col_index].text.strip()
                        type_text = cells[type_col_index].text.strip()
                        len_text = cells[len_col_index].text.strip()
                        opt_text = cells[opt_col_index].text.strip().upper() # Normalize
                        repeat_text = cells[repeat_col_index].text.strip().upper() # Normalize
                        table_text = cells[table_col_index].text.strip()
                        # status_queue.put(('debug', f"    Raw Table Text for '{definition_name}' row '{row_identifier}': '{table_text}'")) # Keep for debugging if needed

                        # Build Part Dictionary
                        part['name'] = convert_to_camel_case(desc_text)
                        part['type'] = type_text if type_text else "Unknown"
                        try: part['length'] = int(len_text) if len_text.isdigit() else -1
                        except ValueError: part['length'] = -1
                        if opt_text in ['R', 'C']: part['mandatory'] = True
                        if 'Y' in repeat_text or '∞' in repeat_text: part['repeats'] = True

                        # Add table key ONLY if valid text was found IN THIS ROW'S cell
                        if table_text:
                            if table_text.isdigit(): part['table'] = table_text
                            elif '.' in table_text:
                                table_parts = table_text.split('.')
                                if len(table_parts) == 2 and table_parts[0].isdigit() and table_parts[1].isdigit():
                                    part['table'] = table_text

                        parts_data.append(part)
                        newly_added_count += 1 # Increment only when adding unique part

                    else: # Log rows that don't even have enough cells
                        row_text_snippet = row.text[:50].replace('\n', ' ') if row.text else "EMPTY ROW"
                        status_queue.put(('debug', f"    Skipping row with insufficient columns ({len(cells)} <= {table_col_index}): '{row_text_snippet}'... in {definition_name}"))

                except StaleElementReferenceException:
                    status_queue.put(('warning', f"    Stale row/cell encountered in {definition_name} scrape processing, continuing pass..."))
                    if row_identifier and row_identifier in processed_row_identifiers and part not in parts_data: # Check if added before rollback
                         processed_row_identifiers.remove(row_identifier) # Attempt rollback
                    continue # Skip this row
                except Exception as cell_err:
                    status_queue.put(('warning', f"    Error processing row/cell in {definition_name} (ID: {row_identifier}): {cell_err}"))
                    if row_identifier and row_identifier in processed_row_identifiers and part not in parts_data: # Check if added before rollback
                         processed_row_identifiers.remove(row_identifier) # Attempt rollback
            # End of row processing loop for this view

            current_parts_count = len(parts_data) # Count after processing view
            status_queue.put(('debug', f"    {definition_type} {definition_name} scroll pass: Found {len(current_view_rows)} rows in view, added {newly_added_count} new unique parts. Total unique: {current_parts_count}"))

            # --- Check if new parts were added this pass ---
            if newly_added_count == 0:
                stale_content_count += 1
                status_queue.put(('debug', f"    No new unique parts added for {definition_name}. Stale count: {stale_content_count}/{max_stale_content_scrolls}"))
            else:
                stale_content_count = 0 # Reset if new content was found

            # --- Scroll until max stale count is reached ---
            if stale_content_count < max_stale_content_scrolls:
                # Scroll down by a fixed amount
                driver.execute_script(f"window.scrollBy(0, {scroll_amount});")
                time.sleep(pause_after_scroll / 2) # Short pause after partial scroll

                # *** ADDED: Explicit scroll to bottom to force loading ***
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                status_queue.put(('debug', f"    Scrolled to bottom for {definition_name} (Attempting to trigger load)"))
                time.sleep(pause_after_scroll) # Full pause after scrolling to bottom

    # --- Exception Handling (remains the same) ---
    except TimeoutException: status_queue.put(('error', f"  Timeout finding table body for {definition_name}.")); return None
    except NoSuchElementException: status_queue.put(('error', f"  Could not find table body for {definition_name}.")); return None
    except KeyboardInterrupt: raise
    except Exception as e: status_queue.put(('error', f"  Unexpected error scraping {definition_name}: {e}")); status_queue.put(('error', traceback.format_exc())); return None

    # --- Return Logic (remains the same) ---
    if not parts_data:
        if not stop_event.is_set():
            status_queue.put(('warning', f"  No parts data scraped for {definition_name} (and not stopped)."))
        return None

    # Add standard segment part *if necessary*
    if definition_type == "Segments":
        hl7_seg_part = {"mandatory": True, "name": "hl7SegmentName", "type": "ST", "table": "0076", "length": 3}
        if not parts_data or parts_data[0].get("name") != "hl7SegmentName":
            parts_data.insert(0, hl7_seg_part)
            status_queue.put(('debug', f"  Prepended standard part for Segment {definition_name} (Scraper)"))

    # --- Assemble final structure with Standard Separator Logic ---
    separator_char = "." 

    final_structure = {
        "separator": separator_char,
        "versions": {
            HL7_VERSION: {
                "appliesTo": "equalOrGreater",
                "totalFields": len(parts_data),
                "length": overall_length,
                "parts": parts_data
            }
        }
    }
    # --- End Separator ---
    status_queue.put(('debug', f"  Finished scraping {definition_type} {definition_name}. Total parts: {len(parts_data)}"))
    return {definition_name: final_structure}

# GUI Class HL7ParserApp (Contains the ONE correct start_processing)
class HL7ParserApp:
    def __init__(self, master): # Abridged
        self.master = master; master.title("HL7 Parser (Scrape+AI Fallback/Zoom)"); master.geometry("700x550")
        self.status_queue = queue.Queue(); self.stop_event = threading.Event();
        self.worker_threads = []; self.orchestrator_thread = None
        self.grand_total_items = 0; self.processed_items_count = 0; self.list_counts_received = set()
        style = ttk.Style(); style.theme_use('clam')
        # GUI setup... (UNCHANGED)
        main_frame = ttk.Frame(master, padding="10"); main_frame.pack(fill=tk.BOTH, expand=True)
        progress_frame = ttk.Frame(main_frame); progress_frame.pack(fill=tk.X, pady=5)
        log_frame = ttk.Frame(main_frame); log_frame.pack(fill=tk.BOTH, expand=True, pady=5)
        button_frame = ttk.Frame(main_frame); button_frame.pack(fill=tk.X, pady=10)
        ttk.Label(progress_frame, text="Overall Progress:").pack(side=tk.LEFT, padx=5)
        self.pb_overall = ttk.Progressbar(progress_frame, orient="horizontal", length=300, mode="determinate"); self.pb_overall.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)
        self.lbl_overall_perc = ttk.Label(progress_frame, text="0%"); self.lbl_overall_perc.pack(side=tk.LEFT, padx=5)
        stage_prog_frame = ttk.Frame(main_frame); stage_prog_frame.pack(fill=tk.X, pady=5); stage_prog_frame.columnconfigure(1, weight=1)
        ttk.Label(stage_prog_frame, text="Tables:").grid(row=0, column=0, padx=5, sticky='w'); self.pb_tables = ttk.Progressbar(stage_prog_frame, orient="horizontal", length=300, mode="determinate"); self.pb_tables.grid(row=0, column=1, padx=5, sticky='ew'); self.lbl_tables_count = ttk.Label(stage_prog_frame, text="0/0"); self.lbl_tables_count.grid(row=0, column=2, padx=5, sticky='e')
        ttk.Label(stage_prog_frame, text="DataTypes:").grid(row=1, column=0, padx=5, sticky='w'); self.pb_datatypes = ttk.Progressbar(stage_prog_frame, orient="horizontal", length=300, mode="determinate"); self.pb_datatypes.grid(row=1, column=1, padx=5, sticky='ew'); self.lbl_datatypes_count = ttk.Label(stage_prog_frame, text="0/0"); self.lbl_datatypes_count.grid(row=1, column=2, padx=5, sticky='e')
        ttk.Label(stage_prog_frame, text="Segments:").grid(row=2, column=0, padx=5, sticky='w'); self.pb_segments = ttk.Progressbar(stage_prog_frame, orient="horizontal", length=300, mode="determinate"); self.pb_segments.grid(row=2, column=1, padx=5, sticky='ew'); self.lbl_segments_count = ttk.Label(stage_prog_frame, text="0/0"); self.lbl_segments_count.grid(row=2, column=2, padx=5, sticky='e')
        ttk.Label(log_frame, text="Log:").pack(anchor='w'); self.log_area = scrolledtext.ScrolledText(log_frame, height=15, wrap=tk.WORD, state='disabled'); self.log_area.pack(fill=tk.BOTH, expand=True); self.log_area.tag_config('error', foreground='red'); self.log_area.tag_config('warning', foreground='orange'); self.log_area.tag_config('debug', foreground='gray')
        self.start_button = ttk.Button(button_frame, text="Start Processing", command=self.start_processing); self.start_button.pack(side=tk.RIGHT, padx=5)
        self.stop_button = ttk.Button(button_frame, text="Stop", command=self.stop_processing, state=tk.DISABLED); self.stop_button.pack(side=tk.RIGHT, padx=5)

    def log_message(self, message, level="info"): # UNCHANGED
        tag=(); prefix="";
        if level == "error": tag,prefix = (('error',),"ERROR: ")
        elif level == "warning": tag,prefix = (('warning',),"WARNING: ")
        elif level == "debug": tag, prefix = (('debug',), "DEBUG: ")
        else: tag, prefix = ((), "")
        def update_log(): self.log_area.config(state='normal'); self.log_area.insert(tk.END, f"{prefix}{message}\n", tag); self.log_area.see(tk.END); self.log_area.config(state='disabled')
        self.master.after(0, update_log)

    def update_progress(self, bar_type, current, total): # UNCHANGED
        def update_gui():
            total_val=max(1,total); percentage=int((current/total_val)*100) if total_val > 0 else 0
            pb,lbl=None,None; count_text=f"{current}/{total}"
            if bar_type=="tables": pb,lbl=(self.pb_tables,self.lbl_tables_count)
            elif bar_type=="datatypes": pb,lbl=(self.pb_datatypes,self.lbl_datatypes_count)
            elif bar_type=="segments": pb,lbl=(self.pb_segments,self.lbl_segments_count)
            elif bar_type=="overall":
                 pb,lbl,count_text=(self.pb_overall,self.lbl_overall_perc,f"{percentage}%")
                 # Simplified the update logic here slightly
                 if pb: pb.config(maximum=total_val, value=current);
                 if lbl: lbl.config(text=count_text);
                 return # Return after handling overall
            # Handle other types (tables, datatypes, segments)
            if pb: pb.config(maximum=total_val, value=current);
            if lbl: lbl.config(text=count_text)
        self.master.after(0, update_gui)


    def check_queue(self): # UNCHANGED
        try:
            while True:
                message=self.status_queue.get_nowait(); msg_type=message[0]
                if msg_type=='status': self.log_message(message[1])
                elif msg_type=='error': self.log_message(message[1], level="error")
                elif msg_type=='warning': self.log_message(message[1], level="warning")
                elif msg_type=='debug': self.log_message(message[1], level="debug")
                elif msg_type=='progress': self.update_progress(message[1], message[2], message[3])
                elif msg_type == 'progress_add': self.processed_items_count += message[1]; self.update_progress("overall", self.processed_items_count, self.grand_total_items)
                elif msg_type == 'list_found':
                     category_name = message[1]; count = message[2]
                     if category_name not in self.list_counts_received:
                         self.grand_total_items += count; self.list_counts_received.add(category_name)
                         self.update_progress(category_name.lower(), 0, count); self.update_progress("overall", self.processed_items_count, self.grand_total_items)
                         self.log_message(f"Found {count} {category_name}.")
                elif msg_type=='finished':
                    error_count = message[1]; self.log_message("Processing finished.")
                    self.start_button.config(state=tk.NORMAL); self.stop_button.config(state=tk.DISABLED)
                    if error_count is not None and error_count > 0: messagebox.showwarning("Complete with Errors", f"Finished, but with {error_count} errors recorded. Check log and screenshots.")
                    elif error_count == 0: messagebox.showinfo("Complete", "Finished successfully!")
                    else: messagebox.showinfo("Complete", "Processing finished (may have been aborted or no items found).")
                    self.worker_threads = []; self.orchestrator_thread = None; return
        except queue.Empty: pass
        orchestrator_alive = self.orchestrator_thread and self.orchestrator_thread.is_alive(); workers_alive = any(t.is_alive() for t in self.worker_threads)
        if workers_alive or orchestrator_alive: self.master.after(150, self.check_queue)
        elif self.start_button['state'] == tk.DISABLED: # Keep checking queue briefly after threads finish to catch final messages
            self.master.after(500, self.check_queue)

    # <<<< NOTE: This is the ONE CORRECT definition of start_processing >>>>
    def start_processing(self):
        if not load_api_key(): return
        if not configure_gemini(): return

        # Check if already running
        orchestrator_alive = self.orchestrator_thread and self.orchestrator_thread.is_alive()
        if orchestrator_alive or any(t.is_alive() for t in self.worker_threads):
            messagebox.showwarning("Busy", "Processing is already in progress.")
            return

        self.stop_event.clear()
        self.start_button.config(state=tk.DISABLED)
        self.stop_button.config(state=tk.NORMAL)
        self.log_message("Starting concurrent processing (Scrape+AI Fallback/Zoom)...")
        self.log_message("Using headless browsers and caching...")

        # Reset progress tracking
        self.grand_total_items = 0
        self.processed_items_count = 0
        self.list_counts_received.clear()
        self.update_progress("tables",0,1); self.lbl_tables_count.config(text="0/0")
        self.update_progress("datatypes",0,1); self.lbl_datatypes_count.config(text="0/0")
        self.update_progress("segments",0,1); self.lbl_segments_count.config(text="0/0")
        self.update_progress("overall",0,1); self.lbl_overall_perc.config(text="0%")

        # Clear previous worker threads list
        self.worker_threads = []

        # Create the shared queue for results
        results_queue = queue.Queue()

        # Start the main orchestrator thread
        # Pass the results_queue to it
        self.orchestrator_thread = threading.Thread(target=self.run_parser_orchestrator, # This references the method below
                                            args=(results_queue, self.stop_event),
                                            daemon=True)
        self.orchestrator_thread.start()
        # Note: We store the orchestrator thread to check its status later

        # Start checking the status queue
        self.master.after(100, self.check_queue)

    def stop_processing(self): # UNCHANGED
        orchestrator_alive = hasattr(self, 'orchestrator_thread') and self.orchestrator_thread and self.orchestrator_thread.is_alive();
        workers_alive = any(t.is_alive() for t in self.worker_threads)
        if workers_alive or orchestrator_alive:
            if not self.stop_event.is_set():
                self.log_message("Stop request received. Signaling background threads...", level="warning");
                self.stop_event.set()
            self.stop_button.config(state=tk.DISABLED) # Disable stop button immediately after signaling
        else:
            self.log_message("Stop requested, but no active process found.", level="info");
            self.stop_button.config(state=tk.DISABLED);
            self.start_button.config(state=tk.NORMAL) # Ensure start is enabled if nothing was running


    # <<<< NOTE: This is the run_parser_orchestrator method, correctly indented within the class >>>>
    def run_parser_orchestrator(self, results_queue, stop_event):
        """Starts workers, collects results, merges with cache, saves."""
        categories = ["Tables", "DataTypes", "Segments"]
        # <<< --- LOAD CACHE --- >>>
        loaded_definitions = load_existing_definitions(OUTPUT_JSON_FILE, self.status_queue)
        # <<< --- END LOAD CACHE --- >>>

        all_new_results = {"Tables": {}, "DataTypes": {}, "Segments": {}} # Store only NEW results from threads
        thread_errors = {"Tables": 0, "DataTypes": 0, "Segments": 0}
        threads_finished = set()
        total_error_count = 0
        self.worker_threads = [] # Ensure this is cleared before starting new ones

        try:
            self.status_queue.put(('status', "Starting worker threads..."))
            for category in categories:
                if stop_event.is_set(): break
                # <<< --- PASS CACHE TO WORKER --- >>>
                # Use the standalone process_category_thread function as the target
                worker = threading.Thread(target=process_category_thread,
                                            args=(category, results_queue, self.status_queue, stop_event, loaded_definitions), # Pass loaded cache
                                            daemon=True, name=f"Worker-{category}")
                # <<< --- END PASS CACHE --- >>>
                self.worker_threads.append(worker)
                worker.start()

            if stop_event.is_set(): raise KeyboardInterrupt("Stop requested during thread startup.")

            # Result Collection
            self.status_queue.put(('status', "Waiting for results from worker threads..."))
            while len(threads_finished) < len(categories):
                workers_still_alive = any(t.is_alive() for t in self.worker_threads)
                if stop_event.is_set() and not workers_still_alive:
                    self.status_queue.put(('warning', "Stopping result collection early due to stop signal and no running workers."))
                    break
                try:
                    result_type, data = results_queue.get(timeout=1.0)
                    if result_type.endswith("_DONE"):
                        category = result_type.replace("_DONE", "")
                        if category in categories:
                            threads_finished.add(category)
                            thread_errors[category] = data
                            total_error_count += data
                            self.status_queue.put(('status', f"Worker thread for {category} finished reporting {data} errors."))
                        else:
                            self.status_queue.put(('warning', f"Received unexpected DONE signal: {result_type}"))
                    elif result_type in categories:
                        all_new_results[result_type].update(data) # Update NEW results dict
                        self.status_queue.put(('debug', f"Received {len(data)} new results for {result_type}."))
                    else:
                        self.status_queue.put(('warning', f"Received unknown result type: {result_type}"))
                except queue.Empty:
                    if stop_event.is_set() and not workers_still_alive: # Double check after timeout
                         self.status_queue.put(('warning', "Stop signal detected and workers finished while waiting for results queue."))
                         break # Exit loop if stopped and workers are done
                    continue # Continue waiting if queue is empty and not stopped/workers done

            self.status_queue.put(('status', "All worker threads have reported completion or stop signal acted upon."))

        except KeyboardInterrupt:
            self.status_queue.put(('warning', "\nOrchestrator aborted by user request."))
            if not stop_event.is_set(): stop_event.set() # Ensure stop is signaled
        except Exception as e:
            self.status_queue.put(('error', f"Orchestrator CRITICAL ERROR: {e}"))
            self.status_queue.put(('error', traceback.format_exc()))
            total_error_count += 1
            if not stop_event.is_set(): stop_event.set() # Signal stop on critical error
        finally:
            # Thread Joining
            self.status_queue.put(('status', "Ensuring all worker threads have terminated..."))
            join_timeout = 10.0
            start_join_time = time.time()
            for t in self.worker_threads:
                 remaining_timeout = max(0.1, join_timeout - (time.time() - start_join_time)) # Reduce timeout for subsequent joins
                 t.join(timeout=remaining_timeout)
                 if t.is_alive():
                      self.status_queue.put(('warning', f"Thread {t.name} did not terminate within its timeout slice."))
            self.status_queue.put(('status', "Worker thread joining complete."))

            # <<< --- MERGE CACHE WITH NEW RESULTS --- >>>
            final_definitions = loaded_definitions # Start with the loaded cache
            processed_segments_for_hl7 = []

            # Only merge/save fully if stop wasn't requested OR if new results were actually gathered before stop
            # This prevents overwriting the file with just the cache if stopped very early.
            should_process_results = not stop_event.is_set() or any(all_new_results.values())

            if should_process_results:
                self.status_queue.put(('status', "Merging cached and new results..."))
                # Update the 'tables' section
                final_definitions.setdefault("tables", {}).update(all_new_results.get("Tables", {}))
                # Update the 'dataTypes' section (includes both DataTypes and Segments)
                final_definitions.setdefault("dataTypes", {}).update(all_new_results.get("DataTypes", {}))
                final_definitions["dataTypes"].update(all_new_results.get("Segments", {}))

                # Get segment names from the FINAL combined dictionary for HL7 structure
                processed_segments_for_hl7 = [k for k, v in final_definitions["dataTypes"].items() if isinstance(v, dict) and v.get('separator') == '.']
                # <<< --- END MERGE --- >>>


                # Build HL7 Structure
                self.status_queue.put(('status', "\n--- Building HL7 Structure ---"))
                hl7_parts=[]; common_order=["MSH","PID","PV1","OBR","OBX"];
                ordered=[s for s in common_order if s in processed_segments_for_hl7]
                other=sorted([s for s in processed_segments_for_hl7 if s not in common_order])
                final_segment_order = ordered + other
                if not final_segment_order:
                    self.status_queue.put(('warning', "No segments found in final combined data to build HL7 structure."))
                else:
                    for seg_name in final_segment_order:
                        seg_def = final_definitions["dataTypes"].get(seg_name)
                        is_mand = False; repeats = False; length = -1
                        if seg_name == "MSH": is_mand = True
                        # Standard segments other than MSH are typically repeatable in many contexts
                        # You might adjust this logic based on specific HL7 message structure needs
                        elif seg_name in ["PID", "PV1", "OBR", "OBX"]: repeats = True

                        # Extract length if available
                        if seg_def and isinstance(seg_def, dict) and 'versions' in seg_def:
                            version_key = next(iter(seg_def.get('versions', {})), None);
                            if version_key and isinstance(seg_def['versions'][version_key], dict):
                                length = seg_def['versions'][version_key].get('length', -1)

                        part={"name":seg_name.lower(),"type":seg_name,"length": length if length is not None else -1}; # Ensure length is not None
                        if is_mand: part.update({"mandatory":True});
                        if repeats: part.update({"repeats":True}); hl7_parts.append(part)

                    final_definitions.setdefault("HL7", {}).update({
                         "separator":"\r",
                         "partId":"type",
                         "versions":{
                             HL7_VERSION: {
                                 "appliesTo":"equalOrGreater",
                                 "length":-1, # Overall HL7 message length isn't typically pre-defined here
                                 "parts":hl7_parts
                             }
                         }
                    })
                    self.status_queue.put(('status', f"HL7 structure updated/built with {len(hl7_parts)} segments."))

            # Write Final JSON (Only if not stopped OR if results were processed)
            if should_process_results:
                self.status_queue.put(('status', f"\nWriting final definitions to {OUTPUT_JSON_FILE}"))
                script_dir=os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
                output_path=os.path.join(script_dir,OUTPUT_JSON_FILE)
                try:
                    with open(output_path,'w',encoding='utf-8') as f:
                        json.dump(final_definitions,f,indent=2,ensure_ascii=False)
                    self.status_queue.put(('status', "JSON file written successfully."))

                            # <<< --- ADD COMPARISON CALL HERE --- >>>
                    try:
                        # Always import first to ensure the local variable exists
                        import hl7_comparison
                        import importlib

                        # Now, reload it to pick up any potential changes if needed.
                        # This assumes you might edit hl7_comparison.py while the app runs.
                        # If that's not the case, you could potentially skip the reload.
                        hl7_comparison = importlib.reload(hl7_comparison)

                        self.status_queue.put(('status', "\n--- Running Comparison Against Reference ---"))
                        script_dir=os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()

                        # Add checks for required attributes before using them
                        if not hasattr(hl7_comparison, 'REFERENCE_FILE'):
                            raise AttributeError("hl7_comparison.py module is missing the required REFERENCE_FILE constant.")
                        if not hasattr(hl7_comparison, 'compare_hl7_definitions'):
                            raise AttributeError("hl7_comparison.py module is missing the required compare_hl7_definitions function.")

                        ref_file_path = os.path.join(script_dir, hl7_comparison.REFERENCE_FILE)
                        comparison_successful = hl7_comparison.compare_hl7_definitions(
                            generated_filepath=output_path,
                            reference_filepath=ref_file_path,
                            status_queue=self.status_queue # Pass the queue for logging
                        )
                        if comparison_successful:
                            self.status_queue.put(('status', "--- Comparison Complete: Files match reference. ---"))
                        else:
                            self.status_queue.put(('warning', "--- Comparison Complete: Differences detected. ---"))

                    except ImportError:
                        self.status_queue.put(('error', "Could not import hl7_comparison.py. Ensure it's in the same directory."))
                    except AttributeError as ae:
                        # This will now catch the missing constant/function more clearly
                        self.status_queue.put(('error', f"Error accessing component in hl7_comparison.py: {ae}"))
                        self.status_queue.put(('error', traceback.format_exc()))
                    except Exception as comp_err:
                        self.status_queue.put(('error', f"Error during comparison: {comp_err}"))
                        self.status_queue.put(('error', traceback.format_exc()))
                    # <<< --- END COMPARISON CALL --- >>>


                    # --- Conditional Cleanup ---
                    if total_error_count == 0 and not stop_event.is_set(): # Only clear if no errors AND not stopped
                        self.status_queue.put(('status', "No errors recorded and process completed, attempting fallback HTML cleanup."))
                        # *** UPDATED FUNCTION CALL ***
                        clear_fallback_html_folder(self.status_queue)
                    elif total_error_count > 0:
                        self.status_queue.put(('warning', f"Errors ({total_error_count}) occurred, fallback HTML files in '{FALLBACK_HTML_DIR}' were NOT deleted."))
                    elif stop_event.is_set():
                         self.status_queue.put(('warning', f"Process was stopped, fallback HTML files in '{FALLBACK_HTML_DIR}' were NOT deleted."))

                except Exception as e:
                    self.status_queue.put(('error', f"Failed to write JSON file: {e}")); total_error_count+=1
            elif stop_event.is_set():
                self.status_queue.put(('warning', f"Processing stopped early, final JSON file '{OUTPUT_JSON_FILE}' was NOT updated with potentially incomplete results."))
            else:
                 self.status_queue.put(('warning', "No new results processed, JSON file not updated."))


            # Signal Overall Completion
            self.status_queue.put(('finished', total_error_count if should_process_results else None)) # Indicate errors only if processing happened


# --- REVISED: process_category_thread (Standalone Function for Worker Threads) ---
# <<<< NOTE: This function is OUTSIDE the HL7ParserApp class >>>>
def process_category_thread(definition_type, results_queue, status_queue, stop_event, loaded_definitions):
    """Worker thread using scrape-first-then-AI fallback and caching."""
    thread_name = f"Thread-{definition_type}"; status_queue.put(('status', f"[{thread_name}] Starting."))
    driver = None; error_count = 0; items_processed_in_thread = 0; items_skipped_cache = 0
    definition_list = []; thread_result_dict = {}
    try: # Outer try for the whole thread function
        status_queue.put(('status', f"[{thread_name}] Initializing WebDriver..."))
        driver = setup_driver();
        if not driver: raise Exception(f"[{thread_name}] WebDriver initialization failed.")
        if stop_event.is_set(): raise KeyboardInterrupt("Stop requested early.")

        definition_list = get_definition_list(driver, definition_type, status_queue, stop_event);
        list_count = len(definition_list)
        status_queue.put(('list_found', definition_type, list_count));
        status_queue.put(('progress', definition_type.lower(), 0, list_count)) # Initialize progress for this category

        if stop_event.is_set(): raise KeyboardInterrupt("Stop requested after list fetch.")

        if definition_list:
            status_queue.put(('status', f"[{thread_name}] Processing/Checking {list_count} {definition_type}..."))
            for i, item_name in enumerate(definition_list):
                if stop_event.is_set():
                    status_queue.put(('warning', f"[{thread_name}] Stop requested before processing '{item_name}'."));
                    break # Exit the loop if stop is requested

                # --- CACHING CHECK ---
                if item_exists_in_cache(definition_type, item_name, loaded_definitions):
                    status_queue.put(('debug', f"[{thread_name}] Skipping '{item_name}' - found in cache."))
                    items_skipped_cache += 1
                    # Need to update BOTH category and overall progress even when skipping
                    status_queue.put(('progress', definition_type.lower(), items_processed_in_thread + items_skipped_cache, list_count));
                    status_queue.put(('progress_add', 1));
                    continue # Move to the next item
                # --- END CACHING CHECK ---

                # Call the processing function (attempts scrape, falls back to AI)
                processed_data, _ = process_definition_page(driver, definition_type, item_name, status_queue, stop_event)
                # Increment processed count ONLY if not skipped by cache
                items_processed_in_thread += 1


                # --- Validation / Storing ---
                corrected_item_data = None; processing_successful = False
                if processed_data and isinstance(processed_data, dict):
                    if len(processed_data) == 1:
                        final_key = next(iter(processed_data)); final_value = processed_data[final_key]
                        expected_key = str(item_name) if definition_type == "Tables" else item_name
                        if final_key == expected_key:
                            # Structure validation based on type
                            if definition_type == "Tables" and isinstance(final_value, list):
                                # Check if list items are dicts with 'value' (basic check)
                                if all(isinstance(item, dict) and 'value' in item for item in final_value):
                                    corrected_item_data = final_value; processing_successful = True
                                else:
                                    status_queue.put(('warning', f"[{thread_name}] Final Table '{item_name}' list items invalid structure. Skip.")); error_count += 1
                            elif definition_type in ["DataTypes", "Segments"] and isinstance(final_value, dict) and "versions" in final_value:
                                corrected_item_data = final_value; processing_successful = True
                                # Add standard segment part check/add happens in orchestrator after merge now
                            else:
                                status_queue.put(('warning', f"[{thread_name}] Final {definition_type} '{item_name}' invalid structure. Skip.")); error_count += 1
                        else:
                            status_queue.put(('warning', f"[{thread_name}] Final {definition_type} key '{final_key}' != expected '{expected_key}'. Skip.")); error_count += 1
                    else:
                        status_queue.put(('warning', f"[{thread_name}] Final {definition_type} '{item_name}' dict has != 1 key. Skip.")); error_count += 1
                elif processed_data is None and not stop_event.is_set():
                    # Only count as error if stop wasn't requested during its processing
                    status_queue.put(('warning', f"[{thread_name}] No final data for '{item_name}'. Skip.")); error_count += 1
                elif processed_data and not stop_event.is_set(): # Check if processed_data is not None but also not a dict
                    status_queue.put(('warning', f"[{thread_name}] Final data for '{item_name}' not dict type: {type(processed_data)}. Skip.")); error_count += 1
                # --- End Validation ---

                if processing_successful and corrected_item_data is not None:
                     # Ensure key is string for tables, as expected by cache check later
                    result_key = str(item_name) if definition_type == "Tables" else item_name
                    thread_result_dict[result_key] = corrected_item_data

                # Update progress bars AFTER processing (or skipping) the item
                # Use combined count for category progress
                current_progress_count = items_processed_in_thread + items_skipped_cache
                status_queue.put(('progress', definition_type.lower(), current_progress_count, list_count));
                status_queue.put(('progress_add', 1)) # Increment overall progress counter by 1 always

        # Send results collected by this thread to the orchestrator
        # Send even if empty or stopped early
        results_queue.put((definition_type, thread_result_dict));
        status_queue.put(('status', f"[{thread_name}] Finished. Processed: {items_processed_in_thread}, Skipped (Cache): {items_skipped_cache}, Errors: {error_count}"))

    # --- Outer Exception Handling ---
    except KeyboardInterrupt:
        status_queue.put(('warning', f"[{thread_name}] Aborted by user request."))
        results_queue.put((definition_type, thread_result_dict)) # Send partial results if any
    except Exception as e:
        status_queue.put(('error', f"[{thread_name}] CRITICAL ERROR: {e}"))
        status_queue.put(('error', traceback.format_exc()))
        error_count += 1 # Count this critical error
        results_queue.put((definition_type, thread_result_dict)) # Send partial results if any
    finally:
        # Always signal completion, even on error/abort
        results_queue.put((definition_type + "_DONE", error_count));
        # Clean up WebDriver
        if driver:
            status_queue.put(('status', f"[{thread_name}] Cleaning up WebDriver..."));
            try:
                driver.quit();
                status_queue.put(('status', f"[{thread_name}] WebDriver closed."))
            except Exception as q_err:
                status_queue.put(('error', f"[{thread_name}] Error quitting WebDriver: {q_err}"))
# --- END process_category_thread ---


# --- Run Application ---
if __name__ == "__main__":
    app = None; root = tk.Tk()
    app = HL7ParserApp(root) # Creates the app instance and sets the global 'app' variable
    try:
        root.mainloop()
    except KeyboardInterrupt:
        print("\nCtrl+C detected in main loop. Signaling stop...")
        if app:
            app.log_message("Shutdown requested (Ctrl+C)...", level="warning")
            if not app.stop_event.is_set():
                app.stop_event.set() # Ensure stop is signaled

            join_timeout = 10.0 # Total time for joining

            # Attempt to join orchestrator first
            if hasattr(app, 'orchestrator_thread') and app.orchestrator_thread and app.orchestrator_thread.is_alive():
                print(f"Waiting up to {join_timeout}s for orchestrator thread...")
                app.orchestrator_thread.join(timeout=join_timeout)
                if app.orchestrator_thread.is_alive():
                    print("Orchestrator thread did not exit gracefully.")
                else:
                    print("Orchestrator thread finished.")

            # Attempt to join any remaining workers (orchestrator should handle this, but as a fallback)
            threads_to_join = [t for t in app.worker_threads if t.is_alive()]
            if threads_to_join:
                print(f"Waiting for {len(threads_to_join)} worker threads (fallback)...")
                start_join_time = time.time()
                for t in threads_to_join:
                    remaining_time = max(0.1, join_timeout - (time.time() - start_join_time))
                    t.join(timeout=remaining_time)
                    if t.is_alive():
                         print(f"Worker thread {t.name} did not exit gracefully.")
            else:
                 # Check if orchestrator might have already joined them
                 if not (hasattr(app, 'orchestrator_thread') and app.orchestrator_thread and app.orchestrator_thread.is_alive()):
                      print("No active worker threads found to join.")


        print("Exiting application.")
        try:
            # Attempt to destroy the Tkinter window if it exists
            if root and root.winfo_exists():
                 root.destroy()
        except tk.TclError:
            pass # Ignore errors if window already destroyed
        sys.exit(0) # Ensure clean exit