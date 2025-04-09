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
SCREENSHOT_DIR = "screenshots_gui_hybrid_zoom" # Updated dir name
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

# analyze_screenshot_with_gemini() UNCHANGED from previous version
def analyze_screenshot_with_gemini(image_path, definition_name, definition_type):
    global app
    if not GEMINI_MODEL: print("Error: Gemini model not configured."); return None
    if app and app.stop_event.is_set(): print(f"  Skip Gemini: Stop requested for {definition_name}."); return None
    print(f"  Analyzing {definition_type} '{definition_name}' with Gemini..."); max_retries=3; retry_delay=3

    prompt = "" # Initialize prompt

    # --- *** CONSTRUCT PROMPT BASED ON DEFINITION TYPE *** ---
    if definition_type == 'Tables':
        prompt = f"""
        Analyze the screenshot showing the HL7 Table definition for ID '{definition_name}', version {HL7_VERSION}. The page might be zoomed out.
        Extract the 'Value' and 'Description' for each visible row in the table.
        Generate a JSON object strictly following these rules:

        1.  The **top-level key MUST be the numeric table ID as a JSON string** (e.g., "{definition_name}").
        2.  The value associated with this key MUST be an **array** of objects.
        3.  Each object in the array represents one row from the table and MUST contain only two keys:
            *   `value`: The exact string from the 'Value' column.
            *   `description`: The exact string from the 'Description' column.
        4.  **Do NOT include** 'separator', 'versions', 'parts', 'length', 'mandatory', 'repeats', or 'table' keys anywhere in the output for Tables.

        Example structure for table "0001":
        {{
          "0001": [
            {{ "value": "F", "description": "Female" }},
            {{ "value": "M", "description": "Male" }},
            {{ "value": "O", "description": "Other" }}
          ]
        }}

        Return ONLY the raw JSON object for table '{definition_name}' without any surrounding text or markdown formatting (` ```json ... ``` `).
        """
    elif definition_type == 'DataTypes' or definition_type == 'Segments':
        prompt = f"""
        Analyze the screenshot showing the HL7 {definition_type} definition for '{definition_name}', version {HL7_VERSION}. The page might be zoomed out.
        Extract the definition structure from the table shown.
        Generate a JSON object strictly following these rules:

        1.  Create a **top-level key which is the {definition_type} name** ('{definition_name}').
        2.  The value associated with this key MUST be an object.
        3.  This object MUST contain:
            *   `separator`: Set to '.' for Segments, '' for DataTypes.
            *   `versions`: An object containing a key for the HL7 version ('{HL7_VERSION}').
        4.  The '{HL7_VERSION}' object MUST contain:
            *   `appliesTo`: Set to 'equalOrGreater'.
            *   `totalFields`: The total count of rows extracted for the 'parts' array.
            *   `length`: The overall length shown at the top if available, otherwise -1.
            *   `parts`: An **array** of objects, one for each row in the definition table.
        5.  Each object within the 'parts' array represents a field/component and MUST contain:
            *   `name`: The field description (from 'DESCRIPTION' or similar column) converted to camelCase (e.g., 'setIdPv1', 'financialClassCode', 'identifierTypeCode'). Remove any prefix like 'PV1-1'. If the description is just '...', use a generic name like 'fieldN' where N is the row number.
            *   `type`: The exact string from the 'DATA TYPE' column (e.g., 'SI', 'IS', 'CWE', 'XPN', 'DTM').
            *   `length`: The numeric value from the 'LEN' or 'LENGTH' column. If it's '*' or empty/blank, use -1. Otherwise, use the integer value.
        6.  **Conditionally include** these keys in the part object ONLY if applicable:
            *   `mandatory`: Set to `true` ONLY if the 'OPT' or 'OPTIONALITY' column is 'R' (Required) or 'C' (Conditional). Omit this key if the column is 'O' (Optional), 'W' (Withdrawn), 'X' (Not Supported), or empty.
            *   `repeats`: Set to `true` ONLY if the 'RP/#MAX' or 'REPEATABILITY' column contains the infinity symbol '∞' or 'Y'. Omit this key otherwise.
            *   `table`: Set to the **numeric table ID as a JSON string** (e.g., "0004", "0125") ONLY if the 'TBL#' or 'TABLE' column contains a numeric value. Omit this key if the column is empty.

        Example structure for a Segment ('PV1') component part:
        {{ "name": "patientClass", "type": "IS", "length": 1, "mandatory": true, "table": "0004" }}

        Example structure for a DataType ('CX') component part:
        {{ "name": "assigningAuthority", "type": "HD", "length": 227, "table": "0363" }}

        Return ONLY the raw JSON object for '{definition_name}' without any surrounding text or markdown formatting (` ```json ... ``` `).
        """
    else:
        print(f"Error: Unknown definition_type '{definition_type}' for Gemini prompt.")
        return None

    # --- END PROMPT CONSTRUCTION ---

    for attempt in range(max_retries):
        if app and app.stop_event.is_set(): print(f"  Skip Gemini attempt {attempt+1}: Stop requested."); return None
        try:
            print(f"  Attempt {attempt + 1} for {definition_name}...") # Add attempt number to log
            img = Image.open(image_path)
            response = GEMINI_MODEL.generate_content([prompt, img]) # Send refined prompt

            # Robust JSON cleaning
            json_text = response.text.strip()
            # print(f"DEBUG: Raw Gemini Response:\n---\n{json_text}\n---") # Uncomment for deep debugging

            # Remove markdown fences first
            if json_text.startswith("```json"): json_text = json_text[7:]
            elif json_text.startswith("```"): json_text = json_text[3:]
            if json_text.endswith("```"): json_text = json_text[:-3]
            json_text = json_text.strip() # Strip again after removing fences

            # Attempt to parse
            parsed_json = json.loads(json_text)
            print(f"  Successfully parsed Gemini response for {definition_name}.")
            return parsed_json
        except json.JSONDecodeError as e:
            print(f"Error: Bad JSON from Gemini for '{definition_name}': {e}")
            # Log more context about the error
            err_line = getattr(e, 'lineno', 'N/A')
            err_col = getattr(e, 'colno', 'N/A')
            print(f"  Error at line ~{err_line}, column ~{err_col}")
            print(f"  Received Text: ```\n{response.text}\n```") # Log full raw response on error
            if attempt == max_retries - 1: return None # Return None only on final failed attempt
            print(f"  Retrying in {retry_delay}s...")
            time.sleep(retry_delay) # Wait before retrying
        except (google.api_core.exceptions.ResourceExhausted, google.api_core.exceptions.InternalServerError, google.api_core.exceptions.ServiceUnavailable, google.api_core.exceptions.GatewayTimeout) as e:
            print(f"Warn: Gemini API error attempt {attempt+1} for '{definition_name}': {e}")
            if attempt < max_retries-1:
                 print(f"  Retrying in {retry_delay}s...")
                 time.sleep(retry_delay)
            else:
                 print(f"Error: Max Gemini retries reached for '{definition_name}'."); return None
        except Exception as e:
            print(f"Error: Unexpected Gemini analysis error attempt {attempt+1} for '{definition_name}': {e}")
            print(traceback.format_exc()) # Print full traceback for unexpected errors
            return None # Don't retry unexpected errors usually

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

# Scraping Functions (Unchanged - they already scroll)
def scrape_table_details(driver, table_id, status_queue, stop_event):
    """Scrapes Value and Description columns for a Table definition."""
    status_queue.put(('debug', f"  Scraping Table {table_id}..."))
    table_data = []
    processed_values = set() # To handle potential duplicates during scroll
    table_locator = (By.XPATH, "//table[contains(@class, 'table-definition') and contains(@class, 'table')]//tbody")
    row_locator = (By.TAG_NAME, "tr")
    value_col_index = 0; desc_col_index = 1 # *** VERIFY INDICES ***
    last_height = driver.execute_script("return document.body.scrollHeight"); stale_scroll_count = 0; max_stale_scrolls = 3; pause_after_scroll = 0.1

    try:
        WebDriverWait(driver, 10).until(EC.presence_of_element_located(table_locator))
        while stale_scroll_count < max_stale_scrolls:
            if stop_event.is_set(): raise KeyboardInterrupt("Stop requested during table scroll scrape.")
            tbody = driver.find_element(*table_locator); rows = tbody.find_elements(*row_locator); newly_added_count = 0
            for row in rows:
                try:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if len(cells) > max(value_col_index, desc_col_index):
                        value_text = cells[value_col_index].text.strip()
                        if value_text and value_text not in processed_values:
                            processed_values.add(value_text); desc_text = cells[desc_col_index].text.strip(); table_data.append({"value": value_text, "description": desc_text}); newly_added_count += 1
                        elif not value_text: status_queue.put(('debug', f"    Skipping row with empty value in Table {table_id}"))
                except StaleElementReferenceException: status_queue.put(('warning', f"    Stale row encountered in Table {table_id} scrape, continuing...")); continue
                except Exception as cell_err: status_queue.put(('warning', f"    Error processing row/cell in Table {table_id}: {cell_err}"))
            status_queue.put(('debug', f"    Scraped pass added {newly_added_count} new rows for Table {table_id}"))
            driver.execute_script("window.scrollBy(0, 600);"); time.sleep(pause_after_scroll); new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height: stale_scroll_count += 1
            else: stale_scroll_count = 0
            last_height = new_height
    except TimeoutException: status_queue.put(('error', f"  Timeout finding table body for Table {table_id}.")); return None
    except NoSuchElementException: status_queue.put(('error', f"  Could not find table body for Table {table_id}.")); return None
    except KeyboardInterrupt: raise
    except Exception as e: status_queue.put(('error', f"  Unexpected error scraping Table {table_id}: {e}")); status_queue.put(('error', traceback.format_exc())); return None
    if not table_data: status_queue.put(('warning', f"  No data scraped for Table {table_id}.")); return None
    return {str(table_id): table_data}

def scrape_segment_or_datatype_details(driver, definition_type, definition_name, status_queue, stop_event):
    """Scrapes details for Segment or DataType definitions."""
    status_queue.put(('debug', f"  Scraping {definition_type} {definition_name}..."))
    parts_data = []
    processed_row_identifiers = set() # Use first column (e.g., "PV1-1") as identifier

    # --- SELECTORS AND COLUMN INDICES (*** MUST VERIFY THESE BY INSPECTING HTML ***) ---
    table_locator = (By.XPATH, "//table[contains(@class, 'table-definition') and contains(@class, 'table')]//tbody")
    row_locator = (By.TAG_NAME, "tr")
    # Assuming column order: Seq/Component#, Description, DataType, Len, Opt, RP/#Max, Tbl#
    seq_col_index = 0     # Example: Column with "PV1-1", "1", etc.
    desc_col_index = 1    # Example: Column with "Set ID - PV1"
    type_col_index = 2    # Example: Column with "SI"
    len_col_index = 3     # Example: Column with "4"
    opt_col_index = 4     # Example: Column with "R", "O", "C"
    repeat_col_index = 5  # Example: Column with "Y" or "∞"
    table_col_index = 6   # Example: Column with "0004"
    # --- ---

    overall_length = -1 # Default overall length

    last_height = driver.execute_script("return document.body.scrollHeight")
    stale_scroll_count = 0
    max_stale_scrolls = 3
    pause_after_scroll = 0.1

    try:
        # --- Try to get overall length (might be outside the main table) ---
        try:
             # Adjust selector based on where the overall length might be displayed
             length_element = driver.find_element(By.XPATH, "//div[contains(@class,'DefinitionPage_definitionContent')]//span[contains(text(),'Length:')]/following-sibling::span")
             length_text = length_element.text.strip()
             if length_text.isdigit():
                 overall_length = int(length_text)
                 status_queue.put(('debug', f"    Found overall length: {overall_length}"))
             else:
                 status_queue.put(('debug', f"    Found length text '{length_text}', couldn't parse as number."))
        except NoSuchElementException:
             status_queue.put(('debug', "    Overall length element not found."))
        except Exception as len_err:
             status_queue.put(('warning', f"    Error getting overall length: {len_err}"))
        # --- End Overall Length ---

        WebDriverWait(driver, 10).until(EC.presence_of_element_located(table_locator))

        while stale_scroll_count < max_stale_scrolls:
            if stop_event.is_set(): raise KeyboardInterrupt("Stop requested during detail scroll scrape.")

            tbody = driver.find_element(*table_locator)
            rows = tbody.find_elements(*row_locator)
            newly_added_count = 0

            for row in rows:
                part = {}
                row_identifier = None
                try:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if len(cells) > max(seq_col_index, desc_col_index, type_col_index, len_col_index, opt_col_index, repeat_col_index, table_col_index):

                        # Get unique identifier for the row (e.g., sequence number)
                        row_identifier = cells[seq_col_index].text.strip()
                        if not row_identifier: continue # Skip rows without identifier

                        if row_identifier in processed_row_identifiers: continue # Skip already processed

                        processed_row_identifiers.add(row_identifier)

                        # Extract Data
                        desc_text = cells[desc_col_index].text.strip()
                        type_text = cells[type_col_index].text.strip()
                        len_text = cells[len_col_index].text.strip()
                        opt_text = cells[opt_col_index].text.strip().upper() # Normalize
                        repeat_text = cells[repeat_col_index].text.strip().upper() # Normalize
                        table_text = cells[table_col_index].text.strip()

                        # Build Part Dictionary
                        part['name'] = convert_to_camel_case(desc_text)
                        part['type'] = type_text if type_text else "Unknown" # Handle empty type

                        # Parse Length
                        try:
                            part['length'] = int(len_text) if len_text.isdigit() else -1
                        except ValueError:
                            part['length'] = -1

                        # Parse Optionality
                        if opt_text in ['R', 'C']: part['mandatory'] = True

                        # Parse Repeatability
                        if 'Y' in repeat_text or '∞' in repeat_text: part['repeats'] = True

                        # Parse Table ID
                        if table_text.isdigit(): part['table'] = table_text
                        # --- FIX: Added colon here ---
                        elif '.' in table_text: parts = table_text.split('.');
                        if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit(): part['table'] = table_text
                        # --- END FIX ---

                        parts_data.append(part)
                        newly_added_count += 1
                    else:
                        # Log if a row doesn't have enough columns, might indicate table structure issues
                        row_text_snippet = row.text[:50].replace('\n', ' ') if row.text else "EMPTY ROW"
                        status_queue.put(('debug', f"    Skipping row with insufficient columns ({len(cells)}): '{row_text_snippet}'... in {definition_name}"))

                except StaleElementReferenceException:
                    status_queue.put(('warning', f"    Stale row encountered in {definition_name} scrape, continuing..."))
                    # Remove potentially partially added identifier if stale occurred mid-processing
                    if row_identifier and row_identifier in processed_row_identifiers and not part:
                         processed_row_identifiers.remove(row_identifier)
                    continue
                except Exception as cell_err:
                    status_queue.put(('warning', f"    Error processing row/cell in {definition_name} (ID: {row_identifier}): {cell_err}"))
                    # Attempt to remove identifier if error occurred after adding it but before finishing part
                    if row_identifier and row_identifier in processed_row_identifiers and not part:
                         processed_row_identifiers.remove(row_identifier)

            status_queue.put(('debug', f"    Scraped pass added {newly_added_count} new parts for {definition_name}"))

            # Scroll and check height
            driver.execute_script("window.scrollBy(0, 600);")
            time.sleep(pause_after_scroll)
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height: stale_scroll_count += 1
            else: stale_scroll_count = 0
            last_height = new_height

    except TimeoutException: status_queue.put(('error', f"  Timeout finding table body for {definition_name}.")); return None
    except NoSuchElementException: status_queue.put(('error', f"  Could not find table body for {definition_name}.")); return None
    except KeyboardInterrupt: raise
    except Exception as e: status_queue.put(('error', f"  Unexpected error scraping {definition_name}: {e}")); status_queue.put(('error', traceback.format_exc())); return None

    if not parts_data:
        status_queue.put(('warning', f"  No parts data scraped for {definition_name}."))
        return None

    # Add standard segment part if it's a segment
    if definition_type == "Segments":
        hl7_seg_part = {"mandatory": True, "name": "hl7SegmentName", "type": "ST", "table": "0076", "length": 3}
        if not parts_data or parts_data[0].get("name") != "hl7SegmentName":
            parts_data.insert(0, hl7_seg_part)
            status_queue.put(('debug', f"  Prepended standard part for Segment {definition_name}"))

    # Assemble final structure
    final_structure = {
        "separator": "." if definition_type == "Segments" else "",
        "versions": {
            HL7_VERSION: {
                "appliesTo": "equalOrGreater",
                "totalFields": len(parts_data),
                "length": overall_length,
                "parts": parts_data
            }
        }
    }
    return {definition_name: final_structure}

# --- REVISED: process_definition_page (Applies Zoom in AI Fallback) ---
def process_definition_page(driver, definition_type, definition_name, status_queue, stop_event):
    """Attempts to scrape data directly. If fails, falls back to screenshot (zoomed out) + AI."""
    url = f"{BASE_URL}/{definition_type}/{definition_name}"
    status_queue.put(('status', f"Processing {definition_type}: {definition_name}"))
    if stop_event.is_set(): return None, definition_name

    scraped_data = None; ai_data = None; screenshot_path = None
    final_data_source = "None"; final_data = None

    # 1. Navigate
    try: driver.get(url); time.sleep(0.2)
    except WebDriverException as nav_err: status_queue.put(('error', f"Error navigating to {url}: {nav_err}")); return None, definition_name

    # 2. Attempt Scraping
    try:
        status_queue.put(('status', f"  Attempting direct scraping..."))
        if definition_type == "Tables": scraped_data = scrape_table_details(driver, definition_name, status_queue, stop_event)
        elif definition_type in ["DataTypes", "Segments"]: scraped_data = scrape_segment_or_datatype_details(driver, definition_type, definition_name, status_queue, stop_event)
        else: status_queue.put(('warning', f"  Unsupported type for scraping: {definition_type}"))

        if scraped_data and isinstance(scraped_data, dict) and scraped_data:
             data_key = next(iter(scraped_data)); data_value = scraped_data[data_key]; valid_scrape = False
             if definition_type == "Tables" and isinstance(data_value, list): valid_scrape = True
             elif definition_type in ["DataTypes", "Segments"] and isinstance(data_value, dict) and "versions" in data_value: valid_scrape = True
             if valid_scrape: status_queue.put(('status', f"  Scraping successful and basic validation passed.")); final_data_source = "Scraping"; final_data = scraped_data
             else: status_queue.put(('warning', f"  Scraping result failed basic validation. Proceeding to AI fallback.")); scraped_data = None
        elif scraped_data is None: status_queue.put(('warning', f"  Scraping function returned None."))
        else: status_queue.put(('warning', f"  Unexpected scraping result type: {type(scraped_data)}. Proceeding to AI fallback.")); scraped_data = None
    except KeyboardInterrupt: status_queue.put(('warning', "Stop requested during scraping attempt.")); return None, definition_name
    except Exception as scrape_err: status_queue.put(('warning', f"  Scraping failed: {scrape_err}. Proceeding to AI fallback.")); status_queue.put(('debug', traceback.format_exc())); scraped_data = None

    # 3. Fallback to Screenshot and AI Analysis (with ZOOM)
    if final_data is None and not stop_event.is_set():
        status_queue.put(('status', f"  Falling back to Screenshot + AI Analysis (Zoomed)..."))
        original_zoom = "100%" # Assume default
        try:
            # --- ZOOM OUT ---
            status_queue.put(('debug', "    Setting zoom to 50%"))
            driver.execute_script("document.body.style.zoom='50%'")
            time.sleep(0.5) # Give browser time to re-render zoomed out

            # --- Screenshot Logic (Scrolling first, then screenshot) ---
            pause_after_detail_scroll = 0.2; scroll_amount_detail = 800 # Use same scroll params
            status_queue.put(('status', "    Scrolling detail page (AI fallback, zoomed)..."))
            # Scroll loop... (identical logic to previous version)
            last_height = driver.execute_script("return document.body.scrollHeight"); stale_height_count = 0; max_stale_detail_scrolls = 3; scroll_attempts = 0; max_scroll_attempts = 25
            while stale_height_count < max_stale_detail_scrolls and scroll_attempts < max_scroll_attempts:
                 if stop_event.is_set(): raise KeyboardInterrupt("Stop requested during detail scroll (AI fallback).")
                 driver.execute_script(f"window.scrollBy(0, {scroll_amount_detail * 2});"); time.sleep(pause_after_detail_scroll) # Scroll more since zoomed
                 new_height = driver.execute_script("return document.body.scrollHeight");
                 if new_height == last_height: stale_height_count += 1; # ... (rest of scroll height check logic) ...
                 else: stale_height_count = 0
                 last_height = new_height; scroll_attempts += 1;
            status_queue.put(('status', "    Detail page scroll complete (AI fallback, zoomed)."))
            status_queue.put(('status', "    Scrolling back to top (AI fallback)...")); driver.execute_script("window.scrollTo(0, 0);"); time.sleep(0.3) # Slightly longer pause after zoom scroll
            if stop_event.is_set(): raise KeyboardInterrupt("Stop requested before screenshot (AI fallback).")

            script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd(); screenshot_full_dir = os.path.join(script_dir, SCREENSHOT_DIR)
            if not os.path.exists(screenshot_full_dir): os.makedirs(screenshot_full_dir)
            screenshot_filename = f"{definition_type}_{definition_name}_AI_fallback_zoomed.png"; screenshot_path = os.path.join(screenshot_full_dir, screenshot_filename)
            status_queue.put(('status', "    Attempting screenshot (AI fallback, zoomed)..."))
            screenshot_saved = driver.save_screenshot(screenshot_path) # Use simple save_screenshot

            # --- AI Analysis ---
            if screenshot_saved:
                status_queue.put(('status', f"    Screenshot saved: {screenshot_filename}"))
                ai_data = analyze_screenshot_with_gemini(screenshot_path, definition_name, definition_type)
                if ai_data: final_data_source = "AI Fallback (Zoomed)"; final_data = ai_data
                else: status_queue.put(('error', f"    AI Analysis failed for zoomed fallback on {definition_name}."))
            else: status_queue.put(('error', f"    Failed to save zoomed screenshot for AI fallback: {definition_name}"))

        except KeyboardInterrupt: status_queue.put(('warning', "Stop requested during AI fallback.")); return None, definition_name
        except WebDriverException as wd_err: status_queue.put(('error', f"WebDriver error during AI fallback on page {url}: {wd_err}")); return None, definition_name
        except Exception as e: status_queue.put(('error', f"Error during AI fallback processing page {url}: {e}")); status_queue.put(('error', traceback.format_exc())); return None, definition_name
        finally:
             # --- ZOOM RESET (CRITICAL) ---
             try:
                 status_queue.put(('debug',"    Resetting zoom to 100%"))
                 driver.execute_script("document.body.style.zoom='100%'")
                 time.sleep(0.2)
             except Exception as zoom_err:
                  status_queue.put(('warning', f"    Could not reset zoom for {definition_name}: {zoom_err}"))
             # --- END ZOOM RESET ---

    # 4. Log final source and return result
    status_queue.put(('status', f"  Finished processing {definition_name}. Source: {final_data_source}"))
    time.sleep(0.1)
    return final_data, definition_name
# --- END REVISED process_definition_page ---

# clear_screenshot_folder() UNCHANGED
def clear_screenshot_folder(status_queue):
    script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
    dir_path = os.path.join(script_dir, SCREENSHOT_DIR)
    if os.path.exists(dir_path):
        status_queue.put(('status', f"Cleaning up screenshot directory: {dir_path}"))
        try:
            if os.path.isdir(dir_path): shutil.rmtree(dir_path); os.makedirs(dir_path); status_queue.put(('status', "Screenshot directory cleared and recreated."))
            else: status_queue.put(('error', f"Path exists but is not a directory: {dir_path}"))
        except OSError as e: status_queue.put(('error', f"Error clearing screenshot directory {dir_path}: {e}"))
        except Exception as e: status_queue.put(('error', f"Unexpected error clearing screenshot directory: {e}"))
    else: status_queue.put(('status', "Screenshot directory does not exist, nothing to clear."))

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

# process_category_thread() UNCHANGED (calls revised process_definition_page)
def scrape_segment_or_datatype_details(driver, definition_type, definition_name, status_queue, stop_event):
    """Scrapes details for Segment or DataType definitions."""
    status_queue.put(('debug', f"  Scraping {definition_type} {definition_name}..."))
    parts_data = []
    processed_row_identifiers = set() # Use first column (e.g., "PV1-1") as identifier

    # --- SELECTORS AND COLUMN INDICES (*** MUST VERIFY THESE BY INSPECTING HTML ***) ---
    table_locator = (By.XPATH, "//table[contains(@class, 'table-definition') and contains(@class, 'table')]//tbody")
    row_locator = (By.TAG_NAME, "tr")
    # Assuming column order: Seq/Component#, Description, DataType, Len, Opt, RP/#Max, Tbl#
    seq_col_index = 0     # Example: Column with "PV1-1", "1", etc.
    desc_col_index = 1    # Example: Column with "Set ID - PV1"
    type_col_index = 2    # Example: Column with "SI"
    len_col_index = 3     # Example: Column with "4"
    opt_col_index = 4     # Example: Column with "R", "O", "C"
    repeat_col_index = 5  # Example: Column with "Y" or "∞"
    table_col_index = 6   # Example: Column with "0004"
    # --- ---

    overall_length = -1 # Default overall length

    last_height = driver.execute_script("return document.body.scrollHeight")
    stale_scroll_count = 0
    max_stale_scrolls = 3
    pause_after_scroll = 0.1

    try:
        # --- Try to get overall length (might be outside the main table) ---
        try:
             # Adjust selector based on where the overall length might be displayed
             length_element = driver.find_element(By.XPATH, "//div[contains(@class,'DefinitionPage_definitionContent')]//span[contains(text(),'Length:')]/following-sibling::span")
             length_text = length_element.text.strip()
             if length_text.isdigit():
                 overall_length = int(length_text)
                 status_queue.put(('debug', f"    Found overall length: {overall_length}"))
             else:
                 status_queue.put(('debug', f"    Found length text '{length_text}', couldn't parse as number."))
        except NoSuchElementException:
             status_queue.put(('debug', "    Overall length element not found."))
        except Exception as len_err:
             status_queue.put(('warning', f"    Error getting overall length: {len_err}"))
        # --- End Overall Length ---

        WebDriverWait(driver, 10).until(EC.presence_of_element_located(table_locator))

        while stale_scroll_count < max_stale_scrolls:
            if stop_event.is_set(): raise KeyboardInterrupt("Stop requested during detail scroll scrape.")

            tbody = driver.find_element(*table_locator)
            rows = tbody.find_elements(*row_locator)
            newly_added_count = 0

            for row in rows:
                part = {}
                row_identifier = None
                try:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if len(cells) > max(seq_col_index, desc_col_index, type_col_index, len_col_index, opt_col_index, repeat_col_index, table_col_index):

                        # Get unique identifier for the row (e.g., sequence number)
                        row_identifier = cells[seq_col_index].text.strip()
                        if not row_identifier: continue # Skip rows without identifier

                        if row_identifier in processed_row_identifiers: continue # Skip already processed

                        processed_row_identifiers.add(row_identifier)

                        # Extract Data
                        desc_text = cells[desc_col_index].text.strip()
                        type_text = cells[type_col_index].text.strip()
                        len_text = cells[len_col_index].text.strip()
                        opt_text = cells[opt_col_index].text.strip().upper() # Normalize
                        repeat_text = cells[repeat_col_index].text.strip().upper() # Normalize
                        table_text = cells[table_col_index].text.strip()

                        # Build Part Dictionary
                        part['name'] = convert_to_camel_case(desc_text)
                        part['type'] = type_text if type_text else "Unknown" # Handle empty type

                        # Parse Length
                        try:
                            part['length'] = int(len_text) if len_text.isdigit() else -1
                        except ValueError:
                            part['length'] = -1

                        # Parse Optionality
                        if opt_text in ['R', 'C']: part['mandatory'] = True

                        # Parse Repeatability
                        if 'Y' in repeat_text or '∞' in repeat_text: part['repeats'] = True

                        # Parse Table ID
                        if table_text.isdigit(): part['table'] = table_text
                        # --- FIX: Added colon here ---
                        elif '.' in table_text: parts = table_text.split('.');
                        if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit(): part['table'] = table_text
                        # --- END FIX ---

                        parts_data.append(part)
                        newly_added_count += 1
                    else:
                        # Log if a row doesn't have enough columns, might indicate table structure issues
                        row_text_snippet = row.text[:50].replace('\n', ' ') if row.text else "EMPTY ROW"
                        status_queue.put(('debug', f"    Skipping row with insufficient columns ({len(cells)}): '{row_text_snippet}'... in {definition_name}"))

                except StaleElementReferenceException:
                    status_queue.put(('warning', f"    Stale row encountered in {definition_name} scrape, continuing..."))
                    # Remove potentially partially added identifier if stale occurred mid-processing
                    if row_identifier and row_identifier in processed_row_identifiers and not part:
                         processed_row_identifiers.remove(row_identifier)
                    continue
                except Exception as cell_err:
                    status_queue.put(('warning', f"    Error processing row/cell in {definition_name} (ID: {row_identifier}): {cell_err}"))
                    # Attempt to remove identifier if error occurred after adding it but before finishing part
                    if row_identifier and row_identifier in processed_row_identifiers and not part:
                         processed_row_identifiers.remove(row_identifier)

            status_queue.put(('debug', f"    Scraped pass added {newly_added_count} new parts for {definition_name}"))

            # Scroll and check height
            driver.execute_script("window.scrollBy(0, 600);")
            time.sleep(pause_after_scroll)
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height: stale_scroll_count += 1
            else: stale_scroll_count = 0
            last_height = new_height

    except TimeoutException: status_queue.put(('error', f"  Timeout finding table body for {definition_name}.")); return None
    except NoSuchElementException: status_queue.put(('error', f"  Could not find table body for {definition_name}.")); return None
    except KeyboardInterrupt: raise
    except Exception as e: status_queue.put(('error', f"  Unexpected error scraping {definition_name}: {e}")); status_queue.put(('error', traceback.format_exc())); return None

    if not parts_data:
        status_queue.put(('warning', f"  No parts data scraped for {definition_name}."))
        return None

    # Add standard segment part if it's a segment
    if definition_type == "Segments":
        hl7_seg_part = {"mandatory": True, "name": "hl7SegmentName", "type": "ST", "table": "0076", "length": 3}
        if not parts_data or parts_data[0].get("name") != "hl7SegmentName":
            parts_data.insert(0, hl7_seg_part)
            status_queue.put(('debug', f"  Prepended standard part for Segment {definition_name}"))

    # Assemble final structure
    final_structure = {
        "separator": "." if definition_type == "Segments" else "",
        "versions": {
            HL7_VERSION: {
                "appliesTo": "equalOrGreater",
                "totalFields": len(parts_data),
                "length": overall_length,
                "parts": parts_data
            }
        }
    }
    return {definition_name: final_structure}

# GUI Class HL7ParserApp and run_parser_orchestrator (UNCHANGED)
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
                 if pb: pb.config(maximum=total_val, value=current);
                 if lbl: lbl.config(text=count_text); return
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
        elif self.start_button['state'] == tk.DISABLED: self.master.after(500, self.check_queue)

    def start_processing(self): # UNCHANGED
        if not load_api_key(): return;
        if not configure_gemini(): return;
        orchestrator_alive = self.orchestrator_thread and self.orchestrator_thread.is_alive()
        if orchestrator_alive or any(t.is_alive() for t in self.worker_threads): messagebox.showwarning("Busy", "Processing is already in progress."); return
        self.stop_event.clear(); self.start_button.config(state=tk.DISABLED); self.stop_button.config(state=tk.NORMAL)
        self.log_message("Starting concurrent processing (Scrape+AI Fallback/Zoom)..."); self.log_message("Using headless browsers and caching...")
        self.grand_total_items = 0; self.processed_items_count = 0; self.list_counts_received.clear()
        self.update_progress("tables",0,1); self.lbl_tables_count.config(text="0/0"); self.update_progress("datatypes",0,1); self.lbl_datatypes_count.config(text="0/0"); self.update_progress("segments",0,1); self.lbl_segments_count.config(text="0/0"); self.update_progress("overall",0,1); self.lbl_overall_perc.config(text="0%")
        self.worker_threads = []
        results_queue = queue.Queue()
        self.orchestrator_thread = threading.Thread(target=self.run_parser_orchestrator, args=(results_queue, self.stop_event), daemon=True)
        self.orchestrator_thread.start(); self.master.after(100, self.check_queue)

    def stop_processing(self): # UNCHANGED
        orchestrator_alive = hasattr(self, 'orchestrator_thread') and self.orchestrator_thread.is_alive(); workers_alive = any(t.is_alive() for t in self.worker_threads)
        if workers_alive or orchestrator_alive:
            if not self.stop_event.is_set(): self.log_message("Stop request received. Signaling background threads...", level="warning"); self.stop_event.set()
            self.stop_button.config(state=tk.DISABLED)
        else: self.log_message("Stop requested, but no active process found.", level="info"); self.stop_button.config(state=tk.DISABLED); self.start_button.config(state=tk.NORMAL)

    def run_parser_orchestrator(self, results_queue, stop_event): # UNCHANGED (merging logic handles scrape/AI results)
        categories = ["Tables", "DataTypes", "Segments"]; loaded_definitions = load_existing_definitions(OUTPUT_JSON_FILE, self.status_queue)
        all_new_results = {"Tables": {}, "DataTypes": {}, "Segments": {}}; thread_errors = {"Tables": 0, "DataTypes": 0, "Segments": 0}
        threads_finished = set(); total_error_count = 0; self.worker_threads = []
        try:
            self.status_queue.put(('status', "Starting worker threads..."))
            for category in categories:
                if stop_event.is_set(): break
                worker = threading.Thread(target=process_category_thread, args=(category, results_queue, self.status_queue, stop_event, loaded_definitions), daemon=True, name=f"Worker-{category}")
                self.worker_threads.append(worker); worker.start()
            if stop_event.is_set(): raise KeyboardInterrupt("Stop requested during thread startup.")
            self.status_queue.put(('status', "Waiting for results from worker threads..."))
            while len(threads_finished) < len(categories):
                if stop_event.is_set() and not any(t.is_alive() for t in self.worker_threads): self.status_queue.put(('warning', "Stopping result collection early due to stop signal.")); break
                try:
                    result_type, data = results_queue.get(timeout=1.0)
                    if result_type.endswith("_DONE"):
                        category = result_type.replace("_DONE", "");
                        if category in categories: threads_finished.add(category); thread_errors[category] = data; total_error_count += data; self.status_queue.put(('status', f"Worker thread for {category} finished reporting {data} errors."))
                        else: self.status_queue.put(('warning', f"Received unexpected DONE signal: {result_type}"))
                    elif result_type in categories: all_new_results[result_type].update(data); self.status_queue.put(('debug', f"Received {len(data)} new results for {result_type}."))
                    else: self.status_queue.put(('warning', f"Received unknown result type: {result_type}"))
                except queue.Empty:
                    if stop_event.is_set(): self.status_queue.put(('warning', "Stop signal detected while waiting for results."))
                    continue
            self.status_queue.put(('status', "All worker threads have reported completion or stop signal received."))
        except KeyboardInterrupt: self.status_queue.put(('warning', "\nOrchestrator aborted by user request."))
        except Exception as e: self.status_queue.put(('error', f"Orchestrator CRITICAL ERROR: {e}")); self.status_queue.put(('error', traceback.format_exc())); total_error_count += 1
        finally:
            self.status_queue.put(('status', "Ensuring all worker threads have terminated...")); join_timeout = 10.0
            for t in self.worker_threads: t.join(timeout=join_timeout); # ... (rest of joining logic) ...
            self.status_queue.put(('status', "Worker thread joining complete."))
            final_definitions = loaded_definitions; processed_segments_for_hl7 = []
            if not stop_event.is_set() or any(all_new_results.values()):
                 self.status_queue.put(('status', "Merging cached and new results..."))
                 final_definitions.setdefault("tables", {}).update(all_new_results.get("Tables", {}))
                 final_definitions.setdefault("dataTypes", {}).update(all_new_results.get("DataTypes", {}))
                 final_definitions["dataTypes"].update(all_new_results.get("Segments", {}))
                 processed_segments_for_hl7 = [k for k, v in final_definitions["dataTypes"].items() if v.get('separator') == '.']
                 self.status_queue.put(('status', "\n--- Building HL7 Structure ---")); hl7_parts=[]; common_order=["MSH","PID","PV1","OBR","OBX"];
                 ordered=[s for s in common_order if s in processed_segments_for_hl7]; other=sorted([s for s in processed_segments_for_hl7 if s not in common_order])
                 final_segment_order = ordered + other
                 if not final_segment_order: self.status_queue.put(('warning', "No segments found in final combined data to build HL7 structure."))
                 else:
                    for seg_name in final_segment_order:
                        seg_def = final_definitions["dataTypes"].get(seg_name); is_mand = False; repeats = False; length = -1
                        if seg_name == "MSH": is_mand = True
                        else: repeats = True
                        if seg_def and 'versions' in seg_def: version_key = next(iter(seg_def.get('versions', {})), None);
                        if version_key: length = seg_def['versions'][version_key].get('length', -1)
                        part={"name":seg_name.lower(),"type":seg_name,"length": length};
                        if is_mand: part.update({"mandatory":True});
                        if repeats: part.update({"repeats":True}); hl7_parts.append(part)
                    final_definitions.setdefault("HL7", {}).update({ "separator":"\r", "partId":"type", "versions":{ HL7_VERSION:{"appliesTo":"equalOrGreater","length":-1,"parts":hl7_parts}}})
                    self.status_queue.put(('status', f"HL7 structure updated/built with {len(hl7_parts)} segments."))
            if not stop_event.is_set():
                self.status_queue.put(('status', f"\nWriting final definitions to {OUTPUT_JSON_FILE}"))
                script_dir=os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd(); output_path=os.path.join(script_dir,OUTPUT_JSON_FILE)
                try:
                    with open(output_path,'w',encoding='utf-8') as f: json.dump(final_definitions,f,indent=2,ensure_ascii=False)
                    self.status_queue.put(('status', "JSON file written successfully."))
                    if total_error_count == 0: self.status_queue.put(('status', "No errors recorded, attempting screenshot cleanup.")); clear_screenshot_folder(self.status_queue)
                    else: self.status_queue.put(('warning', f"Errors ({total_error_count}) occurred, screenshots in '{SCREENSHOT_DIR}' were NOT deleted."))
                except Exception as e: self.status_queue.put(('error', f"Failed to write JSON file: {e}")); total_error_count+=1
            else: self.status_queue.put(('warning', f"Processing stopped, final JSON file '{OUTPUT_JSON_FILE}' reflects merged cache and any new results obtained before stopping."))
            self.status_queue.put(('finished', total_error_count))

# --- Run Application ---
if __name__ == "__main__":
    app = None; root = tk.Tk()
    app = HL7ParserApp(root)
    try: root.mainloop()
    except KeyboardInterrupt:
        print("\nCtrl+C detected in main loop. Signaling stop...")
        if app:
            app.log_message("Shutdown requested (Ctrl+C)...", level="warning"); app.stop_event.set()
            join_timeout = 10.0
            if hasattr(app, 'orchestrator_thread') and app.orchestrator_thread.is_alive(): print("Waiting for orchestrator thread..."); app.orchestrator_thread.join(timeout=join_timeout); # ... (rest of joining logic) ...
            threads_to_join = app.worker_threads
            if threads_to_join: print(f"Waiting for {len(threads_to_join)} worker threads..."); # ... (rest of joining logic) ...
            else: print("No worker threads found to join.")
        print("Exiting application.")
        try: root.destroy()
        except tk.TclError: pass
        sys.exit(0)