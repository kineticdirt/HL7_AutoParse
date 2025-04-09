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
SCREENSHOT_DIR = "screenshots_gui"
API_KEY_FILE = "api_key.txt"
HL7_VERSION = "2.6"
GEMINI_API_KEY = None
GEMINI_MODEL = None
# Global variable to hold the app instance for access in functions
app = None

# --- Gemini API Functions ---
# load_api_key() and configure_gemini() UNCHANGED from your provided code
def load_api_key():
    global GEMINI_API_KEY;
    try:
        # Handle potential issues if __file__ is not defined (e.g., interactive session)
        try:
            script_dir = os.path.dirname(os.path.abspath(__file__))
        except NameError:
             script_dir = os.getcwd() # Fallback to current working directory
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
        GEMINI_MODEL = genai.GenerativeModel('gemini-1.5-flash')
        print("Gemini configured successfully."); return True
    except Exception as e: messagebox.showerror("Gemini Config Error", f"Failed to configure Gemini: {e}"); return False

# analyze_screenshot_with_gemini() UNCHANGED from previous revision
def analyze_screenshot_with_gemini(image_path, definition_name, definition_type):
    global app
    if not GEMINI_MODEL: print("Error: Gemini model not configured."); return None
    if app and app.stop_event.is_set(): print(f"  Skip Gemini: Stop requested for {definition_name}."); return None
    print(f"  Analyzing {definition_type} '{definition_name}' with Gemini..."); max_retries=3; retry_delay=3 # Use 3s retry delay

    prompt = ""

    if definition_type == 'Tables':
        prompt = f"""
        Analyze the screenshot showing the HL7 Table definition for ID '{definition_name}', version {HL7_VERSION}.
        Extract the 'Value' and 'Description' for each row in the table.
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
        Analyze the screenshot showing the HL7 {definition_type} definition for '{definition_name}', version {HL7_VERSION}.
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

    for attempt in range(max_retries):
        if app and app.stop_event.is_set(): print(f"  Skip Gemini attempt {attempt+1}: Stop requested."); return None
        try:
            print(f"  Attempt {attempt + 1} for {definition_name}...")
            img = Image.open(image_path)
            response = GEMINI_MODEL.generate_content([prompt, img])

            json_text = response.text.strip()
            if json_text.startswith("```json"): json_text = json_text[7:]
            elif json_text.startswith("```"): json_text = json_text[3:]
            if json_text.endswith("```"): json_text = json_text[:-3]
            json_text = json_text.strip()

            parsed_json = json.loads(json_text)
            print(f"  Successfully parsed Gemini response for {definition_name}.")
            return parsed_json
        except json.JSONDecodeError as e:
            print(f"Error: Bad JSON from Gemini for '{definition_name}': {e}")
            err_line = getattr(e, 'lineno', 'N/A'); err_col = getattr(e, 'colno', 'N/A')
            print(f"  Error at line ~{err_line}, column ~{err_col}")
            print(f"  Received Text: ```\n{response.text}\n```")
            if attempt == max_retries - 1: return None
            print(f"  Retrying in {retry_delay}s...")
            time.sleep(retry_delay)
        except (google.api_core.exceptions.ResourceExhausted, google.api_core.exceptions.InternalServerError, google.api_core.exceptions.ServiceUnavailable, google.api_core.exceptions.GatewayTimeout) as e:
            print(f"Warn: Gemini API error attempt {attempt+1} for '{definition_name}': {e}")
            if attempt < max_retries-1:
                 print(f"  Retrying in {retry_delay}s...")
                 time.sleep(retry_delay)
            else:
                 print(f"Error: Max Gemini retries reached for '{definition_name}'."); return None
        except Exception as e:
            print(f"Error: Unexpected Gemini analysis error attempt {attempt+1} for '{definition_name}': {e}")
            print(traceback.format_exc())
            return None

    return None

# --- Selenium Functions ---
# --- REVISED setup_driver (Added Headless Mode) ---
def setup_driver():
    """Sets up the Selenium WebDriver with headless option."""
    options = webdriver.ChromeOptions()
    options.add_argument("--headless") # Enable headless mode
    options.add_argument("--disable-gpu") # Often needed for headless
    options.add_argument("--window-size=1920,1080") # Specify window size for headless
    options.add_argument("--log-level=3") # Suppress console logs
    options.add_experimental_option('excludeSwitches', ['enable-logging']) # Suppress DevTools logs

    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        # Implicit wait can still be useful, though explicit waits are better
        driver.implicitly_wait(3) # Reduced implicit wait slightly
        return driver
    except WebDriverException as e:
        error_msg = f"Failed WebDriver init: {e}\n"
        # Check for common errors
        if "net::ERR_INTERNET_DISCONNECTED" in str(e):
             error_msg += "Please check your internet connection.\n"
        elif "session not created" in str(e) and "version is" in str(e):
             error_msg += "ChromeDriver version might be incompatible with your Chrome browser. Try clearing the .wdm cache (see log/docs).\n"
        elif "user data directory is already in use" in str(e):
             error_msg += "Another Chrome process might be using the profile. Close all Chrome instances (including background tasks) and try again.\n"
        else:
             error_msg += "Check Chrome install/updates/antivirus. Clearing .wdm cache might help.\n"
        messagebox.showerror("WebDriver Error", error_msg)
        # Also print to console for non-GUI runs or if messagebox fails
        print(f"WebDriver Error:\n{error_msg}")
        return None
    except Exception as e:
        messagebox.showerror("WebDriver Error", f"Unexpected WebDriver init error: {e}")
        print(f"Unexpected WebDriver init error: {traceback.format_exc()}")
        return None
# --- END REVISED setup_driver ---

# --- REVISED get_definition_list (Timing adjusted) ---
def get_definition_list(driver, definition_type, status_queue, stop_event):
    """Gets list, scrolling, validating Table IDs, with adjusted timing."""
    list_url = f"{BASE_URL}/{definition_type}"
    status_queue.put(('status', f"Fetching {definition_type} list from: {list_url}"))
    if stop_event.is_set(): return []
    try:
        driver.get(list_url)
        # No maximize needed in headless, but wait slightly for initial load
        time.sleep(0.2) # Reduced sleep
    except WebDriverException as e:
        status_queue.put(('error', f"Navigation error: {list_url}: {e}"))
        return []

    definitions = []
    wait_time_initial = 15 # Reduced initial wait
    pause_after_scroll = 0.2 # *** REDUCED SCROLL PAUSE ***
    link_pattern_xpath = f"//a[contains(@href, '/{definition_type}/') and not(contains(@href,'#'))]"

    try:
        status_queue.put(('status', f"  Waiting up to {wait_time_initial}s for initial links..."))
        wait = WebDriverWait(driver, wait_time_initial)
        try:
            wait.until(EC.presence_of_element_located((By.XPATH, link_pattern_xpath)))
            status_queue.put(('status', "  Initial links detected. Starting scroll loop..."))
        except TimeoutException:
            status_queue.put(('error', f"Timeout waiting for initial links for {definition_type}."))
            return [] # Return empty list on timeout

        found_hrefs = set()
        stale_scroll_count = 0
        max_stale_scrolls = 5 # Keep max stale scrolls

        while stale_scroll_count < max_stale_scrolls:
            if stop_event.is_set():
                status_queue.put(('warning', f"Stop requested during {definition_type} list scroll."))
                break

            previous_href_count = len(found_hrefs)
            current_links = []
            try:
                # Use a shorter wait within the loop as content *should* be loading
                WebDriverWait(driver, 3).until(EC.presence_of_element_located((By.XPATH, link_pattern_xpath)))
                current_links = driver.find_elements(By.XPATH, link_pattern_xpath)
            except TimeoutException:
                status_queue.put(('warning', "  Warn: No links found in current view after scroll/wait (likely end of list)."))
                # Don't break immediately, let stale counter handle it
            except Exception as e:
                status_queue.put(('error', f"  Error finding links during scroll: {e}"))
                break # Break on unexpected errors

            if not current_links:
                 status_queue.put(('status', "  No links currently visible."))
                 stale_scroll_count += 1
                 status_queue.put(('status', f"  Incrementing stale count due to no links: {stale_scroll_count}/{max_stale_scrolls}"))
            else:
                newly_added_this_pass = 0
                for link in current_links:
                    try:
                        href = link.get_attribute('href')
                        if href and f"/{definition_type}/" in href and href not in found_hrefs:
                            name = href.split('/')[-1].strip()

                            # Validation Logic (UNCHANGED)
                            is_valid_name = False
                            validation_reason = "Unknown"
                            if definition_type == 'Tables':
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

                            if name and name != "#" and is_valid_name:
                                found_hrefs.add(href)
                                newly_added_this_pass += 1
                            elif name and name != "#" and not is_valid_name:
                                status_queue.put(('debug', f"  Skipping '{name}' for type '{definition_type}' because Is Valid = {is_valid_name} (Reason: {validation_reason})"))

                    except StaleElementReferenceException:
                        status_queue.put(('warning', "  Warn: Stale link encountered during scroll check."))
                        continue
                    except Exception as e:
                        status_queue.put(('warning', f"  Warn: Error processing link attribute: {e}"))

                current_total_hrefs = len(found_hrefs)
                status_queue.put(('status', f"  Added {newly_added_this_pass} new valid links. Total unique valid: {current_total_hrefs}"))

                if current_total_hrefs == previous_href_count:
                    stale_scroll_count += 1
                    status_queue.put(('status', f"  Scroll count stable: {stale_scroll_count}/{max_stale_scrolls}"))
                else:
                    stale_scroll_count = 0 # Reset stale count if new items found

                if stale_scroll_count < max_stale_scrolls and current_links:
                    try:
                        last_element = current_links[-1]
                        # Scroll last element into view using JavaScript
                        driver.execute_script("arguments[0].scrollIntoView(true);", last_element)
                        status_queue.put(('status', f"  Scrolling last item into view. Pausing {pause_after_scroll}s..."))
                        time.sleep(pause_after_scroll) # Use the reduced pause
                    except StaleElementReferenceException:
                         status_queue.put(('warning', "  Warn: Last element became stale before scroll could execute."))
                    except Exception as e:
                        status_queue.put(('error', f"  Error scrolling last element: {e}"))
                        stale_scroll_count += 1 # Increment stale count on scroll error
                        status_queue.put(('status', f"  Incrementing stale count due to scroll error: {stale_scroll_count}/{max_stale_scrolls}"))

        status_queue.put(('status', "  Finished scroll attempts."))

        # Final name extraction (UNCHANGED)
        definitions.clear()
        valid_names_extracted = set()
        for href in found_hrefs:
            try:
                name = href.split('/')[-1].strip()
                if name and name != "#":
                    is_final_valid = False
                    if definition_type == 'Tables':
                         clean_name = name
                         if any(char.isdigit() for char in clean_name):
                             valid_chars = set('0123456789.')
                             if all(char in valid_chars for char in clean_name):
                                 dot_count = clean_name.count('.')
                                 if dot_count == 0 and clean_name.isdigit(): is_final_valid = True
                                 elif dot_count == 1:
                                     parts = clean_name.split('.');
                                     if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit() and parts[0] and parts[1]: is_final_valid = True
                    else:
                        if name.isalnum(): is_final_valid = True

                    if is_final_valid:
                        valid_names_extracted.add(name)
                    else:
                         status_queue.put(('warning', f"  Final check failed for name '{name}' from href '{href}' (Type: {definition_type}). Skipping."))
            except Exception as e:
                 status_queue.put(('warning', f"Warn: Error extracting name from final href '{href}': {e}"))

        definitions = sorted(list(valid_names_extracted))

        if not definitions and len(found_hrefs) > 0:
            status_queue.put(('warning', f"Warning: Collected {len(found_hrefs)} hrefs, but failed to extract valid names matching expected format."))
        elif not definitions and not stop_event.is_set():
             status_queue.put(('warning', f"Warning: No valid {definition_type} definitions found after scrolling and validation."))

    except TimeoutException:
        status_queue.put(('error', f"Timeout waiting for initial links for {definition_type}: {list_url}"))
    except WebDriverException as e:
         status_queue.put(('error', f"WebDriver error during {definition_type} list fetch: {e}"))
    except Exception as e:
        status_queue.put(('error', f"Unexpected error fetching {definition_type} list: {e}"))
        status_queue.put(('error', traceback.format_exc()))

    status_queue.put(('status', f"Final count: Found {len(definitions)} unique valid {definition_type}."))
    return definitions
# --- END REVISED get_definition_list ---

# --- REVISED process_definition_page (Timing Adjusted) ---
def process_definition_page(driver, definition_type, definition_name, status_queue, stop_event):
    """Navigates, scrolls detail page, scrolls TOP, takes screenshot, calls Gemini, timing adjusted."""
    url = f"{BASE_URL}/{definition_type}/{definition_name}"
    status_queue.put(('status', f"Processing {definition_type}: {definition_name}"))
    if stop_event.is_set(): return None, definition_name
    try:
        driver.get(url)
        time.sleep(0.2) # Reduced sleep
    except WebDriverException as nav_err:
         status_queue.put(('error', f"Error navigating to {url}: {nav_err}"))
         return None, definition_name

    screenshot_path = None
    parsed_json = None
    wait_time_content = 15 # Reduced content wait
    pause_after_detail_scroll = 0.2 # *** REDUCED DETAIL SCROLL PAUSE ***
    scroll_amount_detail = 800

    try:
        if stop_event.is_set(): return None, definition_name

        content_locator = ( By.XPATH, "//table[contains(@class, 'table-definition') and contains(@class, 'table')] | //div[contains(@class, 'table-responsive')]//table | //div[contains(@class, 'DefinitionPage_definitionContent')] | //div[@id='MainContent_pnlContent'] | //div[@role='main'] | //main | //body")
        status_queue.put(('status', f"  Waiting up to {wait_time_content}s for content area..."))
        wait = WebDriverWait(driver, wait_time_content)
        try:
            content_element = wait.until(EC.visibility_of_element_located(content_locator))
            status_queue.put(('status', f"  Content area located."))
        except TimeoutException:
            status_queue.put(('error', f"Timeout ({wait_time_content}s) waiting for content area: {url}"))
            pass # Continue to screenshot attempt

        # Detail page scroll (UNCHANGED logic, but uses reduced pause_after_detail_scroll)
        status_queue.put(('status', "  Scrolling detail page to ensure full view..."))
        last_height = driver.execute_script("return document.body.scrollHeight")
        stale_height_count = 0; max_stale_detail_scrolls = 3; scroll_attempts = 0; max_scroll_attempts = 25
        while stale_height_count < max_stale_detail_scrolls and scroll_attempts < max_scroll_attempts:
            if stop_event.is_set(): raise KeyboardInterrupt("Stop requested during detail scroll.")
            driver.execute_script(f"window.scrollBy(0, {scroll_amount_detail});")
            time.sleep(pause_after_detail_scroll) # Uses reduced value
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                stale_height_count += 1
                if stale_height_count >= 2:
                    try:
                       driver.execute_script("window.scrollTo(0, document.body.scrollHeight);"); time.sleep(pause_after_detail_scroll)
                       new_height = driver.execute_script("return document.body.scrollHeight")
                       if new_height == last_height: status_queue.put(('debug',"  Scroll height stable after forced bottom scroll."))
                       else: stale_height_count = 0; status_queue.put(('debug',"  Scroll height changed after forced bottom scroll, continue.")); last_height = new_height
                    except Exception as scroll_err: status_queue.put(('warning', f"  Warning during forced bottom scroll: {scroll_err}"))
            else: stale_height_count = 0
            last_height = new_height; scroll_attempts += 1
            status_queue.put(('debug', f"  Scroll attempt {scroll_attempts}, height {last_height}, stale {stale_height_count}"))

        if scroll_attempts >= max_scroll_attempts: status_queue.put(('warning', f"  Max scroll attempts ({max_scroll_attempts}) reached for detail page {definition_name}."))
        status_queue.put(('status', "  Detail page scroll complete."))

        # Scroll back to top BEFORE screenshot
        status_queue.put(('status', "  Scrolling back to top..."))
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(0.2) # *** REDUCED PAUSE ***

        if stop_event.is_set(): return None, definition_name

        # Screenshot (UNCHANGED Logic)
        script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
        screenshot_full_dir = os.path.join(script_dir, SCREENSHOT_DIR)
        if not os.path.exists(screenshot_full_dir): os.makedirs(screenshot_full_dir)
        screenshot_filename = f"{definition_type}_{definition_name}.png"
        screenshot_path = os.path.join(screenshot_full_dir, screenshot_filename)
        status_queue.put(('status', "  Attempting full page screenshot..."))
        try:
            required_width = driver.execute_script('return document.body.parentNode.scrollWidth'); required_height = driver.execute_script('return document.body.parentNode.scrollHeight')
            original_size = driver.get_window_size(); driver.set_window_size(required_width, required_height); time.sleep(0.2) # Reduced sleep
            content_element_for_shot = driver.find_element(By.TAG_NAME, "body")
            screenshot_success = content_element_for_shot.screenshot(screenshot_path); driver.set_window_size(original_size['width'], original_size['height'])
            status_queue.put(('status', f"  Screenshot attempt finished (Success={screenshot_success is None})."))
        except Exception as ss_err:
             status_queue.put(('warning', f"  Warning during screenshot resize/capture: {ss_err}. Trying standard save_screenshot."))
             screenshot_success = driver.save_screenshot(screenshot_path)

        # Gemini Analysis
        if os.path.exists(screenshot_path):
            status_queue.put(('status', f"  Screenshot saved: {screenshot_filename}"))
            parsed_json = analyze_screenshot_with_gemini(screenshot_path, definition_name, definition_type)
        else:
            status_queue.put(('error', f"Error: Failed to save screenshot for {definition_name} at {screenshot_path}"))

    except KeyboardInterrupt: status_queue.put(('warning', "Stop requested during detail page processing.")); return None, definition_name
    except WebDriverException as wd_err: status_queue.put(('error', f"WebDriver error on page {url}: {wd_err}"))
    except Exception as e: status_queue.put(('error', f"Error processing page {url}: {e}")); import traceback; status_queue.put(('error', traceback.format_exc()))

    # Save error page source (UNCHANGED Logic)
    if parsed_json is None and not stop_event.is_set() and screenshot_path is not None:
         screenshot_dir = os.path.dirname(screenshot_path)
         if os.path.exists(screenshot_dir):
             try:
                  page_source_path = screenshot_path.replace('.png', '_error.html')
                  with open(page_source_path, "w", encoding="utf-8") as f_debug: f_debug.write(driver.page_source)
                  status_queue.put(('warning', f"Saved error page source: {os.path.basename(page_source_path)}"))
             except Exception as dump_err: status_queue.put(('error', f"Failed to dump page source for {definition_name}: {dump_err}"))
         else: status_queue.put(('warning', f"Screenshot directory {screenshot_dir} doesn't exist, cannot save error HTML."))

    time.sleep(0.2) # *** REDUCED FINAL PAUSE ***
    return parsed_json, definition_name
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

# --- NEW: Helper Functions for Caching ---
def load_existing_definitions(output_file, status_queue):
    """Loads existing definitions from the JSON file."""
    script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
    file_path = os.path.join(script_dir, output_file)
    default_structure = {"tables": {}, "dataTypes": {}, "HL7": {}} # Ensure HL7 key exists
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                # Ensure top-level keys exist
                if "tables" not in data: data["tables"] = {}
                if "dataTypes" not in data: data["dataTypes"] = {}
                if "HL7" not in data: data["HL7"] = {} # Ensure HL7 exists
                status_queue.put(('status', f"Loaded {len(data.get('tables', {}))} tables and {len(data.get('dataTypes', {}))} dataTypes/segments from cache."))
                return data
        except json.JSONDecodeError as e:
            status_queue.put(('error', f"Error decoding existing JSON file '{output_file}': {e}. Starting fresh."))
            return default_structure
        except Exception as e:
            status_queue.put(('error', f"Error reading existing JSON file '{output_file}': {e}. Starting fresh."))
            return default_structure
    else:
        status_queue.put(('status', "No existing JSON file found. Starting fresh."))
        return default_structure

def item_exists_in_cache(definition_type, item_name, cache_dict):
    """Checks if an item already exists in the loaded cache."""
    if not cache_dict: return False
    try:
        if definition_type == "Tables":
            return str(item_name) in cache_dict.get("tables", {})
        elif definition_type in ["DataTypes", "Segments"]:
            # DataTypes and Segments are both stored under "dataTypes" key
            return item_name in cache_dict.get("dataTypes", {})
        else:
            return False # Unknown type
    except Exception:
        return False # Avoid errors if cache_dict structure is unexpected

# --- REVISED: process_category_thread (Added Caching Logic) ---
def process_category_thread(definition_type, results_queue, status_queue, stop_event, loaded_definitions):
    """Worker thread to process a single definition category, skipping cached items."""
    thread_name = f"Thread-{definition_type}"
    status_queue.put(('status', f"[{thread_name}] Starting."))
    driver = None
    error_count = 0
    items_processed_in_thread = 0
    items_skipped_cache = 0 # Counter for skipped items
    definition_list = []
    thread_result_dict = {}

    try:
        status_queue.put(('status', f"[{thread_name}] Initializing WebDriver..."))
        driver = setup_driver()
        if not driver: raise Exception(f"[{thread_name}] WebDriver initialization failed.")
        if stop_event.is_set(): raise KeyboardInterrupt("Stop requested early.")

        definition_list = get_definition_list(driver, definition_type, status_queue, stop_event)
        list_count = len(definition_list)
        status_queue.put(('list_found', definition_type, list_count))
        status_queue.put(('progress', definition_type.lower(), 0, list_count)) # Initialize progress

        if stop_event.is_set(): raise KeyboardInterrupt("Stop requested after list fetch.")

        if definition_list:
            status_queue.put(('status', f"[{thread_name}] Processing/Checking {list_count} {definition_type}..."))
            for i, item_name in enumerate(definition_list):
                if stop_event.is_set():
                    status_queue.put(('warning', f"[{thread_name}] Stop requested processing {item_name}."))
                    break

                # <<< --- CACHING CHECK --- >>>
                if item_exists_in_cache(definition_type, item_name, loaded_definitions):
                    status_queue.put(('debug', f"[{thread_name}] Skipping '{item_name}' - found in cache."))
                    items_skipped_cache += 1
                    # Update progress immediately for skipped item
                    status_queue.put(('progress', definition_type.lower(), i + 1, list_count))
                    status_queue.put(('progress_add', 1)) # Increment overall processed count too
                    continue # Move to the next item
                # <<< --- END CACHING CHECK --- >>>

                # Only process if not skipped
                parsed_data, _ = process_definition_page(driver, definition_type, item_name, status_queue, stop_event)
                items_processed_in_thread += 1 # Count items actually processed (not skipped)

                # --- Validation Logic (UNCHANGED) ---
                corrected_item_data = None
                processing_successful = False
                if definition_type == "Tables":
                    if parsed_data and isinstance(parsed_data, dict):
                        if len(parsed_data) == 1:
                            ai_key = next(iter(parsed_data)); ai_value = parsed_data[ai_key]
                            if ai_key == str(item_name):
                                if isinstance(ai_value, list):
                                    if all(isinstance(item, dict) and 'value' in item for item in ai_value):
                                        corrected_item_data = ai_value; processing_successful = True
                                        status_queue.put(('debug', f"[{thread_name}] Valid Table structure for '{item_name}'"))
                                    else: status_queue.put(('warning', f"[{thread_name}] AI table '{item_name}' list items invalid structure. Skip.")); error_count += 1
                                else: status_queue.put(('warning', f"[{thread_name}] AI table '{item_name}' value not a list. Skip.")); error_count += 1
                            else: status_queue.put(('warning', f"[{thread_name}] AI table key '{ai_key}' != expected ID '{item_name}'. Skip.")); error_count += 1
                        else: status_queue.put(('warning', f"[{thread_name}] AI table '{item_name}' dict has != 1 key. Skip.")); error_count += 1
                    elif parsed_data is None and not stop_event.is_set(): status_queue.put(('warning', f"[{thread_name}] No parsed data for '{item_name}'. Skip.")); error_count += 1
                    elif parsed_data and not stop_event.is_set(): status_queue.put(('warning', f"[{thread_name}] AI table '{item_name}' not a dict {type(parsed_data)}. Skip.")); error_count += 1

                elif definition_type in ["DataTypes", "Segments"]:
                     if parsed_data and isinstance(parsed_data, dict):
                        if len(parsed_data) == 1:
                            ai_key = next(iter(parsed_data)); ai_value = parsed_data[ai_key]
                            if ai_key == item_name:
                                if isinstance(ai_value, dict) and 'separator' in ai_value and 'versions' in ai_value and isinstance(ai_value.get('versions'), dict):
                                    version_key = next(iter(ai_value.get('versions', {})), None)
                                    if version_key and 'parts' in ai_value['versions'].get(version_key, {}):
                                        corrected_item_data = ai_value; processing_successful = True
                                        status_queue.put(('debug', f"[{thread_name}] Valid {definition_type} structure for '{item_name}'"))
                                        if definition_type == "Segments":
                                            hl7_seg_part = {"mandatory": True, "name": "hl7SegmentName", "type": "ST", "table": "0076", "length": 3}
                                            try:
                                                parts_list = corrected_item_data["versions"][version_key].setdefault("parts", [])
                                                if not parts_list or parts_list[0].get("name") != "hl7SegmentName":
                                                    parts_list.insert(0, hl7_seg_part.copy()); status_queue.put(('status',f"  [{thread_name}] Std part prepended for {item_name}."))
                                                    # Ensure totalFields is updated *after* potentially adding the part
                                                    corrected_item_data["versions"][version_key]["totalFields"] = len(parts_list)
                                                else:
                                                     status_queue.put(('debug',f"  [{thread_name}] Std part already present for {item_name}."))
                                                     # Ensure totalFields reflects actual count even if not prepended
                                                     corrected_item_data["versions"][version_key]["totalFields"] = len(parts_list)

                                            except Exception as e: status_queue.put(('warning', f"[{thread_name}] Error adding/checking std part {item_name}: {e}"))
                                    else: status_queue.put(('warning', f"[{thread_name}] AI {definition_type} '{item_name}' missing version/parts. Skip.")); error_count += 1
                                else: status_queue.put(('warning', f"[{thread_name}] AI {definition_type} '{item_name}' invalid inner structure. Skip.")); error_count += 1
                            else: status_queue.put(('warning', f"[{thread_name}] AI {definition_type} key '{ai_key}' != expected name '{item_name}'. Skip.")); error_count += 1
                        else: status_queue.put(('warning', f"[{thread_name}] AI {definition_type} '{item_name}' dict has != 1 key. Skip.")); error_count += 1
                     elif parsed_data is None and not stop_event.is_set(): status_queue.put(('warning', f"[{thread_name}] No parsed data for '{item_name}'. Skip.")); error_count += 1
                     elif parsed_data and not stop_event.is_set(): status_queue.put(('warning', f"[{thread_name}] AI {definition_type} '{item_name}' not a dict {type(parsed_data)}. Skip.")); error_count += 1
                # --- End Validation ---

                if processing_successful and corrected_item_data is not None:
                    thread_result_dict[str(item_name)] = corrected_item_data

                # Update progress for this specific category bar
                status_queue.put(('progress', definition_type.lower(), i + 1, list_count))
                # Update overall progress (needs to happen even if skipped)
                status_queue.put(('progress_add', 1))

        results_queue.put((definition_type, thread_result_dict)) # Send newly processed items
        status_queue.put(('status', f"[{thread_name}] Finished. Processed: {items_processed_in_thread}, Skipped (Cache): {items_skipped_cache}, Errors: {error_count}"))

    except KeyboardInterrupt:
        status_queue.put(('warning', f"[{thread_name}] Aborted by user request."))
        results_queue.put((definition_type, thread_result_dict))
    except Exception as e:
        status_queue.put(('error', f"[{thread_name}] CRITICAL ERROR: {e}"))
        status_queue.put(('error', traceback.format_exc()))
        error_count += 1
        results_queue.put((definition_type, thread_result_dict))
    finally:
        results_queue.put((definition_type + "_DONE", error_count)) # Signal completion
        if driver:
            status_queue.put(('status', f"[{thread_name}] Cleaning up WebDriver..."))
            try: driver.quit(); status_queue.put(('status', f"[{thread_name}] WebDriver closed."))
            except Exception as q_err: status_queue.put(('error', f"[{thread_name}] Error quitting WebDriver: {q_err}"))
# --- END REVISED process_category_thread ---

# --- GUI Application Class (Minor adjustments for caching info) ---
class HL7ParserApp:
    def __init__(self, master):
        self.master = master; master.title("HL7 Parser (Concurrent + Cache)"); master.geometry("700x550")
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

    # log_message, update_progress UNCHANGED
    def log_message(self, message, level="info"):
        tag=(); prefix="";
        if level == "error": tag,prefix = (('error',),"ERROR: ")
        elif level == "warning": tag,prefix = (('warning',),"WARNING: ")
        elif level == "debug": tag, prefix = (('debug',), "DEBUG: ")
        else: tag, prefix = ((), "")
        def update_log():
            self.log_area.config(state='normal')
            self.log_area.insert(tk.END, f"{prefix}{message}\n", tag)
            self.log_area.see(tk.END)
            self.log_area.config(state='disabled')
        self.master.after(0, update_log)

    def update_progress(self, bar_type, current, total):
        def update_gui():
            total_val=max(1,total)
            percentage=int((current/total_val)*100) if total_val > 0 else 0
            pb,lbl=None,None
            count_text=f"{current}/{total}"

            if bar_type=="tables": pb,lbl=(self.pb_tables,self.lbl_tables_count)
            elif bar_type=="datatypes": pb,lbl=(self.pb_datatypes,self.lbl_datatypes_count)
            elif bar_type=="segments": pb,lbl=(self.pb_segments,self.lbl_segments_count)
            elif bar_type=="overall":
                 pb,lbl,count_text=(self.pb_overall,self.lbl_overall_perc,f"{percentage}%")
                 if pb: pb.config(maximum=total_val, value=current)
                 if lbl: lbl.config(text=count_text)
                 return

            if pb: pb.config(maximum=total_val, value=current)
            if lbl: lbl.config(text=count_text)
        self.master.after(0, update_gui)

    # check_queue UNCHANGED
    def check_queue(self):
        try:
            while True:
                message=self.status_queue.get_nowait()
                msg_type=message[0]
                if msg_type=='status': self.log_message(message[1])
                elif msg_type=='error': self.log_message(message[1], level="error")
                elif msg_type=='warning': self.log_message(message[1], level="warning")
                elif msg_type=='debug': self.log_message(message[1], level="debug")
                elif msg_type=='progress': self.update_progress(message[1], message[2], message[3])
                elif msg_type == 'progress_add':
                    self.processed_items_count += message[1]; self.update_progress("overall", self.processed_items_count, self.grand_total_items)
                elif msg_type == 'list_found':
                     category_name = message[1]; count = message[2]
                     if category_name not in self.list_counts_received:
                         self.grand_total_items += count; self.list_counts_received.add(category_name)
                         self.update_progress(category_name.lower(), 0, count)
                         self.update_progress("overall", self.processed_items_count, self.grand_total_items)
                         self.log_message(f"Found {count} {category_name}.")
                elif msg_type=='finished':
                    error_count = message[1]; self.log_message("Processing finished.")
                    self.start_button.config(state=tk.NORMAL); self.stop_button.config(state=tk.DISABLED)
                    if error_count is not None and error_count > 0: messagebox.showwarning("Complete with Errors", f"Finished, but with {error_count} errors recorded. Check log and screenshots.")
                    elif error_count == 0: messagebox.showinfo("Complete", "Finished successfully!")
                    else: messagebox.showinfo("Complete", "Processing finished (may have been aborted or no items found).")
                    self.worker_threads = []; self.orchestrator_thread = None
                    return
        except queue.Empty: pass

        orchestrator_alive = self.orchestrator_thread and self.orchestrator_thread.is_alive()
        workers_alive = any(t.is_alive() for t in self.worker_threads)
        if workers_alive or orchestrator_alive: self.master.after(150, self.check_queue)
        elif self.start_button['state'] == tk.DISABLED: self.master.after(500, self.check_queue) # Check again if buttons suggest it's not fully done


    # start_processing UNCHANGED
    def start_processing(self):
        if not load_api_key(): return;
        if not configure_gemini(): return;

        orchestrator_alive = self.orchestrator_thread and self.orchestrator_thread.is_alive()
        if orchestrator_alive or any(t.is_alive() for t in self.worker_threads):
             messagebox.showwarning("Busy", "Processing is already in progress.")
             return

        self.stop_event.clear(); self.start_button.config(state=tk.DISABLED); self.stop_button.config(state=tk.NORMAL)
        self.log_message("Starting concurrent processing (Headless + Cache)...")
        self.log_message("WARNING: This may use significant RAM/CPU (3+ headless browser instances).")

        self.grand_total_items = 0; self.processed_items_count = 0; self.list_counts_received.clear()
        self.update_progress("tables",0,1); self.lbl_tables_count.config(text="0/0")
        self.update_progress("datatypes",0,1); self.lbl_datatypes_count.config(text="0/0")
        self.update_progress("segments",0,1); self.lbl_segments_count.config(text="0/0")
        self.update_progress("overall",0,1); self.lbl_overall_perc.config(text="0%")
        self.worker_threads = []

        results_queue = queue.Queue()
        self.orchestrator_thread = threading.Thread(target=self.run_parser_orchestrator,
                                          args=(results_queue, self.stop_event), daemon=True)
        self.orchestrator_thread.start()
        self.master.after(100, self.check_queue)

    # stop_processing UNCHANGED
    def stop_processing(self):
        orchestrator_alive = hasattr(self, 'orchestrator_thread') and self.orchestrator_thread.is_alive()
        workers_alive = any(t.is_alive() for t in self.worker_threads)
        if workers_alive or orchestrator_alive:
            if not self.stop_event.is_set():
                self.log_message("Stop request received. Signaling background threads...", level="warning"); self.stop_event.set()
            self.stop_button.config(state=tk.DISABLED)
        else:
             self.log_message("Stop requested, but no active process found.", level="info"); self.stop_button.config(state=tk.DISABLED); self.start_button.config(state=tk.NORMAL)

    # --- REVISED: run_parser_orchestrator (Load Cache, Pass to Workers, Merge Results) ---
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
        self.worker_threads = []

        try:
            self.status_queue.put(('status', "Starting worker threads..."))
            for category in categories:
                if stop_event.is_set(): break
                # <<< --- PASS CACHE TO WORKER --- >>>
                worker = threading.Thread(target=process_category_thread,
                                          args=(category, results_queue, self.status_queue, stop_event, loaded_definitions), # Pass loaded cache
                                          daemon=True, name=f"Worker-{category}")
                # <<< --- END PASS CACHE --- >>>
                self.worker_threads.append(worker)
                worker.start()

            if stop_event.is_set(): raise KeyboardInterrupt("Stop requested during thread startup.")

            # Result Collection (UNCHANGED Logic - collects NEW results)
            self.status_queue.put(('status', "Waiting for results from worker threads..."))
            while len(threads_finished) < len(categories):
                if stop_event.is_set() and not any(t.is_alive() for t in self.worker_threads):
                    self.status_queue.put(('warning', "Stopping result collection early due to stop signal."))
                    break
                try:
                    result_type, data = results_queue.get(timeout=1.0)
                    if result_type.endswith("_DONE"):
                        category = result_type.replace("_DONE", "");
                        if category in categories:
                            threads_finished.add(category); thread_errors[category] = data; total_error_count += data
                            self.status_queue.put(('status', f"Worker thread for {category} finished reporting {data} errors."))
                        else: self.status_queue.put(('warning', f"Received unexpected DONE signal: {result_type}"))
                    elif result_type in categories:
                         all_new_results[result_type].update(data) # Update NEW results dict
                         self.status_queue.put(('debug', f"Received {len(data)} new results for {result_type}."))
                    else: self.status_queue.put(('warning', f"Received unknown result type: {result_type}"))
                except queue.Empty:
                    if stop_event.is_set(): self.status_queue.put(('warning', "Stop signal detected while waiting for results."))
                    continue
            self.status_queue.put(('status', "All worker threads have reported completion or stop signal received."))

        except KeyboardInterrupt: self.status_queue.put(('warning', "\nOrchestrator aborted by user request."))
        except Exception as e: self.status_queue.put(('error', f"Orchestrator CRITICAL ERROR: {e}")); self.status_queue.put(('error', traceback.format_exc())); total_error_count += 1
        finally:
            # Thread Joining (UNCHANGED)
            self.status_queue.put(('status', "Ensuring all worker threads have terminated..."))
            join_timeout = 10.0
            for t in self.worker_threads: t.join(timeout=join_timeout); # ... (rest of joining logic) ...
            self.status_queue.put(('status', "Worker thread joining complete."))

            # <<< --- MERGE CACHE WITH NEW RESULTS --- >>>
            final_definitions = loaded_definitions # Start with the loaded cache
            processed_segments_for_hl7 = []

            if not stop_event.is_set() or any(all_new_results.values()): # Proceed if not stopped or if new results exist
                 self.status_queue.put(('status', "Merging cached and new results..."))
                 # Update the 'tables' section
                 final_definitions.setdefault("tables", {}).update(all_new_results.get("Tables", {}))
                 # Update the 'dataTypes' section (includes both DataTypes and Segments)
                 final_definitions.setdefault("dataTypes", {}).update(all_new_results.get("DataTypes", {}))
                 final_definitions["dataTypes"].update(all_new_results.get("Segments", {}))

                 # Get segment names from the FINAL combined dictionary for HL7 structure
                 processed_segments_for_hl7 = [k for k, v in final_definitions["dataTypes"].items() if v.get('separator') == '.']
                 # <<< --- END MERGE --- >>>


                 # Build HL7 Structure (UNCHANGED Logic, uses final segment list)
                 self.status_queue.put(('status', "\n--- Building HL7 Structure ---"))
                 hl7_parts=[]; common_order=["MSH","PID","PV1","OBR","OBX"]; # Keep your desired order
                 ordered=[s for s in common_order if s in processed_segments_for_hl7]
                 other=sorted([s for s in processed_segments_for_hl7 if s not in common_order])
                 final_segment_order = ordered + other
                 if not final_segment_order:
                     self.status_queue.put(('warning', "No segments found in final combined data to build HL7 structure."))
                 else:
                    for seg_name in final_segment_order:
                        seg_def = final_definitions["dataTypes"].get(seg_name) # Get from final merged dict
                        is_mand = False; repeats = False; length = -1
                        if seg_name == "MSH": is_mand = True
                        else: repeats = True
                        if seg_def and 'versions' in seg_def:
                             version_key = next(iter(seg_def.get('versions', {})), None)
                             if version_key: length = seg_def['versions'][version_key].get('length', -1)
                        part={"name":seg_name.lower(),"type":seg_name,"length": length}
                        if is_mand: part.update({"mandatory":True})
                        if repeats: part.update({"repeats":True})
                        hl7_parts.append(part)
                    # Ensure HL7 structure exists before assigning
                    final_definitions.setdefault("HL7", {}).update({ "separator":"\r", "partId":"type", "versions":{ HL7_VERSION:{"appliesTo":"equalOrGreater","length":-1,"parts":hl7_parts}}})
                    self.status_queue.put(('status', f"HL7 structure updated/built with {len(hl7_parts)} segments."))

            # Write Final JSON (UNCHANGED Logic)
            if not stop_event.is_set():
                self.status_queue.put(('status', f"\nWriting final definitions to {OUTPUT_JSON_FILE}"))
                script_dir=os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
                output_path=os.path.join(script_dir,OUTPUT_JSON_FILE)
                try:
                    with open(output_path,'w',encoding='utf-8') as f: json.dump(final_definitions,f,indent=2,ensure_ascii=False)
                    self.status_queue.put(('status', "JSON file written successfully."))
                    if total_error_count == 0:
                        self.status_queue.put(('status', "No errors recorded, attempting screenshot cleanup."))
                        clear_screenshot_folder(self.status_queue)
                    else:
                        self.status_queue.put(('warning', f"Errors ({total_error_count}) occurred, screenshots in '{SCREENSHOT_DIR}' were NOT deleted."))
                except Exception as e:
                     self.status_queue.put(('error', f"Failed to write JSON file: {e}")); total_error_count+=1
            else:
                self.status_queue.put(('warning', f"Processing stopped, final JSON file '{OUTPUT_JSON_FILE}' reflects merged cache and any new results obtained before stopping."))

            # Signal Overall Completion
            self.status_queue.put(('finished', total_error_count))
    # --- END REVISED run_parser_orchestrator ---

# --- Run Application (Ensure app is global) ---
if __name__ == "__main__":
    # app is declared global at the top
    root = tk.Tk()
    app = HL7ParserApp(root) # Assign the global instance
    try:
        root.mainloop()
    except KeyboardInterrupt:
        print("\nCtrl+C detected in main loop. Signaling stop...")
        if app:
            app.log_message("Shutdown requested (Ctrl+C)...", level="warning")
            app.stop_event.set()
            join_timeout = 10.0
            if hasattr(app, 'orchestrator_thread') and app.orchestrator_thread.is_alive():
                print("Waiting for orchestrator thread...")
                app.orchestrator_thread.join(timeout=join_timeout)
                if app.orchestrator_thread.is_alive(): print("Warning: Orchestrator thread did not exit cleanly.")
            threads_to_join = app.worker_threads
            if threads_to_join:
                 print(f"Waiting for {len(threads_to_join)} worker threads...")
                 for t in threads_to_join:
                      if t.is_alive(): t.join(timeout=join_timeout / len(threads_to_join) if len(threads_to_join) > 0 else join_timeout);
                      if t.is_alive(): print(f"Warning: Worker thread {t.name} did not exit cleanly.")
            else: print("No worker threads found to join.")
        print("Exiting application.")
        try: root.destroy()
        except tk.TclError: pass
        sys.exit(0)