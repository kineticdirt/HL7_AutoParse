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
import concurrent.futures # For ThreadPoolExecutor

# --- Configuration, Globals ---
BASE_URL = "https://hl7-definition.caristix.com/v2/HL7v2.6"
OUTPUT_JSON_FILE = "hl7_definitions_v2.6.json"
FALLBACK_HTML_DIR = "fallback_html" # Directory for saving HTML on fallback
API_KEY_FILE = "api_key.txt"
HL7_VERSION = "2.6"
GEMINI_API_KEY = None
GEMINI_MODEL = None
# Global variable to hold the app instance for access in functions
app = None
# --- Parallelization Configuration ---
# Adjust based on your system (CPU cores, RAM) and network/API limits
# Start conservatively (e.g., 8-12) and increase if stable. 16 might be too high for many systems.
MAX_WORKERS = 20 # Max concurrent Selenium instances + AI calls

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
        GEMINI_MODEL = genai.GenerativeModel('gemini-1.5-flash') # Keep flash for now
        print("Gemini configured successfully."); return True
    except Exception as e: messagebox.showerror("Gemini Config Error", f"Failed to configure Gemini: {e}"); return False

# --- Gemini HTML Analysis Functions (Unchanged) ---
def analyze_table_html_with_gemini(html_content, definition_name):
    """Analyzes Table HTML source code with Gemini."""
    global app
    if not GEMINI_MODEL:
        print("Error: Gemini model not configured.")
        return None
    if app and app.stop_event.is_set():
        print(f"  Skip Gemini (Table HTML): Stop requested for {definition_name}.")
        return None

    definition_type = "Table" # For logging and clarity
    print(f"  Analyzing {definition_type} '{definition_name}' HTML with Gemini...")
    max_retries = 3
    retry_delay = 5

    prompt = f"""
        Analyze the provided HTML source code for the HL7 Table definition page for ID '{definition_name}', version {HL7_VERSION}.
        Focus on the main data table, likely marked with classes like 'mat-table', 'table-definition', or similar structured `<tr>` and `<td>` elements within the primary content area (`<tbody>`). Ignore extraneous HTML like headers, footers, scripts, and sidebars.
        Find the table containing 'Value' and 'Description' (or 'Comment') columns.
        Extract the 'Value' and 'Description' for each data row (`<tr>`) within the table body (`<tbody>`).

        Generate a JSON object strictly following these rules:
        1.  The **top-level key MUST be the numeric table ID as a JSON string** (e.g., "{definition_name}").
        2.  The value associated with this key MUST be an **array** of objects.
        3.  Each object in the array represents one row and MUST contain only two keys:
            *   `value`: The exact string content from the 'Value' column cell (`<td>`).
            *   `description`: The exact string content from the 'Description'/'Comment' column cell (`<td>`).
        4.  **Do NOT include** any other keys. Ensure all rows found in the HTML table are included.

        Example structure for table "0001":
        {{
          "{definition_name}": [
            {{ "value": "F", "description": "Female" }},
            {{ "value": "M", "description": "Male" }}
            {{ "value": "O", "description": "Other" }}
          ]
        }}

        Return ONLY the raw JSON object for table '{definition_name}' without any surrounding text or markdown formatting (` ```json ... ``` `).
    """

    for attempt in range(max_retries):
        if app and app.stop_event.is_set():
            print(f"  Skip Gemini {definition_type} HTML attempt {attempt+1}: Stop requested.")
            return None
        try:
            print(f"  Attempt {attempt + 1} for {definition_name} {definition_type} HTML analysis...")
            response = GEMINI_MODEL.generate_content(prompt + "\n\nHTML SOURCE:\n```html\n" + html_content + "\n```")

            json_text = response.text.strip()
            if json_text.startswith("```json"): json_text = json_text[7:]
            elif json_text.startswith("```"): json_text = json_text[3:]
            if json_text.endswith("```"): json_text = json_text[:-3]
            json_text = json_text.strip()

            parsed_json = json.loads(json_text)
            print(f"  Successfully parsed Gemini {definition_type} HTML response for {definition_name}.")
            return parsed_json
        except json.JSONDecodeError as e:
            print(f"Error: Bad JSON from Gemini {definition_type} HTML analysis for '{definition_name}': {e}")
            err_line, err_col = getattr(e, 'lineno', 'N/A'), getattr(e, 'colno', 'N/A')
            print(f"  Error at line ~{err_line}, column ~{err_col}")
            print(f"  Received Text: ```\n{response.text}\n```")
            if attempt == max_retries - 1:
                print(f"  Max retries reached for Gemini {definition_type} HTML analysis of {definition_name}.")
                return None
            print(f"  Retrying Gemini {definition_type} HTML analysis in {retry_delay}s...")
            time.sleep(retry_delay)
        except (google.api_core.exceptions.ResourceExhausted, google.api_core.exceptions.InternalServerError, google.api_core.exceptions.ServiceUnavailable, google.api_core.exceptions.GatewayTimeout) as e:
             print(f"Warn: Gemini API error attempt {attempt+1} for {definition_type} HTML analysis of '{definition_name}': {e}")
             if attempt < max_retries-1:
                  print(f"  Retrying in {retry_delay}s...")
                  time.sleep(retry_delay)
             else:
                  print(f"Error: Max Gemini retries reached for {definition_type} HTML analysis of '{definition_name}'."); return None
        except Exception as e:
            print(f"Error: Unexpected Gemini {definition_type} HTML analysis error attempt {attempt+1} for '{definition_name}': {e}")
            print(traceback.format_exc())
            return None
    return None

def analyze_datatype_html_with_gemini(html_content, definition_name):
    """Analyzes DataType HTML source code with Gemini."""
    global app
    if not GEMINI_MODEL:
        print("Error: Gemini model not configured.")
        return None
    if app and app.stop_event.is_set():
        print(f"  Skip Gemini (DataType HTML): Stop requested for {definition_name}.")
        return None

    definition_type = "DataType" # For logging and clarity
    print(f"  Analyzing {definition_type} '{definition_name}' HTML with Gemini...")
    max_retries = 3
    retry_delay = 5
    separator_value = "."

    prompt = f"""
        Analyze the provided HTML source code for the HL7 {definition_type} definition page for '{definition_name}', version {HL7_VERSION}.
        Focus on the main data table defining the components, likely marked with classes like 'mat-table', 'table-definition', or similar structured `<tr>` and `<td>` elements within the primary content area (`<tbody>`). Look for columns like 'FIELD', 'LENGTH', 'DATA TYPE', 'OPTIONALITY', 'REPEATABILITY', 'TABLE'. Ignore extraneous HTML like headers, footers, scripts, and sidebars.
        Extract the required information based on the rules below.
        Generate a JSON object strictly following the specified rules.
        Return ONLY the raw JSON object for '{definition_name}' without any surrounding text or markdown formatting (` ```json ... ``` `).

        JSON Rules:
        1.  Create a **top-level key which is the {definition_type} name** ('{definition_name}').
        2.  The value associated with this key MUST be an object.
        3.  This object MUST contain:
            *   `separator`: MUST be set to "{separator_value}"
            *   `versions`: An object containing a key for the HL7 version ('{HL7_VERSION}').
        4.  The '{HL7_VERSION}' object MUST contain:
            *   `appliesTo`: Set to 'equalOrGreater'.
            *   `totalFields`: The total count of component rows extracted for the 'parts' array.
            *   `length`: The overall length shown near the top of the page content if available (e.g., text like "LENGTH 831"), otherwise -1. Find this value outside the main table if necessary.
            *   `parts`: An **array** of objects, one for each data row (`<tr>`) in the definition table body (`<tbody>`).
        5.  Each object within the 'parts' array represents a component and MUST contain:
            *   `name`: The field description (from 'FIELD' or similar column) converted to camelCase (e.g., 'setIdPv1', 'financialClassCode'). Remove any prefix like 'NDL-1'. If the description is just '...', use a generic name like 'fieldN' where N is the row number.
            *   `type`: The exact string content from the 'DATA TYPE' column cell (`<td>`).
            *   `length`: The numeric value from the 'LENGTH' column cell (`<td>`). If it's '*' or empty/blank, use -1. Otherwise, use the integer value.
        6.  **Conditionally include** these keys in the part object ONLY if applicable, based on the corresponding column cell (`<td>`) content:
            *   `mandatory`: Set to `true` ONLY if the 'OPTIONALITY' column cell text is 'R', 'C', or 'B'. Omit otherwise (e.g., for 'O', 'W', 'X', '-').
            *   `repeats`: Set to `true` ONLY if the 'REPEATABILITY' column cell text does NOT contain a '-' character (i.e., it has 'Y', '∞', or a number). Omit otherwise.
            *   `table`: Set to the **numeric table ID as a JSON string** ONLY if the 'TABLE' column cell contains a numeric value (e.g., "0004", "0125"). Omit if the cell is empty or non-numeric. Ensure you get the value from the correct row.

        Example structure for a DataType ('CX') component part:
        {{ "name": "assigningAuthority", "type": "HD", "length": 227, "table": "0363" }}
        """

    for attempt in range(max_retries):
        if app and app.stop_event.is_set():
            print(f"  Skip Gemini {definition_type} HTML attempt {attempt+1}: Stop requested.")
            return None
        try:
            print(f"  Attempt {attempt + 1} for {definition_name} {definition_type} HTML analysis...")
            response = GEMINI_MODEL.generate_content(prompt + "\n\nHTML SOURCE:\n```html\n" + html_content + "\n```")

            json_text = response.text.strip()
            if json_text.startswith("```json"): json_text = json_text[7:]
            elif json_text.startswith("```"): json_text = json_text[3:]
            if json_text.endswith("```"): json_text = json_text[:-3]
            json_text = json_text.strip()

            parsed_json = json.loads(json_text)
            print(f"  Successfully parsed Gemini {definition_type} HTML response for {definition_name}.")
            return parsed_json
        except json.JSONDecodeError as e:
            print(f"Error: Bad JSON from Gemini {definition_type} HTML analysis for '{definition_name}': {e}")
            err_line, err_col = getattr(e, 'lineno', 'N/A'), getattr(e, 'colno', 'N/A')
            print(f"  Error at line ~{err_line}, column ~{err_col}")
            print(f"  Received Text: ```\n{response.text}\n```")
            if attempt == max_retries - 1:
                print(f"  Max retries reached for Gemini {definition_type} HTML analysis of {definition_name}.")
                return None
            print(f"  Retrying Gemini {definition_type} HTML analysis in {retry_delay}s...")
            time.sleep(retry_delay)
        except (google.api_core.exceptions.ResourceExhausted, google.api_core.exceptions.InternalServerError, google.api_core.exceptions.ServiceUnavailable, google.api_core.exceptions.GatewayTimeout) as e:
             print(f"Warn: Gemini API error attempt {attempt+1} for {definition_type} HTML analysis of '{definition_name}': {e}")
             if attempt < max_retries-1:
                  print(f"  Retrying in {retry_delay}s...")
                  time.sleep(retry_delay)
             else:
                  print(f"Error: Max Gemini retries reached for {definition_type} HTML analysis of '{definition_name}'."); return None
        except Exception as e:
            print(f"Error: Unexpected Gemini {definition_type} HTML analysis error attempt {attempt+1} for '{definition_name}': {e}")
            print(traceback.format_exc())
            return None
    return None

def analyze_segment_html_with_gemini(html_content, definition_name):
    """Analyzes Segment HTML source code with Gemini."""
    global app
    if not GEMINI_MODEL:
        print("Error: Gemini model not configured.")
        return None
    if app and app.stop_event.is_set():
        print(f"  Skip Gemini (Segment HTML): Stop requested for {definition_name}.")
        return None

    definition_type = "Segment" # For logging and clarity
    print(f"  Analyzing {definition_type} '{definition_name}' HTML with Gemini...")
    max_retries = 3
    retry_delay = 5
    separator_value = "."

    prompt = f"""
        Analyze the provided HTML source code for the HL7 {definition_type} definition page for '{definition_name}', version {HL7_VERSION}.
        Focus on the main data table defining the fields, likely marked with classes like 'mat-table', 'table-definition', or similar structured `<tr>` and `<td>` elements within the primary content area (`<tbody>`). Look for columns like 'FIELD', 'LENGTH', 'DATA TYPE', 'OPTIONALITY', 'REPEATABILITY', 'TABLE'. Ignore extraneous HTML like headers, footers, scripts, and sidebars.
        Extract the required information based on the rules below.
        Generate a JSON object strictly following the specified rules.
        Return ONLY the raw JSON object for '{definition_name}' without any surrounding text or markdown formatting (` ```json ... ``` `).

        JSON Rules:
        1.  Create a **top-level key which is the {definition_type} name** ('{definition_name}').
        2.  The value associated with this key MUST be an object.
        3.  This object MUST contain:
            *   `separator`: MUST be set to "{separator_value}"
            *   `versions`: An object containing a key for the HL7 version ('{HL7_VERSION}').
        4.  The '{HL7_VERSION}' object MUST contain:
            *   `appliesTo`: Set to 'equalOrGreater'.
            *   `totalFields`: The total count of field rows extracted for the 'parts' array. Remember to include the standard 'hl7SegmentName' part if it's not explicitly listed first in the HTML table.
            *   `length`: The overall length shown near the top of the page content if available (e.g., text like "LENGTH 1200"), otherwise -1. Find this value outside the main table if necessary.
            *   `parts`: An **array** of objects, one for each data row (`<tr>`) in the definition table body (`<tbody>`). If the table doesn't start with 'hl7SegmentName' or 'Set ID', prepend this standard part: {{"name": "hl7SegmentName", "type": "ST", "length": 3, "mandatory": true, "table": "0076"}}.
        5.  Each object within the 'parts' array represents a field and MUST contain:
            *   `name`: The field description (from 'FIELD' or similar column) converted to camelCase (e.g., 'setIdPv1', 'patientClass'). Remove any prefix like 'PV1-1'. If the description is just '...', use a generic name like 'fieldN' where N is the row number.
            *   `type`: The exact string content from the 'DATA TYPE' column cell (`<td>`).
            *   `length`: The numeric value from the 'LENGTH' column cell (`<td>`). If it's '*' or empty/blank, use -1. Otherwise, use the integer value.
        6.  **Conditionally include** these keys in the part object ONLY if applicable, based on the corresponding column cell (`<td>`) content:
            *   `mandatory`: Set to `true` ONLY if the 'OPTIONALITY' column cell text is 'R', 'C', or 'B'. Omit otherwise (e.g., for 'O', 'W', 'X', '-').
            *   `repeats`: Set to `true` ONLY if the 'REPEATABILITY' column cell text does NOT contain a '-' character (i.e., it has 'Y', '∞', or a number). Omit otherwise.
            *   `table`: Set to the **numeric table ID as a JSON string** ONLY if the 'TABLE' column cell contains a numeric value (e.g., "0004", "0125"). Omit if the cell is empty or non-numeric. Ensure you get the value from the correct row.

        Example structure for a Segment ('PV1') component part:
        {{ "name": "patientClass", "type": "IS", "length": 1, "mandatory": true, "table": "0004" }}
        """

    for attempt in range(max_retries):
        if app and app.stop_event.is_set():
            print(f"  Skip Gemini {definition_type} HTML attempt {attempt+1}: Stop requested.")
            return None
        try:
            print(f"  Attempt {attempt + 1} for {definition_name} {definition_type} HTML analysis...")
            response = GEMINI_MODEL.generate_content(prompt + "\n\nHTML SOURCE:\n```html\n" + html_content + "\n```")

            json_text = response.text.strip()
            if json_text.startswith("```json"): json_text = json_text[7:]
            elif json_text.startswith("```"): json_text = json_text[3:]
            if json_text.endswith("```"): json_text = json_text[:-3]
            json_text = json_text.strip()

            parsed_json = json.loads(json_text)
            print(f"  Successfully parsed Gemini {definition_type} HTML response for {definition_name}.")
            # --- Post-processing for Segments: Ensure standard part exists ---
            if parsed_json and definition_name in parsed_json:
                segment_data = parsed_json[definition_name]
                if "versions" in segment_data and HL7_VERSION in segment_data["versions"]:
                    version_data = segment_data["versions"][HL7_VERSION]
                    if "parts" in version_data:
                         parts_list = version_data["parts"]
                         hl7_seg_part = {"mandatory": True, "name": "hl7SegmentName", "type": "ST", "table": "0076", "length": 3}
                         if not parts_list or parts_list[0].get("name") != "hl7SegmentName":
                              parts_list.insert(0, hl7_seg_part)
                              # Recalculate totalFields if Gemini didn't already include it
                              if 'totalFields' in version_data:
                                version_data["totalFields"] = len(parts_list) # Update totalFields count
                              print(f"  Prepended standard hl7SegmentName part for {definition_name} (AI Result)")
            # --- End Post-processing ---
            return parsed_json
        except json.JSONDecodeError as e:
            print(f"Error: Bad JSON from Gemini {definition_type} HTML analysis for '{definition_name}': {e}")
            err_line, err_col = getattr(e, 'lineno', 'N/A'), getattr(e, 'colno', 'N/A')
            print(f"  Error at line ~{err_line}, column ~{err_col}")
            print(f"  Received Text: ```\n{response.text}\n```")
            if attempt == max_retries - 1:
                print(f"  Max retries reached for Gemini {definition_type} HTML analysis of {definition_name}.")
                return None
            print(f"  Retrying Gemini {definition_type} HTML analysis in {retry_delay}s...")
            time.sleep(retry_delay)
        except (google.api_core.exceptions.ResourceExhausted, google.api_core.exceptions.InternalServerError, google.api_core.exceptions.ServiceUnavailable, google.api_core.exceptions.GatewayTimeout) as e:
             print(f"Warn: Gemini API error attempt {attempt+1} for {definition_type} HTML analysis of '{definition_name}': {e}")
             if attempt < max_retries-1:
                  print(f"  Retrying in {retry_delay}s...")
                  time.sleep(retry_delay)
             else:
                  print(f"Error: Max Gemini retries reached for {definition_type} HTML analysis of '{definition_name}'."); return None
        except Exception as e:
            print(f"Error: Unexpected Gemini {definition_type} HTML analysis error attempt {attempt+1} for '{definition_name}': {e}")
            print(traceback.format_exc())
            return None
    return None

# --- Selenium Functions (Unchanged) ---
def setup_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1200")
    options.add_argument("--log-level=3")
    options.add_experimental_option('excludeSwitches', ['enable-logging'])

    # Suppress webdriver-manager logs
    os.environ['WDM_LOG_LEVEL'] = '0'
    # Make sure the cache path is user-writable, use temporary dir if needed
    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.implicitly_wait(3) # Implicit wait can sometimes help with dynamic content
        return driver
    except WebDriverException as e:
        error_msg = f"Failed WebDriver init: {e}\n";
        if "net::ERR_INTERNET_DISCONNECTED" in str(e): error_msg += "Please check your internet connection.\n"
        elif "session not created" in str(e) and "version is" in str(e): error_msg += "ChromeDriver version might be incompatible with your Chrome browser. Try manually updating Chrome or clearing the .wdm cache.\n"
        elif "user data directory is already in use" in str(e): error_msg += "Another Chrome process might be using the profile. Close all Chrome instances (including background tasks) and try again.\n"
        else: error_msg += "Check Chrome install/updates/antivirus. Clearing .wdm cache might help.\n"
        # Display error in GUI if possible, otherwise print
        if app and app.master.winfo_exists(): messagebox.showerror("WebDriver Error", error_msg)
        else: print(f"WebDriver Error:\n{error_msg}")
        return None
    except Exception as e:
        err_msg = f"Unexpected WebDriver init error: {e}"
        if app and app.master.winfo_exists(): messagebox.showerror("WebDriver Error", err_msg)
        else: print(err_msg); print(traceback.format_exc())
        return None

def get_definition_list(driver, definition_type, status_queue, stop_event):
    list_url = f"{BASE_URL}/{definition_type}"
    status_queue.put(('status', f"Fetching {definition_type} list from: {list_url}"))
    if stop_event.is_set(): return []
    try: driver.get(list_url); time.sleep(0.2) # Short pause after load
    except WebDriverException as e: status_queue.put(('error', f"Navigation error: {list_url}: {e}")); return []

    definitions = []; wait_time_initial = 15; pause_after_scroll = 0.2
    link_pattern_xpath = f"//a[contains(@href, '/{definition_type}/') and not(contains(@href,'#'))]"
    try:
        status_queue.put(('status', f"  Waiting up to {wait_time_initial}s for initial links..."))
        wait = WebDriverWait(driver, wait_time_initial)
        try: wait.until(EC.presence_of_element_located((By.XPATH, link_pattern_xpath))); status_queue.put(('status', "  Initial links detected. Starting scroll loop..."))
        except TimeoutException: status_queue.put(('error', f"Timeout waiting for initial links for {definition_type}.")); return []

        found_hrefs = set(); stale_scroll_count = 0; max_stale_scrolls = 5
        last_scroll_position = -1 # Track scroll position to detect end more reliably

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
                            # --- Validation Logic (Simplified for clarity, assumed correct from prompt) ---
                            is_valid_name = False
                            if definition_type == 'Tables':
                                if name and (name.isdigit() or (name.count('.') == 1 and all(p.isdigit() for p in name.split('.')))):
                                    is_valid_name = True
                            elif definition_type in ['DataTypes', 'Segments']:
                                if name and name.isalnum():
                                    is_valid_name = True
                            # --- End Validation ---

                            if is_valid_name: found_hrefs.add(href); newly_added_this_pass += 1
                            elif name and name != "#": status_queue.put(('debug', f"  Skipping invalid name '{name}' for type '{definition_type}'"))
                    except StaleElementReferenceException: status_queue.put(('warning', "  Warn: Stale link encountered during scroll check.")); continue
                    except Exception as e: status_queue.put(('warning', f"  Warn: Error processing link attribute: {e}"))

                current_total_hrefs = len(found_hrefs); status_queue.put(('status', f"  Added {newly_added_this_pass} new valid links. Total unique valid: {current_total_hrefs}"))
                if current_total_hrefs == previous_href_count: stale_scroll_count += 1; status_queue.put(('status', f"  Scroll count stable: {stale_scroll_count}/{max_stale_scrolls}"))
                else: stale_scroll_count = 0

                # Scroll Logic: Use last element and check if scroll position changed
                if stale_scroll_count < max_stale_scrolls and current_links:
                    current_scroll_position = driver.execute_script("return window.pageYOffset;")
                    try:
                        # Try scrolling last element into view first
                        driver.execute_script("arguments[0].scrollIntoView(true);", current_links[-1])
                        time.sleep(pause_after_scroll)
                        new_scroll_position = driver.execute_script("return window.pageYOffset;")
                        # If scrollIntoView didn't change position significantly, try scrolling page down
                        if abs(new_scroll_position - current_scroll_position) < 10:
                            driver.execute_script("window.scrollBy(0, window.innerHeight * 0.8);") # Scroll 80% of viewport
                            time.sleep(pause_after_scroll)
                            new_scroll_position = driver.execute_script("return window.pageYOffset;")

                        if abs(new_scroll_position - last_scroll_position) < 10: # Check if position actually changed much
                             status_queue.put(('debug', f"  Scroll position barely changed ({last_scroll_position} -> {new_scroll_position}). Incrementing stale count."))
                             stale_scroll_count += 1
                        else:
                            last_scroll_position = new_scroll_position
                        status_queue.put(('debug', f"  Scrolled. New pos: {new_scroll_position}. Stale: {stale_scroll_count}/{max_stale_scrolls}"))

                    except StaleElementReferenceException: status_queue.put(('warning', "  Warn: Last element became stale before scroll could execute."))
                    except Exception as e: status_queue.put(('error', f"  Error scrolling: {e}")); stale_scroll_count += 1; status_queue.put(('status', f"  Incrementing stale count due to scroll error: {stale_scroll_count}/{max_stale_scrolls}"))

        status_queue.put(('status', "  Finished scroll attempts."))
        # Final name extraction
        definitions.clear(); valid_names_extracted = set()
        for href in found_hrefs:
            try:
                name = href.split('/')[-1].strip()
                if name and name != "#":
                     # Re-validate (redundant but safe)
                    is_final_valid = False
                    if definition_type == 'Tables':
                         if name.isdigit() or (name.count('.') == 1 and all(p.isdigit() for p in name.split('.'))): is_final_valid = True
                    elif definition_type in ['DataTypes', 'Segments']:
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
    text = re.sub(r"^[A-Z0-9]{3}\s*-\s*\d+\s*-\s*", "", text) # Remove PV1-1- type prefixes
    text = re.sub(r"^[A-Z0-9]{3}\s*-\s*\d+\s*", "", text)    # Remove PV1-1 type prefixes
    s = re.sub(r"[^a-zA-Z0-9\s]", "", text).strip()          # Keep only letters, numbers, spaces
    if not s: return "unknownFieldName"
    s = s.title()                                           # Title Case
    s = s.replace(" ", "")                                  # Remove spaces
    return s[0].lower() + s[1:] if s else "unknownFieldName" # camelCase

# --- Direct Scraping Functions (Improved Scroll/Stale Handling, Unchanged from previous version) ---
def scrape_table_details(driver, table_id, status_queue, stop_event):
    """Scrapes Value and Description columns for a Table definition using persistent content-based scrolling."""
    status_queue.put(('debug', f"  Scraping Table {table_id}..."))
    table_data = []
    processed_values = set() # To handle potential duplicates during scroll
    table_locator = (By.XPATH, "//table[contains(@class, 'mat-table') or contains(@class, 'table-definition')]//tbody")
    row_locator = (By.TAG_NAME, "tr")
    value_col_index = 0
    desc_col_index = 1
    pause_after_scroll = 0.5
    stale_content_count = 0
    max_stale_content_scrolls = 10 # Increased tolerance
    scroll_amount = 800 # Pixels to scroll each time

    try:
        WebDriverWait(driver, 15).until(EC.presence_of_element_located(table_locator))
        status_queue.put(('debug', f"    Table body located for Table {table_id}."))

        last_scroll_pos = -1
        while stale_content_count < max_stale_content_scrolls:
            if stop_event.is_set(): raise KeyboardInterrupt("Stop requested during table scroll scrape.")

            tbody = driver.find_element(*table_locator)
            current_view_rows = []
            try:
                current_view_rows = tbody.find_elements(*row_locator)
            except StaleElementReferenceException:
                status_queue.put(('warning', f"    TBody became stale for Table {table_id} while finding rows, retrying scroll/find..."))
                time.sleep(0.3)
                try: driver.execute_script(f"window.scrollBy(0, {scroll_amount // 4});")
                except Exception: pass # Ignore scroll error if driver closed
                time.sleep(pause_after_scroll)
                continue

            newly_added_this_pass = 0
            for row_index, row in enumerate(current_view_rows):
                value_text = None; desc_text = None
                row_identifier_for_log = f"view_row_{row_index}"

                try:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if len(cells) > desc_col_index:
                        try: value_text = cells[value_col_index].text.strip()
                        except StaleElementReferenceException: continue # Skip row if value cell stale
                        row_identifier_for_log = f"value:'{value_text[:20]}...'"

                        if value_text and value_text not in processed_values:
                            try: desc_text = cells[desc_col_index].get_attribute('textContent').strip()
                            except StaleElementReferenceException: continue # Skip row if desc cell stale
                            except Exception as desc_err: desc_text = f"Error: {desc_err}"

                            processed_values.add(value_text)
                            table_data.append({"value": value_text, "description": desc_text or ""})
                            newly_added_this_pass += 1

                except StaleElementReferenceException: continue # Skip row
                except Exception as cell_err: status_queue.put(('warning', f"    Error processing cells row {row_identifier_for_log}: {cell_err}")); continue

            current_total_rows = len(table_data)
            status_queue.put(('debug', f"    Table {table_id} scroll pass: Found {len(current_view_rows)} rows, added {newly_added_this_pass}. Total: {current_total_rows}"))

            if newly_added_this_pass == 0:
                stale_content_count += 1
                status_queue.put(('debug', f"    No new rows Table {table_id}. Stale: {stale_content_count}/{max_stale_content_scrolls}"))
            else:
                stale_content_count = 0

            if stale_content_count < max_stale_content_scrolls:
                try:
                    current_scroll_pos = driver.execute_script("return window.pageYOffset;")
                    # Scroll relative first, then ensure bottom is reached
                    driver.execute_script(f"window.scrollBy(0, {scroll_amount});")
                    time.sleep(pause_after_scroll / 3)
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(pause_after_scroll * 2 / 3)
                    new_scroll_pos = driver.execute_script("return window.pageYOffset;")
                    if abs(new_scroll_pos - last_scroll_pos) < 10: # Check if actually scrolled
                        stale_content_count +=1 # Increment if stuck
                        status_queue.put(('debug', f"    Scroll stuck for {table_id}? Stale: {stale_content_count}/{max_stale_content_scrolls}"))
                    last_scroll_pos = new_scroll_pos
                except Exception as scr_err:
                    status_queue.put(('warning', f"    Scroll error for Table {table_id}: {scr_err}. Assuming end or error."))
                    stale_content_count = max_stale_content_scrolls # Break loop on scroll error

    except TimeoutException: status_queue.put(('error', f"  Timeout finding table body for Table {table_id}.")); return None
    except NoSuchElementException: status_queue.put(('error', f"  Could not find table body for Table {table_id}.")); return None
    except KeyboardInterrupt: raise
    except Exception as e: status_queue.put(('error', f"  Unexpected error scraping Table {table_id}: {e}")); status_queue.put(('error', traceback.format_exc())); return None

    if not table_data and not stop_event.is_set(): status_queue.put(('warning', f"  No data scraped for Table {table_id} (and not stopped)."))
    status_queue.put(('debug', f"  Finished scraping Table {table_id}. Rows: {len(table_data)}"))
    return {str(table_id): table_data} if table_data else None

def scrape_segment_or_datatype_details(driver, definition_type, definition_name, status_queue, stop_event):
    """Scrapes details for Segment or DataType definitions using persistent content-based scrolling."""
    status_queue.put(('debug', f"  Scraping {definition_type} {definition_name}..."))
    parts_data = []
    processed_row_identifiers = set() # Use first column (e.g., "PV1-1") as identifier

    table_locator = (By.XPATH, "//table[contains(@class, 'table-definition') and contains(@class, 'table')]//tbody")
    row_locator = (By.TAG_NAME, "tr")
    seq_col_index = 0; desc_col_index = 1; type_col_index = 2; len_col_index = 3
    opt_col_index = 4; repeat_col_index = 5; table_col_index = 6

    overall_length = -1; pause_after_scroll = 0.5
    stale_content_count = 0; max_stale_content_scrolls = 8 # Increased tolerance
    scroll_amount = 800

    try:
        try: # Get overall length
             length_element = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, "//div[contains(@class,'DefinitionPage_definitionContent')]//span[contains(text(),'Length:')]/following-sibling::span")))
             length_text = length_element.text.strip()
             if length_text.isdigit(): overall_length = int(length_text); status_queue.put(('debug', f"    Found overall length: {overall_length}"))
        except (NoSuchElementException, TimeoutException): status_queue.put(('debug', "    Overall length element not found/timed out."))
        except Exception as len_err: status_queue.put(('warning', f"    Error getting overall length: {len_err}"))

        WebDriverWait(driver, 10).until(EC.presence_of_element_located(table_locator))
        status_queue.put(('debug', f"    Table body located for {definition_name}."))

        last_scroll_pos = -1
        while stale_content_count < max_stale_content_scrolls:
            if stop_event.is_set(): raise KeyboardInterrupt("Stop requested during detail scroll scrape.")

            tbody = driver.find_element(*table_locator)
            current_view_rows = []
            try:
                current_view_rows = tbody.find_elements(*row_locator)
            except StaleElementReferenceException:
                status_queue.put(('warning', f"    TBody became stale for {definition_name}, retrying scroll/find..."))
                time.sleep(0.2)
                try: driver.execute_script(f"window.scrollBy(0, {scroll_amount // 4});")
                except Exception: pass
                time.sleep(pause_after_scroll)
                continue

            newly_added_count = 0
            for row in current_view_rows:
                part = {}; row_identifier = None; table_text = ""

                try:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if len(cells) > table_col_index: # Need all columns
                        try: row_identifier = cells[seq_col_index].text.strip()
                        except StaleElementReferenceException: continue # Skip stale cell
                        if not row_identifier or row_identifier in processed_row_identifiers: continue

                        processed_row_identifiers.add(row_identifier)

                        # Extract Data reliably (using get_attribute for robustness)
                        try: desc_text = cells[desc_col_index].get_attribute('textContent').strip()
                        except StaleElementReferenceException: processed_row_identifiers.remove(row_identifier); continue
                        try: type_text = cells[type_col_index].get_attribute('textContent').strip()
                        except StaleElementReferenceException: processed_row_identifiers.remove(row_identifier); continue
                        try: len_text = cells[len_col_index].get_attribute('textContent').strip()
                        except StaleElementReferenceException: processed_row_identifiers.remove(row_identifier); continue
                        try: opt_text = cells[opt_col_index].get_attribute('textContent').strip().upper()
                        except StaleElementReferenceException: processed_row_identifiers.remove(row_identifier); continue
                        try: repeat_text = cells[repeat_col_index].get_attribute('textContent').strip().upper()
                        except StaleElementReferenceException: processed_row_identifiers.remove(row_identifier); continue
                        try: table_text = cells[table_col_index].get_attribute('textContent').strip()
                        except StaleElementReferenceException: processed_row_identifiers.remove(row_identifier); continue

                        # Build Part Dictionary
                        part['name'] = convert_to_camel_case(desc_text)
                        part['type'] = type_text if type_text else "Unknown"
                        try: part['length'] = int(len_text) if len_text.isdigit() else -1
                        except ValueError: part['length'] = -1
                        if opt_text in ['R', 'C', 'B']: part['mandatory'] = True # Expanded mandatory flags
                        if repeat_text and '-' not in repeat_text: part['repeats'] = True # Simpler repeats check
                        if table_text and (table_text.isdigit() or (table_text.count('.') == 1 and all(p.isdigit() for p in table_text.split('.')))):
                            part['table'] = table_text

                        parts_data.append(part)
                        newly_added_count += 1

                    else: # Log rows with insufficient columns
                         row_text = ""
                         try: row_text = row.text[:60].replace('\n',' ')
                         except StaleElementReferenceException: row_text = "[Stale Row]"
                         status_queue.put(('debug', f"    Skipping row {len(cells)} cols <= {table_col_index}: '{row_text}' in {definition_name}"))

                except StaleElementReferenceException: continue # Skip row if stale during processing
                except Exception as cell_err: status_queue.put(('warning', f"    Error processing row/cell {row_identifier}: {cell_err}")); continue

            current_parts_count = len(parts_data)
            status_queue.put(('debug', f"    {definition_type} {definition_name} scroll pass: Found {len(current_view_rows)}, added {newly_added_count}. Total: {current_parts_count}"))

            if newly_added_count == 0:
                stale_content_count += 1
                status_queue.put(('debug', f"    No new parts {definition_name}. Stale: {stale_content_count}/{max_stale_content_scrolls}"))
            else:
                stale_content_count = 0

            if stale_content_count < max_stale_content_scrolls:
                try:
                    current_scroll_pos = driver.execute_script("return window.pageYOffset;")
                    driver.execute_script(f"window.scrollBy(0, {scroll_amount});")
                    time.sleep(pause_after_scroll / 3)
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(pause_after_scroll * 2/ 3)
                    new_scroll_pos = driver.execute_script("return window.pageYOffset;")
                    if abs(new_scroll_pos - last_scroll_pos) < 10: # Check if actually scrolled
                         stale_content_count +=1 # Increment if stuck
                         status_queue.put(('debug', f"    Scroll stuck for {definition_name}? Stale: {stale_content_count}/{max_stale_content_scrolls}"))
                    last_scroll_pos = new_scroll_pos
                except Exception as scr_err:
                    status_queue.put(('warning', f"    Scroll error for {definition_name}: {scr_err}. Assuming end or error."))
                    stale_content_count = max_stale_content_scrolls # Break loop

    except TimeoutException: status_queue.put(('error', f"  Timeout finding table body for {definition_name}.")); return None
    except NoSuchElementException: status_queue.put(('error', f"  Could not find table body for {definition_name}.")); return None
    except KeyboardInterrupt: raise
    except Exception as e: status_queue.put(('error', f"  Unexpected error scraping {definition_name}: {e}")); status_queue.put(('error', traceback.format_exc())); return None

    if not parts_data and not stop_event.is_set(): status_queue.put(('warning', f"  No parts data scraped for {definition_name} (and not stopped)."))

    # Add standard segment part *if necessary* - done later during final merge now

    # Assemble final structure
    separator_char = "."
    final_structure = {
        "separator": separator_char,
        "versions": {
            HL7_VERSION: {
                "appliesTo": "equalOrGreater",
                "totalFields": len(parts_data), # Will be updated later if standard part added
                "length": overall_length,
                "parts": parts_data
            }
        }
    }
    status_queue.put(('debug', f"  Finished scraping {definition_type} {definition_name}. Parts: {len(parts_data)}"))
    return {definition_name: final_structure} if parts_data else None

# --- Fallback / Combined Processing Function ---
def process_definition_page(driver, definition_type, definition_name, status_queue, stop_event):
    """Attempts direct scraping. If fails or empty, falls back to HTML source + AI."""
    url = f"{BASE_URL}/{definition_type}/{definition_name}"
    status_queue.put(('status', f"Processing {definition_type}: {definition_name}"))
    if stop_event.is_set(): return None, definition_name

    scraped_data = None; ai_data = None; html_save_path = None
    final_data_source = "None"; final_data = None

    # 1. Navigate
    try:
        driver.get(url)
        WebDriverWait(driver, 7).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        time.sleep(0.5) # Extra buffer after body tag appears
    except WebDriverException as nav_err:
        status_queue.put(('error', f"Nav Error {definition_name}: {nav_err}"))
        return None, definition_name
    except TimeoutException:
        status_queue.put(('warning', f"Timeout waiting for body tag on {definition_name}, proceeding anyway."))

    # 2. Attempt Direct Scraping
    try:
        status_queue.put(('status', f"  Scraping {definition_name}..."))
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
                 # Ensure list isn't empty for tables
                 valid_scrape = bool(data_value)
            elif definition_type in ["DataTypes", "Segments"] and isinstance(data_value, dict) and "versions" in data_value:
                 # Ensure parts list isn't empty for types/segments
                 version_key = next(iter(data_value.get("versions", {})), None)
                 if version_key:
                     valid_scrape = bool(data_value["versions"][version_key].get("parts"))

            if valid_scrape:
                status_queue.put(('status', f"  Scraping successful for {definition_name}."))
                final_data_source = "Scraping"
                final_data = scraped_data
            else:
                status_queue.put(('warning', f"  Direct scraping for {definition_name} yielded empty or invalid data. Proceeding to AI fallback."))
                scraped_data = None # Ensure fallback happens
        elif scraped_data is None and not stop_event.is_set():
             status_queue.put(('warning', f"  Direct scraping function returned None for {definition_name}. Proceeding to AI fallback."))
        elif scraped_data is None and stop_event.is_set():
             status_queue.put(('warning', f"  Direct scraping for {definition_name} stopped."))
        else: # Unexpected type
            status_queue.put(('warning', f"  Unexpected scraping result type for {definition_name}: {type(scraped_data)}. Proceeding to AI fallback."))
            scraped_data = None # Ensure fallback happens

    except KeyboardInterrupt:
        status_queue.put(('warning', f"Stop requested during scraping {definition_name}."))
        return None, definition_name
    except Exception as scrape_err:
        status_queue.put(('warning', f"  Direct scraping failed for {definition_name}: {scrape_err}. AI fallback."))
        status_queue.put(('debug', traceback.format_exc()))
        scraped_data = None # Ensure fallback happens

    # 3. Fallback to HTML Source and AI Analysis (if scraping failed/empty and not stopped)
    if final_data is None and not stop_event.is_set():
        status_queue.put(('status', f"  AI Fallback for {definition_name}..."))
        try:
            status_queue.put(('status', f"    Getting HTML source for {definition_name}..."))
            html_content = driver.page_source
            if not html_content or len(html_content) < 500: # Basic check for minimal content
                 raise ValueError(f"Failed to retrieve adequate page source for {definition_name} (len: {len(html_content)}).")
            status_queue.put(('status', f"    Got source ({len(html_content)} bytes)."))

            # --- Save HTML for debugging ---
            try:
                script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
                html_full_dir = os.path.join(script_dir, FALLBACK_HTML_DIR)
                os.makedirs(html_full_dir, exist_ok=True)
                html_filename = f"{definition_type}_{definition_name}_fallback.html"
                html_save_path = os.path.join(html_full_dir, html_filename)
                with open(html_save_path, 'w', encoding='utf-8') as f: f.write(html_content)
                status_queue.put(('debug', f"    Saved fallback HTML: {html_filename}"))
            except Exception as save_err: status_queue.put(('warning', f"    Could not save fallback HTML for {definition_name}: {save_err}"))
            # --- End Save HTML ---

            if stop_event.is_set(): raise KeyboardInterrupt("Stop requested before AI HTML analysis.")

            # --- AI Analysis of HTML ---
            if definition_type == "Tables": ai_data = analyze_table_html_with_gemini(html_content, definition_name)
            elif definition_type == "DataTypes": ai_data = analyze_datatype_html_with_gemini(html_content, definition_name)
            elif definition_type == "Segments": ai_data = analyze_segment_html_with_gemini(html_content, definition_name)
            else: status_queue.put(('error', f"    Unknown type '{definition_type}' for AI fallback."))

            if ai_data:
                # Basic Validation for AI data
                if isinstance(ai_data, dict) and list(ai_data.keys())[0] == (str(definition_name) if definition_type == "Tables" else definition_name):
                     status_queue.put(('status', f"  AI HTML Analysis successful for {definition_name}."))
                     final_data_source = "AI Fallback (HTML)"
                     final_data = ai_data
                else:
                     status_queue.put(('error', f"    AI HTML Analysis for {definition_name} failed validation (key/structure mismatch)."))
                     final_data = None # Ensure it's None if validation fails
            else:
                status_queue.put(('error', f"    AI HTML Analysis failed for {definition_name} (returned None)."))

        except KeyboardInterrupt:
            status_queue.put(('warning', f"Stop requested during AI fallback for {definition_name}."))
            return None, definition_name
        except ValueError as ve: # Catch specific source retrieval error
             status_queue.put(('error', f"Error during AI fallback prep for {definition_name}: {ve}"))
        except WebDriverException as wd_err:
            status_queue.put(('error', f"WebDriver error during AI fallback (source/nav) for {definition_name}: {wd_err}"))
        except Exception as e:
            status_queue.put(('error', f"Error during AI fallback processing {definition_name}: {e}"))
            status_queue.put(('error', traceback.format_exc()))

    # 4. Log final source and return result
    status_queue.put(('status', f"  Finished {definition_name}. Source: {final_data_source}"))
    # time.sleep(0.05) # Reduce sleep
    return final_data, definition_name

# --- Utility Functions (Unchanged) ---
def clear_fallback_html_folder(status_queue):
    """Clears the directory used for saving fallback HTML files."""
    script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
    dir_path = os.path.join(script_dir, FALLBACK_HTML_DIR)
    if os.path.exists(dir_path):
        status_queue.put(('status', f"Cleaning fallback HTML directory: {dir_path}"))
        try:
            if os.path.basename(dir_path) == FALLBACK_HTML_DIR and os.path.isdir(dir_path):
                shutil.rmtree(dir_path)
                os.makedirs(dir_path) # Recreate empty directory
                status_queue.put(('status', "Fallback HTML directory cleared and recreated."))
            else: status_queue.put(('warning', f"Safety check failed: Path name '{os.path.basename(dir_path)}' != '{FALLBACK_HTML_DIR}'. Directory NOT deleted."))
        except Exception as e: status_queue.put(('error', f"Error clearing fallback HTML directory {dir_path}: {e}"))
    else: status_queue.put(('status', "Fallback HTML directory does not exist, nothing to clear."))

def load_existing_definitions(output_file, status_queue):
    script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
    file_path = os.path.join(script_dir, output_file)
    default_structure = {"tables": {}, "dataTypes": {}, "HL7": {}}
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                # Ensure top-level keys exist
                if not isinstance(data, dict): raise ValueError("Invalid format: top level is not a dictionary.")
                data.setdefault("tables", {})
                data.setdefault("dataTypes", {})
                data.setdefault("HL7", {})
                status_queue.put(('status', f"Loaded {len(data.get('tables', {}))} tables, {len(data.get('dataTypes', {}))} dataTypes/segments from cache."))
                return data
        except (json.JSONDecodeError, ValueError, TypeError) as e: status_queue.put(('error', f"Error loading/parsing cache file '{output_file}': {e}. Starting fresh.")); return default_structure
        except Exception as e: status_queue.put(('error', f"Unexpected error reading cache file '{output_file}': {e}. Starting fresh.")); return default_structure
    else: status_queue.put(('status', "No existing JSON file found. Starting fresh.")); return default_structure

def item_exists_in_cache(definition_type, item_name, cache_dict):
    if not cache_dict: return False
    try:
        if definition_type == "Tables": return str(item_name) in cache_dict.get("tables", {})
        elif definition_type in ["DataTypes", "Segments"]: return item_name in cache_dict.get("dataTypes", {})
        else: return False
    except Exception: return False # Be safe

# --- NEW Worker Thread Function (Processes a chunk of definitions) ---
# <<<< NOTE: This function is OUTSIDE the HL7ParserApp class >>>>
def process_definition_chunk_thread(definition_type, definition_chunk, status_queue, stop_event, loaded_definitions):
    """
    Worker thread function that processes a list (chunk) of HL7 definitions.
    It manages its own WebDriver instance.
    REMOVED 'progress_add' calls.
    """
    thread_name = f"Worker-{definition_type}-{os.getpid()}-{threading.get_ident()}" # More unique name
    status_queue.put(('status', f"[{thread_name}] Starting, processing {len(definition_chunk)} items."))
    driver = None
    thread_local_results = {}
    error_count = 0
    items_processed_in_thread = 0
    items_skipped_cache = 0

    try:
        # --- Initialize WebDriver for this worker ---
        driver = setup_driver()
        if not driver:
            error_count = len(definition_chunk)
            items_processed_in_thread = 0
            status_queue.put(('error', f"[{thread_name}] WebDriver init FAILED. Cannot process {len(definition_chunk)} items."))
            # No progress_add to send here anymore
            return thread_local_results, error_count, items_processed_in_thread, items_skipped_cache # Return failure indication

        # --- Process items in the chunk ---
        for item_name in definition_chunk:
            if stop_event.is_set():
                status_queue.put(('warning', f"[{thread_name}] Stop requested before processing '{item_name}'."))
                remaining_items = len(definition_chunk) - (items_processed_in_thread + items_skipped_cache)
                status_queue.put(('debug', f"[{thread_name}] {remaining_items} items not processed due to stop."))
                # No progress_add to send here anymore
                break # Exit the loop for this chunk

            # --- Caching Check ---
            if item_exists_in_cache(definition_type, item_name, loaded_definitions):
                status_queue.put(('debug', f"[{thread_name}] Skipping '{item_name}' - cached."))
                items_skipped_cache += 1
                # status_queue.put(('progress_add', 1)) # <-- REMOVED
                continue # Move to the next item

            # --- Process the Definition Page (Scrape or AI) ---
            processed_data, _ = process_definition_page(driver, definition_type, item_name, status_queue, stop_event)
            items_processed_in_thread += 1 # Increment actual processing attempt count

            # --- Validation / Storing Result ---
            corrected_item_data = None
            processing_successful = False
            if processed_data and isinstance(processed_data, dict):
                if len(processed_data) == 1:
                    final_key = next(iter(processed_data))
                    final_value = processed_data[final_key]
                    expected_key = str(item_name) if definition_type == "Tables" else item_name
                    if final_key == expected_key:
                        if definition_type == "Tables" and isinstance(final_value, list) and final_value: # Must not be empty list
                            if all(isinstance(item, dict) and 'value' in item for item in final_value):
                                corrected_item_data = final_value; processing_successful = True
                        elif definition_type in ["DataTypes", "Segments"] and isinstance(final_value, dict) and "versions" in final_value:
                            version_key = next(iter(final_value.get('versions', {})), None)
                            if version_key and final_value['versions'][version_key].get('parts'): # Must have parts
                                corrected_item_data = final_value; processing_successful = True
                        # Add more specific validation if needed
                        if not processing_successful:
                            status_queue.put(('warning', f"[{thread_name}] Final '{item_name}' failed structure/content validation. Skip."))
                            error_count += 1
                    else: # Key mismatch
                        status_queue.put(('warning', f"[{thread_name}] Final '{item_name}' key mismatch ('{final_key}' vs '{expected_key}'). Skip.")); error_count += 1
                else: # Wrong number of keys
                    status_queue.put(('warning', f"[{thread_name}] Final '{item_name}' dict has != 1 key. Skip.")); error_count += 1
            elif processed_data is None and not stop_event.is_set():
                status_queue.put(('warning', f"[{thread_name}] No final data for '{item_name}' (and not stopped). Skip."))
                error_count += 1
            elif processed_data and not isinstance(processed_data, dict) and not stop_event.is_set(): # Check wrong type
                status_queue.put(('warning', f"[{thread_name}] Final data for '{item_name}' not dict type: {type(processed_data)}. Skip.")); error_count += 1

            if processing_successful and corrected_item_data is not None:
                result_key = str(item_name) if definition_type == "Tables" else item_name
                thread_local_results[result_key] = corrected_item_data

            # status_queue.put(('progress_add', 1)) # <-- REMOVED (This was the main culprit)

        # --- End of item loop ---

    except KeyboardInterrupt:
        status_queue.put(('warning', f"[{thread_name}] Aborted by user request."))
        if not stop_event.is_set(): stop_event.set() # Ensure signal propagates
        error_count += len(definition_chunk) - (items_processed_in_thread + items_skipped_cache) # Count remaining as errors/aborted
        # No progress_add to send here anymore
    except Exception as e:
        status_queue.put(('error', f"[{thread_name}] CRITICAL ERROR: {e}"))
        status_queue.put(('error', traceback.format_exc()))
        error_count += len(definition_chunk) - (items_processed_in_thread + items_skipped_cache) # Count remaining as errors
        # No progress_add to send here anymore
        if not stop_event.is_set(): stop_event.set() # Signal stop on critical error
    finally:
        if driver:
            try:
                driver.quit()
            except Exception as q_err:
                status_queue.put(('error', f"[{thread_name}] Error quitting WebDriver: {q_err}"))

        status_queue.put(('status', f"[{thread_name}] Finished. Processed: {items_processed_in_thread}, Skipped(Cache): {items_skipped_cache}, Errors: {error_count}"))
        # Return the collected results, error count, and processed/skipped counts for this chunk
        return thread_local_results, error_count, items_processed_in_thread, items_skipped_cache

# --- GUI Class ---
class HL7ParserApp:
    def __init__(self, master):
        self.master = master
        master.title(f"HL7 Parser (Scrape+AI Fallback | {MAX_WORKERS} Workers)") # Show worker count
        master.geometry("700x550")
        self.status_queue = queue.Queue()
        self.stop_event = threading.Event()
        self.executor = None # ThreadPoolExecutor instance
        # self.worker_futures = [] # No longer storing futures here, managed in orchestrator
        self.orchestrator_thread = None # For the main orchestrator logic

        # Track progress per category
        self.category_progress = {
            "tables": {"current": 0, "total": 0},
            "datatypes": {"current": 0, "total": 0},
            "segments": {"current": 0, "total": 0}
        }
        self.grand_total_items = 0
        # self.processed_items_count = 0 # <-- REMOVED: No longer needed

        style = ttk.Style(); style.theme_use('clam')
        # --- GUI Setup (Layout remains the same) ---
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
        # --- End GUI Setup ---

    def log_message(self, message, level="info"): # Unchanged
        tag=(); prefix="";
        if level == "error": tag,prefix = (('error',),"ERROR: ")
        elif level == "warning": tag,prefix = (('warning',),"WARNING: ")
        elif level == "debug": tag, prefix = (('debug',), "DEBUG: ")
        else: tag, prefix = ((), "")
        def update_log(): self.log_area.config(state='normal'); self.log_area.insert(tk.END, f"{prefix}{message}\n", tag); self.log_area.see(tk.END); self.log_area.config(state='disabled')
        if self.master.winfo_exists(): # Avoid errors if window closed early
             self.master.after(0, update_log)

    def update_progress(self, bar_type, current, total): # Unchanged (Handles GUI update)
        def update_gui():
            total_val = max(1, total) # Avoid division by zero
            percentage = int((current / total_val) * 100) if total_val > 0 else 0
            pb, lbl = None, None
            count_text = f"{current}/{total}"

            if bar_type == "overall":
                pb, lbl, count_text = (self.pb_overall, self.lbl_overall_perc, f"{percentage}%")
            elif bar_type == "tables":
                pb, lbl = (self.pb_tables, self.lbl_tables_count)
                # Store current/total in self.category_progress (already done in check_queue now)
            elif bar_type == "datatypes":
                pb, lbl = (self.pb_datatypes, self.lbl_datatypes_count)
                # Store current/total in self.category_progress (already done in check_queue now)
            elif bar_type == "segments":
                pb, lbl = (self.pb_segments, self.lbl_segments_count)
                # Store current/total in self.category_progress (already done in check_queue now)

            if pb: pb.config(maximum=total_val, value=min(current, total_val)) # Ensure value doesn't exceed max
            if lbl: lbl.config(text=count_text)

        if self.master.winfo_exists(): # Avoid errors if window closed early
             self.master.after(0, update_gui)

    def check_queue(self): # Handles messages from threads - MODIFIED
        try:
            while True:
                message = self.status_queue.get_nowait()
                msg_type = message[0]
                if msg_type == 'status': self.log_message(message[1])
                elif msg_type == 'error': self.log_message(message[1], level="error")
                elif msg_type == 'warning': self.log_message(message[1], level="warning")
                elif msg_type == 'debug': self.log_message(message[1], level="debug")

                # **** MODIFIED 'progress' HANDLER ****
                elif msg_type == 'progress': # Updates category AND overall progress
                    cat_key, current, total = message[1], message[2], message[3]
                    # 1. Update the specific category's stored progress & GUI
                    if cat_key in self.category_progress:
                        self.category_progress[cat_key]["current"] = current
                        self.category_progress[cat_key]["total"] = total # Update total just in case
                        self.update_progress(cat_key, current, total)

                    # 2. Recalculate and update the overall progress
                    current_overall = sum(prog["current"] for prog in self.category_progress.values())
                    if self.grand_total_items > 0:
                         self.update_progress("overall", current_overall, self.grand_total_items)
                    else:
                         self.update_progress("overall", 0, 1) # Default before totals known

                # **** REMOVED 'progress_add' HANDLER ****

                elif msg_type == 'list_found': # Sets total items and category max - MODIFIED
                    category_name = message[1]; count = message[2]
                    cat_key = category_name.lower()

                    # Calculate difference in total to update grand_total accurately
                    old_total = self.category_progress[cat_key].get("total", 0)
                    self.grand_total_items = self.grand_total_items - old_total + count

                    # Update category progress details
                    self.category_progress[cat_key]["total"] = count
                    self.category_progress[cat_key]["current"] = 0 # Reset current count
                    self.update_progress(cat_key, 0, count) # Update category bar

                    # Update overall bar with potentially new grand total
                    current_overall = sum(prog["current"] for prog in self.category_progress.values())
                    self.update_progress("overall", current_overall, self.grand_total_items)

                    self.log_message(f"Found {count} {category_name}.")

                elif msg_type == 'finished': # Orchestrator finished - MODIFIED
                    error_count = message[1]
                    self.log_message("Processing finished.")
                    # Final progress sync based on totals
                    final_overall_current = sum(prog["total"] for prog in self.category_progress.values())
                    # Ensure grand_total matches sum of category totals
                    self.grand_total_items = final_overall_current
                    self.update_progress("overall", final_overall_current, self.grand_total_items)
                    for cat, prog_data in self.category_progress.items():
                        self.update_progress(cat, prog_data["total"], prog_data["total"])

                    # Reset buttons and show message
                    self.start_button.config(state=tk.NORMAL)
                    self.stop_button.config(state=tk.DISABLED)
                    if error_count is not None and error_count > 0:
                        messagebox.showwarning("Complete with Errors", f"Finished, but with {error_count} errors recorded. Check log and potentially the '{FALLBACK_HTML_DIR}' folder.")
                    elif error_count == 0:
                        messagebox.showinfo("Complete", "Finished successfully!")
                    else: # Likely stopped early or no items
                        messagebox.showinfo("Complete", "Processing finished (may have been aborted or no items found).")
                    self.orchestrator_thread = None
                    self.executor = None
                    # self.worker_futures = [] # Already managed in orchestrator
                    return # Stop checking queue

        except queue.Empty: pass
        # Keep checking if orchestrator is alive
        orchestrator_alive = self.orchestrator_thread and self.orchestrator_thread.is_alive()
        if orchestrator_alive:
            if self.master.winfo_exists(): self.master.after(150, self.check_queue)
        # Perform one last check after orchestrator finishes to catch final messages
        elif self.start_button['state'] == tk.DISABLED:
             if self.master.winfo_exists(): self.master.after(500, self.check_queue)

    def start_processing(self):
        if not load_api_key(): return
        if not configure_gemini(): return

        if self.orchestrator_thread and self.orchestrator_thread.is_alive():
            messagebox.showwarning("Busy", "Processing is already in progress.")
            return

        self.stop_event.clear()
        self.start_button.config(state=tk.DISABLED)
        self.stop_button.config(state=tk.NORMAL)
        self.log_message(f"Starting concurrent processing with up to {MAX_WORKERS} workers...")
        self.log_message("Using headless browsers, caching, scrape+AI fallback...")

        # Reset progress tracking
        self.grand_total_items = 0
        # self.processed_items_count = 0 # Removed
        for cat in self.category_progress:
            self.category_progress[cat] = {"current": 0, "total": 0}
        self.update_progress("tables",0,1); self.lbl_tables_count.config(text="0/0")
        self.update_progress("datatypes",0,1); self.lbl_datatypes_count.config(text="0/0")
        self.update_progress("segments",0,1); self.lbl_segments_count.config(text="0/0")
        self.update_progress("overall",0,1); self.lbl_overall_perc.config(text="0%")

        # self.worker_futures = [] # Removed

        self.orchestrator_thread = threading.Thread(target=self.run_parser_orchestrator,
                                            args=(self.stop_event,),
                                            daemon=True)
        self.orchestrator_thread.start()

        if self.master.winfo_exists(): self.master.after(100, self.check_queue)

    def stop_processing(self): # Unchanged from previous correct version
        if self.orchestrator_thread and self.orchestrator_thread.is_alive():
            if not self.stop_event.is_set():
                self.log_message("Stop request received. Signaling threads...", level="warning")
                self.stop_event.set()
                if self.executor:
                    self.log_message("Attempting to shutdown worker pool...", level="warning")
                    self.executor.shutdown(wait=False, cancel_futures=True)
            self.stop_button.config(state=tk.DISABLED)
        else:
            self.log_message("Stop requested, but no active process found.", level="info")
            self.stop_button.config(state=tk.DISABLED)
            self.start_button.config(state=tk.NORMAL)

    def run_parser_orchestrator(self, stop_event):
        """Orchestrator using ThreadPoolExecutor to manage workers."""
        categories = ["Tables", "DataTypes", "Segments"]
        all_definitions = {} # Holds the lists fetched for each category
        all_new_results = {"Tables": {}, "DataTypes": {}, "Segments": {}}
        total_error_count = 0
        # processed_item_tally = 0 # Not needed

        # Initialize category progress counters locally for orchestrator use
        local_category_progress = {
            "tables": {"current": 0, "total": 0},
            "datatypes": {"current": 0, "total": 0},
            "segments": {"current": 0, "total": 0}
        }
        future_to_category = {} # Map Future objects back to their category

        try:
            # --- Load Cache ---
            self.status_queue.put(('status', "Loading cached definitions..."))
            loaded_definitions = load_existing_definitions(OUTPUT_JSON_FILE, self.status_queue)
            if stop_event.is_set(): raise KeyboardInterrupt("Stop requested during cache load.")

            # --- Get Definition Lists Sequentially ---
            self.status_queue.put(('status', "Fetching definition lists..."))
            list_driver = setup_driver()
            if not list_driver: raise Exception("Failed to create WebDriver for fetching lists.")

            for category in categories:
                if stop_event.is_set(): break
                defs = get_definition_list(list_driver, category, self.status_queue, stop_event)
                all_definitions[category] = defs
                list_count = len(defs)
                cat_key = category.lower()
                local_category_progress[cat_key]["total"] = list_count
                self.status_queue.put(('list_found', category, list_count)) # Signal GUI

            if list_driver: list_driver.quit()
            self.status_queue.put(('status', "Finished fetching lists."))
            if stop_event.is_set(): raise KeyboardInterrupt("Stop requested after list fetch.")

            # --- Setup ThreadPoolExecutor ---
            self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS)
            self.status_queue.put(('status', f"Starting processing with up to {MAX_WORKERS} workers..."))

            # --- Submit Tasks (Chunking) ---
            for category, definitions in all_definitions.items():
                if stop_event.is_set(): break
                if not definitions: continue

                num_items = len(definitions)
                chunk_size = max(1, (num_items + MAX_WORKERS - 1) // MAX_WORKERS)
                chunks = [definitions[i:i + chunk_size] for i in range(0, num_items, chunk_size)]
                self.status_queue.put(('debug', f"Submitting {len(chunks)} chunks for {category} (size ~{chunk_size})"))

                for chunk in chunks:
                    if stop_event.is_set(): break
                    future = self.executor.submit(
                        process_definition_chunk_thread, # Worker function
                        category, chunk, self.status_queue, stop_event, loaded_definitions
                    )
                    future_to_category[future] = category # Map future to its category

                if stop_event.is_set(): break

            if stop_event.is_set():
                 self.status_queue.put(('warning', "Stop requested during task submission. Cancelling pending tasks."))
                 for future in future_to_category:
                     if not future.running() and not future.done():
                         future.cancel()
                 self.executor.shutdown(wait=False, cancel_futures=True)
                 raise KeyboardInterrupt("Stop requested.")

            # --- Collect Results as they Complete ---
            self.status_queue.put(('status', f"Submitted all tasks. Waiting for {len(future_to_category)} chunks to complete..."))
            for future in concurrent.futures.as_completed(future_to_category.keys()):
                category = future_to_category[future] # Get category from completed future
                cat_key = category.lower()
                try:
                    # Get results from worker: results_dict, errors, processed_count, skipped_count
                    chunk_results, chunk_errors, chunk_processed, chunk_skipped = future.result()

                    if chunk_results:
                         all_new_results[category].update(chunk_results) # Add new results to category dict

                    total_error_count += chunk_errors # Accumulate errors

                    # Update local progress counter for this category
                    processed_in_chunk = chunk_processed + chunk_skipped
                    local_category_progress[cat_key]["current"] += processed_in_chunk
                    # Send message for GUI to update its progress bars
                    self.status_queue.put(('progress', cat_key, local_category_progress[cat_key]["current"], local_category_progress[cat_key]["total"]))

                    self.status_queue.put(('debug', f"Chunk for {category} finished. Processed: {chunk_processed}, Skipped: {chunk_skipped}, Errors: {chunk_errors}"))

                except concurrent.futures.CancelledError:
                     self.status_queue.put(('warning', f"Chunk for {category} was cancelled."))
                     total_error_count += 1 # Treat cancellation as an issue
                except Exception as exc:
                    self.status_queue.put(('error', f"Chunk for {category} generated an exception: {exc}"))
                    self.status_queue.put(('error', traceback.format_exc()))
                    total_error_count += 1 # Count chunk failure as error

            self.status_queue.put(('status', "All submitted tasks have completed or been cancelled."))

        except KeyboardInterrupt:
            self.status_queue.put(('warning', "\nOrchestrator aborted by user request."))
            if not stop_event.is_set(): stop_event.set()
            if self.executor:
                 self.executor.shutdown(wait=False, cancel_futures=True)
        except Exception as e:
            self.status_queue.put(('error', f"Orchestrator CRITICAL ERROR: {e}"))
            self.status_queue.put(('error', traceback.format_exc()))
            total_error_count += 1
            if not stop_event.is_set(): stop_event.set()
            if self.executor:
                 self.executor.shutdown(wait=False, cancel_futures=True)
        finally:
            # Shutdown executor gracefully if it hasn't been already
            if self.executor:
                self.executor.shutdown(wait=True) # Wait for running tasks unless stopped
                self.status_queue.put(('status', "Worker pool shutdown complete."))

            # --- Final Merge, Save, Compare, Cleanup ---
            final_definitions = loaded_definitions # Start with the loaded cache
            processed_segments_for_hl7_final = set() # Track segments found for HL7 build
            # Decide if we should proceed with merging and saving
            should_process_results = not stop_event.is_set() or any(all_new_results.values())

            if should_process_results:
                # --- Merge Cache with New Results ---
                self.status_queue.put(('status', "Merging cached and new results..."))
                # Update tables
                final_definitions.setdefault("tables", {}).update(all_new_results.get("Tables", {}))
                # Update dataTypes key with BOTH new DataTypes and new Segments
                final_definitions.setdefault("dataTypes", {}).update(all_new_results.get("DataTypes", {}))
                final_definitions["dataTypes"].update(all_new_results.get("Segments", {}))
                self.status_queue.put(('debug', "Merged Tables, DataTypes, and Segments into final structure."))

                # --- Add Standard Segment Part (Post-processing) ---
                # **** MODIFIED LOGGING FOR CLARITY ****
                self.status_queue.put(('status', "Post-processing: Ensuring standard parts for Segments within final 'dataTypes' structure..."))
                items_to_process = list(final_definitions.get("dataTypes", {}).items()) # Create list to iterate over safely if modifying dict (though we only modify sub-parts here)
                for seg_name, seg_data in items_to_process:
                    # Heuristic to identify items that are likely Segments (3-char alphanumeric name, period separator)
                    # This check runs on ALL items under the 'dataTypes' key now.
                    if isinstance(seg_data, dict) and seg_data.get("separator") == "." and seg_name.isalnum() and len(seg_name) == 3:
                        # If it looks like a segment, add its name for the HL7 structure build
                        processed_segments_for_hl7_final.add(seg_name)
                        # Check if it needs the standard hl7SegmentName part added
                        if "versions" in seg_data and HL7_VERSION in seg_data["versions"]:
                            version_data = seg_data["versions"][HL7_VERSION]
                            if "parts" in version_data:
                                parts_list = version_data["parts"]
                                hl7_seg_part = {"mandatory": True, "name": "hl7SegmentName", "type": "ST", "table": "0076", "length": 3}
                                # Check if first part IS NOT the standard part
                                if not parts_list or parts_list[0].get("name") != "hl7SegmentName":
                                    parts_list.insert(0, hl7_seg_part)
                                    # Update totalFields count after insertion
                                    version_data["totalFields"] = len(parts_list)
                                    # More specific log message:
                                    self.status_queue.put(('debug', f"  Prepended standard part for Segment '{seg_name}' (found in final 'dataTypes' dict)"))

                # --- Build HL7 Structure ---
                self.status_queue.put(('status', "Building/Updating HL7 Structure..."))
                hl7_parts = []
                common_order = ["MSH", "PID", "PV1", "OBR", "OBX"] # Example order
                # Use the set of segment names identified during post-processing
                ordered_segments = [s for s in common_order if s in processed_segments_for_hl7_final]
                other_segments = sorted([s for s in processed_segments_for_hl7_final if s not in common_order])
                final_segment_order = ordered_segments + other_segments

                if not final_segment_order:
                    self.status_queue.put(('warning', "No segments identified in final data to build HL7 structure."))
                else:
                    for seg_name in final_segment_order:
                        # Retrieve the segment definition (from the combined 'dataTypes' dictionary)
                        seg_def = final_definitions["dataTypes"].get(seg_name)
                        # Determine properties for the HL7 part definition
                        is_mand = (seg_name == "MSH")
                        repeats = (seg_name != "MSH") # Simplistic assumption
                        length = -1
                        # Extract length if available in the version data
                        if seg_def and isinstance(seg_def, dict) and 'versions' in seg_def:
                            version_key = next(iter(seg_def.get('versions', {})), None)
                            if version_key and isinstance(seg_def['versions'][version_key], dict):
                                length = seg_def['versions'][version_key].get('length', -1)
                        # Create the part dictionary for the HL7 definition
                        part = {"name": seg_name.lower(), "type": seg_name, "length": length if length is not None else -1}
                        if is_mand: part["mandatory"] = True
                        if repeats: part["repeats"] = True
                        hl7_parts.append(part)
                    # Update the final dictionary with the HL7 definition
                    final_definitions.setdefault("HL7", {}).update({
                         "separator":"\r", "partId":"type",
                         "versions":{ HL7_VERSION: { "appliesTo":"equalOrGreater", "length":-1, "parts":hl7_parts }}
                    })
                    self.status_queue.put(('status', f"HL7 structure updated with {len(hl7_parts)} segments."))
            # --- End should_process_results block ---

            # --- Write Final JSON ---
            if should_process_results:
                self.status_queue.put(('status', f"\nWriting final definitions to {OUTPUT_JSON_FILE}"))
                script_dir=os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
                output_path=os.path.join(script_dir, OUTPUT_JSON_FILE)
                try:
                    # Write the final merged dictionary to the JSON file
                    with open(output_path,'w',encoding='utf-8') as f:
                        json.dump(final_definitions, f, indent=2, ensure_ascii=False)
                    self.status_queue.put(('status', "JSON file written successfully."))

                    # --- Run Comparison ---
                    try:
                        # Dynamically import and potentially reload the comparison module
                        import hl7_comparison
                        import importlib
                        hl7_comparison = importlib.reload(hl7_comparison)
                        self.status_queue.put(('status', "\n--- Running Comparison Against Reference ---"))
                        # Check if necessary components exist in the comparison module
                        if not hasattr(hl7_comparison, 'REFERENCE_FILE') or not hasattr(hl7_comparison, 'compare_hl7_definitions'):
                             raise AttributeError("hl7_comparison module missing required components.")
                        # Construct path to reference file and run comparison
                        ref_file_path = os.path.join(script_dir, hl7_comparison.REFERENCE_FILE)
                        comparison_successful = hl7_comparison.compare_hl7_definitions(
                            generated_filepath=output_path, reference_filepath=ref_file_path, status_queue=self.status_queue
                        )
                        # Log comparison result
                        if comparison_successful: self.status_queue.put(('status', "--- Comparison: Files match reference. ---"))
                        else: self.status_queue.put(('warning', "--- Comparison: Differences detected. ---"))
                    except (ImportError, AttributeError, FileNotFoundError) as comp_err:
                         # Log specific errors related to the comparison module itself
                         self.status_queue.put(('error', f"Comparison skipped: {comp_err}"))
                    except Exception as comp_err:
                        # Log general errors during the comparison process
                        self.status_queue.put(('error', f"Error during comparison: {comp_err}"))
                        self.status_queue.put(('error', traceback.format_exc()))

                    # --- Conditional Cleanup ---
                    # Clear fallback HTML only if NO errors occurred AND processing wasn't stopped
                    if total_error_count == 0 and not stop_event.is_set():
                        self.status_queue.put(('status', "No errors and completed, attempting fallback HTML cleanup."))
                        clear_fallback_html_folder(self.status_queue)
                    elif total_error_count > 0:
                        self.status_queue.put(('warning', f"Errors ({total_error_count}) occurred, fallback HTML files in '{FALLBACK_HTML_DIR}' were NOT deleted."))
                    elif stop_event.is_set():
                         self.status_queue.put(('warning', f"Process stopped, fallback HTML files in '{FALLBACK_HTML_DIR}' were NOT deleted."))

                except Exception as e:
                    # Log error if writing the final JSON file fails
                    self.status_queue.put(('error', f"Failed to write JSON file: {e}")); total_error_count+=1
            elif stop_event.is_set():
                # Log if processing stopped before results could be saved
                self.status_queue.put(('warning', f"Processing stopped early, final JSON file '{OUTPUT_JSON_FILE}' was NOT updated."))
            else:
                 # Log if no new results were processed (e.g., all cached)
                 self.status_queue.put(('status', "No new results processed or processing skipped, JSON file not updated."))

            # Signal Overall Completion to GUI, passing the final error count
            self.status_queue.put(('finished', total_error_count if should_process_results else 0))
# --- Run Application ---
if __name__ == "__main__":
    app = None # Ensure app is None initially
    root = tk.Tk()
    app = HL7ParserApp(root) # Create instance, sets global 'app'
    try:
        root.mainloop()
    except KeyboardInterrupt:
        print("\nCtrl+C detected. Signaling stop...")
        if app:
            app.log_message("Shutdown requested (Ctrl+C)...", level="warning")
            if not app.stop_event.is_set():
                app.stop_event.set()

            # Shutdown executor if running
            if app.executor:
                print("Shutting down worker pool...")
                app.executor.shutdown(wait=False, cancel_futures=True) # Don't wait, try to cancel

            # Wait briefly for orchestrator thread to notice stop signal
            if app.orchestrator_thread and app.orchestrator_thread.is_alive():
                print("Waiting for orchestrator thread...")
                app.orchestrator_thread.join(timeout=5.0) # Short timeout
                if app.orchestrator_thread.is_alive():
                    print("Orchestrator thread did not exit quickly.")

        print("Exiting application.")
        try:
            if root and root.winfo_exists(): root.destroy()
        except tk.TclError: pass # Ignore errors if window already destroyed
        sys.exit(0)