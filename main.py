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
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException, NoSuchElementException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager
# from PIL import Image # No longer needed for screenshots
import google.generativeai as genai
import google.api_core.exceptions
import traceback
# Import the comparison module dynamically later
# import hl7_comparison

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

# --- Gemini API Functions ---
def load_api_key():
    global GEMINI_API_KEY
    try:
        try: script_dir = os.path.dirname(os.path.abspath(__file__))
        except NameError: script_dir = os.getcwd() # Fallback for interactive environments
        key_file_path = os.path.join(script_dir, API_KEY_FILE)
        with open(key_file_path, 'r') as f: GEMINI_API_KEY = f.read().strip()
        if not GEMINI_API_KEY: messagebox.showerror("API Key Error", f"'{API_KEY_FILE}' is empty."); return False
        print("API Key loaded successfully."); return True
    except FileNotFoundError: messagebox.showerror("API Key Error", f"'{API_KEY_FILE}' not found in {script_dir}."); return False
    except Exception as e: messagebox.showerror("API Key Error", f"Error reading API key file: {e}"); return False

def configure_gemini():
    global GEMINI_MODEL
    if not GEMINI_API_KEY: print("Error: API Key not loaded."); return False
    try:
        genai.configure(api_key=GEMINI_API_KEY)
        # Using flash as it's generally faster and sufficient for this text extraction
        GEMINI_MODEL = genai.GenerativeModel('gemini-1.5-flash')
        print("Gemini configured successfully."); return True
    except Exception as e: messagebox.showerror("Gemini Config Error", f"Failed to configure Gemini: {e}"); return False

# --- REVISED Gemini HTML Analysis Functions ---

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

    # --- Prompt Construction for Tables (More Specific) ---
    prompt = f"""
        Analyze the provided HTML source code for the HL7 Table definition page for ID '{definition_name}', version {HL7_VERSION}.
        Focus **exclusively** on the main data table structure, typically within a `<table>` element having classes like 'mat-table' or 'table-definition'. Inside this table, find the `<tbody>` element. Process each `<tr>` element within this `<tbody>`.
        Ignore all HTML outside this primary data table including headers, footers, navigation bars, scripts, styles, and sidebars.
        Within each `<tr>`, identify the `<td>` elements. Assume the **first `<td>` contains the 'Value'** and the **second `<td>` contains the 'Description'** (or sometimes 'Comment'). Extract the exact text content from these two cells for each row.

        Generate a JSON object strictly following these rules:
        1.  The **top-level key MUST be the numeric table ID as a JSON string** (e.g., "{definition_name}").
        2.  The value associated with this key MUST be an **array** of objects.
        3.  Each object in the array represents one data row (`<tr>`) and MUST contain exactly two keys:
            *   `value`: The exact, unmodified string content from the first `<td>` (Value column).
            *   `description`: The exact, unmodified string content from the second `<td>` (Description/Comment column). If the cell is empty, use an empty string "".
        4.  **Do NOT include** any other keys or data. Ensure *all* data rows found in the target `<tbody>` are included.
        5.  Ensure the output is only the raw JSON object, without any explanatory text or markdown ```json ... ``` markers.

        Example structure for table "0001":
        {{
          "{definition_name}": [
            {{ "value": "F", "description": "Female" }},
            {{ "value": "M", "description": "Male" }},
            {{ "value": "O", "description": "Other" }}
          ]
        }}

        Return ONLY the raw JSON object for table '{definition_name}'.
    """
    # --- End Prompt Construction ---

    for attempt in range(max_retries):
        if app and app.stop_event.is_set():
            print(f"  Skip Gemini {definition_type} HTML attempt {attempt+1}: Stop requested.")
            return None
        try:
            print(f"  Attempt {attempt + 1} for {definition_name} {definition_type} HTML analysis...")
            response = GEMINI_MODEL.generate_content(prompt + "\n\nHTML SOURCE:\n```html\n" + html_content + "\n```")

            # Robust JSON cleaning
            json_text = response.text.strip()
            if json_text.startswith("```json"): json_text = json_text[7:]
            elif json_text.startswith("```"): json_text = json_text[3:]
            if json_text.endswith("```"): json_text = json_text[:-3]
            json_text = json_text.strip()

            # Attempt to parse
            parsed_json = json.loads(json_text)
            print(f"  Successfully parsed Gemini {definition_type} HTML response for {definition_name}.")
            # Basic validation
            if not isinstance(parsed_json, dict) or definition_name not in parsed_json or not isinstance(parsed_json[definition_name], list):
                 print(f"Error: Gemini {definition_type} HTML response for {definition_name} failed basic structure validation (top key or list value missing).")
                 return None
            return parsed_json
        except json.JSONDecodeError as e:
            print(f"Error: Bad JSON from Gemini {definition_type} HTML analysis for '{definition_name}': {e}")
            err_line, err_col = getattr(e, 'lineno', 'N/A'), getattr(e, 'colno', 'N/A')
            print(f"  Error at line ~{err_line}, column ~{err_col}")
            print(f"  Received Text (first 500 chars): ```\n{response.text[:500]}\n```") # Log partial raw response
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
            return None # Fail on unexpected errors

    return None # Should only be reached if all retries fail

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

    # --- Prompt Construction for DataTypes (More Specific) ---
    separator_value = "." # Always use period now

    prompt = f"""
        Analyze the provided HTML source code for the HL7 {definition_type} definition page for '{definition_name}', version {HL7_VERSION}.
        Focus **exclusively** on the main data table structure defining the components, typically within a `<table>` element having classes like 'mat-table' or 'table-definition'. Inside this table, find the `<tbody>` element. Process each `<tr>` element within this `<tbody>`.
        Ignore all HTML outside this primary data table including headers, footers, navigation bars, scripts, styles, and sidebars.

        Also, look for an overall 'Length' value, often displayed near the top of the content area, possibly in a `<span>` near text "Length:". If found, extract the numeric value.

        Within each `<tr>` of the main definition table, identify the `<td>` elements corresponding to these columns (order might vary slightly, but use typical HL7 definitions):
        - Column 1 (or near start): Sequence/Identifier (e.g., 'CX-1') - Used for reference only.
        - Column 2: Field Description (e.g., 'ID Number')
        - Column 3: Data Type (e.g., 'ST', 'HD')
        - Column 4: Length (e.g., '15', '*')
        - Column 5: Optionality/Requirement (e.g., 'R', 'O', 'C')
        - Column 6: Repeatability (e.g., 'Y', 'N', '-')
        - Column 7 (or near end): Table ID (e.g., '0363')

        Generate a JSON object strictly following the specified rules:
        1.  Create a **top-level key which is the {definition_type} name** ('{definition_name}').
        2.  The value associated with this key MUST be an object.
        3.  This object MUST contain:
            *   `separator`: **MUST** be set to "{separator_value}" (a period).
            *   `versions`: An object containing a key for the HL7 version ('{HL7_VERSION}').
        4.  The '{HL7_VERSION}' object MUST contain:
            *   `appliesTo`: Set to 'equalOrGreater'.
            *   `totalFields`: The total count of component rows extracted for the 'parts' array below.
            *   `length`: The overall numeric length extracted from the page (as described above). If not found or non-numeric, use -1.
            *   `parts`: An **array** of objects, one for each data row (`<tr>`) processed from the `<tbody>`.
        5.  Each object within the 'parts' array represents a component and MUST contain these core keys:
            *   `name`: Convert the 'Field Description' (column 2) to camelCase (e.g., 'idNumber', 'assigningAuthority'). Remove any prefix like 'CX-1 '. If the description is empty or just '...', use a generic name like 'fieldN' where N is the 1-based row index within the `<tbody>`.
            *   `type`: The exact string content from the 'Data Type' `<td>` (column 3).
            *   `length`: The numeric value from the 'Length' `<td>` (column 4). If the content is '*' or empty/blank or non-numeric, use the integer -1. Otherwise, parse the integer.
        6.  **Conditionally include** these keys in the part object *only* if applicable, based on the corresponding `<td>` content:
            *   `mandatory`: Set to the boolean `true` ONLY if the 'Optionality' `<td>` (column 5) text is exactly 'R', 'C', or 'B' (case-insensitive check is ok, but output 'R'/'C'/'B'). Omit this key entirely otherwise (for 'O', 'W', 'X', '-', etc.).
            *   `repeats`: Set to the boolean `true` ONLY if the 'Repeatability' `<td>` (column 6) text does *not* contain a '-' character (e.g., it contains 'Y', 'N', 'R', '?', or similar indicating it *can* repeat or is conditional). Omit this key entirely if the cell contains only '-'.
            *   `table`: Include this key ONLY if the 'Table ID' `<td>` (column 7) contains a purely numeric string (e.g., "0004", "0363") or a numeric string with one decimal (e.g. "0123.1"). The value associated with the `table` key MUST be that **numeric string**. Omit this key entirely if the cell is empty, non-numeric, or contains other text.
        7. Ensure the output is only the raw JSON object, without any explanatory text or markdown ```json ... ``` markers.

        Example structure for a DataType ('CX') component part object inside the 'parts' array:
        {{ "name": "assigningAuthority", "type": "HD", "length": 227, "table": "0363" }}
        {{ "name": "idNumber", "type": "ST", "length": 15, "mandatory": true }}

        Return ONLY the raw JSON object for '{definition_name}'.
    """
    # --- End Prompt Construction ---

    for attempt in range(max_retries):
        if app and app.stop_event.is_set():
            print(f"  Skip Gemini {definition_type} HTML attempt {attempt+1}: Stop requested.")
            return None
        try:
            print(f"  Attempt {attempt + 1} for {definition_name} {definition_type} HTML analysis...")
            response = GEMINI_MODEL.generate_content(prompt + "\n\nHTML SOURCE:\n```html\n" + html_content + "\n```")

            # Robust JSON cleaning
            json_text = response.text.strip()
            if json_text.startswith("```json"): json_text = json_text[7:]
            elif json_text.startswith("```"): json_text = json_text[3:]
            if json_text.endswith("```"): json_text = json_text[:-3]
            json_text = json_text.strip()

            # Attempt to parse
            parsed_json = json.loads(json_text)
            print(f"  Successfully parsed Gemini {definition_type} HTML response for {definition_name}.")
            # Basic validation
            if not isinstance(parsed_json, dict) or definition_name not in parsed_json or \
               not isinstance(parsed_json[definition_name], dict) or \
               "versions" not in parsed_json[definition_name] or \
               HL7_VERSION not in parsed_json[definition_name]["versions"] or \
               "parts" not in parsed_json[definition_name]["versions"][HL7_VERSION]:
                print(f"Error: Gemini {definition_type} HTML response for {definition_name} failed basic structure validation.")
                return None
            return parsed_json
        except json.JSONDecodeError as e:
            print(f"Error: Bad JSON from Gemini {definition_type} HTML analysis for '{definition_name}': {e}")
            err_line, err_col = getattr(e, 'lineno', 'N/A'), getattr(e, 'colno', 'N/A')
            print(f"  Error at line ~{err_line}, column ~{err_col}")
            print(f"  Received Text (first 500 chars): ```\n{response.text[:500]}\n```")
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

    return None # Should only be reached if all retries fail

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

    # --- Prompt Construction for Segments (More Specific) ---
    separator_value = "." # Always use period now

    prompt = f"""
        Analyze the provided HTML source code for the HL7 {definition_type} definition page for '{definition_name}', version {HL7_VERSION}.
        Focus **exclusively** on the main data table structure defining the fields, typically within a `<table>` element having classes like 'mat-table' or 'table-definition'. Inside this table, find the `<tbody>` element. Process each `<tr>` element within this `<tbody>`.
        Ignore all HTML outside this primary data table including headers, footers, navigation bars, scripts, styles, and sidebars.

        Also, look for an overall 'Length' value, often displayed near the top of the content area, possibly in a `<span>` near text "Length:". If found, extract the numeric value.

        Within each `<tr>` of the main definition table, identify the `<td>` elements corresponding to these columns (order might vary slightly, but use typical HL7 definitions):
        - Column 1 (or near start): Sequence/Identifier (e.g., 'PID-3') - Used for reference only.
        - Column 2: Field Description (e.g., 'Patient Identifier List')
        - Column 3: Data Type (e.g., 'CX')
        - Column 4: Length (e.g., '250', '*')
        - Column 5: Optionality/Requirement (e.g., 'R', 'O', 'C')
        - Column 6: Repeatability (e.g., 'Y', 'N', '-')
        - Column 7 (or near end): Table ID (e.g., '0001')

        Generate a JSON object strictly following the specified rules:
        1.  Create a **top-level key which is the {definition_type} name** ('{definition_name}').
        2.  The value associated with this key MUST be an object.
        3.  This object MUST contain:
            *   `separator`: **MUST** be set to "{separator_value}" (a period).
            *   `versions`: An object containing a key for the HL7 version ('{HL7_VERSION}').
        4.  The '{HL7_VERSION}' object MUST contain:
            *   `appliesTo`: Set to 'equalOrGreater'.
            *   `totalFields`: The total count of field rows extracted for the 'parts' array below. **IMPORTANT**: If the first row's description isn't clearly the segment name itself (e.g., doesn't start with 'Set ID' or similar), assume the standard segment name field needs to be prepended later. Base the count only on the rows you extract from the HTML `<tbody>`.
            *   `length`: The overall numeric length extracted from the page (as described above). If not found or non-numeric, use -1.
            *   `parts`: An **array** of objects, one for each data row (`<tr>`) processed from the `<tbody>`.
        5.  Each object within the 'parts' array represents a field and MUST contain these core keys:
            *   `name`: Convert the 'Field Description' (column 2) to camelCase (e.g., 'patientIdentifierList', 'patientName'). Remove any prefix like 'PID-3 '. If the description is empty or just '...', use a generic name like 'fieldN' where N is the 1-based row index within the `<tbody>`.
            *   `type`: The exact string content from the 'Data Type' `<td>` (column 3).
            *   `length`: The numeric value from the 'Length' `<td>` (column 4). If the content is '*' or empty/blank or non-numeric, use the integer -1. Otherwise, parse the integer.
        6.  **Conditionally include** these keys in the part object *only* if applicable, based on the corresponding `<td>` content:
            *   `mandatory`: Set to the boolean `true` ONLY if the 'Optionality' `<td>` (column 5) text is exactly 'R', 'C', or 'B' (case-insensitive check is ok, but output 'R'/'C'/'B'). Omit this key entirely otherwise (for 'O', 'W', 'X', '-', etc.).
            *   `repeats`: Set to the boolean `true` ONLY if the 'Repeatability' `<td>` (column 6) text does *not* contain a '-' character (e.g., it contains 'Y', 'N', 'R', '?', or similar indicating it *can* repeat or is conditional). Omit this key entirely if the cell contains only '-'.
            *   `table`: Include this key ONLY if the 'Table ID' `<td>` (column 7) contains a purely numeric string (e.g., "0004", "0363") or a numeric string with one decimal (e.g. "0123.1"). The value associated with the `table` key MUST be that **numeric string**. Omit this key entirely if the cell is empty, non-numeric, or contains other text.
        7. Ensure the output is only the raw JSON object, without any explanatory text or markdown ```json ... ``` markers.

        Example structure for a Segment ('PID') component part object inside the 'parts' array:
        {{ "name": "patientIdentifierList", "type": "CX", "length": 250, "mandatory": true, "repeats": true, "table": "0203" }}
        {{ "name": "patientName", "type": "XPN", "length": 250, "mandatory": true, "repeats": true }}

        Return ONLY the raw JSON object for '{definition_name}'.
    """
    # --- End Prompt Construction ---

    for attempt in range(max_retries):
        if app and app.stop_event.is_set():
            print(f"  Skip Gemini {definition_type} HTML attempt {attempt+1}: Stop requested.")
            return None
        try:
            print(f"  Attempt {attempt + 1} for {definition_name} {definition_type} HTML analysis...")
            response = GEMINI_MODEL.generate_content(prompt + "\n\nHTML SOURCE:\n```html\n" + html_content + "\n```")

            # Robust JSON cleaning
            json_text = response.text.strip()
            if json_text.startswith("```json"): json_text = json_text[7:]
            elif json_text.startswith("```"): json_text = json_text[3:]
            if json_text.endswith("```"): json_text = json_text[:-3]
            json_text = json_text.strip()

            # Attempt to parse
            parsed_json = json.loads(json_text)
            print(f"  Successfully parsed Gemini {definition_type} HTML response for {definition_name}.")

            # Basic validation
            if not isinstance(parsed_json, dict) or definition_name not in parsed_json or \
               not isinstance(parsed_json[definition_name], dict) or \
               "versions" not in parsed_json[definition_name] or \
               HL7_VERSION not in parsed_json[definition_name]["versions"] or \
               "parts" not in parsed_json[definition_name]["versions"][HL7_VERSION]:
                print(f"Error: Gemini {definition_type} HTML response for {definition_name} failed basic structure validation.")
                return None

            # --- Post-processing for Segments: Ensure standard part exists ---
            segment_data = parsed_json[definition_name]
            version_data = segment_data["versions"][HL7_VERSION]
            parts_list = version_data["parts"]
            # Standard part definition (matches what scraper produces)
            hl7_seg_part = {"mandatory": True, "name": "hl7SegmentName", "type": "ST", "table": "0076", "length": 3}
            # Check if the first part extracted by AI looks like the standard segment name/ID field
            # This is a heuristic - might need refinement based on actual AI output variations
            first_part_is_standard = False
            if parts_list:
                 first_part_name = parts_list[0].get("name", "").lower()
                 # Common names for the first field (e.g., Set ID - MSH, Set ID - PID)
                 if "setid" in first_part_name or first_part_name == "hl7SegmentName":
                     first_part_is_standard = True
                 # Or if the type is ST and length is 3 (strong indicator for MSH, etc.)
                 elif parts_list[0].get("type") == "ST" and parts_list[0].get("length") == 3:
                     first_part_is_standard = True


            if not first_part_is_standard:
                  parts_list.insert(0, hl7_seg_part)
                  version_data["totalFields"] = len(parts_list) # Update totalFields count
                  print(f"  Prepended standard hl7SegmentName part for {definition_name} (AI Result Post-processing)")
            # --- End Post-processing ---

            return parsed_json
        except json.JSONDecodeError as e:
            print(f"Error: Bad JSON from Gemini {definition_type} HTML analysis for '{definition_name}': {e}")
            err_line, err_col = getattr(e, 'lineno', 'N/A'), getattr(e, 'colno', 'N/A')
            print(f"  Error at line ~{err_line}, column ~{err_col}")
            print(f"  Received Text (first 500 chars): ```\n{response.text[:500]}\n```")
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

    return None # Should only be reached if all retries fail


# --- Selenium Functions ---
def setup_driver():
    """Sets up the Selenium WebDriver."""
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1200") # Set a reasonable default size for headless
    options.add_argument("--log-level=3") # Suppress INFO/WARNING console messages from Chrome/WebDriver
    options.add_experimental_option('excludeSwitches', ['enable-logging']) # Suppress DevTools listening message

    try:
        # Use WebDriver Manager to handle driver installation/updates
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.implicitly_wait(3) # Short implicit wait for elements
        print("WebDriver initialized successfully (headless).")
        return driver
    except WebDriverException as e:
        # Provide more specific user feedback for common WebDriver issues
        error_msg = f"Failed to initialize WebDriver: {e}\n"
        if "net::ERR_INTERNET_DISCONNECTED" in str(e):
            error_msg += "Please check your internet connection.\n"
        elif "session not created" in str(e) and "version is" in str(e):
            error_msg += "ChromeDriver version might be incompatible with your Chrome browser. Try updating Chrome or check WebDriver Manager logs.\n"
        elif "user data directory is already in use" in str(e):
            error_msg += "Another Chrome process might be using the profile. Close all Chrome instances (including background tasks) and try again.\n"
        else:
            error_msg += "Ensure Chrome is installed correctly. Antivirus might interfere. Check WebDriver logs if possible.\n"
        messagebox.showerror("WebDriver Error", error_msg)
        print(f"WebDriver Error:\n{error_msg}")
        return None
    except Exception as e:
        # Catch other potential exceptions during setup
        messagebox.showerror("WebDriver Error", f"An unexpected error occurred during WebDriver initialization: {e}")
        print(f"Unexpected WebDriver Initialization Error:\n{traceback.format_exc()}")
        return None

def get_definition_list(driver, definition_type, status_queue, stop_event):
    """Fetches the list of definitions (Tables, DataTypes, Segments) from the main listing page."""
    list_url = f"{BASE_URL}/{definition_type}"
    status_queue.put(('status', f"Fetching {definition_type} list from: {list_url}"))
    if stop_event.is_set(): return []

    try:
        driver.get(list_url)
        # Allow a brief moment for initial JS execution after load
        time.sleep(0.3)
    except WebDriverException as e:
        status_queue.put(('error', f"Navigation error for {list_url}: {e}"))
        return []

    definitions = []
    wait_time_initial = 15 # Max time to wait for the *first* links to appear
    pause_after_scroll = 0.2 # Pause after scrolling to allow content to load
    # XPath to find relevant links (adjust if site structure changes)
    link_pattern_xpath = f"//a[contains(@href, '/{definition_type}/') and not(contains(@href,'#')) and string-length(substring-after(@href, '/{definition_type}/')) > 0]"

    try:
        status_queue.put(('status', f"  Waiting up to {wait_time_initial}s for initial links..."))
        wait = WebDriverWait(driver, wait_time_initial)
        try:
            # Wait for at least one matching link to be present
            wait.until(EC.presence_of_element_located((By.XPATH, link_pattern_xpath)))
            status_queue.put(('status', "  Initial links detected. Starting scroll loop..."))
        except TimeoutException:
            # If no links appear after the wait, the list might be empty or the page failed to load correctly
            status_queue.put(('error', f"Timeout: No definition links found for {definition_type} on {list_url} after {wait_time_initial}s."))
            return []

        found_hrefs = set()
        stale_scroll_count = 0
        max_stale_scrolls = 5 # Number of consecutive scrolls with no new links before stopping

        while stale_scroll_count < max_stale_scrolls:
            if stop_event.is_set():
                status_queue.put(('warning', f"Stop requested during {definition_type} list scroll."))
                break

            previous_href_count = len(found_hrefs)
            current_links = []
            try:
                # Wait briefly for links to be stable in the current view
                WebDriverWait(driver, 3).until(EC.presence_of_element_located((By.XPATH, link_pattern_xpath)))
                # Find all matching links currently loaded
                current_links = driver.find_elements(By.XPATH, link_pattern_xpath)
            except TimeoutException:
                status_queue.put(('warning', f"  Warn: No links found in current view for {definition_type} after scroll/wait (likely end of list)."))
                # This is expected at the end, contributes to stale count
            except Exception as e:
                status_queue.put(('error', f"  Error finding links during scroll for {definition_type}: {e}"))
                break # Stop if there's an error finding links

            if not current_links:
                # If no links are found at all in the view, increment stale count
                stale_scroll_count += 1
                status_queue.put(('status', f"  No links currently visible. Stale count: {stale_scroll_count}/{max_stale_scrolls}"))
            else:
                newly_added_this_pass = 0
                # Process links found in the current view
                for link in current_links:
                    try:
                        href = link.get_attribute('href')
                        # Basic check for valid href and ensure it hasn't been seen
                        if href and f"/{definition_type}/" in href and href not in found_hrefs:
                            # Extract name (last part of URL)
                            name = href.split('/')[-1].strip()
                            if not name: continue # Skip if name is empty after split

                            # --- Validation Logic ---
                            is_valid_name = False
                            validation_reason = "Unknown"
                            if definition_type == 'Tables':
                                # Allow only purely numeric or numeric with one decimal point
                                clean_name = name
                                if not clean_name: validation_reason = "Name is empty"
                                elif clean_name.count('.') == 0 and clean_name.isdigit():
                                    is_valid_name = True; validation_reason = "Purely numeric"
                                elif clean_name.count('.') == 1:
                                     parts = clean_name.split('.')
                                     if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
                                         is_valid_name = True; validation_reason = "Numeric with one decimal"
                                     else: validation_reason = "Invalid decimal format"
                                else: validation_reason = f"Contains invalid structure (dots={clean_name.count('.')})"
                                if not is_valid_name and validation_reason == "Unknown": # If neither matched
                                    validation_reason = "Contains invalid characters or structure"
                                status_queue.put(('debug', f"Checking Table name: '{name}'. Is Valid: {is_valid_name}. Reason: {validation_reason}"))
                            else: # DataTypes/Segments: Allow alphanumeric
                                if name.isalnum():
                                    is_valid_name = True; validation_reason = "Is alphanumeric"
                                else: validation_reason = "Is not alphanumeric"
                            # --- End Validation ---

                            if is_valid_name:
                                found_hrefs.add(href)
                                newly_added_this_pass += 1
                            else:
                                # Log skipped items only in debug mode to avoid excessive logging
                                status_queue.put(('debug', f"  Skipping '{name}' for type '{definition_type}' because Is Valid = {is_valid_name} (Reason: {validation_reason})"))

                    except StaleElementReferenceException:
                        # Element disappeared between finding and getting attribute
                        status_queue.put(('warning', "  Warn: Stale link encountered during scroll check."))
                        continue # Skip this stale link
                    except Exception as e:
                        status_queue.put(('warning', f"  Warn: Error processing link attribute: {e}"))

                current_total_hrefs = len(found_hrefs)
                status_queue.put(('status', f"  Added {newly_added_this_pass} new valid links. Total unique valid: {current_total_hrefs}"))

                # Check if new links were added in this pass
                if current_total_hrefs == previous_href_count:
                    stale_scroll_count += 1
                    status_queue.put(('status', f"  Scroll count stable: {stale_scroll_count}/{max_stale_scrolls}"))
                else:
                    stale_scroll_count = 0 # Reset stale count if new links were found

                # Scroll the last found item into view to potentially load more
                if stale_scroll_count < max_stale_scrolls and current_links:
                    try:
                        # Scroll the *last element found in this pass* into view
                        driver.execute_script("arguments[0].scrollIntoView(true);", current_links[-1])
                        status_queue.put(('status', f"  Scrolling last item into view. Pausing {pause_after_scroll}s..."))
                        time.sleep(pause_after_scroll)
                    except StaleElementReferenceException:
                        status_queue.put(('warning', "  Warn: Last element became stale before scroll could execute."))
                    except Exception as e:
                        status_queue.put(('error', f"  Error scrolling last element: {e}"))
                        # Increment stale count if scrolling fails, as we can't advance
                        stale_scroll_count += 1
                        status_queue.put(('status', f"  Incrementing stale count due to scroll error: {stale_scroll_count}/{max_stale_scrolls}"))

        status_queue.put(('status', "  Finished scroll attempts."))

        # Final extraction of valid names from the collected hrefs
        definitions.clear()
        valid_names_extracted = set()
        for href in found_hrefs:
            try:
                name = href.split('/')[-1].strip()
                if name:
                    # Re-apply validation to be absolutely sure
                    is_final_valid = False
                    if definition_type == 'Tables':
                         if name.count('.') == 0 and name.isdigit(): is_final_valid = True
                         elif name.count('.') == 1:
                             parts = name.split('.');
                             if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit(): is_final_valid = True
                    else: # DataTypes/Segments
                        if name.isalnum(): is_final_valid = True

                    if is_final_valid:
                        valid_names_extracted.add(name)
                    # else: # Optional: Log final validation failures if needed
                    #    status_queue.put(('debug', f"  Final check failed for name '{name}' from href '{href}'. Skipping."))
            except Exception as e:
                status_queue.put(('warning', f"Warn: Error extracting name from final href '{href}': {e}"))

        definitions = sorted(list(valid_names_extracted))

        if not definitions and len(found_hrefs) > 0:
            status_queue.put(('warning', f"Warning: Collected {len(found_hrefs)} hrefs, but failed to extract valid names for {definition_type}."))
        elif not definitions and not stop_event.is_set():
            status_queue.put(('warning', f"Warning: No valid {definition_type} definitions found (and not stopped)."))

    except TimeoutException:
        status_queue.put(('error', f"Timeout waiting for initial links for {definition_type}: {list_url}"))
    except WebDriverException as e:
        status_queue.put(('error', f"WebDriver error during {definition_type} list fetch: {e}"))
    except Exception as e:
        status_queue.put(('error', f"Unexpected error fetching {definition_type} list: {e}"))
        status_queue.put(('error', traceback.format_exc()))

    # Report final count found by the list function itself
    status_queue.put(('status', f"Final count: Found {len(definitions)} unique valid {definition_type}."))
    return definitions

# Revised convert_to_camel_case function
def convert_to_camel_case(text):
    """Converts a descriptive string to camelCase, handling purely numeric results."""
    if not text: return "unknownFieldName"
    # Remove common prefixes like "PV1-1 -" or "CX-3 "
    text = re.sub(r"^[A-Z0-9]{3}\s*-\s*\d+\s*-\s*", "", text) # e.g., "PV1-1 - "
    text = re.sub(r"^[A-Z0-9]{3}\s*-\s*\d+\s*", "", text) # e.g., "CX-3 "
    # Remove non-alphanumeric characters (except spaces) and strip whitespace
    s = re.sub(r"[^a-zA-Z0-9\s]", "", text).strip()
    if not s: return "unknownFieldName" # Handle empty string after cleaning

    # Check if the cleaned string is purely numeric *before* title casing
    if s.isdigit():
        # If it's just a number, prepend "value" or similar to make it a valid name
        # Using "field" might be slightly more generic if it's not always a value
        return f"field{s}" # e.g., "field120"

    # If not purely numeric, proceed with camel casing
    s = s.title()
    s = s.replace(" ", "")
    # Lowercase the first letter
    return s[0].lower() + s[1:] if s else "unknownFieldName"

# Revised scrape_segment_or_datatype_details with added DEBUG logging
def scrape_segment_or_datatype_details(driver, definition_type, definition_name, status_queue, stop_event):
    """Scrapes details for Segment or DataType definitions using robust scrolling."""
    status_queue.put(('debug', f"  Scraping {definition_type} {definition_name}..."))
    parts_data = []
    processed_row_identifiers = set()

    table_locator = (By.XPATH, "//table[contains(@class, 'table-definition') or contains(@class, 'mat-table')]//tbody")
    row_locator = (By.TAG_NAME, "tr")
    seq_col_index = 0
    desc_col_index = 1 # Field Description <<<<<<<< ASSUMED INDEX
    type_col_index = 2
    len_col_index = 3 # Length <<<<<<<<<< ASSUMED INDEX
    opt_col_index = 4
    repeat_col_index = 5
    table_col_index = 6

    overall_length = -1
    pause_after_scroll = 0.4

    stale_content_count = 0
    max_stale_content_scrolls = 8
    scroll_amount_partial = 600
    scroll_attempts_this_pass = 0
    max_scroll_attempts_per_pass = 2

    try:
        # --- Try to get overall length (remains the same) ---
        try:
             length_element = driver.find_element(By.XPATH, "//div[contains(@class,'DefinitionPage_definitionContent__')]//span[contains(translate(., 'LENGTH:', 'length:'), 'length:')]/following-sibling::span")
             length_text = length_element.text.strip()
             if length_text.isdigit(): overall_length = int(length_text); status_queue.put(('debug', f"    Found overall length: {overall_length}"))
             else: status_queue.put(('debug', f"    Found length text '{length_text}', couldn't parse as number."))
        except NoSuchElementException: status_queue.put(('debug', "    Overall length element not found."))
        except Exception as len_err: status_queue.put(('warning', f"    Error getting overall length: {len_err}"))
        # --- End Overall Length ---

        WebDriverWait(driver, 10).until(EC.presence_of_element_located(table_locator))
        status_queue.put(('debug', f"    Definition table body located for {definition_name}."))

        # --- SCROLLING LOOP ---
        while stale_content_count < max_stale_content_scrolls:
            if stop_event.is_set(): raise KeyboardInterrupt("Stop requested during detail scroll scrape.")

            tbody = None
            try:
                tbody = driver.find_element(*table_locator)
                current_view_rows = tbody.find_elements(*row_locator)
            except (NoSuchElementException, StaleElementReferenceException):
                status_queue.put(('warning', f"    TBody or rows became stale/not found for {definition_name} before processing, attempting recovery scroll..."))
                driver.execute_script("window.scrollBy(0, 100);")
                time.sleep(pause_after_scroll)
                stale_content_count +=1
                continue

            newly_added_count = 0

            # Process rows currently visible
            for row_idx, row in enumerate(current_view_rows): # Use enumerate for clearer row index logging
                part = {}
                row_identifier = f"view_row_{row_idx}" # Default identifier for logging
                table_text = ""

                try:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    # Check if row has enough cells before accessing indices
                    if len(cells) > max(seq_col_index, desc_col_index, type_col_index, len_col_index, opt_col_index, repeat_col_index, table_col_index):

                        # --- Start Detailed Debug Logging ---
                        try:
                            # Safely get row identifier from seq column
                            row_identifier_text = cells[seq_col_index].text.strip()
                            if row_identifier_text:
                                row_identifier = row_identifier_text # Use actual identifier if available

                            # Log all cell texts for this row
                            cell_texts = [cell.text.strip() for cell in cells]
                            status_queue.put(('debug', f"    [{definition_name} Row {row_identifier}] Cells found: {len(cell_texts)}. Texts: {cell_texts}"))
                            # Log specifically the assumed description and length cells
                            desc_candidate = cell_texts[desc_col_index] if len(cell_texts) > desc_col_index else "N/A"
                            len_candidate = cell_texts[len_col_index] if len(cell_texts) > len_col_index else "N/A"
                            status_queue.put(('debug', f"    [{definition_name} Row {row_identifier}] Using Desc Idx[{desc_col_index}]: '{desc_candidate}', Len Idx[{len_col_index}]: '{len_candidate}'"))
                        except StaleElementReferenceException:
                             status_queue.put(('debug', f"    [{definition_name} Row {row_identifier}] Stale element during debug log cell text extraction."))
                             continue # Skip row if critical cell is stale during logging
                        except Exception as dbg_err:
                             status_queue.put(('debug', f"    [{definition_name} Row {row_identifier}] Error during debug log cell text extraction: {dbg_err}"))
                        # --- End Detailed Debug Logging ---

                        # Proceed only if we have an identifier from the sequence column
                        if not row_identifier_text:
                             status_queue.put(('debug', f"    [{definition_name} Row {row_identifier}] Skipping row due to missing sequence identifier (index {seq_col_index})."))
                             continue

                        # Check uniqueness *before* full processing
                        if row_identifier in processed_row_identifiers:
                             status_queue.put(('debug', f"    [{definition_name} Row {row_identifier}] Skipping already processed row."))
                             continue

                        # --- Process data only if unique ---
                        processed_row_identifiers.add(row_identifier)

                        # Extract Data (with individual stale checks)
                        try: desc_text = cells[desc_col_index].text.strip()
                        except StaleElementReferenceException: desc_text = "StaleDesc"
                        try: type_text = cells[type_col_index].text.strip()
                        except StaleElementReferenceException: type_text = "StaleType"
                        try: len_text = cells[len_col_index].text.strip()
                        except StaleElementReferenceException: len_text = "*" # Treat stale length as unknown
                        try: opt_text = cells[opt_col_index].text.strip().upper()
                        except StaleElementReferenceException: opt_text = "O" # Assume optional if stale
                        try: repeat_text = cells[repeat_col_index].text.strip() # Don't uppercase yet
                        except StaleElementReferenceException: repeat_text = "-" # Assume non-repeating if stale
                        try: table_text = cells[table_col_index].text.strip()
                        except StaleElementReferenceException: table_text = "" # Assume no table if stale

                        # -------> Build Part Dictionary using REVISED convert_to_camel_case <-------
                        part['name'] = convert_to_camel_case(desc_text) if desc_text != "StaleDesc" else row_identifier.replace("-","_").lower()
                        part['type'] = type_text if type_text != "StaleType" else "Unknown"

                        # Parse Length
                        try: part['length'] = int(len_text) if len_text.isdigit() else -1
                        except ValueError: part['length'] = -1

                        # Mandatory Flag (R, C, B)
                        if opt_text in ['R', 'C', 'B']: part['mandatory'] = True

                        # Repeats Flag (check if NOT '-')
                        if repeat_text != '-': part['repeats'] = True

                        # Table ID (numeric or numeric.numeric)
                        if table_text:
                            if table_text.isdigit():
                                part['table'] = table_text
                            elif '.' in table_text:
                                table_parts = table_text.split('.')
                                if len(table_parts) == 2 and table_parts[0].isdigit() and table_parts[1].isdigit():
                                    part['table'] = table_text # Keep the string with decimal

                        parts_data.append(part)
                        newly_added_count += 1

                    else: # Log rows with insufficient columns
                       row_text_snippet = row.text[:50].replace('\n', ' ') if row.text else "EMPTY ROW"
                       status_queue.put(('debug', f"    [{definition_name} Row {row_identifier}] Skipping row with insufficient columns ({len(cells)} <= {table_col_index}): '{row_text_snippet}'..."))

                except StaleElementReferenceException:
                    status_queue.put(('warning', f"    [{definition_name} Row {row_identifier}] Stale row/cell encountered mid-processing, continuing pass..."))
                    if row_identifier and row_identifier in processed_row_identifiers and part not in parts_data:
                         try: processed_row_identifiers.remove(row_identifier)
                         except KeyError: pass # Ignore if already removed somehow
                    continue
                except Exception as cell_err:
                    status_queue.put(('warning', f"    [{definition_name} Row {row_identifier}] Error processing row/cell: {cell_err}"))
                    if row_identifier and row_identifier in processed_row_identifiers and part not in parts_data:
                         try: processed_row_identifiers.remove(row_identifier)
                         except KeyError: pass
                    continue
            # End of row processing loop for this view

            # --- Post-pass logic (Stale Check and Scrolling) ---
            current_parts_count = len(parts_data)
            status_queue.put(('debug', f"    [{definition_name}] scroll pass end: Found {len(current_view_rows)} rows in view, added {newly_added_count} new unique parts. Total unique: {current_parts_count}"))

            if newly_added_count == 0:
                stale_content_count += 1
                status_queue.put(('debug', f"    [{definition_name}] No new unique parts added. Stale count: {stale_content_count}/{max_stale_content_scrolls}"))
                scroll_attempts_this_pass = 0 # Reset scroll attempts for next stale check
            else:
                stale_content_count = 0
                scroll_attempts_this_pass = 0

            if stale_content_count < max_stale_content_scrolls:
                # Try different scroll strategies if content seems stale
                if newly_added_count == 0 and scroll_attempts_this_pass < max_scroll_attempts_per_pass:
                     # If first attempt failed, try scrolling to bottom
                     driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                     status_queue.put(('debug', f"    [{definition_name}] Scroll to bottom attempt {scroll_attempts_this_pass + 1}"))
                     scroll_attempts_this_pass += 1
                else:
                    # Otherwise, do a standard partial scroll
                    driver.execute_script(f"window.scrollBy(0, {scroll_amount_partial});")
                    status_queue.put(('debug', f"    [{definition_name}] Partial scroll ({scroll_amount_partial}px)."))
                    # Reset attempts if we found content or finished attempts for this stale check
                    scroll_attempts_this_pass = 0

                time.sleep(pause_after_scroll)


    # --- Exception Handling (remains the same) ---
    except TimeoutException: status_queue.put(('error', f"  Timeout finding definition table body for {definition_name}.")); return None
    except NoSuchElementException: status_queue.put(('error', f"  Could not find definition table body for {definition_name} (check locator).")); return None
    except KeyboardInterrupt: raise # Propagate stop request
    except Exception as e: status_queue.put(('error', f"  Unexpected error scraping {definition_name}: {e}")); status_queue.put(('error', traceback.format_exc())); return None

    # --- Return Logic (remains the same) ---
    if not parts_data and not stop_event.is_set():
        status_queue.put(('warning', f"  No parts data scraped for {definition_name} (and not stopped). Check HTML or scraping logic."))
        return None

    # Add standard segment part IF it's a Segment AND if it's missing
    if definition_type == "Segments":
        first_part_is_standard = False
        if parts_data:
            first_part_name = parts_data[0].get("name", "").lower()
            # Heuristic check (similar to AI post-processing)
            if "setid" in first_part_name or first_part_name == "hl7SegmentName":
                 first_part_is_standard = True
            elif parts_data[0].get("type") == "ST" and parts_data[0].get("length") == 3:
                 first_part_is_standard = True

        if not first_part_is_standard:
            hl7_seg_part = {"mandatory": True, "name": "hl7SegmentName", "type": "ST", "table": "0076", "length": 3}
            parts_data.insert(0, hl7_seg_part)
            status_queue.put(('debug', f"  Prepended standard part for Segment {definition_name} (Scraper)"))

    # --- Assemble final structure with Standard Separator ---
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
    status_queue.put(('debug', f"  Finished scraping {definition_type} {definition_name}. Final parts count: {len(parts_data)}"))
    return {definition_name: final_structure}

# --- REVISED Scraping Functions ---

def scrape_table_details(driver, table_id, status_queue, stop_event):
    """Scrapes Value and Description columns for a Table definition using robust scrolling."""
    status_queue.put(('debug', f"  Scraping Table {table_id}..."))
    table_data = []
    processed_values = set() # Track unique values found to avoid duplicates
    # More specific locator for the table body
    table_locator = (By.XPATH, "//table[contains(@class, 'table-definition') or contains(@class, 'mat-table')]//tbody")
    row_locator = (By.TAG_NAME, "tr")
    value_col_index = 0 # Assume 'Value' is the first column (td)
    desc_col_index = 1  # Assume 'Description'/'Comment' is the second column (td)
    pause_after_scroll = 0.3 # Increased pause slightly

    # --- Scrolling Logic Variables ---
    stale_content_count = 0
    max_stale_content_scrolls = 8 # Increased tolerance for no new content
    scroll_amount_partial = 500 # Pixels for partial scroll
    scroll_attempts_this_pass = 0
    max_scroll_attempts_per_pass = 2 # Try scrolling to bottom twice if partial doesn't work

    try:
        # Wait for the table body to be present
        WebDriverWait(driver, 10).until(EC.presence_of_element_located(table_locator))
        status_queue.put(('debug', f"    Table body located for Table {table_id}."))

        # --- SCROLLING LOOP ---
        while stale_content_count < max_stale_content_scrolls:
            if stop_event.is_set(): raise KeyboardInterrupt("Stop requested during table scroll scrape.")

            tbody = None
            try:
                # Re-find tbody each time in case of DOM changes
                tbody = driver.find_element(*table_locator)
                current_view_rows = tbody.find_elements(*row_locator)
            except (NoSuchElementException, StaleElementReferenceException):
                status_queue.put(('warning', f"    TBody or rows became stale/not found for Table {table_id} before processing, attempting recovery scroll..."))
                driver.execute_script("window.scrollBy(0, 100);") # Gentle scroll to maybe fix view
                time.sleep(pause_after_scroll)
                stale_content_count +=1 # Count this as a stale attempt
                continue # Skip processing this pass

            newly_added_this_pass = 0 # Count new unique items found in *this specific scroll view*

            # Process rows currently visible
            for row_index, row in enumerate(current_view_rows):
                value_text = None
                desc_text = None
                row_identifier_for_log = f"view_row_{row_index}"

                try:
                    # Find cells *within this specific row*
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if len(cells) > desc_col_index: # Check if enough cells exist

                        # Extract Value (handle staleness)
                        try:
                            value_text = cells[value_col_index].text.strip()
                            row_identifier_for_log = f"value:'{value_text[:20]}'" # Use value for logging if found
                        except StaleElementReferenceException:
                            status_queue.put(('debug', f"    Stale Value cell in Table {table_id} row ~{row_identifier_for_log}, skipping row."))
                            continue # Skip this row

                        # Process only if value is found and is unique
                        if value_text and value_text not in processed_values:
                            # Extract Description (handle staleness)
                            try:
                                # Try textContent first, might be more stable
                                desc_text = cells[desc_col_index].get_attribute('textContent').strip()
                                if desc_text is None: # Fallback if textContent is null
                                     desc_text = cells[desc_col_index].text.strip()
                            except StaleElementReferenceException:
                                status_queue.put(('debug', f"    Stale Description cell in Table {table_id} row {row_identifier_for_log}, skipping description."))
                                desc_text = "" # Use empty string if desc cell is stale
                            except Exception as desc_err:
                                status_queue.put(('warning', f"    Error getting description cell text in Table {table_id} row {row_identifier_for_log}: {desc_err}"))
                                desc_text = "Extraction Error"

                            # Add the successfully extracted pair
                            processed_values.add(value_text)
                            table_data.append({"value": value_text, "description": desc_text or ""})
                            newly_added_this_pass += 1
                        # else: # Debugging for skipped rows (can be noisy)
                        #     if value_text and value_text in processed_values:
                        #         status_queue.put(('debug', f"    Skipping duplicate value '{value_text}' in Table {table_id}"))

                    # else: # Log rows with too few columns
                    #    row_text_snippet = row.text[:50].replace('\n',' ') if row.text else "EMPTY ROW"
                    #    status_queue.put(('debug', f"    Skipping row with insufficient columns ({len(cells)} <= {desc_col_index}) in Table {table_id}: '{row_text_snippet}'"))

                except StaleElementReferenceException:
                    status_queue.put(('warning', f"    Stale row encountered mid-processing in Table {table_id} row ~{row_identifier_for_log}, continuing pass..."))
                    continue # Skip this specific problematic row
                except Exception as cell_err:
                    status_queue.put(('warning', f"    Error processing cells in Table {table_id} row ~{row_identifier_for_log}: {cell_err}"))
                    continue

            # --- End of processing rows in current view ---

            current_total_unique_rows = len(table_data)
            status_queue.put(('debug', f"    Table {table_id} scroll pass: Found {len(current_view_rows)} rows in view, added {newly_added_this_pass} new unique rows. Total unique: {current_total_unique_rows}"))

            # --- Stale Check and Scrolling ---
            if newly_added_this_pass == 0:
                stale_content_count += 1
                status_queue.put(('debug', f"    No new unique rows added for Table {table_id}. Stale count: {stale_content_count}/{max_stale_content_scrolls}"))
                scroll_attempts_this_pass = 0 # Reset scroll attempts for next stale check
            else:
                stale_content_count = 0 # Reset if new content found
                scroll_attempts_this_pass = 0

            # Perform scrolling if not reached max stale count
            if stale_content_count < max_stale_content_scrolls:
                # Try partial scroll first
                if scroll_attempts_this_pass == 0:
                    driver.execute_script(f"window.scrollBy(0, {scroll_amount_partial});")
                    status_queue.put(('debug', f"    Partial scroll ({scroll_amount_partial}px) for Table {table_id}."))
                # If still stale, try full scroll to bottom (up to N times)
                else:
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    status_queue.put(('debug', f"    Scroll to bottom attempt {scroll_attempts_this_pass} for Table {table_id}."))

                time.sleep(pause_after_scroll)
                scroll_attempts_this_pass += 1
                if newly_added_this_pass == 0 and scroll_attempts_this_pass >= max_scroll_attempts_per_pass:
                    # If we've tried scrolling multiple ways and still get nothing new,
                    # assume we're done scrolling for this pass, let stale count increase.
                    pass


    # --- Exception Handling ---
    except TimeoutException: status_queue.put(('error', f"  Timeout finding table body for Table {table_id}.")); return None
    except NoSuchElementException: status_queue.put(('error', f"  Could not find table body for Table {table_id} (check locator).")); return None
    except KeyboardInterrupt: raise # Propagate stop request
    except Exception as e: status_queue.put(('error', f"  Unexpected error scraping Table {table_id}: {e}")); status_queue.put(('error', traceback.format_exc())); return None

    # --- Return Logic ---
    if not table_data and not stop_event.is_set():
        status_queue.put(('warning', f"  No data scraped for Table {table_id} (and not stopped). Check HTML structure or scraping logic."))
        return None # Return None if scraping yielded nothing

    status_queue.put(('debug', f"  Finished scraping Table {table_id}. Final unique row count: {len(table_data)}"))
    return {str(table_id): table_data} # Ensure table_id is string key

def scrape_segment_or_datatype_details(driver, definition_type, definition_name, status_queue, stop_event):
    """Scrapes details for Segment or DataType definitions using robust scrolling."""
    status_queue.put(('debug', f"  Scraping {definition_type} {definition_name}..."))
    parts_data = []
    # Use the unique identifier from the first column (e.g., "PV1-1", "CX-1") to track processed rows
    processed_row_identifiers = set()

    # --- SELECTORS AND COLUMN INDICES (Verify these remain consistent) ---
    # Use a locator that's likely stable for the definition table body
    table_locator = (By.XPATH, "//table[contains(@class, 'table-definition') or contains(@class, 'mat-table')]//tbody")
    row_locator = (By.TAG_NAME, "tr")
    # Assuming standard column order based on visual inspection/HL7 norms
    seq_col_index = 0      # Sequence/ID (e.g., PV1-1)
    desc_col_index = 1     # Field Description
    type_col_index = 2     # Data Type
    len_col_index = 3      # Length
    opt_col_index = 4      # Optionality (R/O/C/...)
    repeat_col_index = 5   # Repeatability (Y/N/-)
    table_col_index = 6    # Table ID
    # --- ---

    overall_length = -1
    pause_after_scroll = 0.4 # Slightly longer pause for potentially complex pages

    # --- Scrolling Logic Variables ---
    stale_content_count = 0
    max_stale_content_scrolls = 8 # Increased tolerance
    scroll_amount_partial = 600 # Pixels for partial scroll
    scroll_attempts_this_pass = 0
    max_scroll_attempts_per_pass = 2 # Try scrolling to bottom twice if partial doesn't work

    try:
        # --- Try to get overall length (before main table processing) ---
        try:
             # More specific XPath targeting the length value span
             length_element = driver.find_element(By.XPATH, "//div[contains(@class,'DefinitionPage_definitionContent__')]//span[contains(translate(., 'LENGTH:', 'length:'), 'length:')]/following-sibling::span")
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

        # Wait for the main definition table body
        WebDriverWait(driver, 10).until(EC.presence_of_element_located(table_locator))
        status_queue.put(('debug', f"    Definition table body located for {definition_name}."))

        # --- SCROLLING LOOP ---
        while stale_content_count < max_stale_content_scrolls:
            if stop_event.is_set(): raise KeyboardInterrupt("Stop requested during detail scroll scrape.")

            tbody = None
            try:
                tbody = driver.find_element(*table_locator)
                current_view_rows = tbody.find_elements(*row_locator)
            except (NoSuchElementException, StaleElementReferenceException):
                status_queue.put(('warning', f"    TBody or rows became stale/not found for {definition_name} before processing, attempting recovery scroll..."))
                driver.execute_script("window.scrollBy(0, 100);")
                time.sleep(pause_after_scroll)
                stale_content_count +=1
                continue # Skip this pass

            newly_added_count = 0 # Reset for this pass

            # Process rows currently visible
            for row in current_view_rows:
                part = {}
                row_identifier = None
                table_text = "" # Reset for each row

                try:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    # Ensure row has enough cells to cover all expected columns
                    if len(cells) > table_col_index:

                        # Get row identifier (e.g., "PV1-1")
                        try:
                             row_identifier = cells[seq_col_index].text.strip()
                        except StaleElementReferenceException: continue # Skip if ID cell is stale

                        if not row_identifier: continue # Skip rows without an identifier

                        # Check uniqueness *before* full processing
                        if row_identifier in processed_row_identifiers: continue

                        # --- Process data only if unique ---
                        processed_row_identifiers.add(row_identifier)

                        # Extract Data (with individual stale checks)
                        try: desc_text = cells[desc_col_index].text.strip()
                        except StaleElementReferenceException: desc_text = "StaleDesc"
                        try: type_text = cells[type_col_index].text.strip()
                        except StaleElementReferenceException: type_text = "StaleType"
                        try: len_text = cells[len_col_index].text.strip()
                        except StaleElementReferenceException: len_text = "*" # Treat stale length as unknown
                        try: opt_text = cells[opt_col_index].text.strip().upper()
                        except StaleElementReferenceException: opt_text = "O" # Assume optional if stale
                        try: repeat_text = cells[repeat_col_index].text.strip() # Don't uppercase yet
                        except StaleElementReferenceException: repeat_text = "-" # Assume non-repeating if stale
                        try: table_text = cells[table_col_index].text.strip()
                        except StaleElementReferenceException: table_text = "" # Assume no table if stale

                        # Build Part Dictionary
                        part['name'] = convert_to_camel_case(desc_text) if desc_text != "StaleDesc" else row_identifier.replace("-","_").lower() # Use ID if desc stale
                        part['type'] = type_text if type_text != "StaleType" else "Unknown"

                        # Parse Length
                        try: part['length'] = int(len_text) if len_text.isdigit() else -1
                        except ValueError: part['length'] = -1

                        # Mandatory Flag (R, C, B)
                        if opt_text in ['R', 'C', 'B']: part['mandatory'] = True

                        # Repeats Flag (check if NOT '-')
                        if repeat_text != '-': part['repeats'] = True

                        # Table ID (numeric or numeric.numeric)
                        if table_text:
                            if table_text.isdigit():
                                part['table'] = table_text
                            elif '.' in table_text:
                                table_parts = table_text.split('.')
                                if len(table_parts) == 2 and table_parts[0].isdigit() and table_parts[1].isdigit():
                                    part['table'] = table_text # Keep the string with decimal

                        parts_data.append(part)
                        newly_added_count += 1

                    # else: # Log rows with insufficient columns (debug level)
                    #    row_text_snippet = row.text[:50].replace('\n', ' ') if row.text else "EMPTY ROW"
                    #    status_queue.put(('debug', f"    Skipping row with insufficient columns ({len(cells)} <= {table_col_index}): '{row_text_snippet}'... in {definition_name}"))

                except StaleElementReferenceException:
                    status_queue.put(('warning', f"    Stale row encountered mid-processing in {definition_name} scrape (ID: {row_identifier}), continuing pass..."))
                    # Rollback if potentially added before error
                    if row_identifier and row_identifier in processed_row_identifiers and part not in parts_data:
                         processed_row_identifiers.remove(row_identifier)
                    continue
                except Exception as cell_err:
                    status_queue.put(('warning', f"    Error processing row/cell in {definition_name} (ID: {row_identifier}): {cell_err}"))
                    if row_identifier and row_identifier in processed_row_identifiers and part not in parts_data:
                         processed_row_identifiers.remove(row_identifier)
                    continue
            # End of row processing loop for this view

            current_parts_count = len(parts_data)
            status_queue.put(('debug', f"    {definition_type} {definition_name} scroll pass: Found {len(current_view_rows)} rows in view, added {newly_added_count} new unique parts. Total unique: {current_parts_count}"))

            # --- Stale Check and Scrolling ---
            if newly_added_count == 0:
                stale_content_count += 1
                status_queue.put(('debug', f"    No new unique parts added for {definition_name}. Stale count: {stale_content_count}/{max_stale_content_scrolls}"))
                scroll_attempts_this_pass = 0
            else:
                stale_content_count = 0
                scroll_attempts_this_pass = 0

            if stale_content_count < max_stale_content_scrolls:
                if scroll_attempts_this_pass == 0:
                    driver.execute_script(f"window.scrollBy(0, {scroll_amount_partial});")
                    status_queue.put(('debug', f"    Partial scroll ({scroll_amount_partial}px) for {definition_name}."))
                else:
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    status_queue.put(('debug', f"    Scroll to bottom attempt {scroll_attempts_this_pass} for {definition_name}."))

                time.sleep(pause_after_scroll)
                scroll_attempts_this_pass += 1
                # No need for extra logic here, stale count handles termination

    # --- Exception Handling ---
    except TimeoutException: status_queue.put(('error', f"  Timeout finding definition table body for {definition_name}.")); return None
    except NoSuchElementException: status_queue.put(('error', f"  Could not find definition table body for {definition_name} (check locator).")); return None
    except KeyboardInterrupt: raise # Propagate stop request
    except Exception as e: status_queue.put(('error', f"  Unexpected error scraping {definition_name}: {e}")); status_queue.put(('error', traceback.format_exc())); return None

    # --- Return Logic ---
    if not parts_data and not stop_event.is_set():
        status_queue.put(('warning', f"  No parts data scraped for {definition_name} (and not stopped). Check HTML or scraping logic."))
        return None

    # Add standard segment part IF it's a Segment AND if it's missing
    if definition_type == "Segments":
        first_part_is_standard = False
        if parts_data:
            first_part_name = parts_data[0].get("name", "").lower()
            # Heuristic check (similar to AI post-processing)
            if "setid" in first_part_name or first_part_name == "hl7SegmentName":
                 first_part_is_standard = True
            elif parts_data[0].get("type") == "ST" and parts_data[0].get("length") == 3:
                 first_part_is_standard = True

        if not first_part_is_standard:
            # Define the standard part (consistent with AI prompt/post-processing)
            hl7_seg_part = {"mandatory": True, "name": "hl7SegmentName", "type": "ST", "table": "0076", "length": 3}
            parts_data.insert(0, hl7_seg_part)
            status_queue.put(('debug', f"  Prepended standard part for Segment {definition_name} (Scraper)"))

    # --- Assemble final structure with Standard Separator ---
    separator_char = "." # Hardcoded as per requirement

    final_structure = {
        "separator": separator_char,
        "versions": {
            HL7_VERSION: {
                "appliesTo": "equalOrGreater",
                "totalFields": len(parts_data), # Use the final count after potential prepend
                "length": overall_length,
                "parts": parts_data
            }
        }
    }
    status_queue.put(('debug', f"  Finished scraping {definition_type} {definition_name}. Final parts count: {len(parts_data)}"))
    return {definition_name: final_structure}


# --- REVISED: process_definition_page (Scrape First, AI Fallback using HTML) ---
def process_definition_page(driver, definition_type, definition_name, status_queue, stop_event):
    """Attempts direct scraping. If fails or result is invalid, falls back to HTML source + AI."""
    url = f"{BASE_URL}/{definition_type}/{definition_name}"
    status_queue.put(('status', f"Processing {definition_type}: {definition_name} at {url}"))
    if stop_event.is_set(): return None, definition_name

    scraped_data = None
    ai_data = None
    html_save_path = None # Path for saving HTML on fallback
    final_data_source = "None"
    final_data = None
    error_occurred = False # Track if any significant error happened

    # 1. Navigate
    try:
        driver.get(url)
        # Wait for a common element like the body or a container div to ensure page starts loading
        WebDriverWait(driver, 7).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        time.sleep(0.5) # Small buffer for dynamic elements to potentially load
    except WebDriverException as nav_err:
        status_queue.put(('error', f"Navigation error to {url}: {nav_err}"))
        return None, definition_name # Cannot proceed
    except TimeoutException:
        status_queue.put(('warning', f"Timeout waiting for basic page elements on {url}, proceeding with scrape attempt."))
        # Page might be blank or very slow, scraping will likely fail but try anyway

    # 2. Attempt Direct Scraping (using the improved scraping functions)
    if not stop_event.is_set():
        try:
            status_queue.put(('status', f"  Attempting direct scraping..."))
            if definition_type == "Tables":
                scraped_data = scrape_table_details(driver, definition_name, status_queue, stop_event)
            elif definition_type in ["DataTypes", "Segments"]:
                scraped_data = scrape_segment_or_datatype_details(driver, definition_type, definition_name, status_queue, stop_event)
            else:
                status_queue.put(('warning', f"  Unsupported type for direct scraping: {definition_type}"))
                scraped_data = None # Ensure fallback if type is unknown

            # --- Validate Scraped Data Structure ---
            valid_scrape = False
            if scraped_data and isinstance(scraped_data, dict) and len(scraped_data) == 1:
                data_key = next(iter(scraped_data))
                data_value = scraped_data[data_key]
                # Use string comparison for table keys
                expected_key = str(definition_name) if definition_type == "Tables" else definition_name

                if data_key == expected_key:
                    if definition_type == "Tables" and isinstance(data_value, list):
                        # Basic check: is it a list? (Could add checks for dict items with 'value')
                         valid_scrape = True
                    elif definition_type in ["DataTypes", "Segments"] and isinstance(data_value, dict) and "versions" in data_value:
                         # Basic check: has versions dict? (Could add checks for parts list etc.)
                         valid_scrape = True

            if valid_scrape:
                status_queue.put(('status', f"  Direct scraping successful and structure seems valid."))
                final_data_source = "Scraping"
                final_data = scraped_data
            else:
                # Log reason for invalid scrape if data was returned but failed validation
                if scraped_data is not None: # Check if scrape returned *something*
                     status_queue.put(('warning', f"  Direct scraping result failed validation (Structure invalid or empty). Proceeding to AI fallback."))
                elif not stop_event.is_set(): # Log if scrape returned None and wasn't stopped
                     status_queue.put(('warning', f"  Direct scraping returned no data. Proceeding to AI fallback."))
                scraped_data = None # Ensure fallback happens if scrape was invalid/None

        except KeyboardInterrupt:
            status_queue.put(('warning', "Stop requested during scraping attempt."))
            return None, definition_name # Stop processing this item
        except Exception as scrape_err:
            status_queue.put(('warning', f"  Direct scraping failed with error: {scrape_err}. Proceeding to AI fallback."))
            status_queue.put(('debug', traceback.format_exc()))
            scraped_data = None # Ensure fallback happens
            error_occurred = True # Count scrape errors

    # 3. Fallback to HTML Source and AI Analysis (if scraping failed/invalid and not stopped)
    if final_data is None and not stop_event.is_set():
        status_queue.put(('status', f"  Falling back to HTML Source + AI Analysis..."))
        try:
            # --- Get HTML Source ---
            status_queue.put(('status', "    Getting page source..."))
            html_content = driver.page_source
            if not html_content or len(html_content) < 500: # Basic check for non-empty source
                 raise ValueError("Failed to retrieve valid page source (empty or too short).")
            status_queue.put(('status', f"    Retrieved page source ({len(html_content)} bytes)."))

            # --- Save HTML for debugging ---
            try:
                # Determine script directory safely
                script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
                html_full_dir = os.path.join(script_dir, FALLBACK_HTML_DIR)
                os.makedirs(html_full_dir, exist_ok=True) # Ensure directory exists
                html_filename = f"{definition_type}_{definition_name}_fallback.html"
                html_save_path = os.path.join(html_full_dir, html_filename)
                with open(html_save_path, 'w', encoding='utf-8') as f:
                    f.write(html_content)
                status_queue.put(('debug', f"    Saved fallback HTML to: {html_filename}"))
            except Exception as save_err:
                # Log warning but continue processing
                status_queue.put(('warning', f"    Could not save fallback HTML for {definition_name}: {save_err}"))
            # --- End Save HTML ---

            if stop_event.is_set(): raise KeyboardInterrupt("Stop requested before AI HTML analysis.")

            # --- AI Analysis of HTML - Call specific function ---
            if definition_type == "Tables":
                ai_data = analyze_table_html_with_gemini(html_content, definition_name)
            elif definition_type == "DataTypes":
                ai_data = analyze_datatype_html_with_gemini(html_content, definition_name)
            elif definition_type == "Segments":
                ai_data = analyze_segment_html_with_gemini(html_content, definition_name)
            else:
                status_queue.put(('error', f"    Unknown definition type '{definition_type}' for AI HTML fallback."))
                ai_data = None
                error_occurred = True # Count this as an error
            # --- End Specific Call ---

            # --- Validate AI Data Structure ---
            valid_ai_data = False
            if ai_data and isinstance(ai_data, dict) and len(ai_data) == 1:
                 data_key = next(iter(ai_data))
                 data_value = ai_data[data_key]
                 expected_key = str(definition_name) if definition_type == "Tables" else definition_name

                 if data_key == expected_key:
                     if definition_type == "Tables" and isinstance(data_value, list):
                         valid_ai_data = True # Basic validation passed
                     elif definition_type in ["DataTypes", "Segments"] and isinstance(data_value, dict) and "versions" in data_value:
                         valid_ai_data = True # Basic validation passed
                     else: # Failed inner structure check
                         status_queue.put(('warning', f"    AI result for '{definition_name}' failed inner structure validation."))
                 else: # Key mismatch
                      status_queue.put(('warning', f"    AI result key '{data_key}' mismatch expected '{expected_key}'."))
            elif ai_data: # Data is not None, but failed outer structure check
                 status_queue.put(('warning', f"    AI result for '{definition_name}' failed outer structure validation (not single key dict)."))

            if valid_ai_data:
                final_data_source = "AI Fallback (HTML)"
                final_data = ai_data
            else:
                 status_queue.put(('error', f"    AI HTML Analysis failed validation or returned no data for {definition_name}."))
                 error_occurred = True # Count AI failure/validation error

        except KeyboardInterrupt:
            status_queue.put(('warning', "Stop requested during AI fallback."))
            return None, definition_name # Stop processing this item
        except WebDriverException as wd_err:
            status_queue.put(('error', f"WebDriver error during AI fallback (getting source) for {url}: {wd_err}"))
            error_occurred = True
            return None, definition_name # Likely can't recover
        except ValueError as ve: # Catch specific error for bad page source
             status_queue.put(('error', f"Error during AI fallback for {url}: {ve}"))
             error_occurred = True
             # Don't return immediately, allow thread to finish reporting error
        except Exception as e:
            status_queue.put(('error', f"Error during AI HTML fallback processing for {url}: {e}"))
            status_queue.put(('error', traceback.format_exc()))
            error_occurred = True
            # Don't return immediately

    # 4. Log final source and return result
    status_queue.put(('status', f"  Finished processing {definition_name}. Source: {final_data_source}{' (Error Occurred)' if error_occurred and final_data is None else ''}"))
    time.sleep(0.05) # Shorter delay okay now
    # Return None if an error occurred *and* we ended up with no final data
    return final_data if final_data else None, definition_name


# --- Utility Functions ---
def clear_fallback_html_folder(status_queue):
    """Clears the directory used for saving fallback HTML files."""
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
        dir_path = os.path.join(script_dir, FALLBACK_HTML_DIR)
    except Exception as path_e:
         status_queue.put(('error', f"Error determining script directory for cleanup: {path_e}"))
         return

    if os.path.exists(dir_path):
        status_queue.put(('status', f"Cleaning up fallback HTML directory: {dir_path}"))
        try:
            # Add safety check: only delete if it's the expected directory name and is a directory
            if os.path.basename(dir_path) == FALLBACK_HTML_DIR and os.path.isdir(dir_path):
                shutil.rmtree(dir_path)
                # Optionally recreate the directory if needed immediately after
                # os.makedirs(dir_path)
                status_queue.put(('status', "Fallback HTML directory cleared."))
            elif not os.path.isdir(dir_path):
                 status_queue.put(('warning', f"Path exists but is not a directory: {dir_path}. Not deleting."))
            else:
                 status_queue.put(('warning', f"Safety check failed: Path name '{os.path.basename(dir_path)}' does not match expected '{FALLBACK_HTML_DIR}'. Directory NOT deleted."))
        except OSError as e:
            status_queue.put(('error', f"Error clearing fallback HTML directory {dir_path}: {e}"))
        except Exception as e:
            status_queue.put(('error', f"Unexpected error clearing fallback HTML directory: {e}"))
    else:
        status_queue.put(('status', "Fallback HTML directory does not exist, nothing to clear."))

def load_existing_definitions(output_file, status_queue):
    """Loads existing definitions from the JSON file to use as cache."""
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
        file_path = os.path.join(script_dir, output_file)
    except Exception as path_e:
         status_queue.put(('error', f"Error determining JSON file path: {path_e}"))
         return {"tables": {}, "dataTypes": {}, "HL7": {}} # Return default empty structure

    default_structure = {"tables": {}, "dataTypes": {}, "HL7": {}}
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                # Ensure top-level keys exist
                if "tables" not in data: data["tables"] = {}
                if "dataTypes" not in data: data["dataTypes"] = {} # Segments are stored here too
                if "HL7" not in data: data["HL7"] = {}
                t_count = len(data.get('tables', {}))
                dt_seg_count = len(data.get('dataTypes', {}))
                status_queue.put(('status', f"Loaded {t_count} tables and {dt_seg_count} dataTypes/segments from cache."))
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
    """Checks if a specific item exists in the loaded cache."""
    if not cache_dict: return False
    try:
        if definition_type == "Tables":
            # Table IDs are stored as string keys in the JSON
            return str(item_name) in cache_dict.get("tables", {})
        elif definition_type in ["DataTypes", "Segments"]:
            # DataTypes and Segments are stored under the "dataTypes" key
            return item_name in cache_dict.get("dataTypes", {})
        else:
            return False # Unknown type
    except Exception as e:
        print(f"Warning: Error checking cache for {definition_type} {item_name}: {e}")
        return False


# --- GUI Class HL7ParserApp ---
class HL7ParserApp:
    def __init__(self, master):
        self.master = master
        master.title("HL7 Parser (Scrape+AI Fallback/HTML)") # Updated title
        master.geometry("750x600") # Slightly wider/taller for better layout

        self.status_queue = queue.Queue()
        self.stop_event = threading.Event()
        self.worker_threads = []
        self.orchestrator_thread = None
        self.grand_total_items = 0
        self.processed_items_count = 0
        self.list_counts_received = set() # Track which category lists have been reported

        # --- Styling ---
        style = ttk.Style()
        style.theme_use('clam') # Or 'alt', 'default', 'classic'
        style.configure("TButton", padding=6, relief="flat", background="#ccc")
        style.configure("TProgressbar", thickness=20)

        # --- GUI Layout ---
        main_frame = ttk.Frame(master, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)

        # Overall Progress
        progress_frame = ttk.Frame(main_frame)
        progress_frame.pack(fill=tk.X, pady=(0, 10)) # Add padding below
        ttk.Label(progress_frame, text="Overall Progress:", font=('Segoe UI', 10)).pack(side=tk.LEFT, padx=(0, 5))
        self.pb_overall = ttk.Progressbar(progress_frame, orient="horizontal", length=400, mode="determinate")
        self.pb_overall.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)
        self.lbl_overall_perc = ttk.Label(progress_frame, text="0%", font=('Segoe UI', 10, 'bold'), width=5, anchor='e') # Ensure fixed width
        self.lbl_overall_perc.pack(side=tk.LEFT, padx=(5, 0))

        # Per-Category Progress (using Grid for alignment)
        stage_prog_frame = ttk.Frame(main_frame)
        stage_prog_frame.pack(fill=tk.X, pady=5)
        stage_prog_frame.columnconfigure(1, weight=1) # Make progress bar column expandable

        ttk.Label(stage_prog_frame, text="Tables:", font=('Segoe UI', 9)).grid(row=0, column=0, padx=5, sticky='w')
        self.pb_tables = ttk.Progressbar(stage_prog_frame, orient="horizontal", length=300, mode="determinate")
        self.pb_tables.grid(row=0, column=1, padx=5, sticky='ew')
        self.lbl_tables_count = ttk.Label(stage_prog_frame, text="0/0", font=('Segoe UI', 9), width=10, anchor='e') # Fixed width
        self.lbl_tables_count.grid(row=0, column=2, padx=5, sticky='e')

        ttk.Label(stage_prog_frame, text="DataTypes:", font=('Segoe UI', 9)).grid(row=1, column=0, padx=5, sticky='w')
        self.pb_datatypes = ttk.Progressbar(stage_prog_frame, orient="horizontal", length=300, mode="determinate")
        self.pb_datatypes.grid(row=1, column=1, padx=5, sticky='ew')
        self.lbl_datatypes_count = ttk.Label(stage_prog_frame, text="0/0", font=('Segoe UI', 9), width=10, anchor='e')
        self.lbl_datatypes_count.grid(row=1, column=2, padx=5, sticky='e')

        ttk.Label(stage_prog_frame, text="Segments:", font=('Segoe UI', 9)).grid(row=2, column=0, padx=5, sticky='w')
        self.pb_segments = ttk.Progressbar(stage_prog_frame, orient="horizontal", length=300, mode="determinate")
        self.pb_segments.grid(row=2, column=1, padx=5, sticky='ew')
        self.lbl_segments_count = ttk.Label(stage_prog_frame, text="0/0", font=('Segoe UI', 9), width=10, anchor='e')
        self.lbl_segments_count.grid(row=2, column=2, padx=5, sticky='e')

        # Log Area
        log_frame = ttk.Frame(main_frame)
        log_frame.pack(fill=tk.BOTH, expand=True, pady=5)
        ttk.Label(log_frame, text="Log:", font=('Segoe UI', 10)).pack(anchor='w')
        self.log_area = scrolledtext.ScrolledText(log_frame, height=18, wrap=tk.WORD, state='disabled', font=('Consolas', 9)) # Monospaced font
        self.log_area.pack(fill=tk.BOTH, expand=True)
        # Define log message colors
        self.log_area.tag_config('error', foreground='red', font=('Consolas', 9, 'bold'))
        self.log_area.tag_config('warning', foreground='#E69900') # Orange/Amber
        self.log_area.tag_config('debug', foreground='grey50')
        self.log_area.tag_config('success', foreground='green') # Add success color

        # Buttons
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill=tk.X, pady=(10, 0)) # Padding above
        # Add filler label to push buttons right
        ttk.Label(button_frame).pack(side=tk.LEFT, expand=True)
        self.stop_button = ttk.Button(button_frame, text="Stop", command=self.stop_processing, state=tk.DISABLED)
        self.stop_button.pack(side=tk.RIGHT, padx=(0,5))
        self.start_button = ttk.Button(button_frame, text="Start Processing", command=self.start_processing)
        self.start_button.pack(side=tk.RIGHT, padx=(0,5))

    def log_message(self, message, level="info"):
        """Appends a message to the log area with appropriate color."""
        tag = ()
        prefix = ""
        if level == "error": tag, prefix = (('error',), "ERROR: ")
        elif level == "warning": tag, prefix = (('warning',), "WARNING: ")
        elif level == "debug": tag, prefix = (('debug',), "DEBUG: ")
        elif level == "success": tag, prefix = (('success',), "SUCCESS: ")
        else: tag, prefix = ((), "") # Default/Info

        # Ensure GUI update happens on the main thread
        def update_log():
            self.log_area.config(state='normal')
            self.log_area.insert(tk.END, f"{prefix}{message}\n", tag)
            self.log_area.see(tk.END) # Scroll to the end
            self.log_area.config(state='disabled')
        # Schedule the update using master.after
        self.master.after(0, update_log)
        # Also print to console for non-GUI debugging
        print(f"{prefix}{message}")

    def update_progress(self, bar_type, current, total):
        """Updates the specified progress bar and label."""
        def update_gui():
            # Ensure total is at least 1 to avoid division by zero
            total_val = max(1, total)
            percentage = int((current / total_val) * 100) if total_val > 0 else 0

            pb, lbl = None, None
            count_text = f"{current}/{total}"

            if bar_type == "tables": pb, lbl = (self.pb_tables, self.lbl_tables_count)
            elif bar_type == "datatypes": pb, lbl = (self.pb_datatypes, self.lbl_datatypes_count)
            elif bar_type == "segments": pb, lbl = (self.pb_segments, self.lbl_segments_count)
            elif bar_type == "overall":
                 pb, lbl = (self.pb_overall, self.lbl_overall_perc)
                 count_text = f"{percentage}%" # Overall shows percentage
                 # Update overall progress
                 if pb: pb.config(maximum=total_val, value=current)
                 if lbl: lbl.config(text=count_text)
                 return # Return after handling overall

            # Update category progress bars and count labels
            if pb: pb.config(maximum=total_val, value=current)
            if lbl: lbl.config(text=count_text)

        # Schedule the update on the main thread
        self.master.after(0, update_gui)

    def check_queue(self):
        """Periodically checks the status queue for messages from threads."""
        try:
            while True: # Process all messages currently in the queue
                message = self.status_queue.get_nowait()
                msg_type = message[0]

                if msg_type == 'status': self.log_message(message[1])
                elif msg_type == 'error': self.log_message(message[1], level="error")
                elif msg_type == 'warning': self.log_message(message[1], level="warning")
                elif msg_type == 'debug': self.log_message(message[1], level="debug")
                elif msg_type == 'success': self.log_message(message[1], level="success")
                elif msg_type == 'progress': # ('progress', bar_type, current, total)
                    self.update_progress(message[1], message[2], message[3])
                elif msg_type == 'progress_add': # ('progress_add', count_to_add)
                    self.processed_items_count += message[1]
                    self.update_progress("overall", self.processed_items_count, self.grand_total_items)
                elif msg_type == 'list_found': # ('list_found', category_name, count)
                     category_name = message[1]
                     count = message[2]
                     # Only add to grand total and update category bar if not already done
                     if category_name not in self.list_counts_received:
                         self.grand_total_items += count
                         self.list_counts_received.add(category_name)
                         # Initialize category progress bar
                         self.update_progress(category_name.lower(), 0, count)
                         # Update overall total max value
                         self.update_progress("overall", self.processed_items_count, self.grand_total_items)
                         self.log_message(f"Found {count} {category_name}.")
                elif msg_type == 'finished': # ('finished', total_error_count or None)
                    error_count = message[1]
                    self.log_message("Processing finished.", level="success")
                    self.start_button.config(state=tk.NORMAL)
                    self.stop_button.config(state=tk.DISABLED)
                    # Show final message box
                    if error_count is not None and error_count > 0:
                        messagebox.showwarning("Complete with Errors", f"Finished, but with {error_count} errors recorded. Check log and potentially the '{FALLBACK_HTML_DIR}' folder.")
                    elif error_count == 0:
                        messagebox.showinfo("Complete", "Finished successfully!")
                    else: # error_count is None (likely stopped early)
                        messagebox.showinfo("Complete", "Processing finished (may have been stopped before completion).")
                    # Clear thread references
                    self.worker_threads = []
                    self.orchestrator_thread = None
                    return # Stop checking queue after finished signal

        except queue.Empty:
            pass # No messages currently

        # Check if processing is still running
        orchestrator_alive = self.orchestrator_thread and self.orchestrator_thread.is_alive()
        workers_alive = any(t.is_alive() for t in self.worker_threads)

        if workers_alive or orchestrator_alive:
            # If threads are running, schedule the next check
            self.master.after(150, self.check_queue)
        elif self.start_button['state'] == tk.DISABLED:
            # If threads finished but start button still disabled (processing just ended),
            # check queue one more time after a short delay to catch final messages
            self.master.after(500, self.check_queue)

    def start_processing(self):
        """Starts the main processing orchestrator thread."""
        # --- Pre-checks ---
        if not load_api_key(): return
        if not configure_gemini(): return

        # Check if already running
        orchestrator_alive = self.orchestrator_thread and self.orchestrator_thread.is_alive()
        if orchestrator_alive or any(t.is_alive() for t in self.worker_threads):
            messagebox.showwarning("Busy", "Processing is already in progress.")
            return

        # --- Reset State ---
        self.stop_event.clear()
        self.start_button.config(state=tk.DISABLED)
        self.stop_button.config(state=tk.NORMAL)
        self.log_message("--------------------------------------------------")
        self.log_message("Starting concurrent processing (Scrape+AI Fallback/HTML)...")
        self.log_message("Using headless browsers and caching...")

        # Reset progress tracking variables and GUI elements
        self.grand_total_items = 0
        self.processed_items_count = 0
        self.list_counts_received.clear()
        self.update_progress("tables", 0, 1); self.lbl_tables_count.config(text="0/0")
        self.update_progress("datatypes", 0, 1); self.lbl_datatypes_count.config(text="0/0")
        self.update_progress("segments", 0, 1); self.lbl_segments_count.config(text="0/0")
        self.update_progress("overall", 0, 1); self.lbl_overall_perc.config(text="0%")

        # Clear previous worker threads list
        self.worker_threads = []

        # Create the shared queue for results between workers and orchestrator
        results_queue = queue.Queue()

        # --- Start Orchestrator ---
        self.orchestrator_thread = threading.Thread(
            target=self.run_parser_orchestrator,
            args=(results_queue, self.stop_event),
            daemon=True, # Allows app to exit even if thread hangs (though join is preferred)
            name="Orchestrator"
        )
        self.orchestrator_thread.start()

        # Start polling the status queue for UI updates
        self.master.after(100, self.check_queue)

    def stop_processing(self):
        """Signals the orchestrator and worker threads to stop."""
        orchestrator_alive = hasattr(self, 'orchestrator_thread') and self.orchestrator_thread and self.orchestrator_thread.is_alive()
        workers_alive = any(t.is_alive() for t in self.worker_threads)

        if workers_alive or orchestrator_alive:
            if not self.stop_event.is_set():
                self.log_message("Stop request received. Signaling background threads...", level="warning")
                self.stop_event.set() # Signal the event
            # Disable stop button immediately to prevent multiple signals
            self.stop_button.config(state=tk.DISABLED)
            # Start button will be re-enabled by check_queue when 'finished' signal arrives
        else:
            # If no process was running, just update button states
            self.log_message("Stop requested, but no active process found.", level="info")
            self.stop_button.config(state=tk.DISABLED)
            self.start_button.config(state=tk.NORMAL)

    def run_parser_orchestrator(self, results_queue, stop_event):
        """
        Orchestrator running in a separate thread.
        Starts worker threads, collects results, merges with cache, saves final JSON.
        """
        categories = ["Tables", "DataTypes", "Segments"]
        # Load existing definitions to use as cache
        loaded_definitions = load_existing_definitions(OUTPUT_JSON_FILE, self.status_queue)

        # Dictionary to store only NEW results gathered by the worker threads
        all_new_results = {"Tables": {}, "DataTypes": {}, "Segments": {}}
        thread_errors = {"Tables": 0, "DataTypes": 0, "Segments": 0}
        threads_finished = set()
        total_error_count = 0
        self.worker_threads = [] # Ensure list is clear before starting

        try:
            self.status_queue.put(('status', "Starting worker threads..."))
            for category in categories:
                if stop_event.is_set(): break # Check stop signal before starting each thread
                # Create and start a worker thread for each category
                worker = threading.Thread(
                    target=process_category_thread, # Use the standalone function
                    args=(category, results_queue, self.status_queue, stop_event, loaded_definitions), # Pass cache
                    daemon=True, # Allows main app to exit potentially, but we join later
                    name=f"Worker-{category}"
                )
                self.worker_threads.append(worker)
                worker.start()

            if stop_event.is_set():
                raise KeyboardInterrupt("Stop requested during thread startup.")

            # --- Result Collection Loop ---
            self.status_queue.put(('status', "Waiting for results from worker threads..."))
            while len(threads_finished) < len(categories):
                workers_still_alive = any(t.is_alive() for t in self.worker_threads)
                # If stop is signaled AND all workers have finished, break the loop
                if stop_event.is_set() and not workers_still_alive:
                    self.status_queue.put(('warning', "Stopping result collection early: Stop signaled and workers finished."))
                    break
                try:
                    # Wait for results from workers or a DONE signal
                    result_type, data = results_queue.get(timeout=1.0) # Use timeout to check stop_event

                    if result_type.endswith("_DONE"):
                        category = result_type.replace("_DONE", "")
                        if category in categories:
                            threads_finished.add(category)
                            thread_errors[category] = data # Store error count reported by thread
                            total_error_count += data      # Accumulate total errors
                            self.status_queue.put(('status', f"Worker thread for {category} finished reporting {data} errors."))
                        else:
                            self.status_queue.put(('warning', f"Received unexpected DONE signal: {result_type}"))
                    elif result_type in categories:
                        # Store the NEW results received from this worker
                        all_new_results[result_type].update(data)
                        self.status_queue.put(('debug', f"Received {len(data)} new results for {result_type}."))
                    else:
                        self.status_queue.put(('warning', f"Received unknown result type from queue: {result_type}"))

                except queue.Empty:
                    # Timeout occurred, check stop signal again explicitly
                    if stop_event.is_set() and not workers_still_alive: # Double check after timeout
                         self.status_queue.put(('warning', "Stop signal detected and workers finished while waiting for results queue."))
                         break # Exit loop if stopped and workers are done
                    continue # Continue waiting if queue is empty and not stopped/workers done

            self.status_queue.put(('status', "All worker threads have reported completion or stop signal acted upon."))

        except KeyboardInterrupt: # Catches stop_event being raised as an exception
            self.status_queue.put(('warning', "Orchestrator processing aborted by user request."))
            if not stop_event.is_set(): stop_event.set() # Ensure stop is signaled if not already
        except Exception as e:
            self.status_queue.put(('error', f"Orchestrator CRITICAL ERROR: {e}"))
            self.status_queue.put(('error', traceback.format_exc()))
            total_error_count += 1 # Count the orchestrator error
            if not stop_event.is_set(): stop_event.set() # Signal stop on critical error
        finally:
            # --- Thread Joining ---
            self.status_queue.put(('status', "Ensuring all worker threads have terminated..."))
            join_timeout = 10.0 # Total time allowed for all threads to join
            start_join_time = time.time()
            for t in self.worker_threads:
                 # Calculate remaining time for this thread's join attempt
                 remaining_timeout = max(0.1, join_timeout - (time.time() - start_join_time))
                 try:
                    t.join(timeout=remaining_timeout)
                    if t.is_alive():
                         self.status_queue.put(('warning', f"Thread {t.name} did not terminate within its timeout slice."))
                 except Exception as join_err:
                      self.status_queue.put(('error', f"Error joining thread {t.name}: {join_err}"))
            self.status_queue.put(('status', "Worker thread joining complete."))

            # --- Merge Cache and New Results ---
            # Start with the data loaded from the cache
            final_definitions = loaded_definitions
            processed_segments_for_hl7 = [] # List to hold segment names for HL7 structure

            # Determine if results should be processed and saved
            # Only process fully if stop wasn't requested, OR if some new results were actually gathered before stop.
            # Prevents overwriting the file with just the cache if stopped immediately.
            should_process_results = not stop_event.is_set() or any(all_new_results.values())

            if should_process_results:
                self.status_queue.put(('status', "Merging cached and new results..."))
                # Update the 'tables' section with new table results
                # Ensure keys are strings for tables
                new_tables = {str(k): v for k, v in all_new_results.get("Tables", {}).items()}
                final_definitions.setdefault("tables", {}).update(new_tables)

                # Update the 'dataTypes' section (merging both DataTypes and Segments)
                final_definitions.setdefault("dataTypes", {}).update(all_new_results.get("DataTypes", {}))
                final_definitions["dataTypes"].update(all_new_results.get("Segments", {}))

                # Identify segments from the FINAL combined dictionary for building HL7 structure
                # A segment is assumed to have a "." separator in its definition
                processed_segments_for_hl7 = [
                    k for k, v in final_definitions["dataTypes"].items()
                    if isinstance(v, dict) and v.get('separator') == '.'
                ]
                self.status_queue.put(('debug', f"Segments identified for HL7 structure: {len(processed_segments_for_hl7)}"))


                # --- Build HL7 Structure ---
                self.status_queue.put(('status', "\n--- Building HL7 Structure ---"))
                hl7_parts = []
                # Define a common/expected order for key segments
                common_order = ["MSH", "SFT", "UAC", "PID", "PD1", "NK1", "PV1", "PV2", "DB1", "OBX", "AL1", "DG1", "DRG", "PR1", "GT1", "IN1", "IN2", "IN3", "ACC", "UB1", "UB2", "NTE"]
                # Order segments: common ones first, then others alphabetically
                ordered_segments = [s for s in common_order if s in processed_segments_for_hl7]
                other_segments = sorted([s for s in processed_segments_for_hl7 if s not in common_order])
                final_segment_order = ordered_segments + other_segments

                if not final_segment_order:
                    self.status_queue.put(('warning', "No segments found in final combined data to build HL7 structure."))
                else:
                    for seg_name in final_segment_order:
                        seg_def = final_definitions["dataTypes"].get(seg_name)
                        # Default properties for HL7 message structure parts
                        is_mand = False
                        repeats = False # Default to non-repeating unless specified
                        length = -1

                        # --- Determine properties based on segment type ---
                        if seg_name == "MSH": is_mand = True # MSH is always mandatory and non-repeating

                        # Example: Define common repeating segments (adjust as needed)
                        # This is a simplification; true repeatability depends on the message type
                        elif seg_name in ["NTE", "OBX", "NK1", "PR1", "AL1", "DG1", "IN1", "IN2", "IN3", "SFT"]:
                             repeats = True

                        # Extract length from the segment definition if available
                        if seg_def and isinstance(seg_def, dict) and 'versions' in seg_def:
                            version_key = next(iter(seg_def.get('versions', {})), None)
                            if version_key and isinstance(seg_def['versions'][version_key], dict):
                                length = seg_def['versions'][version_key].get('length', -1)

                        # Create the part dictionary for the HL7 structure
                        part = {"name": seg_name.lower(), "type": seg_name, "length": length if length is not None else -1}
                        if is_mand: part["mandatory"] = True
                        if repeats: part["repeats"] = True
                        hl7_parts.append(part)

                    # Update the final definitions with the built HL7 structure
                    final_definitions.setdefault("HL7", {}).update({
                         "separator": "\r", # Standard HL7 segment separator
                         "partId": "type", # Identify parts by segment type
                         "versions": {
                             HL7_VERSION: {
                                 "appliesTo": "equalOrGreater",
                                 "length": -1, # Overall HL7 message length isn't predefined
                                 "parts": hl7_parts
                             }
                         }
                    })
                    self.status_queue.put(('status', f"HL7 structure updated/built with {len(hl7_parts)} segments."))

            # --- Write Final JSON File ---
            if should_process_results:
                self.status_queue.put(('status', f"\nWriting final definitions to {OUTPUT_JSON_FILE}"))
                try:
                    script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
                    output_path = os.path.join(script_dir, OUTPUT_JSON_FILE)
                    with open(output_path, 'w', encoding='utf-8') as f:
                        json.dump(final_definitions, f, indent=2, ensure_ascii=False)
                    self.status_queue.put(('status', "JSON file written successfully.", "success")) # Use success level

                    # --- Run Comparison ---
                    try:
                        # Dynamically import and reload comparison module
                        import importlib
                        # Ensure the module is imported before attempting reload
                        try:
                            import hl7_comparison
                        except ImportError:
                             self.status_queue.put(('error', "Could not import hl7_comparison.py. Comparison skipped."))
                             raise # Re-raise to be caught by outer comparison try-except

                        hl7_comparison = importlib.reload(hl7_comparison)
                        self.status_queue.put(('status', "\n--- Running Comparison Against Reference ---"))

                        # Check for required attributes in the reloaded module
                        if not hasattr(hl7_comparison, 'REFERENCE_FILE'):
                            raise AttributeError("hl7_comparison.py is missing REFERENCE_FILE constant.")
                        if not hasattr(hl7_comparison, 'compare_hl7_definitions'):
                            raise AttributeError("hl7_comparison.py is missing compare_hl7_definitions function.")

                        ref_file_path = os.path.join(script_dir, hl7_comparison.REFERENCE_FILE)
                        if not os.path.exists(ref_file_path):
                             self.status_queue.put(('error', f"Reference comparison file not found: {ref_file_path}. Comparison skipped."))
                        else:
                            comparison_successful = hl7_comparison.compare_hl7_definitions(
                                generated_filepath=output_path,
                                reference_filepath=ref_file_path,
                                status_queue=self.status_queue # Pass queue for logging differences
                            )
                            if comparison_successful:
                                self.status_queue.put(('status', "--- Comparison Complete: Files match reference. ---", "success"))
                            else:
                                self.status_queue.put(('warning', "--- Comparison Complete: Differences detected. ---"))

                    except ImportError:
                         pass # Already logged above
                    except AttributeError as ae:
                        self.status_queue.put(('error', f"Error accessing component in hl7_comparison.py: {ae}. Comparison skipped."))
                        self.status_queue.put(('debug', traceback.format_exc()))
                    except Exception as comp_err:
                        self.status_queue.put(('error', f"Error during comparison: {comp_err}. Comparison skipped."))
                        self.status_queue.put(('error', traceback.format_exc()))
                    # --- End Comparison ---

                    # --- Conditional Cleanup of Fallback HTML ---
                    # Clear only if processing completed fully without errors and wasn't stopped
                    if total_error_count == 0 and not stop_event.is_set():
                        self.status_queue.put(('status', "No errors recorded and process completed, attempting fallback HTML cleanup."))
                        clear_fallback_html_folder(self.status_queue)
                    elif total_error_count > 0:
                        self.status_queue.put(('warning', f"Errors ({total_error_count}) occurred, fallback HTML files in '{FALLBACK_HTML_DIR}' were NOT deleted."))
                    elif stop_event.is_set():
                         self.status_queue.put(('warning', f"Process was stopped, fallback HTML files in '{FALLBACK_HTML_DIR}' were NOT deleted."))

                except Exception as write_err:
                    self.status_queue.put(('error', f"Failed to write final JSON file: {write_err}"))
                    total_error_count += 1
            elif stop_event.is_set():
                self.status_queue.put(('warning', f"Processing stopped early, final JSON file '{OUTPUT_JSON_FILE}' was NOT updated with potentially incomplete results."))
            else:
                 self.status_queue.put(('warning', "No new results were processed (all cached or errors), JSON file not updated."))


            # --- Signal Overall Completion to GUI ---
            # Pass error count only if results were meant to be processed
            final_error_count = total_error_count if should_process_results else None
            self.status_queue.put(('finished', final_error_count))


# --- Worker Thread Function (Standalone) ---
# <<<< NOTE: This function is OUTSIDE the HL7ParserApp class >>>>
def process_category_thread(definition_type, results_queue, status_queue, stop_event, loaded_definitions):
    """
    Worker thread function executed by each category thread.
    Initializes driver, gets list, processes items (scrape/AI), handles cache.
    """
    thread_name = threading.current_thread().name # Get assigned name
    status_queue.put(('status', f"[{thread_name}] Starting."))
    driver = None
    error_count = 0
    items_processed_in_thread = 0 # Scraped or AI analyzed
    items_skipped_cache = 0       # Found in cache
    definition_list = []
    thread_result_dict = {} # Collect NEW results specific to this thread

    try: # Outer try for the whole thread function
        status_queue.put(('status', f"[{thread_name}] Initializing WebDriver..."))
        driver = setup_driver()
        if not driver:
            raise Exception(f"WebDriver initialization failed.") # Raise exception to be caught
        if stop_event.is_set():
             status_queue.put(('warning', f"[{thread_name}] Stop requested during WebDriver init."))
             raise KeyboardInterrupt("Stop requested early.")

        # Get the list of definitions for this category
        definition_list = get_definition_list(driver, definition_type, status_queue, stop_event)
        list_count = len(definition_list)
        # Report the count found to the orchestrator/GUI
        status_queue.put(('list_found', definition_type, list_count))
        # Initialize progress bar for this category (handled by list_found message now)
        # status_queue.put(('progress', definition_type.lower(), 0, list_count))

        if stop_event.is_set():
             status_queue.put(('warning', f"[{thread_name}] Stop requested after list fetch."))
             raise KeyboardInterrupt("Stop requested after list fetch.")

        if not definition_list:
            status_queue.put(('warning', f"[{thread_name}] No items found for {definition_type}."))
        else:
            status_queue.put(('status', f"[{thread_name}] Processing/Checking {list_count} {definition_type}..."))
            for i, item_name in enumerate(definition_list):
                if stop_event.is_set():
                    status_queue.put(('warning', f"[{thread_name}] Stop requested before processing '{item_name}'."))
                    break # Exit the loop if stop is requested

                # --- Caching Check ---
                if item_exists_in_cache(definition_type, item_name, loaded_definitions):
                    status_queue.put(('debug', f"[{thread_name}] Skipping '{item_name}' - found in cache."))
                    items_skipped_cache += 1
                    # Update progress: category bar shows total touched (processed + skipped)
                    status_queue.put(('progress', definition_type.lower(), items_processed_in_thread + items_skipped_cache, list_count))
                    # Update overall progress (increments by 1 per item touched)
                    status_queue.put(('progress_add', 1))
                    continue # Move to the next item
                # --- End Caching Check ---

                # Process the page (scrape or AI fallback)
                processed_data, _ = process_definition_page(driver, definition_type, item_name, status_queue, stop_event)

                # Increment count ONLY if not skipped by cache (i.e., attempted processing)
                items_processed_in_thread += 1

                # --- Validation / Storing New Result ---
                corrected_item_data = None
                processing_successful = False
                item_had_error = False # Track errors for this specific item

                if processed_data and isinstance(processed_data, dict):
                    if len(processed_data) == 1:
                        final_key = next(iter(processed_data))
                        final_value = processed_data[final_key]
                        expected_key = str(item_name) if definition_type == "Tables" else item_name

                        if final_key == expected_key:
                            # Structure validation based on type
                            if definition_type == "Tables" and isinstance(final_value, list):
                                corrected_item_data = final_value
                                processing_successful = True
                            elif definition_type in ["DataTypes", "Segments"] and isinstance(final_value, dict) and "versions" in final_value:
                                corrected_item_data = final_value
                                processing_successful = True
                            else:
                                status_queue.put(('warning', f"[{thread_name}] Final {definition_type} '{item_name}' invalid structure (inner). Skip."))
                                item_had_error = True
                        else:
                            status_queue.put(('warning', f"[{thread_name}] Final {definition_type} key '{final_key}' != expected '{expected_key}'. Skip."))
                            item_had_error = True
                    else:
                        status_queue.put(('warning', f"[{thread_name}] Final {definition_type} '{item_name}' dict has != 1 key. Skip."))
                        item_had_error = True
                elif processed_data is None and not stop_event.is_set():
                    # Only count as error if stop wasn't requested during its processing
                    status_queue.put(('warning', f"[{thread_name}] No final data returned for '{item_name}'. Skip."))
                    item_had_error = True
                elif processed_data and not stop_event.is_set(): # Data is not None but not a dict
                    status_queue.put(('warning', f"[{thread_name}] Final data for '{item_name}' not dict type: {type(processed_data)}. Skip."))
                    item_had_error = True
                # --- End Validation ---

                if item_had_error:
                    error_count += 1 # Increment thread's error count

                if processing_successful and corrected_item_data is not None:
                    # Store the successfully processed NEW data in the thread's dictionary
                    result_key = str(item_name) if definition_type == "Tables" else item_name
                    thread_result_dict[result_key] = corrected_item_data

                # Update progress bars AFTER processing/skipping the item
                current_progress_count = items_processed_in_thread + items_skipped_cache
                status_queue.put(('progress', definition_type.lower(), current_progress_count, list_count))
                status_queue.put(('progress_add', 1)) # Increment overall progress

        # Send all NEW results collected by this thread back to the orchestrator
        results_queue.put((definition_type, thread_result_dict))
        status_queue.put(('status', f"[{thread_name}] Finished. Processed: {items_processed_in_thread}, Skipped (Cache): {items_skipped_cache}, Errors: {error_count}"))

    # --- Outer Exception Handling ---
    except KeyboardInterrupt: # Handles stop_event being raised
        status_queue.put(('warning', f"[{thread_name}] Aborted by user request."))
        # Send any partial results collected before abort
        results_queue.put((definition_type, thread_result_dict))
    except Exception as e:
        status_queue.put(('error', f"[{thread_name}] CRITICAL THREAD ERROR: {e}"))
        status_queue.put(('error', traceback.format_exc()))
        error_count += 1 # Count this critical error
        # Send any partial results before exiting
        results_queue.put((definition_type, thread_result_dict))
    finally:
        # Always signal completion to the orchestrator, sending the error count
        results_queue.put((definition_type + "_DONE", error_count))
        # Clean up WebDriver instance for this thread
        if driver:
            status_queue.put(('status', f"[{thread_name}] Cleaning up WebDriver..."))
            try:
                driver.quit()
                status_queue.put(('status', f"[{thread_name}] WebDriver closed."))
            except Exception as q_err:
                status_queue.put(('error', f"[{thread_name}] Error quitting WebDriver: {q_err}"))


# --- Run Application ---
if __name__ == "__main__":
    app = None # Ensure app is defined in the main scope
    root = tk.Tk()
    app = HL7ParserApp(root) # Creates the app instance and sets the global 'app' variable

    # Graceful shutdown handling (Ctrl+C or window close)
    def on_closing():
        print("\nClose button pressed or termination signal received.")
        if app:
            app.log_message("Shutdown requested (Window Close/Signal)...", level="warning")
            if app.stop_button['state'] == tk.NORMAL: # Check if processing might be active
                 if not app.stop_event.is_set():
                     app.stop_event.set()
                     app.log_message("Signaled running threads to stop.", level="warning")
                 # Give threads a moment to react before destroying window
                 root.after(500, root.destroy)
                 return # Don't destroy immediately
            else: # No process running
                 root.destroy()
        else:
            root.destroy()

    root.protocol("WM_DELETE_WINDOW", on_closing) # Handle window close button

    try:
        root.mainloop()
    except KeyboardInterrupt:
        print("\nCtrl+C detected in main loop. Initiating shutdown...")
        on_closing() # Trigger the same shutdown sequence
    except Exception as main_loop_err:
         print(f"Unexpected error in main loop: {main_loop_err}")
         print(traceback.format_exc())
         if root and root.winfo_exists():
              root.destroy()
    finally:
        print("Application exiting.")
        # Optional: Final check for any lingering processes if needed,
        # but daemon threads should allow exit.
        sys.exit(0) # Ensure clean exit code