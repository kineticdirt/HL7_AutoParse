# Necessary imports... (rest of the imports are assumed to be present)
import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import threading
import queue
import sys
import json
import os
import shutil
import time
import base64
import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager
from PIL import Image, ImageTk
import google.generativeai as genai
import google.api_core.exceptions

# --- Configuration (Unchanged) ---
BASE_URL = "https://hl7-definition.caristix.com/v2/HL7v2.6"
OUTPUT_JSON_FILE = "hl7_definitions_v2.6.json"
SCREENSHOT_DIR = "screenshots_gui"
API_KEY_FILE = "api_key.txt"
HL7_VERSION = "2.6"

# --- Global Variables (Unchanged) ---
GEMINI_API_KEY = None
GEMINI_MODEL = None

# --- Gemini API Functions (Unchanged) ---
def load_api_key():
    global GEMINI_API_KEY;
    try:
        # Assume __file__ works; handle script dir robustly if needed
        script_dir = os.path.dirname(os.path.abspath(__file__))
        key_file_path = os.path.join(script_dir, API_KEY_FILE)
        with open(key_file_path, 'r') as f: GEMINI_API_KEY = f.read().strip()
        if not GEMINI_API_KEY: messagebox.showerror("API Key Error", f"'{API_KEY_FILE}' is empty."); return False
        print("API Key loaded successfully."); return True
    except FileNotFoundError: messagebox.showerror("API Key Error", f"'{API_KEY_FILE}' not found."); return False
    except Exception as e: messagebox.showerror("API Key Error", f"Error reading API key file: {e}"); return False

def configure_gemini():
    global GEMINI_MODEL;
    if not GEMINI_API_KEY: print("Error: API Key not loaded."); return False
    try:
        genai.configure(api_key=GEMINI_API_KEY)
        GEMINI_MODEL = genai.GenerativeModel('gemini-1.5-flash')
        print("Gemini configured successfully."); return True
    except Exception as e: messagebox.showerror("Gemini Config Error", f"Failed to configure Gemini: {e}"); return False

def analyze_screenshot_with_gemini(image_path, definition_name, definition_type):
    if not GEMINI_MODEL: print("Error: Gemini model not configured."); return None
    # Use app instance created in __main__ to access stop_event
    if app and app.stop_event.is_set(): print(f"  Skip Gemini: Stop requested for {definition_name}."); return None
    print(f"  Analyzing {definition_type} '{definition_name}' with Gemini..."); max_retries=2; retry_delay=4
    for attempt in range(max_retries):
        if app and app.stop_event.is_set(): print(f"  Skip Gemini attempt {attempt+1}: Stop requested."); return None
        try:
            img = Image.open(image_path)
            # KEEP THE DETAILED PROMPT EMPHASIZING TABLE KEY REQUIREMENT
            prompt = f"""
            Analyze the provided screenshot which shows an HL7 {definition_type} definition for '{definition_name}' version {HL7_VERSION}.
            Extract the definition details from the table shown in the image.
            Generate a JSON object strictly following these rules:

            1.  Create a top-level key which is the definition name ('{definition_name}'). **For Tables, this key MUST be the numeric table ID ('{definition_name}'). Ensure this is a string like "0001".**
            2.  The value should be an object containing 'separator', 'versions' (except for Tables).
            3.  'separator' should be '.' for Segments, '' for DataTypes and Tables.
            4.  'versions' should contain a key for the HL7 version '{HL7_VERSION}'.
            5.  The version object should contain 'appliesTo': 'equalOrGreater', 'totalFields' (count of parts), 'length' (overall length if shown, else -1), and 'parts'.
            6.  'parts' should be an array of objects, one for each row in the table (for Segments/DataTypes).
            7.  Each part object must have:
                *   'name': The field description converted to camelCase (e.g., 'setIdPv1', 'financialClassCode'). Remove any prefix like 'PV1.1 - '.
                *   'type': The exact value from the 'DATA TYPE' column (e.g., 'IS', 'CWE', 'DTM').
                *   'length': The numeric value from the 'LENGTH' column. Convert '*' or empty to -1 if necessary, otherwise use the number.
            8.  Include 'mandatory': true ONLY if the 'OPTIONALITY' column is NOT 'O'. Omit the 'mandatory' key otherwise.
            9.  Include 'repeats': true ONLY if the 'REPEATABILITY' column shows the infinity symbol '∞'. Omit the 'repeats' key otherwise.
            10. Include 'table': 'TableValueString' ONLY if the 'TABLE' column has a non-empty value. The TableValueString MUST be the numeric table ID (e.g., "0004"). Omit the 'table' key otherwise.
            11. **Crucially for 'Tables' definitions only:** The **top-level key MUST be the numeric table ID as a string** ('{definition_name}') and the value **must be an array of objects**, each with 'value' and 'description' extracted from the table rows. Do NOT include 'separator' or 'versions' keys for Tables.

            Return ONLY the raw JSON object for '{definition_name}' without any surrounding text or markdown formatting.
            Example for a Segment component: {{"name": "patientClass", "type": "IS", "length": 1, "mandatory": true, "table": "0004"}}
            Example for a Table (if definition_name was '0001'): {{"0001": [{{"value": "F", "description": "Female"}}, {{"value": "M", "description": "Male"}}]}}
            Ensure the Table top-level key is a JSON string: "0001", not 0001.
            """
            response = GEMINI_MODEL.generate_content([prompt, img]); json_text = response.text.strip()
            if json_text.startswith("```json"): json_text = json_text[7:]
            if json_text.startswith("```"): json_text = json_text[3:]
            if json_text.endswith("```"): json_text = json_text[:-3]
            json_text = json_text.strip(); parsed_json = json.loads(json_text)
            print(f"  Successfully parsed Gemini response for {definition_name}."); return parsed_json
        except json.JSONDecodeError as e: print(f"Error: Bad JSON from Gemini: {e}\nReceived: ```\n{response.text}\n```"); return None
        except (google.api_core.exceptions.ResourceExhausted, google.api_core.exceptions.InternalServerError, google.api_core.exceptions.ServiceUnavailable) as e: print(f"Warn: Gemini API error attempt {attempt+1}: {e}"); time.sleep(retry_delay) if attempt < max_retries-1 else print("Error: Max Gemini retries reached."); return None
        except Exception as e: print(f"Error: Unexpected Gemini analysis error attempt {attempt+1}: {e}"); return None
    return None

# --- Selenium Functions ---
def setup_driver():
    # ... (Function unchanged) ...
    options = webdriver.ChromeOptions(); options.add_argument("--disable-gpu"); options.add_argument("--window-size=1920,1200"); options.add_argument("--log-level=3"); options.add_experimental_option('excludeSwitches', ['enable-logging'])
    try: service = Service(ChromeDriverManager().install()); driver = webdriver.Chrome(service=service, options=options); driver.implicitly_wait(5); return driver
    except WebDriverException as e: error_msg = f"Failed WebDriver init: {e}\n"; error_msg += "Close Chrome/chromedriver tasks?" if "user data directory is already in use" in str(e) else "Check Chrome install/updates/antivirus."; messagebox.showerror("WebDriver Error", error_msg); return None
    except Exception as e: messagebox.showerror("WebDriver Error", f"Unexpected WebDriver init error: {e}"); return None

# --- REVISED get_definition_list (With Added Debugging) ---
def get_definition_list(driver, definition_type, status_queue, stop_event):
    """Gets list, scrolling by last item view, validating Table IDs correctly with debugging."""
    list_url = f"{BASE_URL}/{definition_type}"
    status_queue.put(('status', f"Fetching {definition_type} list from: {list_url}"))
    if stop_event.is_set(): return []
    try: driver.get(list_url); driver.maximize_window(); time.sleep(1)
    except WebDriverException as e: status_queue.put(('error', f"Nav err: {list_url}: {e}")); return []

    definitions = []
    wait_time_initial = 30
    pause_after_scroll = 3.0
    link_pattern_xpath = f"//a[contains(@href, '/{definition_type}/') and not(contains(@href,'#'))]"

    try:
        status_queue.put(('status', f"  Waiting up to {wait_time_initial}s for initial links..."))
        wait = WebDriverWait(driver, wait_time_initial)
        try:
            wait.until(EC.presence_of_element_located((By.XPATH, link_pattern_xpath)))
            status_queue.put(('status', "  Initial links detected. Starting scroll loop..."))
        except TimeoutException:
            status_queue.put(('error', f"Timeout waiting for initial links for {definition_type}."))
            return []

        found_hrefs = set()
        stale_scroll_count = 0
        max_stale_scrolls = 5

        while stale_scroll_count < max_stale_scrolls:
            if stop_event.is_set():
                status_queue.put(('warning', f"Stop requested during {definition_type} list scroll."))
                break

            previous_href_count = len(found_hrefs)
            current_links = []
            try:
                WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, link_pattern_xpath)))
                current_links = driver.find_elements(By.XPATH, link_pattern_xpath)
            except TimeoutException:
                status_queue.put(('warning', "  Warn: No links found in current view after scroll/wait."))
            except Exception as e:
                status_queue.put(('error', f"  Error finding links during scroll: {e}"))
                break

            if not current_links:
                 status_queue.put(('status', "  No links currently visible."))
                 stale_scroll_count += 1
                 status_queue.put(('status', f"  Incrementing stale count due to no links: {stale_scroll_count}/{max_stale_scrolls}"))
            else:
                newly_added_this_pass = 0 # Renamed to avoid conflict
                for link in current_links:
                    try:
                        href = link.get_attribute('href')
                        if href and f"/{definition_type}/" in href and href not in found_hrefs:
                            name = href.split('/')[-1].strip() # Get name and strip whitespace

                            # --- STRICT VALIDATION LOGIC ---
                            is_valid_name = False # Default to False
                            validation_reason = "Unknown" # For debugging

                            if definition_type == 'Tables':
                                # Check if name looks numeric (digits, max one '.')
                                clean_name = name # Already stripped
                                if not clean_name:
                                     validation_reason = "Name is empty"
                                elif any(char.isdigit() for char in clean_name):
                                     valid_chars = set('0123456789.')
                                     if all(char in valid_chars for char in clean_name):
                                         dot_count = clean_name.count('.')
                                         if dot_count == 0 and clean_name.isdigit():
                                             is_valid_name = True
                                             validation_reason = "Purely numeric"
                                         elif dot_count == 1:
                                             parts = clean_name.split('.')
                                             if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit() and parts[0] and parts[1]: # Ensure parts are not empty
                                                 is_valid_name = True
                                                 validation_reason = "Numeric with one decimal"
                                             else:
                                                 validation_reason = "Invalid decimal format"
                                         else: # More than one dot or mixed chars
                                              validation_reason = f"Contains valid chars but invalid structure (dots={dot_count})"
                                     else:
                                          validation_reason = "Contains invalid characters"
                                else:
                                     validation_reason = "Contains no digits"
                                # <<< --- ADDED DEBUG LOGGING for Tables --- >>>
                                status_queue.put(('debug', f"Checking Table name: '{name}'. Is Valid: {is_valid_name}. Reason: {validation_reason}"))
                                # <<< --- END DEBUG LOGGING --- >>>

                            else: # For DataTypes/Segments
                                if name.isalnum():
                                    is_valid_name = True
                                    validation_reason = "Is alphanumeric"
                                else:
                                    validation_reason = "Is not alphanumeric"
                                # Optional debug for other types if needed
                                # status_queue.put(('debug', f"Checking {definition_type} name: '{name}'. Is Valid: {is_valid_name}. Reason: {validation_reason}"))
                            # --- END VALIDATION LOGIC ---


                            # Add to set ONLY if deemed valid by the logic above
                            if name and name != "#" and is_valid_name:
                                found_hrefs.add(href)
                                newly_added_this_pass += 1
                            # Log skipped names *only if they weren't empty or '#' and failed validation*
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
                    stale_scroll_count = 0

                if stale_scroll_count < max_stale_scrolls and current_links:
                    try:
                        last_element = current_links[-1]
                        last_element_text = "N/A"
                        if last_element.is_displayed(): last_element_text = last_element.text[:30]
                        status_queue.put(('status', f"  Scrolling last item ({last_element_text}...) into view..."))
                        driver.execute_script("arguments[0].scrollIntoView(true);", last_element)
                        status_queue.put(('status', f"  Pausing {pause_after_scroll}s..."))
                        time.sleep(pause_after_scroll)
                    except StaleElementReferenceException:
                         status_queue.put(('warning', "  Warn: Last element became stale before scroll could execute."))
                    except Exception as e:
                        status_queue.put(('error', f"  Error scrolling last element: {e}"))
                        stale_scroll_count += 1
                        status_queue.put(('status', f"  Incrementing stale count due to scroll error: {stale_scroll_count}/{max_stale_scrolls}"))

        status_queue.put(('status', "  Finished scroll attempts."))

        definitions.clear()
        valid_names_extracted = set() # Use a set to ensure uniqueness of names
        for href in found_hrefs:
            try:
                name = href.split('/')[-1].strip()
                if name and name != "#":
                    # Final check: Ensure the name we extract matches the validated pattern for Tables
                    is_final_valid = False
                    if definition_type == 'Tables':
                         clean_name = name
                         if any(char.isdigit() for char in clean_name):
                             valid_chars = set('0123456789.')
                             if all(char in valid_chars for char in clean_name):
                                 dot_count = clean_name.count('.')
                                 if dot_count == 0 and clean_name.isdigit(): is_final_valid = True
                                 elif dot_count == 1:
                                     parts = clean_name.split('.')
                                     if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit() and parts[0] and parts[1]: is_final_valid = True
                    else: # For other types, assume alnum check was sufficient
                        if name.isalnum(): is_final_valid = True

                    if is_final_valid:
                        valid_names_extracted.add(name)
                    else:
                         # This should ideally not happen if validation during scroll worked
                         status_queue.put(('warning', f"  Final check failed for name '{name}' from href '{href}' (Type: {definition_type}). Skipping."))

            except Exception as e:
                 status_queue.put(('warning', f"Warn: Error extracting name from final href '{href}': {e}"))

        definitions = sorted(list(valid_names_extracted)) # Convert final set to sorted list

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
        import traceback
        status_queue.put(('error', traceback.format_exc()))

    status_queue.put(('status', f"Final count: Found {len(definitions)} unique valid {definition_type}."))
    return definitions
# --- END REVISED get_definition_list ---


# --- process_definition_page (Unchanged, but ensure scrolling is sufficient) ---
def process_definition_page(driver, definition_type, definition_name, status_queue, stop_event):
    """Navigates, scrolls detail page, takes screenshot, calls Gemini analysis."""
    url = f"{BASE_URL}/{definition_type}/{definition_name}"
    status_queue.put(('status', f"Processing {definition_type}: {definition_name}"))
    if stop_event.is_set(): return None, definition_name
    try:
        driver.get(url)
    except WebDriverException as nav_err:
         status_queue.put(('error', f"Error navigating to {url}: {nav_err}"))
         return None, definition_name

    screenshot_path = None
    parsed_json = None
    wait_time_content = 25 # Time to find content area initially
    pause_after_detail_scroll = 1.5 # Pause during detail page scroll
    scroll_amount_detail = 700 # Pixels for scrollBy

    try:
        if stop_event.is_set(): return None, definition_name

        # --- Find main content area first (locator unchanged) ---
        content_locator = ( By.XPATH, "//table[contains(@class, 'table-definition') and contains(@class, 'table')] | //div[contains(@class, 'table-responsive')]//table | //div[contains(@class, 'DefinitionPage_definitionContent')] | //div[@id='MainContent_pnlContent'] | //div[@role='main'] | //main | //body")
        status_queue.put(('status', f"  Waiting up to {wait_time_content}s for content area..."))
        wait = WebDriverWait(driver, wait_time_content)
        content_element = wait.until(EC.visibility_of_element_located(content_locator))
        status_queue.put(('status', f"  Content area located."))

        # <<< START: Scroll detail page fully before screenshot >>>
        status_queue.put(('status', "  Scrolling detail page to ensure full view..."))
        last_height = driver.execute_script("return document.body.scrollHeight")
        stale_height_count = 0
        max_stale_detail_scrolls = 3 # Stop after 3 scrolls don't increase height
        scroll_attempts = 0
        max_scroll_attempts = 20 # Safety break for infinite scroll edge cases

        while stale_height_count < max_stale_detail_scrolls and scroll_attempts < max_scroll_attempts:
            if stop_event.is_set(): raise KeyboardInterrupt("Stop requested during detail scroll.")
            # Use scrollBy for smoother scrolling simulation
            driver.execute_script(f"window.scrollBy(0, {scroll_amount_detail});")
            time.sleep(pause_after_detail_scroll) # Wait for potential lazy loading
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                stale_height_count += 1
            else:
                stale_height_count = 0
            last_height = new_height
            scroll_attempts += 1
        if scroll_attempts >= max_scroll_attempts:
             status_queue.put(('warning', f"  Max scroll attempts ({max_scroll_attempts}) reached for detail page {definition_name}."))
        status_queue.put(('status', "  Detail page scroll complete."))
        driver.execute_script("window.scrollTo(0, 0);") # Scroll back to top
        time.sleep(0.75) # Short pause after scrolling top
         # <<< END: Scrolling detail page >>>

        if stop_event.is_set(): return None, definition_name # Check before screenshot

        script_dir = os.path.dirname(os.path.abspath(__file__))
        screenshot_full_dir = os.path.join(script_dir, SCREENSHOT_DIR)
        if not os.path.exists(screenshot_full_dir): os.makedirs(screenshot_full_dir)
        screenshot_filename = f"{definition_type}_{definition_name}.png"
        screenshot_path = os.path.join(screenshot_full_dir, screenshot_filename)

        status_queue.put(('status', "  Attempting full page screenshot..."))
        # Use save_screenshot for full page reliably
        # For very long pages, might need viewport stitching if this fails, but usually works
        screenshot_success = driver.save_screenshot(screenshot_path)

        if screenshot_success:
            status_queue.put(('status', f"  Screenshot saved: {screenshot_filename}"))
            # Call Gemini only if screenshot succeeded
            parsed_json = analyze_screenshot_with_gemini(screenshot_path, definition_name, definition_type)
        else:
            status_queue.put(('error', f"Error: Failed to save screenshot for {definition_name}"))

    except TimeoutException: status_queue.put(('error', f"Timeout ({wait_time_content}s) waiting for content area: {url}"))
    except NoSuchElementException: status_queue.put(('error', f"No content area element found: {url}."))
    except KeyboardInterrupt: status_queue.put(('warning', "Stop requested during detail page processing.")); return None, definition_name
    except WebDriverException as wd_err: status_queue.put(('error', f"WebDriver error on page {url}: {wd_err}"))
    except Exception as e: status_queue.put(('error', f"Error processing page {url}: {e}")); import traceback; status_queue.put(('error', traceback.format_exc()))

    # Save page source if error occurred (and not just a stop request)
    # Check if parsed_json is None AND stop_event is NOT set
    if parsed_json is None and not stop_event.is_set() and screenshot_path is not None:
         # Ensure the directory exists before trying to save the HTML file
         screenshot_dir = os.path.dirname(screenshot_path)
         if os.path.exists(screenshot_dir):
             try:
                  page_source_path = screenshot_path.replace('.png', '_error.html')
                  with open(page_source_path, "w", encoding="utf-8") as f_debug:
                      f_debug.write(driver.page_source)
                  status_queue.put(('warning', f"Saved error page source: {os.path.basename(page_source_path)}"))
             except Exception as dump_err:
                  status_queue.put(('error', f"Failed to dump page source for {definition_name}: {dump_err}"))
         else:
             status_queue.put(('warning', f"Screenshot directory {screenshot_dir} doesn't exist, cannot save error HTML."))


    time.sleep(0.2) # Small delay before next navigation
    return parsed_json, definition_name
# --- END process_definition_page ---


# --- Cleanup Function (Unchanged) ---
def clear_screenshot_folder(status_queue):
    """Deletes the contents of the screenshot directory."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    dir_path = os.path.join(script_dir, SCREENSHOT_DIR)
    if os.path.exists(dir_path):
        status_queue.put(('status', f"Cleaning up screenshot directory: {dir_path}"))
        try:
            # Ensure it's actually a directory before removing
            if os.path.isdir(dir_path):
                 shutil.rmtree(dir_path) # Remove directory and all contents
                 os.makedirs(dir_path) # Recreate empty directory
                 status_queue.put(('status', "Screenshot directory cleared and recreated."))
            else:
                status_queue.put(('error', f"Path exists but is not a directory: {dir_path}"))

        except OSError as e:
            status_queue.put(('error', f"Error clearing screenshot directory {dir_path}: {e}"))
        except Exception as e:
            status_queue.put(('error', f"Unexpected error clearing screenshot directory: {e}"))

    else:
        status_queue.put(('status', "Screenshot directory does not exist, nothing to clear."))


# --- GUI Application Class (Skeleton - Assuming Unchanged) ---
class HL7ParserApp:
    def __init__(self, master): # Abridged - Assume full implementation exists
        self.master = master; master.title("HL7 Parser"); master.geometry("700x550")
        self.status_queue = queue.Queue(); self.stop_event = threading.Event(); self.processing_thread = None
        style = ttk.Style(); style.theme_use('clam')
        # ... rest of GUI setup (Frames, Progressbars, Labels, Log Area, Button) ...
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
        ttk.Label(log_frame, text="Log:").pack(anchor='w'); self.log_area = scrolledtext.ScrolledText(log_frame, height=15, wrap=tk.WORD, state='disabled'); self.log_area.pack(fill=tk.BOTH, expand=True); self.log_area.tag_config('error', foreground='red'); self.log_area.tag_config('warning', foreground='orange'); self.log_area.tag_config('debug', foreground='gray') # Added debug tag
        self.start_button = ttk.Button(button_frame, text="Start Processing", command=self.start_processing); self.start_button.pack(side=tk.RIGHT, padx=5)
        # Add stop button
        self.stop_button = ttk.Button(button_frame, text="Stop", command=self.stop_processing, state=tk.DISABLED); self.stop_button.pack(side=tk.RIGHT, padx=5)

    def log_message(self, message, level="info"):
        tag=(); prefix="";
        if level == "error": tag,prefix = (('error',),"ERROR: ")
        elif level == "warning": tag,prefix = (('warning',),"WARNING: ")
        elif level == "debug": tag, prefix = (('debug',), "DEBUG: ") # Handle debug level
        else: tag, prefix = ((), "")

        # Ensure GUI updates happen on the main thread
        def update_log():
            self.log_area.config(state='normal')
            self.log_area.insert(tk.END, f"{prefix}{message}\n", tag)
            self.log_area.see(tk.END)
            self.log_area.config(state='disabled')
        self.master.after(0, update_log) # Schedule update on main thread

    def update_progress(self, bar_type, current, total):
        # Ensure GUI updates happen on the main thread
        def update_gui():
            total_val=max(1,total); percentage=int((current/total_val)*100)
            pb,lbl=None,None; count_text=f"{current}/{total}"
            if bar_type=="tables": pb,lbl=(self.pb_tables,self.lbl_tables_count)
            elif bar_type=="datatypes": pb,lbl=(self.pb_datatypes,self.lbl_datatypes_count)
            elif bar_type=="segments": pb,lbl=(self.pb_segments,self.lbl_segments_count)
            elif bar_type=="overall": pb,lbl,count_text=(self.pb_overall,self.lbl_overall_perc,f"{percentage}%")

            if pb: pb.config(maximum=total_val, value=current)
            if lbl: lbl.config(text=count_text)
            # self.master.update_idletasks() # Not strictly needed when using .after()
        self.master.after(0, update_gui) # Schedule update on main thread


    def check_queue(self):
        try:
            while True:
                message=self.status_queue.get_nowait()
                msg_type=message[0]
                if msg_type=='status': self.log_message(message[1])
                elif msg_type=='error': self.log_message(message[1], level="error")
                elif msg_type=='warning': self.log_message(message[1], level="warning")
                elif msg_type=='debug': self.log_message(message[1], level="debug") # Handle debug
                elif msg_type=='progress': self.update_progress(message[1], message[2], message[3])
                elif msg_type=='total_items':
                    self.update_progress("overall", 0, message[1]) # Set max for overall
                    self.log_message(f"Total items to process: {message[1]}")
                elif msg_type=='finished':
                    error_count = message[1]
                    self.log_message("Processing finished.")
                    self.start_button.config(state=tk.NORMAL)
                    self.stop_button.config(state=tk.DISABLED) # Disable stop button
                    if error_count is not None and error_count > 0:
                         messagebox.showwarning("Complete with Errors", f"Finished, but with {error_count} errors recorded. Check log and screenshots.")
                    elif error_count == 0:
                         messagebox.showinfo("Complete", "Finished successfully!")
                    else: # None or other cases (like aborted)
                         messagebox.showinfo("Complete", "Processing finished (may have been aborted or no items found).")
                    self.processing_thread = None # Clear thread reference
                    return # Stop checking queue once finished
        except queue.Empty:
            pass # No messages currently

        # Reschedule check if thread is still running
        if self.processing_thread and self.processing_thread.is_alive():
            self.master.after(150, self.check_queue)
        elif self.processing_thread and not self.processing_thread.is_alive():
            # Handle unexpected thread termination if needed
            if not self.stop_event.is_set(): # Check if it wasn't stopped intentionally
                self.log_message("Background thread stopped unexpectedly.", level="error")
                self.start_button.config(state=tk.NORMAL)
                self.stop_button.config(state=tk.DISABLED)
                messagebox.showerror("Error", "Processing thread stopped unexpectedly.")
            self.processing_thread = None # Clear thread reference


    def start_processing(self):
        if not load_api_key(): return;
        if not configure_gemini(): return;

        # Prevent starting if already running
        if self.processing_thread and self.processing_thread.is_alive():
             messagebox.showwarning("Busy", "Processing is already in progress.")
             return

        self.stop_event.clear()
        self.start_button.config(state=tk.DISABLED)
        self.stop_button.config(state=tk.NORMAL) # Enable stop button
        self.log_message("Starting processing...")
        # Clear log area? Optional.
        # self.log_area.config(state='normal'); self.log_area.delete('1.0', tk.END); self.log_area.config(state='disabled')

        # Reset progress bars
        self.update_progress("tables",0,1); self.lbl_tables_count.config(text="0/0")
        self.update_progress("datatypes",0,1); self.lbl_datatypes_count.config(text="0/0")
        self.update_progress("segments",0,1); self.lbl_segments_count.config(text="0/0")
        self.update_progress("overall",0,1); self.lbl_overall_perc.config(text="0%")

        self.processing_thread = threading.Thread(target=self.run_parser_thread, args=(self.stop_event,), daemon=True)
        self.processing_thread.start()
        self.master.after(100, self.check_queue) # Start checking queue

    def stop_processing(self):
        if self.processing_thread and self.processing_thread.is_alive():
            self.log_message("Stop request received. Signaling background thread...", level="warning")
            self.stop_event.set()
            self.stop_button.config(state=tk.DISABLED) # Disable stop button after clicking
            # Button state will be reset by check_queue when thread finishes
        else:
             self.log_message("Stop requested, but no active process found.", level="info")


    # --- run_parser_thread (No changes needed here based on the fix in get_definition_list) ---
    # Keep the existing logic for Table JSON correction as Gemini might still occasionally get the key wrong.
    def run_parser_thread(self, stop_event):
        driver = None; error_count = 0
        all_definitions = {"tables": {}, "dataTypes": {}} # Initialize top-level keys
        processed_segments_for_hl7 = []
        grand_total_items, processed_items_count = 0, 0

        try:
            self.status_queue.put(('status', "Initializing WebDriver...")); driver = setup_driver()
            if not driver: raise Exception("WebDriver initialization failed.")
            if stop_event.is_set(): raise KeyboardInterrupt("Stop requested before list fetching.")

            # --- Clear screenshots from previous runs ---
            clear_screenshot_folder(self.status_queue)
            if stop_event.is_set(): raise KeyboardInterrupt("Stop requested after cleanup.")

            # --- Fetch Lists ---
            table_ids = get_definition_list(driver, "Tables", self.status_queue, stop_event);
            if stop_event.is_set(): raise KeyboardInterrupt("Stop requested after fetching Tables.")
            datatype_names = get_definition_list(driver, "DataTypes", self.status_queue, stop_event);
            if stop_event.is_set(): raise KeyboardInterrupt("Stop requested after fetching DataTypes.")
            segment_names = get_definition_list(driver, "Segments", self.status_queue, stop_event);
            if stop_event.is_set(): raise KeyboardInterrupt("Stop requested after fetching Segments.")

            grand_total_items = len(table_ids) + len(datatype_names) + len(segment_names)
            if grand_total_items == 0 and not stop_event.is_set():
                 self.status_queue.put(('error', "No definition items found for Tables, DataTypes, or Segments."))
                 # Don't raise exception here, let it finish gracefully with 0 items
            elif grand_total_items > 0:
                 self.status_queue.put(('total_items', grand_total_items))
            # If stop_event is set, grand_total_items might be 0 or partial, proceed to finally block

            # --- Process Tables ---
            if not stop_event.is_set() and table_ids:
                self.status_queue.put(('status', "\n--- Processing Tables ---")); self.status_queue.put(('progress', 'tables', 0, len(table_ids)))
                for i, table_id in enumerate(table_ids):
                    if stop_event.is_set(): break
                    parsed_data, name_processed = process_definition_page(driver, "Tables", table_id, self.status_queue, stop_event)

                    corrected_data = None
                    if parsed_data and isinstance(parsed_data, dict):
                        if len(parsed_data) == 1:
                            ai_key = next(iter(parsed_data))
                            ai_value = parsed_data[ai_key]
                            if isinstance(ai_value, list):
                                if all(isinstance(item, dict) and 'value' in item for item in ai_value):
                                    # Gemini should return the correct numeric ID string now, check against the processed table_id
                                    if ai_key == str(table_id): # Ensure comparison is string vs string
                                        corrected_data = ai_value
                                    else:
                                        self.status_queue.put(('warning', f"AI used key '{ai_key}' for table '{table_id}'. Using AI value anyway."))
                                        # Trust the structure Gemini returned if the key is wrong but value looks ok
                                        corrected_data = ai_value
                                else:
                                    self.status_queue.put(('warning', f"AI table '{table_id}' list items invalid format. Skipping. Data: {ai_value}")); error_count += 1
                            else: self.status_queue.put(('warning', f"AI table '{table_id}' value not a list. Skipping. Data: {parsed_data}")); error_count += 1
                        else: self.status_queue.put(('warning', f"AI table '{table_id}' dict has {len(parsed_data)} keys (expected 1). Skipping. Data: {parsed_data}")); error_count += 1
                    elif parsed_data is None and not stop_event.is_set():
                        self.status_queue.put(('warning', f"No valid JSON data returned for table '{table_id}'.")); error_count += 1
                    elif parsed_data and not stop_event.is_set(): # Parsed data exists but isn't dict or None
                        self.status_queue.put(('warning', f"AI table '{table_id}' returned unexpected type {type(parsed_data)}. Skipping. Data: {parsed_data}")); error_count += 1

                    if corrected_data is not None:
                         all_definitions["tables"][str(table_id)] = corrected_data # Ensure key is string

                    processed_items_count += 1; self.status_queue.put(('progress', 'tables', i + 1, len(table_ids))); self.status_queue.put(('progress', 'overall', processed_items_count, grand_total_items))
                if stop_event.is_set(): raise KeyboardInterrupt("Stop requested during Tables processing.")

            # --- Process DataTypes ---
            if not stop_event.is_set() and datatype_names:
                self.status_queue.put(('status', "\n--- Processing DataTypes ---")); self.status_queue.put(('progress', 'datatypes', 0, len(datatype_names)))
                for i, name in enumerate(datatype_names):
                     if stop_event.is_set(): break
                     parsed_data, _ = process_definition_page(driver, "DataTypes", name, self.status_queue, stop_event)
                     corrected_def = None
                     if parsed_data and isinstance(parsed_data, dict):
                        if len(parsed_data) == 1:
                            ai_key = next(iter(parsed_data)); ai_value = parsed_data[ai_key]
                            if isinstance(ai_value, dict) and 'versions' in ai_value:
                                 corrected_def = ai_value
                                 if ai_key != name: self.status_queue.put(('warning', f"AI used key '{ai_key}' for datatype '{name}'. Correcting."))
                            else: self.status_queue.put(('warning', f"AI dtype '{name}' value invalid format. Skipping. Data: {parsed_data}")); error_count += 1
                        else: self.status_queue.put(('warning', f"AI dtype '{name}' dict has {len(parsed_data)} keys (expected 1). Skipping. Data: {parsed_data}")); error_count += 1
                     elif parsed_data is None and not stop_event.is_set():
                          self.status_queue.put(('warning', f"No valid JSON data returned for datatype '{name}'.")); error_count += 1
                     elif parsed_data and not stop_event.is_set():
                          self.status_queue.put(('warning', f"AI dtype '{name}' returned unexpected type {type(parsed_data)}. Skipping. Data: {parsed_data}")); error_count += 1

                     if corrected_def is not None: all_definitions["dataTypes"][name] = corrected_def

                     processed_items_count += 1; self.status_queue.put(('progress', 'datatypes', i + 1, len(datatype_names))); self.status_queue.put(('progress', 'overall', processed_items_count, grand_total_items))
                if stop_event.is_set(): raise KeyboardInterrupt("Stop requested during DataTypes processing.")

            # --- Process Segments ---
            if not stop_event.is_set() and segment_names:
                self.status_queue.put(('status', "\n--- Processing Segments ---")); self.status_queue.put(('progress', 'segments', 0, len(segment_names)))
                hl7_seg_part = {"mandatory": True, "name": "hl7SegmentName", "type": "ST", "table": "0076", "length": 3} # Definition of the standard first part
                for i, name in enumerate(segment_names):
                    if stop_event.is_set(): break
                    parsed_data, _ = process_definition_page(driver, "Segments", name, self.status_queue, stop_event)
                    corrected_def = None
                    if parsed_data and isinstance(parsed_data, dict):
                        if len(parsed_data) == 1:
                            ai_key = next(iter(parsed_data)); segment_def = parsed_data[ai_key]
                            if isinstance(segment_def, dict) and 'versions' in segment_def:
                                corrected_def = segment_def
                                if ai_key != name: self.status_queue.put(('warning', f"AI used key '{ai_key}' for segment '{name}'. Correcting."))
                                # Add standard first part if missing
                                try:
                                    version_key = next(iter(corrected_def.get("versions", {})))
                                    if version_key: # Ensure there is a version key
                                        parts_list = corrected_def["versions"][version_key].setdefault("parts", [])
                                        if not parts_list or parts_list[0].get("name") != "hl7SegmentName":
                                            parts_list.insert(0, hl7_seg_part.copy())
                                            self.status_queue.put(('status',f"  Standard part prepended for {name}."))
                                            # Update totalFields count
                                            corrected_def["versions"][version_key]["totalFields"] = len(parts_list)
                                        else:
                                             self.status_queue.put(('debug',f"  Std part already present for {name}."))
                                    else:
                                         self.status_queue.put(('warning', f"Segment {name} definition missing version key. Cannot check std part."))
                                except Exception as e: self.status_queue.put(('warning', f" Error checking/adding std part for {name}: {e}"))
                            else: self.status_queue.put(('warning', f"AI segment '{name}' value invalid format. Skipping. Data: {parsed_data}")); error_count += 1
                        else: self.status_queue.put(('warning', f"AI segment '{name}' dict has {len(parsed_data)} keys (expected 1). Skipping. Data: {parsed_data}")); error_count += 1
                    elif parsed_data is None and not stop_event.is_set():
                         self.status_queue.put(('warning', f"No valid JSON data returned for segment '{name}'.")); error_count += 1
                    elif parsed_data and not stop_event.is_set():
                         self.status_queue.put(('warning', f"AI segment '{name}' returned unexpected type {type(parsed_data)}. Skipping. Data: {parsed_data}")); error_count += 1

                    if corrected_def is not None:
                        all_definitions["dataTypes"][name] = corrected_def # Store Segment definition under dataTypes key
                        processed_segments_for_hl7.append(name)

                    processed_items_count+=1; self.status_queue.put(('progress','segments',i+1,len(segment_names))); self.status_queue.put(('progress','overall',processed_items_count,grand_total_items))
                if stop_event.is_set(): raise KeyboardInterrupt("Stop requested during Segments processing.")

            # --- Build HL7 Structure ---
            if not stop_event.is_set():
                self.status_queue.put(('status', "\n--- Building HL7 Structure ---"))
                hl7_parts=[]; common_order=["MSH","PID","PV1","OBR","OBX"]; # Define common order
                ordered=[s for s in common_order if s in processed_segments_for_hl7] # Segments in common order
                other=sorted([s for s in processed_segments_for_hl7 if s not in common_order]) # Other segments sorted
                # Combine lists
                final_segment_order = ordered + other
                if not final_segment_order:
                     self.status_queue.put(('warning', "No segments were successfully processed to build HL7 structure."))
                else:
                    for seg_name in final_segment_order:
                        is_mand=seg_name in ["MSH"] # Only MSH is mandatory here
                        repeats=seg_name not in ["MSH"] # Assume all others can repeat in a generic structure
                        part={"name":seg_name.lower(),"type":seg_name,"length":-1}
                        if is_mand: part.update({"mandatory":True})
                        if repeats: part.update({"repeats":True})
                        hl7_parts.append(part)

                    all_definitions["HL7"]={
                        "separator":"\r",
                        "partId":"type",
                        "versions":{
                            HL7_VERSION:{
                                "appliesTo":"equalOrGreater",
                                "length":-1,
                                "parts":hl7_parts
                            }
                        }
                    }
                    self.status_queue.put(('status', f"HL7 structure built with {len(hl7_parts)} segments."))

            # --- Write Final JSON ---
            if not stop_event.is_set():
                self.status_queue.put(('status', f"\nWriting final definitions to {OUTPUT_JSON_FILE}"))
                script_dir=os.path.dirname(os.path.abspath(__file__)); output_path=os.path.join(script_dir,OUTPUT_JSON_FILE)
                try:
                    with open(output_path,'w',encoding='utf-8') as f: json.dump(all_definitions,f,indent=2,ensure_ascii=False)
                    self.status_queue.put(('status', "JSON file written successfully."))
                    # --- Conditional Cleanup Screenshots ---
                    if error_count == 0:
                        self.status_queue.put(('status', "No errors recorded, attempting screenshot cleanup."))
                        # Re-run cleanup, just in case it failed initially but processing succeeded
                        clear_screenshot_folder(self.status_queue)
                    else:
                        self.status_queue.put(('warning', f"Errors ({error_count}) occurred, screenshots in '{SCREENSHOT_DIR}' were NOT deleted for review."))

                except IOError as e: self.status_queue.put(('error', f"Failed to write JSON file: {e}")); error_count+=1
                except Exception as e: self.status_queue.put(('error', f"Unexpected error writing JSON file: {e}")); error_count+=1; import traceback; self.status_queue.put(('error', traceback.format_exc()))

            self.status_queue.put(('finished', error_count)) # Signal completion

        except KeyboardInterrupt:
            self.status_queue.put(('warning', "\nProcessing aborted by user request (Ctrl+C or Stop button)."))
            self.status_queue.put(('finished', error_count)) # Signal aborted finish
        except WebDriverException as e:
             self.status_queue.put(('error', f"A WebDriver error occurred: {e}"))
             import traceback; self.status_queue.put(('error', traceback.format_exc()))
             error_count += 1 # Count as error
             self.status_queue.put(('finished', error_count))
        except Exception as e:
            self.status_queue.put(('error', f"An critical error occurred: {e}"))
            import traceback; self.status_queue.put(('error', traceback.format_exc()))
            error_count += 1 # Count as error
            self.status_queue.put(('finished', error_count)) # Signal finish with critical error
        finally:
            if driver:
                 try:
                      self.status_queue.put(('status', "Cleaning up WebDriver..."))
                      driver.quit()
                      self.status_queue.put(('status', "WebDriver closed."))
                 except Exception as q_err:
                      self.status_queue.put(('error', f"Error quitting WebDriver: {q_err}"))


# --- Run Application ---
if __name__ == "__main__":
    # Make app instance globally accessible for Gemini stop check
    app = None
    root = tk.Tk()
    app = HL7ParserApp(root) # Create and assign the global app instance
    try:
        root.mainloop()
    except KeyboardInterrupt:
        print("\nCtrl+C detected in main loop. Signaling stop...")
        if app:
            app.log_message("Shutdown requested (Ctrl+C)...", level="warning")
            app.stop_event.set()
            if app.processing_thread and app.processing_thread.is_alive():
                print("Waiting for background thread to finish...")
                app.processing_thread.join(timeout=7.0) # Wait for thread
                if app.processing_thread.is_alive():
                     print("Warning: Background thread did not exit cleanly after timeout.")
            else:
                 print("Background thread was not running or already finished.")
        print("Exiting application.")
        # Try to destroy root window gracefully
        try:
            root.destroy()
        except tk.TclError:
             pass # Window might already be destroyed
        sys.exit(0) # Ensure exit