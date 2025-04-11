import json
import os

# --- Constants (Adjust if your filenames differ) ---
# Assumes the reference file is in a 'comparison_files' subdirectory
# and the generated file is in the main script directory.
REFERENCE_FILE = os.path.join("comparison_files", "HL7_TEST_2.6.json")
GENERATED_FILE = "hl7_definitions_v2.6.json"
HL7_VERSION = "2.6" # Ensure this matches the version used in generation

# --- Helper Functions ---

def load_json_file(filepath, status_queue=None):
    """Loads a JSON file with error handling."""
    log_func = print # Default to print if no queue
    if status_queue:
        def queue_log(msg, level="info"): status_queue.put((level, msg))
        log_func = queue_log

    if not os.path.exists(filepath):
        log_func(f"Error: File not found: {filepath}", "error")
        return None
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        log_func(f"Successfully loaded: {filepath}", "info")
        return data
    except json.JSONDecodeError as e:
        log_func(f"Error decoding JSON from {filepath}: {e}", "error")
        return None
    except Exception as e:
        log_func(f"Error reading file {filepath}: {e}", "error")
        return None

def compare_lists_detailed(ref_list, gen_list, item_key, item_name, category, status_queue):
    """Compares lists of dictionaries based on a key, reporting diffs."""
    diffs_found = False
    log_func = lambda msg, level="info": status_queue.put((level, msg))

    ref_dict = {item.get(item_key): item for item in ref_list if item.get(item_key)}
    gen_dict = {item.get(item_key): item for item in gen_list if item.get(item_key)}

    ref_keys = set(ref_dict.keys())
    gen_keys = set(gen_dict.keys())

    missing_items = ref_keys - gen_keys
    extra_items = gen_keys - ref_keys

    if missing_items:
        diffs_found = True
        log_func(f"  MISMATCH [{category} - {item_name}]: Missing {item_key}(s): {', '.join(sorted(missing_items))}", "warning")

    if extra_items:
        diffs_found = True
        log_func(f"  MISMATCH [{category} - {item_name}]: Extra {item_key}(s): {', '.join(sorted(extra_items))}", "warning")

    # Check content of common items
    for key in ref_keys.intersection(gen_keys):
        ref_item = ref_dict[key]
        gen_item = gen_dict[key]
        if ref_item != gen_item:
            diffs_found = True
            log_func(f"  MISMATCH [{category} - {item_name}]: Content diff for {item_key} '{key}':", "warning")
            log_func(f"    Ref: {ref_item}", "debug")
            log_func(f"    Gen: {gen_item}", "debug")
            # Optional: More granular field comparison within items here if needed

    return diffs_found

def compare_field_part(ref_part, gen_part, def_name, def_type, field_name, status_queue):
    """Compares attributes of a single field/part dictionary."""
    diffs_found = False
    log_func = lambda msg, level="info": status_queue.put((level, msg))
    category = f"{def_type} - {def_name} - Field '{field_name}'"

    # Compare required attributes
    for attr in ['type', 'length']:
        ref_val = ref_part.get(attr)
        gen_val = gen_part.get(attr)
        if ref_val != gen_val:
            diffs_found = True
            log_func(f"  MISMATCH [{category}]: Attribute '{attr}' differs. Ref='{ref_val}', Gen='{gen_val}'", "warning")

    # Compare optional attributes (handle missing keys)
    for attr, default_val in [('mandatory', False), ('repeats', False), ('table', None)]:
        ref_val = ref_part.get(attr, default_val)
        gen_val = gen_part.get(attr, default_val)
        if ref_val != gen_val:
            diffs_found = True
            # Special handling for table None vs empty string
            if attr == 'table' and (ref_val is None and gen_val == "") or (ref_val == "" and gen_val is None):
                continue # Treat None and "" as equivalent for table if needed, otherwise remove this check
            log_func(f"  MISMATCH [{category}]: Attribute '{attr}' differs. Ref='{ref_val}', Gen='{gen_val}'", "warning")

    return diffs_found

def compare_definition_structure(ref_def, gen_def, def_name, def_type, status_queue):
    """Compares the structure of a DataType or Segment definition."""
    diffs_found = False
    log_func = lambda msg, level="info": status_queue.put((level, msg))
    category = f"{def_type} - {def_name}"

    # Compare separator
    ref_sep = ref_def.get('separator')
    gen_sep = gen_def.get('separator')
    if ref_sep != gen_sep:
        diffs_found = True
        log_func(f"  MISMATCH [{category}]: Separator differs. Ref='{ref_sep}', Gen='{gen_sep}'", "warning")

    # Compare versions structure (basic checks)
    ref_versions = ref_def.get('versions', {}).get(HL7_VERSION, {})
    gen_versions = gen_def.get('versions', {}).get(HL7_VERSION, {})

    if not ref_versions: log_func(f"  INFO [{category}]: Reference definition has no version '{HL7_VERSION}'. Skipping version comparison.", "debug"); return diffs_found
    if not gen_versions: log_func(f"  MISMATCH [{category}]: Generated definition missing version '{HL7_VERSION}'.", "warning"); return True

    for key in ['appliesTo', 'length']: # Compare basic version attributes
        ref_val = ref_versions.get(key)
        gen_val = gen_versions.get(key)
        if ref_val != gen_val:
            diffs_found = True
            log_func(f"  MISMATCH [{category}]: Attribute '{key}' differs. Ref='{ref_val}', Gen='{gen_val}'", "warning")

    # Compare Parts
    ref_parts = ref_versions.get('parts', [])
    gen_parts = gen_versions.get('parts', [])

    # Create dicts for easy lookup by field name
    ref_parts_dict = {}
    gen_parts_dict = {}
    ref_names = set()
    gen_names = set()

    for part in ref_parts:
        name = part.get('name')
        if name:
            if name in ref_parts_dict: log_func(f"  WARNING [{category}]: Duplicate field name '{name}' in reference parts.", "warning")
            ref_parts_dict[name] = part
            ref_names.add(name)
        else: log_func(f"  WARNING [{category}]: Reference part missing 'name'. Part: {part}", "warning"); diffs_found = True

    for part in gen_parts:
        name = part.get('name')
        if name:
            if name in gen_parts_dict: log_func(f"  WARNING [{category}]: Duplicate field name '{name}' in generated parts.", "warning")
            gen_parts_dict[name] = part
            gen_names.add(name)
        else: log_func(f"  MISMATCH [{category}]: Generated part missing 'name'. Part: {part}", "warning"); diffs_found = True

    # Find missing/extra fields
    missing_fields = ref_names - gen_names
    extra_fields = gen_names - ref_names

    if missing_fields:
        diffs_found = True
        log_func(f"  MISMATCH [{category}]: Missing Field(s): {', '.join(sorted(missing_fields))}", "warning")
    if extra_fields:
        diffs_found = True
        log_func(f"  MISMATCH [{category}]: Extra Field(s): {', '.join(sorted(extra_fields))}", "warning")

    # Compare common fields
    for field_name in ref_names.intersection(gen_names):
        if compare_field_part(ref_parts_dict[field_name], gen_parts_dict[field_name], def_name, def_type, field_name, status_queue):
            diffs_found = True

    return diffs_found

# --- Main Comparison Function ---
def compare_hl7_definitions(generated_filepath=GENERATED_FILE, reference_filepath=REFERENCE_FILE, status_queue=None):
    """Compares the generated HL7 definition file against a reference file."""

    log_func = print # Default logger
    if status_queue:
        def queue_log(msg, level="info"): status_queue.put((level, msg))
        log_func = queue_log
    else:
        # Simple wrapper if no queue
        def print_log(msg, level="info"):
             prefix = f"{level.upper()}: " if level != "info" else ""
             print(f"{prefix}{msg}")
        log_func = print_log


    log_func("\n--- Starting HL7 Definition Comparison ---", "info")

    gen_data = load_json_file(generated_filepath, status_queue)
    ref_data = load_json_file(reference_filepath, status_queue)

    if gen_data is None or ref_data is None:
        log_func("Comparison aborted due to file loading errors.", "error")
        return False

    any_differences = False

    # 1. Compare Top-Level Keys
    log_func("Comparing top-level structure...", "info")
    ref_keys = set(ref_data.keys())
    gen_keys = set(gen_data.keys())
    expected_keys = {"tables", "dataTypes", "HL7"}

    missing_top_keys = expected_keys - gen_keys
    extra_top_keys = gen_keys - expected_keys # Keys present in generated but not expected

    if missing_top_keys:
        any_differences = True
        log_func(f"MISMATCH: Generated file missing top-level key(s): {', '.join(missing_top_keys)}", "error")
    if extra_top_keys:
        any_differences = True
        log_func(f"MISMATCH: Generated file has unexpected top-level key(s): {', '.join(extra_top_keys)}", "warning")

    # Proceed only if essential keys exist in generated data
    gen_tables = gen_data.get("tables", {}) if "tables" in gen_keys else None
    gen_datatypes = gen_data.get("dataTypes", {}) if "dataTypes" in gen_keys else None
    gen_hl7 = gen_data.get("HL7", {}) if "HL7" in gen_keys else None

    ref_tables = ref_data.get("tables", {})
    ref_datatypes = ref_data.get("dataTypes", {})
    ref_hl7 = ref_data.get("HL7", {})

    # 2. Compare Tables
    log_func("\nComparing 'tables' section...", "info")
    if gen_tables is None:
        log_func("  Skipping table comparison (missing 'tables' key in generated file).", "warning")
    elif not isinstance(gen_tables, dict):
         log_func(f"  MISMATCH: Generated 'tables' is not a dictionary (type: {type(gen_tables)}).", "error"); any_differences = True
    else:
        ref_table_ids = set(ref_tables.keys())
        gen_table_ids = set(gen_tables.keys())

        missing_tables = ref_table_ids - gen_table_ids
        extra_tables = gen_table_ids - ref_table_ids

        if missing_tables:
            any_differences = True
            log_func(f"  MISMATCH: Missing Table ID(s): {', '.join(sorted(missing_tables))}", "warning")
        if extra_tables:
            # This might be acceptable if the generator finds more tables
            log_func(f"  INFO: Extra Table ID(s) found: {', '.join(sorted(extra_tables))}", "info")

        # Compare content of common tables
        for table_id in ref_table_ids.intersection(gen_table_ids):
            ref_content = ref_tables.get(table_id, [])
            gen_content = gen_tables.get(table_id, [])
            if not isinstance(ref_content, list) or not isinstance(gen_content, list):
                 log_func(f"  MISMATCH [Table {table_id}]: Content is not a list in ref or gen.", "error"); any_differences = True; continue
            # Use helper for detailed list comparison based on 'value' key
            if compare_lists_detailed(ref_content, gen_content, 'value', table_id, 'Table', status_queue):
                any_differences = True

    # 3. Compare DataTypes/Segments
    log_func("\nComparing 'dataTypes' section (includes Segments)...", "info")
    if gen_datatypes is None:
        log_func("  Skipping dataTypes comparison (missing 'dataTypes' key in generated file).", "warning")
    elif not isinstance(gen_datatypes, dict):
        log_func(f"  MISMATCH: Generated 'dataTypes' is not a dictionary (type: {type(gen_datatypes)}).", "error"); any_differences = True
    else:
        ref_dt_names = set(ref_datatypes.keys())
        gen_dt_names = set(gen_datatypes.keys())

        missing_defs = ref_dt_names - gen_dt_names
        extra_defs = gen_dt_names - ref_dt_names

        if missing_defs:
            any_differences = True
            log_func(f"  MISMATCH: Missing DataType/Segment Definition(s): {', '.join(sorted(missing_defs))}", "warning")
        if extra_defs:
            # Acceptable if generator finds more
             log_func(f"  INFO: Extra DataType/Segment Definition(s) found: {', '.join(sorted(extra_defs))}", "info")

        # Compare common definitions
        for def_name in ref_dt_names.intersection(gen_dt_names):
            ref_def_struct = ref_datatypes.get(def_name)
            gen_def_struct = gen_datatypes.get(def_name)
            def_type = "Segment" if gen_def_struct.get('separator') == '.' else "DataType" # Infer type

            if not isinstance(ref_def_struct, dict) or not isinstance(gen_def_struct, dict):
                 log_func(f"  MISMATCH [{def_type} - {def_name}]: Definition structure is not a dict in ref or gen.", "error"); any_differences = True; continue

            if compare_definition_structure(ref_def_struct, gen_def_struct, def_name, def_type, status_queue):
                any_differences = True

    # 4. Compare HL7 Segment Structure
    log_func("\nComparing 'HL7' section structure...", "info")
    if gen_hl7 is None:
        log_func("  Skipping HL7 structure comparison (missing 'HL7' key in generated file).", "warning")
    elif not isinstance(gen_hl7, dict):
         log_func(f"  MISMATCH: Generated 'HL7' is not a dictionary (type: {type(gen_hl7)}).", "error"); any_differences = True
    else:
        ref_hl7_parts = ref_hl7.get('versions', {}).get(HL7_VERSION, {}).get('parts', [])
        gen_hl7_parts = gen_hl7.get('versions', {}).get(HL7_VERSION, {}).get('parts', [])

        # Compare based on segment names present in the HL7 parts list
        ref_hl7_seg_names = set(p.get('type') for p in ref_hl7_parts if p.get('type'))
        gen_hl7_seg_names = set(p.get('type') for p in gen_hl7_parts if p.get('type'))

        missing_hl7_segs = ref_hl7_seg_names - gen_hl7_seg_names
        extra_hl7_segs = gen_hl7_seg_names - ref_hl7_seg_names

        if missing_hl7_segs:
            any_differences = True
            log_func(f"  MISMATCH [HL7 Parts]: Missing Segment(s) in list: {', '.join(sorted(missing_hl7_segs))}", "warning")
        if extra_hl7_segs:
            # Acceptable if generator processes more segments defined elsewhere
            log_func(f"  INFO [HL7 Parts]: Extra Segment(s) found in list: {', '.join(sorted(extra_hl7_segs))}", "info")
        # Note: Comparing the full details (mandatory, repeats, length) within the HL7 parts
        # would require cross-referencing with the dataTypes definitions again, which adds complexity.
        # This basic check focuses on whether the expected segments are listed.

    # --- Final Summary ---
    log_func("\n--- Comparison Summary ---", "info")
    if any_differences:
        log_func("Differences found between generated and reference files. Check warnings/errors above.", "warning")
        return False
    else:
        log_func("No significant differences found between generated and reference files.", "info")
        return True

# --- Main execution block for standalone running ---
if __name__ == "__main__":
    # Get script directory to build full paths
    script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
    gen_file_path = os.path.join(script_dir, GENERATED_FILE)
    ref_file_path = os.path.join(script_dir, REFERENCE_FILE)

    # Create dummy status queue for standalone execution logging
    class PrintQueue:
        def put(self, item):
             level = "info"
             msg = item
             if isinstance(item, tuple):
                 level = item[0]
                 msg = item[1]
             prefix = f"{level.upper()}: " if level != "info" else ""
             print(f"{prefix}{msg}")

    print_q = PrintQueue()

    compare_hl7_definitions(gen_file_path, ref_file_path, print_q)