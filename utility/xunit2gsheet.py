"""
Sheet Structure:
  The script maintains four worksheets in the Google Sheet:
    1. Properties - Metadata about test runs (build, version, etc.)
    2. Summary - Aggregated test suite results (pass/fail counts)
    3. Test Case Status - Individual test case statuses
    4. Test Case Timings - Individual test case execution times

Features:
  - Automatic column width adjustment based on content
  - Automatic component detection from suite names
  - Color-coded status columns (Passed/Failed/Error/Skipped)
  - Version-aware sorting of test results (newest first)
  - Smart merging of new results with existing data
  - Automatic spreadsheet renaming with build number

Note:
  - The script preserves existing data while adding new version columns
  - Test cases are matched by name and polarion-testcase-id when available
  - First run creates the worksheets if they don't exist

Service Account Setup:
  1. Create service account in Google Cloud Console
  2. Download JSON credentials file as 'service_account.json'
  3. Share your Google Sheet with the service account email
  4. Place JSON file in the same directory as this script

Reference:
  Google Sheets API documentation:
  https://developers.google.com/sheets/api/guides/concepts
"""

import os
import re
import sys
import xml.etree.ElementTree as ET
from collections import OrderedDict

import gspread
import gspread_formatting as gsf
import pandas as pd
from docopt import docopt
from google.oauth2.service_account import Credentials
from gspread_dataframe import get_as_dataframe, set_with_dataframe

# --- Constants ---
SHEET_PROPERTIES = "Properties"
SHEET_SUMMARY = "Summary"
SHEET_TEST_STATUS = "Test Case Status"
SHEET_TEST_TIMINGS = "Test Case Timings"

PREFERRED_PROPERTIES = [
    "polarion-project-id",
    "polarion-testrun-id",
    "polarion-group-id",
    "build",
    "ceph-version",
    "suite-name",
    "distro",
    "container-tag",
    "run-id",
    "cloud-type",
    "invoked-by",
    "ceph-ansible-version",
    "conf-file",
    "container-registry",
    "container-image",
    "instance-name",
]

DOC = """
    xunit2gsheet.py - Upload xUnit test results to Google Sheets

    Usage:
        xunit2gsheet.py --gsheet <gsheet_id> --xml <xml_file_path> \
            --version <ceph_version>
        xunit2gsheet.py (-h | --help)

    Options:
        -h --help               Show this help message
        --gsheet <gsheet_id>    Google Sheets ID to update
        --xml <xml_file_path>   Path to xUnit XML test results file
        --version <ceph_version>  Ceph version

    Requirements:
        - A JSON file named 'service_account.json' must be present
        - Python packages: gspread, pandas, xml.etree.ElementTree, google-auth
    """


def parse_suite_component(suite_name):
    """
    Parse test suite name to extract component (rgw, rbd, nfs, etc.)
    Args:
        suite_name (str): The suite name to parse
    Returns:
        str: The component name if found, otherwise empty string
    """
    if not suite_name or pd.isna(suite_name):
        return ""
    lower_name = suite_name.lower()

    components = ["rgw", "rbd", "nfs", "cephfs", "rados"]
    for comp in components:
        if comp in lower_name:
            return comp.upper()
    return ""


def parse_ceph_version(version_str):
    """
    Parse Ceph version string into comparable components.
    Args:
        version_str (str): The version string to parse (e.g., "1.2.3-1234")
    Returns:
        tuple: (major, minor, patch, build) version components as integers
    """
    if not version_str or pd.isna(version_str):
        return (0, 0, 0, 0)
    clean_str = re.sub(r"[^0-9.-]", "", str(version_str))
    parts = re.split(r"[.-]", clean_str)
    major = int(parts[0]) if len(parts) > 0 and parts[0] else 0
    minor = int(parts[1]) if len(parts) > 1 and parts[1] else 0
    patch = int(parts[2]) if len(parts) > 2 and parts[2] else 0
    build = int(parts[3]) if len(parts) > 3 and parts[3] else 0
    return (major, minor, patch, build)


def sort_version_columns(columns):
    """
    Sort version columns in descending order (newest first).
    Args:
        columns (list): List of column names to sort
    Returns:
        list: Sorted version-specific columns ordered newest first
    """
    version_cols_status = [
        col for col in columns if (isinstance(col, str) and col.startswith("Status ("))
    ]
    version_cols_time = [
        col
        for col in columns
        if isinstance(col, str) and col.startswith("Time (seconds) (")
    ]
    # Sort Status columns
    version_tuples_status = []
    for col in version_cols_status:
        version_str = col.split("(")[-1].rstrip(")")
        version_tuples_status.append((version_str, col))
    version_tuples_status.sort(key=lambda x: parse_ceph_version(x[0]), reverse=True)
    sorted_status_cols = [col for (ver, col) in version_tuples_status]
    version_tuples_time = []
    for col in version_cols_time:
        version_str = col.split("(")[-1].rstrip(")")
        version_tuples_time.append((version_str, col))
    version_tuples_time.sort(key=lambda x: parse_ceph_version(x[0]), reverse=True)
    sorted_time_cols = [col for (ver, col) in version_tuples_time]
    non_version_cols = [
        col
        for col in columns
        if col not in version_cols_status and col not in version_cols_time
    ]
    ordered_base_cols = []
    if "Test Suite Name" in non_version_cols:
        ordered_base_cols.append("Test Suite Name")
    if "Component" in non_version_cols:
        ordered_base_cols.append("Component")
    if "Test Case Name" in non_version_cols:
        ordered_base_cols.append("Test Case Name")
    if "polarion-testcase-id" in non_version_cols:
        ordered_base_cols.append("polarion-testcase-id")
    remaining_non_version_cols = [
        col for col in non_version_cols if col not in ordered_base_cols
    ]
    return (
        ordered_base_cols
        + remaining_non_version_cols
        + sorted_status_cols
        + sorted_time_cols
    )


def parse_xml_file(xml_file_path, ceph_version=None):
    """
    Parse xUnit XML file and extract test data.
    Args:
        xml_file_path (str): Path to the xUnit XML file
        ceph_version (str, optional): Ceph version to
        associate with these results
    Returns:
        tuple: Four lists containing:
            - Properties data (list of dicts)
            - Summary data (list of dicts)
            - Test status data (list of dicts)
            - Test timings data (list of dicts)
    """
    try:
        tree = ET.parse(xml_file_path)
        root = tree.getroot()
    except ET.ParseError as e:
        print(f"Error parsing XML file {xml_file_path}: {e}")
        return None, None, None, None

    current_file_properties = OrderedDict()
    global_props_element = root.find("properties")
    if global_props_element is not None:
        for prop in global_props_element.findall("property"):
            if prop.get("name"):
                current_file_properties[prop.get("name")] = prop.get("value")

    if ceph_version and "ceph-version" not in current_file_properties:
        current_file_properties["ceph-version"] = ceph_version

    all_testsuites_elements = root.findall("testsuite")
    if all_testsuites_elements:
        first_suite_props_element = all_testsuites_elements[0].find("properties")
        if first_suite_props_element is not None:
            for prop in first_suite_props_element.findall("property"):
                if prop.get("name") and prop.get("name") not in current_file_properties:
                    current_file_properties[prop.get("name")] = prop.get("value")

    filtered_properties = OrderedDict()
    global PREFERRED_PROPERTIES
    if not isinstance(PREFERRED_PROPERTIES, list):
        PREFERRED_PROPERTIES = list(PREFERRED_PROPERTIES)

    for key in list(PREFERRED_PROPERTIES):
        if key in current_file_properties:
            filtered_properties[key] = current_file_properties[key]
    for key, value in current_file_properties.items():
        if key not in filtered_properties:
            filtered_properties[key] = value
            if key not in PREFERRED_PROPERTIES:
                PREFERRED_PROPERTIES.append(key)
    properties_data = [filtered_properties]

    summary_data = []
    test_status_data = []
    test_timings_data = []

    for suite in all_testsuites_elements:
        suite_name = suite.get("name", "N/A_Suite")
        if pd.isna(suite_name) or suite_name == "N/A_Suite":
            continue

        try:
            tests = int(suite.get("tests", 0))
            failures = int(suite.get("failures", 0))
            errors = int(suite.get("errors", 0))
            skipped = int(suite.get("skipped", 0))
            time_sec = float(suite.get("time", 0.0))
        except ValueError:
            tests = failures = errors = skipped = 0
            time_sec = 0.0

        passed_count = tests - (failures + errors + skipped)
        success_rate_float = (passed_count / tests * 100) if tests > 0 else 0.0
        success_rate_str = f"{success_rate_float:.2f}%"

        summary_data.append(
            OrderedDict(
                [
                    ("Test Suite Name", suite_name),
                    ("Total Tests", tests),
                    ("Passed", passed_count),
                    ("Failures", failures),
                    ("Errors", errors),
                    ("Skipped", skipped),
                    ("Success Rate", success_rate_str),
                    ("Time (seconds)", time_sec),
                ]
            )
        )

        for tc in suite.findall("testcase"):
            tc_name = tc.get("name", "N/A_Case")
            if pd.isna(tc_name) or tc_name == "N/A_Case":
                continue

            try:
                tc_time = float(tc.get("time", 0.0))
            except ValueError:
                tc_time = 0.0

            actual_tc_status = "Passed"
            if tc.find("failure") is not None:
                actual_tc_status = "Failed"
            elif tc.find("error") is not None:
                actual_tc_status = "Error"
            elif tc.find("skipped") is not None or tc.get("status") == "skipped":
                actual_tc_status = "Skipped"

            tc_polarion_id = None
            tc_props_el = tc.find("properties")
            if tc_props_el is not None:
                for prop in tc_props_el.findall("property"):
                    if prop.get("name") == "polarion-testcase-id":
                        tc_polarion_id = prop.get("value")
                        break
            test_status_data.append(
                OrderedDict(
                    [
                        ("Test Suite Name", suite_name),
                        ("Component", parse_suite_component(suite_name)),
                        ("Test Case Name", tc_name),
                        (
                            f"Status ({ceph_version})" if ceph_version else "Status",
                            actual_tc_status,
                        ),
                        ("polarion-testcase-id", tc_polarion_id),
                    ]
                )
            )
            test_timings_data.append(
                OrderedDict(
                    [
                        ("Test Suite Name", suite_name),
                        ("Component", parse_suite_component(suite_name)),
                        ("Test Case Name", tc_name),
                        (
                            (
                                f"Time (seconds) ({ceph_version})"
                                if ceph_version
                                else "Time (seconds)"
                            ),
                            tc_time,
                        ),
                    ]
                )
            )

    return properties_data, summary_data, test_status_data, test_timings_data


def get_gsheet_client(credentials_path):
    """
    Authenticates with Google Sheets API using service account credentials.
    Args:
        credentials_path (str): Path to service account JSON credentials file
    Returns:
        gspread.Client: Authenticated Google Sheets client
    """
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.file",
    ]
    creds = Credentials.from_service_account_file(credentials_path, scopes=scopes)
    return gspread.authorize(creds)


def get_or_create_worksheet(spreadsheet, sheet_name):
    """
    Gets a worksheet by name or creates it if it doesn't exist.
    Args:
        spreadsheet (gspread.Spreadsheet): The parent spreadsheet
        sheet_name (str): Name of worksheet to get/create
    Returns:
        gspread.Worksheet: The requested worksheet
    """
    try:
        worksheet = spreadsheet.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows="100", cols="20")
    return worksheet


def apply_gsheet_status_colors(worksheet):
    """
    Applies color coding to Status columns in Google Sheets.
    Args:
        worksheet (gspread.Worksheet): Worksheet to format
    Colors:
        - PASSED: Light green
        - FAILED: Light red
        - ERROR: Light yellow
        - SKIPPED: Light gray
    """
    if worksheet.row_count <= 1:
        return
    # Define formats for different statuses
    formats = {
        "PASSED": {"backgroundColor": {"red": 0.85, "green": 0.92, "blue": 0.83}},
        "FAILED": {"backgroundColor": {"red": 0.96, "green": 0.8, "blue": 0.8}},
        "ERROR": {"backgroundColor": {"red": 1.0, "green": 0.9, "blue": 0.7}},
        "SKIPPED": {"backgroundColor": {"red": 0.8, "green": 0.8, "blue": 0.8}},
    }
    # Get all conditional format rules
    rules = gsf.get_conditional_format_rules(worksheet)
    rules.clear()  # Clear existing rules to avoid duplicates
    header_row = worksheet.row_values(1)
    if not header_row:
        return

    for col_idx, header_value in enumerate(header_row, 1):
        if isinstance(header_value, str) and header_value.startswith("Status ("):
            col_letter = gspread.utils.rowcol_to_a1(1, col_idx)[0]
            range_str = f"{col_letter}2:{col_letter}{worksheet.row_count}"
            for status, fmt in formats.items():
                rule = gsf.ConditionalFormatRule(
                    ranges=[gsf.GridRange.from_a1_range(range_str, worksheet)],
                    booleanRule=gsf.BooleanRule(
                        condition=gsf.BooleanCondition("TEXT_EQ", [status]),
                        format=gsf.CellFormat(
                            backgroundColor=gsf.Color(
                                fmt["backgroundColor"]["red"],
                                fmt["backgroundColor"]["green"],
                                fmt["backgroundColor"]["blue"],
                            )
                        ),
                    ),
                )
                rules.append(rule)
    rules.save()


def auto_adjust_gsheet_column_width(worksheet, df):
    """
    Adjust column widths based on content using batch_update.
    Args:
        worksheet (gspread.Worksheet): Worksheet to adjust
        df (pd.DataFrame): DataFrame containing the data to size columns for
    Notes:
        - Column widths are set between 100-300 pixels
        - Width is based on max content length or header length
    """
    requests = []
    for i, col_name in enumerate(df.columns, 1):
        # Convert column name to string
        col_name_str = str(col_name)
        # Get maximum length between data and column name
        max_data_len = df[col_name].astype(str).str.len().max()
        max_len = max(max_data_len, len(col_name_str))
        # Set reasonable column width (in pixels)
        pixel_size = min(300, max(100, int(max_len * 7)))
        requests.append(
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": worksheet.id,
                        "dimension": "COLUMNS",
                        "startIndex": i - 1,
                        "endIndex": i,
                    },
                    "properties": {"pixelSize": pixel_size},
                    "fields": "pixelSize",
                }
            }
        )
    if requests:
        worksheet.spreadsheet.batch_update({"requests": requests})


def update_gsheet(
    gsheet_id,
    all_properties_data,
    all_summary_data,
    all_test_status_data,
    all_test_timings_data,
    credentials_path,
):
    """
    Update Google Sheet with new test data.
    Args:
        gsheet_id (str): ID of the Google Sheet to update
        all_properties_data (list): Test properties metadata
        all_summary_data (list): Test suite summary data
        all_test_status_data (list): Individual test statuses
        all_test_timings_data (list): Individual test timings
        credentials_path (str): Path to service account credentials
    Behavior:
        - Renames spreadsheet with build number if available
        - Updates four worksheets (Properties, Summary, Status, Timings)
        - Preserves existing data while adding new version columns
        - Applies formatting and auto-resizing
    """
    client = get_gsheet_client(credentials_path)
    spreadsheet = client.open_by_key(gsheet_id)
    ceph_version = (
        all_properties_data[0].get("ceph-version", None)
        if all_properties_data
        else None
    )
    build_number = (
        all_properties_data[0].get("build", "").strip() if all_properties_data else ""
    )
    if build_number:
        try:
            current_title = spreadsheet.title
            if f"({build_number})" not in current_title:
                new_title = f"RHCS Test Results ({build_number})"
                spreadsheet.update_title(new_title)
                print(f"Spreadsheet renamed to: {new_title}")
        except Exception as e:
            print(f"Error renaming spreadsheet: {e}")

    # --- Properties Sheet ---
    ws_properties = get_or_create_worksheet(spreadsheet, SHEET_PROPERTIES)
    df_new_properties = pd.DataFrame(all_properties_data)
    try:
        df_existing_properties = get_as_dataframe(
            ws_properties, evaluate_formulas=False, header=0
        )
        df_existing_properties.dropna(how="all", inplace=True)
        if not df_existing_properties.empty:
            df_properties_final = pd.concat(
                [df_existing_properties, df_new_properties], axis=0
            )
            df_properties_final = df_properties_final.loc[
                :, ~df_properties_final.columns.duplicated()
            ]
            df_properties_final = df_properties_final.fillna("")
        else:
            df_properties_final = df_new_properties
    except Exception as e:
        print(f"Error merging properties data: {e}, using new data only.")
        df_properties_final = df_new_properties
    ws_properties.clear()
    set_with_dataframe(
        ws_properties, df_properties_final, include_index=False, resize=True
    )
    if not df_properties_final.empty:
        auto_adjust_gsheet_column_width(ws_properties, df_properties_final)

    # --- Summary Sheet ---
    ws_summary = get_or_create_worksheet(spreadsheet, SHEET_SUMMARY)
    df_new_summary = pd.DataFrame(all_summary_data)
    df_existing_summary = get_as_dataframe(
        ws_summary, evaluate_formulas=False, header=0
    )
    df_existing_summary.dropna(how="all", inplace=True)
    if not df_existing_summary.empty:
        merge_cols = ["Test Suite Name"]
        for col in merge_cols:
            if col not in df_existing_summary.columns:
                df_existing_summary[col] = pd.NA
            if col not in df_new_summary.columns:
                df_new_summary[col] = pd.NA
        existing_suites = set(df_existing_summary["Test Suite Name"].dropna().unique())
        new_suites = set(df_new_summary["Test Suite Name"].dropna().unique())
        new_to_add = new_suites - existing_suites
        if new_to_add:
            df_summary_final = pd.concat(
                [
                    df_existing_summary,
                    df_new_summary[df_new_summary["Test Suite Name"].isin(new_to_add)],
                ]
            )
        else:
            df_summary_final = df_existing_summary
    else:
        df_summary_final = df_new_summary
    ws_summary.clear()
    set_with_dataframe(ws_summary, df_summary_final, include_index=False, resize=True)
    if not df_summary_final.empty:
        auto_adjust_gsheet_column_width(ws_summary, df_summary_final)
    ws_status = get_or_create_worksheet(spreadsheet, SHEET_TEST_STATUS)
    df_new_test_status = pd.DataFrame(all_test_status_data)
    try:
        df_existing_test_status = get_as_dataframe(
            ws_status, evaluate_formulas=False, header=0
        )
        df_existing_test_status.dropna(how="all", inplace=True)
        if not df_existing_test_status.empty:
            version_col_pattern = (
                f"Status ({ceph_version})" if ceph_version else "Status"
            )
            new_status_col_name = df_new_test_status.columns[3]
            df_new_test_status_renamed = df_new_test_status.rename(
                columns={new_status_col_name: version_col_pattern}
            )
            merge_cols = [
                "Test Suite Name",
                "Component",
                "Test Case Name",
                "polarion-testcase-id",
            ]
            for col in merge_cols:
                if col not in df_existing_test_status.columns:
                    df_existing_test_status[col] = pd.NA
                if col not in df_new_test_status_renamed.columns:
                    df_new_test_status_renamed[col] = pd.NA
            if version_col_pattern in df_existing_test_status.columns:
                df_existing = df_existing_test_status.set_index(merge_cols)
                df_new = df_new_test_status_renamed.set_index(merge_cols)
                new_test_cases = df_new.index.difference(df_existing.index)
                # Only add new test cases to avoid overwriting existing data
                if not new_test_cases.empty:
                    df_combined = pd.concat([df_existing, df_new.loc[new_test_cases]])
                else:
                    df_combined = df_existing
                # Reset index to get back to normal columns
                df_test_status_final = df_combined.reset_index()
            else:
                # Version column doesn't exist - proceed with normal merge
                df_test_status_final = pd.merge(
                    df_existing_test_status,
                    df_new_test_status_renamed,
                    on=merge_cols,
                    how="outer",
                    suffixes=("_old", "_new"),
                )
                # Clean up any duplicate columns from merge
                df_test_status_final = df_test_status_final.loc[
                    :, ~df_test_status_final.columns.duplicated()
                ]
            df_test_status_final = df_test_status_final[
                sort_version_columns(df_test_status_final.columns)
            ]
        else:
            # No existing data - just use the new data
            df_test_status_final = df_new_test_status
            if ceph_version and len(df_test_status_final.columns) > 3:
                status_col_name = df_test_status_final.columns[3]
                df_test_status_final.rename(
                    columns={status_col_name: f"Status({ceph_version})"}, inplace=True
                )

    except Exception as e:
        print(f"Error merging test status data: {e}, using new data only.")
        df_test_status_final = df_new_test_status
        if ceph_version and len(df_test_status_final.columns) > 3:
            status_col_name = df_test_status_final.columns[3]
            df_test_status_final.rename(
                columns={status_col_name: f"Status ({ceph_version})"}, inplace=True
            )
    # --- Test Case Timings Sheet ---
    ws_timings = get_or_create_worksheet(spreadsheet, SHEET_TEST_TIMINGS)
    df_new_test_timings = pd.DataFrame(all_test_timings_data)
    try:
        df_existing_test_timings = get_as_dataframe(
            ws_timings, evaluate_formulas=False, header=0
        )
        df_existing_test_timings.dropna(how="all", inplace=True)
        if not df_existing_test_timings.empty:
            time_version_col_pattern = (
                f"Time (seconds) ({ceph_version})" if ceph_version else "Time (seconds)"
            )
            # Prepare the new data with proper column names
            new_time_col_name = df_new_test_timings.columns[3]
            df_new_test_timings_renamed = df_new_test_timings.rename(
                columns={new_time_col_name: time_version_col_pattern}
            )
            # Merge columns to identify common test cases
            merge_cols_time = ["Test Suite Name", "Component", "Test Case Name"]
            # Ensure all merge columns exist in both dataframes
            for col in merge_cols_time:
                if col not in df_existing_test_timings.columns:
                    df_existing_test_timings[col] = pd.NA
                if col not in df_new_test_timings_renamed.columns:
                    df_new_test_timings_renamed[col] = pd.NA
            if time_version_col_pattern in df_existing_test_timings.columns:
                # Set index for both dataframes
                df_existing = df_existing_test_timings.set_index(merge_cols_time)
                df_new = df_new_test_timings_renamed.set_index(merge_cols_time)
                new_test_cases = df_new.index.difference(df_existing.index)
                # Only add new test cases to avoid overwriting existing data
                if not new_test_cases.empty:
                    df_combined = pd.concat([df_existing, df_new.loc[new_test_cases]])
                else:
                    df_combined = df_existing
                # Reset index to get back to normal columns
                df_test_timings_final = df_combined.reset_index()
            else:
                # Version column doesn't exist - proceed with normal merge
                df_test_timings_final = pd.merge(
                    df_existing_test_timings,
                    df_new_test_timings_renamed,
                    on=merge_cols_time,
                    how="outer",
                    suffixes=("_old", "_new"),
                )
                # Clean up any duplicate columns from merge
                df_test_timings_final = df_test_timings_final.loc[
                    :, ~df_test_timings_final.columns.duplicated()
                ]
            # Ensure proper column order
            df_test_timings_final = df_test_timings_final[
                sort_version_columns(df_test_timings_final.columns)
            ]
        else:
            # No existing data - just use the new data
            df_test_timings_final = df_new_test_timings
            if ceph_version and len(df_test_timings_final.columns) > 3:
                time_col_name = df_test_timings_final.columns[3]
                df_test_timings_final.rename(
                    columns={time_col_name: f"Time (seconds)" f"({ceph_version})"},
                    inplace=True,
                )

    except Exception as e:
        print(f"Error merging test timings data: {e}, using new data only.")
        df_test_timings_final = df_new_test_timings
        if ceph_version and len(df_test_timings_final.columns) > 3:
            time_col_name = df_test_timings_final.columns[3]
            df_test_timings_final.rename(
                columns={time_col_name: f"Time (seconds) ({ceph_version})"},
                inplace=True,
            )

    # --- Write to Google Sheets ---
    ws_status.clear()
    set_with_dataframe(
        ws_status, df_test_status_final, include_index=False, resize=True
    )
    if not df_test_status_final.empty:
        auto_adjust_gsheet_column_width(ws_status, df_test_status_final)
        apply_gsheet_status_colors(ws_status)

    ws_timings.clear()
    set_with_dataframe(
        ws_timings, df_test_timings_final, include_index=False, resize=True
    )
    if not df_test_timings_final.empty:
        auto_adjust_gsheet_column_width(ws_timings, df_test_timings_final)

    print(f"Google Sheet '{spreadsheet.title}' updated successfully.")


if __name__ == "__main__":
    args = docopt(DOC)
    # --- Configuration ---
    GOOGLE_CREDENTIALS_PATH = "service_account.json"
    if not os.path.exists(GOOGLE_CREDENTIALS_PATH):
        print(
            f"Error: Google credentials file not"
            f"found at '{GOOGLE_CREDENTIALS_PATH}'"
        )
        sys.exit(1)
    gsheet_id = args["--gsheet"]
    xml_file_path = args["--xml"]
    ceph_version_arg = args["--version"]
    print(
        f"Processing {xml_file_path} for version"
        f"{ceph_version_arg} into GSheet ID {gsheet_id}"
    )
    properties, summary, test_status, test_timings = parse_xml_file(
        xml_file_path, ceph_version_arg
    )
    if properties and summary and test_status and test_timings:
        update_gsheet(
            gsheet_id,
            properties,
            summary,
            test_status,
            test_timings,
            GOOGLE_CREDENTIALS_PATH,
        )
        print(f"https://docs.google.com/spreadsheets/d/{gsheet_id}")
    else:
        print("Error: Failed to process XML file, GSheet not updated.")
