"""
990 Foundation Explorer — Local XML Edition
--------------------------------------------
Reads IRS 990/990PF XML files from a local folder structure, matched by EIN
via an index CSV, and exports a filtered spreadsheet.

Expected inputs:
  - XML root folder: directory containing subfolders named by XML_BATCH_ID,
                     each containing files named {OBJECT_ID}_public.xml
  - Index CSV:       must contain columns EIN, OBJECT_ID (clean integer), XML_BATCH_ID
                     Additional columns (TAX_PERIOD, RETURN_TYPE, etc.) are used
                     for metadata but are not required.
  - EIN list:        one EIN per line, dashes optional

Flow:
  Step 1 – Provide paths + EINs
  Step 2 – Parse matched XML files; all fields unioned across all files
  Step 3 – Uncheck fields to exclude
  Step 4 – Preview + download (one row per filing, all filings per EIN)

XML parsing strategy:
  - Namespace-agnostic: strips {namespace} prefixes from all tags
  - Flattens nested elements with dot-notation keys (e.g. Filer.USAddress.CityNm)
  - Repeating groups (e.g. multiple officers) are serialised as JSON strings
    in a single cell — one column per repeating group type
  - Fields from ReturnHeader and the primary IRS990/IRS990PF element are merged
    into one row per filing
"""

import io
import json
import os
import re
import xml.etree.ElementTree as ET
from collections import defaultdict

import pandas as pd
import streamlit as st

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Index CSV column names — adjust here if your CSV uses different names
COL_EIN = "EIN"
COL_OBJECT_ID = "OBJECT_ID"
COL_BATCH_ID = "XML_BATCH_ID"

# XML elements to parse as the primary return body.
# Any tag not in this list that appears under ReturnData is also captured.
PRIMARY_RETURN_ELEMENTS = {"IRS990", "IRS990PF", "IRS990EZ", "IRS990T"}

# Tags to skip entirely (binary/boilerplate, not useful as data fields)
SKIP_TAGS = {"BuildTS", "softwareId", "softwareVersionNum", "returnVersion",
             "documentCnt", "binaryAttachmentCnt", "xsi:schemaLocation"}

# Human-readable labels for common fields. Raw tag names are used for anything
# not listed here — they're already fairly readable (CamelCase IRS tag names).
FIELD_LABELS = {
    "ReturnHeader.ReturnTs":                        "Filing Timestamp",
    "ReturnHeader.TaxPeriodEndDt":                  "Tax Period End",
    "ReturnHeader.TaxPeriodBeginDt":                "Tax Period Begin",
    "ReturnHeader.TaxYr":                           "Tax Year",
    "ReturnHeader.ReturnTypeCd":                    "Return Type",
    "ReturnHeader.Filer.EIN":                       "EIN",
    "ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt": "Organization Name",
    "ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt": "Organization Name (Line 2)",
    "ReturnHeader.Filer.PhoneNum":                  "Phone",
    "ReturnHeader.Filer.USAddress.AddressLine1Txt": "Address",
    "ReturnHeader.Filer.USAddress.CityNm":          "City",
    "ReturnHeader.Filer.USAddress.StateAbbreviationCd": "State",
    "ReturnHeader.Filer.USAddress.ZIPCd":           "ZIP",
    "ReturnHeader.BusinessOfficerGrp.PersonNm":     "Signing Officer Name",
    "ReturnHeader.BusinessOfficerGrp.PersonTitleTxt": "Signing Officer Title",
    "ReturnHeader.BusinessOfficerGrp.PhoneNum":     "Signing Officer Phone",
    "ReturnHeader.PreparerFirmGrp.PreparerFirmName.BusinessNameLine1Txt": "Preparer Firm",
    "ReturnHeader.PreparerPersonGrp.PreparerPersonNm": "Preparer Name",
    "ReturnHeader.PreparerPersonGrp.PhoneNum":      "Preparer Phone",
}

# Fields checked by default in Step 3
DEFAULT_CHECKED = {
    "ReturnHeader.TaxYr",
    "ReturnHeader.ReturnTypeCd",
    "ReturnHeader.TaxPeriodEndDt",
    "ReturnHeader.Filer.EIN",
    "ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt",
    "ReturnHeader.Filer.PhoneNum",
    "ReturnHeader.Filer.USAddress.AddressLine1Txt",
    "ReturnHeader.Filer.USAddress.CityNm",
    "ReturnHeader.Filer.USAddress.StateAbbreviationCd",
    "ReturnHeader.Filer.USAddress.ZIPCd",
    "ReturnHeader.BusinessOfficerGrp.PersonNm",
    "ReturnHeader.BusinessOfficerGrp.PersonTitleTxt",
    "ReturnHeader.BusinessOfficerGrp.PhoneNum",
}

# ---------------------------------------------------------------------------
# XML parsing
# ---------------------------------------------------------------------------

def strip_ns(tag: str) -> str:
    """Remove XML namespace prefix from a tag: '{http://...}TagName' -> 'TagName'."""
    return re.sub(r"^\{[^}]+\}", "", tag)


def flatten_element(el: ET.Element, prefix: str = "") -> tuple[dict, dict]:
    """
    Recursively flatten an XML element into a dict of dot-notation key -> value.
    Returns:
      scalar_fields  – {key: value} for leaf text nodes
      repeated_fields – {key: [list of dicts]} for elements that appear >1 time
                        under the same parent (e.g. officer groups)

    Repeated elements are detected at parse time and stored separately so the
    caller can decide how to serialise them.
    """
    scalar_fields = {}
    repeated_fields = {}

    # Count child tag occurrences to detect repeating groups
    child_tag_counts = defaultdict(int)
    for child in el:
        child_tag_counts[strip_ns(child.tag)] += 1

    for child in el:
        tag = strip_ns(child.tag)
        if tag in SKIP_TAGS:
            continue
        key = f"{prefix}.{tag}" if prefix else tag
        children = list(child)

        if child_tag_counts[tag] > 1:
            # Repeating group — collect all siblings under parent key
            if key not in repeated_fields:
                repeated_fields[key] = []
            if children:
                sub_scalar, _ = flatten_element(child, prefix="")
                repeated_fields[key].append(sub_scalar)
            else:
                repeated_fields[key].append(child.text or "")
        elif children:
            sub_scalar, sub_repeated = flatten_element(child, prefix=key)
            scalar_fields.update(sub_scalar)
            repeated_fields.update(sub_repeated)
        else:
            scalar_fields[key] = (child.text or "").strip()

    return scalar_fields, repeated_fields


def parse_xml_file(path: str) -> list[dict]:
    """
    Parse one XML file and return a list of row dicts (usually just one row,
    but structured to allow for extension).

    Merges ReturnHeader fields with the primary return body (IRS990, IRS990PF, etc.).
    Repeating groups are JSON-serialised into a single column per group.
    Prefixes header fields with 'ReturnHeader.' and body fields with the element
    name (e.g. 'IRS990PF.') for unambiguous column naming.
    """
    try:
        tree = ET.parse(path)
    except ET.ParseError as e:
        return [{"_parse_error": str(e), "_source_file": path}]

    root = tree.getroot()
    row = {"_source_file": path}
    repeated = {}

    # --- ReturnHeader ---
    header_el = None
    for child in root:
        if strip_ns(child.tag) == "ReturnHeader":
            header_el = child
            break

    if header_el is not None:
        scalars, reps = flatten_element(header_el, prefix="ReturnHeader")
        row.update(scalars)
        repeated.update({f"ReturnHeader.{k}" if not k.startswith("ReturnHeader") else k: v
                         for k, v in reps.items()})

    # --- ReturnData: primary body element ---
    for child in root:
        if strip_ns(child.tag) != "ReturnData":
            continue
        for body_el in child:
            tag = strip_ns(body_el.tag)
            scalars, reps = flatten_element(body_el, prefix=tag)
            row.update(scalars)
            repeated.update(reps)
        break  # only one ReturnData expected

    # Serialise repeated groups as JSON strings
    for key, items in repeated.items():
        row[key] = json.dumps(items, ensure_ascii=False)

    return [row]


# ---------------------------------------------------------------------------
# Index + file resolution
# ---------------------------------------------------------------------------

def load_index(csv_path: str) -> pd.DataFrame:
    """Load and normalise the index CSV."""
    df = pd.read_csv(csv_path, dtype={COL_OBJECT_ID: str, COL_EIN: str})
    df[COL_EIN] = df[COL_EIN].str.strip().str.replace(r"\D", "", regex=True)
    df[COL_OBJECT_ID] = df[COL_OBJECT_ID].str.strip()
    df[COL_BATCH_ID] = df[COL_BATCH_ID].str.strip()
    return df


def resolve_filings(index_df: pd.DataFrame, eins: list[str]) -> pd.DataFrame:
    """
    Return all index rows matching the supplied EINs.
    """
    matched = index_df[index_df[COL_EIN].isin(set(eins))].copy()
    return matched


def xml_path(root_folder: str, batch_id: str, object_id: str) -> str:
    return os.path.join(root_folder, batch_id, f"{object_id}_public.xml")


# ---------------------------------------------------------------------------
# Field ordering helpers
# ---------------------------------------------------------------------------

def ordered_columns(all_columns: list[str]) -> list[str]:
    """
    Return columns in a logical order:
      1. ReturnHeader fields (identity + contact first)
      2. IRS990PF fields
      3. IRS990 fields
      4. IRS990EZ fields
      5. IRS990T fields
      6. Everything else
      7. Internal fields (_source_file, _parse_error) last
    """
    def sort_key(col):
        if col.startswith("_"):
            return (99, col)
        if col.startswith("ReturnHeader.Filer"):
            return (0, col)
        if col.startswith("ReturnHeader"):
            return (1, col)
        if col.startswith("IRS990PF"):
            return (2, col)
        if col.startswith("IRS990."):
            return (3, col)
        if col.startswith("IRS990EZ"):
            return (4, col)
        if col.startswith("IRS990T"):
            return (5, col)
        return (6, col)

    return sorted(all_columns, key=sort_key)


def label_for(key: str) -> str:
    return FIELD_LABELS.get(key, key)


# ---------------------------------------------------------------------------
# Streamlit UI
# ---------------------------------------------------------------------------

st.set_page_config(page_title="990 Foundation Explorer", layout="wide")
st.title("990 Foundation Explorer")
st.caption("Parses local IRS 990/990PF XML files matched via an index CSV.")

if "parsed_df" not in st.session_state:
    st.session_state.parsed_df = None
if "all_columns" not in st.session_state:
    st.session_state.all_columns = []

# ---------------------------------------------------------------------------
# Step 1: Inputs
# ---------------------------------------------------------------------------
st.header("Step 1 — Inputs")

col_a, col_b = st.columns(2)

with col_a:
    xml_root = st.text_input(
        "XML root folder path",
        placeholder="/Users/you/irs_990_xmls",
        help="Folder containing subfolders named by XML_BATCH_ID (e.g. 2025_TEOS_XML_12A/)",
    )
    index_path = st.text_input(
        "Index CSV path",
        placeholder="/Users/you/index_2025.csv",
        help="Must contain columns: EIN, OBJECT_ID (clean integer), XML_BATCH_ID",
    )

with col_b:
    ein_input = st.text_area(
        "EINs (one per line, dashes optional)",
        height=200,
        placeholder="237125454\n330759830\n933607439",
    )

eins_raw = [line.strip() for line in ein_input.splitlines() if line.strip()]
eins_cleaned = [re.sub(r"\D", "", e) for e in eins_raw if re.sub(r"\D", "", e)]

seen = set()
eins_deduped = []
for e in eins_cleaned:
    if e not in seen:
        seen.add(e)
        eins_deduped.append(e)

if eins_deduped:
    st.caption(f"{len(eins_deduped)} unique EIN(s) entered.")
    if len(eins_cleaned) != len(eins_deduped):
        st.warning(f"{len(eins_cleaned) - len(eins_deduped)} duplicate(s) removed.")

# ---------------------------------------------------------------------------
# Step 2: Parse
# ---------------------------------------------------------------------------
st.header("Step 2 — Parse XML Files")

inputs_ready = xml_root and index_path and eins_deduped

if not inputs_ready:
    st.info("Complete all inputs above to continue.")
else:
    if st.button("Parse Files", type="primary"):
        errors = []

        # Load index
        if not os.path.isfile(index_path):
            st.error(f"Index CSV not found: {index_path}")
            st.stop()
        if not os.path.isdir(xml_root):
            st.error(f"XML root folder not found: {xml_root}")
            st.stop()

        with st.spinner("Loading index CSV…"):
            try:
                index_df = load_index(index_path)
            except Exception as e:
                st.error(f"Failed to load index CSV: {e}")
                st.stop()

        filings = resolve_filings(index_df, eins_deduped)

        if filings.empty:
            st.error("None of the supplied EINs were found in the index CSV.")
            st.stop()

        not_found = [e for e in eins_deduped if e not in filings[COL_EIN].values]
        if not_found:
            st.warning(f"{len(not_found)} EIN(s) not found in index: {', '.join(not_found)}")

        # Parse XML files
        all_rows = []
        progress = st.progress(0, text="Starting…")
        status = st.empty()
        total = len(filings)

        for i, (_, idx_row) in enumerate(filings.iterrows()):
            ein = idx_row[COL_EIN]
            batch = idx_row[COL_BATCH_ID]
            obj_id = idx_row[COL_OBJECT_ID]
            fpath = xml_path(xml_root, batch, obj_id)

            status.text(f"Parsing {os.path.basename(fpath)} ({i + 1}/{total})…")

            if not os.path.isfile(fpath):
                errors.append({"EIN": ein, "File": fpath, "Error": "File not found"})
                all_rows.append({"ReturnHeader.Filer.EIN": ein, "_source_file": fpath,
                                 "_parse_error": "File not found"})
            else:
                rows = parse_xml_file(fpath)
                for r in rows:
                    # Attach index metadata that may not be in the XML
                    r.setdefault("ReturnHeader.Filer.EIN", ein)
                    # Carry through any extra index columns (TAX_PERIOD, RETURN_TYPE, etc.)
                    for col in index_df.columns:
                        if col not in {COL_EIN, COL_OBJECT_ID, COL_BATCH_ID}:
                            r.setdefault(f"_index.{col}", idx_row[col])
                all_rows.extend(rows)

            progress.progress((i + 1) / total, text=f"{i + 1}/{total} files processed")

        status.empty()
        progress.empty()

        df = pd.DataFrame(all_rows)
        all_cols = ordered_columns(list(df.columns))
        df = df[all_cols]

        st.session_state.parsed_df = df
        st.session_state.all_columns = all_cols

        success = total - len(errors)
        st.success(f"{success}/{total} files parsed successfully, {len(errors)} error(s).")
        if errors:
            with st.expander(f"⚠️ {len(errors)} file error(s)"):
                st.dataframe(pd.DataFrame(errors), use_container_width=True)

# ---------------------------------------------------------------------------
# Step 3: Field selection
# ---------------------------------------------------------------------------
if st.session_state.all_columns:
    df_full = st.session_state.parsed_df
    all_cols = st.session_state.all_columns

    # Separate internal cols (_source_file, _parse_error, _index.*) from data cols
    internal_cols = [c for c in all_cols if c.startswith("_")]
    data_cols = [c for c in all_cols if not c.startswith("_")]

    st.header("Step 3 — Select Fields")
    st.write(
        f"{len(data_cols)} fields found across all parsed files. "
        "Uncheck anything you don't need in the export."
    )

    # Two-column checkbox layout
    mid = (len(data_cols) + 1) // 2
    col1, col2 = st.columns(2)
    selected_cols = []

    for i, col in enumerate(data_cols):
        target = col1 if i < mid else col2
        checked = target.checkbox(
            label_for(col),
            value=(col in DEFAULT_CHECKED),
            key=f"col_{col}",
        )
        if checked:
            selected_cols.append(col)

    # Always append _parse_error if any errors occurred, so they're visible in export
    if "_parse_error" in internal_cols and df_full["_parse_error"].notna().any():
        selected_cols.append("_parse_error")

    # ---------------------------------------------------------------------------
    # Step 4: Preview + download
    # ---------------------------------------------------------------------------
    st.header("Step 4 — Export")

    if not selected_cols:
        st.warning("Select at least one field above.")
    else:
        df_export = df_full[[c for c in selected_cols if c in df_full.columns]].copy()

        # Rename columns to human-readable labels for the export
        df_export.rename(columns={c: label_for(c) for c in df_export.columns}, inplace=True)

        st.subheader("Preview")
        st.dataframe(df_export, use_container_width=True)

        excel_buf = io.BytesIO()
        with pd.ExcelWriter(excel_buf, engine="openpyxl") as writer:
            df_export.to_excel(writer, index=False, sheet_name="990 Data")
        excel_buf.seek(0)

        dl1, dl2 = st.columns(2)
        dl1.download_button(
            label="Download as Excel (.xlsx)",
            data=excel_buf,
            file_name="990_foundation_data.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        dl2.download_button(
            label="Download as CSV",
            data=df_export.to_csv(index=False).encode("utf-8"),
            file_name="990_foundation_data.csv",
            mime="text/csv",
        )
