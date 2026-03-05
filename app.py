"""
990 Foundation Explorer
-----------------------
Reads IRS 990/990PF XML files from uploaded ZIP archives, matched by EIN
via an uploaded index CSV, and exports a filtered spreadsheet.

Flow:
  Step 1 – Upload index CSV + paste EINs
            App resolves which XML_BATCH_IDs are needed and tells you exactly
            which ZIP files to upload.
  Step 2 – Upload only the required ZIP files
  Step 3 – Parse; app extracts and parses only the matched XML files
  Step 4 – Uncheck fields to exclude
  Step 5 – Preview + download

XML parsing strategy:
  - Namespace-agnostic: strips {namespace} prefixes from all tags
  - Flattens nested elements with dot-notation keys (e.g. Filer.USAddress.CityNm)
  - Repeating groups (e.g. multiple officers) are serialised as JSON strings
    in a single cell
  - Fields from ReturnHeader and the primary IRS990/IRS990PF element are merged
    into one row per filing
"""

import io
import json
import re
import os
import tempfile
import zipfile
from collections import defaultdict
import xml.etree.ElementTree as ET

import pandas as pd
import streamlit as st

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

COL_EIN = "EIN"
COL_OBJECT_ID = "OBJECT_ID"
COL_BATCH_ID = "XML_BATCH_ID"

SKIP_TAGS = {
    "BuildTS", "softwareId", "softwareVersionNum", "returnVersion",
    "documentCnt", "binaryAttachmentCnt", "xsi:schemaLocation",
}

FIELD_LABELS = {
    "ReturnHeader.ReturnTs":                                                "Filing Timestamp",
    "ReturnHeader.TaxPeriodEndDt":                                          "Tax Period End",
    "ReturnHeader.TaxPeriodBeginDt":                                        "Tax Period Begin",
    "ReturnHeader.TaxYr":                                                   "Tax Year",
    "ReturnHeader.ReturnTypeCd":                                            "Return Type",
    "ReturnHeader.Filer.EIN":                                               "EIN",
    "ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt":                 "Organization Name",
    "ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt":                 "Organization Name (Line 2)",
    "ReturnHeader.Filer.PhoneNum":                                          "Phone",
    "ReturnHeader.Filer.USAddress.AddressLine1Txt":                         "Address",
    "ReturnHeader.Filer.USAddress.CityNm":                                  "City",
    "ReturnHeader.Filer.USAddress.StateAbbreviationCd":                     "State",
    "ReturnHeader.Filer.USAddress.ZIPCd":                                   "ZIP",
    "ReturnHeader.BusinessOfficerGrp.PersonNm":                             "Signing Officer Name",
    "ReturnHeader.BusinessOfficerGrp.PersonTitleTxt":                       "Signing Officer Title",
    "ReturnHeader.BusinessOfficerGrp.PhoneNum":                             "Signing Officer Phone",
    "ReturnHeader.PreparerFirmGrp.PreparerFirmName.BusinessNameLine1Txt":   "Preparer Firm",
    "ReturnHeader.PreparerPersonGrp.PreparerPersonNm":                      "Preparer Name",
    "ReturnHeader.PreparerPersonGrp.PhoneNum":                              "Preparer Phone",
}

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
    return re.sub(r"^\{[^}]+\}", "", tag)


def flatten_element(el: ET.Element, prefix: str = "") -> tuple:
    scalar_fields = {}
    repeated_fields = {}

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


def parse_xml_bytes(data: bytes, filename: str) -> list:
    try:
        root = ET.fromstring(data)
    except ET.ParseError as e:
        return [{"_parse_error": str(e), "_source_file": filename}]

    row = {"_source_file": filename}
    repeated = {}

    for child in root:
        if strip_ns(child.tag) == "ReturnHeader":
            scalars, reps = flatten_element(child, prefix="ReturnHeader")
            row.update(scalars)
            repeated.update(reps)
            break

    for child in root:
        if strip_ns(child.tag) != "ReturnData":
            continue
        for body_el in child:
            tag = strip_ns(body_el.tag)
            scalars, reps = flatten_element(body_el, prefix=tag)
            row.update(scalars)
            repeated.update(reps)
        break

    for key, items in repeated.items():
        row[key] = json.dumps(items, ensure_ascii=False)

    return [row]


# ---------------------------------------------------------------------------
# Index helpers
# ---------------------------------------------------------------------------

def load_index(uploaded_file) -> pd.DataFrame:
    df = pd.read_csv(uploaded_file, dtype={COL_OBJECT_ID: str, COL_EIN: str})
    df[COL_EIN] = df[COL_EIN].str.strip().str.replace(r"\D", "", regex=True)
    df[COL_OBJECT_ID] = df[COL_OBJECT_ID].str.strip()
    df[COL_BATCH_ID] = df[COL_BATCH_ID].str.strip()
    return df


def ordered_columns(cols: list) -> list:
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
    return sorted(cols, key=sort_key)


def label_for(key: str) -> str:
    return FIELD_LABELS.get(key, key)


# ---------------------------------------------------------------------------
# ZIP extraction
# ---------------------------------------------------------------------------

def extract_zips(uploaded_zips, required_object_ids: set) -> tuple:
    """
    Extract only the required XML files from uploaded ZIPs into a temp dir.
    Returns (file_map, errors)
      file_map: {object_id: bytes}
      errors:   list of error strings
    """
    file_map = {}
    errors = []

    for uploaded_zip in uploaded_zips:
        try:
            with zipfile.ZipFile(io.BytesIO(uploaded_zip.read())) as zf:
                for name in zf.namelist():
                    if not name.endswith("_public.xml"):
                        continue
                    obj_id = os.path.basename(name).replace("_public.xml", "")
                    if obj_id in required_object_ids:
                        file_map[obj_id] = zf.read(name)
        except zipfile.BadZipFile:
            errors.append(f"{uploaded_zip.name}: not a valid ZIP file")
        except Exception as e:
            errors.append(f"{uploaded_zip.name}: {e}")

    return file_map, errors


# ---------------------------------------------------------------------------
# Streamlit UI
# ---------------------------------------------------------------------------

st.set_page_config(page_title="990 Foundation Explorer", layout="wide")
st.title("990 Foundation Explorer")
st.caption(
    "Upload an index CSV and paste EINs to find out which ZIPs you need, "
    "then upload only those ZIPs to extract and export the data."
)

# Session state
for key, default in [
    ("index_df", None),
    ("filings", None),
    ("required_batches", None),
    ("file_map", {}),
    ("parsed_df", None),
    ("all_columns", []),
    ("_zip_names_key", None),
]:
    if key not in st.session_state:
        st.session_state[key] = default

# ---------------------------------------------------------------------------
# Step 1: Index CSV + EINs
# ---------------------------------------------------------------------------
st.header("Step 1 — Index CSV & EINs")

col_a, col_b = st.columns(2)

with col_a:
    uploaded_index = st.file_uploader(
        "Index CSV",
        type="csv",
        help="Must contain columns: EIN, OBJECT_ID (clean integer), XML_BATCH_ID",
    )

with col_b:
    ein_input = st.text_area(
        "EINs (one per line, dashes optional)",
        height=200,
        placeholder="237125454\n330759830\n933607439",
    )

eins_cleaned = [re.sub(r"\D", "", e.strip()) for e in ein_input.splitlines() if e.strip()]
eins_cleaned = [e for e in eins_cleaned if e]
seen = set()
eins_deduped = []
for e in eins_cleaned:
    if e not in seen:
        seen.add(e)
        eins_deduped.append(e)

if eins_deduped and len(eins_cleaned) != len(eins_deduped):
    st.warning(f"{len(eins_cleaned) - len(eins_deduped)} duplicate EIN(s) removed.")

if uploaded_index and eins_deduped:
    if st.button("Look Up EINs", type="primary"):
        try:
            index_df = load_index(uploaded_index)
        except Exception as e:
            st.error(f"Failed to load index CSV: {e}")
            st.stop()

        filings = index_df[index_df[COL_EIN].isin(set(eins_deduped))].copy()

        not_found = [e for e in eins_deduped if e not in filings[COL_EIN].values]
        if not_found:
            st.warning(f"{len(not_found)} EIN(s) not in index: {', '.join(not_found)}")

        if filings.empty:
            st.error("None of the supplied EINs were found in the index CSV.")
            st.stop()

        st.session_state.index_df = index_df
        st.session_state.filings = filings
        st.session_state.required_batches = sorted(filings[COL_BATCH_ID].unique())
        # Reset downstream state when EINs/index change
        st.session_state.file_map = {}
        st.session_state.parsed_df = None
        st.session_state.all_columns = []
        st.session_state._zip_names_key = None
else:
    if not uploaded_index:
        st.info("Upload an index CSV to continue.")
    elif not eins_deduped:
        st.info("Enter at least one EIN to continue.")

# ---------------------------------------------------------------------------
# Step 2: Upload ZIPs
# ---------------------------------------------------------------------------
if st.session_state.required_batches is not None:
    filings = st.session_state.filings
    required_batches = st.session_state.required_batches
    n_filings = len(filings)
    n_eins = filings[COL_EIN].nunique()

    st.header("Step 2 — Upload ZIP Files")
    st.success(
        f"Found {n_filings} filing(s) across {n_eins} EIN(s). "
        f"You need to upload **{len(required_batches)}** ZIP file(s):"
    )
    for batch in required_batches:
        n = len(filings[filings[COL_BATCH_ID] == batch])
        st.markdown(f"- `{batch}.zip` — {n} filing(s)")

    uploaded_zips = st.file_uploader(
        "Upload the ZIP file(s) listed above",
        type="zip",
        accept_multiple_files=True,
        help="Each ZIP should contain {OBJECT_ID}_public.xml files in a flat structure. Max 500MB per file.",
    )

    if uploaded_zips:
        zip_names_key = tuple(sorted(f.name for f in uploaded_zips))
        if zip_names_key != st.session_state._zip_names_key:
            required_object_ids = set(st.session_state.filings[COL_OBJECT_ID].astype(str))
            with st.spinner(f"Extracting {len(uploaded_zips)} ZIP file(s)…"):
                file_map, zip_errors = extract_zips(uploaded_zips, required_object_ids)
            st.session_state.file_map = file_map
            st.session_state._zip_names_key = zip_names_key
            # Reset parse state
            st.session_state.parsed_df = None
            st.session_state.all_columns = []
            if zip_errors:
                for e in zip_errors:
                    st.error(e)

        file_map = st.session_state.file_map
        if file_map:
            st.caption(f"{len(file_map):,} matching XML file(s) extracted from uploaded ZIPs.")

            # Warn about any batches not yet covered by uploaded ZIPs
            uploaded_zip_names = {f.name.replace(".zip", "") for f in uploaded_zips}
            missing_batches = [b for b in required_batches if b not in uploaded_zip_names]
            if missing_batches:
                st.warning(
                    f"ZIPs not yet uploaded for: {', '.join(missing_batches)}. "
                    "Filings from those batches will show as missing."
                )

# ---------------------------------------------------------------------------
# Step 3: Parse
# ---------------------------------------------------------------------------
if st.session_state.file_map:
    st.header("Step 3 — Parse")

    if st.button("Parse Files", type="primary"):
        filings = st.session_state.filings
        index_df = st.session_state.index_df
        file_map = st.session_state.file_map
        all_rows = []
        file_errors = []
        total = len(filings)

        progress = st.progress(0, text="Starting…")
        status = st.empty()

        for i, (_, idx_row) in enumerate(filings.iterrows()):
            ein = idx_row[COL_EIN]
            obj_id = str(idx_row[COL_OBJECT_ID]).strip()
            filename = f"{obj_id}_public.xml"

            status.text(f"Parsing {filename} ({i + 1}/{total})…")

            if obj_id not in file_map:
                file_errors.append({"EIN": ein, "File": filename, "Error": "Not found in uploaded ZIPs"})
                all_rows.append({
                    "ReturnHeader.Filer.EIN": ein,
                    "_source_file": filename,
                    "_parse_error": "File not found in uploaded ZIPs",
                })
            else:
                rows = parse_xml_bytes(file_map[obj_id], filename)
                for r in rows:
                    r.setdefault("ReturnHeader.Filer.EIN", ein)
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

        success = total - len(file_errors)
        st.success(f"{success}/{total} files parsed successfully, {len(file_errors)} error(s).")
        if file_errors:
            with st.expander(f"⚠️ {len(file_errors)} file error(s)"):
                st.dataframe(pd.DataFrame(file_errors), use_container_width=True)

# ---------------------------------------------------------------------------
# Step 4: Field selection
# ---------------------------------------------------------------------------
if st.session_state.all_columns:
    df_full = st.session_state.parsed_df
    all_cols = st.session_state.all_columns

    internal_cols = [c for c in all_cols if c.startswith("_")]
    data_cols = [c for c in all_cols if not c.startswith("_")]

    st.header("Step 4 — Select Fields")
    st.write(
        f"{len(data_cols)} fields found across all parsed files. "
        "Uncheck anything you don't need in the export."
    )

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

    if "_parse_error" in internal_cols and df_full["_parse_error"].notna().any():
        selected_cols.append("_parse_error")

    # ---------------------------------------------------------------------------
    # Step 5: Export
    # ---------------------------------------------------------------------------
    st.header("Step 5 — Export")

    if not selected_cols:
        st.warning("Select at least one field above.")
    else:
        df_export = df_full[[c for c in selected_cols if c in df_full.columns]].copy()
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
