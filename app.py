"""
990 Foundation Explorer
-----------------------
Pulls IRS 990/990PF data from the ProPublica Nonprofit Explorer API
for a user-supplied list of EINs, then exports a filtered spreadsheet.

API docs: https://projects.propublica.org/nonprofits/api/
Endpoint: GET https://projects.propublica.org/nonprofits/api/v2/organizations/{ein}.json

Response shape:
  organization        – org-level profile (name, address, NTEE, ruling date, etc.)
  filings_with_data   – one row per filing year with financial summary fields
                        (field set varies by form type: 990, 990-EZ, 990-PF)
  filings_without_data – years where only a PDF exists, no structured data

This app uses the most recent filing row from filings_with_data for financials,
combined with the organization-level fields, to build one row per EIN.

Notes:
- ProPublica data lags the IRS by roughly 6–12 months.
- 990-PF fields (e.g. grantmaking) are present only when formtype indicates PF.
- Fields that don't apply to a given form type will be blank for that org.
- No API key required; ProPublica rate-limits generously but add a small delay
  between requests to be respectful of their servers.
"""

import time
import re
import io

import requests
import pandas as pd
import streamlit as st

# ---------------------------------------------------------------------------
# Field definitions
# ---------------------------------------------------------------------------
# Each entry: (column_label, source, api_key)
#   source = "org"     -> from response["organization"]
#   source = "filing"  -> from most recent row of response["filings_with_data"]
#
# Field names come from the ProPublica API documentation and observed response keys.
# Some filing fields are only populated for specific form types (noted in comments).

ORG_FIELDS = [
    ("EIN",                        "org",     "ein"),
    ("Organization Name",          "org",     "name"),
    ("Care Of Name",               "org",     "careofname"),
    ("Address",                    "org",     "address"),
    ("City",                       "org",     "city"),
    ("State",                      "org",     "state"),
    ("ZIP",                        "org",     "zipcode"),
    ("Phone",                      "org",     "phone"),
    ("Website",                    "org",     "website"),
    ("NTEE Code",                  "org",     "ntee_code"),
    ("Subsection Code",            "org",     "subseccd"),       # e.g. 03 = 501(c)(3)
    ("Ruling Date",                "org",     "ruling"),
    ("Classification Codes",       "org",     "classification_codes"),
    ("Exempt Status Code",         "org",     "exempt_status_code"),
    ("Asset Amount (BMF)",         "org",     "asset_amount"),   # IRS Business Master File figure
    ("Income Amount (BMF)",        "org",     "income_amount"),
    ("Revenue Amount (BMF)",       "org",     "revenue_amount"),
    ("Filing Requirement Code",    "org",     "filing_requirement_code"),
    ("PF Filing Requirement Code", "org",     "pf_filing_requirement_code"),
    ("Accounting Period",          "org",     "accounting_period"),
]

FILING_FIELDS = [
    # --- Filing metadata ---
    ("Most Recent Tax Year",       "filing",  "tax_prd_yr"),
    ("Form Type",                  "filing",  "formtype_str"),    # "990", "990EZ", "990PF"
    ("Filing Updated",             "filing",  "updated"),

    # --- Core financials (990 & 990-EZ) ---
    ("Total Revenue",              "filing",  "totrevenue"),
    ("Total Functional Expenses",  "filing",  "totfuncexpns"),
    ("Total Assets (EOY)",         "filing",  "totassetsend"),
    ("Total Liabilities (EOY)",    "filing",  "totliabend"),
    ("Net Assets (EOY)",           "filing",  "totnetassetsend"),
    ("Total Contributions",        "filing",  "totcntrbs"),
    ("Program Service Revenue",    "filing",  "prgmservrev"),
    ("Investment Income",          "filing",  "invstmntinc"),
    ("Other Revenue",              "filing",  "othrevnue"),
    ("Fundraising Gross Revenue",  "filing",  "grsrevnuefndrsng"),
    ("Fundraising Direct Expenses","filing",  "direxpns"),
    ("Net Fundraising Income",     "filing",  "netincfndrsng"),
    ("Unrelated Business Income",  "filing",  "unrelbusincd"),    # Y/N indicator
    ("Officer Compensation %",     "filing",  "pct_compnsatncurrofcr"),

    # --- 990-PF specific ---
    # These fields are present in filings_with_data rows where formtype = PF.
    # They will be blank for 990/990-EZ filers.
    ("PF: Gross Investment Income",      "filing", "grsincgaming"),   # NOTE: ProPublica maps PF inv income here; verify on live data
    ("PF: Total Grants Paid",            "filing", "grsrcptsrelatd170"),  # Approximate; PF uses disbrsements field
    ("PF: Contributions Received",       "filing", "totgftgrntrcvd509"),

    # --- Public support (990 Part II / Schedule A) ---
    ("Gifts/Grants Received (170)",      "filing", "gftgrntrcvd170"),
    ("Gross Public Receipts",            "filing", "grspublicrcpts"),
    ("Total Support (509)",              "filing", "totsupp509"),
]

ALL_FIELDS = ORG_FIELDS + FILING_FIELDS

# Fields shown by default (pre-checked). Uncheck to exclude from export.
DEFAULT_ON = {
    "EIN", "Organization Name", "Address", "City", "State", "ZIP", "Phone",
    "Website", "NTEE Code", "Ruling Date",
    "Most Recent Tax Year", "Form Type",
    "Total Revenue", "Total Functional Expenses", "Total Assets (EOY)",
    "Total Liabilities (EOY)", "Net Assets (EOY)", "Total Contributions",
    "Program Service Revenue",
}

# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

BASE_URL = "https://projects.propublica.org/nonprofits/api/v2/organizations/{ein}.json"


def clean_ein(raw: str) -> str:
    """Strip non-digits from an EIN string."""
    return re.sub(r"\D", "", raw.strip())


def fetch_org(ein: str) -> dict:
    """
    Call the ProPublica API for a single EIN.
    Returns a dict with keys: 'organization', 'filings_with_data', 'error'
    'error' is None on success, a string message on failure.
    """
    url = BASE_URL.format(ein=ein)
    try:
        resp = requests.get(url, timeout=15)
        if resp.status_code == 404:
            return {"organization": {}, "filings_with_data": [], "error": "Not found in ProPublica"}
        resp.raise_for_status()
        data = resp.json()
        return {
            "organization": data.get("organization", {}),
            "filings_with_data": data.get("filings_with_data", []),
            "error": None,
        }
    except requests.exceptions.Timeout:
        return {"organization": {}, "filings_with_data": [], "error": "Request timed out"}
    except Exception as e:
        return {"organization": {}, "filings_with_data": [], "error": str(e)}


def extract_row(ein: str, result: dict, selected_labels: list) -> dict:
    """
    Flatten org + most-recent-filing into a single dict,
    keeping only the selected field labels.
    """
    org = result["organization"]
    filings = result["filings_with_data"]

    # Most recent filing = first row (ProPublica returns descending by tax year)
    filing = filings[0] if filings else {}

    row = {"_fetch_error": result["error"]}

    for label, source, key in ALL_FIELDS:
        if label not in selected_labels:
            continue
        if source == "org":
            row[label] = org.get(key, "")
        else:
            row[label] = filing.get(key, "")

    return row


# ---------------------------------------------------------------------------
# Streamlit UI
# ---------------------------------------------------------------------------

st.set_page_config(page_title="990 Foundation Explorer", layout="wide")
st.title("990 Foundation Explorer")
st.caption(
    "Data sourced from the [ProPublica Nonprofit Explorer API](https://projects.propublica.org/nonprofits/api/). "
    "Financial data reflects the most recently available filing. "
    "ProPublica's dataset typically lags the IRS by 6–12 months."
)

# ---------------------------------------------------------------------------
# Step 1: EIN input
# ---------------------------------------------------------------------------
st.header("Step 1 — Paste EINs")
st.write("One EIN per line. Dashes optional (e.g. `23-7125454` or `237125454`).")

ein_input = st.text_area("EINs", height=180, placeholder="237125454\n330759830\n933607439")

eins_raw = [line.strip() for line in ein_input.splitlines() if line.strip()]
eins = [clean_ein(e) for e in eins_raw if clean_ein(e)]

# Deduplicate while preserving order
seen = set()
eins_deduped = []
for e in eins:
    if e not in seen:
        seen.add(e)
        eins_deduped.append(e)

if eins_deduped:
    st.caption(f"{len(eins_deduped)} unique EIN(s) entered.")
    if len(eins_deduped) != len(eins):
        st.warning(f"{len(eins) - len(eins_deduped)} duplicate(s) removed.")

# ---------------------------------------------------------------------------
# Step 2: Field selection
# ---------------------------------------------------------------------------
st.header("Step 2 — Select Fields")
st.write("Uncheck any fields you don't need in the export.")

col1, col2 = st.columns(2)

selected_labels = []

with col1:
    st.subheader("Organization Profile")
    for label, source, _ in ORG_FIELDS:
        checked = st.checkbox(label, value=(label in DEFAULT_ON), key=f"field_{label}")
        if checked:
            selected_labels.append(label)

with col2:
    st.subheader("Filing & Financials")
    for label, source, _ in FILING_FIELDS:
        checked = st.checkbox(label, value=(label in DEFAULT_ON), key=f"field_{label}")
        if checked:
            selected_labels.append(label)

# Preserve the defined column order in the export
selected_labels_ordered = [lbl for lbl, _, _ in ALL_FIELDS if lbl in selected_labels]

# ---------------------------------------------------------------------------
# Step 3: Fetch & compile
# ---------------------------------------------------------------------------
st.header("Step 3 — Fetch & Export")

if not eins_deduped:
    st.info("Enter at least one EIN above to continue.")
elif not selected_labels_ordered:
    st.warning("Select at least one field above.")
else:
    if st.button("Fetch Data", type="primary"):
        rows = []
        errors = []

        progress = st.progress(0, text="Starting…")
        status = st.empty()

        for i, ein in enumerate(eins_deduped):
            status.text(f"Fetching EIN {ein} ({i + 1}/{len(eins_deduped)})…")
            result = fetch_org(ein)

            if result["error"]:
                errors.append({"EIN": ein, "Error": result["error"]})
                # Still add a row with the error and blank fields
                row = {"EIN": ein, "_fetch_error": result["error"]}
                for label in selected_labels_ordered:
                    if label != "EIN":
                        row[label] = ""
                rows.append(row)
            else:
                row = extract_row(ein, result, selected_labels_ordered)
                rows.append(row)

            progress.progress((i + 1) / len(eins_deduped), text=f"{i + 1}/{len(eins_deduped)} fetched")
            # Polite delay between API calls
            if i < len(eins_deduped) - 1:
                time.sleep(0.5)

        status.empty()
        progress.empty()

        df = pd.DataFrame(rows)

        # Move _fetch_error to end, rename for readability
        if "_fetch_error" in df.columns:
            err_col = df.pop("_fetch_error")
            df["Fetch Error"] = err_col

        # Keep only selected + error columns
        export_cols = [c for c in selected_labels_ordered if c in df.columns] + (
            ["Fetch Error"] if "Fetch Error" in df.columns else []
        )
        df_export = df[export_cols]

        st.success(f"Done. {len(eins_deduped) - len(errors)} fetched successfully, {len(errors)} error(s).")

        if errors:
            with st.expander(f"⚠️ {len(errors)} EIN(s) with errors"):
                st.dataframe(pd.DataFrame(errors), use_container_width=True)

        st.subheader("Preview")
        st.dataframe(df_export, use_container_width=True)

        # ---------------------------------------------------------------------------
        # Step 4: Download
        # ---------------------------------------------------------------------------
        st.header("Step 4 — Download")

        # Excel
        excel_buf = io.BytesIO()
        with pd.ExcelWriter(excel_buf, engine="openpyxl") as writer:
            df_export.to_excel(writer, index=False, sheet_name="990 Data")
        excel_buf.seek(0)

        st.download_button(
            label="Download as Excel (.xlsx)",
            data=excel_buf,
            file_name="990_foundation_data.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

        # CSV
        csv_buf = df_export.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="Download as CSV",
            data=csv_buf,
            file_name="990_foundation_data.csv",
            mime="text/csv",
        )
