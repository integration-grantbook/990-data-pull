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

Flow:
  Step 1 – Paste EINs
  Step 2 – Fetch all EINs; raw data stored in session state
  Step 3 – Select which fields to keep (derived from actual API response keys)
  Step 4 – Preview + download

Notes:
- ProPublica data lags the IRS by roughly 6–12 months.
- 990-PF fields are present only when formtype indicates PF; blank otherwise.
- No API key required; 0.5s delay between requests out of courtesy.
"""

import time
import re
import io

import requests
import pandas as pd
import streamlit as st

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BASE_URL = "https://projects.propublica.org/nonprofits/api/v2/organizations/{ein}.json"

# Human-readable labels for known API keys.
# Any key not listed here is displayed as the raw key name.
KEY_LABELS = {
    "ein": "EIN",
    "name": "Organization Name",
    "careofname": "Care Of Name",
    "address": "Address",
    "city": "City",
    "state": "State",
    "zipcode": "ZIP",
    "phone": "Phone",
    "website": "Website",
    "ntee_code": "NTEE Code",
    "subseccd": "Subsection Code",
    "ruling": "Ruling Date",
    "classification_codes": "Classification Codes",
    "exempt_status_code": "Exempt Status Code",
    "asset_amount": "Asset Amount (BMF)",
    "income_amount": "Income Amount (BMF)",
    "revenue_amount": "Revenue Amount (BMF)",
    "filing_requirement_code": "Filing Requirement Code",
    "pf_filing_requirement_code": "PF Filing Requirement Code",
    "accounting_period": "Accounting Period",
    "tax_prd_yr": "Most Recent Tax Year",
    "tax_prd": "Tax Period",
    "formtype_str": "Form Type",
    "updated": "Filing Updated",
    "totrevenue": "Total Revenue",
    "totfuncexpns": "Total Functional Expenses",
    "totassetsend": "Total Assets (EOY)",
    "totliabend": "Total Liabilities (EOY)",
    "totnetassetsend": "Net Assets (EOY)",
    "totcntrbs": "Total Contributions",
    "prgmservrev": "Program Service Revenue",
    "invstmntinc": "Investment Income",
    "othrevnue": "Other Revenue",
    "grsrevnuefndrsng": "Fundraising Gross Revenue",
    "direxpns": "Fundraising Direct Expenses",
    "netincfndrsng": "Net Fundraising Income",
    "unrelbusincd": "Unrelated Business Income (Y/N)",
    "unrelbusinccd": "Unrelated Business Income Code",
    "pct_compnsatncurrofcr": "Officer Compensation %",
    "compnsatncurrofcr": "Officer Compensation",
    "othrsalwages": "Other Salaries & Wages",
    "payrolltx": "Payroll Tax",
    "profndraising": "Professional Fundraising",
    "totexcessyr": "Excess/Deficit for Year",
    "othrchgsnetassetfnd": "Other Changes in Net Assets",
    "initiationfee": "Initiation Fees",
    "grsincgaming": "Gross Gaming Income",
    "gftgrntrcvd170": "Gifts/Grants Received (170)",
    "gftgrntsrcvd170": "Gifts/Grants Received (170b)",
    "grspublicrcpts": "Gross Public Receipts",
    "totsupp509": "Total Support (509)",
    "totgftgrntrcvd509": "Total Gifts/Grants (509)",
    "grsrcptsrelatd170": "Gross Receipts Related (170)",
    "grsrcptsrelated170": "Gross Receipts Related (170b)",
    "grsrcptsadmiss509": "Gross Receipts Admissions (509)",
    "grsrcptsadmissn509": "Gross Receipts Admissions (509b)",
    "nonpfrea": "Non-PF Reason Code",
    "txrevnuelevied170": "Tax Revenue Levied (170)",
    "txrevnuelevied509": "Tax Revenue Levied (509)",
    "srvcsval170": "Services Value (170)",
    "srvcsval509": "Services Value (509)",
    "grsinc170": "Gross Income (170)",
    "grsincmembers": "Gross Income from Members",
    "grsincother": "Gross Income Other",
    "totcntrbgfts": "Total Contributions & Gifts",
    "totprgmrevnue": "Total Program Revenue",
    "txexmptbndsproceeds": "Tax-Exempt Bond Proceeds",
    "txexmptbndsend": "Tax-Exempt Bonds (EOY)",
    "royaltsinc": "Royalties Income",
    "grsrntsreal": "Gross Rents (Real)",
    "grsrntsprsnl": "Gross Rents (Personal)",
    "rntlexpnsreal": "Rental Expenses (Real)",
    "rntlexpnsprsnl": "Rental Expenses (Personal)",
    "rntlincreal": "Rental Income (Real)",
    "rntlincprsnl": "Rental Income (Personal)",
    "netrntlinc": "Net Rental Income",
    "grsalesecur": "Gross Sales (Securities)",
    "grsalesothr": "Gross Sales (Other)",
    "cstbasisecur": "Cost Basis (Securities)",
    "cstbasisothr": "Cost Basis (Other)",
    "gnlsecur": "Gain/Loss (Securities)",
    "gnlsothr": "Gain/Loss (Other)",
    "netgnls": "Net Gains/Losses",
    "grsincfndrsng": "Gross Fundraising Income",
    "lessdirfndrsng": "Less Direct Fundraising",
    "lessdirgaming": "Less Direct Gaming",
    "netincgaming": "Net Gaming Income",
    "grsalesinvent": "Gross Sales (Inventory)",
    "lesscstofgoods": "Less Cost of Goods",
    "netincsales": "Net Income from Sales",
    "miscrevtot11e": "Miscellaneous Revenue",
    "secrdmrtgsend": "Secured Mortgages (EOY)",
    "unsecurednotesend": "Unsecured Notes (EOY)",
    "retainedearnend": "Retained Earnings (EOY)",
    "totnetassetend": "Total Net Assets (EOY)",
    "grsalesminusret": "Gross Sales Minus Returns",
    "costgoodsold": "Cost of Goods Sold",
    "grsprft": "Gross Profit",
    "duesassesmnts": "Dues & Assessments",
    "othrinvstinc": "Other Investment Income",
    "grsamtsalesastothr": "Gross Amount Sales (Other Assets)",
    "basisalesexpnsothr": "Basis/Sales Expenses (Other)",
    "gnsaleofastothr": "Gain on Sale (Other Assets)",
    "subtotsuppinc509": "Subtotal Support Income (509)",
    "totgftgrntrcvd170": "Total Gifts/Grants (170)",
}

# Keys checked by default in Step 3
DEFAULT_CHECKED_KEYS = {
    "ein", "name", "address", "city", "state", "zipcode", "phone", "website",
    "ntee_code", "ruling",
    "tax_prd_yr", "formtype_str",
    "totrevenue", "totfuncexpns", "totassetsend", "totliabend", "totnetassetsend",
    "totcntrbs", "prgmservrev",
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def clean_ein(raw: str) -> str:
    """Strip non-digits from an EIN string."""
    return re.sub(r"\D", "", raw.strip())


def fetch_org(ein: str) -> dict:
    """
    Call the ProPublica API for one EIN.
    Returns: {'organization': dict, 'filings_with_data': list, 'error': str|None}
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


def flatten_result(result: dict) -> dict:
    """
    Merge org fields and most-recent filing fields into one flat dict.
    Org keys overwrite filing keys on collision (org data is more authoritative
    for shared fields like 'ein').
    """
    org = result.get("organization", {})
    filings = result.get("filings_with_data", [])
    filing = filings[0] if filings else {}

    flat = {**filing, **org}  # org wins on collision
    flat["_error"] = result.get("error")
    return flat


def collect_all_keys(raw_results: dict) -> list:
    """
    Return an ordered list of all non-internal keys that appear across all results.
    Org-level keys come first (in first-seen order), then filing-only keys.
    """
    org_keys = []
    filing_keys = []
    org_keys_set = set()
    filing_keys_set = set()

    for ein, result in raw_results.items():
        if result["error"]:
            continue
        org = result.get("organization", {})
        filings = result.get("filings_with_data", [])
        filing = filings[0] if filings else {}

        for k in org:
            if k not in org_keys_set:
                org_keys_set.add(k)
                org_keys.append(k)
        for k in filing:
            if k not in org_keys_set and k not in filing_keys_set:
                filing_keys_set.add(k)
                filing_keys.append(k)

    return org_keys + filing_keys


def label_for(key: str) -> str:
    return KEY_LABELS.get(key, key)


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

if "raw_results" not in st.session_state:
    st.session_state.raw_results = {}
if "all_keys" not in st.session_state:
    st.session_state.all_keys = []

# ---------------------------------------------------------------------------
# Step 1: EIN input
# ---------------------------------------------------------------------------
st.header("Step 1 — Paste EINs")
st.write("One EIN per line. Dashes optional (e.g. `23-7125454` or `237125454`).")

ein_input = st.text_area("EINs", height=180, placeholder="237125454\n330759830\n933607439")

eins_raw = [line.strip() for line in ein_input.splitlines() if line.strip()]
eins = [clean_ein(e) for e in eins_raw if clean_ein(e)]

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
# Step 2: Fetch
# ---------------------------------------------------------------------------
st.header("Step 2 — Fetch Data")

if not eins_deduped:
    st.info("Enter at least one EIN above to continue.")
else:
    if st.button("Fetch Data", type="primary"):
        raw_results = {}
        progress = st.progress(0, text="Starting…")
        status = st.empty()

        for i, ein in enumerate(eins_deduped):
            status.text(f"Fetching EIN {ein} ({i + 1}/{len(eins_deduped)})…")
            raw_results[ein] = fetch_org(ein)
            progress.progress((i + 1) / len(eins_deduped), text=f"{i + 1}/{len(eins_deduped)} fetched")
            if i < len(eins_deduped) - 1:
                time.sleep(0.5)

        status.empty()
        progress.empty()

        st.session_state.raw_results = raw_results
        st.session_state.all_keys = collect_all_keys(raw_results)

        errors = {ein: r["error"] for ein, r in raw_results.items() if r["error"]}
        st.success(f"{len(raw_results) - len(errors)} fetched successfully, {len(errors)} error(s).")
        if errors:
            with st.expander(f"⚠️ {len(errors)} EIN(s) with errors"):
                st.dataframe(
                    pd.DataFrame([{"EIN": k, "Error": v} for k, v in errors.items()]),
                    use_container_width=True,
                )

# ---------------------------------------------------------------------------
# Step 3: Field selection — only shown after a successful fetch
# ---------------------------------------------------------------------------
if st.session_state.all_keys:
    st.header("Step 3 — Select Fields")
    st.write(
        f"{len(st.session_state.all_keys)} fields found across your EINs. "
        "Uncheck anything you don't need in the export."
    )

    all_keys = st.session_state.all_keys
    mid = (len(all_keys) + 1) // 2
    col1, col2 = st.columns(2)
    selected_keys = []

    for i, key in enumerate(all_keys):
        col = col1 if i < mid else col2
        checked = col.checkbox(
            label_for(key),
            value=(key in DEFAULT_CHECKED_KEYS),
            key=f"field_{key}",
        )
        if checked:
            selected_keys.append(key)

    # ---------------------------------------------------------------------------
    # Step 4: Preview + download
    # ---------------------------------------------------------------------------
    st.header("Step 4 — Export")

    if not selected_keys:
        st.warning("Select at least one field above.")
    else:
        rows = []
        for ein, result in st.session_state.raw_results.items():
            flat = flatten_result(result)
            row = {label_for(k): flat.get(k, "") for k in selected_keys}
            row["Fetch Error"] = flat.get("_error") or ""
            rows.append(row)

        df = pd.DataFrame(rows)

        # Move Fetch Error to end; drop it entirely if no errors
        err_col = df.pop("Fetch Error")
        if err_col.ne("").any():
            df["Fetch Error"] = err_col

        st.subheader("Preview")
        st.dataframe(df, use_container_width=True)

        excel_buf = io.BytesIO()
        with pd.ExcelWriter(excel_buf, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="990 Data")
        excel_buf.seek(0)

        dl_col1, dl_col2 = st.columns(2)
        dl_col1.download_button(
            label="Download as Excel (.xlsx)",
            data=excel_buf,
            file_name="990_foundation_data.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        dl_col2.download_button(
            label="Download as CSV",
            data=df.to_csv(index=False).encode("utf-8"),
            file_name="990_foundation_data.csv",
            mime="text/csv",
        )
