"""
app.py
BounceGuard by ContractorFlow

Standalone Email Deliverability & DNS MX Validator
Updated: Safer language, stronger junk traps, better risk categories, typo detection,
null MX handling, disposable/domain trap expansion, and clearer "domain verified"
vs. "mailbox verified" messaging.

Important:
This tool validates email syntax, traps obvious junk, checks known-risk patterns, and verifies
whether a domain appears configured to receive mail. It does NOT guarantee that a specific
mailbox exists unless you add a true third-party verification API or SMTP recipient probing.
"""

import streamlit as st
import pandas as pd
import asyncio
import aiohttp
import re
import io
import math
import os
import hashlib
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple


# ============================================================
# PAGE SETUP
# ============================================================

st.set_page_config(
    page_title="BounceGuard | ContractorFlow",
    page_icon="🛡️",
    layout="wide"
)


# ============================================================
# BRANDING
# ============================================================

col_logo, col_title = st.columns([1, 4])
with col_logo:
    if os.path.exists("logo.png"):
        st.image("logo.png", use_column_width=True)
    else:
        st.markdown("<h1>🛡️</h1>", unsafe_allow_html=True)

with col_title:
    st.title("BounceGuard")
    st.caption("Protect your sender reputation. Powered by ContractorFlow.")


# ============================================================
# CONSTANTS
# ============================================================

STATUS_EMPTY = "⚪ Empty"
STATUS_DOMAIN_VERIFIED = "✅ Domain Verified"
STATUS_HIGH_RISK = "🚨 Invalid / High Bounce Risk"
STATUS_ROLE_BASED = "⚠️ Role-Based Address"
STATUS_CATCH_ALL_RISK = "⚠️ Catch-All / Unverifiable Domain"
STATUS_UNKNOWN = "❔ Unknown / Needs Review"
STATUS_TYPO = "⚠️ Likely Domain Typo"
STATUS_DISPOSABLE = "🚨 Disposable / Temporary Email"
STATUS_NO_MX = "🚨 Domain Cannot Receive Email"
STATUS_PENDING = "PENDING"

# These local parts are usually placeholders, test records, fake records, or junk CRM values.
FAKE_LOCAL_PARTS = {
    "test", "testing", "something", "anything", "fake", "email", "noemail",
    "donotemail", "do-not-email", "spam", "customer", "client", "na", "n/a",
    "none", "null", "unknown", "unkown", "no", "nobody", "blank", "placeholder",
    "example", "sample", "demo", "asdf", "qwerty", "xxx", "x"
}

# If any of these terms appear anywhere in the full email string, treat it as high-risk junk.
# This intentionally catches unknown@unknown.com, unkown@unknown.com, john.unknown@gmail.com, etc.
JUNK_TERMS_ANYWHERE = {
    "unknown", "unkown", "noemail", "donotemail", "do-not-email", "noreply",
    "no-reply", "notprovided", "not-provided", "notavailable", "not-available",
    "none", "null", "placeholder"
}

# These are not automatically bad, but they usually represent a department inbox rather than a person.
GENERIC_EMAIL_PREFIXES = {
    "info", "admin", "sales", "support", "contact", "hello", "office",
    "service", "customerservice", "customer.service", "billing", "accounts",
    "marketing", "webmaster", "help", "team", "jobs", "careers", "hr"
}

# Exact junk emails often seen in CRMs.
FAKE_EMAILS_FULL = {
    "na@na.com",
    "n/a@n/a.com",
    "none@none.com",
    "na@gmail.com",
    "none@gmail.com",
    "test@test.com",
    "testing@test.com",
    "email@email.com",
    "no@email.com",
    "unknown@unknown.com",
    "unkown@unknown.com",
    "unknown@gmail.com",
    "unkown@gmail.com",
    "noemail@noemail.com",
    "donotemail@donotemail.com",
}

# Disposable/temp domains. This is not exhaustive, but it catches common offenders.
# For production-grade validation, replace or supplement this with a maintained disposable-domain list/API.
DISPOSABLE_DOMAIN_KEYWORDS = {
    "mailinator", "yopmail", "tempmail", "10minute", "guerrillamail",
    "sharklasers", "throwawaymail", "trashmail", "getnada", "dispostable",
    "fakeinbox", "mintemail", "maildrop", "moakt", "temp-mail", "tempmailo",
    "emailondeck", "burnermail", "spamgourmet"
}

# Suspicious domains that are almost never valid customer contact domains.
SUSPECT_DOMAIN_PATTERN = re.compile(
    r"^(fake|demo|test|mock|example|sample|unknown|unkown)(\.|-|$)",
    re.IGNORECASE
)

# Domains that are commonly real and high-volume.
# This does NOT prove an individual mailbox exists.
KNOWN_CONSUMER_DOMAINS = {
    "gmail.com", "googlemail.com", "yahoo.com", "hotmail.com", "aol.com",
    "outlook.com", "live.com", "icloud.com", "comcast.net", "msn.com",
    "sbcglobal.net", "att.net", "verizon.net", "mac.com", "me.com",
    "bellsouth.net", "charter.net", "proton.me", "protonmail.com"
}

# Common typo domains mapped to the likely intended domain.
COMMON_DOMAIN_TYPOS = {
    "gamil.com": "gmail.com",
    "gmial.com": "gmail.com",
    "gmai.com": "gmail.com",
    "gmail.co": "gmail.com",
    "gmail.con": "gmail.com",
    "gmail.comm": "gmail.com",
    "gnail.com": "gmail.com",
    "hotmial.com": "hotmail.com",
    "hotmai.com": "hotmail.com",
    "hotmail.co": "hotmail.com",
    "hotmal.com": "hotmail.com",
    "outlok.com": "outlook.com",
    "outloo.com": "outlook.com",
    "outlook.co": "outlook.com",
    "yaho.com": "yahoo.com",
    "yahoo.co": "yahoo.com",
    "yahoo.con": "yahoo.com",
    "icloud.co": "icloud.com",
    "iclod.com": "icloud.com",
    "comast.net": "comcast.net",
    "comcast.co": "comcast.net",
    "aol.co": "aol.com",
}

# Better email syntax than the original regex, while still practical for CRM cleanup.
EMAIL_REGEX = re.compile(
    r"^(?!.*\.\.)[A-Z0-9.!#$%&'*+/=?^_`{|}~-]+@"
    r"(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+"
    r"[A-Z]{2,63}$",
    re.IGNORECASE
)


# ============================================================
# DATA CLASSES
# ============================================================

@dataclass
class ValidationResult:
    clean_email: str
    status: str
    reason: str
    recommendation: str
    suggested_fix: str = ""


# ============================================================
# LOCAL VALIDATION ENGINE
# ============================================================

def normalize_email(email) -> str:
    """Normalize a raw email value for validation."""
    if pd.isna(email):
        return ""

    clean = str(email).strip().lower()

    # Remove obvious wrapping characters commonly introduced by copy/paste.
    clean = clean.strip("<>()[]{}\"' ")

    # Remove mailto prefix if pasted from a hyperlink.
    if clean.startswith("mailto:"):
        clean = clean.replace("mailto:", "", 1).strip()

    return clean


def split_email(clean_email: str) -> Tuple[str, str]:
    """Return local part and domain part. Empty strings if invalid split."""
    if clean_email.count("@") != 1:
        return "", ""

    local_part, domain_part = clean_email.split("@", 1)
    return local_part, domain_part


def contains_disposable_domain(domain: str) -> bool:
    """Detect common disposable/temp mail domains by keyword."""
    domain_lower = domain.lower()
    return any(keyword in domain_lower for keyword in DISPOSABLE_DOMAIN_KEYWORDS)


def contains_junk_term_anywhere(clean_email: str) -> Optional[str]:
    """Flag junk terms appearing anywhere in the full email address."""
    compact = clean_email.replace(".", "").replace("_", "").replace("-", "")
    for term in JUNK_TERMS_ANYWHERE:
        compact_term = term.replace(".", "").replace("_", "").replace("-", "")
        if compact_term and compact_term in compact:
            return term
    return None


def format_and_trap_email(email) -> ValidationResult:
    """
    Phase 1:
    Normalize the address, catch obvious invalid values, classify known-risk local parts,
    role-based addresses, known consumer domains, typo domains, and disposable domains.

    Returns PENDING when a DNS check is needed.
    """
    clean_email = normalize_email(email)

    if clean_email == "" or clean_email == "nan":
        return ValidationResult(
            clean_email="",
            status=STATUS_EMPTY,
            reason="No email address was provided.",
            recommendation="No action needed unless this record requires an email address."
        )

    if clean_email in FAKE_EMAILS_FULL:
        return ValidationResult(
            clean_email=clean_email,
            status=STATUS_HIGH_RISK,
            reason="This is a known fake or placeholder email address.",
            recommendation="Suppress this email before sending."
        )

    junk_term = contains_junk_term_anywhere(clean_email)
    if junk_term:
        return ValidationResult(
            clean_email=clean_email,
            status=STATUS_HIGH_RISK,
            reason=f"The email contains the junk placeholder term '{junk_term}'.",
            recommendation="Suppress this email before sending."
        )

    if not EMAIL_REGEX.match(clean_email):
        return ValidationResult(
            clean_email=clean_email,
            status=STATUS_HIGH_RISK,
            reason="The email address does not pass syntax validation.",
            recommendation="Correct the address or suppress it before sending."
        )

    local_part, domain_part = split_email(clean_email)

    if not local_part or not domain_part:
        return ValidationResult(
            clean_email=clean_email,
            status=STATUS_HIGH_RISK,
            reason="The email address could not be split into a valid local part and domain.",
            recommendation="Correct the address or suppress it before sending."
        )

    if local_part in FAKE_LOCAL_PARTS:
        return ValidationResult(
            clean_email=clean_email,
            status=STATUS_HIGH_RISK,
            reason="The local part looks like a fake, test, or placeholder value.",
            recommendation="Suppress this email before sending."
        )

    if re.search(r"\d+bad\d+", local_part) or local_part == "bad":
        return ValidationResult(
            clean_email=clean_email,
            status=STATUS_HIGH_RISK,
            reason="The local part matches a known bad/test pattern.",
            recommendation="Suppress this email before sending."
        )

    if SUSPECT_DOMAIN_PATTERN.search(domain_part):
        return ValidationResult(
            clean_email=clean_email,
            status=STATUS_HIGH_RISK,
            reason="The domain looks like a fake, demo, test, sample, or placeholder domain.",
            recommendation="Suppress this email before sending."
        )

    if contains_disposable_domain(domain_part):
        return ValidationResult(
            clean_email=clean_email,
            status=STATUS_DISPOSABLE,
            reason="The domain appears to be a disposable or temporary email provider.",
            recommendation="Suppress this email from marketing campaigns."
        )

    if domain_part in COMMON_DOMAIN_TYPOS:
        return ValidationResult(
            clean_email=clean_email,
            status=STATUS_TYPO,
            reason=f"The domain looks like a typo. Did you mean {COMMON_DOMAIN_TYPOS[domain_part]}?",
            recommendation="Correct the email address before sending.",
            suggested_fix=f"{local_part}@{COMMON_DOMAIN_TYPOS[domain_part]}"
        )

    if local_part in GENERIC_EMAIL_PREFIXES:
        return ValidationResult(
            clean_email=clean_email,
            status=STATUS_ROLE_BASED,
            reason="This is a role-based or department inbox, not a person-specific address.",
            recommendation="Use with caution. These often have lower engagement and higher complaint risk."
        )

    if domain_part in KNOWN_CONSUMER_DOMAINS:
        return ValidationResult(
            clean_email=clean_email,
            status=STATUS_DOMAIN_VERIFIED,
            reason="The email syntax is valid and the domain is a commonly used consumer email domain.",
            recommendation="This is lower risk, but the specific mailbox is not verified."
        )

    return ValidationResult(
        clean_email=clean_email,
        status=STATUS_PENDING,
        reason="The email passed local checks and needs DNS verification.",
        recommendation="Run DNS verification before sending."
    )


# ============================================================
# DNS VALIDATION ENGINE
# ============================================================

class EmailDomainValidator:
    """
    Async DNS validator using Google's DNS-over-HTTPS endpoint.

    What this can verify:
    - Domain has valid MX records.
    - Domain has null MX records, meaning it explicitly does not accept email.
    - Optional A record fallback when MX is missing.

    What this cannot verify:
    - Whether a specific mailbox exists.
    - Whether the recipient will accept your message.
    - Whether a server is catch-all without SMTP probing or third-party verification.
    """

    def __init__(self, max_concurrent: int = 150):
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.domain_cache: Dict[str, ValidationResult] = {}

    async def _dns_lookup(self, session: aiohttp.ClientSession, domain: str, record_type: str) -> dict:
        url = f"https://dns.google/resolve?name={domain}&type={record_type}"
        async with session.get(url, timeout=10) as response:
            if response.status != 200:
                return {"Status": -1, "Answer": []}
            return await response.json()

    @staticmethod
    def _has_null_mx(data: dict) -> bool:
        """
        Null MX is represented as "." in MX records and means the domain does not accept email.
        """
        answers = data.get("Answer", []) or []
        for answer in answers:
            mx_data = str(answer.get("data", "")).strip()
            if mx_data == "0 ." or mx_data.endswith(" ."):
                return True
        return False

    async def _check_domain(self, session: aiohttp.ClientSession, domain: str) -> ValidationResult:
        async with self.semaphore:
            if domain in self.domain_cache:
                return self.domain_cache[domain]

            try:
                mx_data = await self._dns_lookup(session, domain, "MX")

                if self._has_null_mx(mx_data):
                    result = ValidationResult(
                        clean_email="",
                        status=STATUS_NO_MX,
                        reason="The domain publishes a null MX record, meaning it does not accept email.",
                        recommendation="Suppress addresses on this domain."
                    )
                    self.domain_cache[domain] = result
                    return result

                if mx_data.get("Status") == 0 and mx_data.get("Answer"):
                    result = ValidationResult(
                        clean_email="",
                        status=STATUS_DOMAIN_VERIFIED,
                        reason="The domain has MX records and appears configured to receive email.",
                        recommendation="Lower domain-level bounce risk. The specific mailbox is still not verified."
                    )
                    self.domain_cache[domain] = result
                    return result

                # Fallback:
                # SMTP technically allows mail delivery to A/AAAA records if no MX exists,
                # but modern marketing systems generally treat no MX as high risk.
                a_data = await self._dns_lookup(session, domain, "A")
                if a_data.get("Status") == 0 and a_data.get("Answer"):
                    result = ValidationResult(
                        clean_email="",
                        status=STATUS_UNKNOWN,
                        reason="The domain has an A record but no MX record. This is technically possible but risky for marketing sends.",
                        recommendation="Treat as unknown or verify with a dedicated email verification provider."
                    )
                    self.domain_cache[domain] = result
                    return result

                result = ValidationResult(
                    clean_email="",
                    status=STATUS_NO_MX,
                    reason="The domain does not appear to have MX records and does not clearly accept email.",
                    recommendation="Suppress this email before sending."
                )
                self.domain_cache[domain] = result
                return result

            except Exception as exc:
                result = ValidationResult(
                    clean_email="",
                    status=STATUS_UNKNOWN,
                    reason=f"DNS check failed: {exc}",
                    recommendation="Retry later or verify with a dedicated email verification provider."
                )
                self.domain_cache[domain] = result
                return result

    async def check_single(self, domain: str) -> ValidationResult:
        async with aiohttp.ClientSession() as session:
            return await self._check_domain(session, domain)

    async def process_batch(self, df: pd.DataFrame, email_col: str) -> pd.DataFrame:
        df_result = df.copy()

        for col in [
            "BounceGuard_Status",
            "BounceGuard_Reason",
            "BounceGuard_Recommendation",
            "BounceGuard_Suggested_Fix"
        ]:
            if col not in df_result.columns:
                df_result[col] = ""

        tasks = []
        task_meta = []

        async with aiohttp.ClientSession() as session:
            for idx, row in df_result.iterrows():
                current_status = row.get("BounceGuard_Status", "")

                # Only DNS-check records that need DNS verification or role-based records
                # where the domain still needs to be confirmed.
                if current_status in [STATUS_PENDING, STATUS_ROLE_BASED]:
                    email = str(row.get(email_col, ""))
                    _, domain = split_email(email)

                    if domain:
                        tasks.append(self._check_domain(session, domain))
                        task_meta.append((idx, current_status))

            if tasks:
                results = await asyncio.gather(*tasks)

                for i, domain_result in enumerate(results):
                    idx, original_status = task_meta[i]

                    if original_status == STATUS_ROLE_BASED:
                        if domain_result.status == STATUS_DOMAIN_VERIFIED:
                            df_result.at[idx, "BounceGuard_Status"] = STATUS_ROLE_BASED
                            df_result.at[idx, "BounceGuard_Reason"] = (
                                "The domain is configured to receive email, but the address is role-based."
                            )
                            df_result.at[idx, "BounceGuard_Recommendation"] = (
                                "Use with caution. Prefer a person-specific address for marketing."
                            )
                        else:
                            df_result.at[idx, "BounceGuard_Status"] = domain_result.status
                            df_result.at[idx, "BounceGuard_Reason"] = domain_result.reason
                            df_result.at[idx, "BounceGuard_Recommendation"] = domain_result.recommendation
                    else:
                        df_result.at[idx, "BounceGuard_Status"] = domain_result.status
                        df_result.at[idx, "BounceGuard_Reason"] = domain_result.reason
                        df_result.at[idx, "BounceGuard_Recommendation"] = domain_result.recommendation

        return df_result


# ============================================================
# RISK SCORING
# ============================================================

def get_risk_score(status: str) -> int:
    """
    0 = lowest risk
    100 = highest risk
    """
    if status == STATUS_DOMAIN_VERIFIED:
        return 15
    if status == STATUS_ROLE_BASED:
        return 45
    if status == STATUS_CATCH_ALL_RISK:
        return 60
    if status == STATUS_UNKNOWN:
        return 70
    if status == STATUS_TYPO:
        return 85
    if status in [STATUS_HIGH_RISK, STATUS_DISPOSABLE, STATUS_NO_MX]:
        return 100
    if status == STATUS_EMPTY:
        return 0
    return 70


def add_risk_score_columns(df: pd.DataFrame) -> pd.DataFrame:
    df_result = df.copy()
    df_result["BounceGuard_Risk_Score"] = df_result["BounceGuard_Status"].apply(get_risk_score)

    def bucket(score):
        if score == 0:
            return "No Email"
        if score <= 25:
            return "Low"
        if score <= 55:
            return "Medium"
        if score <= 80:
            return "High"
        return "Critical"

    df_result["BounceGuard_Risk_Level"] = df_result["BounceGuard_Risk_Score"].apply(bucket)
    return df_result


# ============================================================
# EXCEL GENERATOR
# ============================================================

def generate_excel(df):
    output = io.BytesIO()
    writer = pd.ExcelWriter(output, engine="xlsxwriter")
    df.to_excel(writer, index=False, sheet_name="Validated Emails")

    workbook = writer.book
    worksheet = writer.sheets["Validated Emails"]
    worksheet.freeze_panes(1, 0)

    if not df.empty:
        worksheet.autofilter(0, 0, len(df), len(df.columns) - 1)

        status_idx = df.columns.get_loc("BounceGuard_Status")
        risk_level_idx = df.columns.get_loc("BounceGuard_Risk_Level") if "BounceGuard_Risk_Level" in df.columns else None

        red_fmt = workbook.add_format({"bg_color": "#FFC7CE", "font_color": "#9C0006"})
        green_fmt = workbook.add_format({"bg_color": "#C6EFCE", "font_color": "#006100"})
        yellow_fmt = workbook.add_format({"bg_color": "#FFF2CC", "font_color": "#9C6500"})
        gray_fmt = workbook.add_format({"font_color": "#7F7F7F"})
        orange_fmt = workbook.add_format({"bg_color": "#FCE4D6", "font_color": "#C65911"})

        worksheet.conditional_format(1, status_idx, len(df), status_idx, {
            "type": "text",
            "criteria": "containing",
            "value": "Domain Verified",
            "format": green_fmt
        })
        worksheet.conditional_format(1, status_idx, len(df), status_idx, {
            "type": "text",
            "criteria": "containing",
            "value": "Invalid",
            "format": red_fmt
        })
        worksheet.conditional_format(1, status_idx, len(df), status_idx, {
            "type": "text",
            "criteria": "containing",
            "value": "Cannot Receive",
            "format": red_fmt
        })
        worksheet.conditional_format(1, status_idx, len(df), status_idx, {
            "type": "text",
            "criteria": "containing",
            "value": "Disposable",
            "format": red_fmt
        })
        worksheet.conditional_format(1, status_idx, len(df), status_idx, {
            "type": "text",
            "criteria": "containing",
            "value": "Role-Based",
            "format": yellow_fmt
        })
        worksheet.conditional_format(1, status_idx, len(df), status_idx, {
            "type": "text",
            "criteria": "containing",
            "value": "Likely Domain Typo",
            "format": orange_fmt
        })
        worksheet.conditional_format(1, status_idx, len(df), status_idx, {
            "type": "text",
            "criteria": "containing",
            "value": "Unknown",
            "format": orange_fmt
        })
        worksheet.conditional_format(1, status_idx, len(df), status_idx, {
            "type": "text",
            "criteria": "containing",
            "value": "Empty",
            "format": gray_fmt
        })

        if risk_level_idx is not None:
            worksheet.conditional_format(1, risk_level_idx, len(df), risk_level_idx, {
                "type": "text",
                "criteria": "containing",
                "value": "Critical",
                "format": red_fmt
            })
            worksheet.conditional_format(1, risk_level_idx, len(df), risk_level_idx, {
                "type": "text",
                "criteria": "containing",
                "value": "High",
                "format": orange_fmt
            })
            worksheet.conditional_format(1, risk_level_idx, len(df), risk_level_idx, {
                "type": "text",
                "criteria": "containing",
                "value": "Medium",
                "format": yellow_fmt
            })
            worksheet.conditional_format(1, risk_level_idx, len(df), risk_level_idx, {
                "type": "text",
                "criteria": "containing",
                "value": "Low",
                "format": green_fmt
            })

    for idx, col in enumerate(df.columns):
        max_len = max(df[col].astype(str).map(len).max(), len(col)) + 2
        worksheet.set_column(idx, idx, min(max_len, 55))

    writer.close()
    return output.getvalue()


# ============================================================
# UI HELPERS
# ============================================================

def render_single_result(result: ValidationResult):
    st.markdown("---")

    if result.status == STATUS_DOMAIN_VERIFIED:
        st.success(f"**{result.clean_email}**")
        st.success("✅ **Domain Verified**")
        st.markdown(result.reason)
        st.info("This does **not** guarantee the specific mailbox exists.")

    elif result.status == STATUS_ROLE_BASED:
        st.warning(f"**{result.clean_email}**")
        st.warning("⚠️ **Role-Based Address**")
        st.markdown(result.reason)
        st.info(result.recommendation)

    elif result.status in [STATUS_TYPO, STATUS_UNKNOWN, STATUS_CATCH_ALL_RISK]:
        st.warning(f"**{result.clean_email}**")
        st.warning(f"{result.status}")
        st.markdown(result.reason)
        st.info(result.recommendation)
        if result.suggested_fix:
            st.markdown(f"**Suggested fix:** `{result.suggested_fix}`")

    elif result.status == STATUS_EMPTY:
        st.info("⚪ **Empty**")
        st.markdown(result.reason)

    else:
        st.error(f"**{result.clean_email}**")
        st.error(f"{result.status}")
        st.markdown(result.reason)
        st.info(result.recommendation)


def status_matches_filter(df: pd.DataFrame, filter_choice: str) -> pd.DataFrame:
    if filter_choice == "All Records":
        return df
    if filter_choice == "✅ Domain Verified":
        return df[df["BounceGuard_Status"].eq(STATUS_DOMAIN_VERIFIED)]
    if filter_choice == "⚠️ Caution / Review":
        return df[df["BounceGuard_Status"].isin([STATUS_ROLE_BASED, STATUS_TYPO, STATUS_UNKNOWN, STATUS_CATCH_ALL_RISK])]
    if filter_choice == "🚨 Suppress":
        return df[df["BounceGuard_Status"].isin([STATUS_HIGH_RISK, STATUS_DISPOSABLE, STATUS_NO_MX])]
    if filter_choice == "⚪ Empty":
        return df[df["BounceGuard_Status"].eq(STATUS_EMPTY)]
    return df


def anonymized_cache_key(value: str) -> str:
    """
    Generates a simple anonymous fingerprint for debugging without exposing the email.
    Not used for validation, but useful if you later want safe logging.
    """
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:12]


# ============================================================
# UI ROUTING
# ============================================================

tab_single, tab_bulk, tab_methods = st.tabs([
    "🎯 Quick Check",
    "📁 Bulk List Scrubber",
    "🔬 Verification Methods"
])


# ============================================================
# TAB 1: SINGLE CHECK
# ============================================================

with tab_single:
    st.markdown("### Real-Time Email Risk Check")
    st.markdown(
        "This checks syntax, obvious junk patterns, role-based addresses, known typos, disposable domains, "
        "and whether the domain appears configured to receive email."
    )

    single_email = st.text_input("Enter Email Address:", placeholder="name@company.com")

    if st.button("Verify Address", type="primary"):
        if not single_email:
            st.warning("Please enter an email address.")
        else:
            with st.spinner("Analyzing..."):
                local_result = format_and_trap_email(single_email)
                final_result = local_result

                if local_result.status in [STATUS_PENDING, STATUS_ROLE_BASED]:
                    _, domain = split_email(local_result.clean_email)
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    validator = EmailDomainValidator()
                    dns_result = loop.run_until_complete(validator.check_single(domain))

                    if local_result.status == STATUS_ROLE_BASED:
                        if dns_result.status == STATUS_DOMAIN_VERIFIED:
                            final_result = ValidationResult(
                                clean_email=local_result.clean_email,
                                status=STATUS_ROLE_BASED,
                                reason="The domain is configured to receive email, but the address is role-based.",
                                recommendation="Use with caution. Prefer a person-specific address for marketing."
                            )
                        else:
                            final_result = ValidationResult(
                                clean_email=local_result.clean_email,
                                status=dns_result.status,
                                reason=dns_result.reason,
                                recommendation=dns_result.recommendation
                            )
                    else:
                        final_result = ValidationResult(
                            clean_email=local_result.clean_email,
                            status=dns_result.status,
                            reason=dns_result.reason,
                            recommendation=dns_result.recommendation
                        )

                render_single_result(final_result)


# ============================================================
# TAB 2: BULK LIST SCRUBBER
# ============================================================

with tab_bulk:
    st.markdown("### Bulk List Scrubber")
    st.markdown("Upload your contact list to reduce obvious bounce and deliverability risk before a campaign.")

    uploaded_file = st.file_uploader("Upload Data (.csv or .xlsx)", type=["csv", "xlsx"])

    if "df_final" not in st.session_state:
        st.session_state.df_final = None

    if uploaded_file:
        if uploaded_file.name.endswith(".csv"):
            df = pd.read_csv(uploaded_file)
        else:
            df = pd.read_excel(uploaded_file)

        columns = list(df.columns)
        guess_idx = 0
        for i, col in enumerate(columns):
            if "email" in col.lower():
                guess_idx = i
                break

        st.markdown("---")
        target_col = st.selectbox("🎯 Target Email Column:", options=columns, index=guess_idx)
        heal_data = st.checkbox(
            "Self-Heal Suppressed Emails",
            value=False,
            help="Clears high-risk emails and stores the original value in Legacy_Invalid_Email."
        )

        if st.button("🚀 Run Batch Validation", type="primary", use_container_width=True):
            with st.spinner("Running local checks..."):
                df["BounceGuard_Status"] = ""
                df["BounceGuard_Reason"] = ""
                df["BounceGuard_Recommendation"] = ""
                df["BounceGuard_Suggested_Fix"] = ""

                for idx, row in df.iterrows():
                    raw_email = row[target_col]
                    result = format_and_trap_email(raw_email)

                    df.at[idx, target_col] = result.clean_email
                    df.at[idx, "BounceGuard_Status"] = result.status
                    df.at[idx, "BounceGuard_Reason"] = result.reason
                    df.at[idx, "BounceGuard_Recommendation"] = result.recommendation
                    df.at[idx, "BounceGuard_Suggested_Fix"] = result.suggested_fix

                total_processed = len(df[df[target_col] != ""])
                locally_completed_count = (~df["BounceGuard_Status"].isin([STATUS_PENDING, STATUS_ROLE_BASED])).sum()
                dns_ping_count = df["BounceGuard_Status"].isin([STATUS_PENDING, STATUS_ROLE_BASED]).sum()

            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            chunk_size = 1000
            num_chunks = max(math.ceil(len(df) / chunk_size), 1)

            progress_bar = st.progress(0, text=f"Verifying domains... (0/{len(df):,})")
            validator = EmailDomainValidator(max_concurrent=150)

            processed_chunks = []
            for i in range(num_chunks):
                chunk = df.iloc[i * chunk_size: (i + 1) * chunk_size]
                chunk_res = loop.run_until_complete(validator.process_batch(chunk, target_col))
                processed_chunks.append(chunk_res)

                records_done = min((i + 1) * chunk_size, len(df))
                progress_bar.progress(
                    (i + 1) / num_chunks,
                    text=f"Verifying domains... ({records_done:,}/{len(df):,})"
                )

            df_final = pd.concat(processed_chunks, ignore_index=True)
            df_final = add_risk_score_columns(df_final)
            progress_bar.empty()

            suppress_statuses = [STATUS_HIGH_RISK, STATUS_DISPOSABLE, STATUS_NO_MX, STATUS_TYPO]
            if heal_data:
                mask_dead = df_final["BounceGuard_Status"].isin(suppress_statuses)
                df_final["Legacy_Invalid_Email"] = ""
                df_final.loc[mask_dead, "Legacy_Invalid_Email"] = df_final.loc[mask_dead, target_col]
                df_final.loc[mask_dead, target_col] = ""

            st.session_state.df_final = df_final
            st.session_state.total_processed = total_processed
            st.session_state.locally_completed_count = locally_completed_count
            st.session_state.dns_ping_count = dns_ping_count
            st.session_state.target_col = target_col

        if st.session_state.df_final is not None:
            df_final = st.session_state.df_final
            target_col = st.session_state.target_col

            domain_verified = df_final["BounceGuard_Status"].eq(STATUS_DOMAIN_VERIFIED).sum()
            caution = df_final["BounceGuard_Status"].isin([STATUS_ROLE_BASED, STATUS_TYPO, STATUS_UNKNOWN, STATUS_CATCH_ALL_RISK]).sum()
            suppress = df_final["BounceGuard_Status"].isin([STATUS_HIGH_RISK, STATUS_DISPOSABLE, STATUS_NO_MX]).sum()
            empty = df_final["BounceGuard_Status"].eq(STATUS_EMPTY).sum()

            st.markdown("### 🏆 Protection Report")
            col_a, col_b, col_c, col_d, col_e = st.columns(5)
            col_a.metric("Emails Processed", f"{st.session_state.total_processed:,}")
            col_b.metric("✅ Domain Verified", f"{domain_verified:,}")
            col_c.metric("⚠️ Caution / Review", f"{caution:,}")
            col_d.metric("🚨 Suppress", f"{suppress:,}", delta="Risk Reduced", delta_color="normal")
            col_e.metric("⚪ Empty", f"{empty:,}")

            st.markdown("---")

            with st.expander("⚙️ Stats for Nerds", expanded=False):
                efficiency_rate = (
                    st.session_state.locally_completed_count / max(len(df_final), 1)
                ) * 100

                st.markdown(f"""
                **Network Throughput Analysis**

                * **Total Rows:** {len(df_final):,}
                * **Total Valid Inputs:** {st.session_state.total_processed:,}
                * **Completed by Local Rules:** {st.session_state.locally_completed_count:,}
                * **Live DNS Pings Executed:** {st.session_state.dns_ping_count:,}

                **Efficiency Rate:** **{efficiency_rate:.1f}%** of this file was handled by local validation before DNS checks.
                """)

            st.markdown("### 🔍 Data Explorer")
            filter_choice = st.radio(
                "Filter Results:",
                ["All Records", "✅ Domain Verified", "⚠️ Caution / Review", "🚨 Suppress", "⚪ Empty"],
                horizontal=True
            )

            df_display = status_matches_filter(df_final.copy(), filter_choice)

            display_cols = df_display.columns.tolist()
            priority_cols = [
                target_col,
                "BounceGuard_Status",
                "BounceGuard_Risk_Level",
                "BounceGuard_Risk_Score",
                "BounceGuard_Reason",
                "BounceGuard_Recommendation",
                "BounceGuard_Suggested_Fix"
            ]

            ordered_cols = [col for col in priority_cols if col in display_cols]
            ordered_cols += [col for col in display_cols if col not in ordered_cols]

            st.dataframe(df_display[ordered_cols].head(250), use_container_width=True)

            st.download_button(
                label="📥 Download Full Validated List (.xlsx)",
                data=generate_excel(df_final),
                file_name="BounceGuard_Validated_List.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                type="primary",
                use_container_width=True
            )


# ============================================================
# TAB 3: METHODS
# ============================================================

with tab_methods:
    st.markdown("### What BounceGuard Checks Today")

    st.markdown("""
    BounceGuard currently performs these checks:

    1. **Normalization**
       - Lowercases the email.
       - Trims spaces and common wrapping characters.
       - Removes `mailto:` if pasted from a hyperlink.

    2. **Syntax Validation**
       - Checks whether the address is shaped like a valid email address.

    3. **Junk and Placeholder Traps**
       - Catches obvious junk like `test@test.com`, `na@gmail.com`, `unknown@unknown.com`, and emails containing terms like `unknown`, `unkown`, `noemail`, `donotemail`, or `placeholder`.

    4. **Role-Based Detection**
       - Flags addresses like `info@`, `sales@`, `admin@`, and `support@`.

    5. **Disposable Domain Detection**
       - Flags common temporary email providers.

    6. **Common Typo Detection**
       - Flags common mistakes like `gmial.com`, `gamil.com`, `hotmial.com`, and `comast.net`.

    7. **DNS MX Verification**
       - Checks whether the domain has MX records and appears configured to receive mail.

    8. **Null MX Handling**
       - Detects domains that explicitly publish a null MX record, meaning they do not accept email.
    """)

    st.markdown("### What This Still Does Not Prove")
    st.warning(
        "A domain-level check does not prove that the individual mailbox exists. "
        "For example, proving that gmail.com can receive email does not prove that a specific Gmail address exists."
    )

    st.markdown("### Reliable Ways to Move Closer to a True Verifier")

    st.markdown("""
    **Best next steps, in order:**

    1. **Use a Dedicated Verification API**
       - Examples: NeverBounce, ZeroBounce, Kickbox, BriteVerify, Emailable, Hunter.
       - This is the most practical upgrade.
       - You usually get richer statuses like `valid`, `invalid`, `catch-all`, `unknown`, `disposable`, and `role`.

    2. **Add Catch-All Detection**
       - A catch-all domain accepts mail for many or all random recipients.
       - If a domain is catch-all, you often cannot prove whether a specific mailbox exists.

    3. **Add SMTP Recipient Probing**
       - This attempts to connect to the receiving mail server and test the recipient.
       - It is not perfectly reliable because many servers block, rate-limit, greylist, or intentionally hide mailbox existence.
       - It can also look suspicious if abused, so this should be throttled and used carefully.

    4. **Maintain a Suppression List**
       - Store past hard bounces, unsubscribes, spam complaints, and invalids.
       - This is one of the most reliable internal signals because it is based on actual send history.

    5. **Track Engagement**
       - Prioritize people who opened, clicked, replied, submitted a form, booked an appointment, or recently interacted.
       - Engagement does not verify a mailbox directly, but it is a strong deliverability signal.

    6. **Use a Maintained Disposable Domain Dataset**
       - Your built-in pattern catches common throwaway domains, but a maintained dataset will be much stronger.

    7. **Add Domain Age and Website Signals**
       - New, parked, or dead domains are higher risk.
       - This is useful as a risk signal, not proof.
    """)

    st.markdown("### Brutal Truth")
    st.info(
        "This app is now a much better preflight scrubber, but it is still not a full mailbox verifier. "
        "The cleanest production version would combine this local engine with a paid verification API and your own historical suppression data."
    )
