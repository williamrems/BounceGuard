"""
app.py
BounceGuard by ContractorFlow

Email Deliverability Risk Scrubber
- Local junk/format validation
- Domain MX validation
- Optional automatic deep scan for clean questionable records only
- Multi-resolver DNS recheck
- Optional external verification API pass for only selected questionable records

Important:
BounceGuard reduces obvious bounce and sender reputation risk. It does not guarantee
that a specific mailbox exists unless a verification provider confirms it.
"""

import streamlit as st
import pandas as pd
import asyncio
import aiohttp
import re
import io
import math
import os
from dataclasses import dataclass
from typing import Dict, Optional, Tuple, List


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
# STATUS CONSTANTS
# ============================================================

STATUS_EMPTY = "⚪ Empty"
STATUS_DOMAIN_VERIFIED = "✅ Domain Verified"
STATUS_HIGH_RISK = "🚨 Invalid / High Bounce Risk"
STATUS_ROLE_BASED = "⚠️ Role-Based Address"
STATUS_UNKNOWN = "❔ Unknown / Needs Review"
STATUS_TYPO = "⚠️ Likely Domain Typo"
STATUS_DISPOSABLE = "🚨 Disposable / Temporary Email"
STATUS_NO_MX = "🚨 Domain Cannot Receive Email"
STATUS_SANDBOX_INVALID = "🚫 Salesforce Sandbox Invalid Email"
STATUS_PENDING = "PENDING"

DEEP_NOT_NEEDED = "Not Needed"
DEEP_NOT_ELIGIBLE = "Not Eligible"
DEEP_PENDING = "Pending"
DEEP_MX_CONFIRMED = "✅ Deep Scan: MX Confirmed"
DEEP_NO_MX_CONFIRMED = "🚨 Deep Scan: No MX Confirmed"
DEEP_WEBSITE_ONLY = "⚠️ Deep Scan: Website Exists, No MX"
DEEP_DNS_INCONCLUSIVE = "❔ Deep Scan: DNS Inconclusive"
DEEP_API_VALID = "✅ Verification API: Valid"
DEEP_API_INVALID = "🚨 Verification API: Invalid"
DEEP_API_RISKY = "⚠️ Verification API: Risky / Unknown"
DEEP_API_SKIPPED = "Verification API Skipped"


# ============================================================
# LOCAL RULE CONSTANTS
# ============================================================

FAKE_LOCAL_PARTS = {
    "test", "testing", "something", "anything", "fake", "email", "noemail",
    "donotemail", "do-not-email", "spam", "customer", "client", "na", "n/a",
    "none", "null", "unknown", "unkown", "no", "nobody", "blank", "placeholder",
    "example", "sample", "demo", "asdf", "qwerty", "xxx", "x"
}

JUNK_TERMS_ANYWHERE = {
    "unknown", "unkown", "noemail", "donotemail", "do-not-email",
    "notprovided", "not-provided", "notavailable", "not-available",
    "placeholder"
}

GENERIC_EMAIL_PREFIXES = {
    "info", "admin", "sales", "support", "contact", "hello", "office",
    "service", "customerservice", "customer.service", "billing", "accounts",
    "marketing", "webmaster", "help", "team", "jobs", "careers", "hr"
}

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

DISPOSABLE_DOMAIN_KEYWORDS = {
    "mailinator", "yopmail", "tempmail", "10minute", "guerrillamail",
    "sharklasers", "throwawaymail", "trashmail", "getnada", "dispostable",
    "fakeinbox", "mintemail", "maildrop", "moakt", "temp-mail", "tempmailo",
    "emailondeck", "burnermail", "spamgourmet"
}

SUSPECT_DOMAIN_PATTERN = re.compile(
    r"^(fake|demo|test|mock|example|sample|unknown|unkown)(\.|-|$)",
    re.IGNORECASE
)

KNOWN_CONSUMER_DOMAINS = {
    "gmail.com", "googlemail.com", "yahoo.com", "hotmail.com", "aol.com",
    "outlook.com", "live.com", "icloud.com", "comcast.net", "msn.com",
    "sbcglobal.net", "att.net", "verizon.net", "mac.com", "me.com",
    "bellsouth.net", "charter.net", "proton.me", "protonmail.com"
}

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


@dataclass
class DeepScanResult:
    status: str
    reason: str
    recommendation: str
    mx_found_count: int = 0
    no_mx_count: int = 0
    resolver_errors: int = 0
    api_provider: str = ""
    api_status: str = ""
    api_reason: str = ""


# ============================================================
# BASIC HELPERS
# ============================================================

def normalize_email(email) -> str:
    if pd.isna(email):
        return ""

    clean = str(email).strip().lower()
    clean = clean.strip("<>()[]{}\"' ")

    if clean.startswith("mailto:"):
        clean = clean.replace("mailto:", "", 1).strip()

    return clean


def split_email(clean_email: str) -> Tuple[str, str]:
    if clean_email.count("@") != 1:
        return "", ""

    local_part, domain_part = clean_email.split("@", 1)
    return local_part, domain_part


def get_domain(clean_email: str) -> str:
    _, domain = split_email(clean_email)
    return domain


def contains_disposable_domain(domain: str) -> bool:
    domain_lower = domain.lower()
    return any(keyword in domain_lower for keyword in DISPOSABLE_DOMAIN_KEYWORDS)


def contains_junk_term_anywhere(clean_email: str) -> Optional[str]:
    compact = clean_email.replace(".", "").replace("_", "").replace("-", "")
    for term in JUNK_TERMS_ANYWHERE:
        compact_term = term.replace(".", "").replace("_", "").replace("-", "")
        if compact_term and compact_term in compact:
            return term
    return None


def is_salesforce_sandbox_invalid_domain(domain: str) -> bool:
    return domain.endswith(".invalid")


def is_role_based_email(clean_email: str) -> bool:
    local_part, _ = split_email(clean_email)
    return local_part in GENERIC_EMAIL_PREFIXES


def has_basic_person_local_part(clean_email: str) -> bool:
    local_part, _ = split_email(clean_email)

    if not local_part:
        return False

    if local_part in GENERIC_EMAIL_PREFIXES:
        return False

    if local_part in FAKE_LOCAL_PARTS:
        return False

    # Avoid deep scanning obvious numeric/system/test values.
    if local_part.isdigit():
        return False

    if len(local_part) < 2:
        return False

    return True


# ============================================================
# LOCAL VALIDATION
# ============================================================

def format_and_trap_email(email) -> ValidationResult:
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

    if is_salesforce_sandbox_invalid_domain(domain_part):
        return ValidationResult(
            clean_email=clean_email,
            status=STATUS_SANDBOX_INVALID,
            reason="This email ends with .invalid, which is commonly created by Salesforce sandbox email masking.",
            recommendation="Exclude this from campaign sends. Do not deep scan it."
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
# FIRST-PASS DNS VALIDATOR
# ============================================================

class EmailDomainValidator:
    def __init__(self, max_concurrent: int = 150):
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.domain_cache: Dict[str, ValidationResult] = {}

    async def _dns_lookup_google(self, session: aiohttp.ClientSession, domain: str, record_type: str) -> dict:
        url = f"https://dns.google/resolve?name={domain}&type={record_type}"
        async with session.get(url, timeout=10) as response:
            if response.status != 200:
                return {"Status": -1, "Answer": []}
            return await response.json()

    @staticmethod
    def _has_null_mx(data: dict) -> bool:
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
                mx_data = await self._dns_lookup_google(session, domain, "MX")

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

                a_data = await self._dns_lookup_google(session, domain, "A")
                if a_data.get("Status") == 0 and a_data.get("Answer"):
                    result = ValidationResult(
                        clean_email="",
                        status=STATUS_UNKNOWN,
                        reason="The domain has a website/IP record but no MX record was found.",
                        recommendation="Do not include in the first send. Review or deep scan."
                    )
                    self.domain_cache[domain] = result
                    return result

                result = ValidationResult(
                    clean_email="",
                    status=STATUS_NO_MX,
                    reason="The domain does not appear to have MX records and does not clearly accept email.",
                    recommendation="Suppress this email before sending, unless deep scan later corrects it."
                )
                self.domain_cache[domain] = result
                return result

            except Exception as exc:
                result = ValidationResult(
                    clean_email="",
                    status=STATUS_UNKNOWN,
                    reason=f"DNS check failed: {exc}",
                    recommendation="Retry later or deep scan."
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
# DEEP SCAN ELIGIBILITY
# ============================================================

def is_deep_scan_candidate(row: pd.Series, email_col: str) -> Tuple[bool, str]:
    clean_email = str(row.get(email_col, "")).strip().lower()
    status = str(row.get("BounceGuard_Status", "")).strip()
    local_part, domain = split_email(clean_email)

    if not clean_email or status == STATUS_EMPTY:
        return False, "Empty email."

    if not EMAIL_REGEX.match(clean_email):
        return False, "Email syntax failed. Deep scan skipped."

    if is_salesforce_sandbox_invalid_domain(domain):
        return False, "Salesforce sandbox .invalid email. Deep scan skipped."

    if is_role_based_email(clean_email):
        return False, "Role-based address. Deep scan skipped by rule."

    if contains_junk_term_anywhere(clean_email):
        return False, "Junk placeholder term found. Deep scan skipped."

    if local_part in FAKE_LOCAL_PARTS:
        return False, "Fake or placeholder local part. Deep scan skipped."

    if contains_disposable_domain(domain):
        return False, "Disposable email domain. Deep scan skipped."

    if domain in COMMON_DOMAIN_TYPOS:
        return False, "Likely domain typo. Correct before deep scanning."

    if status not in [STATUS_NO_MX, STATUS_UNKNOWN]:
        return False, "Status does not require deep scan."

    if not has_basic_person_local_part(clean_email):
        return False, "Address does not look like a person-specific mailbox."

    return True, "Eligible for deep scan."


# ============================================================
# MULTI-RESOLVER DEEP DNS SCANNER
# ============================================================

class DeepDomainScanner:
    def __init__(self, max_concurrent: int = 50):
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.domain_cache: Dict[str, DeepScanResult] = {}

    async def _lookup_google(self, session: aiohttp.ClientSession, domain: str, record_type: str) -> dict:
        url = f"https://dns.google/resolve?name={domain}&type={record_type}"
        async with session.get(url, timeout=10) as response:
            if response.status != 200:
                return {"resolver": "Google", "ok": False, "Status": -1, "Answer": []}
            data = await response.json()
            data["resolver"] = "Google"
            data["ok"] = True
            return data

    async def _lookup_cloudflare(self, session: aiohttp.ClientSession, domain: str, record_type: str) -> dict:
        url = f"https://cloudflare-dns.com/dns-query?name={domain}&type={record_type}"
        headers = {"accept": "application/dns-json"}
        async with session.get(url, headers=headers, timeout=10) as response:
            if response.status != 200:
                return {"resolver": "Cloudflare", "ok": False, "Status": -1, "Answer": []}
            data = await response.json()
            data["resolver"] = "Cloudflare"
            data["ok"] = True
            return data

    async def _lookup_quad9(self, session: aiohttp.ClientSession, domain: str, record_type: str) -> dict:
        url = f"https://dns.quad9.net:5053/dns-query?name={domain}&type={record_type}"
        headers = {"accept": "application/dns-json"}
        async with session.get(url, headers=headers, timeout=10) as response:
            if response.status != 200:
                return {"resolver": "Quad9", "ok": False, "Status": -1, "Answer": []}
            data = await response.json()
            data["resolver"] = "Quad9"
            data["ok"] = True
            return data

    @staticmethod
    def _has_answers(data: dict) -> bool:
        return data.get("Status") == 0 and bool(data.get("Answer"))

    @staticmethod
    def _has_null_mx(data: dict) -> bool:
        answers = data.get("Answer", []) or []
        for answer in answers:
            mx_data = str(answer.get("data", "")).strip()
            if mx_data == "0 ." or mx_data.endswith(" ."):
                return True
        return False

    async def _resolver_group_lookup(self, session: aiohttp.ClientSession, domain: str, record_type: str) -> List[dict]:
        tasks = [
            self._lookup_google(session, domain, record_type),
            self._lookup_cloudflare(session, domain, record_type),
            self._lookup_quad9(session, domain, record_type),
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        clean_results = []
        for result in results:
            if isinstance(result, Exception):
                clean_results.append({"resolver": "Unknown", "ok": False, "Status": -1, "Answer": []})
            else:
                clean_results.append(result)

        return clean_results

    async def scan_domain(self, session: aiohttp.ClientSession, domain: str) -> DeepScanResult:
        async with self.semaphore:
            if domain in self.domain_cache:
                return self.domain_cache[domain]

            mx_results = await self._resolver_group_lookup(session, domain, "MX")

            null_mx_count = sum(1 for r in mx_results if self._has_null_mx(r))
            mx_found_count = sum(1 for r in mx_results if self._has_answers(r) and not self._has_null_mx(r))
            resolver_errors = sum(1 for r in mx_results if not r.get("ok"))

            if null_mx_count > 0:
                result = DeepScanResult(
                    status=DEEP_NO_MX_CONFIRMED,
                    reason="At least one resolver found a null MX record. The domain explicitly does not accept email.",
                    recommendation="Suppress addresses on this domain.",
                    mx_found_count=mx_found_count,
                    no_mx_count=3 - mx_found_count,
                    resolver_errors=resolver_errors
                )
                self.domain_cache[domain] = result
                return result

            if mx_found_count >= 1:
                result = DeepScanResult(
                    status=DEEP_MX_CONFIRMED,
                    reason=f"MX records were found by {mx_found_count} resolver(s). The first-pass no-MX result was likely too strict or temporary.",
                    recommendation="Move this record back into review or domain-verified status. The mailbox is still not guaranteed.",
                    mx_found_count=mx_found_count,
                    no_mx_count=3 - mx_found_count,
                    resolver_errors=resolver_errors
                )
                self.domain_cache[domain] = result
                return result

            # Check whether the domain exists as a website/domain even though no MX was found.
            a_results = await self._resolver_group_lookup(session, domain, "A")
            a_found_count = sum(1 for r in a_results if self._has_answers(r))
            a_errors = sum(1 for r in a_results if not r.get("ok"))

            if a_found_count >= 1:
                result = DeepScanResult(
                    status=DEEP_WEBSITE_ONLY,
                    reason=f"No MX records were found, but A records were found by {a_found_count} resolver(s). The website/domain may exist, but email is not configured.",
                    recommendation="Do not send unless a verification API or manual review confirms the email.",
                    mx_found_count=0,
                    no_mx_count=3,
                    resolver_errors=resolver_errors + a_errors
                )
                self.domain_cache[domain] = result
                return result

            ns_results = await self._resolver_group_lookup(session, domain, "NS")
            ns_found_count = sum(1 for r in ns_results if self._has_answers(r))
            ns_errors = sum(1 for r in ns_results if not r.get("ok"))

            if ns_found_count >= 1:
                result = DeepScanResult(
                    status=DEEP_NO_MX_CONFIRMED,
                    reason="The domain has DNS records, but no MX records were found by multiple resolvers.",
                    recommendation="Suppress or manually verify before sending.",
                    mx_found_count=0,
                    no_mx_count=3,
                    resolver_errors=resolver_errors + a_errors + ns_errors
                )
                self.domain_cache[domain] = result
                return result

            result = DeepScanResult(
                status=DEEP_DNS_INCONCLUSIVE,
                reason="Multiple resolvers could not confirm MX, A, or NS records.",
                recommendation="Treat as high risk. Suppress unless manually verified.",
                mx_found_count=0,
                no_mx_count=3,
                resolver_errors=resolver_errors + a_errors + ns_errors
            )
            self.domain_cache[domain] = result
            return result

    async def process_candidates(self, df: pd.DataFrame, email_col: str) -> pd.DataFrame:
        df_result = df.copy()

        for col in [
            "DeepScan_Eligible",
            "DeepScan_Status",
            "DeepScan_Reason",
            "DeepScan_Recommendation",
            "DeepScan_MX_Resolvers_Found",
            "DeepScan_Resolver_Errors"
        ]:
            if col not in df_result.columns:
                df_result[col] = ""

        candidates = []
        async with aiohttp.ClientSession() as session:
            for idx, row in df_result.iterrows():
                eligible, eligibility_reason = is_deep_scan_candidate(row, email_col)
                df_result.at[idx, "DeepScan_Eligible"] = "Yes" if eligible else "No"

                if not eligible:
                    current_status = row.get("BounceGuard_Status", "")
                    if current_status in [STATUS_NO_MX, STATUS_UNKNOWN]:
                        df_result.at[idx, "DeepScan_Status"] = DEEP_NOT_ELIGIBLE
                        df_result.at[idx, "DeepScan_Reason"] = eligibility_reason
                    else:
                        df_result.at[idx, "DeepScan_Status"] = DEEP_NOT_NEEDED
                        df_result.at[idx, "DeepScan_Reason"] = eligibility_reason
                    continue

                email = str(row.get(email_col, "")).strip().lower()
                domain = get_domain(email)
                candidates.append((idx, domain))

            if candidates:
                tasks = [self.scan_domain(session, domain) for _, domain in candidates]
                results = await asyncio.gather(*tasks)

                for i, scan_result in enumerate(results):
                    idx, _ = candidates[i]

                    df_result.at[idx, "DeepScan_Status"] = scan_result.status
                    df_result.at[idx, "DeepScan_Reason"] = scan_result.reason
                    df_result.at[idx, "DeepScan_Recommendation"] = scan_result.recommendation
                    df_result.at[idx, "DeepScan_MX_Resolvers_Found"] = scan_result.mx_found_count
                    df_result.at[idx, "DeepScan_Resolver_Errors"] = scan_result.resolver_errors

                    # Upgrade or refine first-pass status when deep scan improves confidence.
                    if scan_result.status == DEEP_MX_CONFIRMED:
                        df_result.at[idx, "BounceGuard_Status"] = STATUS_DOMAIN_VERIFIED
                        df_result.at[idx, "BounceGuard_Reason"] = (
                            "Deep scan found MX records through at least one resolver."
                        )
                        df_result.at[idx, "BounceGuard_Recommendation"] = (
                            "Accept as domain verified, but remember the mailbox itself is not guaranteed."
                        )
                    elif scan_result.status == DEEP_WEBSITE_ONLY:
                        df_result.at[idx, "BounceGuard_Status"] = STATUS_UNKNOWN
                        df_result.at[idx, "BounceGuard_Reason"] = (
                            "Deep scan found a website/IP record but no MX records."
                        )
                        df_result.at[idx, "BounceGuard_Recommendation"] = (
                            "Do not include in the first send unless a verification API or manual review confirms it."
                        )
                    elif scan_result.status in [DEEP_NO_MX_CONFIRMED, DEEP_DNS_INCONCLUSIVE]:
                        df_result.at[idx, "BounceGuard_Status"] = STATUS_NO_MX
                        df_result.at[idx, "BounceGuard_Reason"] = scan_result.reason
                        df_result.at[idx, "BounceGuard_Recommendation"] = scan_result.recommendation

        return df_result


# ============================================================
# OPTIONAL VERIFICATION API
# ============================================================

async def verify_with_zerobounce(session: aiohttp.ClientSession, email: str, api_key: str) -> DeepScanResult:
    url = "https://api.zerobounce.net/v2/validate"
    params = {"api_key": api_key, "email": email}

    try:
        async with session.get(url, params=params, timeout=20) as response:
            if response.status != 200:
                return DeepScanResult(
                    status=DEEP_API_RISKY,
                    reason=f"ZeroBounce returned HTTP {response.status}.",
                    recommendation="Treat as unknown unless manually verified.",
                    api_provider="ZeroBounce",
                    api_status="api_error"
                )

            data = await response.json()
            status = str(data.get("status", "")).lower()
            sub_status = str(data.get("sub_status", "")).lower()

            if status == "valid":
                return DeepScanResult(
                    status=DEEP_API_VALID,
                    reason="ZeroBounce returned valid.",
                    recommendation="Accept as verified by API.",
                    api_provider="ZeroBounce",
                    api_status=status,
                    api_reason=sub_status
                )

            if status == "invalid":
                return DeepScanResult(
                    status=DEEP_API_INVALID,
                    reason=f"ZeroBounce returned invalid. Detail: {sub_status or 'none'}",
                    recommendation="Suppress before sending.",
                    api_provider="ZeroBounce",
                    api_status=status,
                    api_reason=sub_status
                )

            return DeepScanResult(
                status=DEEP_API_RISKY,
                reason=f"ZeroBounce returned {status or 'unknown'}. Detail: {sub_status or 'none'}",
                recommendation="Do not include in first send unless manually approved.",
                api_provider="ZeroBounce",
                api_status=status,
                api_reason=sub_status
            )

    except Exception as exc:
        return DeepScanResult(
            status=DEEP_API_RISKY,
            reason=f"ZeroBounce check failed: {exc}",
            recommendation="Treat as unknown unless manually verified.",
            api_provider="ZeroBounce",
            api_status="exception"
        )


async def verify_with_neverbounce(session: aiohttp.ClientSession, email: str, api_key: str) -> DeepScanResult:
    url = "https://api.neverbounce.com/v4/single/check"
    params = {"key": api_key, "email": email}

    try:
        async with session.get(url, params=params, timeout=20) as response:
            if response.status != 200:
                return DeepScanResult(
                    status=DEEP_API_RISKY,
                    reason=f"NeverBounce returned HTTP {response.status}.",
                    recommendation="Treat as unknown unless manually verified.",
                    api_provider="NeverBounce",
                    api_status="api_error"
                )

            data = await response.json()
            result = str(data.get("result", "")).lower()
            flags = data.get("flags", [])

            if result == "valid":
                return DeepScanResult(
                    status=DEEP_API_VALID,
                    reason="NeverBounce returned valid.",
                    recommendation="Accept as verified by API.",
                    api_provider="NeverBounce",
                    api_status=result,
                    api_reason=", ".join(flags) if isinstance(flags, list) else str(flags)
                )

            if result == "invalid":
                return DeepScanResult(
                    status=DEEP_API_INVALID,
                    reason="NeverBounce returned invalid.",
                    recommendation="Suppress before sending.",
                    api_provider="NeverBounce",
                    api_status=result,
                    api_reason=", ".join(flags) if isinstance(flags, list) else str(flags)
                )

            return DeepScanResult(
                status=DEEP_API_RISKY,
                reason=f"NeverBounce returned {result or 'unknown'}.",
                recommendation="Do not include in first send unless manually approved.",
                api_provider="NeverBounce",
                api_status=result,
                api_reason=", ".join(flags) if isinstance(flags, list) else str(flags)
            )

    except Exception as exc:
        return DeepScanResult(
            status=DEEP_API_RISKY,
            reason=f"NeverBounce check failed: {exc}",
            recommendation="Treat as unknown unless manually verified.",
            api_provider="NeverBounce",
            api_status="exception"
        )


async def run_verification_api_on_deep_candidates(
    df: pd.DataFrame,
    email_col: str,
    provider: str,
    api_key: str,
    max_concurrent: int = 10
) -> pd.DataFrame:
    df_result = df.copy()

    for col in [
        "VerificationAPI_Provider",
        "VerificationAPI_Status",
        "VerificationAPI_Reason",
        "VerificationAPI_Recommendation"
    ]:
        if col not in df_result.columns:
            df_result[col] = ""

    if not provider or provider == "None" or not api_key:
        df_result["VerificationAPI_Status"] = DEEP_API_SKIPPED
        return df_result

    semaphore = asyncio.Semaphore(max_concurrent)

    async def run_one(session: aiohttp.ClientSession, idx: int, email: str) -> Tuple[int, DeepScanResult]:
        async with semaphore:
            if provider == "ZeroBounce":
                result = await verify_with_zerobounce(session, email, api_key)
            elif provider == "NeverBounce":
                result = await verify_with_neverbounce(session, email, api_key)
            else:
                result = DeepScanResult(
                    status=DEEP_API_SKIPPED,
                    reason="No supported verification API provider selected.",
                    recommendation="No API verification was performed."
                )
            return idx, result

    tasks = []

    async with aiohttp.ClientSession() as session:
        for idx, row in df_result.iterrows():
            deep_eligible = str(row.get("DeepScan_Eligible", "")).strip() == "Yes"

            # API is only used on the clean questionable group, not role-based,
            # not .invalid, and not already clean domain-verified from first pass.
            if deep_eligible:
                email = str(row.get(email_col, "")).strip().lower()
                tasks.append(run_one(session, idx, email))

        if tasks:
            results = await asyncio.gather(*tasks)

            for idx, api_result in results:
                df_result.at[idx, "VerificationAPI_Provider"] = api_result.api_provider or provider
                df_result.at[idx, "VerificationAPI_Status"] = api_result.status
                df_result.at[idx, "VerificationAPI_Reason"] = api_result.reason
                df_result.at[idx, "VerificationAPI_Recommendation"] = api_result.recommendation

                if api_result.status == DEEP_API_VALID:
                    df_result.at[idx, "BounceGuard_Status"] = STATUS_DOMAIN_VERIFIED
                    df_result.at[idx, "BounceGuard_Reason"] = "External verification API returned valid."
                    df_result.at[idx, "BounceGuard_Recommendation"] = "Accept as verified by API."
                elif api_result.status == DEEP_API_INVALID:
                    df_result.at[idx, "BounceGuard_Status"] = STATUS_HIGH_RISK
                    df_result.at[idx, "BounceGuard_Reason"] = "External verification API returned invalid."
                    df_result.at[idx, "BounceGuard_Recommendation"] = "Suppress before sending."
                elif api_result.status == DEEP_API_RISKY:
                    df_result.at[idx, "BounceGuard_Status"] = STATUS_UNKNOWN
                    df_result.at[idx, "BounceGuard_Reason"] = api_result.reason
                    df_result.at[idx, "BounceGuard_Recommendation"] = api_result.recommendation

    return df_result


# ============================================================
# RISK SCORING
# ============================================================

def get_risk_score(status: str) -> int:
    if status == STATUS_DOMAIN_VERIFIED:
        return 15
    if status == STATUS_ROLE_BASED:
        return 45
    if status == STATUS_UNKNOWN:
        return 70
    if status == STATUS_TYPO:
        return 85
    if status in [STATUS_HIGH_RISK, STATUS_DISPOSABLE, STATUS_NO_MX, STATUS_SANDBOX_INVALID]:
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
            "type": "text", "criteria": "containing", "value": "Domain Verified", "format": green_fmt
        })
        worksheet.conditional_format(1, status_idx, len(df), status_idx, {
            "type": "text", "criteria": "containing", "value": "Invalid", "format": red_fmt
        })
        worksheet.conditional_format(1, status_idx, len(df), status_idx, {
            "type": "text", "criteria": "containing", "value": "Cannot Receive", "format": red_fmt
        })
        worksheet.conditional_format(1, status_idx, len(df), status_idx, {
            "type": "text", "criteria": "containing", "value": "Temporary", "format": red_fmt
        })
        worksheet.conditional_format(1, status_idx, len(df), status_idx, {
            "type": "text", "criteria": "containing", "value": "Sandbox", "format": red_fmt
        })
        worksheet.conditional_format(1, status_idx, len(df), status_idx, {
            "type": "text", "criteria": "containing", "value": "Role-Based", "format": yellow_fmt
        })
        worksheet.conditional_format(1, status_idx, len(df), status_idx, {
            "type": "text", "criteria": "containing", "value": "Likely Domain Typo", "format": orange_fmt
        })
        worksheet.conditional_format(1, status_idx, len(df), status_idx, {
            "type": "text", "criteria": "containing", "value": "Unknown", "format": orange_fmt
        })
        worksheet.conditional_format(1, status_idx, len(df), status_idx, {
            "type": "text", "criteria": "containing", "value": "Empty", "format": gray_fmt
        })

        if risk_level_idx is not None:
            worksheet.conditional_format(1, risk_level_idx, len(df), risk_level_idx, {
                "type": "text", "criteria": "containing", "value": "Critical", "format": red_fmt
            })
            worksheet.conditional_format(1, risk_level_idx, len(df), risk_level_idx, {
                "type": "text", "criteria": "containing", "value": "High", "format": orange_fmt
            })
            worksheet.conditional_format(1, risk_level_idx, len(df), risk_level_idx, {
                "type": "text", "criteria": "containing", "value": "Medium", "format": yellow_fmt
            })
            worksheet.conditional_format(1, risk_level_idx, len(df), risk_level_idx, {
                "type": "text", "criteria": "containing", "value": "Low", "format": green_fmt
            })

    for idx, col in enumerate(df.columns):
        max_len = max(df[col].astype(str).map(len).max(), len(col)) + 2
        worksheet.set_column(idx, idx, min(max_len, 65))

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
        st.info("This does not guarantee the specific mailbox exists.")

    elif result.status == STATUS_ROLE_BASED:
        st.warning(f"**{result.clean_email}**")
        st.warning("⚠️ **Role-Based Address**")
        st.markdown(result.reason)
        st.info(result.recommendation)

    elif result.status in [STATUS_TYPO, STATUS_UNKNOWN]:
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
        return df[df["BounceGuard_Status"].isin([STATUS_ROLE_BASED, STATUS_TYPO, STATUS_UNKNOWN])]
    if filter_choice == "🚨 Suppress":
        return df[df["BounceGuard_Status"].isin([STATUS_HIGH_RISK, STATUS_DISPOSABLE, STATUS_NO_MX, STATUS_SANDBOX_INVALID])]
    if filter_choice == "🔬 Deep Scan Candidates":
        return df[df.get("DeepScan_Eligible", "").eq("Yes")]
    if filter_choice == "⚪ Empty":
        return df[df["BounceGuard_Status"].eq(STATUS_EMPTY)]
    return df


# ============================================================
# SIDEBAR SETTINGS
# ============================================================

st.sidebar.header("BounceGuard Settings")

auto_deep_scan_default = st.sidebar.checkbox(
    "Auto deep scan clean questionable records",
    value=True,
    help="Runs a second-pass multi-resolver DNS check only on clean, non-role-based questionable records."
)

api_provider_default = st.sidebar.selectbox(
    "Optional verification API",
    ["None", "ZeroBounce", "NeverBounce"],
    index=0,
    help="Only runs on clean deep-scan candidates. API key required."
)

api_key_default = st.sidebar.text_input(
    "Verification API key",
    value="",
    type="password",
    help="Optional. Leave blank to skip the API pass."
)

st.sidebar.caption(
    "Deep scan skips Salesforce .invalid emails, role-based addresses like info@ or sales@, obvious junk, disposable emails, and typo domains."
)


# ============================================================
# TABS
# ============================================================

tab_single, tab_bulk, tab_about = st.tabs([
    "🎯 Quick Check",
    "📁 Bulk List Scrubber",
    "ℹ️ About"
])


# ============================================================
# TAB 1: SINGLE CHECK
# ============================================================

with tab_single:
    st.markdown("### Real-Time Email Risk Check")
    st.markdown(
        "Checks email format, obvious junk patterns, role-based addresses, common typos, disposable domains, "
        "and whether the domain appears configured to receive email."
    )

    single_email = st.text_input("Enter Email Address:", placeholder="name@company.com")

    single_col_a, single_col_b = st.columns([1, 1])
    with single_col_a:
        run_single = st.button("Verify Address", type="primary")
    with single_col_b:
        run_single_deep = st.checkbox("Include deep scan if questionable", value=True)

    if run_single:
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

                if run_single_deep and final_result.status in [STATUS_NO_MX, STATUS_UNKNOWN]:
                    temp_df = pd.DataFrame([{
                        "Email": final_result.clean_email,
                        "BounceGuard_Status": final_result.status,
                        "BounceGuard_Reason": final_result.reason,
                        "BounceGuard_Recommendation": final_result.recommendation
                    }])

                    eligible, reason = is_deep_scan_candidate(temp_df.iloc[0], "Email")
                    st.markdown("### Deep Scan")
                    if not eligible:
                        st.info(f"Deep scan skipped: {reason}")
                    else:
                        with st.spinner("Running deep scan..."):
                            loop = asyncio.new_event_loop()
                            asyncio.set_event_loop(loop)
                            deep_scanner = DeepDomainScanner(max_concurrent=10)
                            deep_df = loop.run_until_complete(deep_scanner.process_candidates(temp_df, "Email"))
                            st.dataframe(deep_df, use_container_width=True)


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

        col_settings_a, col_settings_b = st.columns([1, 1])
        with col_settings_a:
            heal_data = st.checkbox(
                "Self-Heal Suppressed Emails",
                value=False,
                help="Clears high-risk emails and stores the original value in Legacy_Invalid_Email."
            )

        with col_settings_b:
            auto_deep_scan = st.checkbox(
                "Run automatic deep scan",
                value=auto_deep_scan_default,
                help="Only checks clean questionable records after the first pass."
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
            progress_bar.empty()

            # Add deep scan columns and run only on clean questionable records.
            if auto_deep_scan:
                st.info("Running deep scan on clean questionable records only...")
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)

                deep_scanner = DeepDomainScanner(max_concurrent=50)
                df_final = loop.run_until_complete(deep_scanner.process_candidates(df_final, target_col))

                deep_candidates = (df_final["DeepScan_Eligible"] == "Yes").sum()

                if api_provider_default != "None" and api_key_default and deep_candidates > 0:
                    st.info(f"Running {api_provider_default} verification API on {deep_candidates:,} deep scan candidates...")
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    df_final = loop.run_until_complete(
                        run_verification_api_on_deep_candidates(
                            df_final,
                            target_col,
                            api_provider_default,
                            api_key_default,
                            max_concurrent=10
                        )
                    )
                else:
                    if "VerificationAPI_Status" not in df_final.columns:
                        df_final["VerificationAPI_Status"] = DEEP_API_SKIPPED
            else:
                df_final["DeepScan_Eligible"] = "No"
                df_final["DeepScan_Status"] = DEEP_NOT_NEEDED
                df_final["DeepScan_Reason"] = "Automatic deep scan was turned off."
                df_final["DeepScan_Recommendation"] = ""
                df_final["DeepScan_MX_Resolvers_Found"] = ""
                df_final["DeepScan_Resolver_Errors"] = ""
                df_final["VerificationAPI_Status"] = DEEP_API_SKIPPED

            df_final = add_risk_score_columns(df_final)

            suppress_statuses = [STATUS_HIGH_RISK, STATUS_DISPOSABLE, STATUS_NO_MX, STATUS_SANDBOX_INVALID, STATUS_TYPO]
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
            caution = df_final["BounceGuard_Status"].isin([STATUS_ROLE_BASED, STATUS_TYPO, STATUS_UNKNOWN]).sum()
            suppress = df_final["BounceGuard_Status"].isin([STATUS_HIGH_RISK, STATUS_DISPOSABLE, STATUS_NO_MX, STATUS_SANDBOX_INVALID]).sum()
            empty = df_final["BounceGuard_Status"].eq(STATUS_EMPTY).sum()
            deep_candidates = (df_final.get("DeepScan_Eligible", "") == "Yes").sum() if "DeepScan_Eligible" in df_final.columns else 0
            deep_recovered = (df_final.get("DeepScan_Status", "") == DEEP_MX_CONFIRMED).sum() if "DeepScan_Status" in df_final.columns else 0

            st.markdown("### 🏆 Protection Report")
            col_a, col_b, col_c, col_d, col_e, col_f = st.columns(6)
            col_a.metric("Emails Processed", f"{st.session_state.total_processed:,}")
            col_b.metric("✅ Domain Verified", f"{domain_verified:,}")
            col_c.metric("⚠️ Caution / Review", f"{caution:,}")
            col_d.metric("🚨 Suppress", f"{suppress:,}", delta="Risk Reduced", delta_color="normal")
            col_e.metric("🔬 Deep Scan Candidates", f"{deep_candidates:,}")
            col_f.metric("Recovered by Deep Scan", f"{deep_recovered:,}")

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
                * **First-Pass DNS Checks:** {st.session_state.dns_ping_count:,}
                * **Clean Deep Scan Candidates:** {deep_candidates:,}
                * **Recovered by Deep Scan:** {deep_recovered:,}

                **Local Efficiency Rate:** **{efficiency_rate:.1f}%** of this file was handled by local validation before DNS checks.
                """)

            st.markdown("### 🔍 Data Explorer")
            filter_choice = st.radio(
                "Filter Results:",
                ["All Records", "✅ Domain Verified", "⚠️ Caution / Review", "🚨 Suppress", "🔬 Deep Scan Candidates", "⚪ Empty"],
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
                "BounceGuard_Suggested_Fix",
                "DeepScan_Eligible",
                "DeepScan_Status",
                "DeepScan_Reason",
                "DeepScan_Recommendation",
                "VerificationAPI_Status",
                "VerificationAPI_Reason"
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
# TAB 3: SIMPLE CUSTOMER-FACING ABOUT
# ============================================================

with tab_about:
    st.markdown("### What BounceGuard Does")

    st.markdown("""
    BounceGuard helps clean email lists before a campaign by checking for common problems that can cause bounces or hurt sender reputation.

    **It checks for:**

    * Missing emails
    * Bad email formatting
    * Fake or placeholder values like `unknown@unknown.com`
    * Salesforce sandbox `.invalid` emails
    * Role-based addresses like `info@`, `sales@`, and `admin@`
    * Common domain typos like `gmial.com`
    * Disposable or temporary email domains
    * Domains that do not appear configured to receive email
    * Questionable domains that deserve a deeper second pass

    **Deep Scan**

    When enabled, BounceGuard runs an extra check only on clean questionable records. It skips obvious junk, Salesforce sandbox emails, role-based addresses, and typo domains. The deeper check asks multiple DNS providers whether the domain can receive email.

    **Important**

    Domain verification means the domain appears able to receive email. It does not always prove the exact mailbox exists.
    """)

    st.info(
        "For the highest-confidence results, connect a verification API such as ZeroBounce or NeverBounce. "
        "BounceGuard will only use that API on the small group of clean questionable records."
    )
