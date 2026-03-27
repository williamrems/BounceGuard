"""
app.py
BounceGuard by ContractorFlow
Standalone Email Deliverability & DNS MX Validator (Dual-Mode)
"""
import streamlit as st
import pandas as pd
import asyncio
import aiohttp
import re
import io
import math
import os

st.set_page_config(page_title="BounceGuard | ContractorFlow", page_icon="🛡️", layout="centered")

# --- BRANDING ---
col_logo, col_title = st.columns([1, 4])
with col_logo:
    # Drop a file named 'logo.png' into your repo, and it will render here automatically.
    if os.path.exists("logo.png"):
        st.image("logo.png", use_container_width=True)
    else:
        st.markdown("<h1>🛡️</h1>", unsafe_allow_html=True)
with col_title:
    st.title("BounceGuard")
    st.caption("Powered by ContractorFlow Data Architecture")

# --- CONSTANTS & TRAPS ---
FAKE_LOCAL_PARTS = {'test', 'something', 'anything', 'fake', 'email', 'noemail', 'donotemail', 'spam', 'customer', 'na', 'none', 'no'}
GENERIC_EMAIL_PREFIXES = {'info', 'admin', 'sales', 'support', 'contact', 'hello', 'office'}
FAKE_EMAILS_FULL = {'na@na.com', 'none@none.com', 'na@gmail.com', 'none@gmail.com', 'test@test.com', 'email@email.com', 'no@email.com'}

# --- REGEX ENGINE ---
def format_and_trap_email(email):
    """Phase 1: Local Regex & Syntax Validation"""
    if pd.isna(email) or str(email).strip() == '' or str(email).strip().lower() == 'nan': 
        return "", "EMPTY"
        
    clean_str = str(email).strip().lower()
    
    if '..' in clean_str: return clean_str, "INVALID_FORMAT: (DOUBLE PERIOD)"
    if clean_str in FAKE_EMAILS_FULL: return clean_str, "INVALID_FORMAT: (KNOWN FAKE)"
    if not re.match(r'^[\w\.-]+@[\w\.-]+\.\w+$', clean_str): return clean_str, "INVALID_FORMAT: (SYNTAX)"
    
    try:
        local_part = clean_str.split('@')[0]
        domain_part = clean_str.split('@')[1]
        
        if re.search(r'\d+bad\d+', local_part) or local_part == 'bad':
            return clean_str, "INVALID_FORMAT: (SUSPECT SPAM)"
            
        if local_part in FAKE_LOCAL_PARTS or domain_part in {'example.com', 'fake.com'}:
            return clean_str, "INVALID_FORMAT: (KNOWN FAKE)"
            
        if re.search(r'(fake|demo|test|mock)', domain_part):
            return clean_str, "INVALID_FORMAT: (SUSPECT DOMAIN)"
            
        if local_part in GENERIC_EMAIL_PREFIXES:
            return clean_str, "WARNING: (GENERIC PREFIX)"
            
    except Exception:
        pass
        
    return clean_str, "PENDING_DNS"

# --- ASYNC DNS-OVER-HTTPS ENGINE ---
class EmailDomainValidator:
    def __init__(self, max_concurrent: int = 150):
        self.semaphore = asyncio.Semaphore(max_concurrent)

    async def _check_mx(self, session: aiohttp.ClientSession, domain: str) -> str:
        async with self.semaphore:
            try:
                url = f"https://dns.google/resolve?name={domain}&type=MX"
                async with session.get(url, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data.get('Status') == 0 and 'Answer' in data:
                            return "VALID_DOMAIN"
                        elif data.get('Status') == 3:
                            return "DEAD_DOMAIN"
                        else:
                            return "NO_MX_RECORDS"
                    else:
                        return "DNS_TIMEOUT"
            except Exception:
                return "DNS_TIMEOUT"

    async def check_single(self, domain: str) -> str:
        """Bypass for the Single-Check Terminal"""
        async with aiohttp.ClientSession() as session:
            return await self._check_mx(session, domain)

    async def process_batch(self, df: pd.DataFrame, email_col: str) -> pd.DataFrame:
        df_result = df.copy()
        if 'Email_Domain_Status' not in df_result.columns:
            df_result['Email_Domain_Status'] = ''

        tasks = []
        indices = []

        async with aiohttp.ClientSession() as session:
            for idx, row in df_result.iterrows():
                current_status = row.get('Email_Domain_Status', '')
                
                if current_status in ["PENDING_DNS", "WARNING: (GENERIC PREFIX)"]:
                    email = str(row.get(email_col, ''))
                    domain = email.split('@')[-1]
                    
                    tasks.append(self._check_mx(session, domain))
                    indices.append(idx)

            if tasks:
                results = await asyncio.gather(*tasks)
                for i, res in enumerate(results):
                    original_status = df_result.at[indices[i], 'Email_Domain_Status']
                    if original_status == "WARNING: (GENERIC PREFIX)" and res == "VALID_DOMAIN":
                        df_result.at[indices[i], 'Email_Domain_Status'] = "VALID_DOMAIN (GENERIC PREFIX)"
                    else:
                        df_result.at[indices[i], 'Email_Domain_Status'] = res

        return df_result

# --- EXCEL GENERATOR ---
def generate_excel(df):
    output = io.BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    df.to_excel(writer, index=False, sheet_name='Validated Emails')
    
    workbook = writer.book
    worksheet = writer.sheets['Validated Emails']
    worksheet.freeze_panes(1, 0)
    
    if not df.empty:
        worksheet.autofilter(0, 0, len(df), len(df.columns) - 1)
        
        status_idx = df.columns.get_loc('Email_Domain_Status')
        red_fmt = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})
        green_fmt = workbook.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100'})
        yellow_fmt = workbook.add_format({'bg_color': '#FFF2CC', 'font_color': '#9C6500'})
        gray_fmt = workbook.add_format({'font_color': '#7F7F7F'})
        
        worksheet.conditional_format(1, status_idx, len(df), status_idx, {'type': 'text', 'criteria': 'containing', 'value': 'VALID_DOMAIN', 'format': green_fmt})
        worksheet.conditional_format(1, status_idx, len(df), status_idx, {'type': 'text', 'criteria': 'containing', 'value': 'DEAD', 'format': red_fmt})
        worksheet.conditional_format(1, status_idx, len(df), status_idx, {'type': 'text', 'criteria': 'containing', 'value': 'INVALID', 'format': red_fmt})
        worksheet.conditional_format(1, status_idx, len(df), status_idx, {'type': 'text', 'criteria': 'containing', 'value': 'NO_MX', 'format': red_fmt})
        worksheet.conditional_format(1, status_idx, len(df), status_idx, {'type': 'text', 'criteria': 'containing', 'value': 'GENERIC', 'format': yellow_fmt})
        worksheet.conditional_format(1, status_idx, len(df), status_idx, {'type': 'text', 'criteria': 'containing', 'value': 'EMPTY', 'format': gray_fmt})

    for idx, col in enumerate(df.columns):
        max_len = max(df[col].astype(str).map(len).max(), len(col)) + 2
        worksheet.set_column(idx, idx, min(max_len, 40))
        
    writer.close()
    return output.getvalue()


# --- UI ROUTING ---
tab_single, tab_bulk = st.tabs(["🎯 Quick Check Terminal", "📁 Bulk List Engine"])

# ==========================================
# TAB 1: SINGLE CHECK TERMINAL
# ==========================================
with tab_single:
    st.markdown("### Real-Time Deliverability Check")
    st.markdown("Instantly ping a single email address against global DNS registries.")
    
    single_email = st.text_input("Enter Email Address:", placeholder="name@company.com")
    
    if st.button("Verify Address", type="primary"):
        if not single_email:
            st.warning("Please enter an email address.")
        else:
            with st.spinner("Analyzing syntax and pinging MX records..."):
                clean_em, status = format_and_trap_email(single_email)
                
                if status == "PENDING_DNS" or status == "WARNING: (GENERIC PREFIX)":
                    domain = clean_em.split('@')[-1]
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    validator = EmailDomainValidator()
                    dns_result = loop.run_until_complete(validator.check_single(domain))
                    
                    if status == "WARNING: (GENERIC PREFIX)" and dns_result == "VALID_DOMAIN":
                        final_status = "VALID_DOMAIN (GENERIC PREFIX)"
                    else:
                        final_status = dns_result
                else:
                    final_status = status
                    
                st.markdown("---")
                if "VALID_DOMAIN" in final_status:
                    st.success(f"**{clean_em}**")
                    st.success(f"✅ **{final_status}**: The domain is active and configured to receive email.")
                elif "WARNING" in final_status:
                    st.warning(f"**{clean_em}**")
                    st.warning(f"⚠️ **{final_status}**: The domain is active, but this is a role-based address (info@, admin@).")
                else:
                    st.error(f"**{clean_em}**")
                    st.error(f"🚨 **{final_status}**: This email will bounce. Do not send.")

# ==========================================
# TAB 2: BULK LIST ENGINE
# ==========================================
with tab_bulk:
    st.markdown("### Enterprise Batch Processor")
    uploaded_file = st.file_uploader("Upload Data (.csv or .xlsx)", type=['csv', 'xlsx'])

    if uploaded_file:
        if uploaded_file.name.endswith('.csv'):
            df = pd.read_csv(uploaded_file)
        else:
            df = pd.read_excel(uploaded_file)
            
        st.success(f"File loaded successfully! ({len(df):,} rows)")
        
        columns = list(df.columns)
        guess_idx = 0
        for i, col in enumerate(columns):
            if 'email' in col.lower():
                guess_idx = i
                break
                
        st.markdown("---")
        col1, col2 = st.columns([2, 1])
        with col1:
            target_col = st.selectbox("🎯 Target Email Column:", options=columns, index=guess_idx)
        with col2:
            st.markdown("<br>", unsafe_allow_html=True)
            heal_data = st.checkbox("Self-Heal Dead Emails", value=False, help="Automatically moves bad emails to a 'Legacy_Invalid_Email' column and clears the primary field to protect your CRM.")

        if st.button("🚀 Run Batch Validation", type="primary", use_container_width=True):
            st.info("💡 Tip: You can cancel this process at any time by clicking 'Stop' in the top right.")
            
            with st.spinner("Applying regex formatting and spam traps..."):
                df['Email_Domain_Status'] = ''
                for idx, row in df.iterrows():
                    raw_email = row[target_col]
                    clean_em, status = format_and_trap_email(raw_email)
                    df.at[idx, target_col] = clean_em
                    df.at[idx, 'Email_Domain_Status'] = status
                    
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            chunk_size = 1000
            num_chunks = math.ceil(len(df) / chunk_size)
            
            progress_bar = st.progress(0, text=f"Pinging DNS registries... (0/{len(df):,})")
            validator = EmailDomainValidator(max_concurrent=150)
            
            processed_chunks = []
            for i in range(num_chunks):
                chunk = df.iloc[i*chunk_size : (i+1)*chunk_size]
                chunk_res = loop.run_until_complete(validator.process_batch(chunk, target_col))
                processed_chunks.append(chunk_res)
                
                records_done = min((i+1)*chunk_size, len(df))
                progress_bar.progress((i + 1) / num_chunks, text=f"Pinging DNS registries... ({records_done:,}/{len(df):,})")

            df_final = pd.concat(processed_chunks, ignore_index=True)
            progress_bar.empty()
            
            if heal_data:
                bad_statuses = ['DEAD_DOMAIN', 'NO_MX_RECORDS', 'INVALID_FORMAT']
                mask_dead = df_final['Email_Domain_Status'].str.contains('|'.join(bad_statuses))
                
                df_final['Legacy_Invalid_Email'] = ''
                df_final.loc[mask_dead, 'Legacy_Invalid_Email'] = df_final.loc[mask_dead, target_col]
                df_final.loc[mask_dead, target_col] = ''
                st.success(f"✅ Validation Complete! Automatically quarantined {mask_dead.sum():,} bad emails.")
            else:
                st.success("✅ Validation Complete!")
                
            st.dataframe(df_final.head(100))
            
            st.download_button(
                label="📥 Download Validated List (.xlsx)",
                data=generate_excel(df_final),
                file_name="BounceGuard_Validated_List.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                type="primary",
                use_container_width=True
            )
