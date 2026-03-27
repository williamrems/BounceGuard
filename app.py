"""
app.py
Standalone Email Deliverability & DNS MX Validator
Designed for deployment on PaaS providers like Railway.app
"""
import streamlit as st
import pandas as pd
import asyncio
import aiodns
import re
import io
import math

st.set_page_config(page_title="Email Deliverability Engine", page_icon="📧", layout="centered")

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
            
        if local_part in GENERIC_EMAIL_PREFIXES:
            return clean_str, "WARNING: (GENERIC PREFIX)"
            
    except Exception:
        pass
        
    return clean_str, "PENDING_DNS"

# --- ASYNC DNS ENGINE ---
class EmailDomainValidator:
    def __init__(self, max_concurrent: int = 150):
        self.semaphore = asyncio.Semaphore(max_concurrent)

    async def _check_mx(self, domain: str, resolver: aiodns.DNSResolver) -> str:
        async with self.semaphore:
            try:
                answers = await resolver.query(domain, 'MX')
                return "VALID_DOMAIN" if answers else "NO_MX_RECORDS"
            except aiodns.error.DNSError:
                return "DEAD_DOMAIN"
            except Exception:
                return "DNS_TIMEOUT"

    async def process_batch(self, df: pd.DataFrame, email_col: str) -> pd.DataFrame:
        df_result = df.copy()
        
        if 'Email_Domain_Status' not in df_result.columns:
            df_result['Email_Domain_Status'] = ''

        # THE FIX: Remove hardcoded nameservers so Railway uses its native internal resolver.
        # This bypasses the Port 53 outbound UDP firewall block.
        resolver = aiodns.DNSResolver()
        
        tasks = []
        indices = []

        for idx, row in df_result.iterrows():
            current_status = row.get('Email_Domain_Status', '')
            
            # Only ping domains that passed the Regex Phase
            if current_status in ["PENDING_DNS", "WARNING: (GENERIC PREFIX)"]:
                email = str(row.get(email_col, ''))
                domain = email.split('@')[-1]
                
                tasks.append(self._check_mx(domain, resolver))
                indices.append(idx)

        if tasks:
            results = await asyncio.gather(*tasks)
            for i, res in enumerate(results):
                original_status = df_result.at[indices[i], 'Email_Domain_Status']
                # Preserve the generic warning if the domain is valid
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
        
        # Conditional Formatting for the Status Column
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

# --- STREAMLIT UI ---
st.title("📧 Email Deliverability Engine")
st.markdown("Upload a list, target the email column, and ping global DNS registries to verify Mail Exchange (MX) records. Dead domains and bad formats are flagged instantly.")

uploaded_file = st.file_uploader("Upload Data (.csv or .xlsx)", type=['csv', 'xlsx'])

if uploaded_file:
    if uploaded_file.name.endswith('.csv'):
        df = pd.read_csv(uploaded_file)
    else:
        df = pd.read_excel(uploaded_file)
        
    st.success(f"File loaded successfully! ({len(df):,} rows)")
    
    # --- AUTO-DETECT EMAIL COLUMN ---
    columns = list(df.columns)
    guess_idx = 0
    for i, col in enumerate(columns):
        if 'email' in col.lower():
            guess_idx = i
            break
            
    st.markdown("---")
    col1, col2 = st.columns([2, 1])
    with col1:
        target_col = st.selectbox("🎯 Select the column containing Email Addresses:", options=columns, index=guess_idx)
    with col2:
        st.markdown("<br>", unsafe_allow_html=True)
        heal_data = st.checkbox("Self-Heal Dead Emails", value=False, help="Automatically moves bad emails to a 'Legacy_Invalid_Email' column and clears the primary field to protect your CRM.")

    if st.button("🚀 Run DNS Validation", type="primary", use_container_width=True):
        st.info("💡 Tip: You can cancel this process at any time by clicking 'Stop' in the top right.")
        
        # Phase 1: Local Regex Scrubber
        with st.spinner("Applying regex formatting and spam traps..."):
            df['Email_Domain_Status'] = ''
            for idx, row in df.iterrows():
                raw_email = row[target_col]
                clean_em, status = format_and_trap_email(raw_email)
                df.at[idx, target_col] = clean_em
                df.at[idx, 'Email_Domain_Status'] = status
                
        # Phase 2: Async DNS Engine
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
        
        # Phase 3: Self-Healing (Optional)
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
            file_name="Validated_Email_List.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            type="primary",
            use_container_width=True
        )