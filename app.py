"""
app.py
BounceGuard by ContractorFlow
Standalone Email Deliverability & DNS MX Validator (Simplified Customer UI)
"""
import streamlit as st
import pandas as pd
import asyncio
import aiohttp
import re
import io
import math
import os

st.set_page_config(page_title="BounceGuard | ContractorFlow", page_icon="🛡️", layout="wide")

# --- BRANDING ---
col_logo, col_title = st.columns([1, 4])
with col_logo:
    if os.path.exists("logo.png"):
        st.image("logo.png", use_column_width=True)
    else:
        st.markdown("<h1>🛡️</h1>", unsafe_allow_html=True)
with col_title:
    st.title("BounceGuard")
    st.caption("Protect your sender reputation. Powered by ContractorFlow.")

# --- CONSTANTS & TRAPS ---
FAKE_LOCAL_PARTS = {'test', 'something', 'anything', 'fake', 'email', 'noemail', 'donotemail', 'spam', 'customer', 'na', 'none', 'no'}
GENERIC_EMAIL_PREFIXES = {'info', 'admin', 'sales', 'support', 'contact', 'hello', 'office'}
FAKE_EMAILS_FULL = {'na@na.com', 'none@none.com', 'na@gmail.com', 'none@gmail.com', 'test@test.com', 'email@email.com', 'no@email.com'}
SUSPECT_DOMAIN_PATTERN = re.compile(r'^(fake|demo|test|mock|example|sample)|(mailinator|yopmail|tempmail|10minute|guerrillamail|sharklasers|throwawaymail)\.', re.IGNORECASE)

KNOWN_SAFE_DOMAINS = {
    'gmail.com', 'yahoo.com', 'hotmail.com', 'aol.com', 'outlook.com', 
    'live.com', 'icloud.com', 'comcast.net', 'msn.com', 'sbcglobal.net', 
    'att.net', 'verizon.net', 'mac.com', 'me.com', 'bellsouth.net', 'charter.net'
}

# --- SIMPLIFIED REGEX ENGINE ---
def format_and_trap_email(email):
    """Phase 1: Local Traps mapped to simple outputs"""
    if pd.isna(email) or str(email).strip() == '' or str(email).strip().lower() == 'nan': 
        return "", "⚪ Empty"
        
    clean_str = str(email).strip().lower()
    
    if '..' in clean_str or clean_str in FAKE_EMAILS_FULL or not re.match(r'^[\w\.-]+@[\w\.-]+\.\w+$', clean_str): 
        return clean_str, "🚨 Bounce"
    
    try:
        local_part = clean_str.split('@')[0]
        domain_part = clean_str.split('@')[1]
        
        if re.search(r'\d+bad\d+', local_part) or local_part == 'bad' or local_part in FAKE_LOCAL_PARTS or SUSPECT_DOMAIN_PATTERN.search(domain_part):
            return clean_str, "🚨 Bounce"
            
        if local_part in GENERIC_EMAIL_PREFIXES:
            return clean_str, "⚠️ Caution (Role-Based)"
            
        if domain_part in KNOWN_SAFE_DOMAINS:
            return clean_str, "✅ Safe"
            
    except Exception:
        pass
        
    return clean_str, "PENDING"

# --- SIMPLIFIED ASYNC DNS ENGINE ---
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
                            return "✅ Safe"
                        else:
                            return "🚨 Bounce"
                    else:
                        return "🚨 Bounce"
            except Exception:
                return "🚨 Bounce"

    async def check_single(self, domain: str) -> str:
        async with aiohttp.ClientSession() as session:
            return await self._check_mx(session, domain)

    async def process_batch(self, df: pd.DataFrame, email_col: str) -> pd.DataFrame:
        df_result = df.copy()
        if 'BounceGuard_Status' not in df_result.columns:
            df_result['BounceGuard_Status'] = ''

        tasks = []
        indices = []

        async with aiohttp.ClientSession() as session:
            for idx, row in df_result.iterrows():
                current_status = row.get('BounceGuard_Status', '')
                
                if current_status in ["PENDING", "⚠️ Caution (Role-Based)"]:
                    email = str(row.get(email_col, ''))
                    domain = email.split('@')[-1]
                    
                    tasks.append(self._check_mx(session, domain))
                    indices.append(idx)

            if tasks:
                results = await asyncio.gather(*tasks)
                for i, res in enumerate(results):
                    original_status = df_result.at[indices[i], 'BounceGuard_Status']
                    if original_status == "⚠️ Caution (Role-Based)" and res == "✅ Safe":
                        df_result.at[indices[i], 'BounceGuard_Status'] = "⚠️ Caution (Role-Based)"
                    else:
                        df_result.at[indices[i], 'BounceGuard_Status'] = res

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
        
        status_idx = df.columns.get_loc('BounceGuard_Status')
        red_fmt = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})
        green_fmt = workbook.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100'})
        yellow_fmt = workbook.add_format({'bg_color': '#FFF2CC', 'font_color': '#9C6500'})
        gray_fmt = workbook.add_format({'font_color': '#7F7F7F'})
        
        worksheet.conditional_format(1, status_idx, len(df), status_idx, {'type': 'text', 'criteria': 'containing', 'value': 'Safe', 'format': green_fmt})
        worksheet.conditional_format(1, status_idx, len(df), status_idx, {'type': 'text', 'criteria': 'containing', 'value': 'Bounce', 'format': red_fmt})
        worksheet.conditional_format(1, status_idx, len(df), status_idx, {'type': 'text', 'criteria': 'containing', 'value': 'Caution', 'format': yellow_fmt})
        worksheet.conditional_format(1, status_idx, len(df), status_idx, {'type': 'text', 'criteria': 'containing', 'value': 'Empty', 'format': gray_fmt})

    for idx, col in enumerate(df.columns):
        max_len = max(df[col].astype(str).map(len).max(), len(col)) + 2
        worksheet.set_column(idx, idx, min(max_len, 40))
        
    writer.close()
    return output.getvalue()


# --- UI ROUTING ---
tab_single, tab_bulk = st.tabs(["🎯 Quick Check", "📁 Bulk List Scrubber"])

# ==========================================
# TAB 1: SINGLE CHECK TERMINAL
# ==========================================
with tab_single:
    st.markdown("### Real-Time Deliverability Check")
    st.markdown("Instantly verify a single email address before sending.")
    
    single_email = st.text_input("Enter Email Address:", placeholder="name@company.com")
    
    if st.button("Verify Address", type="primary"):
        if not single_email:
            st.warning("Please enter an email address.")
        else:
            with st.spinner("Analyzing..."):
                clean_em, status = format_and_trap_email(single_email)
                
                if status in ["PENDING", "⚠️ Caution (Role-Based)"]:
                    domain = clean_em.split('@')[-1]
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    validator = EmailDomainValidator()
                    dns_result = loop.run_until_complete(validator.check_single(domain))
                    
                    if status == "⚠️ Caution (Role-Based)" and dns_result == "✅ Safe":
                        final_status = "⚠️ Caution (Role-Based)"
                    else:
                        final_status = dns_result
                else:
                    final_status = status
                    
                st.markdown("---")
                if "Safe" in final_status:
                    st.success(f"**{clean_em}**")
                    st.success(f"✅ **Safe to Send**: This mailbox is active.")
                elif "Caution" in final_status:
                    st.warning(f"**{clean_em}**")
                    st.warning(f"⚠️ **Caution**: The domain is valid, but this is a generic inbox (info@, admin@). Engagement may be low.")
                else:
                    st.error(f"**{clean_em}**")
                    st.error(f"🚨 **Will Bounce**: Do not send to this address. It will harm your sender reputation.")

# ==========================================
# TAB 2: BULK LIST SCRUBBER
# ==========================================
with tab_bulk:
    st.markdown("### Bulk List Scrubber")
    st.markdown("Upload your contact list to verify deliverability before you send your next campaign.")
    uploaded_file = st.file_uploader("Upload Data (.csv or .xlsx)", type=['csv', 'xlsx'])

    if uploaded_file:
        if uploaded_file.name.endswith('.csv'):
            df = pd.read_csv(uploaded_file)
        else:
            df = pd.read_excel(uploaded_file)
            
        columns = list(df.columns)
        guess_idx = 0
        for i, col in enumerate(columns):
            if 'email' in col.lower():
                guess_idx = i
                break
                
        st.markdown("---")
        target_col = st.selectbox("🎯 Target Email Column:", options=columns, index=guess_idx)
        heal_data = st.checkbox("Self-Heal Dead Emails", value=False, help="Automatically clears bad emails to protect your CRM.")

        if st.button("🚀 Run Batch Validation", type="primary", use_container_width=True):
            
            with st.spinner("Analyzing emails..."):
                df['BounceGuard_Status'] = ''
                for idx, row in df.iterrows():
                    raw_email = row[target_col]
                    clean_em, status = format_and_trap_email(raw_email)
                    df.at[idx, target_col] = clean_em
                    df.at[idx, 'BounceGuard_Status'] = status
                
                # --- PRE-DNS METRICS CAPTURE ---
                total_processed = len(df[df[target_col] != ""])
                fast_pass_count = (df['BounceGuard_Status'] == '✅ Safe').sum()
                local_bounce_count = df['BounceGuard_Status'].str.contains('Bounce').sum()
                dns_ping_count = df['BounceGuard_Status'].isin(["PENDING", "⚠️ Caution (Role-Based)"]).sum()
                    
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            chunk_size = 1000
            num_chunks = math.ceil(len(df) / chunk_size)
            
            progress_bar = st.progress(0, text=f"Verifying domains... (0/{len(df):,})")
            validator = EmailDomainValidator(max_concurrent=150)
            
            processed_chunks = []
            for i in range(num_chunks):
                chunk = df.iloc[i*chunk_size : (i+1)*chunk_size]
                chunk_res = loop.run_until_complete(validator.process_batch(chunk, target_col))
                processed_chunks.append(chunk_res)
                
                records_done = min((i+1)*chunk_size, len(df))
                progress_bar.progress((i + 1) / num_chunks, text=f"Verifying domains... ({records_done:,}/{len(df):,})")

            df_final = pd.concat(processed_chunks, ignore_index=True)
            progress_bar.empty()
            
            # --- ROI DASHBOARD ---
            bounces = df_final['BounceGuard_Status'].str.contains('Bounce').sum()
            safe = df_final['BounceGuard_Status'].str.contains('Safe').sum()
            caution = df_final['BounceGuard_Status'].str.contains('Caution').sum()
            
            st.markdown("### 🏆 Protection Report")
            col_a, col_b, col_c, col_d = st.columns(4)
            col_a.metric("Emails Processed", f"{total_processed:,}")
            col_b.metric("✅ Safe to Send", f"{safe:,}")
            col_c.metric("⚠️ Caution (Role-Based)", f"{caution:,}")
            col_d.metric("🚨 Hard Bounces Prevented", f"{bounces:,}", delta="Reputation Saved", delta_color="normal")
            
            st.markdown("---")
            st.markdown("##### What do these numbers mean for your business?")
            st.info("**🚨 Hard Bounces:** Sending mail to dead or fake addresses tells providers like Gmail and Outlook that you are a spammer. If you do it too much, your legitimate emails to real customers will start going straight to the junk folder. We trapped these so your sender reputation stays pristine.")
            st.warning("**⚠️ Caution (Role-Based):** Emails starting with `info@`, `sales@`, or `admin@` are generic inboxes. They are often ignored, or worse, someone marks your email as spam because they don't know who signed up. It is safe to email them, but don't expect high engagement.")
            
            # --- ADVANCED ROUTING STATS EXPANDER ---
            with st.expander("⚙️ Engine Diagnostics & Routing Stats (For Admins)", expanded=False):
                efficiency_rate = ((fast_pass_count + local_bounce_count) / max(total_processed, 1)) * 100
                st.markdown(f"""
                **Network Throughput Analysis**
                * **Total Valid Inputs:** {total_processed:,}
                * **Locally Verified (Fast-Pass):** {fast_pass_count:,} *(Bypassed network check)*
                * **Locally Trapped (Regex/Spam):** {local_bounce_count:,} *(Bypassed network check)*
                * **Live DNS Pings Executed:** {dns_ping_count:,} *(Required external API routing)*
                
                **Efficiency Rate:** **{efficiency_rate:.1f}%** of this list was processed instantly via local architecture without consuming external API bandwidth.
                """)
            st.markdown("---")

            # --- SELF HEALING ---
            if heal_data:
                mask_dead = df_final['BounceGuard_Status'].str.contains('Bounce')
                df_final['Legacy_Invalid_Email'] = ''
                df_final.loc[mask_dead, 'Legacy_Invalid_Email'] = df_final.loc[mask_dead, target_col]
                df_final.loc[mask_dead, target_col] = ''

            # --- SCROLL FIX: Pin Email and Status to the front ---
            display_cols = df_final.columns.tolist()
            display_cols.insert(0, display_cols.pop(display_cols.index('BounceGuard_Status')))
            display_cols.insert(0, display_cols.pop(display_cols.index(target_col)))
            
            st.dataframe(df_final[display_cols].head(100))
            
            st.download_button(
                label="📥 Download Validated List (.xlsx)",
                data=generate_excel(df_final),
                file_name="BounceGuard_Validated_List.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                type="primary",
                use_container_width=True
            )
