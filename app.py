import streamlit as st
import pandas as pd
import duckdb
import requests
import re
import io
from PIL import Image

# ---- CONFIG ---- #
API_KEY = "sk-153c4decdf4e4a79995c685af7fad5c8"
API_URL = "https://api.deepseek.com/v1/chat/completions"
ENCODINGS = ['utf-8', 'latin1', 'iso-8859-1', 'cp1252', 'utf-16', 'utf-32']
st.set_page_config(page_title="Lead Intelligence for Almo Media", layout="wide")

# ---- Custom CSS for Background and Rounded Boxes ---- #
st.markdown(
    """
    <style>
    body {
        background-color: #f0f2f6; /* Light grey background */
    }
    .rounded-box {
        background-color: white;
        padding: 20px;
        border-radius: 10px;
        margin-bottom: 15px;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1); /* Subtle shadow */
    }
    .header-container {
        display: flex;
        flex-direction: column; /* Arrange logo and text vertically */
        align-items: flex-start; /* Align items to the left */
        padding-bottom: 1rem;
        border-bottom: 1px solid #eee;
        margin-bottom: 1.5rem;
    }
    .logo-container {
        margin-bottom: 0.5rem; /* Space between logo and subtitle */
    }
    .logo-img {
        max-height: none; /* Remove max-height to allow larger scaling */
        width: 300px; /* Increased width for a larger logo */
    }
    .sub-title {
        font-size: 16px !important; /* Slightly smaller subtitle */
        line-height: 1.4; /* Improved readability */
        text-align: left !important; /* Ensure left alignment */
    }
    .block-container {
        padding-top: 2rem;
    }
    .st-expander {
        border: 1px solid #ddd;
        border-radius: 5px;
        margin-bottom: 0.5rem;
        background-color: white; /* Add white background to expanders */
        padding: 10px; /* Add some padding */
        box-shadow: 0 1px 3px rgba(0,0,0,0.05); /* Subtle shadow */
    }
    .st-metric {
        padding: 15px;
        border-radius: 5px;
        background-color: #f9f9f9;
        box-shadow: 0 1px 3px rgba(0,0,0,0.05);
    }
    .stDownloadButton {
        background-color: #0F52BA !important;
        color: white !important;
        font-weight: bold !important;
        border-radius: 5px !important;
        padding: 10px 20px !important;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        transition: background-color 0.3s ease;
    }
    .stDownloadButton:hover {
        background-color: #0A3D80 !important;
    }
    .stTextInput > div > div > input {
        border-radius: 5px;
        border: 1px solid #ccc;
        padding: 10px;
    }
    .horizontal-columns {
        display: flex;
        flex-wrap: wrap;
        gap: 5px; /* Adjust gap as needed */
    }
    .column-name {
        font-size: 0.9em;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# ---- Load Almo Media Logo ---- #
try:
    almo_logo = Image.open("logo.jpg")
except FileNotFoundError:
    almo_logo = None
    st.warning("⚠️ Almo Media logo not found at the specified path.")

# ---- Custom Header with Logo and Branding (Larger Logo) ---- #
with st.container():
    st.markdown('<div class="header-container">', unsafe_allow_html=True)
    if almo_logo:
        st.image(almo_logo, use_container_width=False, width=300) # Increased width
    st.markdown('<div class="sub-title">Unlock valuable insights from your lead data to drive strategic decisions. Powered by Yugensys Software.</div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

st.divider()

# ---- Upload File Section ---- #
st.markdown('<div class="rounded-box">', unsafe_allow_html=True)
st.subheader("📤 Upload Your Lead Data")
uploaded_file = st.file_uploader("Drag and drop your CSV or Excel file here", type=["csv", "xls", "xlsx"])
st.markdown('</div>', unsafe_allow_html=True)

if uploaded_file:
    try:
        df = read_file_with_encoding(uploaded_file)
        st.markdown('<div class="rounded-box">', unsafe_allow_html=True)
        st.success("✅ File successfully loaded.") # Removed encoding information
        st.markdown('</div>', unsafe_allow_html=True)
    except Exception as e:
        st.error(f"⚠️ Error loading file: {str(e)}")
        st.stop()

    # ---- Data Processing and Feature Engineering (No Changes Here) ---- #
    def clean_numeric_column(col):
        col = col.astype(str).str.replace(r"[\$,]", "", regex=True).str.lower()
        def convert(val):
            try:
                val = val.strip()
                if 'billion' in val: return float(re.findall(r'[0-9.]+', val)[0]) * 1e9
                elif 'million' in val: return float(re.findall(r'[0-9.]+', val)[0]) * 1e6
                elif 'thousand' in val: return float(re.findall(r'[0-9.]+', val)[0]) * 1e3
                elif 'b' in val: return float(val.replace('b', '')) * 1e9
                elif 'm' in val: return float(val.replace('m', '')) * 1e6
                elif 'k' in val: return float(val.replace('k', '')) * 1e3
                return float(val)
            except:
                return None
        return col.apply(convert)

    for col in df.columns:
        if df[col].dtype == 'object':
            sample = df[col].dropna().astype(str).str.lower()
            if sample.str.contains(r'\d', regex=True).any():
                converted = clean_numeric_column(df[col])
                df[col] = converted.fillna(df[col])
            else:
                df[col] = df[col].fillna("Unknown")
        else:
            df[col] = df[col].fillna(df[col].median())

    def extract_min_emp_size(size_str):
        try:
            size_str = str(size_str).replace(",", "").strip()
            if '+' in size_str: return int(re.findall(r'\d+', size_str)[0])
            elif '-' in size_str: return int(size_str.split('-')[0])
            else: return int(size_str)
        except:
            return None

    emp_columns = ["Employee Size range", "LinkedIn Emp Size", "Headcount", "Size"]
    fallback_columns = ["Revenue Size", "Annual Revenue"]

    emp_size_col = next((col for col in emp_columns if col in df.columns), None)
    fallback_col = next((col for col in fallback_columns if col in df.columns), None)

    if emp_size_col:
        df["Emp Size Num"] = df[emp_size_col].apply(extract_min_emp_size)
    elif fallback_col:
        df["Emp Size Num"] = df[fallback_col]

    if "Revenue Size" in df.columns:
        df["Revenue Size"] = clean_numeric_column(df["Revenue Size"])

    try:
        duckdb.unregister("leads")
    except:
        pass
    duckdb.register("leads", df)

    st.markdown('<div class="rounded-box">', unsafe_allow_html=True)
    st.success("✅ Data processing complete.")
    st.markdown('</div>', unsafe_allow_html=True)

    # ---- Data Overview Metrics (Styled) ---- #
    st.subheader("📊 Data Overview")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Leads", f"{len(df):,}")
    with col2:
        st.metric("Number of Attributes", len(df.columns))
    with col3:
        st.metric("Numeric Attributes", df.select_dtypes(include='number').shape[1])

    # ---- Optional Data Exploration (Styled Expanders) ---- #
    with st.expander("🔍 Explore Sample Data"):
        st.dataframe(df.head(10), use_container_width=True)

    with st.expander("📌 View All Column Names"):
        column_names_text = ", ".join(df.columns)
        st.markdown(f"<div style='font-size: 0.9em; white-space: pre-wrap;'>{column_names_text}</div>", unsafe_allow_html=True)

    # ---- Querying Section (Styled Input) ---- #
    st.subheader("🧠 Ask Questions About Your Leads")
    user_query = st.text_input("Enter your question here (e.g., 'Show me leads from companies with more than 50 employees')", "")

    if user_query:
        def query_deepseek(user_query, table_name, df):
            schema_description = "\n".join([f'- "{col}" ({str(dtype)})' for col, dtype in zip(df.columns, df.dtypes)])
            sample_rows = df.head(5).to_dict(orient="records")

            system_prompt = f"""You are a smart SQL assistant that converts natural language into precise SQL queries for DuckDB.

### CONTEXT:
You will be working with a table named: leads
This table is loaded from a CSV/XLS file, so column types may vary (e.g., dates as strings, numbers with symbols, or missing values).
The user may not know exact column names or data types, so your job is to interpret intent and output valid SQL.

### RULES FOR WRITING SQL:
- Always use only the table name: leads
- Wrap all column names in **double quotes**
- Do NOT guess column names — only use those provided
- Ensure SQL is compatible with DuckDB syntax

### COLUMN METADATA:
Below are the column names and their data types:
{schema_description}

### SAMPLE ROWS:
These are example rows to understand context and content:
{sample_rows}

### SPECIAL HANDLING RULES:
1. If a column contains ranges like "1001-5000" or "10,001+", use the "Emp Size Num" column (if present) for numeric filtering.
2. If employee size is missing, but revenue exists, assume "Emp Size Num" may have been inferred from "Revenue Size".
3. For partial text matches (like job titles or industries), use:
    - `ILIKE '%keyword%'`
4. For boolean-like values (e.g. "is verified", "has funding"):
    - Match values like 'yes', 'true', or 1
5. If comparing values in **string columns** with:
    - **Dates**: CAST to TIMESTAMP → `CAST("Last Funding Date" AS TIMESTAMP)`
    - **Numbers**: CAST to DOUBLE → `CAST("Revenue Size" AS DOUBLE)`
6. When comparing dates like "last 60 days":
    - Use: `CAST("Last Funding Date" AS TIMESTAMP) >= CURRENT_DATE - INTERVAL '60 days'`
7. Always ensure correct casting before numeric or timestamp comparisons.
8. Always handle missing/null values gracefully using `IS NOT NULL` where needed.
9. Combine multiple conditions with AND/OR using parentheses properly.
10. If the user uses general terms like "revenue", "employee size", or "headcount", map them to the actual column names:
    - "revenue" → "Revenue Size"
    - "employee size", "headcount" → "Emp Size Num"
    - "employees", "staff count" → "Emp Size Num"
    - "annual revenue", "total revenue" → "Revenue Size"
    - These mappings should be used only if the actual column exists in the provided schema.

### OUTPUT FORMAT:
Respond with only the valid SQL query (no markdown, no extra text, no explanations)."""

            headers = {
                "Authorization": f"Bearer {API_KEY}",
                "Content-Type": "application/json",
            }

            payload = {
                "model": "deepseek-chat",
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_query}
                ],
                "temperature": 0.3
            }

            response = requests.post(API_URL, headers=headers, json=payload)

            if response.status_code != 200:
                raise Exception(f"DeepSeek API Error: {response.text}")

            raw_sql = response.json()["choices"][0]["message"]["content"].strip()
            cleaned_sql = raw_sql.replace("```sql", "").replace("```", "").strip()
            return cleaned_sql

        with st.spinner("🔍 Processing your query..."):
            try:
                sql_query = query_deepseek(user_query, "leads", df)
                result_df = duckdb.sql(sql_query).df()

                if not result_df.empty:
                    st.markdown('<div class="rounded-box">', unsafe_allow_html=True)
                    st.success(f"✅ Found {len(result_df):,} matching leads.")
                    st.dataframe(result_df, use_container_width=True)

                    csv = result_df.to_csv(index=False).encode("utf-8")
                    st.download_button(
                        label="📥 Download Query Results as CSV",
                        data=csv,
                        file_name="almo_media_lead_query_results.csv",
                        mime="text/csv",
                        use_container_width=True,
                    )
                    st.markdown('</div>', unsafe_allow_html=True)
                else:
                    st.markdown('<div class="rounded-box">', unsafe_allow_html=True)
                    st.info("No leads found matching your criteria.")
                    st.markdown('</div>', unsafe_allow_html=True)

            except Exception as e:
                st.markdown('<div class="rounded-box">', unsafe_allow_html=True)
                st.error("⚠️ Sorry, an error occurred while processing your query.")
                st.caption(str(e))
                st.markdown('</div>', unsafe_allow_html=True)

else:
    st.markdown('<div class="rounded-box">', unsafe_allow_html=True)
    st.info("📂 Please upload a CSV or Excel file to begin analyzing your lead data.")
    st.markdown('</div>', unsafe_allow_html=True)

# ---- Footer ---- #
st.markdown("---")
st.markdown(
    '<p style="text-align: center; color: #888;">Powered by <a href="https://www.yugensys.com/" target="_blank" style="color: #0F52BA;">Yugensys Software</a></p>',
    unsafe_allow_html=True,
)
