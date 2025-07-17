import streamlit as st
import pandas as pd
import numpy as np
import re
import io
import zipfile


def read_uploaded_file(file, filename):
    if filename.endswith('.csv'):
        return pd.read_csv(file)
    elif filename.endswith(('.xls', '.xlsx')):
        return pd.read_excel(file)
    else:
        return None

def extract_zip_and_identify(zip_bytes):
    with zipfile.ZipFile(zip_bytes, 'r') as z:
        file_list = z.namelist()

        eu_file = next((f for f in file_list if "eu" in f.lower()), None)
        ogrds_file = next((f for f in file_list if "ogrds" in f.lower()), None)

        if not eu_file or not ogrds_file:
            st.error("Could not identify both EU and OGRDS files in the zip.")
            return None, None

        with z.open(eu_file) as ef:
            df_eu = read_uploaded_file(io.BytesIO(ef.read()), eu_file)
        with z.open(ogrds_file) as of:
            df_ogrds = read_uploaded_file(io.BytesIO(of.read()), ogrds_file)

        return df_eu, df_ogrds


def split_brand_columns(df):
    if 'BRAND_VALIDATED' in df.columns:
        brand_split = df['BRAND_VALIDATED'].str.split(';', expand=True)
        brand_split.columns = ['BOI_VALIDATED', 'BRAND1_VALIDATED', 'GBE_VALIDATED']
        df = pd.concat([df, brand_split], axis=1)
    elif 'BRAND_1' in df.columns:
        df['BRAND1_VALIDATED'] = df['BRAND_1']
    else:
        raise KeyError("Neither 'BRAND_VALIDATED' nor 'BRAND_1' found in DataFrame.")
    return df

def clean_and_merge_supergroup(df):
    df['BRAND_1_CLEAN'] = df['BRAND1_VALIDATED'].str.extract(r'^([^\[\(]+)').astype(str)
    df['BRAND_1_CLEAN'] = df['BRAND_1_CLEAN'].str.replace(r' +$', '', regex=True)
    if 'SUPER_GROUP' in df.columns:
        df['SG_B1'] = df['SUPER_GROUP'] + " " + df['BRAND_1_CLEAN']
    elif 'SUPER_GROUP_DSCR' in df.columns:
        df['SG_B1'] = df['SUPER_GROUP_DSCR'] + " " + df['BRAND_1_CLEAN']
    return df

def generate_boi_suggest(ogrds_df):
    boi_suggest_df = (
        ogrds_df.groupby(['SG_B1', 'BRAND_OWNER_INTERNATIONAL'])
        .size().reset_index(name='count')
        .sort_values(['SG_B1', 'count'], ascending=[True, False])
        .drop_duplicates(subset=['SG_B1'])
        .rename(columns={'BRAND_OWNER_INTERNATIONAL': 'BOI_SUGGEST'})
    )
    return boi_suggest_df[['SG_B1', 'BOI_SUGGEST']]

def merge_boi_suggest(eu_df, boi_suggest_df):
    eu_df = eu_df.merge(boi_suggest_df, on='SG_B1', how='left')
    eu_df['BOI_SUGGEST'] = eu_df['BOI_SUGGEST'].fillna('SG_B1 not present in OGRDS')
    return eu_df

def fix_spacing(val):
    if pd.isna(val):
        return val
    parts = val.split(';')
    new_parts = []
    for part in parts:
        fixed = re.sub(r'([^\s])(\()', r'\1 (', part)
        new_parts.append(fixed)
    return ';'.join(new_parts)

def apply_spacing_fix(df):
    df['BRAND_VALIDATED_FIXED'] = df['BRAND_VALIDATED'].apply(fix_spacing)
    return df

def check_gbe_match(row):
    brand_fixed = row.get('BRAND1_VALIDATED')
    gbe = row.get('GBE_VALIDATED')
    if pd.isna(brand_fixed) or pd.isna(gbe):
        return 'Missing Data'
    brand_clean = brand_fixed.split('(')[0].strip()
    return 'Correct GBE' if gbe.startswith(brand_clean) else 'Incorrect GBE'

def apply_gbe_validation(df):
    df['GBE_STATUS'] = df.apply(check_gbe_match, axis=1)
    return df

def process_pipeline(eu_df, ogrds_df):
    eu_df = split_brand_columns(eu_df)
    ogrds_df = split_brand_columns(ogrds_df)

    eu_df = clean_and_merge_supergroup(eu_df)
    ogrds_df = clean_and_merge_supergroup(ogrds_df)

    boi_suggest_df = generate_boi_suggest(ogrds_df)
    eu_df = merge_boi_suggest(eu_df, boi_suggest_df)

    eu_df = apply_spacing_fix(eu_df)
    eu_df = apply_gbe_validation(eu_df)

    return eu_df


st.set_page_config(page_title="Brand & BOI Processor", layout="centered")
st.title("Brand & BOI Alignment Tool with ZIP Upload")

st.markdown("""
Upload a *ZIP file* containing your *EU* and *OGRDS* files (CSV or Excel).
Make sure file names include "eu" and "ogrds" to help with identification.
""")

zip_file = st.file_uploader("Upload ZIP (.zip)", type=["zip"])

if zip_file:
    df_eu, df_ogrds = extract_zip_and_identify(zip_file)

    if df_eu is not None and df_ogrds is not None:
        with st.spinner("Processing..."):
            final_df = process_pipeline(df_eu, df_ogrds)
        st.success("File processed successfully!")

        output = io.BytesIO()
        final_df.to_excel(output, index=False, engine='openpyxl')
        output.seek(0)

        st.download_button(
            label="Download Final Output Excel",
            data=output,
            file_name="Final_Europe_Processed.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

        st.subheader("🔍 Preview (First 10 Rows):")
        st.dataframe(final_df.head(10))
    else:
        st.info("Please upload a valid .zip file containing both EU and OGRDS files.")
else:
    st.info("Please upload a .zip file to start processing.")