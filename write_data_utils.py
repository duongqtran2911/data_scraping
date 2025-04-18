import pandas as pd
import datetime
from datetime import datetime, timezone
from pymongo import MongoClient
import uuid
from datetime import datetime
import numpy as np
import json
import re
import unicodedata


# Find the indices of metadata
def find_meta_data(df, indicator_text="thời điểm thẩm định giá"):
    # Detect the raw table end by locating the second occurrence of "Thời điểm thẩm định giá"
    # indicator_text = "thời điểm thẩm định giá"  # lowercase for matching
    indicator_indices = []

    for idx, row in df[[0, 1]].iterrows():
        for val in row:
            if isinstance(val, str) and indicator_text in val.lower():
                indicator_indices.append(idx)
    return indicator_indices


# Find the last row of the raw data table by scanning downward for known markers like:
def find_raw_table_end(df, second_eval_row=None):
    """
    Find the last row of the raw data table by scanning downward for known markers like:
    'hình ảnh thông tin quy hoạch', 'hình ảnh tin rao', 'link'
    """
    target_keywords = ["hình ảnh thông tin quy hoạch", "hình ảnh tin rao", "link"]
    
    last_match_index = None

    for i in range(len(df)):
        first_cell = str(df.iloc[i, 0]).strip().lower()
        for keyword in target_keywords:
            if keyword in first_cell:
                last_match_index = i
    
    if last_match_index is not None:
        return last_match_index
    
    # fallback: if no match found, return a conservative guess
    return second_eval_row - 4 if second_eval_row else None


# Check if the value is a valid ord cell
def is_valid_ord(value):
    """
    Check if the ord cell looks like a valid comparison field code.
    """
    if pd.isna(value):
        return True  # continuation rows have empty ord
    value = str(value).strip().upper()
    return bool(re.match(r"^[A-E]\d{0,1}$", value)) or value in ["A", "B", "C", "D"]

# Find the last row of the comparison table in the DataFrame
def find_comparison_table_end(df):
    """
    Find the last row of the comparison table in the DataFrame.
    """
    ord_col = df.columns[1]

    for i in reversed(df.index):
        ord_val = df.at[i, ord_col]
        row_values = df.iloc[i, 2:]  # skip STT + ord column
        if is_valid_ord(ord_val):
            # Check if the row contains any numeric value in other columns
            if row_values.apply(lambda x: isinstance(x, (int, float)) or str(x).replace(',', '').replace('.', '').isdigit()).any():
                return i
    raise ValueError("Comparison table end not found.")



# Convert a string to a float, handling various formats
def smart_parse_float(s):
    """
    Convert a string like "1.234,56" or "1,234.56" or "203,5" to float.
    Handles both European and US-style formats dynamically.
    """
    if not isinstance(s, str):
        return s  # Already a number or None

    s = s.strip().lower()
    s = re.sub(r"[^\d.,-]", "", s)  # Remove everything except digits, commas, dots

    # Case 1: Only one separator → assume it's the decimal
    if s.count(",") == 1 and s.count(".") == 0:
        return float(s.replace(",", "."))

    if s.count(".") == 1 and s.count(",") == 0:
        return float(s)

    # Case 2: Both separators → guess decimal from position
    if "," in s and "." in s:
        last_dot = s.rfind(".")
        last_comma = s.rfind(",")

        if last_comma > last_dot:
            # Assume European format: 1.234,56 → 1234.56
            s = s.replace(".", "").replace(",", ".")
        else:
            # Assume US format: 1,234.56 → 1234.56
            s = s.replace(",", "")

        return float(s)

    # Case 3: More than one comma → likely thousand separator
    if s.count(",") > 1:
        s = s.replace(",", "")
        return float(s)

    # Case 4: Just digits
    return float(s)

# Normalize a string by removing accents and collapsing whitespace
def normalize_string(s):
    if not isinstance(s, str):
        return ""
    s = unicodedata.normalize("NFKD", s)  # remove accents
    s = ''.join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"\s+", " ", s)  # collapse all whitespace
    return s.strip().lower()


# Find the first row index containing a specific keyword in any column
def find_row_index_containing(df, keyword):
    norm_keyword = normalize_string(keyword)
    for i, row in df.iterrows():
        row_strs = row.astype(str).apply(normalize_string)
        if row_strs.str.contains(norm_keyword, case=False, na=False).any():
            return i
    return None


# Find where comparison table begins (looks for 'ord' codes like C1, C2, ...)
def find_comparison_table_start(df):
    for i, row in df.iterrows():
        if row.astype(str).str.match(r"C\d+", na=False).any():
            return i-3  # Adjusting to get the header row
    return None


# Re-import necessary module after state reset
def normalize_att(attr):
    if not isinstance(attr, str):
        return attr
    attr = attr.strip().lower()
    replacements = {
        # comparison table
        "yếu tố khác (nếu có)": "yếu tố khác",
        "yếu tố khác": "yếu tố khác",  
        'giá thị trường (giá trước điều chỉnh) (đồng/m²/năm)': 'giá thị trường (giá trước điều chỉnh) (đồng/m²)',
        'giá thị trường (giá trước điều chỉnh) (đồng)': 'giá thị trường (giá trước điều chỉnh) (đồng/m²)',
        'dân cư, kinh doanh': "dân cư",
        "chiều dài (m)": "chiều dài",
        "chiều rộng (m)": "chiều rộng",
        # raw table
        "giá đất odt (đồng/m²)": "giá đất (đồng/m²)",
        "đơn giá đất cln (đồng/m²)": "giá đất (đồng/m²)",
    }
    return replacements.get(attr, attr)

# Get the value of landPrice from raw data table
def get_land_price_raw(d):
    val = smart_parse_float(d.get(normalize_att("Giá đất (đồng/m²)"), None))
    # if val is None:
    #     val = smart_parse_float(d.get("Giá đất ODT (đồng/m²)", None))

        # Đơn giá đất CLN (đồng/m²)
    return val

# Get the value of landPrice from comparison data table
def get_land_price_pct(d):
    return smart_parse_float(d[('A', normalize_att('Giá thị trường (Giá trước điều chỉnh) (đồng/m²)'))])


def convert_dms_to_decimal(dms_str):
    match = re.match(r"(\d+)°(\d+)'([\d.]+)\"?([NSEW])", dms_str.strip())
    if not match:
        return None
    degrees, minutes, seconds, direction = match.groups()
    decimal = float(degrees) + float(minutes) / 60 + float(seconds) / 3600
    if direction in ['S', 'W']:
        decimal *= -1
    return decimal

def get_info_location(info):
    if pd.isna(info) or info is None or str(info).strip() == "":
        return None

    info_str = str(info).strip()

    # Case 1: standard lat, lon format (e.g. 10.97, 108.22)
    if "," in info_str and all(char not in info_str for char in "°'\""):
        try:
            lat, lon = info_str.split(",")
            return {
                "type": "Point",
                "coordinates": [float(lon.strip()), float(lat.strip())]  # MongoDB expects [longitude, latitude]
            }
        except Exception:
            return None

    # Case 2: DMS format (e.g. 10°58'10.4"N 108°13'46.8"E)
    if "°" in info_str:
        try:
            dms_parts = info_str.split()
            if len(dms_parts) == 2:
                lat_decimal = convert_dms_to_decimal(dms_parts[0])
                lon_decimal = convert_dms_to_decimal(dms_parts[1])
                return {
                    "type": "Point",
                    "coordinates": [lon_decimal, lat_decimal]
                }
        except Exception:
            return None

    return None
