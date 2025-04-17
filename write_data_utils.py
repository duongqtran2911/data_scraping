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
        "yếu tố khác (nếu có)": "yếu tố khác",
        "yếu tố khác": "yếu tố khác",  
        'giá thị trường (giá trước điều chỉnh) (đồng/m²/năm)': 'giá thị trường (giá trước điều chỉnh) (đồng/m²)',
    }
    return replacements.get(attr, attr)

# Get the value of landPrice from raw data table
def get_land_price_raw(d):
    val = smart_parse_float(d.get("Giá đất (đồng/m²)", None))
    if val is None:
        val = smart_parse_float(d.get("Giá đất ODT (đồng/m²)", None))
    return val

# Get the value of landPrice from comparison data table
def get_land_price_pct(d):
    return smart_parse_float(d[('A', normalize_att('Giá thị trường (Giá trước điều chỉnh) (đồng/m²)'))])


