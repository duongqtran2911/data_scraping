import pandas as pd
import datetime
from datetime import datetime, timezone
from pymongo import MongoClient
import uuid
from datetime import datetime
import numpy as np
import json



# Find first row containing a keyword
def find_row_index_containing(df, keyword):
    for i, row in df.iterrows():
        if row.astype(str).str.contains(keyword, case=False, na=False).any():
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
        "yếu tố khác": "yếu tố khác",  # base canonical form
        # Add more known aliases below if needed
    }
    return replacements.get(attr, attr)

# Get the value of landPrice from raw data table
def get_land_price_raw(d):
    return d.get("Giá đất (đồng/m²)", None)

# Get the value of landPrice from comparison data table
def get_land_price_pct(d):
    return d[('A', normalize_att('Giá thị trường (Giá trước điều chỉnh) (đồng/m²)'))]


