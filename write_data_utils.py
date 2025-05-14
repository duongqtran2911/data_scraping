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


# Find the indices of metadata
def find_meta_data(df, indicator_text="thời điểm thẩm định giá"):
    # Detect the raw table end by locating the second occurrence of "Thời điểm thẩm định giá"
    # indicator_text = "thời điểm thẩm định giá"  # lowercase for matching
    indicator_indices = []

    for idx, row in df[[1]].iterrows():
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
        first_cell = str(df.iloc[i, 1]).strip().lower()
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


# Check if a value is numeric
def is_numeric_like(x):
    try:
        return isinstance(x, (int, float)) or str(x).replace(',', '').replace('.', '').replace('-', '').isdigit()
    except:
        return False


# Find the last row of the comparison table in the DataFrame
def find_comparison_table_end(df):
    """
    Find the last row of the comparison table in the DataFrame.
    """
    ord_col = df.columns[0]

    for i in reversed(df.index):
        ord_val = df.at[i, ord_col]
        row_values = df.iloc[i, 1:]  # skip STT + ord column
        if is_valid_ord(ord_val):
            # Check if the row contains any numeric value in other columns
            if row_values.apply(lambda x: isinstance(x, (int, float)) or str(x).replace(',', '').replace('.', '').isdigit()).any():
                return i
    raise ValueError("Comparison table end not found.")


# Function that detects human notations and converts them to numbers
def parse_human_number(text):
    """
    Parse a string that might contain informal human-readable numbers like:
    - "2ty" => 2,000,000,000
    - "2ty7" => 2,700,000,000
    - "2 ty 7" => 2,700,000,000
    - "1 người rao 1ty2" => 1,200,000,000 (first match only)
    - "Giá chào từ CDT 26.5ty" => 26,500,000,000
    """

    if not isinstance(text, str):
        return None

    text = text.lower().replace(',', '').replace('đồng', '')

    # Match full-form human numbers like '2ty7', '2 ty 7', '2ty', '26.5ty'
    match = re.search(r'(\d+(?:\.\d+)?)(?:\s*ty\s*(\d)?)?', text)

    if match:
        billion = float(match.group(1))
        hundred_million = match.group(2)
        if hundred_million and hundred_million.isdigit():
            value = int(billion * 1e9 + int(hundred_million) * 1e8)
        else:
            value = int(billion * 1e9)
        return value

    # Try to fallback to extract any number from string
    match = re.search(r'\d[\d.]*', text)
    if match:
        try:
            return float(match.group(0).replace('.', '').replace(',', ''))
        except ValueError:
            return None

    return None


# Convert a string to a float, handling various formats
def smart_parse_float(s):
    """
    Extract and convert the first numeric value found in a string to float.
    Supports formats like: 
        - 1,234.56 (US)
        - 1.234,56 (EU)
        - '4.84 (Vạt góc 2.34)'
        - 'Giá từ CDT 26.5ty' (combined with ty_parser if needed)
    """
    if not isinstance(s, str):
        return s  # Already a float or None

    s = s.strip().lower()
    if "chưa biết" in s:
        return np.nan  # Handle "unknown" cases

    # Match number patterns: optional thousand separators, one decimal separator
    # Capture first number only
    number_pattern = r"(\d{1,3}(?:[\.,]\d{3})*(?:[\.,]\d+)?|\d+)"
    match = re.search(number_pattern, s)
    if not match:
        return None

    number_str = match.group(1)

    # Normalize to float: detect format (dot/comma as decimal)
    if "," in number_str and "." in number_str:
        # Decide based on last separator position
        if number_str.rfind(",") > number_str.rfind("."):
            number_str = number_str.replace(".", "").replace(",", ".")
        else:
            number_str = number_str.replace(",", "")
    elif number_str.count(",") == 1 and number_str.count(".") == 0:
        # Likely European format
        number_str = number_str.replace(",", ".")
    else:
        # Remove commas (thousand separator)
        number_str = number_str.replace(",", "")

    try:
        return float(number_str)
    except:
        # raise ValueError(f"Cannot convert '{s}' to float.")
        return None


# Normalize a string by removing accents and collapsing whitespace
def normalize_string(s):
    if not isinstance(s, str):
        return ""
    s = unicodedata.normalize("NFKD", s)  # remove accents
    s = ''.join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"\s+", " ", s)  # collapse all whitespace
    return s.strip().lower()


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
        'giá thị trường \n(giá trước điều chỉnh) (đồng/m²)': 'giá thị trường (giá trước điều chỉnh) (đồng/m²)',
        'giá thị trường \n(giá trước điều chỉnh) (đồng)': 'giá thị trường (giá trước điều chỉnh) (đồng/m²)',
        'giá thị trường \n(giá trước điều chỉnh) \n(đồng)': 'giá thị trường (giá trước điều chỉnh) (đồng/m²)',
        'giá thị trường (giá trước điều chỉnh) (đồng)': 'giá thị trường (giá trước điều chỉnh) (đồng/m²)',
        'dân cư, kinh doanh': "dân cư",
        "chiều dài (m)": "chiều dài",
        "chiều rộng (m)": "chiều rộng",
        "chiều rộng giáp mặt tiền đường (m)":"chiều rộng (m)",
        "chiều rộng tiếp giáp mặt tiền đường (m)": "chiều rộng (m)",
        # raw table
        "quy mô diện tích (m²)": "quy mô diện tích (m²)\n(đã trừ đất thuộc quy hoạch lộ giới)",
        "quy mô diên tích (m²)\n(đã trừ quy hoạch lộ giới)":"quy mô diện tích (m²)\n(đã trừ đất thuộc quy hoạch lộ giới)",
        "quy mô diện tích (m²)\n(đã trừ quy hoạch lộ giới)":"quy mô diện tích (m²)\n(đã trừ đất thuộc quy hoạch lộ giới)",
        "quy mô diên tích (m²)":"quy mô diện tích (m²)\n(đã trừ đất thuộc quy hoạch lộ giới)",
        "quy mô diện tích (m²)\n(đã trừ đất thuộc quy hoạch lộ giới và quy hoạch cây xanh)": "quy mô diện tích (m²)\n(đã trừ đất thuộc quy hoạch lộ giới)",
        "quy mô diên tích (m²)\n(đã trử lộ giới)":"quy mô diện tích (m²)\n(đã trừ đất thuộc quy hoạch lộ giới)",

        "quy mô diện tích (m²)\n(đã trừ quy hoạch lộ giới đất nn)":"quy mô diện tích (m²)\n(đã trừ đất thuộc quy hoạch lộ giới)",

        "giá đất (đồng/m²/năm)": "giá đất (đồng/m²)",
        "giá đất odt (đồng/m²)": "giá đất (đồng/m²)",
        "giá đất cln (đồng/m²)": "giá đất (đồng/m²)",
        "giá đất ont (đồng/m²)": "giá đất (đồng/m²)",
        "giá đất hnk (đồng/m²)": "giá đất (đồng/m²)",
        "giá đất luc (đồng/m²)": "giá đất (đồng/m²)",
        "đơn giá (đồng/m²)": "giá đất (đồng/m²)",
        "đơn giá đất (đồng/m²)": "giá đất (đồng/m²)",
        "đơn giá đất cln (đồng/m²)": "giá đất (đồng/m²)",
        "đơn giá đất odt (đồng/m²)": "giá đất (đồng/m²)",
        "đơn giá đất ont (đồng/m²)": "giá đất (đồng/m²)",
        "đơn giá đất luc (đồng/m²)": "giá đất (đồng/m²)",
        "đơn giá đất hnk (đồng/m²)": "giá đất (đồng/m²)",
        "đơn giá đất ont(đồng/m²)": "giá đất (đồng/m²)",
        "đơn giá đất cln(đồng/m²)": "giá đất (đồng/m²)",
        "đơn giá đất luc(đồng/m²)": "giá đất (đồng/m²)",
        "đơn giá đất skc đến ngày 01/01/2046 (đồng/m²)": "giá đất (đồng/m²)",
        "giá căn hộ theo diện tích thông thủy (đồng/m²)": "giá đất (đồng/m²)",
        "đơn giá đất nông nghiệp đã trừ phần quy hoạch lộ giới (đồng/m²)": "giá đất (đồng/m²)",
        "giá rao bán (đồng) (không có vat):":"giá rao bán (đồng)",
    }
    return replacements.get(attr, attr)

# Get the value of landPrice from raw data table
def get_land_price_raw(d):
    val = smart_parse_float(d.get(normalize_att("Giá đất (đồng/m²)"), None))
    return val

# Get the value of landPrice from comparison data table
def get_land_price_pct(d):
    return smart_parse_float(d[('A', normalize_att('Giá thị trường (Giá trước điều chỉnh) (đồng/m²)'))])

# Match indices of reference percentages to raw data
def match_idx(ref_pcts, ref_raws):
    matched_idx = []  
    used_indices = set()

    for ref_pct in ref_pcts:
        pct_price = get_land_price_pct(ref_pct)
        diffs = [
            abs(get_land_price_raw(ref_raw) - pct_price)
            if i not in used_indices and pd.notna(get_land_price_raw(ref_raw)) else np.inf
            for i, ref_raw in enumerate(ref_raws)
        ]

        best_idx = int(np.argmin(diffs))
        matched_idx.append(best_idx)
        used_indices.add(best_idx)

    for i, idx in enumerate(matched_idx):
        print(f"ref_pcts[{i}] matched with ref_raws[{idx}]")

    idx_matches = dict(enumerate(matched_idx))
    return idx_matches

# Convert DMS (Degrees, Minutes, Seconds) to Decimal Degrees
def convert_dms_to_decimal(dms_str):
    match = re.match(r"(\d+)°(\d+)'([\d.]+)\"?([NSEW])", dms_str.strip())
    if not match:
        return None
    degrees, minutes, seconds, direction = match.groups()
    decimal = float(degrees) + float(minutes) / 60 + float(seconds) / 3600
    if direction in ['S', 'W']:
        decimal *= -1
    return decimal

# Function to get the location from the info string
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


# Function to get the purpose and area from the info string
def get_info_purpose(info):
    if not isinstance(info, str) or not info.strip():
        return info, np.nan
    res = []
    
    # Split by newline or semicolon
    parts = re.split(r"[\n;]", info)

    for part in parts:
        if ":" in part:
            try:
                name, area = part.split(":", 1)
                area_clean = area.strip().lower().replace("m²", "").replace("m2", "")

                # Fix number formatting
                area_clean = area_clean.replace(".", "").replace(",", ".")  # remove thousand sep, fix decimal

                match = re.search(r"[\d.]+", area_clean)
                area_val = float(match.group()) if match else np.nan

                res.append({
                    "name": name.strip(),
                    "area": area_val
                })
            except Exception as e:
                print(f"⚠️ Could not parse part '{part}' in purpose field: {e}")
        else:
            res.append({
                "name": part.strip(),
                "area": np.nan
            })
    return res


# Function to get the purpose and area from the info string
def get_info_unit_price(info):
    """
    Extracts the first float-like number from a string, removing thousand separators (dots) 
    and converting to float. Returns np.nan if no valid number found.
    """
    if not isinstance(info, str):
        return np.nan
    
    # Search for a number with optional dot thousands separators (e.g., 1.234.567)
    match = re.search(r'\d{1,3}(?:\.\d{3})*(?:,\d+)?|\d+(?:,\d+)?', info)
    
    if match:
        num_str = match.group()
        # Remove dot separators and convert comma to decimal (European format)
        num_str = num_str.replace('.', '').replace(',', '.')
        try:
            return float(num_str)
        except ValueError:
            return np.nan
    return np.nan


def get_max_width(width):
    if not isinstance(width, str):
        return float(width)  # nếu là số thì trả về luôn

    # Loại bỏ dấu phẩy kiểu '24,33' thành '24.33'
    width = width.replace(',', '.')

    # Regex để tìm giá trị sau cụm "nở hậu"
    match = re.search(r"nở hậu\s*([0-9.]+)", width, flags=re.IGNORECASE)
    if match:
        try:
            return float(match.group(1))
        except ValueError:
            return None

    # Nếu không có "nở hậu", cố gắng lấy giá trị đầu tiên làm max_width luôn
    match_fallback = re.search(r"([0-9.]+)", width)
    if match_fallback:
        try:
            return float(match_fallback.group(1))
        except ValueError:
            return None

    return None
