import os

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
import logging
from fuzzywuzzy import process
from sympy.codegen.ast import continue_

from get_coordinate_guland import action_open_guland_driver


# Tạo thư mục nếu chưa tồn tại
log_dir = "logs_status_coordinate"
os.makedirs(log_dir, exist_ok=True)

# Đường dẫn đầy đủ đến file log
log_path = os.path.join(log_dir, "status_coordinate-2-2025-(2).log")

# Tạo logger riêng cho ứng dụng
app_logger = logging.getLogger("app_logger1")
app_logger.setLevel(logging.INFO)

# Đảm bảo logger này không bị ảnh hưởng bởi các logger khác
app_logger.propagate = False

# Thêm handler nếu chưa có
if not app_logger.handlers:
    file_handler = logging.FileHandler(log_path, mode="a", encoding="utf-8")
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    app_logger.addHandler(file_handler)

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
    """
    if isinstance(text, (int, float)):
        return text  # số thật, không xử lý thêm

    if not isinstance(text, str):
        return None

    text = text.lower().replace(',', '').replace('đồng', '').strip()

    # Nếu text là chuỗi số rõ ràng, thì chuyển trực tiếp
    if text.isdigit():
        return int(text)

    # Match full-form human numbers like '2ty7', '2 ty 7', '26.5ty'
    match = re.search(r'(\d+(?:\.\d+)?)(?:\s*ty\s*(\d)?)?', text)

    if match:
        billion = float(match.group(1))
        hundred_million = match.group(2)
        if hundred_million and hundred_million.isdigit():
            value = int(billion * 1e9 + int(hundred_million) * 1e8)
        else:
            value = int(billion * 1e9)
        return value

    # Fallback: extract first number
    match = re.search(r'\d[\d.]*', text)
    if match:
        try:
            return float(match.group(0).replace('.', '').replace(',', ''))
        except ValueError:
            return None

    return None



# Convert a string to a float, handling various formats
def smart_parse_float(s, log_missing=False):
    """
    Extract and convert a float from a string or raw input.
    
    If `log_missing=True`, raise ValueError when:
    - The field is missing (None)
    - A number cannot be parsed from the input

    :param s: input value (string or float-like)
    :param field_name: optional field label for error context
    :param entry_id: optional ID for traceability
    :param log_missing: if True, raise error instead of returning None or NaN
    :return: float value, np.nan, or raises error if log_missing is True
    """
    # If truly missing (field not present)
    if s is None or pd.isna(s):
        if log_missing:
            raise ValueError(f"[MISSING] Required field is missing.\n")
        return np.nan

    # If already numeric (float/int), return directly
    if isinstance(s, (int, float)):
        return float(s)

    s = str(s).strip().lower()

    if "chưa biết" in s:
        return np.nan

    # Match the first valid number in a messy string
    number_pattern = r"(\d{1,3}(?:[\.,]\d{3})*(?:[\.,]\d+)?|\d+)"
    match = re.search(number_pattern, s)

    if not match:
        if log_missing:
            raise ValueError(f"[UNPARSABLE] Cannot extract number from '{s}' in field.\n")
        return np.nan

    number_str = match.group(1)

    # Normalize based on decimal/thousand separator
    if "," in number_str and "." in number_str:
        if number_str.rfind(",") > number_str.rfind("."):
            number_str = number_str.replace(".", "").replace(",", ".")
        else:
            number_str = number_str.replace(",", "")
    elif "," in number_str and "." not in number_str:
        number_str = number_str.replace(",", ".")
    else:
        number_str = number_str.replace(",", "")

    try:
        return float(number_str)
    except Exception:
        if log_missing:
            raise ValueError(f"[ERROR] Failed to convert '{s}' to float.\n")
        return np.nan



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
        "chiều dài": "chiều dài (m)",
        "chiều rộng": "chiều rộng (m)",

        "chiều sâu": "chiều sâu (m)",

        "chiều rộng giáp mặt đường (m)": "độ rộng mặt tiền (m)",
        "chiều rộng giáp mặt tiền đường (m)": "độ rộng mặt tiền (m)",
        "chiều rộng tiếp giáp mặt tiền đường (m)": "độ rộng mặt tiền (m)",
        "chiều rộng mặt tiền tiếp giáp đường(m)": "độ rộng mặt tiền (m)",
        "chiều rộng mặt tiền tiếp giáp đường (m)": "độ rộng mặt tiền (m)",
        "chiều rộng tiếp giáp mặt tiền (m)": "độ rộng mặt tiền (m)",

        # raw table
        "địa chỉ": "địa chỉ tài sản",

        "quy mô diện tích (m²)": "quy mô diện tích (m²)\n(đã trừ đất thuộc quy hoạch lộ giới)",
        "quy mô diên tích (m²)\n(đã trừ quy hoạch lộ giới)": "quy mô diện tích (m²)\n(đã trừ đất thuộc quy hoạch lộ giới)",
        "quy mô diện tích (m²)\n(đã trừ quy hoạch lộ giới)": "quy mô diện tích (m²)\n(đã trừ đất thuộc quy hoạch lộ giới)",
        "quy mô diên tích (m²)": "quy mô diện tích (m²)\n(đã trừ đất thuộc quy hoạch lộ giới)",
        "quy mô diện tích (m²)\n(đã trừ đất thuộc quy hoạch lộ giới và quy hoạch cây xanh)": "quy mô diện tích (m²)\n(đã trừ đất thuộc quy hoạch lộ giới)",
        "quy mô diên tích (m²)\n(đã trử lộ giới)": "quy mô diện tích (m²)\n(đã trừ đất thuộc quy hoạch lộ giới)",
        "quy mô diện tích (m²)\n(đã trừ quy hoạch lộ giới đất nn)": "quy mô diện tích (m²)\n(đã trừ đất thuộc quy hoạch lộ giới)",

        "quy mô diên tích (m²)\n(trong gcn qsdđ)": "quy mô diện tích (m²)\n(đã trừ đất thuộc quy hoạch lộ giới)",
        "quy mô diện tích (m²)\n(đã trừ qhlg)": "quy mô diện tích (m²)\n(đã trừ đất thuộc quy hoạch lộ giới)",
        "quy mô diên tích (m²) (đã trừ lộ giới)": "quy mô diện tích (m²)\n(đã trừ đất thuộc quy hoạch lộ giới)",
        "quy mô diên tích(m²) (đã trừ lộ giới)": "quy mô diện tích (m²)\n(đã trừ đất thuộc quy hoạch lộ giới)",
        "quy mô diên tích cln (m²) (đã trừ lộ giới)": "quy mô diện tích (m²)\n(đã trừ đất thuộc quy hoạch lộ giới)",
        "quy mô diên tích (m²)\n(đã trừ đất nông nghiệp thuộc quy hoạch lộ giới)": "quy mô diện tích (m²)\n(đã trừ đất thuộc quy hoạch lộ giới)",

        "quy mô diện tích (m²)\n(chưa trừ đất thuộc quy hoạch lộ giới)": "quy mô diện tích (m²)\n(đã trừ đất thuộc quy hoạch lộ giới)",
        "quy mô diện tích (m²)\n(đã trừ đất nông nghiệp thuộc quy hoạch lộ giới)": "quy mô diện tích (m²)\n(đã trừ đất thuộc quy hoạch lộ giới)",
        "quy mô diên tích (m²)\n(đã trừ lộ giới quy hoạch)": "quy mô diện tích (m²)\n(đã trừ đất thuộc quy hoạch lộ giới)",

        "quy mô diện tích (m²) \n(đã trừ lộ giới)": "quy mô diện tích (m²)\n(đã trừ đất thuộc quy hoạch lộ giới)",
        "quy mô diên tích (m²)\n(theo diện tích thực tế)": "quy mô diện tích (m²)\n(đã trừ đất thuộc quy hoạch lộ giới)",
        "quy mô diên tích (đã trừ lộ giới) (m²)\n(đã trừ quy hoạch lộ giới)": "quy mô diện tích (m²)\n(đã trừ đất thuộc quy hoạch lộ giới)",
        "quy mô diên tích thông thuỷ (m²)": "quy mô diện tích (m²)\n(đã trừ đất thuộc quy hoạch lộ giới)",
        "quy mô diên tích thông thủy(m²)": "quy mô diện tích (m²)\n(đã trừ đất thuộc quy hoạch lộ giới)",

        "diện tích (m²)": "quy mô diện tích (m²)\n(đã trừ đất thuộc quy hoạch lộ giới)",
        "diện tích sàn (thông thủy) (m²)": "quy mô diện tích (m²)\n(đã trừ đất thuộc quy hoạch lộ giới)",
        "diện tích sàn": "quy mô diện tích (m²)\n(đã trừ đất thuộc quy hoạch lộ giới)",
        "diện tích sàn sử dụng (tim tường) (m²)": "quy mô diện tích (m²)\n(đã trừ đất thuộc quy hoạch lộ giới)",
        "diện tích sàn sử dụng (m²)": "quy mô diện tích (m²)\n(đã trừ đất thuộc quy hoạch lộ giới)",
        "quy mô diên tích tim tường (m²)": "quy mô diện tích (m²)\n(đã trừ đất thuộc quy hoạch lộ giới)",

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
        "đơn giá đất odt(đồng/m²)": "giá đất (đồng/m²)",
        "đơn giá đất ont (đồng/m²)": "giá đất (đồng/m²)",
        "đơn giá đất luc (đồng/m²)": "giá đất (đồng/m²)",
        "đơn giá đất hnk (đồng/m²)": "giá đất (đồng/m²)",
        "đơn giá đất ont(đồng/m²)": "giá đất (đồng/m²)",
        "đơn giá đất cln(đồng/m²)": "giá đất (đồng/m²)",
        "đơn giá đất luc(đồng/m²)": "giá đất (đồng/m²)",
        "đơn giá đất skc đến ngày 01/01/2046 (đồng/m²)": "giá đất (đồng/m²)",
        "giá căn hộ theo diện tích thông thủy (đồng/m²)": "giá đất (đồng/m²)",
        "đơn giá đất nông nghiệp đã trừ phần quy hoạch lộ giới (đồng/m²)": "giá đất (đồng/m²)",
        "giá đất skc, thời hạn đến ngày 20/12/2054 (đồng/m²)": "giá đất (đồng/m²)",

        "giá đất tmdv, thời hạn đến ngày 09/01/2067 (đồng/m²)": "giá đất (đồng/m²)",
        "giá đất luc/bhk (đồng/m²)": "giá đất (đồng/m²)",
        "giá đất tmdv (đồng/m²)": "giá đất (đồng/m²)",
        "giá đất tmdv, thời hạn đến ngày 09/01/2067 (đồng/m²)": "giá đất (đồng/m²)",
        "giá đất skc (đồng/m²)": "giá đất (đồng/m²)",
        "đơn giá đất nông nghiệp (đồng/m²)": "giá đất (đồng/m²)",
        "đơn giá đất ở (đồng/m²)": "giá đất (đồng/m²)",

        # 2025
        "giá trị đất odt (đồng)": "giá trị đất (đồng)",
        "giá trị đất ont(đồng)": "giá trị đất (đồng)",
        "giá trị đất ont (đồng)": "giá trị đất (đồng)",
        "giá trị đất cln (đồng)": "giá trị đất (đồng)",
        "giá trị đất cln(đồng)": "giá trị đất (đồng)",
        "giá trị đất rsx (đồng)": "giá trị đất (đồng)",
        "giá trị đất rsx(đồng)": "giá trị đất (đồng)",
        "giá trị đất tmd (đồng)": "giá trị đất (đồng)",

        # 2024
        "giá trị đất nn (đồng)": "giá trị đất (đồng)",
        "giá trị đất nn vt1 (đồng)": "giá trị đất (đồng)",
        "giá trị đất luk(đồng)": "giá trị đất (đồng)",
        "giá trị đất hnk (đồng)": "giá trị đất (đồng)",
        "giá trị đất odt(đồng)": "giá trị đất (đồng)",

        "giá trị đất luc (đồng)": "giá trị đất (đồng)",
        "giá trị đất lua (đồng)": "giá trị đất (đồng)",

        "giá trị đất bhk (đồng)": "giá trị đất (đồng)",

        # 5/2024
        "giá trị đất đã trừ phần quy hoạch lộ giới (đồng)": "giá trị đất (đồng)",

        "giá rao bán (đồng) (không có vat):": "giá rao bán (đồng)",
    }
    return replacements.get(attr, attr)

# Get the value of landPrice from raw data table
def get_land_price_raw(d, field="Giá đất (đồng/m²)"):
    val = smart_parse_float(d.get(normalize_att(field), None))
    return val

# Get the value of landPrice from comparison data table
def get_land_price_pct(d, field=('A', normalize_att('Giá thị trường (Giá trước điều chỉnh) (đồng/m²)'))):
    return smart_parse_float(d[field])

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
# Hàm chính để lấy thông tin vị trí (cải tiến)
def get_info_location(info, address, driver1, driver2 ,file_path):
    """
    Hàm lấy thông tin vị trí từ tọa độ hoặc địa chỉ

    Args:
        info: Thông tin tọa độ (chuỗi)
        location: Địa chỉ (phòng hờ khi không có tọa độ)

    Returns:
        Đối tượng GeoJSON Point hoặc None
    """
    # Kiểm tra tọa độ đầu vào trước
    if pd.notna(info) and info is not None and str(info).strip() != "":
        info_str = str(info).strip()

        # Trường hợp 1: định dạng lat, lon tiêu chuẩn (VD: 10.97, 108.22)
        if "," in info_str and all(char not in info_str for char in "°'\""):
            try:
                lat, lon = info_str.split(",")
                return {
                    "type": "Point",
                    "coordinates": [float(lon.strip()), float(lat.strip())]  # MongoDB cần [longitude, latitude]
                }
            except Exception as e:
                print(f"❌ Lỗi khi phân tích tọa độ tiêu chuẩn: {e}")

        # Trường hợp 2: định dạng DMS (VD: 10°58'10.4"N 108°13'46.8"E)
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
            except Exception as e:
                print(f"❌ Lỗi khi phân tích tọa độ DMS: {e}")

    # Trường hợp 3: Nếu không có tọa độ trực tiếp, thử dùng địa chỉ
    if address and str(address).strip() != "":
        log_message = f"ℹ️ Không có tọa độ trong dữ liệu. Đang sử dụng địa chỉ: {address}"
        print("\nℹ️ Không có tọa độ trong dữ liệu. Đang sử dụng địa chỉ:" + address)
        app_logger.info(file_path)
        app_logger.info(log_message)
        # app_logger.info(f" - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -\n")
        # address_infor = parse_location_info(address)
        return action_open_guland_driver(address,driver1,driver2,file_path)



    # Nếu tất cả các phương pháp thất bại
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
    
    if pd.isna(width):
        return np.nan
    
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
            return np.nan

    return np.nan


def normalize_unicode(text):
    if not isinstance(text, str):
        text = str(text)
    return unicodedata.normalize("NFKC", text).strip().lower()

def get_facade_info(width_raw, location_info):
    width_str = normalize_unicode(width_raw)
    if pd.isna(width_str) or width_str == "" or width_str == "nan":
        return {"has_facade": False, "value": np.nan}
    location_str = normalize_unicode(location_info)

    # Denial keywords → explicitly no facade
    deny_keywords = ["không có mặt tiền", "hẻm", "mặt hậu", "không tiếp giáp", "nở hậu"]
    if any(kw in width_str for kw in deny_keywords) or any(kw in location_str for kw in deny_keywords):
        return {"has_facade": False, "value": np.nan}

    # Positive facade signals — expanded
    strong_positive_patterns = [
        r"\bmặt\s*tiền\b",
        r"\bmặt\s*đường\b",
        r"\bmặt\s*tiền\s*đường\b",
        r"\bmặt\s*phố\b",
        r"\bgiáp\s*đường\b",
        r"\btrục\s*đường\b",
        r"\bđường\s*lớn\b",
        r"\bquốc\s*lộ\b"
    ]

    for pattern in strong_positive_patterns:
        if re.search(pattern, location_str):
            return {"hasFacade": True, "value": np.nan}

    # Regex match from width_str: '1.96m mặt tiền'
    match = re.search(r'([\d,\.]+)\s*m[^,\n]*?(mặt\s*tiền)', width_str)
    if match:
        try:
            value = float(match.group(1).replace(',', '.'))
            return {"hasFacade": True, "value": value}
        except ValueError:
            pass

    # Fallback: get first number if nothing else matched
    match = re.search(r'([\d,\.]+)\s*m', width_str)
    if match:
        try:
            value = float(match.group(1).replace(',', '.'))
            return {"hasFacade": True, "value": value}
        except ValueError:
            pass

    # Default: can't detect
    return {"hasFacade": False, "value": np.nan}

# function to dynamically assign 'chiều sâu' to missing field
def assign_dimensions(width, height, depth, has_facade):
    if width and height:
        return width, height
    if not width and height and depth:
        return depth, height
    if width and not height and depth:
        return width, depth
    if not width and not height and depth:
        return None, depth
    if not width and has_facade and depth:
        return None, depth
    return width, height


def fuzzy_get(entry, target_key, default = np.nan, threshold=70):
    if not entry:
        return default

    keys = list(entry.keys())
    match, score = process.extractOne(target_key, keys)

    # In ra thông tin so sánh
    print(f"🔍 Tìm kiếm: '{target_key}'")
    print(f"   ➤ Tìm thấy: '{match}' (Độ tương đồng: {score}%)")

    if score >= threshold:
        value = entry.get(match)
        if pd.notna(value) and str(value).strip() != "":
            print(f"   ✅ Giá trị: {value}")
            print(f"   {'=' * 50}")
            return value
        else:
            print(f"   ⚠️  Giá trị rỗng hoặc NaN")
            print(f"   {'=' * 50}")
    else:
        print(f"   ❌ Độ tương đồng thấp ({score}% < {threshold}%)")
        print(f"   {'=' * 50}")

    return default

