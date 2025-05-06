import pandas as pd
import datetime
from datetime import datetime, timezone
from pymongo import MongoClient
import uuid
from datetime import datetime
import numpy as np
import json
from write_data_utils import normalize_att, find_row_index_containing, smart_parse_float, \
        find_comparison_table_start, get_land_price_raw, get_land_price_pct, get_info_location, get_info_purpose, \
        get_info_unit_price, find_meta_data, find_comparison_table_end, find_raw_table_end, match_idx, parse_human_number
import os
import re
from pymongo import MongoClient
import traceback
import logging



# ---- CONFIGURATION ----
# EXCEL_FILE = "DV_Can Giuoc.xlsx"
# EXCEL_FILE = "DV_Le Trong Tan.xlsx"
MONGO_URI = "mongodb://dev-valuemind:W57mFPVT57lt3wU@10.10.0.42:27021/?replicaSet=rs0&directConnection=true&authSource=assets-valuemind"


# ---- GLOBAL PARAMETERS ----
sheet_idx = 0
raw_col_length = 11
pct_col_length = 6
year = 2024
month = "8"

# Read list of Excel paths
# with open(f"comparison_files_{month}_{year}.txt", "r", encoding="utf-8") as f:
#     excel_paths = [line.strip() for line in f if line.strip()]
# open(log_file_path", "w", encoding="utf-8").close()
# total_sheets = 0


# ---- LOAD EXCEL PATHS AND SHEETS ----
# comparison_file_path = f"comparison_files_{month}_{year}.txt"
# sheet_map = {}
#
# detect_folder = os.path.join(os.getcwd(), f"file_detection_{year}")
# comparison_file = os.path.join(detect_folder, comparison_file_path)
#
# with open(comparison_file, "r", encoding="utf-8") as f:
#     for line in f:
#         if ">>>" in line:
#             path, sheets = line.rstrip("\n").split(" >>> ")
#             sheet_map[path.strip()] = [s for s in sheets.split("&&")]

folder_path = r"D:/excel"
sheet_map = {}

# Quét toàn bộ file Excel trong thư mục
for filename in os.listdir(folder_path):
    if filename.endswith(".xlsx") or filename.endswith(".xls"):
        full_path = os.path.join(folder_path, filename)
        try:
            xls = pd.ExcelFile(full_path)
            sheet_map[full_path] = xls.sheet_names  # Lấy danh sách sheet
        except Exception as e:
            print(f"⚠️ Lỗi đọc file {full_path}: {e}")

# ---- LOG FILE ----
log_folder = os.path.join(os.getcwd(), f"reading_logs_{year}")
os.makedirs(log_folder, exist_ok=True)


# 📄 Create log file path inside the year-based folder
# log_file_path = f"reading_logs_{month}_{year}.txt"
log_file_path = os.path.join(log_folder, f"reading_logs_{month}_{year}.txt")
open(log_file_path, "w", encoding="utf-8").close()

found = 0
missing = 0

# ---- PROCESS EACH FILE AND SHEET ----
for file_path, sheet_list in sheet_map.items():
    try:
        if not os.path.isfile(file_path):
            print(f"⭕️ File not found: {file_path}")
            missing += 1
            with open(log_file_path, "a", encoding="utf-8") as log_file:
                log_file.write(f"⭕️ File not found: {file_path}\n")
                log_file.write("----------------------------------------------------------------------------------------------\n")
            continue
        
        with open(log_file_path, "a", encoding="utf-8") as log_file:
            log_file.write(f"sheet_list: {sheet_list}\n")
        xls = pd.ExcelFile(file_path)
        sheet_names = xls.sheet_names
        with open(log_file_path, "a", encoding="utf-8") as log_file:
            log_file.write(f"sheet_names: {sheet_names}\n")

        for sheet in sheet_list:
            try: 
                if sheet not in xls.sheet_names:
                    with open(log_file_path, "a", encoding="utf-8") as log_file:
                        log_file.write(f"❌ Sheet '{sheet}' not found in {file_path}\n")
                        log_file.write("----------------------------------------------------------------------------------------------\n") 
                    continue

                # --- Insert your full parsing/writing logic here ---
                print(f"⏳ Processing {file_path}")
                with open(log_file_path, "a", encoding="utf-8") as log_file:
                    log_file.write(f"⏳ Processing sheet: {sheet}, {file_path}\n")

                # xls = pd.ExcelFile(file_path)
                # sheet = xls.sheet_names[sheet_idx]

                
                df = pd.read_excel(xls, sheet_name=sheet, header=None)
                df = df.dropna(axis=1, how='all')
                min_valid_cells = 5
                df = df.loc[:, df.notna().sum() >= min_valid_cells].reset_index(drop=True)

                # with open(log_file_path, "a", encoding="utf-8") as log_file:
                #     log_file.write(f"df:\n {df}")

                

                # ---- DETECT RAW DATA TABLE AND COMPARISON TABLE ---- 
                raw_start_idx = find_row_index_containing(df, "HẠNG MỤC") + 1                     #  Tìm từ hạng mục trong sheet
                pct_start_idx = find_comparison_table_start(df)                                             #  Tìm bảng để so sánh

                # with open(log_file_path, "a", encoding="utf-8") as log_file:
                #     log_file.write(f"raw_start_idx: {raw_start_idx}\n")
                #     log_file.write(f"pct_start_idx: {pct_start_idx}\n")

                # Slice the first half of the file
                df_raw = df.iloc[:pct_start_idx]                # Cắt phần đầu của file đến ngay trước bảng so sánh, được coi là dữ liệu thô.
                df_raw = df_raw.dropna(axis=1, how='all')       # Bỏ các cột toàn giá trị NaN (trống hoàn toàn).
                min_valid_cells = 10                            # Chỉ giữ lại các cột có ít nhất 10 ô không trống.
                df_raw = df_raw.loc[:, df_raw.notna().sum() >= min_valid_cells].reset_index(drop=True)

                non_empty_cols_raw = df_raw.columns[df_raw.notna().any()].tolist()          # Tìm vị trí cột đầu tiên có dữ liệu thực, sau đó raw_col_start là cột tiếp theo — thường dùng để bắt đầu đọc phần giá trị hoặc thông số.
                first_valid_raw_col_idx = df_raw.columns.get_loc(non_empty_cols_raw[0])

                raw_col_start = first_valid_raw_col_idx + 1  # TSTDG usually comes after attributes



                # Slice the second half of the file
                df_pct = df.iloc[pct_start_idx:]                # Cắt phần còn lại từ vị trí pct_start_idx trở đi để lấy bảng so sánh.
                df_pct = df_pct.dropna(axis=1, how='all')       # Bỏ cột trống hoàn toàn.
                min_valid_cells = 5
                df_pct = df_pct.loc[:, df_pct.notna().sum() >= min_valid_cells].reset_index(drop=True)          # Giữ lại các cột có ít nhất 5 ô có dữ liệu.

                non_empty_cols_cmp = df_pct.columns[df_pct.notna().any()].tolist()          # Xác định cột bắt đầu của dữ liệu so sánh, không cần cộng thêm nếu dữ liệu bắt đầu ngay từ đó.
                first_valid_cmp_col_idx = df_pct.columns.get_loc(non_empty_cols_cmp[0])

                pct_col_start = first_valid_cmp_col_idx + 0  # attributes (e.g., "Giá thị trường") are in this column
                
                pct_start_idx = find_comparison_table_start(df_pct)         # Gọi lại hàm để định vị lại (nếu cần) vị trí bảng so sánh trong phần đã cắt df_pct.


                # with open(log_file_path, "a", encoding="utf-8") as log_file:
                #     log_file.write(f"raw_col_start: {raw_col_start}\n")
                #     log_file.write(f"pct_col_start: {pct_col_start}\n")

                with open(log_file_path, "a", encoding="utf-8") as log_file:
                    log_file.write(f"df_pct:\n {df_pct}\n")                                                         # Ghi nội dung bảng so sánh (df_pct) vào file log để tiện debug nếu cần.

                # Indices of subtext in the excel file => Helper function for finding the end of the raw data table
                indicator_indices = find_meta_data(df, indicator_text="thời điểm")                               # Tìm các dòng chứa từ khóa "thời điểm" — thường dùng làm dấu mốc cuối bảng dữ liệu gốc.
                if len(indicator_indices) < 2:
                    with open(log_file_path, "a", encoding="utf-8") as log_file:
                        log_file.write("⚠️ Could not find the second 'Thời điểm ...' to determine raw_end_idx, switching to 'STT'\n")       # Nếu không đủ dữ kiện để xác định điểm kết thúc, chuyển sang tìm "stt" làm chỉ mục thay thế.
                        log_file.write(f"indicator_indices: {indicator_indices}\n")
                    indicator_indices = find_meta_data(df, indicator_text="stt")
                    # raise ValueError("⚠️ Could not find the second 'Thời điểm ...' to determine raw_end_idx")

                # with open(log_file_path, "a", encoding="utf-8") as log_file:
                #     log_file.write(f"indicator_indices: {indicator_indices}\n")

                # with open(log_file_path, "a", encoding="utf-8") as log_file:
                #     log_file.write(f"indicator_indices[1]: {indicator_indices[1]}\n")
                #     log_file.write(f"find_raw_table_end: {find_raw_table_end(df, second_eval_row=indicator_indices[1])}\n")
                    # log_file.write(f"df_pct index: {df_pct.index}\n")
                #     log_file.write(f"find_comparison_table_end: {find_comparison_table_end(df_pct)}\n")
                    
                raw_end_idx = find_raw_table_end(df, second_eval_row=indicator_indices[1])              # raw_end_idx: Vị trí dòng cuối cùng của bảng dữ liệu gốc (df_raw)
                pct_end_idx = find_comparison_table_end(df_pct)                                        # pct_end_idx: Vị trí kết thúc bảng so sánh (df_pct)


                raw_col_end = raw_col_start + raw_col_length
                pct_col_end = pct_col_start + pct_col_length                                # Tính chỉ số cột kết thúc dựa trên số lượng cột đã biết (raw_col_length, pct_col_length có thể là biến cấu hình).


                # ---- FIND AND PARSE TABLES FROM EXCEL FILES ----

                # Extract the raw table
                raw_table = df_raw.iloc[raw_start_idx:raw_end_idx+1, raw_col_start:raw_col_end].dropna(how="all")           # Cắt vùng dữ liệu từ df_raw theo dòng và cột xác định. Bỏ các dòng trống hoàn toàn.
                raw_table.reset_index(drop=True, inplace=True)

                # Dynamically assign column names based on actual number of columns
                num_raw_cols = raw_table.shape[1]                                                       # Kiểm tra số cột thực tế có trong bảng.

                if num_raw_cols < 3:
                    raise ValueError(f"Expected at least 3 columns (attribute + TSTDG + at least 1 TSSS), but got {num_raw_cols}")          # Yêu cầu bảng thô phải có ít nhất 3 cột: "attribute", "TSTDG", và ít nhất một "TSSS"

                raw_col_names = ["attribute", "TSTDG"] + [f"TSSS{i}" for i in range(1, num_raw_cols - 1)]               # Tạo tên cột động: thuộc tính + TSTDG + TSSS1, TSSS2, .
                raw_table.columns = raw_col_names
                raw_table['attribute'] = raw_table['attribute'].apply(normalize_att)                                # Gán tên cột động: "attribute" (thuộc tính), "TSTDG" (thông số tiêu chuẩn định giá), "TSSS" (thông số so sánh).
                                                                                                                    # Normalize tên thuộc tính cho chuẩn hóa so sánh.
                with open(log_file_path, "a", encoding="utf-8") as log_file:
                    log_file.write(f"raw_table:\n {raw_table}\n") 
                    log_file.write(f" - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -\n")



                # Bottom table has percentage comparison values (C1, C2...)
                pct_table = df_pct.iloc[pct_start_idx:pct_end_idx+1, pct_col_start:pct_col_end].dropna(how="all")           # Cắt dữ liệu từ df_pct theo vùng xác định.
                pct_table.columns = ["ord", "attribute", "TSTDG", "TSSS1", "TSSS2", "TSSS3"]                                # Gán tên cột. ord được điền giá trị liên tục nếu có dòng bị trống.
                pct_table.reset_index(drop=True, inplace=True)
                pct_table['ord'] = pct_table['ord'].ffill()
                # pct_table['ord'] = pct_table['ord'].astype(str).str.strip()
                pct_table['attribute'] = pct_table['attribute'].apply(normalize_att)                                        # Chuẩn hóa tên thuộc tính.
                with open(log_file_path, "a", encoding="utf-8") as log_file:
                    log_file.write(f"pct_table:\n {pct_table}\n")
                    log_file.write(f" - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -\n")
                

                # Match attributes with corresponding field names: DB fieldName -> attribute
                att_en_vn = {                                                   # Tạo ánh xạ giữa tên trường trong CSDL (tiếng Anh) với thuộc tính gốc (đã chuẩn hóa).
                    "legalStatus": normalize_att("Tình trạng pháp lý"),
                    "location": normalize_att("Vị trí "),
                    "traffic": normalize_att("Giao thông"), 
                    "area": normalize_att("Quy mô diện tích (m²)"), 
                    "width": normalize_att("Chiều rộng"), 
                    "height": normalize_att("Chiều dài"), 
                    "population": normalize_att("Dân cư"),
                    "shape": normalize_att("Hình dáng"),
                    "other": normalize_att("Yếu tố khác (nếu có)"),
                }


                # Match attributes with corresponding "ord" values: attribute -> ord C1, C2, ...
                skip_attrs = [normalize_att(i) for i in ["Tỷ lệ", "Tỷ lệ điều chỉnh", "Mức điều chỉnh"]]            # Loại bỏ những thuộc tính không cần ánh xạ (ví dụ như tỷ lệ).
                main_rows = pct_table[~pct_table["attribute"].isin(skip_attrs)]                                     # Lọc ra các dòng trong pct_table có cột "attribute" không nằm trong skip_attrs.
                main_rows = main_rows.drop_duplicates(subset=["ord","attribute"], keep="first")                     # Xử lý dữ liệu bị trùng lặp trong bảng pct_table. Giữ lại dòng đầu tiên cho mỗi cặp (ord, attribute). Tránh việc một thuộc tính được ánh xạ nhiều lần sang cùng một ord.
                main_rows['attribute'] = main_rows['attribute'].apply(normalize_att)                                # Áp dụng chuẩn hóa cho từng giá trị trong cột "attribute" (vì một số có thể chưa được chuẩn hóa trước đó).
                att_to_ord = dict(zip(main_rows["attribute"], main_rows["ord"]))                                    # Mục đích: Tạo ánh xạ từ thuộc tính (đã chuẩn hóa) → mã ord tương ứng (C1, C2, ...). Ví dụ: "vị trí" → "C2", "tình trạng pháp lý → "C1"
                print("att_to_ord:", att_to_ord)
                with open(log_file_path, "a", encoding="utf-8") as log_file:
                        log_file.write(f"att_to_ord: {att_to_ord}\n")                                               # Sau đó ghi kết quả ánh xạ vào file log.
                


                # ---- EXTRACT RAW AND COMPARISON TABLES ----
                # Function to extract the raw table
                def extract_col_raw(col_name):            # trả về một dict dạng {attribute: value} từ bảng dữ liệu gốc.
                    return dict(zip(raw_table["attribute"], raw_table[col_name].fillna(np.nan)))        # Dùng .fillna(np.nan) để đảm bảo giá trị thiếu được xử lý nhất quán.


                # Function to extract the comparison table
                def extract_col_pct(col_name):
                    ord_key = list(zip(pct_table["ord"], pct_table["attribute"]))       # Kết hợp cả ord và attribute làm key → {(ord, attribute): value}.
                    ord_val = pct_table[col_name].fillna(np.nan)                        # Dùng trong trường hợp các nhóm có thể lặp lại cùng một attribute (nhưng khác ord, như C1, C2...).
                    return dict(zip(ord_key, ord_val))


                # Dynamically extract all TSSS columns
                main_raw = extract_col_raw("TSTDG")                                    # Lấy dữ liệu "TSTDG" từ bảng gốc → dùng làm giá trị chính để so sánh sau này.
                ref_raws = [
                    extract_col_raw(col)
                    for col in raw_col_names
                    if col.startswith("TSSS") or col.startswith("TSCM")                # Lấy ra tất cả các cột tham chiếu (TSSS1, TSSS2, ...) → là các bộ dữ liệu dùng để so sánh với TSTDG
                ]

                # Filter out very empty reference columns
                ref_raws = [ref for ref in ref_raws if sum(pd.notna(v) for v in ref.values()) >= 10]        # Lọc ra các cột tham chiếu có quá ít dữ liệu (ít hơn 10 giá trị không null) → để tránh làm nhiễu dữ liệu khi so sánh.


                # Dynamically extract all TSSS columns from the comparison table        # Trích xuất dữ liệu so sánh từ bảng phần trăm
                main_pct = extract_col_pct("TSTDG")                                     # Trích xuất dữ liệu chính từ bảng phần trăm để làm chuẩn đối chiếu.
                num_pct_cols = pct_table.shape[1]
                if num_pct_cols < 3:
                    raise ValueError(f"Expected at least 3 columns (ord, attribute, TSTDG, TSSS...), got {num_pct_cols}")       # Kiểm tra bảng phần trăm phải có ít nhất 3 cột (ord, attribute, TSTDG...).

                pct_col_names = ["ord", "attribute"] + [f"TSTDG"] + [f"TSSS{i}" for i in range(1, num_pct_cols - 2)]        # Cập nhật tên cột động theo số lượng thực tế (ví dụ nếu có 3 cột TSSS → tạo TSSS1, TSSS2, TSSS3).
                pct_table.columns = pct_col_names

                # Extract TSSS* columns dynamically from pct_table
                ref_pcts = [extract_col_pct(col) for col in pct_col_names if col.startswith("TSSS") or col.startswith("TSCM")]      # Tạo danh sách các dict chứa dữ liệu phần trăm cho từng cột TSSS từ bảng pct_table.

                
                # Match the index of reference properties from comparison tables to raw tables
                # def match_idx(ref_pcts, ref_raws):
                matched_idx = []                    # lưu kết quả ánh xạ giữa các bộ dữ liệu (ref_pct vs ref_raw).
                used_indices = set()                # tránh ánh xạ trùng lặp với cùng một bộ dữ liệu.

                for ref_pct in ref_pcts:            # Mỗi ref_pct tương ứng với một cột TSSS trong bảng phần trăm. Mục tiêu: tìm xem nó khớp nhất với ref_raw nào (dựa trên giá trị định giá đất).
                    pct_price = get_land_price_pct(ref_pct)         # Tính toán giá trị định giá đất từ bảng phần trăm (ref_pct). Có thể tính theo trọng số, tỷ lệ phần trăm, hoặc đơn giản là giá trị tổng hợp.
                    with open(log_file_path, "a", encoding="utf-8") as log_file:
                        log_file.write(f"ref: ref{ref_pcts.index(ref_pct)+1}_pct, pct_price: {pct_price}\n")        # Ghi tên bảng phần trăm và giá trị định giá đất tương ứng vào file log (debug dễ hơn).
                        # log_file.write(f"ref_raws: {ref_raws}\n")
                    diffs = []
                    # diffs = [
                    #     abs(get_land_price_raw(ref_raw) - pct_price)
                    #     if i not in used_indices and pd.notna(get_land_price_raw(ref_raw)) else np.inf
                    #     for i, ref_raw in enumerate(ref_raws)
                    # ]
                    for i, ref_raw in enumerate(ref_raws):                                                          # Với mỗi ref_raw (một bảng dữ liệu thô):
                        raw_price = get_land_price_raw(ref_raw)                                                         # Tính giá trị định giá đất của nó (raw_price)
                        with open(log_file_path, "a", encoding="utf-8") as log_file:
                            log_file.write(f"ref_raw: ref{ref_raws.index(ref_raw)+1}_raw, raw_price: {raw_price}\n")
                            log_file.write(f"ref{ref_raws.index(ref_raw)+1}_raw:\n {ref_raw}\n")
                        
                        if i not in used_indices and pd.notna(raw_price):                                               # Nếu chưa được ghép (không nằm trong used_indices) và không bị thiếu, tính |raw_price - pct_price|
                            diffs.append(abs(raw_price - pct_price))
                        else:
                            diffs.append(np.inf)                                                                        # Ngược lại, nếu đã dùng rồi hoặc giá trị thiếu → gán khoảng cách vô cực (np.inf) để loại bỏ.

                    with open(log_file_path, "a", encoding="utf-8") as log_file:
                        log_file.write(f"ref: ref{ref_pcts.index(ref_pct)+1}_pct, diffs: {diffs}\n")

                    best_idx = int(np.argmin(diffs))                                        #  Chọn cặp khớp tốt nhất. Tìm chỉ số của ref_raw có sai khác nhỏ nhất so với ref_pct.
                    matched_idx.append(best_idx)                                                    # Ghi nhận ánh xạ tốt nhất (best_idx).
                    used_indices.add(best_idx)                                                      # Đánh dấu chỉ số đã dùng trong used_indices để không ghép lại.
                    with open(log_file_path, "a", encoding="utf-8") as log_file:
                        log_file.write(f"used_indices: {used_indices}\n")
                        log_file.write(f"best_idx: {best_idx}\n")

                for i, idx in enumerate(matched_idx):                                       # In và ghi lại kết quả ánh xạ
                    print(f"ref_pcts[{i}] matched with ref_raws[{idx}]")                        # In ra ánh xạ từ mỗi ref_pct[i] sang ref_raws[idx] tương ứng.
                    with open(log_file_path, "a", encoding="utf-8") as log_file:
                        log_file.write(f"ref_pcts[{i}] matched with ref_raws[{idx}]\n")

                idx_matches = dict(enumerate(matched_idx))                                      # Tạo dict ánh xạ chỉ số: {0: best_idx_1, 1: best_idx_2, ...}
                # return idx_matches


                # ---- STEP 3: BUILD DATA STRUCTURES ----

                # Function to build the assetsManagement structure
                def build_assets_management(entry):         # Tạo phần thông tin tài sản chính từ một entry (dòng dữ liệu), bao gồm:
                    return {
                        "geoJsonPoint": get_info_location(entry.get(normalize_att("Tọa độ vị trí"))),       # Thông tin tọa độ
                        "basicAssetsInfo": {
                            "basicAddressInfo": {
                                "fullAddress": str(entry.get(normalize_att("Địa chỉ tài sản"), "")),
                            },
                            "totalPrice": smart_parse_float(entry.get(normalize_att("Giá đất (đồng/m²)"))),                         # Địa chỉ, giá đất, mục đích sử dụng, diện tích, kích thước
                            "landUsePurposeInfo": get_info_purpose(str(entry.get(normalize_att("Mục đích sử dụng đất ")))),
                            "valuationLandUsePurposeInfo": get_info_purpose(str(entry.get(normalize_att("Mục đích sử dụng đất ")))),
                            "area": smart_parse_float(entry.get(normalize_att("Quy mô diện tích (m²)\n(Đã trừ đất thuộc quy hoạch lộ giới)"))),
                            "width": smart_parse_float(entry.get(normalize_att("Chiều rộng (m)"))),
                            "height": smart_parse_float(entry.get(normalize_att("Chiều dài (m)"))),
                            # "percentQuality": float(entry.get(normalize_att("Chất lượng còn lại (%)"), 0)) if pd.notna(entry.get(normalize_att("Chất lượng còn lại (%)"), 0)) else np.nan,
                            "percentQuality": float(val) if pd.notna(val := entry.get(normalize_att("Chất lượng còn lại (%)"))) and str(val).strip() != "" else 1.0,        # Các giá trị liên quan đến giá trị xây dựng, thương lượng, chuyển đổi, v.v.
                            "newConstructionUnitPrice": get_info_unit_price(str(entry.get(normalize_att("Đơn giá xây dựng mới (đồng/m²)"), 0))),
                            "constructionValue": float(entry.get(normalize_att("Giá trị công trình xây dựng (đồng)"), 0)),
                            "sellingPrice": float(entry.get(normalize_att("Giá rao bán (đồng)"))),
                            "negotiablePrice": parse_human_number(entry.get(normalize_att("Giá thương lượng (đồng)"))),
                            "landConversion": parse_human_number(entry.get(normalize_att("Chi phí chuyển mục đích sử dụng đất/ Chênh lệch tiền chuyển mục đích sử dụng đất (đồng)"), 0)),
                            "landRoadBoundary": float(entry.get(normalize_att("Giá trị phần đất thuộc lộ giới (đồng)"), np.nan)),
                            "landValue": float(entry.get(normalize_att("Giá trị đất (đồng)"), np.nan)),
                            "landPrice": float(entry.get(normalize_att("Giá đất (đồng/m²)"))),
                        },
                        
                    }
                

                # Function to build the comparison/percentage fields structure
                def build_compare_fields(entry):
                    res = {}                                # Tạo các trường dùng để so sánh (adjustment fields) giữa tài sản chính và tài sản tham chiếu, bao gồm mô tả + phần trăm điều chỉnh.
                    for key, att in att_en_vn.items():      # Tạo dictionary res để chứa kết quả cuối cùng. Mỗi trường dữ liệu (vd: legalStatus, location) sẽ là 1 key trong res.
                        norm_att = normalize_att(att)       # Chuẩn hóa lại tên thuộc tính một lần nữa để đảm bảo đồng nhất khi tra cứu trong các dict như att_to_ord.
                        if norm_att in att_to_ord:          # Kiểm tra xem tên thuộc tính đã chuẩn hóa có tồn tại trong ánh xạ att_to_ord hay không. att_to_ord ánh xạ từ tên thuộc tính chuẩn hóa sang một số thứ tự (ordinal),
                            try:
                                ord_val = att_to_ord[norm_att]
                                # Add base description field
                                res[key] = {                # Từ entry, lấy giá trị mô tả tại vị trí (ord_val, norm_att) Vì dữ liệu được lưu trong entry là kiểu MultiIndex (tuple key), nên truy cập bằng (ord_val, norm_att)
                                    "description": str(entry.get((ord_val, norm_att), ""))
                                }
                                # Add the percentage adjustments
                                res[key].update(add_pct(entry, att))        # Gọi hàm add_pct() để thêm các giá trị:percent: tỷ lệ gốc, percentAdjust: tỷ lệ điều chỉnh, valueAdjust: mức điều chỉnh (giá trị quy đổi)


                            except Exception as e:
                                print(f"⚠️ Skipping attribute {key} due to error: {e}")
                                with open(log_file_path, "a", encoding="utf-8") as log_file:
                                    log_file.write(f"⚠️ Skipping attribute {key} due to error: {e}\n")      # Ghi log lỗi vào file nếu có lỗi truy cập entry, tránh chương trình bị crash.
                                continue
                        else:
                            print(f"⚠️ Skipping attribute '{key}' because it's missing in att_to_ord")      # Ghi log cảnh báo rằng thuộc tính này không thể xử lý vì chưa có ánh xạ thứ tự.
                            with open(log_file_path, "a", encoding="utf-8") as log_file:
                                log_file.write(f"⚠️ Skipping attribute '{key}' because it's missing in att_to_ord\n")
                    return res

                # Function to add percentage values to the comparison fields
                def add_pct(entry, att):
                    # print("This is entry:", entry)
                    # print("Tỷ lệ", float(entry.get((att_to_ord[att], "Tỷ lệ"), 0)))
                    # print("Tỷ lệ điều chỉnh", float(entry.get((att_to_ord[att], "Tỷ lệ điều chỉnh"), 0)))
                    # print("Mức điều chỉnh", float(entry.get((att_to_ord[att], "Mức điều chỉnh"), 0)))
                    return {                                                                           # Hàm thêm các giá trị tỷ lệ điều chỉnh
                        "percent": float(entry.get((att_to_ord[att], normalize_att("Tỷ lệ")), 0)),
                        "percentAdjust": float(entry.get((att_to_ord[att], normalize_att("Tỷ lệ điều chỉnh")), 0)),
                        "valueAdjust": float(entry.get((att_to_ord[att], normalize_att("Mức điều chỉnh")), 0)),
                    }

                # Function to create the assetsCompareManagement structure
                def create_assets_compare(entry_pct, is_main=False):
                    data = {}
                    if not is_main:       # if it is a reference property       # Với is_main=False: Dùng ánh xạ idx_matches giữa ref_pcts và ref_raws để ghép đúng tài sản gốc tương ứng.
                        idx_pct = ref_pcts.index(entry_pct)
                        idx_raw = idx_matches[idx_pct]
                        entry_raw = ref_raws[idx_raw]
                        data["assetsManagement"] = build_assets_management(entry_raw)
                        data.update(build_compare_fields(entry_pct))
                        data["isCompare"] = True
                    else:                 # if it is the main property
                        data["assetsManagement"] = build_assets_management(main_raw)        # Với is_main=True: Tạo đối tượng từ main_raw (dữ liệu gốc).
                        data.update(build_compare_fields(entry_pct))
                        data["isCompare"] = False
                    return data


                # Create the structures for main and reference properties
                assets_cmp_mng_main = create_assets_compare(main_pct, is_main=True)
                assets_cmp_mng_refs = []
                for ref in ref_pcts:
                    assets_cmp_mng_ref = create_assets_compare(ref, is_main=False)
                    assets_cmp_mng_refs.append(assets_cmp_mng_ref)

                # Get current UTC time
                now = datetime.now(timezone.utc)
                # Create formatted string
                created_date_str = now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]  # trim to milliseconds
                timestamp_int = int(now.timestamp())

                # Create the final assetsCompareManagements structure
                assets_compare_managements = [assets_cmp_mng_main] + assets_cmp_mng_refs


                # Create the final data structure for an Excel file 
                new_data = {                    # Tạo cấu trúc dữ liệu tổng thể new_data
                    "createdDate": created_date_str,
                    "assetsCompareManagements": assets_compare_managements,
                }
                for i in range(4):
                    if new_data["assetsCompareManagements"][i]["assetsManagement"]["geoJsonPoint"] == None:
                        del new_data["assetsCompareManagements"][i]["assetsManagement"]["geoJsonPoint"]


                # ---- INSERT DATA INTO MONGODB ----

                client = MongoClient(MONGO_URI)
                # Use the correct database and collection
                db = client["assets-valuemind"]
                collection = db["Danh"]

                # Insert data into MongoDB
                insert_excel_data = collection.insert_one(new_data)
                insert_id = insert_excel_data.inserted_id
                # print(f"✅ Inserted excel data with ID: {insert_id}")
                with open(log_file_path, "a", encoding="utf-8") as log_file:
                    log_file.write(f"✅ Inserted excel data with ID: {insert_id}\n")
                    log_file.write("----------------------------------------------------------------------------------------------\n")

                    
            except Exception as e:
                error_message = traceback.format_exc()
                print(f"❌ Failed to process sheet {sheet} in {file_path}:\n{error_message}")
                with open(log_file_path, "a", encoding="utf-8") as log_file:
                    log_file.write(f"❌ Failed to process sheet {sheet} in {file_path}\n{error_message}\n")
                    log_file.write("----------------------------------------------------------------------------------------------\n")
                continue
        

    except Exception as e:
        error_message = traceback.format_exc()
        print(f"❌ Failed to process {file_path}:\n{error_message}")
        with open(log_file_path, "a", encoding="utf-8") as log_file:
            log_file.write(f"❌ Failed to process {file_path}\n{error_message}\n")
            log_file.write("----------------------------------------------------------------------------------------------\n")
        continue
    
    found += len(sheet_list)
    # with open(log_file_path, "a", encoding="utf-8") as log_file:
    #     log_file.write("----------------------------------------------------------------------------------------------\n")

# with open(log_file_path, "r", encoding="utf-8") as f:
#         processed_files = [line.strip() for line in f if line.strip()]
# print("Total number of processed files:", len(processed_files))

print("Total number of files:", len(sheet_map))
total_sheets = sum(len(sheets) for sheets in sheet_map.values())
# print(f"Total number of sheets: {total_sheets}")
print(f"Total number of sheets: {found + missing}")
with open(log_file_path, "a", encoding="utf-8") as log_file:
    log_file.write(f"Total number of sheets: {found + missing}\n")
# print("x:", x)
