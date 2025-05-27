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
        get_info_unit_price, find_meta_data, find_comparison_table_end, find_raw_table_end, match_idx, parse_human_number, \
        get_max_width, get_facade_info, assign_dimensions
import os
import re
from pymongo import MongoClient
import traceback
import logging
import argparse


# ---- CONFIGURATION ----
# EXCEL_FILE = "DV_Can Giuoc.xlsx"
# EXCEL_FILE = "DV_Le Trong Tan.xlsx"
MONGO_URI = "mongodb://dev-valuemind:W57mFPVT57lt3wU@10.10.0.42:27021/?replicaSet=rs0&directConnection=true&authSource=assets-valuemind"
collection_name = "test-dim"
LOG_MISSING = False

# ---- GLOBAL PARAMETERS ----
sheet_idx = 0
raw_col_length = 11
pct_col_length = 6


# ---- ARGUMENT PARSER ----
parser = argparse.ArgumentParser(description="Real estate data ingestion")
parser.add_argument("-y", type=int, required=True, help="Year of the dataset (e.g., 2024)")
parser.add_argument("-m", type=str, required=True, help="Month of the dataset (e.g., '1' or '01')")

args = parser.parse_args()

# Use args.year and args.month in your script
year = args.y
month_ = args.m

# Normalize month string based on year rule
if year in [2022, 2025]:
    month = f"{int(month_):02d}"  # zero-padded: "01", "02", ...
else:
    month = str(int(month_)) 



# Read list of Excel paths
# with open(f"comparison_files_{month}_{year}.txt", "r", encoding="utf-8") as f:
#     excel_paths = [line.strip() for line in f if line.strip()]
# open(log_file_path", "w", encoding="utf-8").close()
# total_sheets = 0

# ---- LOAD EXCEL PATHS AND SHEETS ----
comparison_file_path = f"comparison_files_{month}_{year}.txt"
sheet_map = {}

detect_folder = os.path.join(os.getcwd(), f"file_detection_{year}")
comparison_file = os.path.join(detect_folder, comparison_file_path)

with open(comparison_file, "r", encoding="utf-8") as f:
    for line in f:
        if ">>>" in line:
            path, sheets = line.rstrip("\n").split(" >>> ")
            sheet_map[path.strip()] = [s for s in sheets.split("&&")]

# ---- LOG FILE ----
log_folder = os.path.join(os.getcwd(), f"reading_logs_{year}")
os.makedirs(log_folder, exist_ok=True)


# 📄 Create log file path inside the year-based folder
# log_file_path = f"reading_logs_{month}_{year}.txt"
log_file_path = os.path.join(log_folder, f"reading_logs_{month}_{year}.txt")
open(log_file_path, "w", encoding="utf-8").close()

found = 0
missing = 0
succeed = 0

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
                raw_start_idx = find_row_index_containing(df, "HẠNG MỤC") + 1
                pct_start_idx = find_comparison_table_start(df)

                # with open(log_file_path, "a", encoding="utf-8") as log_file:
                #     log_file.write(f"raw_start_idx: {raw_start_idx}\n")
                #     log_file.write(f"pct_start_idx: {pct_start_idx}\n")

                # Slice the first half of the file
                df_raw = df.iloc[:pct_start_idx]
                df_raw = df_raw.dropna(axis=1, how='all')
                min_valid_cells = 10
                df_raw = df_raw.loc[:, df_raw.notna().sum() >= min_valid_cells].reset_index(drop=True)

                non_empty_cols_raw = df_raw.columns[df_raw.notna().any()].tolist()
                first_valid_raw_col_idx = df_raw.columns.get_loc(non_empty_cols_raw[0])

                raw_col_start = first_valid_raw_col_idx + 1  # TSTDG usually comes after attributes



                # Slice the second half of the file
                df_pct = df.iloc[pct_start_idx:]
                df_pct = df_pct.dropna(axis=1, how='all')
                min_valid_cells = 5
                df_pct = df_pct.loc[:, df_pct.notna().sum() >= min_valid_cells].reset_index(drop=True)

                non_empty_cols_cmp = df_pct.columns[df_pct.notna().any()].tolist()
                first_valid_cmp_col_idx = df_pct.columns.get_loc(non_empty_cols_cmp[0])

                pct_col_start = first_valid_cmp_col_idx + 0  # attributes (e.g., "Giá thị trường") are in this column
                
                pct_start_idx = find_comparison_table_start(df_pct)


                # with open(log_file_path, "a", encoding="utf-8") as log_file:
                #     log_file.write(f"raw_col_start: {raw_col_start}\n")
                #     log_file.write(f"pct_col_start: {pct_col_start}\n")

                with open(log_file_path, "a", encoding="utf-8") as log_file:
                    log_file.write(f"df_pct:\n {df_pct}\n")

                # Indices of subtext in the excel file => Helper function for finding the end of the raw data table
                indicator_indices = find_meta_data(df, indicator_text="thời điểm")
                if len(indicator_indices) < 2:
                    with open(log_file_path, "a", encoding="utf-8") as log_file:
                        log_file.write("⚠️ Could not find the second 'Thời điểm ...' to determine raw_end_idx, switching to 'STT'\n")
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
                    
                raw_end_idx = find_raw_table_end(df, second_eval_row=indicator_indices[1])
                pct_end_idx = find_comparison_table_end(df_pct)


                raw_col_end = raw_col_start + raw_col_length
                pct_col_end = pct_col_start + pct_col_length


                # ---- FIND AND PARSE TABLES FROM EXCEL FILES ----

                # Extract the raw table
                raw_table = df_raw.iloc[raw_start_idx:raw_end_idx+1, raw_col_start:raw_col_end].dropna(how="all")
                raw_table.reset_index(drop=True, inplace=True)

                # Dynamically assign column names based on actual number of columns
                num_raw_cols = raw_table.shape[1]
                if num_raw_cols < 3:
                    raise ValueError(f"Expected at least 3 columns (attribute + TSTDG + at least 1 TSSS), but got {num_raw_cols}")

                raw_col_names = ["attribute", "TSTDG"] + [f"TSSS{i}" for i in range(1, num_raw_cols - 1)]
                raw_table.columns = raw_col_names
                raw_table['attribute'] = raw_table['attribute'].apply(normalize_att)
                with open(log_file_path, "a", encoding="utf-8") as log_file:
                    log_file.write(f"raw_table:\n {raw_table}\n") 
                    log_file.write(f" - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -\n")



                # Bottom table has percentage comparison values (C1, C2...)
                pct_table = df_pct.iloc[pct_start_idx:pct_end_idx+1, pct_col_start:pct_col_end].dropna(how="all")
                pct_table.columns = ["ord", "attribute", "TSTDG", "TSSS1", "TSSS2", "TSSS3"]
                pct_table.reset_index(drop=True, inplace=True)
                pct_table['ord'] = pct_table['ord'].ffill()
                # pct_table['ord'] = pct_table['ord'].astype(str).str.strip()
                pct_table['attribute'] = pct_table['attribute'].apply(normalize_att)
                with open(log_file_path, "a", encoding="utf-8") as log_file:
                    log_file.write(f"pct_table:\n {pct_table}\n")
                    log_file.write(f" - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -\n")
                

                # Match attributes with corresponding field names: DB fieldName -> attribute
                att_en_vn = {
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
                skip_attrs = [normalize_att(i) for i in ["Tỷ lệ", "Tỷ lệ điều chỉnh", "Mức điều chỉnh"]]
                main_rows = pct_table[~pct_table["attribute"].isin(skip_attrs)] 
                main_rows = main_rows.drop_duplicates(subset=["ord","attribute"], keep="first")
                main_rows['attribute'] = main_rows['attribute'].apply(normalize_att)
                att_to_ord = dict(zip(main_rows["attribute"], main_rows["ord"]))
                print("att_to_ord:", att_to_ord)
                # with open(log_file_path, "a", encoding="utf-8") as log_file:
                #         log_file.write(f"att_to_ord: {att_to_ord}\n")
                


                # ---- EXTRACT RAW AND COMPARISON TABLES ----
                # Function to extract the raw table
                def extract_col_raw(col_name):
                    return dict(zip(raw_table["attribute"], raw_table[col_name].fillna(np.nan)))


                # Function to extract the comparison table
                def extract_col_pct(col_name):
                    ord_key = list(zip(pct_table["ord"], pct_table["attribute"]))
                    ord_val = pct_table[col_name].fillna(np.nan)
                    return dict(zip(ord_key, ord_val))


                # Dynamically extract all TSSS columns
                main_raw = extract_col_raw("TSTDG")
                ref_raws = [
                    extract_col_raw(col)
                    for col in raw_col_names
                    if col.startswith("TSSS") or col.startswith("TSCM")
                ]

                # Filter out very empty reference columns
                ref_raws = [ref for ref in ref_raws if sum(pd.notna(v) for v in ref.values()) >= 10]


                # Dynamically extract all TSSS columns from the comparison table
                main_pct = extract_col_pct("TSTDG")
                num_pct_cols = pct_table.shape[1]
                if num_pct_cols < 3:
                    raise ValueError(f"Expected at least 3 columns (ord, attribute, TSTDG, TSSS...), got {num_pct_cols}")

                pct_col_names = ["ord", "attribute"] + [f"TSTDG"] + [f"TSSS{i}" for i in range(1, num_pct_cols - 2)]
                pct_table.columns = pct_col_names

                # Extract TSSS* columns dynamically from pct_table
                ref_pcts = [extract_col_pct(col) for col in pct_col_names if col.startswith("TSSS") or col.startswith("TSCM")]

                
                # Match the index of reference properties from comparison tables to raw tables
                # def match_idx(ref_pcts, ref_raws):
                matched_idx = []  
                used_indices = set()

                for ref_pct in ref_pcts:
                    pct_price = get_land_price_pct(ref_pct)
                    with open(log_file_path, "a", encoding="utf-8") as log_file:
                        log_file.write(f"ref: ref{ref_pcts.index(ref_pct)+1}_pct, pct_price: {pct_price}\n")
                        # log_file.write(f"ref_raws: {ref_raws}\n")
                    diffs = []
                    # diffs = [
                    #     abs(get_land_price_raw(ref_raw) - pct_price)
                    #     if i not in used_indices and pd.notna(get_land_price_raw(ref_raw)) else np.inf
                    #     for i, ref_raw in enumerate(ref_raws)
                    # ]
                    for i, ref_raw in enumerate(ref_raws):
                        raw_price = get_land_price_raw(ref_raw)
                        # with open(log_file_path, "a", encoding="utf-8") as log_file:
                        #     log_file.write(f"ref_raw: ref{ref_raws.index(ref_raw)+1}_raw, raw_price: {raw_price}\n")
                        #     log_file.write(f"ref{ref_raws.index(ref_raw)+1}_raw:\n {ref_raw}\n")
                        
                        if i not in used_indices and pd.notna(raw_price):
                            diffs.append(abs(raw_price - pct_price))
                        else:
                            diffs.append(np.inf)

                    with open(log_file_path, "a", encoding="utf-8") as log_file:
                        log_file.write(f"ref: ref{ref_pcts.index(ref_pct)+1}_pct, diffs: {diffs}\n")

                    best_idx = int(np.argmin(diffs))
                    matched_idx.append(best_idx)
                    used_indices.add(best_idx)
                    with open(log_file_path, "a", encoding="utf-8") as log_file:
                        log_file.write(f"used_indices: {used_indices}\n")
                        log_file.write(f"best_idx: {best_idx}\n")

                for i, idx in enumerate(matched_idx):
                    print(f"ref_pcts[{i}] matched with ref_raws[{idx}]")
                    with open(log_file_path, "a", encoding="utf-8") as log_file:
                        log_file.write(f"ref_pcts[{i}] matched with ref_raws[{idx}]\n")

                idx_matches = dict(enumerate(matched_idx))
                # return idx_matches


                # ---- STEP 3: BUILD DATA STRUCTURES ----
                
                # Function to build the assetsManagement structure
                def build_assets_management(entry):
                    width = smart_parse_float(entry.get(normalize_att("Chiều rộng (m)")), log_missing=LOG_MISSING)
                    height = smart_parse_float(entry.get(normalize_att("Chiều dài (m)")), log_missing=LOG_MISSING)
                    depth = smart_parse_float(entry.get(normalize_att("Chiều sâu (m)")), log_missing=LOG_MISSING)
                    location = str(entry.get(normalize_att("Vị trí"), ""))

                    facade = get_facade_info(
                                entry.get(normalize_att("Độ rộng mặt tiền (m)")),
                                entry.get(normalize_att("Vị trí"))
                            )
                    has_facade = facade.get("hasFacade", False)
                    width, height = assign_dimensions(width, height, depth, has_facade)

                    return {
                        "geoJsonPoint": get_info_location(entry.get(normalize_att("Tọa độ vị trí"))),
                        "basicAssetsInfo": {
                            "basicAddressInfo": {
                                "fullAddress": str(entry.get(normalize_att("Địa chỉ tài sản"))),
                            },
                            "totalPrice": smart_parse_float(entry.get(normalize_att("Giá đất (đồng/m²)"))),
                            "landUsePurposeInfo": get_info_purpose(str(entry.get(normalize_att("Mục đích sử dụng đất ")))),
                            "valuationLandUsePurposeInfo": get_info_purpose(str(entry.get(normalize_att("Mục đích sử dụng đất ")))),
                            "area": smart_parse_float(entry.get(normalize_att("Quy mô diện tích (m²)\n(Đã trừ đất thuộc quy hoạch lộ giới)")), log_missing=LOG_MISSING),
                            "location": location,
                            "width": width,
                            "maxWidth": get_max_width(width),
                            "facade": facade,
                            "height": height,
                            # "percentQuality": float(entry.get(normalize_att("Chất lượng còn lại (%)"), 0)) if pd.notna(entry.get(normalize_att("Chất lượng còn lại (%)"), 0)) else np.nan,
                            "percentQuality": float(val) if pd.notna(val := entry.get(normalize_att("Chất lượng còn lại (%)"))) and str(val).strip() != "" else 1.0,
                            "newConstructionUnitPrice": get_info_unit_price(str(entry.get(normalize_att("Đơn giá xây dựng mới (đồng/m²)"), np.nan))),
                            "constructionValue": float(entry.get(normalize_att("Giá trị công trình xây dựng (đồng)"), np.nan)),
                            "sellingPrice": float(entry.get(normalize_att("Giá rao bán (đồng)"))),
                            "negotiablePrice": parse_human_number(entry.get(normalize_att("Giá thương lượng (đồng)"))),
                            "landConversion": parse_human_number(entry.get(normalize_att("Chi phí chuyển mục đích sử dụng đất/ Chênh lệch tiền chuyển mục đích sử dụng đất (đồng)"), 0)),
                            "landRoadBoundary": float(entry.get(normalize_att("Giá trị phần đất thuộc lộ giới (đồng)"), np.nan)),
                            "landValue": float(entry.get(normalize_att("Giá trị đất (đồng)"))),
                            "landPrice": float(entry.get(normalize_att("Giá đất (đồng/m²)"))),
                        },
                        
                    }
                

                # Function to build the comparison/percentage fields structure
                def build_compare_fields(entry):
                    res = {}
                    for key, att in att_en_vn.items():
                        norm_att = normalize_att(att)
                        if norm_att in att_to_ord:
                            try:
                                ord_val = att_to_ord[norm_att]
                                # Add base description field
                                res[key] = {
                                    "description": str(entry.get((ord_val, norm_att), ""))
                                }
                                # Add the percentage adjustments
                                res[key].update(add_pct(entry, att))
                            except Exception as e:
                                print(f"⚠️ Skipping attribute {key} due to error: {e}")
                                with open(log_file_path, "a", encoding="utf-8") as log_file:
                                    log_file.write(f"⚠️ Skipping attribute {key} due to error: {e}\n")
                                continue
                        else:
                            print(f"⚠️ Skipping attribute '{key}' because it's missing in att_to_ord")
                            with open(log_file_path, "a", encoding="utf-8") as log_file:
                                log_file.write(f"⚠️ Skipping attribute '{key}' because it's missing in att_to_ord\n")
                    return res

                # Function to add percentage values to the comparison fields
                def add_pct(entry, att):
                    # print("This is entry:", entry)
                    # print("Tỷ lệ", float(entry.get((att_to_ord[att], "Tỷ lệ"), 0)))
                    # print("Tỷ lệ điều chỉnh", float(entry.get((att_to_ord[att], "Tỷ lệ điều chỉnh"), 0)))
                    # print("Mức điều chỉnh", float(entry.get((att_to_ord[att], "Mức điều chỉnh"), 0)))
                    return {
                        "percent": float(entry.get((att_to_ord[att], normalize_att("Tỷ lệ")), 0)),
                        "percentAdjust": float(entry.get((att_to_ord[att], normalize_att("Tỷ lệ điều chỉnh")), 0)),
                        "valueAdjust": float(entry.get((att_to_ord[att], normalize_att("Mức điều chỉnh")), 0)),
                    }

                # Function to create the assetsCompareManagement structure
                def create_assets_compare(entry_pct, is_main=False):
                    data = {}
                    if not is_main:       # if it is a reference property
                        idx_pct = ref_pcts.index(entry_pct)
                        idx_raw = idx_matches[idx_pct]
                        entry_raw = ref_raws[idx_raw]
                        data["assetsManagement"] = build_assets_management(entry_raw)
                        data.update(build_compare_fields(entry_pct))
                        data["isCompare"] = True
                    else:                 # if it is the main property
                        data["assetsManagement"] = build_assets_management(main_raw)
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
                new_data = {
                    "createdDate": created_date_str,
                    "assetsCompareManagements": assets_compare_managements,
                    "fileRepo": sheet + "||" + file_path,
                }
                for i in range(4):
                    if new_data["assetsCompareManagements"][i]["assetsManagement"]["geoJsonPoint"] == None:
                        del new_data["assetsCompareManagements"][i]["assetsManagement"]["geoJsonPoint"]


                # ---- INSERT DATA INTO MONGODB ----

                client = MongoClient(MONGO_URI)
                # Use the correct database and collection
                db = client["assets-valuemind"]
                collection = db[collection_name]

                # Insert data into MongoDB
                insert_excel_data = collection.insert_one(new_data)
                insert_id = insert_excel_data.inserted_id
                # print(f"✅ Inserted excel data with ID: {insert_id}")
                with open(log_file_path, "a", encoding="utf-8") as log_file:
                    log_file.write(f"✅ Inserted excel data with ID: {insert_id}\n")
                    log_file.write("----------------------------------------------------------------------------------------------\n")
                succeed += 1

                    
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
print(f"Total number of success: {succeed}")
# print("x:", x)
