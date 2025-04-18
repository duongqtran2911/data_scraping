import pandas as pd
import datetime
from datetime import datetime, timezone
from pymongo import MongoClient
import uuid
from datetime import datetime
import numpy as np
import json
from write_data_utils import normalize_att, find_row_index_containing, smart_parse_float, \
        find_comparison_table_start, get_land_price_raw, get_land_price_pct, get_info_location, \
        find_meta_data, find_comparison_table_end, find_raw_table_end
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
year = 2025
month = "01"

# Read list of Excel paths
# with open(f"comparison_files_{month}_{year}.txt", "r", encoding="utf-8") as f:
#     excel_paths = [line.strip() for line in f if line.strip()]
# open(f"reading_logs_{month}_{year}.txt", "w", encoding="utf-8").close()
# total_sheets = 0


# ---- LOAD EXCEL PATHS AND SHEETS ----
comparison_file_path = f"comparison_files_{month}_{year}.txt"
sheet_map = {}

with open(comparison_file_path, "r", encoding="utf-8") as f:
    for line in f:
        if ">>>" in line:
            path, sheets = line.rstrip("\n").split(" >>> ")
            # sheet_map[path.strip()] = [s.strip() for s in sheets.split(",") if s.strip()]
            sheet_map[path.strip()] = [s for s in sheets.split("&&")]

# ---- LOG FILE ----
log_file_path = f"reading_logs_{month}_{year}.txt"
open(log_file_path, "w", encoding="utf-8").close()

# ---- PROCESS EACH FILE AND SHEET ----
for file_path, sheet_list in sheet_map.items():
    try:
        if not os.path.isfile(file_path):
            print(f"⭕️ File not found: {file_path}")
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
                # xls = pd.ExcelFile(file_path)
                # sheet = xls.sheet_names[sheet_idx]

                df = pd.read_excel(xls, sheet_name=sheet, header=None)

                # ---- DETECT RAW DATA TABLE AND COMPARISON TABLE ---- 
                raw_start_idx = find_row_index_containing(df, "HẠNG MỤC") + 1
                pct_start_idx = find_comparison_table_start(df)

                indicator_indices = find_meta_data(df, indicator_text="thời điểm")
                if len(indicator_indices) < 2:
                    raise ValueError("⚠️ Could not find the second 'Thời điểm ...' to determine raw_end_idx")
                
                raw_end_idx = find_raw_table_end(df, second_eval_row=indicator_indices[1])
                
                pct_end_idx = find_comparison_table_end(df)

                raw_col_start = 2
                raw_col_end = raw_col_start + raw_col_length

                pct_col_start = 1
                pct_col_end = pct_col_start + pct_col_length


                # ---- FIND AND PARSE TABLES FROM EXCEL FILES ----
                # Assume the top table starts around row 5 and has 4 columns: Attribute | TSDG | TSSS1 | TSSS2 | TSSS3

                # Extract the raw table
                raw_table = df.iloc[raw_start_idx:raw_end_idx+1, raw_col_start:raw_col_end].dropna(how="all")
                raw_table.reset_index(drop=True, inplace=True)

                # Dynamically assign column names based on actual number of columns
                num_raw_cols = raw_table.shape[1]
                if num_raw_cols < 3:
                    raise ValueError(f"Expected at least 3 columns (attribute + TSTDG + at least 1 TSSS), but got {num_raw_cols}")

                raw_col_names = ["attribute", "TSTDG"] + [f"TSSS{i}" for i in range(1, num_raw_cols - 1)]
                raw_table.columns = raw_col_names
                raw_table['attribute'] = raw_table['attribute'].apply(normalize_att)


                # Bottom table has % comparison values (C1, C2...)
                pct_table = df.iloc[pct_start_idx:pct_end_idx+1, pct_col_start:pct_col_end].dropna(how="all")
                pct_table.columns = ["ord", "attribute", "TSTDG", "TSSS1", "TSSS2", "TSSS3"]
                pct_table.reset_index(drop=True, inplace=True)
                pct_table['ord'] = pct_table['ord'].ffill()
                # pct_table['ord'] = pct_table['ord'].astype(str).str.strip()
                pct_table['attribute'] = pct_table['attribute'].apply(normalize_att)
                # with open(f"reading_logs_{month}_{year}.txt", "a", encoding="utf-8") as log_file:
                #     log_file.write(f"pct_table: {pct_table[["ord", "attribute"]]}\n")
                

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
                with open(f"reading_logs_{month}_{year}.txt", "a", encoding="utf-8") as log_file:
                        log_file.write(f"att_to_ord: {att_to_ord}\n")
                


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
                    if col.startswith("TSSS")
                ]

                # Filter out very empty reference columns
                ref_raws = [ref for ref in ref_raws if sum(pd.isna(v) for v in ref.values()) <= 10]


                # Dynamically extract all TSSS columns from the comparison table
                main_pct = extract_col_pct("TSTDG")
                num_pct_cols = pct_table.shape[1]
                if num_pct_cols < 3:
                    raise ValueError(f"Expected at least 3 columns (ord, attribute, TSTDG, TSSS...), got {num_pct_cols}")

                pct_col_names = ["ord", "attribute"] + [f"TSTDG"] + [f"TSSS{i}" for i in range(1, num_pct_cols - 2)]
                pct_table.columns = pct_col_names

                # Extract TSSS* columns dynamically from pct_table
                ref_pcts = [extract_col_pct(col) for col in pct_col_names if col.startswith("TSSS")]



                # Extract the raw and comparison tables
                # main_raw = extract_col_raw("TSTDG")
                # ref1_raw = extract_col_raw("TSSS1")
                # ref2_raw = extract_col_raw("TSSS2")
                # ref3_raw = extract_col_raw("TSSS3")
                # ref4_raw = extract_col_raw("TSSS4")
                # ref5_raw = extract_col_raw("TSSS5")
                # ref6_raw = extract_col_raw("TSSS6")
                # ref7_raw = extract_col_raw("TSSS7")
                # ref8_raw = extract_col_raw("TSSS8")
                # ref9_raw = extract_col_raw("TSSS9")


                # main_pct = extract_col_pct("TSTDG")
                # ref1_pct = extract_col_pct("TSSS1")
                # ref2_pct = extract_col_pct("TSSS2")
                # ref3_pct = extract_col_pct("TSSS3")


                # # Save all reference tables data in lists 
                # ref_raws = [ref1_raw, ref2_raw, ref3_raw, ref4_raw, ref5_raw, \
                #             ref6_raw, ref7_raw, ref8_raw, ref9_raw]
                            # , ref10_raw, ref11_raw]
                # print(len(ref_raws))

                # ref_raws = [ref for ref in ref_raws if sum(pd.isna(v) for v in ref.values()) <= 10]
                # ref_pcts = [ref1_pct, ref2_pct, ref3_pct]


                # Match the index of reference properties from comparison tables to raw tables
                matched_idx = []  # indices in ref_raws that match each ref_pct in order
                used_indices = set()

                for ref_pct in ref_pcts:
                    pct_price = get_land_price_pct(ref_pct)
                    
                    # # Compare against all ref_raws that haven't been used
                    # valid_refs = [
                    #     (i, get_land_price_raw(ref_raw))
                    #     for i, ref_raw in enumerate(ref_raws)
                    #     if i not in used_indices and get_land_price_raw(ref_raw) is not None
                    # ]

                    # if not valid_refs:
                    #     raise ValueError(f"No usable reference properties found to match with comparison price: {pct_price}")
                    # diffs = [abs(raw_price - pct_price) for _, raw_price in valid_refs]
                    # best_idx = valid_refs[int(np.argmin(diffs))][0]

                    diffs = [
                        abs(get_land_price_raw(ref_raw) - pct_price)
                        if i not in used_indices and get_land_price_raw(ref_raw) is not None else np.inf
                        for i, ref_raw in enumerate(ref_raws)
                    ]

                    best_idx = int(np.argmin(diffs))
                    matched_idx.append(best_idx)
                    used_indices.add(best_idx)

                for i, idx in enumerate(matched_idx):
                    print(f"ref_pcts[{i}] matched with ref_raws[{idx}]")

                idx_matches = dict(enumerate(matched_idx))


                # ---- STEP 3: BUILD DATA STRUCTURES ----

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



                

                # Function to build the assetsManagement structure
                def build_assets_management(entry):
                    # print(entry.get("Tọa độ vị trí", 0))
                    # location_info = get_info_location(entry.get("Tọa độ vị trí", 0))
                    # print(location_info)
                    return {
                        "geoJsonPoint": get_info_location(entry.get(normalize_att("Tọa độ vị trí"), 0)),
                        "basicAssetsInfo": {
                            "basicAddressInfo": {
                                "fullAddress": str(entry.get(normalize_att("Địa chỉ tài sản"), "")),
                            },
                            "totalPrice": smart_parse_float(entry.get(normalize_att("Giá đất (đồng/m²)"), 0)),
                            "landUsePurposeInfo": get_info_purpose(str(entry.get(normalize_att("Mục đích sử dụng đất "), ""))),
                            "valuationLandUsePurposeInfo": get_info_purpose(str(entry.get(normalize_att("Mục đích sử dụng đất "), ""))),
                            "area": smart_parse_float(entry.get(normalize_att("Quy mô diện tích (m²)\n(Đã trừ đất thuộc quy hoạch lộ giới)"), 0)),
                            "width": smart_parse_float(entry.get(normalize_att("Chiều rộng (m)"), 0)),
                            "height": smart_parse_float(entry.get(normalize_att("Chiều dài (m)"), 0)),
                            "percentQuality": float(entry.get(normalize_att("Chất lượng còn lại (%)"), 0)) if entry.get(normalize_att("Chất lượng còn lại (%)"), 0) != "" else np.nan,
                            "newConstructionUnitPrice": float(entry.get(normalize_att("Đơn giá xây dựng mới (đồng/m²)"), 0)),
                            "constructionValue": float(entry.get(normalize_att("Giá trị công trình xây dựng (đồng)"), 0)),
                            "sellingPrice": float(entry.get(normalize_att("Giá rao bán (đồng)"), 0)),
                            "negotiablePrice": float(entry.get(normalize_att("Giá thương lượng (đồng)"), 0)),
                            "landConversion": float(entry.get(normalize_att("Chi phí chuyển mục đích sử dụng đất/ Chênh lệch tiền chuyển mục đích sử dụng đất (đồng)"), 0)),
                            "landRoadBoundary": float(entry.get(normalize_att("Giá trị phần đất thuộc lộ giới (đồng)"), 0)),
                            "landValue": float(entry.get(normalize_att("Giá trị đất (đồng)"), 0)),
                            "landPrice": float(entry.get(normalize_att("Giá đất (đồng/m²)"), 0)),
                        },
                        
                    }

                # Function to build the comparison/percentage fields structure
                # def build_compare_fields(entry):
                    res = {
                        "legalStatus": {
                            "description": str(entry.get((att_to_ord[normalize_att("Tình trạng pháp lý")], normalize_att("Tình trạng pháp lý")), "")),
                        },
                        "location":{
                            "description": str(entry.get((att_to_ord[normalize_att("Vị trí ")],normalize_att("Vị trí ")), "")),
                        },
                        "traffic": {
                            "description": str(entry.get((att_to_ord[normalize_att("Giao thông")], normalize_att("Giao thông")), "")),
                        },
                        "area": {
                            "description": str(entry.get((att_to_ord[normalize_att("Quy mô diện tích (m²)")], normalize_att("Quy mô diện tích (m²)")), "")),
                        },
                        "width": {
                            "description": str(entry.get((att_to_ord[normalize_att("Chiều rộng")], normalize_att("Chiều rộng")), "")),
                        }, 
                        "height": { 
                            "description": str(entry.get((att_to_ord[normalize_att("Chiều dài")], normalize_att("Chiều dài")), "")),
                        },
                        "population": {
                            "description": str(entry.get((att_to_ord[normalize_att("Dân cư")],normalize_att("Dân cư")), "")),
                        },
                        "shape": {
                            "description": str(entry.get((att_to_ord[normalize_att("Hình dáng")], normalize_att("Hình dáng")), "")),
                        },
                        "other": {
                            "description": str(entry.get((att_to_ord[normalize_att("Yếu tố khác (nếu có)")], normalize_att("Yếu tố khác (nếu có)")), "")),
                        }

                    }
                    for key in res.keys():
                        res[key].update(add_pct(entry, att_en_vn[key]))
                    return res
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
                                continue
                        else:
                            print(f"⚠️ Skipping attribute '{key}' because it's missing in att_to_ord")
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
                }
                for i in range(4):
                    if new_data["assetsCompareManagements"][i]["assetsManagement"]["geoJsonPoint"] == None:
                        del new_data["assetsCompareManagements"][i]["assetsManagement"]["geoJsonPoint"]


                # ---- INSERT DATA INTO MONGODB ----

                client = MongoClient(MONGO_URI)
                # Use the correct database and collection
                db = client["assets-valuemind"]
                collection = db["test"]

                # Insert data into MongoDB
                insert_excel_data = collection.insert_one(new_data)
                insert_id = insert_excel_data.inserted_id
                # print(f"✅ Inserted excel data with ID: {insert_id}")
                with open(f"reading_logs_{month}_{year}.txt", "a", encoding="utf-8") as log_file:
                    log_file.write(f"✅ Inserted excel data with ID: {insert_id}\n")
                    log_file.write("----------------------------------------------------------------------------------------------\n")

                    
            except Exception as e:
                error_message = traceback.format_exc()
                print(f"❌ Failed to process sheet {sheet} in {file_path}:\n{error_message}")
                with open(f"reading_logs_{month}_{year}.txt", "a", encoding="utf-8") as log_file:
                    log_file.write(f"❌ Failed to process sheet {sheet} in {file_path}:\n{error_message}\n")
                    log_file.write("----------------------------------------------------------------------------------------------\n")
                continue
        

    except Exception as e:
        error_message = traceback.format_exc()
        print(f"❌ Failed to process {file_path}:\n{error_message}")
        with open(f"reading_logs_{month}_{year}.txt", "a", encoding="utf-8") as log_file:
            log_file.write(f"❌ Failed to process {file_path}:\n{error_message}\n")
            log_file.write("----------------------------------------------------------------------------------------------\n")
        continue

    # with open(f"reading_logs_{month}_{year}.txt", "a", encoding="utf-8") as log_file:
    #     log_file.write("----------------------------------------------------------------------------------------------\n")

# with open(f"reading_logs_{month}_{year}.txt", "r", encoding="utf-8") as f:
#         processed_files = [line.strip() for line in f if line.strip()]
# print("Total number of processed files:", len(processed_files))

print("Total number of files:", len(sheet_map))
total_sheets = sum(len(sheets) for sheets in sheet_map.values())
print(f"Total number of sheets: {total_sheets}")
