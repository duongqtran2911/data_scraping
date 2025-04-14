import pandas as pd
import datetime
from datetime import datetime, timezone
from pymongo import MongoClient
import uuid
from datetime import datetime
import numpy as np
import json
from write_data_utils import normalize_att, find_row_index_containing, \
    find_comparison_table_start, get_land_price_raw, get_land_price_pct 
import os
import re
from pymongo import MongoClient



# ---- CONFIGURATION ----
# EXCEL_FILE = "DV_Can Giuoc.xlsx"
EXCEL_FILE = "DV_Le Trong Tan.xlsx"
MONGO_URI = "mongodb://dev-valuemind:W57mFPVT57lt3wU@10.10.0.42:27021/?replicaSet=rs0&directConnection=true&authSource=assets-valuemind"



# ---- GLOBAL PARAMETERS ----
sheet_idx = 0
raw_col_length = 11
pct_col_length = 6


# ---- LOAD EXCEL ----
xls = pd.ExcelFile(EXCEL_FILE)
sheet = xls.sheet_names[sheet_idx]

df = pd.read_excel(xls, sheet_name=sheet, header=None)




# ---- DETECT RAW DATA TABLE AND COMPARISON TABLE ---- 
raw_start_idx = find_row_index_containing(df, "TSTĐG") + 1
pct_start_idx = find_comparison_table_start(df)

raw_end_idx = int(df[df[1] == 29].index[0])
pct_end_idx = df[df[1] == "E4"].index[0]

raw_col_start = 2
raw_col_end = raw_col_start + raw_col_length

pct_col_start = 1
pct_col_end = pct_col_start + pct_col_length




# ---- FIND AND PARSE TABLES FROM EXCEL FILES ----
# Assume the top table starts around row 5 and has 4 columns: Attribute | TSDG | TSSS1 | TSSS2 | TSSS3

raw_table = df.iloc[raw_start_idx:raw_end_idx+1, raw_col_start:raw_col_end].dropna(how="all")
raw_table.columns = ["attribute", "TSTDG", "TSSS1", "TSSS2", "TSSS3", "TSSS4", "TSSS5", "TSSS6", "TSSS7", "TSSS8", "TSSS9"]
raw_table.reset_index(drop=True, inplace=True)


# Bottom table has % comparison values (C1, C2...)
pct_table = df.iloc[pct_start_idx:pct_end_idx+1, pct_col_start:pct_col_end].dropna(how="all")
pct_table.columns = ["ord", "attribute", "TSTDG", "TSSS1", "TSSS2", "TSSS3"]
pct_table.reset_index(drop=True, inplace=True)
pct_table['ord'] = pct_table['ord'].ffill()
# pct_table['ord'] = pct_table['ord'].astype(str).str.strip()
pct_table['attribute'] = pct_table['attribute'].apply(normalize_att)
pct_table


# Match attributes with corresponding field names: fieldName -> attribute
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
main_rows = main_rows.drop_duplicates(subset="ord", keep="first")
main_rows['attribute'] = main_rows['attribute'].apply(normalize_att)
att_to_ord = dict(zip(main_rows["attribute"], main_rows["ord"]))




# ---- EXTRACT RAW AND COMPARISON TABLES ----
# Function to extract the raw table
def extract_col_raw(col_name):
    return dict(zip(raw_table["attribute"], raw_table[col_name].fillna(np.nan)))


# Function to extract the comparison table
def extract_col_pct(col_name):
    ord_key = list(zip(pct_table["ord"], pct_table["attribute"]))
    ord_val = pct_table[col_name].fillna(np.nan)
    return dict(zip(ord_key, ord_val))


# Extract the raw and comparison tables
main_raw = extract_col_raw("TSTDG")
ref1_raw = extract_col_raw("TSSS1")
ref2_raw = extract_col_raw("TSSS2")
ref3_raw = extract_col_raw("TSSS3")
ref4_raw = extract_col_raw("TSSS4")
ref5_raw = extract_col_raw("TSSS5")
ref6_raw = extract_col_raw("TSSS6")
ref7_raw = extract_col_raw("TSSS7")
ref8_raw = extract_col_raw("TSSS8")
ref9_raw = extract_col_raw("TSSS9")

main_pct = extract_col_pct("TSTDG")
ref1_pct = extract_col_pct("TSSS1")
ref2_pct = extract_col_pct("TSSS2")
ref3_pct = extract_col_pct("TSSS3")


# Save all reference tables data in lists 
ref_raws = [ref1_raw, ref2_raw, ref3_raw, ref4_raw, ref5_raw, \
            ref6_raw, ref7_raw, ref8_raw, ref9_raw]
# print(len(ref_raws))

ref_raws = [ref for ref in ref_raws if sum(pd.isna(v) for v in ref.values()) <= 10]
ref_pcts = [ref1_pct, ref2_pct, ref3_pct]


# Match the index of reference properties from comparison tables to raw tables
matched_idx = []  # indices in ref_raws that match each ref_pct in order
used_indices = set()

for ref_pct in ref_pcts:
    pct_price = get_land_price_pct(ref_pct)
    
    # Compare against all ref_raws that haven't been used
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
    if '\n' in info:
        res = []
        purposes =  [i.split(":")[0] for i in info.split("\n")]
        p_area = [float(j.split(":")[1].rstrip("m²").replace(",", ".")) for j in info.split("\n")]
        for i in range(len(purposes)):
            res.append({
                "name": purposes[i],
                "area": p_area[i]
            })
        return res
    else:  
        return info, np.nan

# Function to get the coordinates from the info string
def get_info_location(info):
    res = {}
    if pd.isna(info) or info is None or str(info).strip() == "":
        return None
    else:
        res["x"] = float(str(info).split(",")[1])
        res["y"] = float(str(info).split(",")[0])
        res["type"] = "Point"
        res["coordinates"] = [res["x"], res["y"]]
        return res

# Function to build the assetsManagement structure
def build_assets_management(entry):
    # print(entry.get("Tọa độ vị trí", 0))
    # location_info = get_info_location(entry.get("Tọa độ vị trí", 0))
    # print(location_info)
    return {
        "geoJsonPoint": get_info_location(entry.get("Tọa độ vị trí", 0)),
        "basicAssetsInfo": {
            "basicAddressInfo": {
                "fullAddress": str(entry.get("Địa chỉ tài sản", "")),
            },
            "totalPrice": float(entry.get("Giá đất (đồng/m²)", 0)),
            "landUsePurposeInfo": get_info_purpose(str(entry.get("Mục đích sử dụng đất ", ""))),
            "valuationLandUsePurposeInfo": get_info_purpose(str(entry.get("Mục đích sử dụng đất ", ""))),
            "area": float(entry.get("Quy mô diện tích (m²)\n(Đã trừ đất thuộc quy hoạch lộ giới)", 0)),
            "width": float(entry.get("Chiều rộng (m)", 0)),
            "height": float(entry.get("Chiều dài (m)", 0)),
            "percentQuality": float(entry.get("Chất lượng còn lại (%)", 0)),
            "newConstructionUnitPrice": float(entry.get("Đơn giá xây dựng mới (đồng/m²)", 0)),
            "constructionValue": float(entry.get("Giá trị công trình xây dựng (đồng)", 0)),
            "sellingPrice": float(entry.get("Giá rao bán (đồng)", 0)),
            "negotiablePrice": float(entry.get("Giá thương lượng (đồng)", 0)),
            "landConversion": float(entry.get("Chi phí chuyển mục đích sử dụng đất/ Chênh lệch tiền chuyển mục đích sử dụng đất (đồng)", 0)),
            "landRoadBoundary": float(entry.get("Giá trị phần đất thuộc lộ giới (đồng)", 0)),
            "landValue": float(entry.get("Giá trị đất (đồng)", 0)),
            "landPrice": float(entry.get("Giá đất (đồng/m²)", 0)),
        },
        
    }

# Function to build the comparison/percentage fields structure
def build_compare_fields(entry):
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
print(f"Inserted excel data with ID: {insert_id}")
