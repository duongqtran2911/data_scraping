import os
import pandas as pd

# Set your root network folder

year = 2025
month = "05"
root_dir = rf"\\192.168.1.250\department\03. APPRAISAL\03. REAL ESTATE\03. PROJECT\09. IMM\03. BAO GIA\01. IMM_VV\{year}\THANG {month}"

# Output files (saved locally)
comparison_file_log = rf"D:\Project\data_scraping\comparison_files_{month}_{year}.txt"
irrelevant_file_log = rf"D:\Project\data_scraping\irrelevant_files_{month}_{year}.txt"

# Keywords that strongly indicate a comparison section
comparison_keywords = [
    "Tình trạng pháp lý",
    "Vị trí",
    "Giao thông",
    "Quy mô diện tích",
    "Chiều rộng",
    "Chiều dài",
    "Dân cư",
    "Hình dáng",
    "Yếu tố khác"
]

# Excel file extensions to consider
excel_extensions = ('.xls', '.xlsx', '.xlsm')

# Prepare lists
comparison_files = []
irrelevant_files = []

def contains_comparison_section(file_path):
    try:
        xl = pd.ExcelFile(file_path)
        for sheet in xl.sheet_names:
            try:
                df = xl.parse(sheet, header=None)
                text_values = df.astype(str).values.flatten()
                matches = [kw for kw in comparison_keywords if any(kw in cell for cell in text_values)]
                if len(matches) >= 3:  # heuristic: if at least 3 keywords are found
                    return True
            except Exception:
                continue
    except Exception as e:
        print(f"Error reading {file_path} → {e}")
        pass
    return False

print(f"Checking folder: {root_dir}")

for dirpath, _, filenames in os.walk(root_dir):
    for filename in filenames:
        if filename.lower().endswith(excel_extensions):
            file_path = os.path.join(dirpath, filename)
            print(f"🔍 Found: {file_path}")
            try:
                if contains_comparison_section(file_path):
                    print("✅ Match found")
                    comparison_files.append(file_path)
                else:
                    print("❌ No match")
                    irrelevant_files.append(file_path)
            except Exception as e:
                print(f"🚨 Error processing file: {e}")

# Write results to local logs
with open(comparison_file_log, "w", encoding="utf-8") as f:
    for path in comparison_files:
        f.write(path + "\n")

with open(irrelevant_file_log, "w", encoding="utf-8") as f:
    for path in irrelevant_files:
        f.write(path + "\n")

print(f"✅ Done! Found {len(comparison_files)} comparison files and {len(irrelevant_files)} irrelevant files.")
