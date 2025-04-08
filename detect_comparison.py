import os
import pandas as pd
import argparse
import os
import pandas as pd

# 🎯 Get year and month from command-line arguments, with defaults
parser = argparse.ArgumentParser(description="Detect comparison sections in Excel files.")
parser.add_argument("--y", type=int, default=2025, help="Year to search (default: 2025)")
parser.add_argument("--m", type=str, default="01", help="Month to search (default: '01')")

args = parser.parse_args()

# Ensure two-digit month formatting
year = args.y
month = args.m.zfill(2)

root_dir = rf"\\192.168.1.250\department\03. APPRAISAL\03. REAL ESTATE\03. PROJECT\09. IMM\03. BAO GIA\01. IMM_VV\{year}\THANG {month}"

# 📄 Output file paths
base_path = r"D:\Project\data_scraping"
comparison_file_log = os.path.join(base_path, f"comparison_files_{month}_{year}.txt")
irrelevant_file_log = os.path.join(base_path, f"irrelevant_files_{month}_{year}.txt")
unclassified_file_log = os.path.join(base_path, f"unclassified_files_{month}_{year}.txt")
full_log_file = os.path.join(base_path, f"all_checked_files_{month}_{year}.txt")

# 🔍 Keywords to detect comparison section
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

# 🧾 Excel file extensions
excel_extensions = ('.xls', '.xlsx', '.xlsm')

# 📊 Initialize
comparison_files = []
irrelevant_files = []
unclassified_files = []
all_files_checked = 0

# 🔍 Heuristic comparison checker
def contains_comparison_section(file_path):
    try:
        engine = "xlrd" if file_path.lower().endswith(".xls") else None
        xl = pd.ExcelFile(file_path, engine=engine)
        for sheet in xl.sheet_names:
            try:
                df = xl.parse(sheet, header=None)
                text_values = df.astype(str).values.flatten()
                matches = [kw for kw in comparison_keywords if any(kw in cell for cell in text_values)]
                if len(matches) >= 3:
                    return True
            except Exception:
                continue
    except Exception as e:
        print(f"⚠️ Error reading {file_path} → {e}")
        raise e
    return False

# 🚀 Start checking
print(f"📂 Scanning folder: {root_dir}")

for dirpath, _, filenames in os.walk(root_dir):
    for filename in filenames:
        if filename.lower().endswith(excel_extensions):
            file_path = os.path.join(dirpath, filename)
            all_files_checked += 1
            print(f"🔍 Checking: {file_path}")
            try:
                if contains_comparison_section(file_path):
                    print("✅ Match found")
                    comparison_files.append(file_path)
                else:
                    print("❌ No match")
                    irrelevant_files.append(file_path)
            except Exception as e:
                print(f"🚨 Skipped due to error: {e}")
                unclassified_files.append(file_path)

# 💾 Save results
def save_list(filepath, items, label):
    try:
        with open(filepath, "w", encoding="utf-8") as f:
            for path in items:
                f.write(path + "\n")
        print(f"✅ {label} saved to {filepath} ({len(items)} items)")
    except Exception as e:
        print(f"❌ Failed to save {label}: {e}")

save_list(comparison_file_log, comparison_files, "Comparison files")
save_list(irrelevant_file_log, irrelevant_files, "Irrelevant files")
save_list(unclassified_file_log, unclassified_files, "Unclassified files")

# 📘 Save combined log
try:
    with open(full_log_file, "w", encoding="utf-8") as f:
        for path in comparison_files:
            f.write(f"{path},comparison\n")
        for path in irrelevant_files:
            f.write(f"{path},irrelevant\n")
        for path in unclassified_files:
            f.write(f"{path},error\n")
    print(f"📝 Full log saved to {full_log_file}")
except Exception as e:
    print(f"❌ Failed to write full log: {e}")

# 📊 Summary
print(f"\n📊 Summary:")
print(f"   Total Excel files found: {all_files_checked}")
print(f"   ✅ Comparison files: {len(comparison_files)}")
print(f"   ❌ Irrelevant files: {len(irrelevant_files)}")
print(f"   ⚠️ Unclassified (errors): {len(unclassified_files)}")
