import os
import pandas as pd

# 🔧 CONFIGURATION
year = 2025
month = "04"
month = month.zfill(2)

root_dir = rf"\\192.168.1.250\department\03. APPRAISAL\03. REAL ESTATE\03. PROJECT\09. IMM\03. BAO GIA\01. IMM_VV\{year}\THANG {month}"

# 📁 Log paths
base_path = r"D:\Project\data_scraping"
comparison_log = os.path.join(base_path, f"comparison_files_{month}_{year}.txt")
irrelevant_log = os.path.join(base_path, f"irrelevant_files_{month}_{year}.txt")
unclassified_log = os.path.join(base_path, f"unclassified_files_{month}_{year}.txt")
full_log_file = os.path.join(base_path, f"all_checked_files_{month}_{year}.txt")

# 🔍 Keywords to search
comparison_keywords = [
    "Tình trạng pháp lý", "Vị trí", "Giao thông",
    "Quy mô diện tích", "Chiều rộng", "Chiều dài",
    "Dân cư", "Hình dáng", "Yếu tố khác"
]

excel_extensions = ('.xls', '.xlsx', '.xlsm')

# 🗃️ Containers
comparison_matches = {}
irrelevant_files = []
unclassified_files = []
all_files_checked = 0

# 🧠 Check if sheet contains comparison data
def sheet_has_comparison_section(df):
    try:
        values = df.astype(str).values.flatten()
        matches = [kw for kw in comparison_keywords if any(kw in cell for cell in values)]
        return len(matches) >= 3
    except Exception:
        return False

# 🚀 Start scanning
for dirpath, _, filenames in os.walk(root_dir):
    for filename in filenames:
        if filename.lower().endswith(excel_extensions):
            file_path = os.path.join(dirpath, filename)
            all_files_checked += 1
            try:
                engine = "xlrd" if file_path.lower().endswith(".xls") else None
                xls = pd.ExcelFile(file_path, engine=engine)
                matched_sheets = []

                for sheet in xls.sheet_names:
                    try:
                        df = xls.parse(sheet, header=None)
                        if sheet_has_comparison_section(df):
                            matched_sheets.append(sheet)
                    except Exception:
                        continue

                if matched_sheets:
                    comparison_matches[file_path] = matched_sheets
                else:
                    irrelevant_files.append(file_path)
            except Exception as e:
                unclassified_files.append(file_path)

# 💾 Save logs
def save_list(path, data, label):
    try:
        with open(path, "w", encoding="utf-8") as f:
            if isinstance(data, dict):
                for file, sheets in data.items():
                    f.write(f"{file} >>> {'&&'.join(sheets)}")
            else:
                for file in data:
                    f.write(file + "\n")
        print(f"✅ {label} saved to {path} ({len(data)} items)")
    except Exception as e:
        print(f"❌ Failed to save {label}: {e}")

save_list(comparison_log, comparison_matches, "Comparison files with sheets")
save_list(irrelevant_log, irrelevant_files, "Irrelevant files")
save_list(unclassified_log, unclassified_files, "Unclassified files")

# 📘 Save full log
try:
    with open(full_log_file, "w", encoding="utf-8") as f:
        for path, sheets in comparison_matches.items():
            f.write(f"{path},comparison,{','.join(sheets)}\n")
        for path in irrelevant_files:
            f.write(f"{path},irrelevant,\n")
        for path in unclassified_files:
            f.write(f"{path},error,\n")
    print(f"📝 Full log saved to {full_log_file}")
except Exception as e:
    print(f"❌ Failed to write full log: {e}")

# 📊 Final summary
print(f"\n📊 Summary:")
print(f"   Total Excel files checked: {all_files_checked}")
print(f"   ✅ Files with comparison sections: {len(comparison_matches)}")
print(f"   ❌ Irrelevant files: {len(irrelevant_files)}")
print(f"   ⚠️ Files with errors: {len(unclassified_files)}")
