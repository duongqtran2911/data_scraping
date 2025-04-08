import os

total = 0

# for i in ["01", "02", "03", "04", "05", "06"]:
for i in ["01"]:
    try:
        with open(f"D:\Project\data_scraping\comparison_files_{i}_2025.txt", "r", encoding="utf-8") as f1:
            lines1 = f1.readlines()
            total += len(lines1)
    except FileNotFoundError:
        pass

# for i in ["01", "02", "03", "04", "05", "06"]:
    try:
        with open(f"D:\Project\data_scraping\irrelevant_files_{i}_2025.txt", "r", encoding="utf-8") as f2:
            lines2 = f2.readlines()
            total += len(lines2)
    except FileNotFoundError:
        pass


print(total)


# with open("D:\Project\data_scraping\comparison_files_01_2025.txt", "r", encoding="utf-8") as f:
#     lines = f.readlines()
#     print(f"Number of lines: {len(lines)}")
