import os

total = 0
year = 2025

for i in ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]:
# for i in ["01", "02", "03", "04", "05", "06"]:
# for i in ["01"]:
    try:
        with open(rf"D:\Project\data_scraping\comparison_files_{i}_{year}.txt", "r", encoding="utf-8") as f1:
            lines1 = f1.readlines()
            total += len(lines1)
    except FileNotFoundError:
        pass

    try:
        with open(rf"D:\Project\data_scraping\irrelevant_files_{i}_{year}.txt", "r", encoding="utf-8") as f2:
            lines2 = f2.readlines()
            total += len(lines2)
    except FileNotFoundError:
        pass

    try:
        with open(rf"D:\Project\data_scraping\unclassified_files_{i}_{year}.txt", "r", encoding="utf-8") as f3:
            lines3 = f3.readlines()
            total += len(lines3)
    except FileNotFoundError:
        pass


print(total)


# with open("D:\Project\data_scraping\comparison_files_01_2025.txt", "r", encoding="utf-8") as f:
#     lines = f.readlines()
#     print(f"Number of lines: {len(lines)}")
