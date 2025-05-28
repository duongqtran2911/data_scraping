import pandas as pd

# Đọc file Excel
df = pd.read_excel('DV_Can Giuoc.xlsx')


def find_hang_muc_column(df):
    """
    Tìm cột chứa 'HẠNG MỤC' trong các dòng đầu tiên (3-5)
    """
    search_rows = [1,2, 3, 4]  # Dòng 3, 4, 5 (index 2, 3, 4)

    for row_idx in search_rows:
        if row_idx < len(df):
            for col in df.columns:
                cell_value = str(df.iloc[row_idx, df.columns.get_loc(col)])
                if 'HẠNG MỤC' in cell_value.upper():
                    print(f"Tìm thấy 'HẠNG MỤC' tại dòng {row_idx + 1}, cột '{col}': {cell_value}")
                    return col, row_idx

    # Nếu không tìm thấy trong dòng 3-5, tìm trong toàn bộ DataFrame
    print("Không tìm thấy 'HẠNG MỤC' trong dòng 3-5, đang tìm trong toàn bộ file...")
    for index, row in df.iterrows():
        for col in df.columns:
            cell_value = str(row[col])
            if 'HẠNG MỤC' in cell_value.upper():
                print(f"Tìm thấy 'HẠNG MỤC' tại dòng {index + 1}, cột '{col}': {cell_value}")
                return col, index

    return None, None


def find_gia_dat_in_hang_muc(df, hang_muc_col):
    """
    Tìm 'Giá đất' trong cột HẠNG MỤC đã xác định
    """
    search_text = "Giá đất"

    matches = df[df[hang_muc_col].astype(str).str.contains(search_text, na=False, case=False)]

    if not matches.empty:
        print(f"\nTìm thấy {len(matches)} hàng chứa '{search_text}' trong cột '{hang_muc_col}':")

        results = []
        for index, row in matches.iterrows():
            print(f"\nHàng {index + 1}:")
            print(f"  {hang_muc_col}: {row[hang_muc_col]}")

            # In toàn bộ dữ liệu của hàng này
            print("  Toàn bộ hàng:")
            row_data = {}
            for col in df.columns:
                print(f"    {col}: {row[col]}")
                row_data[col] = row[col]

            results.append({
                'row_index': index,
                'hang_muc_content': row[hang_muc_col],
                'full_row': row_data
            })

        return results
    else:
        print(f"Không tìm thấy '{search_text}' trong cột '{hang_muc_col}'")
        return []


# Bước 1: Tìm cột HẠNG MỤC
print("=== BƯỚC 1: TÌM CỘT HẠNG MỤC ===")
hang_muc_col, hang_muc_row = find_hang_muc_column(df)

if hang_muc_col:
    # Bước 2: Tìm "Giá đất" trong cột HẠNG MỤC
    print(f"\n=== BƯỚC 2: TÌM 'GIÁ ĐẤT' TRONG CỘT '{hang_muc_col}' ===")
    gia_dat_results = find_gia_dat_in_hang_muc(df, hang_muc_col)
else:
    print("❌ Không tìm thấy cột HẠNG MỤC")
    print("Các cột có sẵn:", list(df.columns))
    gia_dat_results = []

# Lưu kết quả vào log
with open('output.log', 'w', encoding='utf-8') as f:
    f.write("=== TÌM KIẾM HẠNG MỤC VÀ GIÁ ĐẤT ===\n\n")

    # Ghi thông tin tìm cột HẠNG MỤC
    if hang_muc_col:
        f.write(f"✅ Tìm thấy cột HẠNG MỤC: '{hang_muc_col}' tại dòng {hang_muc_row + 1}\n\n")

        # Ghi kết quả tìm Giá đất
        if gia_dat_results:
            f.write(f"✅ Tìm thấy {len(gia_dat_results)} hàng chứa 'Giá đất':\n\n")

            for i, result in enumerate(gia_dat_results, 1):
                f.write(f"KẾT QUẢ {i}:\n")
                f.write(f"  Vị trí: Hàng {result['row_index'] + 1}\n")
                f.write(f"  Nội dung HẠNG MỤC: {result['hang_muc_content']}\n")
                f.write("  Toàn bộ hàng:\n")
                for col, value in result['full_row'].items():
                    f.write(f"    {col}: {value}\n")
                f.write("\n" + "=" * 60 + "\n\n")
        else:
            f.write("❌ Không tìm thấy 'Giá đất' trong cột HẠNG MỤC\n")
    else:
        f.write("❌ Không tìm thấy cột HẠNG MỤC\n")
        f.write("Các cột có sẵn: " + str(list(df.columns)) + "\n")

    # Thêm toàn bộ dữ liệu Excel
    f.write("\n" + "=" * 80 + "\n")
    f.write("TOÀN BỘ DỮ LIỆU EXCEL:\n")
    f.write("=" * 80 + "\n\n")
    text_raw = df.to_string(index=False)
    f.write(text_raw)

print("\n✅ Đã lưu kết quả vào file 'output.log'")

# Thêm chức năng hiển thị preview của các dòng đầu
print(f"\n=== PREVIEW CÁC DÒNG ĐẦU (1-5) ===")
for i in range(min(5, len(df))):
    print(f"Dòng {i + 1}:")
    for col in df.columns:
        print(f"  {col}: {df.iloc[i][col]}")
    print()