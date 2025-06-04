def process_file(filepath):
    seen = set()
    unique_lines = []

    with open(filepath, 'r', encoding='utf-8') as file:
        for line in file:
            line = line.strip()
            if not line:
                continue
            if line not in seen:
                seen.add(line)
                unique_lines.append(line)

    # In ra kết quả
    for line in unique_lines:
        print(line)

def process_file_no_sheet(filepath):
    seen = set()
    unique_lines = []

    with open(filepath, 'r', encoding='utf-8') as file:
        for line in file:
            line = line.strip()
            if not line:
                continue
            if line not in seen:
                seen.add(line)
                unique_lines.append(line)

    # In ra kết quả
    for line in unique_lines:
        print(line)

def main():
    filepath = 'missing_area.txt'  # Đường dẫn đến file của bạn
    process_file(filepath)
    # process_file_no_sheet(filepath)


if __name__ == '__main__':
    main()
