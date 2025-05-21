# Phân tích địa chỉ thành các thành phần: tỉnh, huyện, xã
import re

def parse_location_info(location):
    """Phân tích địa chỉ thành các thành phần cho Guland"""
    so_thua = ""
    so_to = ""
    tinh = ""
    huyen = ""
    xa = ""

    if location and isinstance(location, str) and location.strip():
        parts = location.split(',')
        parts = [p.strip() for p in parts if p.strip()]

        # Tìm số thửa (ví dụ: "Thửa đất số 2")
        thuad_match = re.search(r"[Tt]hửa đất số\s*(\d+)", location)
        if thuad_match:
            so_thua = thuad_match.group(1)

        # Tìm số tờ (ví dụ: "Tờ bản đồ số 14")
        tobd_match = re.search(r"[Tt]ờ bản đồ số\s*(\d+)", location)
        if tobd_match:
            so_to = tobd_match.group(1)

        # Tìm phần tỉnh/thành phố
        for i, part in enumerate(parts):
            part_lower = part.lower()
            if "tỉnh" in part_lower or "thành phố" in part_lower or "tp" in part_lower:
                tinh = part_lower.replace("tỉnh", "").replace("thành phố", "").replace("tp", "").strip()

                # Tìm phần huyện/quận
                for j, part2 in enumerate(parts):
                    part2_lower = part2.lower()
                    if "huyện" in part2_lower or "quận" in part2_lower or "thị xã" in part2_lower:
                        huyen = part2_lower.strip()

                        # Tìm phần xã/phường
                        for k, part3 in enumerate(parts):
                            part3_lower = part3.lower()
                            if "xã" in part3_lower or "phường" in part3_lower or "thị trấn" in part3_lower:
                                xa = part3_lower.strip()
                                break
                        break
                break

        # Dự phòng nếu không đủ thông tin từ từ khóa
        if not tinh and len(parts) >= 1:
            tinh = parts[-1].lower().strip()
        if not huyen and len(parts) >= 2:
            huyen = parts[-2].lower().strip()
        if not xa and len(parts) >= 3:
            xa = parts[-3].lower().strip()

    return {
        "so_thua": so_thua,
        "so_to": so_to,
        "tinh": tinh,
        "huyen": huyen,
        "xa": xa
    }

test = 'Thửa đất số 2 (pcl); 3; 193; 194, tờ bản đồ số 14, ấp 2/5, xã Long Hậu, huyện Cần Giuộc, tỉnh Long An'


parse_location_info(test)

print(parse_location_info(test))