# Phân tích địa chỉ thành các thành phần: tỉnh, huyện, xã
import re

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
        parts_lower = [p.lower() for p in parts]

        # Số thửa: "Thửa đất số 104", "Thửa số 104", "TĐS 723", "TĐ số 723"
        thuad_match = re.search(r"(?:[Tt]hửa (?:đất )?số\s*|TĐS\s*|TĐ số\s*)(\d+)", location)
        if thuad_match:
            so_thua = thuad_match.group(1)

        # Số tờ: "Tờ bản đồ 06", "Tờ bản đồ số 06", "TBĐ số 06", "TBĐS 06"
        tobd_match = re.search(r"(?:[Tt]ờ bản đồ(?: số)?\s*|TBĐS\s*|TBĐ số\s*)(\d+)", location)
        if tobd_match:
            so_to = tobd_match.group(1)

        # Tìm tỉnh/thành phố từ cuối danh sách parts
        for part in reversed(parts):
            part_lower = part.lower().strip()
            if any(kw in part_lower for kw in ["tỉnh", "thành phố", "tp"]):
                if not re.match(r"(đường|quốc lộ|ql|tl|tỉnh lộ)\s*\d+", part_lower):
                    tinh = re.sub(r"\b(tỉnh|thành phố|tp\.?)\b", "", part_lower).strip()
                    break

        # Nếu vẫn chưa có tỉnh, lấy phần cuối nếu không phải là đường
        if not tinh and parts:
            last = parts[-1].lower()
            if not re.match(r"(đường|quốc lộ|ql|tl|tỉnh lộ)\s*\d+", last):
                tinh = last

        # Huyện/thị xã/quận
        for part in parts:
            part_lower = part.lower()
            if any(kw in part_lower for kw in ["huyện", "quận", "thị xã", "tx "]):
                huyen = part_lower.replace("tx", "thị xã").strip()
                break

        # Xã/phường/thị trấn - ưu tiên regex để tránh dính "tỉnh lộ"
        xa_match = re.search(r"\b(phường|xã|thị trấn)\s+[a-zA-ZÀ-Ỹà-ỹ0-9\s\-]+", location, re.IGNORECASE)
        if xa_match:
            xa = xa_match.group(0).strip().lower()

        # Dự phòng nếu vẫn thiếu
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


test = "Thửa đất số 53, tờ bản đồ số 10 - Số 122 đường Ba Cu, Phường 3, TP. Vũng Tàu"


parse_location_info(test)

print(parse_location_info(test))