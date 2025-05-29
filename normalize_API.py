from difflib import SequenceMatcher
import re
from typing import Dict, List, Optional


class SmartAttributeNormalizer:
    def __init__(self):
        # Chỉ giữ lại các mapping chuẩn quan trọng
        self.standard_attributes = {

            "tọa độ vị trí": ["tọa độ","tọa độ vị trí" ,"coordinates"],

            "địa chỉ tài sản": ["địa chỉ", "address", "location"],

            "giá đất (đồng/m²)": [
                "đơn giá", "đơn giá đất", "giá đất", "unit price"
            ],

            "mục đích sử dụng đất": [
                "mục đích sử dụng", "land use purpose"
            ],

            "quy mô diện tích (m²)\n(đã trừ đất thuộc quy hoạch lộ giới)": [
                "quy mô diện tích", "diện tích", "area", "diện tích sàn"
            ],

            # Kích thước
            "chiều dài (m)": ["chiều dài", "length"],
            "chiều rộng (m)": ["chiều rộng", "width"],
            "chiều sâu (m)": ["chiều sâu", "depth"],

            "độ rộng mặt tiền (m)": [
                "chiều rộng giáp mặt đường", "chiều rộng mặt tiền",
                "chiều rộng tiếp giáp mặt tiền", "mặt tiền","chiều rộng mặt tiền tiếp giáp đường"
            ],

            "chất lượng còn lại (%)": [
                "chất lượng", "quality remaining"
            ],

            "đơn giá xây dựng mới (đồng/m²)":[
                "đơn giá xây dựng", "đơn giá xây dựng mới"
            ],

            "giá trị công trình xây dựng (đồng)":[
                "giá trị công trình xây dựng"
            ],

            "giá rao bán (đồng)": [
                "giá rao bán", "selling price", "asking price"
            ],

            "giá thương lượng (đồng)":[
                "giá thương lượng",
            ],

            "chi phí chuyển mục đích sử dụng đất/ chênh lệch tiền chuyển mục đích sử dụng đất (đồng)":[
                "chi phí chuyển mục đích sử dụng đất/ chênh lệch tiền chuyển mục đích sử dụng đất",
                "chi phí chuyển mục đích sử dụng đất"
            ],

            "giá trị phần đất thuộc lộ giới (đồng)":[
                "giá trị phần đất thuộc lộ giới",

            ],

            "giá trị đất (đồng)": [
                "giá trị đất",
                "giá trị đất đã trừ phần quy hoạch lộ giới (đồng)",
                "giá trị đất phù hợp quy hoạch",
                "giá trị đất ONT + CLN (đồng)"
            ],

            "giá thị trường (giá trước điều chỉnh) (đồng/m²)": [
                "giá thị trường"
            ],

            "yếu tố khác": ["yếu tố khác", "other factors"],
        }

        # Tạo reverse mapping để tìm kiếm nhanh
        self.reverse_mapping = {}
        for standard, variants in self.standard_attributes.items():
            for variant in variants:
                self.reverse_mapping[self._clean_text(variant)] = standard
            # Thêm chính standard key vào mapping
            self.reverse_mapping[self._clean_text(standard)] = standard

    def _clean_text(self, text: str) -> str:
        """Làm sạch text để so sánh"""
        if not isinstance(text, str):
            return str(text)

        # Chuyển về lowercase và xóa khoảng trắng thừa
        text = text.strip().lower()

        # Xóa các ký tự đặc biệt và normalize
        text = re.sub(r'\s+', ' ', text)  # Multiple spaces to single
        text = re.sub(r'[^\w\s\(\)\²\³\⁴\⁵\⁶\⁷\⁸\⁹\⁰/]', '', text)  # Keep basic chars

        return text

    def _similarity(self, a: str, b: str) -> float:
        """Tính độ tương đồng giữa 2 chuỗi"""
        return SequenceMatcher(None, a, b).ratio()

    def _fuzzy_match(self, input_attr: str, threshold: float = 0.8) -> Optional[str]:
        """Tìm attribute tương đồng nhất"""
        cleaned_input = self._clean_text(input_attr)

        best_match = None
        best_score = 0

        # Tìm trong reverse mapping trước (exact match)
        if cleaned_input in self.reverse_mapping:
            return self.reverse_mapping[cleaned_input]

        # Nếu không có exact match, tìm fuzzy match
        for variant, standard in self.reverse_mapping.items():
            score = self._similarity(cleaned_input, variant)
            if score > best_score and score >= threshold:
                best_score = score
                best_match = standard

        return best_match

    def _pattern_based_normalization(self, attr: str) -> str:
        """Chuẩn hóa dựa trên pattern với độ ưu tiên cao đến thấp"""
        cleaned = self._clean_text(attr)

        # Ưu tiên 1: Pattern cụ thể và dài (tránh nhầm lẫn)
        # Chi phí chuyển mục đích sử dụng đất (pattern dài nhất)
        if all(keyword in cleaned for keyword in ['chi phí', 'chuyển', 'mục đích sử dụng đất']):
            return "chi phí chuyển mục đích sử dụng đất/ chênh lệch tiền chuyển mục đích sử dụng đất (đồng)"

        # Giá trị công trình xây dựng
        if all(keyword in cleaned for keyword in ['giá trị', 'công trình', 'xây dựng']):
            return "giá trị công trình xây dựng (đồng)"

        # Đơn giá xây dựng mới
        if all(keyword in cleaned for keyword in ['đơn giá', 'xây dựng']):
            return "đơn giá xây dựng mới (đồng/m²)"

        # Giá trị phần đất thuộc lộ giới
        if all(keyword in cleaned for keyword in ['giá trị', 'đất', 'lộ giới']):
            return "giá trị phần đất thuộc lộ giới (đồng)"

        # Giá thương lượng
        if all(keyword in cleaned for keyword in ['giá', 'thương lượng']):
            return "giá thương lượng (đồng)"

        # Giá rao bán
        if all(keyword in cleaned for keyword in ['giá', 'rao bán']):
            return "giá rao bán (đồng)"

        # Giá thị trường
        if all(keyword in cleaned for keyword in ['giá', 'thị trường']):
            if 'đồng/m²' in cleaned or 'đồng/m2' in cleaned:
                return "giá thị trường (giá trước điều chỉnh) (đồng/m²)"

        # Ưu tiên 2: Pattern cho diện tích
        if any(keyword in cleaned for keyword in ['diện tích', 'quy mô']) and 'chi phí' not in cleaned:
            if any(keyword in cleaned for keyword in ['trừ', 'lộ giới', 'quy hoạch']):
                return "quy mô diện tích (m²)\n(đã trừ đất thuộc quy hoạch lộ giới)"
            elif 'm²' in cleaned or 'm2' in cleaned or 'area' in cleaned:
                return "quy mô diện tích (m²)\n(đá trừ đất thuộc quy hoạch lộ giới)"

        # Ưu tiên 3: Pattern cho giá đất và giá trị đất
        if any(keyword in cleaned for keyword in ['giá đất', 'đơn giá đất']) and 'chi phí' not in cleaned:
            if 'đồng/m²' in cleaned or 'đồng/m2' in cleaned:
                return "giá đất (đồng/m²)"

        if 'giá trị đất' in cleaned and 'lộ giới' not in cleaned and 'chi phí' not in cleaned:
            return "giá trị đất (đồng)"

        # Ưu tiên 4: Mục đích sử dụng đất (chỉ khi không có "chi phí")
        if 'mục đích sử dụng đất' in cleaned and 'chi phí' not in cleaned and 'chuyển' not in cleaned:
            return "mục đích sử dụng đất"

        # Ưu tiên 5: Pattern cho kích thước
        if 'chiều' in cleaned:
            if 'dài' in cleaned:
                return "chiều dài (m)"
            elif 'rộng' in cleaned:
                if any(keyword in cleaned for keyword in ['mặt tiền', 'tiếp giáp', 'giáp mặt']):
                    return "độ rộng mặt tiền (m)"
                else:
                    return "chiều rộng (m)"
            elif 'sâu' in cleaned or 'sau' in cleaned:
                return "chiều sâu (m)"

        # Ưu tiên 6: Các pattern đơn giản khác
        if 'chất lượng' in cleaned and '%' in cleaned:
            return "chất lượng còn lại (%)"

        if 'tọa độ' in cleaned:
            return "tọa độ vị trí"

        if 'địa chỉ' in cleaned and 'tài sản' not in cleaned:
            return "địa chỉ tài sản"

        if 'yếu tố khác' in cleaned:
            return "yếu tố khác"

        return attr  # Trả về original nếu không match pattern nào

    def normalize_attribute(self, attr: str, similarity_threshold: float = 0.8) -> str:
        """
        Chuẩn hóa attribute với nhiều phương pháp

        Args:
            attr: Attribute cần chuẩn hóa
            similarity_threshold: Ngưỡng độ tương đồng (0.0-1.0)

        Returns:
            Attribute đã được chuẩn hóa
        """
        if not isinstance(attr, str):
            return attr

        # Bước 1: Thử fuzzy matching
        fuzzy_result = self._fuzzy_match(attr, similarity_threshold)
        if fuzzy_result:
            return fuzzy_result

        # Bước 2: Thử pattern-based normalization
        pattern_result = self._pattern_based_normalization(attr)
        if pattern_result != attr:  # Nếu có thay đổi
            return pattern_result

        # Bước 3: Trả về bản làm sạch cơ bản
        return self._clean_text(attr)

    def add_custom_mapping(self, standard_attr: str, variants: List[str]):
        """Thêm mapping tùy chỉnh"""
        if standard_attr not in self.standard_attributes:
            self.standard_attributes[standard_attr] = []

        self.standard_attributes[standard_attr].extend(variants)

        # Update reverse mapping
        for variant in variants:
            self.reverse_mapping[self._clean_text(variant)] = standard_attr

    def get_similarity_report(self, attr: str, top_n: int = 5) -> List[tuple]:
        """Trả về top N attributes tương đồng nhất để debug"""
        cleaned_input = self._clean_text(attr)
        similarities = []

        for variant, standard in self.reverse_mapping.items():
            score = self._similarity(cleaned_input, variant)
            similarities.append((standard, variant, score))

        return sorted(similarities, key=lambda x: x[2], reverse=True)[:top_n]


# Hàm helper để sử dụng dễ dàng
def normalize_att_2(attr: str, threshold: float = 0.7) -> str:
    """
    Hàm chuẩn hóa attribute thông minh

    Args:
        attr: Attribute cần chuẩn hóa
        threshold: Ngưỡng độ tương đồng (default 0.8)

    Returns:
        Attribute đã chuẩn hóa
    """
    if not hasattr(normalize_att_2, '_normalizer'):
        normalize_att_2._normalizer = SmartAttributeNormalizer()

    return normalize_att_2._normalizer.normalize_attribute(attr, threshold)


# Example usage và test
# Example usage và test
if __name__ == "__main__":
    normalizer = SmartAttributeNormalizer()

    print("=== Smart Attribute Normalizer ===")
    print("Nhập 'exit' hoặc 'quit' để thoát")
    print("-" * 50)

    while True:
        try:
            test_string = input("\nNhập mẫu test: ").strip()

            if not test_string:
                continue

            if test_string.lower() in ['exit', 'quit', 'q']:
                print("Goodbye!")
                break

            # Test cases
            test_cases = [test_string]
            print("\n=== Test case ===")
            print(test_cases)
            print("\n=== Test Results ===")
            for test in test_cases:
                result = normalize_att_2(test)
                print(f"'{test}' -> '{result}'")

            print("\n=== Similarity Report ===")
            report = normalizer.get_similarity_report(test_string)
            for standard, variant, score in report:
                print(f"{score:.3f}: {standard} (matched: {variant})")

            print("-" * 50)

        except KeyboardInterrupt:
            print("\n\nThoát bằng Ctrl+C. Goodbye!")
            break
        except Exception as e:
            print(f"❌ Lỗi: {e}")