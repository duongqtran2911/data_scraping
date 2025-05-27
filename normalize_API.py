from difflib import SequenceMatcher
import re
from typing import Dict, List, Optional


class SmartAttributeNormalizer:
    def __init__(self):
        # Chỉ giữ lại các mapping chuẩn quan trọng
        self.standard_attributes = {

            # Địa chỉ và vị trí
            "địa chỉ tài sản": ["địa chỉ", "address", "location"],
            "tọa độ vị trí": ["tọa độ", "coordinates", "vị trí"],

            # Diện tích
            "quy mô diện tích (m²)\n(đã trừ đất thuộc quy hoạch lộ giới)": [
                "quy mô diện tích", "diện tích", "area", "diện tích sàn"
            ],

            # Kích thước
            "chiều dài (m)": ["chiều dài", "length"],
            "chiều rộng (m)": ["chiều rộng", "width"],
            "chiều sâu (m)": ["chiều sâu", "depth"],
            "độ rộng mặt tiền (m)": [
                "chiều rộng giáp mặt đường", "chiều rộng mặt tiền",
                "chiều rộng tiếp giáp mặt tiền", "mặt tiền"
            ],

            # Giá cả
            "giá đất (đồng/m²)": [
                "đơn giá", "đơn giá đất", "giá đất", "unit price"
            ],
            "giá trị đất (đồng)": [
                "giá trị đất", "land value", "total land value"
            ],
            "giá rao bán (đồng)": ["giá rao bán", "selling price", "asking price"],
            "giá thị trường (giá trước điều chỉnh) (đồng/m²)": [
                "giá thị trường", "market price"
            ],

            # Khác
            "mục đích sử dụng đất": ["mục đích sử dụng", "land use purpose"],
            "chất lượng còn lại (%)": ["chất lượng", "quality remaining"],
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
        """Chuẩn hóa dựa trên pattern"""
        cleaned = self._clean_text(attr)

        # Pattern cho diện tích
        if any(keyword in cleaned for keyword in ['diện tích', 'quy mô', 'area']):
            if any(keyword in cleaned for keyword in ['trừ', 'lộ giới', 'quy hoạch']):
                return "quy mô diện tích (m²)\n(đã trừ đất thuộc quy hoạch lộ giới)"
            elif 'm²' in cleaned or 'm2' in cleaned:
                return "quy mô diện tích (m²)\n(đã trừ đất thuộc quy hoạch lộ giới)"

        # Pattern cho giá đất
        if any(keyword in cleaned for keyword in ['giá đất', 'đơn giá']):
            if 'đồng/m²' in cleaned or 'đồng/m2' in cleaned:
                return "giá đất (đồng/m²)"
            elif 'đồng' in cleaned and ('m²' not in cleaned and 'm2' not in cleaned):
                return "giá trị đất (đồng)"

        # Pattern cho kích thước
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
def normalize_att(attr: str, threshold: float = 0.7) -> str:
    """
    Hàm chuẩn hóa attribute thông minh

    Args:
        attr: Attribute cần chuẩn hóa
        threshold: Ngưỡng độ tương đồng (default 0.8)

    Returns:
        Attribute đã chuẩn hóa
    """
    if not hasattr(normalize_att, '_normalizer'):
        normalize_att._normalizer = SmartAttributeNormalizer()

    return normalize_att._normalizer.normalize_attribute(attr, threshold)


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

            print("\n=== Test Results ===")
            for test in test_cases:
                result = normalize_att(test)
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