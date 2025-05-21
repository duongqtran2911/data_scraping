import pandas as pd
from seleniumwire import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import json
import io
import gzip
import re


# Hàm chuyển đổi tọa độ DMS sang decimal
def convert_dms_to_decimal(dms_str):
    """Chuyển đổi tọa độ từ định dạng DMS sang decimal"""
    direction = dms_str[-1]
    dms_str = dms_str[:-1]

    parts = re.split('[°\'"]', dms_str.strip())
    degrees = float(parts[0])
    minutes = float(parts[1]) if len(parts) > 1 and parts[1] else 0
    seconds = float(parts[2]) if len(parts) > 2 and parts[2] else 0

    decimal = degrees + (minutes / 60) + (seconds / 3600)

    if direction in ['S', 'W']:
        decimal = -decimal

    return decimal


# Cấu hình driver Selenium
def setup_driver(headless=True):
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless")
    driver = webdriver.Chrome(options=options)
    driver.maximize_window()
    return driver


# Mở trang Guland
def open_guland_page(driver):
    driver.get("https://guland.vn/ban-do-gia")
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "to-thua-search")))
    time.sleep(1)


# Điền form tìm kiếm
def fill_form(driver, so_thua, so_to, tinh, huyen, xa):
    wait = WebDriverWait(driver, 10)

    # 0. Nhấn mở form "Tờ thửa"
    try:
        to_thua_button = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="to-thua-search"]/a')))
        to_thua_button.click()
        time.sleep(1)
    except Exception as e:
        print(f"❌ Không thể mở lại form tờ thửa: {e}")
        return False

    # 1. Xóa và điền số thửa, số tờ
    input_thua = driver.find_element(By.XPATH, '//*[@id="form-to-thua"]/div[1]/div/div/div/div[2]/div/div/input[1]')
    input_to = driver.find_element(By.XPATH, '//*[@id="form-to-thua"]/div[1]/div/div/div/div[2]/div/div/input[2]')
    input_thua.clear()
    input_to.clear()
    input_thua.send_keys(so_thua)
    input_to.send_keys(so_to)

    # 2. Chọn Tỉnh
    driver.find_element(By.ID, "select2-province_id_4-container").click()
    wait.until(EC.visibility_of_element_located((By.CLASS_NAME, "select2-search__field")))
    search = driver.find_element(By.CLASS_NAME, "select2-search__field")
    search.send_keys(Keys.CONTROL + "a")
    search.send_keys(Keys.BACKSPACE)
    search.send_keys(tinh)
    search.send_keys(Keys.ENTER)

    # 3. Chọn Huyện
    time.sleep(0.5)
    driver.find_element(By.ID, "select2-district_id_4-container").click()
    wait.until(EC.visibility_of_element_located((By.CLASS_NAME, "select2-search__field")))
    search = driver.find_element(By.CLASS_NAME, "select2-search__field")
    search.send_keys(Keys.CONTROL + "a")
    search.send_keys(Keys.BACKSPACE)
    search.send_keys(huyen)
    search.send_keys(Keys.ENTER)

    # 4. Chọn Xã
    time.sleep(0.5)
    driver.find_element(By.ID, "select2-ward_id_4-container").click()
    wait.until(EC.visibility_of_element_located((By.CLASS_NAME, "select2-search__field")))
    search = driver.find_element(By.CLASS_NAME, "select2-search__field")
    search.send_keys(Keys.CONTROL + "a")
    search.send_keys(Keys.BACKSPACE)
    search.send_keys(xa)
    search.send_keys(Keys.ENTER)

    # 5. Nhấn tìm kiếm
    time.sleep(1)
    search_button = driver.find_element(By.XPATH, '//*[@id="TabContent-SqhSearch-3"]/div/div[2]/button')
    search_button.click()
    time.sleep(3)

    return True


# Trích xuất tọa độ từ requests
def extract_coordinates_from_requests(driver):
    correct_url = "https://guland.vn/post/check-plan?screen=ban-do-gia"
    lat, lng = None, None
    polygon_points = []

    # Lấy các requests gần đây nhất
    for request in driver.requests:
        if request.method == "POST" and correct_url in request.url and request.response:
            try:
                raw = request.response.body
                try:
                    decompressed = gzip.GzipFile(fileobj=io.BytesIO(raw)).read().decode("utf-8")
                except OSError:
                    decompressed = raw.decode("utf-8")

                response_data = json.loads(decompressed)
                lat = response_data["data"]["lat"]
                lng = response_data["data"]["lng"]

                print(f"✅ Tìm thấy tọa độ: lat = {lat}, lng = {lng}")

                if "points" in response_data["data"]:
                    print("🧭 Đường biên đa giác:")
                    polygon_points = response_data["data"]["points"]
                    for pt in polygon_points:
                        print(f"  {pt}")

            except Exception as e:
                print("❌ Lỗi khi phân tích JSON:", e)
            break

    if lat is None or lng is None:
        print("❌ Không tìm thấy tọa độ.")

    return lat, lng, polygon_points


# Phân tích địa chỉ thành các thành phần: tỉnh, huyện, xã
def parse_location_info(location):
    """Phân tích địa chỉ thành các thành phần cho Guland"""
    # Mặc định khi không phân tích được đầy đủ địa chỉ
    so_thua = "2"
    so_to = "14"
    tinh = ""
    huyen = ""
    xa = ""

    if location and isinstance(location, str) and location.strip():
        parts = location.split(',')
        parts = [p.strip() for p in parts if p.strip()]

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

        # Nếu không tìm thấy đủ thông tin từ các từ khóa, dùng cách truyền thống
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


# Hàm chuyển đổi địa chỉ thành tọa độ
def convert_address_to_coordinates(location):
    """Chuyển đổi địa chỉ thành tọa độ sử dụng Selenium và Guland"""
    print("🔍 Đang chuyển đổi địa chỉ thành tọa độ...")

    if not location or not isinstance(location, str) or not location.strip():
        print("❌ Địa chỉ trống hoặc không hợp lệ")
        return None

    # Phân tích thông tin địa chỉ
    location_info = parse_location_info(location)

    # Kiểm tra xem có đủ thông tin để tìm kiếm không
    if not location_info["tinh"] or not location_info["huyen"] or not location_info["xa"]:
        print(f"❌ Không đủ thông tin để tìm kiếm: {location_info}")
        return None

    print(
        f"📍 Thông tin đã phân tích: Tỉnh={location_info['tinh']}, Huyện={location_info['huyen']}, Xã={location_info['xa']}")

    driver = None
    try:
        driver = setup_driver(headless=True)
        open_guland_page(driver)
        print("✅ Đã mở trang Guland")

        # Xóa requests cũ
        del driver.requests

        # Điền form và tìm kiếm
        if fill_form(driver,
                     location_info["so_thua"],
                     location_info["so_to"],
                     location_info["tinh"],
                     location_info["huyen"],
                     location_info["xa"]):

            # Trích xuất tọa độ
            lat, lng, points = extract_coordinates_from_requests(driver)

            if lat is not None and lng is not None:
                print(f"✅ Đã lấy được tọa độ: {lng}, {lat}")
                return {
                    "type": "Point",
                    "coordinates": [float(lng), float(lat)]  # MongoDB expects [longitude, latitude]
                }
            else:
                print("❌ Không lấy được tọa độ từ kết quả tìm kiếm")
        else:
            print("❌ Không thể điền form tìm kiếm")

    except Exception as e:
        print(f"❌ Lỗi Selenium: {e}")
        import traceback
        traceback.print_exc()

    finally:
        if driver:
            driver.quit()

    return None


# # Hàm chính để lấy thông tin vị trí (cải tiến)
# def get_info_location(info, location):
#     """
#     Hàm lấy thông tin vị trí từ tọa độ hoặc địa chỉ
#
#     Args:
#         info: Thông tin tọa độ (chuỗi)
#         location: Địa chỉ (phòng hờ khi không có tọa độ)
#
#     Returns:
#         Đối tượng GeoJSON Point hoặc None
#     """
#     # Kiểm tra tọa độ đầu vào trước
#     if pd.notna(info) and info is not None and str(info).strip() != "":
#         info_str = str(info).strip()
#
#         # Trường hợp 1: định dạng lat, lon tiêu chuẩn (VD: 10.97, 108.22)
#         if "," in info_str and all(char not in info_str for char in "°'\""):
#             try:
#                 lat, lon = info_str.split(",")
#                 return {
#                     "type": "Point",
#                     "coordinates": [float(lon.strip()), float(lat.strip())]  # MongoDB cần [longitude, latitude]
#                 }
#             except Exception as e:
#                 print(f"❌ Lỗi khi phân tích tọa độ tiêu chuẩn: {e}")
#
#         # Trường hợp 2: định dạng DMS (VD: 10°58'10.4"N 108°13'46.8"E)
#         if "°" in info_str:
#             try:
#                 dms_parts = info_str.split()
#                 if len(dms_parts) == 2:
#                     lat_decimal = convert_dms_to_decimal(dms_parts[0])
#                     lon_decimal = convert_dms_to_decimal(dms_parts[1])
#                     return {
#                         "type": "Point",
#                         "coordinates": [lon_decimal, lat_decimal]
#                     }
#             except Exception as e:
#                 print(f"❌ Lỗi khi phân tích tọa độ DMS: {e}")
#
#     # Trường hợp 3: Nếu không có tọa độ trực tiếp, thử dùng địa chỉ
#     if location and str(location).strip() != "":
#         print(f"ℹ️ Không có tọa độ trong dữ liệu. Đang sử dụng địa chỉ: {location}")
#         return convert_address_to_coordinates(location)
#
#     # Nếu tất cả các phương pháp thất bại
#     return None
#
#
# # Để kiểm tra hàm, có thể thêm đoạn code sau
# if __name__ == "__main__":
#     # Test với địa chỉ cụ thể
#     test_location = "Xã Long Hậu, Huyện Cần Giuộc, Tỉnh Long An"
#     result = get_info_location(None, test_location)
#     print(f"Kết quả: {result}")