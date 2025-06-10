import logging
import os
import re

from selenium.common import NoSuchElementException, ElementClickInterceptedException, ElementNotInteractableException
from seleniumwire import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import json
import io
import gzip


log_dir = "logs_status_coordinate_2025"
os.makedirs(log_dir, exist_ok=True)

# Đường dẫn đầy đủ đến file log
log_path = os.path.join(log_dir, "status_coordinate-2-2025.log")

# Tạo logger riêng cho ứng dụng
app_logger = logging.getLogger("app_logger1")
app_logger.setLevel(logging.INFO)

# Đảm bảo logger này không bị ảnh hưởng bởi các logger khác
app_logger.propagate = False

address_raw = None

# Thêm handler nếu chưa có
if not app_logger.handlers:
    file_handler = logging.FileHandler(log_path, mode="a", encoding="utf-8")
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    app_logger.addHandler(file_handler)

def setup_driver(headless=True):
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless")
    driver = webdriver.Chrome(options=options)
    driver.maximize_window()
    return driver


def open_guland_page(driver):
    driver.get("https://guland.vn/ban-do-gia")
    WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.ID, "to-thua-search")))
    time.sleep(1)  # giữ lại chút thời gian để trang load ổn định


def fill_form(driver, so_thua, so_to, tinh, huyen, xa):
    # mở lại form tờ thửa
    wait = WebDriverWait(driver, 5)

    # 0. Nhấn mở lại form "Tờ thửa"
    try:
        to_thua_button = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="to-thua-search"]/a')))
        to_thua_button.click()
        time.sleep(1)
    except Exception as e:
        print(f"❌ Không thể mở lại form tờ thửa: {e}")
        return False

    # 1. Xóa số thửa và số tờ cũ (nếu có) rồi nhập mới
    input_thua = driver.find_element(By.XPATH, '//*[@id="form-to-thua"]/div[1]/div/div/div/div[2]/div/div/input[1]')
    input_to = driver.find_element(By.XPATH, '//*[@id="form-to-thua"]/div[1]/div/div/div/div[2]/div/div/input[2]')
    input_thua.clear()
    input_to.clear()
    input_thua.send_keys(so_thua)
    input_to.send_keys(so_to)

    # 2. Reset và chọn lại Tỉnh
    driver.find_element(By.ID, "select2-province_id_4-container").click()
    wait.until(EC.visibility_of_element_located((By.CLASS_NAME, "select2-search__field")))
    search = driver.find_element(By.CLASS_NAME, "select2-search__field")
    search.send_keys(Keys.CONTROL + "a")
    search.send_keys(Keys.BACKSPACE)
    search.send_keys(tinh)
    search.send_keys(Keys.ENTER)

    # 3. Reset và chọn lại Huyện
    time.sleep(2)
    driver.find_element(By.ID, "select2-district_id_4-container").click()
    wait.until(EC.visibility_of_element_located((By.CLASS_NAME, "select2-search__field")))
    search = driver.find_element(By.CLASS_NAME, "select2-search__field")
    search.send_keys(Keys.CONTROL + "a")
    search.send_keys(Keys.BACKSPACE)
    search.send_keys(huyen)
    search.send_keys(Keys.ENTER)

    # 4. Reset và chọn lại Xã
    time.sleep(2)
    driver.find_element(By.ID, "select2-ward_id_4-container").click()
    wait.until(EC.visibility_of_element_located((By.CLASS_NAME, "select2-search__field")))
    search = driver.find_element(By.CLASS_NAME, "select2-search__field")
    search.send_keys(Keys.CONTROL + "a")
    search.send_keys(Keys.BACKSPACE)
    search.send_keys(xa)
    search.send_keys(Keys.ENTER)

    # 5. Nhấn tìm kiếm
    time.sleep(1.5)
    search_button = driver.find_element(By.XPATH, '//*[@id="TabContent-SqhSearch-3"]/div/div[2]/button')
    search_button.click()
    time.sleep(3)

    return True


def extract_coordinates_from_requests(driver):
    correct_url = "https://guland.vn/post/check-plan?screen=ban-do-gia"
    lat, lng = None, None
    polygon_points = []

    # Get only the most recent requests
    for request in driver.requests:
        if request.method == "POST" and correct_url in request.url and request.response:
            try:
                raw = request.response.body
                try:
                    decompressed = gzip.GzipFile(fileobj=io.BytesIO(raw)).read().decode("utf-8")
                except OSError:
                    decompressed = raw.decode("utf-8")

                response_data = json.loads(decompressed)

                # KIỂM TRA XEM RESPONSE CÓ CHỨA DATA VÀ TỌA ĐỘ KHÔNG
                if "data" in response_data and response_data["data"]:
                    data = response_data["data"]
                    if "lat" in data and "lng" in data and data["lat"] and data["lng"]:
                        lat = data["lat"]
                        lng = data["lng"]

                        print(f"✅ Found parcel coordinates: lat = {lat}, lng = {lng}")
                        app_logger.info(f"✅ Found parcel coordinates: lat = {lat}, lng = {lng}")

                        if "points" in data:
                            print("🧭 Polygon boundary:")
                            polygon_points = data["points"]
                            for pt in polygon_points:
                                print(f"  {pt}")
                    else:
                        print("❌ Response data không chứa tọa độ hợp lệ.")
                        app_logger.info("❌ Response data không chứa tọa độ hợp lệ.")
                else:
                    print("❌ Response không chứa data hợp lệ.")
                    app_logger.info("❌ Response không chứa data hợp lệ.")

            except Exception as e:
                print("❌ Failed to parse JSON:", e)
                app_logger.info(f"❌ Failed to parse JSON: {e}")
            break

    if lat is None or lng is None:
        print("❌ Could not find coordinates.")
        app_logger.info("❌ Could not find coordinates.")

    return lat, lng, polygon_points


def normalize_tinh_name(tinh):
    """Chuẩn hóa tên tỉnh"""
    tinh = tinh.lower().strip()
    replacements = {
        "hồ chí minh": "tp. hồ chí minh",
        "tp hcm": "tp. hồ chí minh",
        "tp.hcm": "tp. hồ chí minh",
        "tp ho chi minh": "tp. hồ chí minh",
        "thành phố hồ chí minh": "tp. hồ chí minh",
        "ho chi minh": "tp. hồ chí minh",
        "hcm": "tp. hồ chí minh",
        ".hcm": "tp. hồ chí minh",
        ". hcm": "tp. hồ chí minh",
        ". hồ chí minh.": "tp. hồ chí minh",

        ".ct" : "cần thơ",
        ". ct": "cần thơ",
        ". cần thơ": "cần thơ",
        "ct" : "cần thơ",

        ". đà nẵng":"đà nẵng",
        ".đà nẵng": "đà nẵng",
        ".đn": "đà nẵng",
        ". đn": "đà nẵng"
    }
    return replacements.get(tinh, tinh)

import re

def parse_location_info(location):
    """Phân tích địa chỉ thành các thành phần cho Guland"""
    so_thua = ""
    so_to = ""
    tinh = ""
    huyen = ""
    xa = ""

    app_logger.info(f"📍 Địa chỉ trước khi lọc và chuẩn hóa: {location}")

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

        tinh = normalize_tinh_name(tinh)



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


    location_info = {
        "so_thua": so_thua,
        "so_to": so_to,
        "tinh": tinh,
        "huyen": huyen,
        "xa": xa
    }

    app_logger.info(f"📍 Địa chỉ sau khi lọc và chuẩn hóa: {location_info}")

    return {
        "so_thua": so_thua,
        "so_to": so_to,
        "tinh": tinh,
        "huyen": huyen,
        "xa": xa
    }


def interactive_loop(driver, address_info, file_path):
    print("Dữ liệu đã lọc được:")
    print(address_info)

    # Lấy thông tin
    so_thua = address_info.get("so_thua", "").strip()
    so_to = address_info.get("so_to", "").strip()
    tinh = address_info.get("tinh", "").strip()
    huyen = address_info.get("huyen", "").strip()
    xa = address_info.get("xa", "").strip()

    print(so_thua, so_to, tinh, huyen, xa)
    # Nếu thiếu bất kỳ dữ liệu nào thì bỏ qua
    if not all([so_thua, so_to, tinh, huyen, xa]):
        print("⚠️ Thiếu dữ liệu bắt buộc (số thửa, số tờ). Chuyển sang lấy tọa đồ bằng toàn bộ địa chỉ.")
        # app_logger.info(file_path)
        #app_logger.info(address_info)
        app_logger.info("⚠️ Thiếu dữ liệu bắt buộc (số thửa, số tờ). Chuyển sang lấy tọa đồ bằng toàn bộ địa chỉ.")
        app_logger.info(
            f" - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -\n")
        return None

    to_thua_button = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, '//*[@id="to-thua-search"]/a'))
    )

    try:
        # Clear existing requests before each search
        del driver.requests

        success = fill_form(driver, so_thua, so_to, tinh, huyen, xa)

        if success:
            try:
                close_button = driver.find_element("xpath", '//*[@id="Modal-Sample"]/div/div/button')
                if close_button.is_displayed():
                    print("🚫 Không tìm thấy khu vực theo tờ thửa. Đang đóng popup...")
                    # app_logger.info(file_path)
                    ##app_logger.info(address_info)
                    app_logger.info("🚫 Không tìm thấy khu vực theo tờ thửa. Đang đóng popup...")
                    app_logger.info(
                        f" - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -\n")
                    close_button.click()

                    to_thua_button.click()

                    time.sleep(1)
                    return None  # sau khi đóng popup thì bỏ qua địa chỉ này luôn
            except NoSuchElementException:
                pass  # Không có popup, tiếp tục như bình thường
            except ElementClickInterceptedException:
                print("❌ Không thể click để đóng popup.")
                return None

            # Nếu không có popup lỗi, trích xuất toạ độ
            lat, lng, points = extract_coordinates_from_requests(driver)

            # KIỂM TRA XEM CÓ THẬT SỰ LẤY ĐƯỢC TỌA ĐỘ KHÔNG
            if lat is not None and lng is not None:
                # app_logger.info(file_path)
                #app_logger.info(address_info)
                app_logger.info("✅ Lấy tọa độ thành công.")
                app_logger.info(
                    f" - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -\n")
                return {
                    "type": "Point",
                    "coordinates": [lng, lat]
                }
            else:
                print("❌ Không lấy được tọa độ từ response.")
                # app_logger.info(file_path)
                #app_logger.info(address_info)
                app_logger.info("❌ Không lấy được tọa độ từ response.")
                app_logger.info(
                    f" - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -\n")

                # ĐÓNG TẤT CẢ POPUP/MODAL TRƯỚC KHI RETURN
                cleanup_popups_and_modals(driver)
                return None

        else:
            print("❌ Không thể điền form tìm kiếm.")
            # app_logger.info(file_path)
            #app_logger.info(address_info)
            app_logger.info("❌ Không thể điền form tìm kiếm.")
            app_logger.info(
                f" - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -\n")

            # ĐÓNG TẤT CẢ POPUP/MODAL TRƯỚC KHI RETURN
            cleanup_popups_and_modals(driver)
            return None

    except Exception as e:
        to_thua_button.click()
        print(f"❌ Lỗi: {e}")
        # app_logger.info(file_path)
        #app_logger.info(address_info)
        app_logger.info(f"❌ Lỗi: {e}")
        app_logger.info(
            f" - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -\n")

        # ĐÓNG TẤT CẢ POPUP/MODAL TRƯỚC KHI RETURN
        cleanup_popups_and_modals(driver)
        return None

def clean_location_names(parsed_location):
    """
    Làm sạch địa chỉ:
    - Giữ lại "phường 1", "quận 5" nếu sau là số.
    - Loại bỏ tiền tố nếu sau là chữ.
    - Giữ lại so_thua và so_to.
    """

    def remove_prefix_smart(value, prefixes, keep_if_number=True):
        if not value:
            return ""
        value = value.strip().lower()

        for prefix in prefixes:
            pattern = fr"^{prefix}[\.]?\s+(.*)$"
            match = re.match(pattern, value)
            if match:
                after = match.group(1).strip()
                if keep_if_number and re.match(r"^\d+$", after):  # Giữ nguyên nếu sau prefix là số
                    return value
                else:
                    value = after
                    break

        # Xoá dấu chấm/dấu phẩy và khoảng trắng dư ở đầu
        value = re.sub(r"^[\.\,\s]+", "", value)
        return value

    return {
        "so_thua": parsed_location.get("so_thua", ""),
        "so_to": parsed_location.get("so_to", ""),
        "xa": remove_prefix_smart(parsed_location.get("xa", ""), ["xã", "phường", "thị trấn"]),
        "huyen": remove_prefix_smart(parsed_location.get("huyen", ""), ["huyện", "quận", "thị xã", "tx", "thành phố"], keep_if_number=True),
        "tinh": remove_prefix_smart(parsed_location.get("tinh", ""), ["tp", "tp.", "tỉnh", "thành phố"], keep_if_number=False)
    }

def cleanup_popups_and_modals(driver):
    """Đóng tất cả popup và modal có thể còn mở"""
    try:
        # Thử đóng modal chính (Modal-Sample)
        try:
            close_button = driver.find_element("xpath", '//*[@id="Modal-Sample"]/div/div/button')
            if close_button.is_displayed():
                print("🧹 Đang đóng Modal-Sample...")
                close_button.click()
                time.sleep(0.5)
        except (NoSuchElementException, ElementNotInteractableException):
            pass

        # Thử đóng các modal khác có thể xuất hiện
        modal_selectors = [
            "//button[contains(@class, 'close') or contains(@class, 'btn-close')]",
            "//button[contains(text(), 'Đóng') or contains(text(), 'Close') or contains(text(), '×')]",
            "//*[@class='modal-header']//button",
            "//*[contains(@class, 'modal')]//button[contains(@class, 'close')]"
        ]

        for selector in modal_selectors:
            try:
                buttons = driver.find_elements("xpath", selector)
                for button in buttons:
                    if button.is_displayed() and button.is_enabled():
                        print(f"🧹 Đang đóng modal với selector: {selector}")
                        button.click()
                        time.sleep(0.3)
                        break
            except Exception:
                continue

        # Thử nhấn ESC để đóng modal
        try:
            from selenium.webdriver.common.keys import Keys
            driver.find_element("tag name", "body").send_keys(Keys.ESCAPE)
            time.sleep(0.3)
        except Exception:
            pass

        # Kiểm tra xem có overlay nào đang che màn hình không
        try:
            overlays = driver.find_elements("xpath", "//*[contains(@class, 'overlay') or contains(@class, 'backdrop')]")
            for overlay in overlays:
                if overlay.is_displayed():
                    print("🧹 Đang click để đóng overlay...")
                    overlay.click()
                    time.sleep(0.3)
        except Exception:
            pass

        print("🧹 Hoàn thành cleanup popup/modal.")

    except Exception as e:
        print(f"⚠️ Lỗi khi cleanup popup/modal: {e}")

def action_open_guland_driver(address, driver, file_path):
    # === Actions ===
    # driver = setup_driver(headless=True)
    address_raw = address
    try:
        # open_guland_page(driver)
        # print("✅ Trang Guland đã sẵn sàng.")

        address_parse = parse_location_info(address)

        return interactive_loop(driver,address_parse, file_path)
    except:
        print("❌ Fail to open Guland 2")


    # finally:
    #     driver.quit()

def main():
    action_open_guland_driver("Thửa đất số 2 (pcl); 3; 193; 194, tờ bản đồ số 14, ấp 2/5, xã Long Hậu, huyện Cần Giuộc, tỉnh Long An"
)


if __name__ == "__main__":
    main()