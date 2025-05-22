import logging
import os
import re

from selenium.common import NoSuchElementException, ElementClickInterceptedException
from seleniumwire import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import json
import io
import gzip


log_dir = "logs_status_coordinate"
os.makedirs(log_dir, exist_ok=True)

# Đường dẫn đầy đủ đến file log
log_path = os.path.join(log_dir, "status_coordinate-1.log")

# Tạo logger riêng cho ứng dụng
app_logger = logging.getLogger("app_logger1")
app_logger.setLevel(logging.INFO)

# Đảm bảo logger này không bị ảnh hưởng bởi các logger khác
app_logger.propagate = False

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
    time.sleep(0.5)
    driver.find_element(By.ID, "select2-district_id_4-container").click()
    wait.until(EC.visibility_of_element_located((By.CLASS_NAME, "select2-search__field")))
    search = driver.find_element(By.CLASS_NAME, "select2-search__field")
    search.send_keys(Keys.CONTROL + "a")
    search.send_keys(Keys.BACKSPACE)
    search.send_keys(huyen)
    search.send_keys(Keys.ENTER)

    # 4. Reset và chọn lại Xã
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
                lat = response_data["data"]["lat"]
                lng = response_data["data"]["lng"]

                print(f"✅ Found parcel coordinates: lat = {lat}, lng = {lng}")
                app_logger.info(f"✅ Found parcel coordinates: lat = {lat}, lng = {lng}")

                if "points" in response_data["data"]:
                    print("🧭 Polygon boundary:")
                    polygon_points = response_data["data"]["points"]
                    for pt in polygon_points:
                        print(f"  {pt}")

            except Exception as e:
                print("❌ Failed to parse JSON:", e)
                app_logger.info("❌ Failed to parse JSON:", e)
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
        ".ct" : "cần thơ",
        ". ct": "cần thơ",
        "ct" : "cần thơ"
    }
    return replacements.get(tinh, tinh)

# Phân tích địa chỉ thành các thành phần: tỉnh, huyện, xã
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

        # Số thửa: "Thửa đất số 104" hoặc "Thửa số 104"
        thuad_match = re.search(r"(?:[Tt]hửa (?:đất )?số\s*)(\d+)", location)
        if thuad_match:
            so_thua = thuad_match.group(1)

        # Số tờ: "Tờ bản đồ 06" hoặc "Tờ bản đồ số 06"
        tobd_match = re.search(r"[Tt]ờ bản đồ(?: số)?\s*(\d+)", location)
        if tobd_match:
            so_to = tobd_match.group(1)

        # Tìm phần tỉnh/thành phố
        for part in parts:
            part_lower = part.lower()
            if "tỉnh" in part_lower or "thành phố" in part_lower or "tp" in part_lower:
                tinh = part_lower.replace("tỉnh", "").replace("thành phố", "").replace("tp", "").strip()
                break

        # tinh = normalize_tinh_name(tinh)
        # Tìm phần huyện/quận/thị xã (bao gồm "tx" là viết tắt thị xã)
        for part in parts:
            part_lower = part.lower()
            if ("huyện" in part_lower or "quận" in part_lower or
                "thị xã" in part_lower or part_lower.startswith("tx ")):
                huyen = part_lower.replace("tx", "thị xã").strip()
                break

        # Tìm phần xã/phường/thị trấn
        for part in parts:
            part_lower = part.lower()
            if "xã" in part_lower or "phường" in part_lower or "thị trấn" in part_lower:
                xa = part_lower.strip()
                break

        # Dự phòng nếu vẫn thiếu
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
        print("⚠️ Thiếu dữ liệu bắt buộc (số thửa, số tờ, tỉnh, huyện, xã). Bỏ qua địa chỉ này.")
        # app_logger.info(file_path)
        app_logger.info(address_info)
        app_logger.info("⚠️ Thiếu dữ liệu bắt buộc (số thửa, số tờ, tỉnh, huyện, xã). Bỏ qua địa chỉ này.")
        app_logger.info(
            f" - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -\n")
        return None

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
                    app_logger.info(address_info)
                    app_logger.info("🚫 Không tìm thấy khu vực theo tờ thửa. Đang đóng popup...")
                    app_logger.info(
                        f" - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -\n")
                    close_button.click()
                    time.sleep(1)
                    return None  # sau khi đóng popup thì bỏ qua địa chỉ này luôn
            except NoSuchElementException:
                pass  # Không có popup, tiếp tục như bình thường
            except ElementClickInterceptedException:
                print("❌ Không thể click để đóng popup.")
                return None

                # Nếu không có popup lỗi, trích xuất toạ độ
            lat, lng, points = extract_coordinates_from_requests(driver)
            # app_logger.info(file_path)
            app_logger.info(address_info)
            app_logger.info("✅ Lấy tọa độ thành công.")
            app_logger.info(
                f" - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -\n")
            return {
                "type": "Point",
                "coordinates": [lng, lat]
            }
        else:
            print("❌ Không thể điền form tìm kiếm.")
            app_logger.info(address_info)
            app_logger.info("❌ Không thể điền form tìm kiếm.")
            app_logger.info(
                f" - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -\n")

    except Exception as e:
        print(f"❌ Lỗi: {e}")
        # app_logger.info(file_path)
        app_logger.info(address_info)
        app_logger.info(f"❌ Lỗi: {e}")
        app_logger.info(
            f" - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -\n")

    return None

def action_open_guland_driver(address, driver, file_path):
    # === Actions ===
    # driver = setup_driver(headless=True)
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