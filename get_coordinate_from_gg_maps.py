import time
import logging
import re
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException, TimeoutException
import urllib.parse

# Setup logging
log_path = "gmaps_coordinates.log"
app_logger = logging.getLogger("gmaps_coordinates")
app_logger.setLevel(logging.INFO)

# Thêm handler nếu chưa có
if not app_logger.handlers:
    file_handler = logging.FileHandler(log_path, mode="a", encoding="utf-8")
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    app_logger.addHandler(file_handler)


def setup_driver(headless=True):
    """Khởi tạo Chrome driver"""
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)

    driver = webdriver.Chrome(options=options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    driver.maximize_window()
    return driver


def open_google_maps(driver):
    """Mở Google Maps"""
    try:
        driver.get("https://www.google.com/maps")
        # Đợi trang load
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "searchboxinput"))
        )
        time.sleep(2)
        print("✅ Google Maps đã sẵn sàng.")
        app_logger.info("✅ Google Maps đã sẵn sàng.")
        return True
    except TimeoutException:
        print("❌ Không thể tải Google Maps.")
        app_logger.error("❌ Không thể tải Google Maps.")
        return False


def search_address(driver, address):
    """Tìm kiếm địa chỉ trên Google Maps"""
    try:
        wait = WebDriverWait(driver, 10)

        # Tìm ô search và xóa nội dung cũ
        search_box = wait.until(EC.element_to_be_clickable((By.ID, "searchboxinput")))
        search_box.clear()
        search_box.send_keys(address)

        # Nhấn Enter hoặc click nút search
        search_box.send_keys(Keys.ENTER)

        # Đợi kết quả tìm kiếm
        time.sleep(3)

        print(f"✅ Đã tìm kiếm địa chỉ: {address}")
        app_logger.info(f"✅ Đã tìm kiếm địa chỉ: {address}")
        return True

    except Exception as e:
        print(f"❌ Lỗi khi tìm kiếm địa chỉ: {e}")
        app_logger.error(f"❌ Lỗi khi tìm kiếm địa chỉ: {e}")
        return False


def extract_coordinates_from_url(driver):
    """Trích xuất tọa độ từ URL Google Maps"""
    try:
        # Đợi URL cập nhật
        time.sleep(2)
        current_url = driver.current_url

        # Pattern để tìm tọa độ trong URL Google Maps
        # Format: @lat,lng,zoom hoặc !3d<lat>!4d<lng>

        # Thử pattern @lat,lng
        pattern1 = r'@(-?\d+\.\d+),(-?\d+\.\d+)'
        match1 = re.search(pattern1, current_url)

        if match1:
            lat = float(match1.group(1))
            lng = float(match1.group(2))
            print(f"✅ Tìm thấy tọa độ từ URL (pattern @): lat = {lat}, lng = {lng}")
            app_logger.info(f"✅ Tìm thấy tọa độ từ URL (pattern @): lat = {lat}, lng = {lng}")
            return lat, lng

        # Thử pattern !3d<lat>!4d<lng>
        pattern2 = r'!3d(-?\d+\.\d+)!4d(-?\d+\.\d+)'
        match2 = re.search(pattern2, current_url)

        if match2:
            lat = float(match2.group(1))
            lng = float(match2.group(2))
            print(f"✅ Tìm thấy tọa độ từ URL (pattern !3d!4d): lat = {lat}, lng = {lng}")
            app_logger.info(f"✅ Tìm thấy tọa độ từ URL (pattern !3d!4d): lat = {lat}, lng = {lng}")
            return lat, lng

        # Thử pattern place data
        pattern3 = r'!2d(-?\d+\.\d+)!3d(-?\d+\.\d+)'
        match3 = re.search(pattern3, current_url)

        if match3:
            lng = float(match3.group(1))  # !2d là longitude
            lat = float(match3.group(2))  # !3d là latitude
            print(f"✅ Tìm thấy tọa độ từ URL (pattern !2d!3d): lat = {lat}, lng = {lng}")
            app_logger.info(f"✅ Tìm thấy tọa độ từ URL (pattern !2d!3d): lat = {lat}, lng = {lng}")
            return lat, lng

        print("❌ Không tìm thấy tọa độ trong URL")
        app_logger.warning("❌ Không tìm thấy tọa độ trong URL")
        return None, None

    except Exception as e:
        print(f"❌ Lỗi khi trích xuất tọa độ từ URL: {e}")
        app_logger.error(f"❌ Lỗi khi trích xuất tọa độ từ URL: {e}")
        return None, None


def extract_coordinates_from_page(driver):
    """Trích xuất tọa độ từ các element trên trang"""
    try:
        wait = WebDriverWait(driver, 5)

        # Thử tìm tọa độ trong các element khác nhau
        coordinate_selectors = [
            "[data-value*='@']",  # Element chứa @lat,lng
            "button[data-value*='@']",
            "[data-lat]",  # Element có attribute data-lat
            "[data-lng]",
            ".widget-pane-link",  # Link trong widget pane
            "button[jsaction*='share']"  # Nút share
        ]

        for selector in coordinate_selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                for element in elements:
                    # Kiểm tra data-value
                    data_value = element.get_attribute("data-value")
                    if data_value and "@" in data_value:
                        pattern = r'@(-?\d+\.\d+),(-?\d+\.\d+)'
                        match = re.search(pattern, data_value)
                        if match:
                            lat = float(match.group(1))
                            lng = float(match.group(2))
                            print(f"✅ Tìm thấy tọa độ từ data-value: lat = {lat}, lng = {lng}")
                            app_logger.info(f"✅ Tìm thấy tọa độ từ data-value: lat = {lat}, lng = {lng}")
                            return lat, lng

                    # Kiểm tra href
                    href = element.get_attribute("href")
                    if href and "@" in href:
                        pattern = r'@(-?\d+\.\d+),(-?\d+\.\d+)'
                        match = re.search(pattern, href)
                        if match:
                            lat = float(match.group(1))
                            lng = float(match.group(2))
                            print(f"✅ Tìm thấy tọa độ từ href: lat = {lat}, lng = {lng}")
                            app_logger.info(f"✅ Tìm thấy tọa độ từ href: lat = {lat}, lng = {lng}")
                            return lat, lng

            except Exception:
                continue

        # Thử click chuột phải để lấy menu context
        try:
            # Tìm map container và click chuột phải
            map_element = driver.find_element(By.CSS_SELECTOR, "[role='main'], #map, [data-ved]")
            driver.execute_script("arguments[0].dispatchEvent(new MouseEvent('contextmenu', {bubbles: true}));",
                                  map_element)
            time.sleep(1)

            # Tìm option "What's here?" hoặc tương tự
            context_menu_items = driver.find_elements(By.CSS_SELECTOR, "[role='menuitem'], .context-menu-item")
            for item in context_menu_items:
                if "here" in item.text.lower() or "đây" in item.text.lower():
                    item.click()
                    time.sleep(2)

                    # Sau khi click, tọa độ có thể xuất hiện trong URL hoặc popup
                    return extract_coordinates_from_url(driver)

        except Exception:
            pass

        return None, None

    except Exception as e:
        print(f"❌ Lỗi khi trích xuất tọa độ từ trang: {e}")
        app_logger.error(f"❌ Lỗi khi trích xuất tọa độ từ trang: {e}")
        return None, None


def get_coordinates_from_address(driver, address):
    """Lấy tọa độ từ địa chỉ"""
    try:
        # Tìm kiếm địa chỉ
        if not search_address(driver, address):
            return None

        # Đợi trang load kết quả
        time.sleep(3)

        # Thử trích xuất tọa độ từ URL trước
        lat, lng = extract_coordinates_from_url(driver)

        if lat is not None and lng is not None:
            return {
                "type": "Point",
                "coordinates": [lng, lat],
                "source": "google_maps_url"
            }

        # Nếu không lấy được từ URL, thử từ các element trên trang
        lat, lng = extract_coordinates_from_page(driver)

        if lat is not None and lng is not None:
            return {
                "type": "Point",
                "coordinates": [lng, lat],
                "source": "google_maps_page"
            }

        print("❌ Không thể lấy tọa độ từ Google Maps")
        app_logger.warning("❌ Không thể lấy tọa độ từ Google Maps")
        return None

    except Exception as e:
        print(f"❌ Lỗi khi lấy tọa độ: {e}")
        app_logger.error(f"❌ Lỗi khi lấy tọa độ: {e}")
        return None


def clean_address_for_search(address):
    """Làm sạch địa chỉ để tìm kiếm tốt hơn trên Google Maps"""
    if not address:
        return ""

    # Loại bỏ thông tin thửa/tờ vì Google Maps không hiểu
    address = re.sub(r'[Tt]hửa (?:đất )?số\s*\d+[,\s]*', '', address)
    address = re.sub(r'TĐS\s*\d+[,\s]*', '', address)
    address = re.sub(r'[Tt]ờ bản đồ(?: số)?\s*\d+[,\s]*', '', address)
    address = re.sub(r'TBĐS?\s*\d+[,\s]*', '', address)

    # Chuẩn hóa dấu phẩy và khoảng trắng
    address = re.sub(r'\s*,\s*', ', ', address)
    address = re.sub(r'\s+', ' ', address).strip()

    # Thêm "Vietnam" vào cuối nếu chưa có
    if not re.search(r'\b(vietnam|việt nam)\b', address, re.IGNORECASE):
        address += ", Vietnam"

    return address


def process_single_address(driver, address, file_path=None):
    """Xử lý một địa chỉ duy nhất"""
    print(f"🔍 Đang xử lý địa chỉ: {address}")
    app_logger.info(f"🔍 Đang xử lý địa chỉ: {address}")

    if file_path:
        app_logger.info(f"📁 File: {file_path}")

    # Làm sạch địa chỉ
    clean_address = clean_address_for_search(address)
    print(f"🧹 Địa chỉ sau khi làm sạch: {clean_address}")
    app_logger.info(f"🧹 Địa chỉ sau khi làm sạch: {clean_address}")

    # Lấy tọa độ
    result = get_coordinates_from_address(driver, clean_address)

    if result:
        print(f"✅ Thành công: {result}")
        app_logger.info(f"✅ Thành công: {result}")
    else:
        print("❌ Không lấy được tọa độ")
        app_logger.warning("❌ Không lấy được tọa độ")

    app_logger.info("-" * 80)
    return result


def main():
    """Hàm chính để test"""
    driver = setup_driver(headless=False)  # Set False để xem browser

    try:
        if not open_google_maps(driver):
            return

        # Test với một số địa chỉ
        test_addresses = [
            "19 Nguyễn Đình Chiểu, Quận 1, TP.HCM",
        ]

        for address in test_addresses:
            result = process_single_address(driver, address)
            print(f"Kết quả cho '{address}': {result}")
            print("-" * 50)
            time.sleep(2)

    finally:
        driver.quit()


if __name__ == "__main__":
    main()