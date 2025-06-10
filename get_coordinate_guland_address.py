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

def setup_driver_address2(headless=True):
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless")

    # Cấu hình selenium-wire để capture requests tốt hơn
    seleniumwire_options = {
        'disable_encoding': True,  # Tắt encoding để dễ đọc response
        'suppress_connection_errors': False,
        'verify_ssl': False,
    }

    driver = webdriver.Chrome(options=options, seleniumwire_options=seleniumwire_options)
    driver.maximize_window()
    return driver


def open_guland_page_2(driver):
    driver.get("https://guland.vn")
    # Tìm và nhấn nút sign in
    try:
        sign_in_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "/html/body/div[1]/div[2]/div/div[1]/div/div/ul/li[1]/a"))
        )
        sign_in_button.click()
        print("Đã nhấn nút sign in thành công")

        # Chờ một chút để trang sign in load
        time.sleep(2)

        login_guland(driver,"0382250988","112233445566")

    except Exception as e:
        print(f"Lỗi khi nhấn nút sign in: {e}")
        # Có thể thử cách khác nếu xpath không hoạt động
        try:
            # Thử tìm bằng text link
            sign_in_link = driver.find_element(By.LINK_TEXT, "Đăng nhập")
            sign_in_link.click()
            print("Đã nhấn nút sign in bằng cách khác")
        except:
            print("Không thể tìm thấy nút sign in")

    time.sleep(1)  # giữ lại chút thời gian để trang load ổn định


def login_guland(driver, phone, password):
    try:
        # Nhập số điện thoại
        phone_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "/html/body/div[1]/div[2]/div/div/div/div/div/div/div/div[2]/div/form/div[1]/input"))
        )
        phone_input.clear()
        phone_input.send_keys(phone)
        print("✅ Đã nhập số điện thoại")

        # Nhập mật khẩu
        password_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "/html/body/div[1]/div[2]/div/div/div/div/div/div/div/div[2]/div/form/div[2]/input"))
        )
        password_input.clear()
        password_input.send_keys(password)
        print("✅ Đã nhập mật khẩu")

        # Nhấn nút đăng nhập
        login_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "/html/body/div[1]/div[2]/div/div/div/div/div/div/div/div[2]/div/form/div[4]/button"))
        )
        login_button.click()
        print("✅ Đã nhấn nút đăng nhập")

        click_price_map_button(driver)

    except Exception as e:
        print(f"❌ Lỗi khi đăng nhập: {e}")


def click_price_map_button(driver):
    try:
        # Đợi và nhấn vào nút "Bản đồ giá nhà đất"
        price_map_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "/html/body/div[1]/div[3]/div/div[1]/div/div/ul/li[6]/a"))
        )
        price_map_button.click()
        print("✅ Đã nhấn vào nút 'Bản đồ giá nhà đất'")

    except Exception as e:
        print(f"❌ Lỗi khi nhấn nút 'Bản đồ giá nhà đất': {e}")

def click_dia_diem_tab(driver, adress):

    if not is_valid_address(adress):
        print(f"❌ Địa chỉ không hợp lệ: {adress}")
        return None

    try:
        # Xóa request cũ trước khi xử lý địa chỉ mới
        # clear_previous_requests(driver)
        del driver.requests


        dia_diem_tab = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, '//*[@id="TabNav-SqhSearch"]/li[2]/a'))
        )
        dia_diem_tab.click()
        # print("✅ Đã nhấn vào tab 'Địa điểm'")

        return enter_address(driver,adress)

    except Exception as e:
        print(f"❌ Lỗi khi nhấn tab 'Địa điểm': {e}")


def enter_address(driver, address_text):


    try:
        address_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="form-lat-lng"]/div/div[1]/div[3]/input'))
        )
        address_input.clear()
        address_input.send_keys(address_text)
        print(f"✅ Đã nhập địa chỉ: {address_text}")

        time.sleep(4)
        return select_first_address_suggestion(driver)

    except Exception as e:
        print(f"❌ Lỗi khi nhập địa chỉ: {e}")


def is_valid_address(address_text):
    """
    Kiểm tra địa chỉ hợp lệ:
    - Phải có số nhà (1 hoặc nhiều số + có thể có ký tự A/B/C/...) + tên đường
    - Và phải có ít nhất một từ khóa về địa phương như phường/quận/tp/tỉnh
    """

    # Kiểm tra phần số nhà + tên đường (rộng hơn để chấp nhận dạng "21 + 23A", "12B-14C", v.v.)
    has_number_and_street = re.search(r'\d+[A-Za-z]*([\s\+\-\/]*\d*[A-Za-z]*)*\s+[^\d]+', address_text)

    # Kiểm tra từ khóa về phường/quận/thành phố
    # has_location_info = any(
    #     keyword in address_text.lower()
    #     for keyword in ['phường', 'p ', 'p.', 'quận', 'q ', 'q.', 'tp', 'tp.', 'thành phố', 'tỉnh', 'hcm', 'hồ chí minh']
    # )

    return bool(has_number_and_street)

def select_first_address_suggestion(driver):
    try:
        # Xóa các request cũ trước khi click
        del driver.requests
        # Đợi container của danh sách gợi ý hiển thị
        suggestions_container = WebDriverWait(driver, 10).until(
            EC.visibility_of_element_located((By.XPATH, '//*[@id="form-lat-lng"]/div/div[2]/div/div'))
        )

        # Đợi cho đến khi phần tử đầu tiên trong danh sách có text mới
        first_suggestion_xpath = '//*[@id="form-lat-lng"]/div/div[2]/div/div/div[1]'
        WebDriverWait(driver, 10).until(
            lambda d: d.find_element(By.XPATH, first_suggestion_xpath).text.strip() != ""
        )

        # Lấy phần tử gợi ý đầu tiên
        first_suggestion = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, first_suggestion_xpath))
        )

        suggestion_text = first_suggestion.text.strip()

        # In gợi ý ra để xác minh
        print(f"📍 Gợi ý đầu tiên hiện tại: {suggestion_text}")

        # Click chọn địa điểm
        first_suggestion.click()
        # print(f"✅ Đã chọn địa điểm gợi ý đầu tiên: {suggestion_text}")

        # Đợi và lấy response từ API check-plan
        coordinates = wait_for_check_plan_response(driver, timeout=10)

        if coordinates:
            return coordinates
        else:
            print("❌ Không thể lấy được tọa độ từ API")
            return None

    except Exception as e:
        print(f"❌ Lỗi khi chọn gợi ý địa điểm đầu tiên: {e}")
        return None



def decode_response_body(response_body, content_encoding=None):
    """
    Decode response body, xử lý các trường hợp nén (gzip, deflate)
    """
    try:
        # Nếu là bytes và có content-encoding
        if isinstance(response_body, bytes):
            # Kiểm tra xem có phải gzip không
            if content_encoding == 'gzip' or (len(response_body) >= 2 and response_body[:2] == b'\x1f\x8b'):
                try:
                    # Decode gzip
                    with gzip.GzipFile(fileobj=io.BytesIO(response_body)) as gz:
                        return gz.read().decode('utf-8')
                except:
                    pass

            # Thử decode bình thường
            try:
                return response_body.decode('utf-8')
            except UnicodeDecodeError:
                # Thử các encoding khác
                for encoding in ['latin1', 'cp1252', 'iso-8859-1']:
                    try:
                        return response_body.decode(encoding)
                    except:
                        continue

                # Nếu tất cả đều fail, return dạng string với errors='ignore'
                return response_body.decode('utf-8', errors='ignore')

        return response_body

    except Exception as e:
        print(f"❌ Lỗi decode response body: {e}")
        return None


def wait_for_check_plan_response(driver, timeout=15):
    """
    Đợi và lấy response từ API check-plan
    """
    start_time = time.time()

    while time.time() - start_time < timeout:
        # Duyệt qua tất cả các request đã được capture
        for request in driver.requests:
            if request.response and ('check-plan' in request.url or 'check-plan' in str(request.url)):
                try:
                    # print(f"🔍 Tìm thấy request: {request.url}")
                    # print(f"📊 Response status: {request.response.status_code}")

                    # Lấy content-encoding header
                    content_encoding = None
                    if hasattr(request.response, 'headers'):
                        content_encoding = request.response.headers.get('content-encoding', '').lower()
                        # print(f"📦 Content-Encoding: {content_encoding}")

                    # Đọc response body
                    response_body = request.response.body
                    # print(f"📄 Response body type: {type(response_body)}")
                    # print(f"📏 Response body length: {len(response_body) if response_body else 0}")

                    # Decode response body
                    response_text = decode_response_body(response_body, content_encoding)

                    if not response_text:
                        print("❌ Không thể decode response body")
                        continue

                    # print(f"📝 Response text preview: {response_text[:200]}...")

                    # Parse JSON
                    response_data = json.loads(response_text)

                    # Kiểm tra xem có chứa lat/lng không
                    if 'data' in response_data and response_data['data']:
                        data = response_data['data']
                        if 'lat' in data and 'lng' in data:
                            lat = float(data['lat'])
                            lng = float(data['lng'])
                            print(f"✅ Đã lấy được tọa độ từ check-plan API:")
                            print(f"   Latitude: {lat}")
                            print(f"   Longitude: {lng}")
                            print(f"   Address: {data.get('address', 'N/A')}")
                            print(f"   Full response: {response_data}")

                            return {
                                "type": "Point",
                                "coordinates": [lng, lat]  # Lưu ý: GeoJSON yêu cầu [lng, lat]
                            }
                    else:
                        print(f"⚠️ Response không chứa lat/lng: {response_data}")

                except json.JSONDecodeError as e:
                    print(f"❌ Lỗi parse JSON: {e}")
                    print(f"📝 Response text: {response_text[:500] if 'response_text' in locals() else 'N/A'}")
                    continue
                except Exception as e:
                    print(f"❌ Lỗi khi xử lý response từ check-plan: {e}")
                    continue

        time.sleep(0.5)  # Chờ 0.5 giây trước khi check lại

    print("❌ Không tìm thấy response từ check-plan API trong thời gian chờ")

    # Debug: In ra tất cả các request để kiểm tra
    print("\n🔍 Debug - Tất cả các request đã capture:")
    for i, request in enumerate(driver.requests[-10:]):  # Chỉ in 10 request cuối
        print(f"  {i + 1}. {request.url}")
        if request.response:
            print(f"      Status: {request.response.status_code}")

    return None


def clear_previous_requests(driver):
    """
    Xóa các request đã capture trước đó.
    """
    try:
        driver.requests.clear()
        # print("🧹 Đã xóa tất cả các request cũ.")
    except Exception as e:
        print(f"❌ Lỗi khi xóa request: {e}")


def action_open_guland_driver():
    driver = setup_driver_address2(headless=False)

    try:
        open_guland_page_2(driver)

        while True:
            address = input("\n🔍 Nhập địa chỉ để tìm (hoặc nhập 'exit' để thoát): ").strip()
            if address.lower() == "exit":
                break

            # Nhấn lại tab Địa điểm
            click_dia_diem_tab(driver,address)

    except Exception as e:
        print(f"❌ Lỗi khi mở Guland: {e}")

    finally:
        input("⏳ Nhấn Enter để đóng trình duyệt...")
        driver.quit()

def main():
    action_open_guland_driver()


if __name__ == "__main__":
    main()