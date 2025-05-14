from seleniumwire import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import json
import requests
import io
import gzip

i = 0

def setup_driver(headless=True):
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless")
    driver = webdriver.Chrome(options=options)
    driver.maximize_window()
    return driver


def open_guland_page(driver):
    driver.get("https://guland.vn/ban-do-gia")
    WebDriverWait(driver, 1).until(
        EC.element_to_be_clickable((By.XPATH, '//*[@id="to-thua-search"]/a'))
    ).click()
    time.sleep(1)

def fill_form(driver, so_thua, so_to, tinh, huyen, xa):
    # mở lại form tờ thửa


    wait = WebDriverWait(driver, 1)

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
    driver.find_element(By.XPATH, '//*[@id="TabContent-SqhSearch-3"]/div/div[2]/button').click()
    time.sleep(3)



def extract_coordinates_from_requests(driver):
    correct_url = "https://guland.vn/post/check-plan?screen=ban-do-gia"
    lat, lng = None, None

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

                if "points" in response_data["data"]:
                    print("🧭 Polygon boundary:")
                    for pt in response_data["data"]["points"]:
                        print(f"  {pt}")

            except Exception as e:
                print("❌ Failed to parse JSON:", e)
            break

    if lat is None or lng is None:
        print("❌ Could not find coordinates.")
    return lat, lng


def interactive_loop(driver):
    while True:
        print("\n👉 Nhập thông tin để tìm kiếm (gõ 'exit' ở bất kỳ đâu để thoát):")

        so_thua = input("  Nhập số thửa: ")
        if so_thua.lower() == 'exit': break

        so_to = input("  Nhập số tờ: ")
        if so_to.lower() == 'exit': break

        tinh = input("  Nhập tỉnh: ")
        if tinh.lower() == 'exit': break

        huyen = input("  Nhập huyện: ")
        if huyen.lower() == 'exit': break

        xa = input("  Nhập xã: ")
        if xa.lower() == 'exit': break

        try:
            fill_form(driver, so_thua, so_to, tinh, huyen, xa)
            extract_coordinates_from_requests(driver)

        except Exception as e:
            print(f"❌ Lỗi: {e}")

def main():
    # === Actions ===
    driver = setup_driver(headless=True)
    try:
        open_guland_page(driver)
        print("✅ Trang Guland đã sẵn sàng.")

        interactive_loop(driver)

    finally:
        driver.quit()


if __name__ == "__main__":
    main()
