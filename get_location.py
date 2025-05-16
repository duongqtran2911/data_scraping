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

so_thua = "2"
so_to = "14"
tinh = "Long An"
huyen = "Huyện Cần Giuộc"
xa = "Xã Long Hậu"

# Khởi tạo trình duyệt
options = webdriver.ChromeOptions()
options.add_argument("--headless")  # Nếu muốn chạy ẩn
driver = webdriver.Chrome(options=options)  # Cần có chromedriver đúng version

# Bước 1: Truy cập Guland.vn
driver.get("https://guland.vn/ban-do-gia")

driver.maximize_window()

wait = WebDriverWait(driver, 10)

# Nhấn nút "Tờ Thửa"
to_thua_button = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="to-thua-search"]/a')))
to_thua_button.click()

time.sleep(3)  # Chờ trang load

# Điền thông tin
# Nhập số tờ
driver.find_element(By.XPATH, '//*[@id="form-to-thua"]/div[1]/div/div/div/div[2]/div/div/input[1]').send_keys(so_thua)

# Nhập số thửa
driver.find_element(By.XPATH, '//*[@id="form-to-thua"]/div[1]/div/div/div/div[2]/div/div/input[2]').send_keys(so_to)

# Chọn tỉnh
driver.find_element(By.XPATH, '//*[@id="select2-province_id_4-container"]').click()
time.sleep(1)
driver.find_element(By.CLASS_NAME, 'select2-search__field').send_keys(tinh)
driver.find_element(By.CLASS_NAME, 'select2-search__field').send_keys(Keys.ENTER)

# Chọn huyện
time.sleep(1)
driver.find_element(By.XPATH, '//*[@id="select2-district_id_4-container"]').click()
time.sleep(1)
driver.find_element(By.CLASS_NAME, 'select2-search__field').send_keys(huyen)
driver.find_element(By.CLASS_NAME, 'select2-search__field').send_keys(Keys.ENTER)

# Chọn xã
time.sleep(1)
driver.find_element(By.XPATH, '//*[@id="select2-ward_id_4-container"]').click()
time.sleep(1)
driver.find_element(By.CLASS_NAME, 'select2-search__field').send_keys(xa)
driver.find_element(By.CLASS_NAME, 'select2-search__field').send_keys(Keys.ENTER)

# Nhấn nút Tìm kiếm
time.sleep(1)
driver.find_element(By.XPATH, '//*[@id="TabContent-SqhSearch-3"]/div/div[2]/button').click()

# Đợi dữ liệu phản hồi
time.sleep(5)


time.sleep(2)  # Chờ trang tải

print("Tiêu đề trang:", driver.title)

if "Bảng giá" in driver.title:
    print("✅ Truy cập trang Guland thành công!")
else:
    print("❌ Không truy cập được trang Guland.")

# === Step 5: Intercept check-plan POST request ===
lat, lng = None, None
correct_url = "https://guland.vn/post/check-plan?screen=ban-do-gia"

for request in driver.requests:
    if request.method == "POST" and correct_url in request.url and request.response:
        try:
            raw = request.response.body
            try:
                # Try decompressing with gzip (most likely)
                decompressed = gzip.GzipFile(fileobj=io.BytesIO(raw)).read().decode("utf-8")
            except OSError:
                # Fallback: assume it's not gzipped
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

# === Step 6: Clean up ===
driver.quit()