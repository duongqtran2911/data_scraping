from seleniumwire import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import json
import requests

so_to = "14"
so_thua = "2"
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
driver.find_element(By.XPATH, '//*[@id="form-to-thua"]/div[1]/div/div/div/div[2]/div/div/input[1]').send_keys(so_to)

# Nhập số thửa
driver.find_element(By.XPATH, '//*[@id="form-to-thua"]/div[1]/div/div/div/div[2]/div/div/input[2]').send_keys(so_thua)

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



# Gửi request lấy dữ liệu tọa độ

# Duyệt các request mà trình duyệt đã gửi
check_plan_url = None
for request in driver.requests:
    if "check-plan" in request.url and request.response:
        check_plan_url = request.url
        break

# Nếu có URL rồi thì fetch như bình thường
if check_plan_url:
    print("✅ Tìm thấy URL:", check_plan_url)

    response = requests.get(check_plan_url)
    if response.status_code == 200:
        data = response.json()
        if data["status"] == 1:
            for i, item in enumerate(data["data"]):
                print(f"\n🏷️ Mảnh đất {i+1}: {item['title']}")
                print(f"  📍 Tọa độ: lat = {item['lat']}, lng = {item['lng']}")
        else:
            print("❌ API lỗi: status != 1")
    else:
        print("❌ Request lỗi:", response.status_code)
else:
    print("❌ Không tìm thấy request check-plan.")

# Đóng trình duyệt
driver.quit()