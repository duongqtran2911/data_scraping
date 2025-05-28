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

def setup_driver(headless=True):
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless")
    driver = webdriver.Chrome(options=options)
    driver.maximize_window()
    return driver


def open_guland_page(driver):
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

def action_open_guland_driver(address):
    # === Actions ===
    driver = setup_driver(headless=False)

    try:
        open_guland_page(driver)
        print("✅ Trang Guland đã sẵn sàng.")
    except:
        print("❌ Fail to open Guland 2")

    finally:
        pass

def main():
    action_open_guland_driver("")


if __name__ == "__main__":
    main()