import os
import time
import random
import logging
import pandas as pd
from math import ceil
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.edge.service import Service as EdgeService
from selenium.webdriver.edge.options import Options as EdgeOptions
from selenium.webdriver.common.by import By
from webdriver_manager.microsoft import EdgeChromiumDriverManager

# --- CẤU HÌNH LOGGING ---
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
log_filename = f"crawl_tgdd_list_{datetime.now().strftime('%Y%m%d')}.log"
log_path = os.path.join(LOG_DIR, log_filename)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_path, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- CẤU HÌNH ĐƯỜNG DẪN DỮ LIỆU ---
RAW_DATA_PATH = "data/raw/fact"
os.makedirs(RAW_DATA_PATH, exist_ok=True)

DRIVER_DIR = "drivers"
os.makedirs(DRIVER_DIR, exist_ok=True)

# --- CÁC HÀM HỖ TRỢ ---

def setup_driver(headless=True):
    """Cài đặt WebDriver và các option."""
    logger.info("Đang khởi tạo WebDriver...")
    options = EdgeOptions()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-notifications")
    
    # Tự động tải driver vào thư mục drivers/
    driver_executable = EdgeChromiumDriverManager(path=DRIVER_DIR).install()
    service = EdgeService(executable_path=driver_executable)
    
    driver = webdriver.Edge(service=service, options=options)
    return driver

def calculate_pi_max(items_per_page, remain_count):
    """Tính số page tối đa cần load."""
    total_items = items_per_page + remain_count
    pi_max = ceil(total_items / items_per_page) - 1
    return pi_max, total_items

def crawl_tgdd_list():
    base_url = "https://www.thegioididong.com/laptop"
    request_url_template = "https://www.thegioididong.com/laptop#c=44&o=13&pi={}"
    
    driver = None
    try:
        driver = setup_driver(headless=True)
        
        # Bước 1: Lấy tổng số sản phẩm để tính pi_max
        logger.info(f"Đang truy cập {base_url} để lấy thông tin tổng quát...")
        driver.get(base_url)
        time.sleep(3)
        
        items = driver.find_elements(By.CSS_SELECTOR, "a.main-contain")
        remain_elements = driver.find_elements(By.CSS_SELECTOR, "strong.see-more-btn span.remain")
        
        if not remain_elements:
            logger.warning("Không tìm thấy nút 'Xem thêm', có thể dữ liệu đã hiển thị hết.")
            remain_count = 0
        else:
            remain_count = int(remain_elements[0].text.replace('.', '').strip())
            
        items_per_page = len(items)
        pi_max, total_expected = calculate_pi_max(items_per_page, remain_count)
        
        logger.info(f"Phát hiện {items_per_page} sp/trang. Tổng cộng khoảng {total_expected} sp. Cần load pi={pi_max}")
        
        # Bước 2: Truy cập URL với pi_max để load toàn bộ sản phẩm
        full_url = request_url_template.format(pi_max)
        logger.info(f"Đang tải toàn bộ sản phẩm từ: {full_url}")
        driver.get(full_url)
        
        # Chờ đợi một chút để JavaScript render
        wait_time = random.randint(5, 8)
        logger.info(f"Đợi {wait_time}s để trang tải hoàn tất...")
        time.sleep(wait_time)
        
        # Bước 3: Trích xuất dữ liệu
        all_items = driver.find_elements(By.CSS_SELECTOR, "a.main-contain")
        logger.info(f"Tìm thấy {len(all_items)} sản phẩm thực tế trên trang.")
        
        data = []
        for item in all_items:
            try:
                product = {
                    "pid": item.get_attribute("data-id"),
                    "link": item.get_attribute("href"),
                    "name": item.get_attribute("data-name"),
                    "price": item.get_attribute("data-price"),
                    "brand": item.get_attribute("data-brand")
                }
                data.append(product)
            except Exception as e:
                logger.error(f"Lỗi khi lấy thông tin 1 sản phẩm: {e}")
        
        # Bước 4: Lưu dữ liệu
        if data:
            df = pd.DataFrame(data)
            output_file = os.path.join(RAW_DATA_PATH, "raw_data.csv")
            df.to_csv(output_file, index=False, encoding='utf-8-sig')
            logger.info(f"Đã lưu thành công {len(df)} dòng vào {output_file}")
        else:
            logger.error("Không lấy được dữ liệu nào!")

    except Exception as e:
        logger.critical(f"LỖI HỆ THỐNG: {e}", exc_info=True)
    finally:
        if driver:
            driver.quit()
            logger.info("Đã đóng trình duyệt.")

if __name__ == "__main__":
    start_time = time.time()
    logger.info("--- BẮT ĐẦU CRAWL DANH SÁCH SẢN PHẨM ---")
    crawl_tgdd_list()
    end_time = time.time()
    duration = (end_time - start_time) / 60
    logger.info(f"--- HOÀN THÀNH TRONG {duration:.2f} PHÚT ---")
