import os
import time
import random
import logging
import pandas as pd
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.edge.service import Service as EdgeService
from selenium.webdriver.edge.options import Options as EdgeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.microsoft import EdgeChromiumDriverManager

# --- CẤU HÌNH LOGGING ---
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
log_filename = f"crawl_tgdd_details_{datetime.now().strftime('%Y%m%d')}.log"
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

# --- CẤU HÌNH ĐƯỜNG DẪN ---
INPUT_FILE = "data/raw/fact/raw_data.csv"
OUTPUT_DIM_DIR = "data/raw/dim"
OUTPUT_FACT_DIR = "data/raw/fact"
DRIVER_DIR = "drivers"
CHECKPOINT_FILE = "logs/crawled_pids.txt"

os.makedirs(OUTPUT_DIM_DIR, exist_ok=True)
os.makedirs(OUTPUT_FACT_DIR, exist_ok=True)
os.makedirs(DRIVER_DIR, exist_ok=True)

# --- HÀM HỖ TRỢ ---

def setup_driver(headless=True):
    options = EdgeOptions()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-notifications")
    options.add_argument("--blink-settings=imagesEnabled=false") # Tắt ảnh để load nhanh hơn
    
    driver_path = EdgeChromiumDriverManager(path=DRIVER_DIR).install()
    service = EdgeService(executable_path=driver_path)
    return webdriver.Edge(service=service, options=options)

def get_crawled_pids():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            return set(line.strip() for line in f)
    return set()

def save_checkpoint(pid):
    with open(CHECKPOINT_FILE, "a") as f:
        f.write(f"{pid}\n")

def extract_specs(driver):
    """Trích xuất toàn bộ thông số kỹ thuật từ trang chi tiết."""
    specs = {}
    try:
        # Chờ section chính hiển thị
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "section.detail.detailv2"))
        )
        
        boxes = driver.find_elements(By.CSS_SELECTOR, "div.box-specifi")
        for box in boxes:
            # Click để mở rộng thông số (nếu có thể)
            try:
                driver.execute_script("arguments[0].click();", box)
                time.sleep(0.2)
            except:
                pass

            titles = box.find_elements(By.TAG_NAME, "h3")
            group_name = titles[0].text.strip() if titles else "Thông tin khác"
            specs[group_name] = {}

            items = box.find_elements(By.CSS_SELECTOR, "ul.text-specifi li")
            for item in items:
                strongs = item.find_elements(By.TAG_NAME, "strong")
                key = strongs[0].text.strip().replace(":", "") if strongs else "Chi tiết"

                asides = item.find_elements(By.CSS_SELECTOR, "aside span, aside a")
                parts = [v.text.strip() for v in asides if v.text.strip()]

                if parts:
                    value = parts if len(parts) > 1 else parts[0]
                else:
                    # Fallback lấy text thuần
                    value = item.text.replace(key, "").strip(" -:\n")
                
                specs[group_name][key] = value
        return specs
    except Exception as e:
        logger.error(f"Lỗi khi trích xuất specs: {e}")
        return None

def crawl_details():
    if not os.path.exists(INPUT_FILE):
        logger.error(f"Không tìm thấy file đầu vào: {INPUT_FILE}")
        return

    df_links = pd.read_csv(INPUT_FILE)
    crawled_pids = get_crawled_pids()
    
    total = len(df_links)
    logger.info(f"Bắt đầu cào chi tiết cho {total} sản phẩm. Đã hoàn thành trước đó: {len(crawled_pids)}")

    # Khởi tạo các list để lưu data tạm thời
    all_specs_data = {} # {group_name: [list of dicts]}
    rating_sales_data = []

    driver = setup_driver(headless=True)
    
    try:
        for index, row in df_links.iterrows():
            pid = str(row['pid'])
            if pid in crawled_pids:
                continue

            logger.info(f"[{index+1}/{total}] Đang cào: {row['name']} (PID: {pid})")
            
            try:
                driver.get(row['link'])
                time.sleep(random.uniform(1.5, 3.0))

                # 1. Lấy Specs
                specs = extract_specs(driver)
                
                # 2. Lấy Rating & Sales
                try:
                    sales_text = driver.find_element(By.CSS_SELECTOR, "span.quantity-sale").text
                except:
                    sales_text = None
                
                try:
                    rating_text = driver.find_element(By.CSS_SELECTOR, "div.detail-rate p").text
                except:
                    rating_text = None

                # Phân loại specs vào các group
                if specs:
                    for group, values in specs.items():
                        if group not in all_specs_data:
                            all_specs_data[group] = []
                        row_data = values.copy()
                        row_data["pid"] = pid
                        all_specs_data[group].append(row_data)

                # Lưu Rating/Sales
                rating_sales_data.append({
                    "pid": pid,
                    "rating": rating_text,
                    "quantity_sales": sales_text,
                    "crawl_date": datetime.now().strftime("%Y-%m-%d")
                })

                # Đánh dấu đã xong
                save_checkpoint(pid)
                
                # Batch Save & Reset Driver mỗi 20 sản phẩm
                if (len(rating_sales_data) % 20 == 0):
                    save_data_to_csv(all_specs_data, rating_sales_data)
                    # Reset data tạm
                    all_specs_data = {}
                    rating_sales_data = []
                    # Khởi động lại trình duyệt để tránh lag
                    driver.quit()
                    driver = setup_driver(headless=True)
                    logger.info("Đã reset trình duyệt và lưu batch dữ liệu.")

            except Exception as e:
                logger.error(f"Lỗi tại sản phẩm {pid}: {e}")
                continue

        # Lưu lần cuối số còn lại
        save_data_to_csv(all_specs_data, rating_sales_data)

    finally:
        if driver:
            driver.quit()

def save_data_to_csv(specs_dict, rating_list):
    """Lưu dữ liệu vào các file CSV tương ứng (chế độ append)."""
    # 1. Lưu các bảng Specs (DIM)
    for group, rows in specs_dict.items():
        if not rows: continue
        filename = f"{group}.csv".replace("/", "_") # Tránh lỗi đường dẫn
        file_path = os.path.join(OUTPUT_DIM_DIR, filename)
        
        df_new = pd.DataFrame(rows)
        if os.path.exists(file_path):
            df_old = pd.read_csv(file_path)
            df_final = pd.concat([df_old, df_new], ignore_index=True).drop_duplicates(subset=['pid'], keep='last')
        else:
            df_final = df_new
        
        df_final.to_csv(file_path, index=False, encoding='utf-8-sig')

    # 2. Lưu Rating & Sales (FACT)
    if rating_list:
        fact_path = os.path.join(OUTPUT_FACT_DIR, "rating_sales.csv")
        df_new_fact = pd.DataFrame(rating_list)
        if os.path.exists(fact_path):
            df_old_fact = pd.read_csv(fact_path)
            df_final_fact = pd.concat([df_old_fact, df_new_fact], ignore_index=True).drop_duplicates(subset=['pid'], keep='last')
        else:
            df_final_fact = df_new_fact
        df_final_fact.to_csv(fact_path, index=False, encoding='utf-8-sig')

if __name__ == "__main__":
    start_time = time.time()
    logger.info("--- BẮT ĐẦU CRAWL CHI TIẾT SẢN PHẨM ---")
    crawl_details()
    duration = (time.time() - start_time) / 60
    logger.info(f"--- HOÀN THÀNH TRONG {duration:.2f} PHÚT ---")
