import os
import pandas as pd
import logging
from sqlalchemy import create_engine
from dotenv import load_dotenv

# --- CẤU HÌNH LOGGING ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- LOAD CẤU HÌNH TỪ .ENV ---
dotenv_path = os.path.join("databases", ".env")
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)
    logger.info(f"Đã tải cấu hình từ {dotenv_path}")
else:
    logger.error(f"Không tìm thấy file {dotenv_path}")

# Lấy thông tin từ biến môi trường
DB_USER = os.getenv("MYSQL_USER", "crawler_user")
DB_PASS = os.getenv("MYSQL_PASSWORD", "crawler_password")
DB_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
DB_PORT = os.getenv("MYSQL_PORT", "3306")
DB_NAME = os.getenv("MYSQL_DATABASE", "laptop_db")

# --- KHỞI TẠO KẾT NỐI ---
try:
    connection_url = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(connection_url)
    logger.info(f"Đang kết nối tới database: {DB_NAME} tại {DB_HOST}")
except Exception as e:
    logger.error(f"Lỗi khởi tạo kết nối: {e}")
    exit(1)

def load_csv_to_mysql(directory):
    """Duyệt file trong thư mục và đẩy vào MySQL."""
    if not os.path.exists(directory):
        logger.warning(f"Thư mục {directory} không tồn tại. Bỏ qua.")
        return

    for filename in os.listdir(directory):
        if filename.endswith(".csv"):
            file_path = os.path.join(directory, filename)
            table_name = filename.replace(".csv", "").replace("_clean", "")
            
            try:
                logger.info(f"Đang đọc file: {filename}...")
                df = pd.read_csv(file_path)
                df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
                logger.info(f"Đã load {len(df)} dòng vào bảng '{table_name}'")
            except Exception as e:
                logger.error(f"Lỗi khi load file {filename}: {e}")

if __name__ == "__main__":
    logger.info("--- BẮT ĐẦU LOAD DỮ LIỆU VÀO MYSQL ---")
    
    # Load từ thư mục dim
    load_csv_to_mysql("data/processing/dim")
    
    # Load từ thư mục fact
    load_csv_to_mysql("data/processing/fact")

    logger.info("--- HOÀN THÀNH PIPELINE LOAD DỮ LIỆU ---")
