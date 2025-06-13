import pandas as pd
import os
from datetime import datetime

# Đường dẫn
RAW_PATH = 'data/raw/Sleep_health_and_lifestyle_dataset.csv'
CLEAN_PATH = f'data/clean/cleaned_sleep_data_{datetime.now().strftime("%Y%m%d")}.parquet'

# Bước 1: Load dữ liệu
df = pd.read_csv(RAW_PATH)

# Bước 2: Làm sạch sơ bộ
df.dropna(inplace=True)
df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
df = df.drop_duplicates()

# Thêm xử lý bạn đã làm trong EDA
df['gender'] = df['gender'].str.strip().str.capitalize()
df['age'] = df['age'].astype(int)

# Kiểm tra logic dữ liệu
assert df['age'].between(0, 120).all()
assert df['sleep_duration'].notnull().all()

# Bước 3: Lưu file parquet
df.to_parquet(CLEAN_PATH, index=False)

print(f"[✔] Dữ liệu sạch đã được lưu tại: {CLEAN_PATH}")
