# ETL Pipeline - DummyJSON Products

ETL Pipeline sederhana yang mengambil data dari DummyJSON API, melakukan transformasi data, dan menyimpannya ke PostgreSQL.

## Teknologi
- Python
- Pandas
- Requests
- SQLAlchemy
- PostgreSQL
- Python-dotenv

## Setup

1. Clone repo ini
2. Install dependencies:
   pip install -r requirements.txt

3. Buat file .env (lihat .env.example):
   API_URL=https://dummyjson.com/products
   DB_URL=postgresql://postgres@localhost:5432/latihan_etl

4. Jalankan:
   python main.py

## Alur ETL

1. Extract - Ambil data dari DummyJSON API
2. Cleaning - Standarisasi format data
3. Validate - Filter data yang tidak valid
4. Handling - Hapus duplikat
5. Drop - Hapus kolom yang tidak diperlukan
6. Feature Engineering - Tambah kolom baru
7. Load - Simpan ke PostgreSQL