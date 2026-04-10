import pandas as pd
import requests
import os
import logging
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)

class ETLPipeline:

    def __init__(self):
        self.api_url = os.getenv("API_URL")
        self.db_url = os.getenv("DB_URL")
        self.df = None

    def extract_data(self):
        try:
            req = requests.get(self.api_url, timeout=5)
            req.raise_for_status()
            data = req.json()
            self.df = pd.DataFrame(data['products'])
            logging.info("Extract Berhasil")
        except Exception as e:
            logging.error(f"Extract Gagal: {e}")
            raise

    def cleaning_data(self):
        self.df['title'] = self.df['title'].astype(str).str.strip()
        self.df['price'] = pd.to_numeric(self.df['price'], errors='coerce')
        self.df['stock'] = pd.to_numeric(self.df['stock'], errors='coerce')
        logging.info("Cleaning Done")

    def validate_data(self):
        self.df = self.df[self.df['price'] > 0]
        self.df = self.df[self.df['stock'] >= 0]
        logging.info("Validate Done")

    def handling_data(self):
        self.df = self.df.drop_duplicates().reset_index(drop=True)
        logging.info("Handling Done")

    def drop_data(self):
        self.df = self.df.drop(columns=['thumbnail'])

        cols_to_convert = ['tags', 'dimensions', 'reviews', 'images', 'meta']
        for col in cols_to_convert:
            if col in self.df.columns:
                self.df[col] = self.df[col].astype(str)

        logging.info("Drop Data Done")

    def feature_engineering(self):
        self.df['inventory_value'] = self.df['price'] * self.df['stock']
        logging.info("Feature has been created")

    def summary(self):
        if self.df is None:
            raise ValueError("DataFrame masih kosong.")

        summary = self.df.groupby("category").agg({
            "title": "count",
            "inventory_value": "sum"
        })

        avg_price = self.df.groupby("category").agg({
            "price": "mean"
        })

        logging.info("Summary Done")
        return summary, avg_price

    def load_data(self):
        engine = create_engine(self.db_url)
        self.df.to_sql(name="products_clean", con=engine, if_exists="replace", index=False)
        logging.info("Load Data Done")

    def run(self):
        self.extract_data()
        self.cleaning_data()
        self.validate_data()
        self.drop_data()
        self.handling_data()
        self.feature_engineering()
        self.load_data()
        logging.info("Running Data Complete")
        return self.summary()

if __name__ == "__main__":
    pipeline = ETLPipeline()
    summary, avg = pipeline.run()

    print("Summary per Category")
    print(summary)
    print("Rata-rata Harga per Category")
    print(avg)