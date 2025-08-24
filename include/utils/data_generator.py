from datetime import datetime, timedelta
import os
from dataclasses import dataclass, field
import sys
import random
from typing import Dict, List, Tuple, Optional
import pandas as pd
import numpy as np
from pathlib import Path
root = Path(__file__).resolve().parents[2]
sys.path.append(str(root))
import holidays
import logging
from include.utils.logging_utils import setup_logging

print(root)
# Create logs folder if it doesn't exist
log_dir = root / "logs"
log_dir.mkdir(exist_ok=True)
setup_logging(log_dir / "data_generation.log")

@dataclass
class RealisticSalesDataGenerator:
    """A complicated class to generate Sales data considering stores and factors like holiday, store renovation and more"""
    start_date: datetime
    end_date: datetime
    us_holiday: holidays.US = field(default_factory=holidays.US)

    def __post_init__(self) -> None:
        # Store configurations
        self.stores = {
            'store_001': {'location': 'New York', 'size': 'large', 'base_traffic': 1000},
            'store_002': {'location': 'Los Angeles', 'size': 'large', 'base_traffic': 950},
            'store_003': {'location': 'Chicago', 'size': 'medium', 'base_traffic': 700},
            'store_004': {'location': 'Houston', 'size': 'medium', 'base_traffic': 650},
            'store_005': {'location': 'Phoenix', 'size': 'small', 'base_traffic': 400},
            'store_006': {'location': 'Philadelphia', 'size': 'medium', 'base_traffic': 600},
            'store_007': {'location': 'San Antonio', 'size': 'small', 'base_traffic': 350},
            'store_008': {'location': 'San Diego', 'size': 'medium', 'base_traffic': 550},
            'store_009': {'location': 'Dallas', 'size': 'large', 'base_traffic': 850},
            'store_010': {'location': 'Miami', 'size': 'medium', 'base_traffic': 600}
        }

        # Product categories and items
        self.product_categories = {
            'Electronics': {
                'ELEC_001': {'name': 'Smartphone', 'price': 699, 'margin': 0.15, 'seasonality': 'holiday'},
                'ELEC_002': {'name': 'Laptop', 'price': 999, 'margin': 0.12, 'seasonality': 'back_to_school'},
                'ELEC_003': {'name': 'Headphones', 'price': 199, 'margin': 0.25, 'seasonality': 'holiday'},
                'ELEC_004': {'name': 'Tablet', 'price': 499, 'margin': 0.18, 'seasonality': 'holiday'},
                'ELEC_005': {'name': 'Smart Watch', 'price': 299, 'margin': 0.20, 'seasonality': 'fitness'},
                'ELEC_006': {'name': 'Monitor', 'price': 1099, 'margin': 0.25, 'seasonality': 'back_to_school'},
                'ELEC_007': {'name': 'USB Drive', 'price': 99, 'margin': 0.15, 'seasonality': 'holiday'},
            },
            'Clothing': {
                'CLTH_001': {'name': 'T-Shirt', 'price': 29, 'margin': 0.50, 'seasonality': 'summer'},
                'CLTH_002': {'name': 'Jeans', 'price': 79, 'margin': 0.45, 'seasonality': 'all_year'},
                'CLTH_003': {'name': 'Jacket', 'price': 149, 'margin': 0.40, 'seasonality': 'winter'},
                'CLTH_004': {'name': 'Dress', 'price': 89, 'margin': 0.48, 'seasonality': 'summer'},
                'CLTH_005': {'name': 'Shoes', 'price': 119, 'margin': 0.42, 'seasonality': 'all_year'},
                'CLTH_006': {'name': 'Sweater', 'price': 69, 'margin': 0.44, 'seasonality': 'winter'},
                'CLTH_007': {'name': 'Shorts', 'price': 39, 'margin': 0.47, 'seasonality': 'summer'}
            },
            'Home': {
                'HOME_001': {'name': 'Coffee Maker', 'price': 79, 'margin': 0.30, 'seasonality': 'holiday'},
                'HOME_002': {'name': 'Blender', 'price': 49, 'margin': 0.35, 'seasonality': 'summer'},
                'HOME_003': {'name': 'Vacuum Cleaner', 'price': 199, 'margin': 0.28, 'seasonality': 'spring'},
                'HOME_004': {'name': 'Air Purifier', 'price': 149, 'margin': 0.32, 'seasonality': 'all_year'},
                'HOME_005': {'name': 'Toaster', 'price': 39, 'margin': 0.40, 'seasonality': 'holiday'},
                'HOME_006': {'name': 'Water Dispenser', 'price': 59, 'margin': 0.20, 'seasonality': 'all_year'},
                'HOME_007': {'name': 'Gas Cooker', 'price': 199, 'margin': 0.25, 'seasonality': 'winter'}
            },
            'Sports': {
                'SPRT_001': {'name': 'Yoga Mat', 'price': 29, 'margin': 0.55, 'seasonality': 'fitness'},
                'SPRT_002': {'name': 'Dumbbells', 'price': 49, 'margin': 0.45, 'seasonality': 'fitness'},
                'SPRT_003': {'name': 'Running Shoes', 'price': 129, 'margin': 0.38, 'seasonality': 'spring'},
                'SPRT_004': {'name': 'Bicycle', 'price': 399, 'margin': 0.25, 'seasonality': 'summer'},
                'SPRT_005': {'name': 'Tennis Racket', 'price': 89, 'margin': 0.35, 'seasonality': 'summer'},
                'SPRT_006': {'name': 'Basketball', 'price': 35, 'margin': 0.50, 'seasonality': 'all-season'},
                'SPRT_007': {'name': 'Ski Jacket', 'price': 159, 'margin': 0.40, 'seasonality': 'winter'}
            }
        }

        self.all_products = {}
        for category, products in self.product_categories.items():
            for product_id, product_info in products.items():
                self.all_products[product_id] = {
                    **product_info,
                    "category": category
                }

    def generate_promotions(self) -> pd.DataFrame:
        promotions = []
        major_events = [
            ("Black Friday", 11, 4, 5, 0.25), # 4th FRIDAY of November, 5 days, 25% off
            ("Cyber Monday", 11, 4, 2, 0.20), # Monday after Black Friday, 2 days 20% off
            ("Christmas Sale", 12, 15, 10, 0.15), # 15th December, 10 days 15% off
            ("New Year Sale", 1, 1, 7, 0.20), # 1st of January fo 7 days, 20% off
            ("President Days", 2, 15, 3, 0.15), # 15th February, 3 days, 15% off
            ('Memorial Day Sale', 5, 25, 3, 0.20), # Last Monday of May, 3 days, 20% off
            ("Independence Day sale", 8, 15, 3, 0.20),
            ('July 4th Sale', 7, 1, 5, 0.20), # 4th July, 3 days, 20% off
            ('Labor Day', 9, 1, 3, 0.15), # First Monday of September, 3 days, 15% off
            ('Back to School', 8, 1, 14, 0.10),
            ("Halloween Sale", 10, 31, 5, 0.25) # Halloween 5 days, 25% off
        ]
        current_date = self.start_date
        while current_date <= self.end_date:
            year = current_date.year
            for event_name, month, day, duration, discount in major_events:
                if event_name == 'Black Friday':
                    # Calculate 25th Thursday of November, then add 1 for Friday
                    november = pd.Timestamp(year=year, month=11, day=1)
                    thursdays = pd.date_range(november, november + timedelta(days=30), freq='W-THU')
                    event_date = thursdays[3] + timedelta(days=1)
                else:
                    try:
                        event_date = pd.Timestamp(year, month, day)
                    except Exception as e:
                        logging.debug(f"Exception with: {e}")
                        continue
                # Only add promotions if the event date is inside your chosen date range.
                if self.start_date <= event_date <= self.end_date:
                    # Loops over each day of the event (duration).
                    for d in range(duration):
                        promo_date = event_date + timedelta(days=d)
                        if promo_date <= self.end_date:
                            promo_product = random.sample(list(self.all_products.keys()),
                                                    k = random.randint(5, 15))
                            for product_id in promo_product:
                                promotions.append({
                                    "product_id": product_id,
                                    "date": promo_date,
                                    "discount_percent": discount,
                                    "promotion_type": event_name
                                })
            current_date = current_date + pd.DateOffset(years=1)
        # Add random flash sales
        n_flash_sales = int((self.end_date - self.start_date).days * 0.05)  # 5% of days
        flash_dates = pd.date_range(self.start_date, self.end_date, periods=n_flash_sales)

        for date in flash_dates:
            promo_products = random.sample(list(self.all_products.keys()), k=random.randint(3, 8))
            for product_id in promo_products:
                promotions.append({
                    'date': date,
                    'product_id': product_id,
                    'promotion_type': 'Flash Sale',
                    'discount_percent': random.uniform(0.1, 0.3)
                })
        logging.info("Data Loaded Successfully into Pandas Dataframe")
        return pd.DataFrame(promotions)


    def generate_store_events(self) -> pd.DataFrame:
        ...

    def generate_sales_data(self, output_dir: str = "/tmp/sales_data", num_days: int = 365) -> Dict[str, List[str]]:
        os.makedirs(output_dir, exist_ok=True)

        ## Generating the supplementary data
        promotions_df = self.generate_promotions()
        store_events_df = self.generate_store_events()



