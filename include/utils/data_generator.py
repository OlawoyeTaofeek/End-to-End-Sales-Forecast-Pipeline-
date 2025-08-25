import datetime as dt
from datetime import timedelta, datetime
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Tuple, Dict, Any
import os
from dateutil.relativedelta import relativedelta, FR

import logging
import random
import sys
import pandas as pd
import holidays
from jmespath.ast import current_node

# Local imports
root = Path(__file__).resolve().parents[2]
sys.path.append(str(root))
from include.utils.logging_utils import setup_logging
from include.utils.extra_utils import *  # expects `stores`, `product_categories`, `events`

# Setup logging
log_dir = root / "logs"
log_dir.mkdir(exist_ok=True)
setup_logging(log_dir / "data_generation.log")


@dataclass
class RealisticSalesDataGenerator:
    """Generate realistic sales data considering stores, holidays, events, and promotions."""
    start_date: datetime
    end_date: datetime
    us_holiday: holidays.US = field(default_factory=holidays.US)

    def __post_init__(self) -> None:
        # Store and product setup
        self.stores = stores
        self.product_categories = product_categories
        # Flatten all products into one dictionary
        self.all_products = {
            product_id: {**info, "category": category}
            for category, products in self.product_categories.items()
            for product_id, info in products.items()
        }

    def get_day_of_week_factor(self, date: pd.Timestamp) -> float:
        """Get multiplier based on day of week
        """
        dow = date.dayofweek
        # Monday=0, Sunday=6
        dow_factors = [0.9, 0.85, 0.85, 0.9, 1.1, 1.3, 1.2]
        return dow_factors[dow]

    def generate_event_dates(self, major_events: List[Tuple]):
        """
        Generate event dates between start_date and end_date.

        Returns list of tuples: (event_name, event_datetime, duration, discount)
        """
        event_list = []
        for year in range(self.start_date.year, self.end_date.year + 1):
            for event_name, month, day, duration, discount in major_events:
                if event_name == "Black Friday":
                    # 4th Friday of November
                    november = dt.datetime(year, 11, 1)
                    event_datetime = november + relativedelta(weekday=FR(4))
                else:
                    try:
                        event_datetime = dt.datetime(year, month, day)
                    except Exception as e:
                        logging.debug(f"Skipping {event_name} for {year}: {e}")
                        continue

                if self.start_date <= event_datetime <= self.end_date:
                    event_list.append((event_name, event_datetime, duration, discount))
        return event_list

    def generate_promotions(self) -> pd.DataFrame:
        """
        Generate promotions for all major events and flash sales.
        """
        promotion: list[dict[str, datetime | Any] | dict[str, str | float | Any]] = []
        # Generate promotions for major events
        event_dates = self.generate_event_dates(events)
        for event_name, event_datetime, duration, discount in event_dates:
            logging.info(f"{event_name} on {event_datetime.date()} → {duration} days at {discount * 100:.0f}% off")

            for d in range(duration):
                promo_date = event_datetime + timedelta(days=d)
                if promo_date <= self.end_date:
                    promo_products = random.sample(list(self.all_products.keys()), k=random.randint(5, 20))
                    for product_id in promo_products:
                        promotion.append({
                            "product_id": product_id,
                            "date": promo_date,
                            "discount_percent": discount,
                            "promotion_type": event_name
                        })
        # Add random flash sales (5% of total days)
        total_days = (self.end_date - self.start_date).days
        n_flash_sales = max(1, int(total_days * 0.05))  # at least 1 flash sale
        flash_dates = pd.date_range(self.start_date, self.end_date, periods=n_flash_sales)

        for flash_date in flash_dates:
            promo_products = random.sample(list(self.all_products.keys()), k=min(random.randint(3, 8), len(self.all_products))) # To avoid error, Ensure k <= len(self.all_products)
            for product_id in promo_products:
                promotion.append({
                    "product_id": product_id,
                    "date": flash_date.to_pydatetime(),  # keep consistent datetime
                    "promotion_type": "Flash Sale",
                    "discount_percent": random.uniform(0.1, 0.3)
                })
        logging.info("Promotions generated successfully")
        return pd.DataFrame(promotion)

    def generate_store_events(self) -> pd.DataFrame:
        """
        Generate store-specific events (closures, renovations, grand openings).
        """
        store_events = []
        for store_id, store_info in self.stores.items():
            # === Closures ===
            n_closure = random.randint(2, 5)
            closure_dates = pd.date_range(self.start_date, self.end_date, periods=n_closure)
            logging.info(f"Closure dates {list(closure_dates)} generated for store {store_id}")
            logging.info(f"{len(list(closure_dates))} generated for closures and store_id {store_id}")

            for date in closure_dates:
                store_events.append({
                    "store_id": store_id,
                    "date": date,
                    "event_type": "closure",
                    "impact": -1.0  # 100% reduction
                })
            # === Renovations ===
            if random.random() < 0.3:  # 30% chance
                renovation_start = self.start_date + timedelta(days=random.randint(200, 700))
                renovation_duration = random.randint(7, 31)
                logging.info(f"Renovation of {renovation_duration} days generated for store {store_id}")

                for d in range(renovation_duration):
                    renovation_date = renovation_start + timedelta(days=d)
                    if renovation_date <= self.end_date:
                        store_events.append({
                            "store_id": store_id,
                            "date": renovation_date,
                            "event_type": "renovation",
                            "impact": -0.3  # 30% reduction
                        })
            # === Grand Openings ===
            if random.random() >= 0.6:  # 60% chance
                grand_opening_date = self.start_date + timedelta(days=random.randint(0, 60))
                grand_opening_duration = random.randint(3, 10)  # lasts a few days
                logging.info(f"Grand opening ({grand_opening_duration} days) generated for store {store_id}")

                for d in range(grand_opening_duration):
                    opening_date = grand_opening_date + timedelta(days=d)
                    if opening_date <= self.end_date:
                        store_events.append({
                            "store_id": store_id,
                            "date": opening_date,
                            "event_type": "grand_opening",
                            "impact": random.uniform(0.1, 0.5)  # 10%–50% boost
                        })
        # Final check & return
        df = pd.DataFrame(store_events)
        if df.empty:
            logging.warning("The store events DataFrame is empty — something went wrong.")
        else:
            logging.info(f"Store events DataFrame created with {len(df)} rows")
        return df

    def generate_sales_data(self, output_dir: str = "/tmp/sales_data") -> Dict[str, List[str]]:
        """
        Generate full sales-related datasets and save them as CSV.
        Returns dict of saved file paths.
        """
        os.makedirs(output_dir, exist_ok=True)
        # Generate Supplementary data
        promotions_df = self.generate_promotions()
        store_events_df = self.generate_store_events()

        file_paths = {
            "sales": [],
            "inventory": [],
            "customer_traffic": [],
            "promotions": [],
            "store_events": []
        }
        # Supplementary
        promotions_path = os.path.join(output_dir, "promotions/promotions.parquet")
        os.makedirs(os.path.dirname(promotions_path), exist_ok=True)
        promotions_df.to_parquet(promotions_path, index=False)
        file_paths['promotions'].append(promotions_path)

        store_events_path = os.path.join(output_dir, "store_events/store_events.parquet")
        os.makedirs(os.path.dirname(store_events_path), exist_ok=True)
        store_events_df.to_parquet(store_events_path, index=False)
        file_paths['store_events'].append(store_events_path)

        # Generate sales data by day
        current_date = self.start_date
        while current_date <= self.end_date:
            date_str = current_date.strftime("%Y-%m-%d")
            logging.info(f"Generating sales data for {date_str}")
            # daily sales data for all store
            daily_sales_data = []
            daily_traffic_data = []
            daily_inventory_data = []

            for store_id, store_info in self.stores.items():
                base_traffic = store_info['base_traffic']
                # date factors
                dow_factor = self.get_day_of_week_factor(current_date)

        return file_paths




if __name__ == "__main__":
    generator = RealisticSalesDataGenerator(
        start_date=dt.datetime(2021, 1, 1),
        end_date=dt.datetime(2023, 12, 31)
    )
    # Generate promotions DataFrame
    promotions = generator.generate_promotions()
    print(promotions.head())

    store_events_ = generator.generate_store_events()
    print(store_events_.head())
    print(promotions.shape)
    print(store_events_.shape)

    # # Generate all sales-related data and save CSVs
    # files = generator.generate_sales_data(output_dir="data_output")
    # print("Files saved:", files)

