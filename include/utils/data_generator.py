import datetime as dt
from datetime import timedelta
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Tuple, Dict
import os
from dateutil.relativedelta import relativedelta, FR

import logging
import random
import sys
import pandas as pd
import holidays

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
        promotions = []

        # Generate promotions for major events
        event_dates = self.generate_event_dates(events)

        for event_name, event_datetime, duration, discount in event_dates:
            logging.info(f"{event_name} on {event_datetime.date()} → {duration} days at {discount * 100:.0f}% off")

            for d in range(duration):
                promo_date = event_datetime + timedelta(days=d)
                if promo_date <= self.end_date:
                    promo_products = random.sample(list(self.all_products.keys()), k=random.randint(5, 15))
                    for product_id in promo_products:
                        promotions.append({
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
            promo_products = random.sample(list(self.all_products.keys()), k=random.randint(3, 8))
            for product_id in promo_products:
                promotions.append({
                    "product_id": product_id,
                    "date": flash_date.to_pydatetime(),  # keep consistent datetime
                    "promotion_type": "Flash Sale",
                    "discount_percent": random.uniform(0.1, 0.3)
                })

        logging.info("Promotions generated successfully")
        return pd.DataFrame(promotions)

    def generate_store_events(self) -> pd.DataFrame:
        """
        Placeholder for store-specific events (e.g., renovations, grand openings).
        Returns an empty DataFrame for now.
        """
        return pd.DataFrame(columns=["store_id", "date", "event_type"])

    def generate_sales_data(self, output_dir: str = "/tmp/sales_data") -> Dict[str, str]:
        """
        Generate full sales-related datasets and save them as CSV.
        Returns dict of saved file paths.
        """
        os.makedirs(output_dir, exist_ok=True)

        # Generate data
        promotions_df = self.generate_promotions()
        store_events_df = self.generate_store_events()

        # Save to disk
        promotions_path = os.path.join(output_dir, "promotions.csv")
        store_events_path = os.path.join(output_dir, "store_events.csv")

        promotions_df.to_csv(promotions_path, index=False)
        store_events_df.to_csv(store_events_path, index=False)

        logging.info(f"Sales data saved to {output_dir}")
        return {"promotions": promotions_path, "store_events": store_events_path}



if __name__ == "__main__":
    generator = RealisticSalesDataGenerator(
        start_date=dt.datetime(2021, 1, 1),
        end_date=dt.datetime(2023, 12, 31)
    )

    # Generate promotions DataFrame
    promotions = generator.generate_promotions()
    print(promotions.head())

    # Generate all sales-related data and save CSVs
    files = generator.generate_sales_data(output_dir="data_output")
    print("Files saved:", files)

