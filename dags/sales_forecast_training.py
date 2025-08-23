import pandas as pd
import seaborn as sns
from datetime import datetime, timedelta
# import numpy as np
from airflow.decorators import dag, task
import sys
from include.utils.data_generator import RealisticSalesDataGenerator
import logging
from include.utils.logging_utils import setup_logging
from pathlib import Path
root = Path(__file__).resolve().parents[2]
sys.path.append(str(root))

# Create logs folder if it doesn't exist
log_dir = root / "logs"
log_dir.mkdir(exist_ok=True)
setup_logging(f"{log_dir}/sales_forecast_training.log")

sys.path.append("/usr/local/airflow/include")
default_args = {
    "owner": "olawoyetaofeek",
    "depend_on_past": False,
    "start_date": datetime(2025, 8, 22),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "catchup": False,
    "schedule": "@weekly",
}

@dag(
    "machine-learning-pipeline",
    default_args=default_args,
    description="A Sales Forecasting end to end Pipeline",
    tags=['ml-pipeline', 'training', 'salas-forecast', 'sales']
)
def sales_forecast_training():
    @task
    def extract_data_task():
        data_output_dir = "/tmp/sales_data"

        generator = RealisticSalesDataGenerator(
            start_date = datetime.strptime("2021-01-01", "%Y-%m-%d"),
            end_date= datetime.now()
        )
        logging.info("Generating realistic sales data.....")
        file_path = generator.generate_sales_data(output_dir=data_output_dir)




