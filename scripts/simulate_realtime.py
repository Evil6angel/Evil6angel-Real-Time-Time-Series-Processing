import csv
import time
import requests
import logging
from datetime import datetime, timedelta
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

LAST_INGESTED_FILE = "last_ingested.txt"

class BitcoinDataSimulator:
    def __init__(self, csv_file, telegraf_url, ingestion_interval=1):
        """
        Initialize the Bitcoin data simulator.

        Args:
            csv_file (str): Path to the CSV file containing Bitcoin data
            telegraf_url (str): URL for Telegraf's HTTP listener
            ingestion_interval (int): Time between data points in seconds
        """
        self.csv_file = csv_file
        self.telegraf_url = telegraf_url
        self.ingestion_interval = ingestion_interval
        self.last_ingested_timestamp = self.load_last_ingested_timestamp()

        # Set up time shift (from 2012 to today)
        self.original_start_time = datetime(2012, 1, 1, 10, 1, 0)
        self.current_start_time = datetime.utcnow()
        self.time_shift = self.current_start_time - self.original_start_time

    def load_last_ingested_timestamp(self):
        """Load the last ingested timestamp from a file."""
        if os.path.exists(LAST_INGESTED_FILE):
            try:
                with open(LAST_INGESTED_FILE, "r") as f:
                    timestamp = float(f.read().strip())
                    logger.info(f"📌 Resuming from last ingested timestamp: {timestamp}")
                    return timestamp
            except Exception as e:
                logger.warning(f"⚠️ Failed to read last ingested timestamp, starting from the beginning: {e}")
        return None

    def save_last_ingested_timestamp(self, timestamp):
        """Save the last successfully ingested timestamp to a file."""
        try:
            with open(LAST_INGESTED_FILE, "w") as f:
                f.write(str(timestamp))
        except Exception as e:
            logger.error(f"❌ Error saving last ingested timestamp: {e}")

    def send_data_to_telegraf(self, line_protocol):
        """Send data to Telegraf using HTTP API."""
        headers = {"Content-Type": "text/plain"}
        max_retries = 3
        retry_delay = 2

        logger.info(f"📤 Sending data: {line_protocol}")

        for attempt in range(max_retries):
            try:
                response = requests.post(self.telegraf_url, data=line_protocol, headers=headers)
                logger.info(f"📡 Telegraf response: {response.status_code} {response.text}")

                if response.status_code == 204:
                    logger.info("✅ Successfully sent data point")
                    return True
                else:
                    logger.warning(f"⚠️ Failed to send data: {response.status_code} - {response.text}")

            except requests.exceptions.ConnectionError as e:
                logger.error(f"❌ Connection Error: {e}")
            except requests.exceptions.RequestException as e:
                logger.error(f"❌ Error sending data: {e}")

            if attempt < max_retries - 1:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2

        return False

    def create_line_protocol(self, row):
        """Create InfluxDB line protocol string from row data."""
        try:
            original_timestamp = float(row["Timestamp"])

            if self.last_ingested_timestamp and original_timestamp <= self.last_ingested_timestamp:
                return None  # Skip already processed data

            adjusted_timestamp = datetime.fromtimestamp(original_timestamp) + self.time_shift
            timestamp_ns = int(adjusted_timestamp.timestamp() * 1e9)

            logger.info(f"Original timestamp: {original_timestamp}, Adjusted timestamp: {adjusted_timestamp}")

            if not all(k in row for k in ["Open", "High", "Low", "Close", "Volume"]):
                logger.warning(f"⚠️ Missing fields in row: {row}")
                return None

            base_metrics = f'Open={float(row["Open"])},' \
                         f'High={float(row["High"])},' \
                         f'Low={float(row["Low"])},' \
                         f'Close={float(row["Close"])},' \
                         f'Volume={float(row["Volume"])}'

            return f"bitcoin,source=csv {base_metrics} {timestamp_ns}", original_timestamp
        except (KeyError, ValueError) as e:
            logger.error(f"❌ Error creating line protocol: {e}")
            return None, None

    def run(self):
        """Run the simulation by reading CSV file and sending data to Telegraf."""
        try:
            with open(self.csv_file, "r") as file:
                reader = csv.DictReader(file)
                for row in reader:
                    try:
                        line_protocol, timestamp = self.create_line_protocol(row)
                        if line_protocol:
                            if self.send_data_to_telegraf(line_protocol):
                                self.save_last_ingested_timestamp(timestamp)
                            else:
                                logger.warning("⚠️ Failed to send data after all retries")
                        time.sleep(self.ingestion_interval)
                    except Exception as e:
                        logger.error(f"❌ Error processing row: {e}")
                        continue
        except FileNotFoundError:
            logger.error(f"❌ CSV file not found: {self.csv_file}")
        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}")


def main():
    CSV_FILE = "./dataset/bitcoin_processed.csv"
    TELEGRAF_URL = "http://localhost:8186/write"
    INGESTION_INTERVAL = 1

    simulator = BitcoinDataSimulator(
        csv_file=CSV_FILE,
        telegraf_url=TELEGRAF_URL,
        ingestion_interval=INGESTION_INTERVAL
    )

    logger.info("🚀 Starting Bitcoin data simulation...")
    simulator.run()


if __name__ == "__main__":
    main()
