import csv
import time
import requests
from datetime import datetime
from collections import deque
import statistics
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BitcoinDataProcessor:
    def __init__(self, input_file, telegraf_url):
        self.input_file = input_file
        self.telegraf_url = telegraf_url
        self.batch_size = 1000
        self.processed_count = 0
        self.error_count = 0
        
        # Windows for calculating metrics
        self.window_size = 5
        self.close_prices = deque(maxlen=self.window_size)
        self.volumes = deque(maxlen=self.window_size)
        self.high_prices = deque(maxlen=self.window_size)
        self.low_prices = deque(maxlen=self.window_size)

    def clean_numeric(self, value):
        """Clean and convert numeric values, handling various formats"""
        try:
            value = value.strip()
            if not value or value.lower() == 'nan':
                return 0.0
            return float(value)
        except (ValueError, AttributeError):
            self.error_count += 1
            return 0.0

    def calculate_metrics(self):
        """Calculate various technical indicators and metrics"""
        if len(self.close_prices) < self.window_size:
            return None

        try:
            # Calculate SMA
            sma = statistics.mean(self.close_prices)
            
            # Calculate volatility using price ranges
            high_low_range = max(self.high_prices) - min(self.low_prices)
            avg_price = statistics.mean(self.close_prices)
            volatility = (high_low_range / avg_price) * 100 if avg_price > 0 else 0
            
            # Calculate VWAP
            price_volume_sum = sum(p * v for p, v in zip(self.close_prices, self.volumes))
            volume_sum = sum(self.volumes)
            vwap = price_volume_sum / volume_sum if volume_sum > 0 else None
            
            # Calculate additional batch metrics
            price_std = statistics.stdev(self.close_prices) if len(self.close_prices) > 1 else 0
            price_momentum = self.close_prices[-1] - self.close_prices[0] if len(self.close_prices) > 1 else 0
            
            return {
                'sma': round(sma, 2),
                'volatility': round(volatility, 2),
                'vwap': round(vwap, 2) if vwap is not None else None,
                'std_dev': round(price_std, 2),
                'momentum': round(price_momentum, 2)
            }
        except Exception as e:
            logger.error(f"Error calculating metrics: {e}")
            return None

    def process_line(self, row):
        """Convert a CSV row to InfluxDB line protocol format with metrics"""
        try:
            # Convert timestamp to integer nanoseconds
            timestamp = int(float(row['Timestamp'])) * 1000000000

            # Clean and convert all numeric fields
            open_price = self.clean_numeric(row["Open"])
            high_price = self.clean_numeric(row["High"])
            low_price = self.clean_numeric(row["Low"])
            close_price = self.clean_numeric(row["Close"])
            volume = self.clean_numeric(row["Volume"])

            # Update sliding windows
            self.close_prices.append(close_price)
            self.volumes.append(volume)
            self.high_prices.append(high_price)
            self.low_prices.append(low_price)

            # Calculate metrics
            metrics = self.calculate_metrics()
            
            # Base measurements
            line_protocol = (
                f'bitcoin,source=historical,market=default '
                f'open={open_price},high={high_price},'
                f'low={low_price},close={close_price},'
                f'volume={volume}'
            )

            # Add calculated metrics if available
            if metrics:
                line_protocol += (
                    f',sma={metrics["sma"]},'
                    f'volatility={metrics["volatility"]}'
                )
                if metrics["vwap"]:
                    line_protocol += f',vwap={metrics["vwap"]}'
                line_protocol += (
                    f',std_dev={metrics["std_dev"]},'
                    f'momentum={metrics["momentum"]}'
                )

            # Add timestamp
            line_protocol += f' {timestamp}'
            
            return line_protocol
        except Exception as e:
            logger.error(f"Error processing row: {row}")
            logger.error(f"Error details: {str(e)}")
            self.error_count += 1
            return None

    def send_batch(self, batch):
        """Send a batch of data points to Telegraf"""
        if not batch:
            return

        data = '\n'.join(batch)
        max_retries = 3
        retry_delay = 2

        for attempt in range(max_retries):
            try:
                response = requests.post(
                    self.telegraf_url,
                    data=data,
                    headers={'Content-Type': 'text/plain'}
                )
                if response.status_code == 204:
                    self.processed_count += len(batch)
                    logger.info(f"✅ Successfully sent {len(batch)} points. Total: {self.processed_count}")
                    return
                else:
                    logger.warning(f"❌ Failed to send batch: {response.status_code} - {response.text}")
            except Exception as e:
                logger.error(f"❌ Error sending batch: {str(e)}")
            
            if attempt < max_retries - 1:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2

    def process_file(self):
        """Process the entire CSV file"""
        batch = []
        start_time = time.time()
        row_count = 0

        logger.info(f"Starting to process file: {self.input_file}")
        
        try:
            with open(self.input_file, 'r') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    row_count += 1
                    line = self.process_line(row)
                    
                    if line:
                        batch.append(line)

                    if len(batch) >= self.batch_size:
                        self.send_batch(batch)
                        batch = []
                        time.sleep(0.1)  # Rate limiting

                # Send any remaining data
                if batch:
                    self.send_batch(batch)

            duration = time.time() - start_time
            logger.info("\nProcessing Summary:")
            logger.info(f"Total rows read: {row_count}")
            logger.info(f"Successfully processed points: {self.processed_count}")
            logger.info(f"Errors encountered: {self.error_count}")
            logger.info(f"Duration: {duration:.2f} seconds")
            logger.info(f"Processing rate: {self.processed_count/duration:.2f} points/second")
            
        except FileNotFoundError:
            logger.error(f"Error: Could not find file {self.input_file}")
        except Exception as e:
            logger.error(f"Error processing file: {str(e)}")

if __name__ == "__main__":
    # Print current working directory to help with file path issues
    import os
    logger.info(f"Current working directory: {os.getcwd()}")
    
    processor = BitcoinDataProcessor(
        input_file="./dataset/bitcoin_processed.csv",
        telegraf_url="http://localhost:8186/write"
    )
    processor.process_file()