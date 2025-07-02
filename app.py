import aiohttp       # Library for asynchronous HTTP requests
import asyncio       # Library for writing asynchronous code
import aiofiles      # Library for asynchronous file operations (not used currently)
import zipfile       # Library for working with ZIP archives
import io            # Library for working with I/O streams
import pandas as pd  # Library for data manipulation and analysis
from datetime import datetime, timedelta  # Libraries for date manipulation
import os            # Library for OS operations like creating directories
import time          # Library for time-related functions
import logging       # Library for logging information and errors
import random        # Library for generating random numbers for sleep time

# Setup logging configuration to track program execution
logging.basicConfig(
    level=logging.INFO,     # Set minimum log level to INFO
    format='%(asctime)s [%(levelname)s] %(message)s',  # Format: timestamp, level, message
    handlers=[
        logging.FileHandler("bhavcopy_downloader.log"),  # Log to file
        logging.StreamHandler()                          # Also log to console
    ]
)

# NSE Holiday list - days when the stock market is closed
# Format: YYYYMMDD
NSE_HOLIDAYS = {
    "20250126", "20250815", "20251002"  # Republic Day, Independence Day, Gandhi Jayanti
    # Extend this set with all NSE holidays for your years
}

async def fetch_and_extract(session, date_str, semaphore):
    """
    Download and extract BhavCopy data for a specific date
    
    Args:
        session: aiohttp client session
        date_str: Date string in YYYYMMDD format
        semaphore: Controls concurrent downloads
        
    Returns:
        pandas DataFrame with the data or None if download fails
    """
    # Add random sleep time (3-7 seconds) to avoid getting blocked by NSE
    # This prevents too many requests in a short time which could trigger IP blocking
    sleep_time = random.uniform(3.0, 7.0)
    await asyncio.sleep(sleep_time)
    
    # Construct filenames according to NSE format
    # NSE follows a specific naming convention for their BhavCopy files
    base_filename = f"BhavCopy_NSE_CM_0_0_0_{date_str}_F_0000"
    zip_filename = f"{base_filename}.csv.zip"
    csv_filename = f"{base_filename}.csv"
    url = f"https://nsearchives.nseindia.com/content/cm/{zip_filename}"

    # Set user agent to avoid being blocked
    # This makes our request look like it's coming from a browser instead of a script
    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    # Use semaphore to limit concurrent downloads
    # This prevents overwhelming the server with too many simultaneous requests
    async with semaphore:
        try:
            # Log download attempt for tracking progress
            logging.info(f"Downloading: {zip_filename}")
            
            # Make HTTP request with timeout to prevent hanging on slow responses
            async with session.get(url, headers=headers, timeout=30) as response:
                if response.status != 200:
                    # If response is not successful (HTTP 200), log the error and return None
                    logging.warning(f"Failed {date_str}: Status {response.status}")
                    return None

                # Read response content (zip file) into memory
                content = await response.read()
                
                # Open the zip file in memory without saving to disk
                with zipfile.ZipFile(io.BytesIO(content)) as z:
                    if csv_filename not in z.namelist():
                        # If the CSV file isn't in the zip (possibly a holiday), log and return None
                        logging.warning(f"{date_str}: CSV not found in zip (possibly holiday).")
                        return None

                    # Extract and read the CSV file from the zip directly into pandas
                    with z.open(csv_filename) as csv_file:
                        df = pd.read_csv(csv_file)
                        df['Date'] = date_str  # Add date column to track which day this data is for
                        logging.info(f"Loaded {csv_filename}")
                        
                        # Create output directory if it doesn't exist
                        # This ensures we have a place to save our CSV files
                        os.makedirs("BhavCopy_Data", exist_ok=True)
                        
                        # Save individual file per date
                        # Each trading day gets its own CSV file
                        output_filename = f"BhavCopy_Data/BhavCopy_{date_str}.csv"
                        df.to_csv(output_filename, index=False)
                        logging.info(f"Saved daily BhavCopy data to {output_filename}")
                        
                        return df  # Return the DataFrame with the data for possible further processing

        except Exception as e:
            # Log any errors that occur during download or processing
            # This catches network errors, timeout errors, parsing errors, etc.
            logging.error(f"Error downloading {date_str}: {e}")
            return None

async def download_bhavcopy_year(year, month_tasks, concurrency=2):
    """
    Download BhavCopy data for a specific year
    
    Args:
        year: Year to download data for
        month_tasks: List of dates to download
        concurrency: Maximum number of concurrent downloads (default: 2)
    """
    # Create a connection pool with connection limits to avoid overwhelming the server
    # This helps manage resources and prevents too many open connections
    connector = aiohttp.TCPConnector(limit_per_host=concurrency)
    
    # Create an HTTP client session that will be reused for all requests
    # Reusing the session improves performance and resource usage
    async with aiohttp.ClientSession(connector=connector) as session:
        # Create a semaphore to limit concurrent downloads
        # This ensures we only have 'concurrency' number of downloads happening at once
        semaphore = asyncio.Semaphore(concurrency)
        
        # Download all files concurrently but limited by the semaphore
        # This runs multiple downloads in parallel for better performance
        results = await asyncio.gather(*[fetch_and_extract(session, date_str, semaphore) for date_str in month_tasks])

        # Filter out failed downloads (None values)
        # We only want to count successful downloads
        dataframes = [df for df in results if df is not None]

        if dataframes:
            # Log summary of downloaded files to track progress
            logging.info(f"Downloaded {len(dataframes)} daily BhavCopy files for year {year}")
        else:
            # If no data was downloaded, log a warning as this might indicate a problem
            logging.warning(f"No data downloaded for year {year}.")

def generate_date_list(year):
    """
    Generate a list of trading days for a given year
    
    Args:
        year: Year to generate dates for
        
    Returns:
        List of date strings in YYYYMMDD format
    """
    # Get the current date to avoid trying to download future data
    today = datetime.today()
    dates = []
    
    # Loop through all months in the year
    for month in range(1, 13):
        # Skip future months of the current year
        # We can't download data that doesn't exist yet
        if year == today.year and month > today.month:
            continue
            
        # Start at the first day of the month
        date = datetime(year, month, 1)
        
        # Loop through all days of the month
        while date.month == month:
            date_str = date.strftime("%Y%m%d")
            
            # Include only weekdays (0-4 is Monday to Friday) and non-holidays
            # Stock markets are closed on weekends and holidays
            if date.weekday() < 5 and date_str not in NSE_HOLIDAYS:
                dates.append(date_str)
                
            # Move to the next day
            date += timedelta(days=1)
            
    return dates

async def main(n_years):
    """
    Main function to download BhavCopy data for the last n years
    
    Args:
        n_years: Number of years of data to download
    """
    # Get the current date to calculate the starting year
    today = datetime.today()
    
    # Calculate the starting year based on how many years we want
    # For example, if n_years=2 and current year is 2025, we'll download 2024 and 2025
    start_year = today.year - n_years + 1

    # Loop through each year from start_year to the current year
    for year in range(start_year, today.year + 1):
        logging.info(f"Starting download for year {year}")
        
        # Generate list of trading days for this year
        # This gives us all the dates we need to download
        month_tasks = generate_date_list(year)
        
        # Download data for this year
        # This starts the download process for all trading days in the year
        await download_bhavcopy_year(year, month_tasks)

if __name__ == "__main__":
    n_years = 1  # Number of years to download (change as needed)
    asyncio.run(main(n_years))  # Start the async program with the event loop
