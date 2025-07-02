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
    # Construct filenames according to NSE format
    base_filename = f"BhavCopy_NSE_CM_0_0_0_{date_str}_F_0000"
    zip_filename = f"{base_filename}.csv.zip"
    csv_filename = f"{base_filename}.csv"
    url = f"https://nsearchives.nseindia.com/content/cm/{zip_filename}"

    # Set user agent to avoid being blocked
    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    # Use semaphore to limit concurrent downloads
    async with semaphore:
        try:
            # Log download attempt
            logging.info(f"Downloading: {zip_filename}")
            
            # Make HTTP request with timeout
            async with session.get(url, headers=headers, timeout=30) as response:
                if response.status != 200:
                    # If response is not successful, log and return None
                    logging.warning(f"Failed {date_str}: Status {response.status}")
                    return None

                # Read response content (zip file)
                content = await response.read()
                
                # Open the zip file in memory
                with zipfile.ZipFile(io.BytesIO(content)) as z:
                    if csv_filename not in z.namelist():
                        # If the CSV file isn't in the zip (possibly a holiday), log and return None
                        logging.warning(f"{date_str}: CSV not found in zip (possibly holiday).")
                        return None

                    # Extract and read the CSV file from the zip
                    with z.open(csv_filename) as csv_file:
                        df = pd.read_csv(csv_file)
                        df['Date'] = date_str  # Add date column to the data
                        logging.info(f"Loaded {csv_filename}")
                        return df  # Return the DataFrame with the data

        except Exception as e:
            # Log any errors that occur during download or processing
            logging.error(f"Error downloading {date_str}: {e}")
            return None

async def download_bhavcopy_year(year, month_tasks, concurrency=5):
    """
    Download BhavCopy data for a specific year
    
    Args:
        year: Year to download data for
        month_tasks: List of dates to download
        concurrency: Maximum number of concurrent downloads
    """
    # Create a connection pool with connection limits to avoid overwhelming the server
    connector = aiohttp.TCPConnector(limit_per_host=concurrency)
    
    # Create an HTTP client session
    async with aiohttp.ClientSession(connector=connector) as session:
        # Create a semaphore to limit concurrent downloads
        semaphore = asyncio.Semaphore(concurrency)
        
        # Download all files concurrently
        results = await asyncio.gather(*[fetch_and_extract(session, date_str, semaphore) for date_str in month_tasks])

        # Filter out failed downloads (None values)
        dataframes = [df for df in results if df is not None]

        if dataframes:
            # If we have data, combine all DataFrames and save to CSV
            final_df = pd.concat(dataframes, ignore_index=True)
            
            # Create output directory if it doesn't exist
            os.makedirs("BhavCopy_Data", exist_ok=True)
            
            # Construct output filename and save the data
            output_filename = f"BhavCopy_Data/BhavCopy_{year}.csv"
            final_df.to_csv(output_filename, index=False)
            logging.info(f"Saved yearly BhavCopy data to {output_filename}")
        else:
            # If no data was downloaded, log a warning
            logging.warning(f"No data downloaded for year {year}.")

def generate_date_list(year):
    """
    Generate a list of trading days for a given year
    
    Args:
        year: Year to generate dates for
        
    Returns:
        List of date strings in YYYYMMDD format
    """
    # Get the current date
    today = datetime.today()
    dates = []
    
    # Loop through all months
    for month in range(1, 13):
        # Skip future months of the current year
        if year == today.year and month > today.month:
            continue
            
        # Start at the first day of the month
        date = datetime(year, month, 1)
        
        # Loop through all days of the month
        while date.month == month:
            date_str = date.strftime("%Y%m%d")
            
            # Include only weekdays (0-4 is Monday to Friday) and non-holidays
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
    # Get the current date
    today = datetime.today()
    
    # Calculate the starting year based on how many years we want
    start_year = today.year - n_years + 1

    # Loop through each year
    for year in range(start_year, today.year + 1):
        logging.info(f"Starting download for year {year}")
        
        # Generate list of trading days for this year
        month_tasks = generate_date_list(year)
        
        # Download data for this year
        await download_bhavcopy_year(year, month_tasks)

if __name__ == "__main__":
    n_years = 2  # Number of years to download (change as needed)
    asyncio.run(main(n_years))  # Start the async program
