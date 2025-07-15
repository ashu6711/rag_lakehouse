from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, date
import json
import requests
from bs4 import BeautifulSoup
import sys
from lxml import html
import time
import logging
from urllib.parse import urljoin, urlparse
import random
import uuid
sys.path.insert(0,'./')

from helper.minio_manager import MinIOHelper
from config.config import BASE_URL, BOOK_DETAILS_XPATH, BUCKET, DELAY, MAX_PAGES, USER_AGENTS, MIN_IO_URL, MIN_IO_ACCESS_KEY, MIN_IO_SECRET_KEY


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class ScrapeBookDetails:

    def __init__(self):
        self.base_url = BASE_URL
        self.max_pages = MAX_PAGES
        self.delay = DELAY
        self.today = date.today()
        self.run_id = str(uuid.uuid4())
        self.upload_loc = f'{self.run_id}/raw/{self.today}'
        

    def collect_book_urls(self):
        """
        Collect all book URLs from the catalog pages.
        
        Args:
            self.base_url: Base URL of the bookstore
            self.max_pages: Maximum number of pages to scrape
            delay: Delay between requests in seconds
            
        Returns:
            List of book URLs
        """

        book_urls = []
        session = requests.Session()
        request_count = 0
        
        def rotate_user_agent():
            """Rotate user agent every 5 requests"""
            nonlocal request_count
            if request_count % 5 == 0:
                current_user_agent = USER_AGENTS[request_count // 5 % len(USER_AGENTS)]
                session.headers.update({'User-Agent': current_user_agent})
                logging.info(f"Request #{request_count + 1}: Using User-Agent: {current_user_agent[:50]}...")
            request_count += 1
        
        try:
            for page_num in range(1, self.max_pages + 1):
                try:
                    # Rotate user agent
                    rotate_user_agent()
                    
                    # Construct page URL
                    if page_num == 1:
                        page_url = f"{self.base_url}/catalogue/page-{page_num}.html"
                    else:
                        page_url = f"{self.base_url}/catalogue/page-{page_num}.html"
                    
                    logging.info(f"Scraping page {page_num}: {page_url}")
                    
                    # Make request
                    response = session.get(page_url, timeout=15)
                    response.raise_for_status()
                    
                    # Parse HTML
                    soup = BeautifulSoup(response.content, 'html.parser')
                    
                    # Find all book containers using CSS selectors
                    books = soup.find_all('article', class_='product_pod')
                    
                    if not books:
                        logging.warning(f"No books found on page {page_num}")
                        continue
                    
                    # Extract book URLs
                    page_book_urls = []
                    for book in books:
                        try:
                            # Find the book link
                            title_elem = book.find('h3')
                            if title_elem:
                                link_elem = title_elem.find('a')
                                if link_elem and link_elem.get('href'):
                                    # Convert relative URL to absolute
                                    book_url = urljoin(f'{self.base_url}/catalogue/', link_elem.get('href'))
                                    page_book_urls.append(book_url)
                        except Exception as e:
                            logging.error(f"Error extracting book URL: {e}")
                            continue
                    
                    book_urls.extend(page_book_urls)
                    logging.info(f"Found {len(page_book_urls)} book URLs on page {page_num}")
                    
                    # Add delay between requests
                    time.sleep(self.delay)
                    
                except requests.exceptions.RequestException as e:
                    logging.error(f"Error scraping page {page_num}: {e}")
                    continue
                except Exception as e:
                    logging.error(f"Unexpected error on page {page_num}: {e}")
                    continue
        
        except Exception as e:
            logging.error(f"Fatal error in collect_book_urls: {e}")
        
        logging.info(f"Total book URLs collected: {len(book_urls)}")
        return book_urls

    def scrape_book_details(self, counter, book_url):
        """
        Scrape detailed information from a book's detail page using XPath.
        
        Args:
            book_url: URL of the book detail page
            delay: Delay before making request
            
        Returns:
            Dictionary containing book details or None if failed
        """
        
        try:
            # Add delay
            time.sleep(self.delay)
            
            # Setup session with random user agent
            session = requests.Session()
            session.headers.update({
                'User-Agent': random.choice(USER_AGENTS),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            })
            
            logging.info(f"Scraping book details: {book_url}")
            
            # Make request
            response = session.get(book_url, timeout=15)
            response.raise_for_status()
            
            # Parse HTML with lxml for XPath support
            tree = html.fromstring(response.content)
            
            # Extract book details using XPath
            book_details = {}

            for key, xpath in BOOK_DETAILS_XPATH.items():
                for i in xpath:
                    value = tree.xpath(i)
                    if value:
                        book_details[key] = value[0].strip()
                        break

            
            # Add metadata
            book_details['book_url'] = book_url
            book_details['id'] = str(uuid.uuid4())
            book_details['scraped_at'] = time.strftime('%Y-%m-%d %H:%M:%S')
            
            # Log success
            logging.info(f"Successfully scraped: {book_details.get('title_xpath', 'Unknown Title')}")

            file_path = f"{self.upload_loc}/{counter}.json"

            status = self.minio.upload_data(bucket_name=BUCKET,object_name=file_path,data=json.dumps(book_details))
            
            return status
            
        except requests.exceptions.RequestException as e:
            logging.error(f"Request error for {book_url}: {e}")
            return None
        except Exception as e:
            logging.error(f"Error scraping {book_url}: {e}")
            return None
    
    def scrape_multiple_books(self, page_urls, max_threads=10):
        results = []

        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            future_to_url = {executor.submit(self.scrape_book_details, i, url): url for i,url in enumerate(page_urls)}

            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    data = future.result()
                    results.append(data)
                except Exception as e:
                    print(f"Error scraping {url}: {e}")
        
        return results

    def execute(self, **context):
        
        self.minio = MinIOHelper(endpoint=MIN_IO_URL, access_key=MIN_IO_ACCESS_KEY, secret_key= MIN_IO_SECRET_KEY)

        #CREATE bucket if not exists
        self.minio.create_bucket(bucket_name=BUCKET)

        page_urls = self.collect_book_urls()
        all_details = self.scrape_multiple_books(page_urls)

        # return {"raw_location": self.upload_loc}
        return [self.upload_loc]


if __name__ == "__main__":
    sdb = ScrapeBookDetails()

    sdb.scrape_book_details(0, "https://books.toscrape.com/catalogue/the-natural-history-of-us-the-fine-art-of-pretending-2_941/index.html")