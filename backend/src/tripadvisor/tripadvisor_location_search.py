import json
import re
import sys
import random
import time
from typing import List, TypedDict, Optional
from urllib.parse import urljoin
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from loguru import logger as log
from parsel import Selector

# Configure logging
log.remove()
log.add(sys.stderr, level="DEBUG")

# Type definitions
class Preview(TypedDict):
    """Preview data for any TripAdvisor listing"""
    url: str
    name: str
    type: str
    rating: Optional[float]
    review_count: Optional[int]
    country: str

# Constants
BASE_URL = "https://www.tripadvisor.com"
# uncomment the relevant countries you want to scrape - ideally one at a time since it takes a long time to run and you want to avoid getting banned
ATTRACTIONS_PATH = {
    # "Thailand": "/Attractions-g293915-Activities-oa0-Thailand.html",
    # "Singapore": "/Attractions-g294265-Activities-oa0-Singapore.html",
    # "Malaysia": "/Attractions-g293951-Activities-oa0-Malaysia.html",
    # "Indonesia": "/Attractions-g294225-Activities-oa0-Indonesia.html",
    # "Vietnam": "/Attractions-g293921-Activities-oa0-Vietnam.html",
    # "Philippines": "/Attractions-g294245-Activities-oa0-Philippines.html",
    # "Cambodia": "/Attractions-g293939-Activities-oa0-Cambodia.html",
    # "Myanmar": "/Attractions-g294190-Activities-oa0-Myanmar.html",
    # "Laos": "/Attractions-g293949-Activities-oa0-Laos.html",
    "Brunei": "/Attractions-g293937-Activities-oa0-Brunei_Darussalam.html"
}
OUTPUT_DIR = "/."

def create_browser_headers() -> dict:
    """Create headers that mimic a real browser"""
    chrome_version = f"{random.randint(90, 112)}.0.{random.randint(1000, 9999)}.0"
    return {
        "User-Agent": f"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{chrome_version} Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Cache-Control": "max-age=0",
        "Sec-Ch-Ua": f'"Google Chrome";v="{chrome_version}", "Chromium";v="{chrome_version}", "Not=A?Brand";v="24"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"macOS"',
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "Cookie": f"TASession={random.getrandbits(32):x}; ServerPool=X; TASSK={random.getrandbits(32):x}",
        "DNT": "1"
    }

def create_session() -> requests.Session:
    """Create a requests session with retry strategy"""
    session = requests.Session()
    
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(create_browser_headers())
    
    return session

def extract_attraction_data(listing, country: str) -> Optional[Preview]:
    """Extract attraction data from a listing element"""
    try:
        # Get title
        title = ''.join(listing.xpath('.//div[contains(@class, "XfVdV")]//text()').getall())
        if not title:
            return None
        title = re.sub(r'^\d+\.\s*', '', title.strip())
        
        # Get URL
        url = listing.xpath('.//a[contains(@href, "Attraction_Review")]/@href').get()
        if not url:
            return None
            
        # Get rating
        rating = None
        rating_elem = listing.xpath('.//svg[contains(@class, "UctUV")]/title/text()').get()
        if rating_elem:
            rating_match = re.search(r'(\d+\.?\d*)', rating_elem)
            if rating_match:
                rating = float(rating_match.group(1))
        
        # Get review count
        review_count = None
        review_count_text = listing.xpath('.//div[contains(@class, "jVDab")]//span[contains(@class, "biGQs") and contains(@class, "osNWb")]/text()').get()
        if review_count_text:
            review_count = int(review_count_text.replace(',', ''))
        
        return {
            "url": urljoin(BASE_URL, url),
            "name": title,
            "type": "attraction",
            "rating": rating,
            "review_count": review_count,
            "country": country  # Pass country as parameter instead
        }
    except Exception as e:
        log.warning(f"Error extracting data: {str(e)}")
        return None

def parse_search_page(html_content: str, country: str) -> List[Preview]:
    """Parse attractions from a search page"""
    parsed = []
    selector = Selector(html_content)
    
    for listing in selector.xpath('//div[contains(@class, "alPVI") and contains(@class, "eNNhq")]'):
        attraction = extract_attraction_data(listing, country)  # Pass country as parameter
        if attraction:
            parsed.append(attraction)
            log.debug(f"Successfully added: {attraction['name']}")
    
    if not parsed:
        log.warning("No attractions found on this page")
    else:
        log.info(f"Found {len(parsed)} attractions on this page")
    
    return parsed

def scrape_country_attractions(country: str, url_path: str, session: requests.Session, max_pages: int = 2) -> List[Preview]:
    """Scrape attractions from a country"""
    results = []
    base_url = BASE_URL + url_path
    
    try:
        # Initialize session with homepage visit
        session.get(BASE_URL, timeout=15)
        # time.sleep(random.uniform(2, 4))
        # time.sleep(random.uniform(1, 2))
        time.sleep(random.uniform(8, 12))
        
        for page in range(max_pages):
            current_url = base_url.replace("oa0", f"oa{page * 30}") if page > 0 else base_url
            log.info(f"Fetching page {page + 1}: {current_url}")
            
            # Add delays and update referer
            time.sleep(random.uniform(5, 8))
            # time.sleep(random.uniform(2, 3))
            session.headers.update({"Referer": BASE_URL if page == 0 else base_url})
            
            # Fetch and parse page
            response = session.get(current_url, timeout=15)
            if response.status_code == 200:
                page_results = parse_search_page(response.text, country)
                if page_results:
                    results.extend(page_results)
                    log.info(f"Found {len(page_results)} attractions on page {page + 1}")
                else:
                    break
            else:
                log.error(f"Failed to fetch page {page + 1}: {response.status_code}")
                break
                
    except Exception as e:
        log.error(f"Error during scraping: {str(e)}")
    
    return results

def save_results(results: List[Preview], country: str):
    """Save results to a JSON file"""
    filename = f"{country.lower()}_attractions.json"
    filepath = f"{OUTPUT_DIR}/{filename}"
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    log.info(f"Saved {len(results)} attractions for {country} to {filepath}")

def main():
    """Main execution function"""
    session = create_session()
    
    try:
        for country, url_path in ATTRACTIONS_PATH.items():
            log.info(f"Starting scraping for {country}")
            attractions = scrape_country_attractions(country, url_path, session, max_pages=10)
            
            if attractions:
                save_results(attractions, country)
                log.info(f"Completed scraping {len(attractions)} attractions for {country}")
            else:
                log.warning(f"No attractions found for {country}")
                
            time.sleep(random.uniform(3, 5))
            
    except Exception as e:
        log.error(f"Error during scraping: {str(e)}")

if __name__ == "__main__":
    main()