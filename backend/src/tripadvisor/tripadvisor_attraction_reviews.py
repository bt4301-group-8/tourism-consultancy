import json
import re
import sys
import random
import time
from typing import List, TypedDict, Optional
from loguru import logger as log
from parsel import Selector
import requests
from tripadvisor_location_search import create_session

# Configure logging
log.remove()
log.add(sys.stderr, level="DEBUG")

HOME_URL = "https://www.tripadvisor.com"

class Review(TypedDict):
    """Review data structure"""
    title: str
    text: str
    rating: float
    trip_date: str
    trip_category: str
    attraction_name: str
    attraction_url: str
    country: str  # Added country field

def extract_review_data(review_card) -> Optional[Review]:
    """Extract data from a review card"""
    try:
        selectors = {
            'title': './/div[contains(@class, "biGQs") and contains(@class, "fiohW")]/a/span/text()',
            'text': './/div[contains(@class, "fIrGe")]//span[@class="yCeTE"]/text()',
            'rating': './/svg[contains(@class, "UctUV")]/title/text()',
            'trip_info': './/div[contains(@class, "RpeCd")]/text()'
        }
        
        data = {key: review_card.xpath(selector).get() for key, selector in selectors.items()}
        
        if not all(data.values()):
            return None
            
        # Process rating
        rating_match = re.search(r'(\d+\.?\d*)', data['rating'])
        if not rating_match:
            return None
            
        # Process trip info with safer splitting
        trip_info_parts = data['trip_info'].split('•')
        trip_date = trip_info_parts[0].strip() if trip_info_parts else "Unknown"
        trip_category = trip_info_parts[1].strip() if len(trip_info_parts) > 1 else "Unknown"
        
        return {
            "title": data['title'],
            "text": data['text'],
            "rating": float(rating_match.group(1)),
            "trip_date": trip_date,
            "trip_category": trip_category
        }
            
    except Exception as e:
        log.warning(f"Error extracting review data: {str(e)}")
        return None

def scrape_attraction_reviews(attraction_url: str, attraction_name: str, country: str, session: requests.Session, max_pages: int = 2) -> List[Review]:
    """Scrape reviews for a specific attraction"""
    reviews = []
    base_url = attraction_url.replace("-Reviews-", "-Reviews-or{}-")
    
    try:
        # Initialize session with homepage visit
        session.get(HOME_URL, timeout=15)
        time.sleep(random.uniform(1, 2))
        # time.sleep(random.uniform(3, 5))  # Longer initial delay
        
        for page in range(max_pages):
            current_url = base_url.format(page * 10) if page > 0 else attraction_url
            log.info(f"Fetching reviews page {page + 1} for {attraction_name}")
            
            session.headers.update({"Referer": current_url})
            # time.sleep(random.uniform(2, 3))
            time.sleep(random.uniform(5, 8))
            
            response = session.get(current_url, timeout=15)
            if response.status_code != 200:
                log.error(f"Failed to fetch page {page + 1}: {response.status_code}")
                break
                
            selector = Selector(response.text)
            review_cards = selector.xpath('//div[@data-automation="reviewCard"]')
            
            page_reviews = []
            for card in review_cards:
                if review := extract_review_data(card):
                    review.update({
                        "attraction_name": attraction_name,
                        "attraction_url": attraction_url,
                        "country": country  # Added country field
                    })
                    page_reviews.append(review)
            
            if not page_reviews:
                log.warning(f"No reviews found on page {page + 1}")
                break
                
            reviews.extend(page_reviews)
            log.info(f"Found {len(page_reviews)} reviews on page {page + 1}")
                
    except Exception as e:
        log.error(f"Error scraping reviews for {attraction_name}: {str(e)}")
    
    return reviews

def get_output_filename(attraction_name: str, country: str) -> str:
    """Generate standardized output filename for reviews"""
    # Clean attraction name for filename
    clean_name = re.sub(r'[^\w\s-]', '', attraction_name).strip().replace(' ', '_').lower()
    return f"./{country}_{clean_name}_reviews.json"

def main():
    try:
        # replace with the path to your JSON file
        with open("./brunei_attractions.json", 'r') as f:
            attractions = json.load(f)
            
        # Filter attractions with reasonable review counts
        filtered_attractions = [
            attr for attr in attractions 
            if attr.get("review_count") is not None and 500 <= attr["review_count"]
        ][:30]  # Take first 30 attractions
        
        session = create_session()
        
        for attraction in filtered_attractions:
            # Pass country to scrape_attraction_reviews
            reviews = scrape_attraction_reviews(
                attraction["url"], 
                attraction["name"],
                attraction["country"],  # Added country parameter
                session,
                max_pages=20
            )
            
            output_file = get_output_filename(attraction["name"], attraction["country"].lower())
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(reviews, f, indent=2, ensure_ascii=False)
            
            log.info(f"Saved {len(reviews)} reviews for {attraction['name']}")
            # time.sleep(random.uniform(3, 5))
            time.sleep(random.uniform(5, 8))
            
    except Exception as e:
        log.error(f"Error during review scraping: {str(e)}")

if __name__ == "__main__":
    main()