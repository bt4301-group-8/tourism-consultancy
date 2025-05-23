# Airflow imports
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

# Third-party imports
from curl_cffi import requests as cureq
from parsel import Selector
from pymongo import MongoClient, errors as pymongo_errors
import pendulum
from dotenv import load_dotenv
from pathlib import Path

# Standard library imports
from datetime import datetime, timedelta
from urllib.parse import urlparse
import time
import re
import random
import os
import logging

# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# --- Load Environment Variables ---
# Define base directory (project root is two levels up from the DAG file's parent)
BASE_DIR = Path(__file__).resolve().parent.parent.parent # Go up from dags -> airflow -> tourism-consultancy
env_path = BASE_DIR / '.env'
load_dotenv(dotenv_path=env_path)
mongo_uri = os.getenv("MONGO_URI")

# define constants
BASE_URL = "https://www.tripadvisor.com"
ATTRACTION_URLS = [
    "https://www.tripadvisor.com/Attraction_Review-g297390-d317907-Reviews-Angkor_Wat-Siem_Reap_Siem_Reap_Province.html",
    "https://www.tripadvisor.com/Attraction_Review-g297390-d552507-Reviews-Bayon_Temple-Siem_Reap_Siem_Reap_Province.html",
    "https://www.tripadvisor.com/Attraction_Review-g297390-d325222-Reviews-Ta_Prohm-Siem_Reap_Siem_Reap_Province.html",
    "https://www.tripadvisor.com/Attraction_Review-g293940-d324063-Reviews-Tuol_Sleng_Genocide_Museum-Phnom_Penh.html",
    "https://www.tripadvisor.com/Attraction_Review-g293940-d469300-Reviews-Choeung_Ek_Genocidal_Center-Phnom_Penh.html",
    "https://www.tripadvisor.com/Attraction_Review-g297390-d469308-Reviews-Banteay_Srei-Siem_Reap_Siem_Reap_Province.html",
    "https://www.tripadvisor.com/Attraction_Review-g297390-d325215-Reviews-Angkor_Thom-Siem_Reap_Siem_Reap_Province.html",
    "https://www.tripadvisor.com/Attraction_Review-g297390-d13185212-Reviews-APOPO_Visitor_Center-Siem_Reap_Siem_Reap_Province.html",
    "https://www.tripadvisor.com/Attraction_Review-g297390-d541872-Reviews-Cambodia_Landmine_Museum-Siem_Reap_Siem_Reap_Province.html",
    "https://www.tripadvisor.com/Attraction_Review-g297390-d1908787-Reviews-Angkor_National_Museum-Siem_Reap_Siem_Reap_Province.html",
    "https://www.tripadvisor.com/Attraction_Review-g293940-d19239914-Reviews-Garden_City_Water_Park-Phnom_Penh.html",
    "https://www.tripadvisor.com/Attraction_Review-g2366676-d655155-Reviews-Prasat_Beng_Mealea-Siem_Reap_Province.html",
    "https://www.tripadvisor.com/Attraction_Review-g297390-d4066214-Reviews-Made_in_Cambodia_Market-Siem_Reap_Siem_Reap_Province.html",
    "https://www.tripadvisor.com/Attraction_Review-g303666-d2664524-Reviews-Battambang_Bat_Caves-Battambang_Battambang_Province.html",
    "https://www.tripadvisor.com/Attraction_Review-g297390-d1727839-Reviews-Angkor_Archaeological_Park-Siem_Reap_Siem_Reap_Province.html",
    "https://www.tripadvisor.com/Attraction_Review-g608455-d9756229-Reviews-La_Plantation-Kampot_Kampot_Province.html",
    "https://www.tripadvisor.com/Attraction_Review-g293940-d325304-Reviews-Mekong_River-Phnom_Penh.html",
    "https://www.tripadvisor.com/Attraction_Review-g297390-d552505-Reviews-Preah_Khan-Siem_Reap_Siem_Reap_Province.html",
    "https://www.tripadvisor.com/Attraction_Review-g1125878-d586700-Reviews-Koh_Ker_Temple-Preah_Vihear_Province.html",
    "https://www.tripadvisor.com/Attraction_Review-g25128660-d1020086-Reviews-Angkor_Silk_Farm-Puok_Siem_Reap_Province.html",
    "https://www.tripadvisor.com/Attraction_Review-g1125878-d1126477-Reviews-Preah_Vihear_Temple-Preah_Vihear_Province.html",
    "https://www.tripadvisor.com/Attraction_Review-g297390-d2284749-Reviews-Angkor_Wat_Tuktuk_Driver_By_Kakada-Siem_Reap_Siem_Reap_Province.html",
    "https://www.tripadvisor.com/Attraction_Review-g297390-d2639521-Reviews-Angkor_Centre_for_Conservation_of_Biodiversity-Siem_Reap_Siem_Reap_Province.html",
    "https://www.tripadvisor.com/Attraction_Review-g297390-d618884-Reviews-Baphuon_Temple-Siem_Reap_Siem_Reap_Province.html",
    "https://www.tripadvisor.com/Attraction_Review-g297390-d1635577-Reviews-Banteay_Srey_Butterfly_Centre-Siem_Reap_Siem_Reap_Province.html",
    "https://www.tripadvisor.com/Attraction_Review-g608456-d6062982-Reviews-Sothy_s_Pepper_Farm-Kep_Kep_Province.html",
    "https://www.tripadvisor.com/Attraction_Review-g297390-d12071108-Reviews-Wang_Tuk_Tuk_Driver-Siem_Reap_Siem_Reap_Province.html",
    "https://www.tripadvisor.com/Attraction_Review-g729351-d2299654-Reviews-Koh_Trong-Kratie_Kratie_Province.html",
    "https://www.tripadvisor.com/Attraction_Review-g297390-d6385685-Reviews-Ta_Prohm_Temple-Siem_Reap_Siem_Reap_Province.html",
    "https://www.tripadvisor.com/Attraction_Review-g303666-d12524046-Reviews-Human_Gallery_Joseba_Etxebarria_Photography-Battambang_Battambang_Province.html"
]

COUNTRY_NAME = "Cambodia"

# function to create a curl_cffi session with chrome impersonation
def create_session():
    return cureq.Session(
        timeout=20,
        impersonate="chrome110"
    )

def get_attraction_name_from_url(url):
    """Extracts a readable attraction name from the URL"""
    try:
        name = urlparse(url).path.split('-Reviews-')[-1].rsplit('-', 1)[0].replace('_', ' ')
        return ' '.join(word.capitalize() for word in name.split())
    except Exception:
        return "Unknown Attraction"

def extract_review_data_and_written_date(review_card, attraction_url, attraction_name, country):
    """
    Extracts core review data including attraction name and country.
    Uses specific XPaths provided by user.
    Returns: (review_data_dict or None, written_datetime or None)
    """
    review_data_dict = None
    written_datetime = None
    try:
        # --- Use User-Provided XPaths ---
        title = review_card.xpath('.//div[@class="biGQs _P fiohW qWPrE ncFvv fOtGX"]//span[@class="yCeTE"]/text()').get()
        # Get all text nodes and join them for the text field
        text_nodes = review_card.xpath('.//div[@class="fIrGe _T bgMZj"]//div[@class="biGQs _P pZUbB KxBGd"]//span[@class="JguWG"]//span[@class="yCeTE"]/text()').getall()
        text = ' '.join(t.strip() for t in text_nodes if t.strip()) if text_nodes else None

        rating_str = review_card.xpath('.//div[contains(@class, "VVbkp")]//svg[contains(@class, "evwcZ")]/title/text()').get()
        trip_info = review_card.xpath('.//div[contains(@class, "RpeCd")]/text()').get()
        written_date_xpath = ".//div[contains(@class, 'TreSq')]//div[starts-with(text(), 'Written ')]/text()"
        written_date_str_raw = review_card.xpath(written_date_xpath).get()

        # --- Parse Written Date ---
        if written_date_str_raw:
            cleaned_date_str = written_date_str_raw.replace("Written", "").strip()
            if cleaned_date_str:
                try:
                    written_datetime = datetime.strptime(cleaned_date_str, "%B %d, %Y")
                except ValueError:
                    logging.debug(f"  Could not parse date: {cleaned_date_str}")
                    pass # Ignore parse errors

        # --- Process other fields ---
        rating = None
        if rating_str and (match := re.search(r'(\d+\.?\d*)', rating_str)):
            rating = float(match.group(1))

        trip_date, trip_category = None, None
        if trip_info:
            parts = trip_info.split('•')
            trip_date = parts[0].strip() if parts else None
            trip_category = parts[1].strip() if len(parts) > 1 else None

        # --- Create data dict including new fields ---
        # Ensure essential fields (like text or title) are present before creating dict
        if text and title:
            review_data_dict = {
                "title": title.strip(),
                "text": text.strip(), # Text is already stripped and joined
                "rating": rating,
                "trip_date": trip_date,
                "trip_category": trip_category,
                "attraction_name": attraction_name,
                "attraction_url": attraction_url,
                "country": country
            }
        else:
             # Optional: Log if essential parts missing
             logging.debug(f"  Missing title or text for a review card.")
             pass

    except Exception as e:
        logging.warning(f"  Error extracting card data: {e}")

    return review_data_dict, written_datetime

# --- Scraper Function ---
def scrape_reviews_until_month_changes(attraction_url, attraction_name, country, target_month, target_year, session):
    """
    Scrapes reviews for a single attraction, stopping when 'Written Date'
    month is before target. Includes attraction_name and country in data.
    """
    collected_reviews = []
    current_url = attraction_url
    page_offset = 0
    processed_review_count = 0
    max_pages_to_try = 5

    logging.info(f"\nStarting scrape for: {attraction_name} ({country})")
    logging.info(f" -> URL: {attraction_url}")
    logging.info(f" -> Targeting: {target_month}/{target_year}")

    for page_num in range(max_pages_to_try):
        # Construct URL first for correct logging
        if page_num > 0:
            page_offset = page_num * 10
            if "-Reviews-" in attraction_url:
                try:
                    parts = attraction_url.split("-Reviews-")
                    suffix_part = parts[1].split("-", 1)[1]
                    current_url = f"{parts[0]}-Reviews-or{page_offset}-{suffix_part}"
                except IndexError:
                     logging.error("  Cannot determine pagination URL structure.")
                     break
            else:
                logging.error("  Cannot determine pagination URL structure based on '-Reviews-'.")
                break
        else:
            current_url = attraction_url # Ensure first page uses the base URL

        logging.info(f"  Fetching page {page_num + 1} (offset {page_offset})...")
        time.sleep(random.uniform(4, 8)) # Keep increased delay

        try:
            # Use session's impersonation settings
            response = session.get(current_url)
            if response.status_code != 200:
                logging.warning(f"  Received status code {response.status_code} for {current_url}. Skipping page.")
                continue # Skip this page

            html_content = response.content.decode('utf-8', errors='ignore')
            selector = Selector(text=html_content)

        except Exception as e:
            logging.error(f"  Failed to fetch {current_url}: {e}")
            break

        review_cards = selector.xpath('//div[@data-automation="reviewCard"]')

        if not review_cards:
            logging.info(f"   No review cards found on page {page_num + 1}.")
            if page_num == 0:
                 logging.info("   No reviews on first page, stopping for this attraction.")
                 break
            else:
                 logging.info("   No more reviews found, stopping pagination for this attraction.")
                 break

        logging.info(f"   Found {len(review_cards)} review cards.")
        stop_processing_attraction = False
        page_processed_count = 0

        for card in review_cards:
            review_data, written_dt = extract_review_data_and_written_date(
                card, attraction_url, attraction_name, country
            )
            page_processed_count += 1

            if not written_dt: continue

            # --- Core Stop Logic ---
            if written_dt.year == target_year and written_dt.month == target_month:
                if review_data:
                    collected_reviews.append(review_data)
                # No need for reviews_on_page_matched flag if we stop below
            elif written_dt.year < target_year or (written_dt.year == target_year and written_dt.month < target_month):
                logging.info(f"   STOP condition met: Review written {written_dt.strftime('%B %Y')} is before target {target_month}/{target_year}.")
                stop_processing_attraction = True
                break

        processed_review_count += page_processed_count

        if stop_processing_attraction:
            break # Stop the outer page loop

        # --- No need for pagination if loop finishes naturally after max_pages_to_try ---

    logging.info(f" Finished scraping for {attraction_name}. Processed approx {processed_review_count} reviews. Found {len(collected_reviews)} matching target month.")
    return collected_reviews

# dag definition
@dag(
    dag_id=f"tripadvisor_{COUNTRY_NAME}_scraper",
    schedule_interval="0 6 1 * *",  # Run at 5am (UTC+8) on the 1st of each month
    start_date=pendulum.datetime(2025, 4, 1, tz="Asia/Singapore"),
    catchup=False,
    tags=["tripadvisor", "scraping", "cambodia"],
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    doc_md=f"""
    ### TripAdvisor Review Scraper DAG for {COUNTRY_NAME}
    - Scrapes reviews for top 30 attractions using curl_cffi
    - Stops scraping if reviews are written before the first of the month
    - Stores scraped data in MongoDB
    """
)

def tripadvisor_cambodia_scraper():
    # task to determine the target month and year
    @task
    def determine_target_month_and_year(**context):
        # use logical date for scheduled runs if available, else use now()
        logical_date = context.get("logical_date") or pendulum.now(tz="Asia/Singapore")
        first_of_run_month = logical_date.start_of("month")
        last_day_of_previous_month = first_of_run_month.subtract(days=1)
        
        target_month = last_day_of_previous_month.month
        target_year = last_day_of_previous_month.year

        logging.info(f"Logical Date: {logical_date}")
        logging.info(f"Determined Target Period: {target_month}/{target_year}")
        return {"target_month": target_month, "target_year": target_year}

    # task to scrape reviews
    @task
    def scrape_reviews_task(target_period):
        target_month = target_period.get("target_month")
        target_year = target_period.get("target_year")

        all_new_reviews = []
        # create session
        session = create_session()

        # try visit base url first
        try:
            logging.info(f"Visiting {BASE_URL}")
            response_base = session.get(BASE_URL)
            response_base.raise_for_status()
            logging.info(f"Visited {BASE_URL} successfully")
            # sleep
            time.sleep(random.uniform(2, 4))
        except Exception as e:
            logging.warning(f"Could not visit base URL {BASE_URL}: {e}")

        # go through each attraction url
        for i, url in enumerate(ATTRACTION_URLS):
            attraction_name = get_attraction_name_from_url(url)
            try:
                reviews = scrape_reviews_until_month_changes(
                    url, attraction_name, COUNTRY_NAME, target_month, target_year, session
                )
                if reviews:
                    all_new_reviews.extend(reviews)
            except Exception as e:
                logging.error(f" Failed to scrape reviews for {attraction_name}: {e}")

            # time delay between attractions
            time.sleep(random.uniform(2, 8))

        logging.info(f"--- Finished scraping for {COUNTRY_NAME}. Total new reviews found: {len(all_new_reviews)} ---")
        # close session
        if session:
            session.close()
        return all_new_reviews

    # task to load reviews into mongodb
    @task
    def load_reviews_to_mongodb(reviews, target_period):
        target_month = target_period.get("target_month")
        target_year = target_period.get("target_year")
        if not reviews:
            logging.info("No reviews to load to MongoDB.")
            return 0 # Return count of loaded reviews

        client = None # Initialize client to None for the finally block
        inserted_count = 0
        start_time = datetime.now()
        logging.info(f"Starting data loading to MongoDB at {start_time}")

        try:
            # Use MongoClient directly with the URI from .env
            logging.info(f"Connecting to MongoDB...")
            client = MongoClient(mongo_uri)
            # Verify connection
            client.admin.command('ping')
            logging.info("MongoDB connection successful.")

            db = client.posts
            review_collection = db.tripadvisor
            logging.info(f"Connected to MongoDB. Database: {db.name}, Collection: {review_collection.name}")
        
            # insert reviews into collection
            try:
                result = review_collection.insert_many(reviews)
                inserted_count = len(result.inserted_ids)
                logging.info(f"Successfully inserted {inserted_count} out of {len(reviews)} reviews into MongoDB.")
            except pymongo_errors.BulkWriteError as bwe:
                inserted_count = bwe.details['nInserted']
                errors = bwe.details['writeErrors']
                logging.error(f"Bulk write error inserting reviews. Inserted: {inserted_count}. Errors: {len(errors)}")
            except Exception as e:
                logging.error(f"Failed to insert reviews into MongoDB: {e}")

        except pymongo_errors.ConnectionFailure as e:
            logging.error(f"Failed to connect to MongoDB: {e}")
            raise # Re-raise connection errors as they are critical
        except Exception as e:
            logging.error(f"An unexpected error occurred during MongoDB operations: {e}")
            raise # Re-raise other critical err
        finally:
            if client:
                client.close()
                logging.info("MongoDB connection closed.")

        total_time = datetime.now() - start_time
        logging.info(f"MongoDB loading finished in {total_time}. Inserted {inserted_count} reviews.")
        return inserted_count # Return the count of successfully inserted reviews

    # define task dependencies
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    target_period_dict = determine_target_month_and_year()
    scraped_reviews_list = scrape_reviews_task(target_period_dict)
    load_results = load_reviews_to_mongodb(scraped_reviews_list, target_period_dict)

    start >> target_period_dict >> scraped_reviews_list >> load_results >> end

tripadvisor_cambodia_scraper()
    