from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException, StaleElementReferenceException

import undetected_chromedriver as uc

import time
import random
import urllib.parse
import csv
from bs4 import BeautifulSoup
import logging
import os
from datetime import datetime, timedelta
import dateutil.parser

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Progress Tracking ---
PROGRESS_FILE = "linkedin_scraper_progress.txt"
DEFAULT_OUTPUT_FILENAME = "data_saudi_arabia_jobs.csv" # Hardcoded as per your request

def save_progress(current_scrolled_pages, filename=PROGRESS_FILE):
    """Saves the number of pages successfully scrolled (and processed)."""
    try:
        with open(filename, 'w') as f:
            f.write(str(current_scrolled_pages))
        logging.info(f"Progress saved: Scrolled and processed up to page {current_scrolled_pages}")
    except IOError as e:
        logging.error(f"Failed to save progress to {filename}: {e}")

def load_progress(filename=PROGRESS_FILE):
    """Loads the last saved page number. Returns 0 if no progress found."""
    if os.path.exists(filename):
        try:
            with open(filename, 'r') as f:
                content = f.read().strip()
                if content:
                    return int(content)
        except (IOError, ValueError) as e:
            logging.warning(f"Could not load progress from {filename}: {e}. Starting from page 0 (will process page 1).")
    return 0 # Default to page 0 (meaning start from page 1, as loop is range(start_page, target_pages))


# --- Helper Functions ---

def normalize_linkedin_job_url(url):
    """
    Normalizes a LinkedIn job URL by removing tracking parameters.
    Example:
    https://www.linkedin.com/jobs/view/3929420956/?alternateChannel=search&refId=...
    becomes
    https://www.linkedin.com/jobs/view/3929420956/
    """
    if "linkedin.com/jobs/view/" in url:
        # Find the part after /view/ up to the next / or ?
        parts = url.split("linkedin.com/jobs/view/")
        if len(parts) > 1:
            job_id_and_rest = parts[1]
            # Split by '?' to get rid of query parameters
            # Split by '/' to get rid of trailing path components
            job_id = job_id_and_rest.split('/')[0].split('?')[0]
            return f"https://www.linkedin.com/jobs/view/{job_id}/"
    return url # Return original if not a standard job view URL


def get_existing_job_links(filename):
    """Loads existing job links from a CSV file to prevent duplicates."""
    existing_links = set()
    if os.path.exists(filename):
        try:
            with open(filename, 'r', newline='', encoding='utf-8') as f:
                reader = csv.reader(f)
                header = next(reader, None) # Skip header
                if header:
                    try:
                        link_index = header.index("Link")
                    except ValueError:
                        logging.warning(f"CSV header in {filename} does not contain 'Link' column. Cannot check for duplicates.")
                        return existing_links # Return empty set if link column not found

                    for row in reader:
                        if len(row) > link_index:
                            # Normalize the link before adding to the set
                            normalized_link = normalize_linkedin_job_url(row[link_index])
                            existing_links.add(normalized_link)
        except Exception as e:
            logging.warning(f"Could not read existing links from {filename}: {e}")
    return existing_links

def human_like_type(element, text):
    """Types text into an element with human-like delays."""
    for char in text:
        element.send_keys(char)
        time.sleep(random.uniform(0.05, 0.2)) # Simulate key press delay

def scroll_to_bottom_human_like(driver):
    """Scrolls to the very bottom of the page in segments."""
    last_height = driver.execute_script("return document.body.scrollHeight")
    logging.info(f"Initial document scroll height: {last_height}")
    
    # Scroll in segments until bottom is reached or content stops growing
    scroll_attempts = 0
    max_scroll_attempts_per_batch = 3 # Try scrolling a few times per iteration of the main loop
    
    while scroll_attempts < max_scroll_attempts_per_batch:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(random.uniform(2, 4)) # Pause after scroll

        new_height = driver.execute_script("return document.body.scrollHeight")
        logging.info(f"New document scroll height: {new_height}")

        if new_height == last_height:
            logging.info("Reached the bottom of the scrollable area (or content stopped loading).")
            return True # Successfully reached bottom or content didn't grow
        last_height = new_height
        scroll_attempts += 1
    
    logging.warning(f"Exceeded {max_scroll_attempts_per_batch} scroll attempts without reaching true bottom or seeing new content. May not have loaded all.")
    return False # Did not definitively reach bottom

# --- Helper to extract and save jobs ---
def extract_and_save_jobs(driver, csv_writer, existing_links, scraped_job_counter, scroll_id="initial"):
    """
    Extracts job listings from the current page HTML and saves new ones to CSV.
    Returns the updated scraped_job_counter and the total number of job cards found on the page.
    """
    logging.info(f"Extracting jobs from current page view (scroll {scroll_id})...")
    current_page_html = ""
    try:
        current_page_html = driver.execute_script("return document.body.innerHTML;")
        logging.debug("Successfully extracted current page HTML.")
    except WebDriverException as e:
        logging.error(f"WebDriver error getting current page innerHTML for scroll {scroll_id}: {e}")
        driver.save_screenshot(f"html_extraction_error_scroll_{scroll_id}.png")
        logging.info(f"Screenshot 'html_extraction_error_scroll_{scroll_id}.png' saved.")
        return scraped_job_counter, 0 # Return current count and 0 total jobs

    soup = BeautifulSoup(current_page_html, 'html.parser')
    job_list_container_soup = soup.find('ul', class_='jobs-search__results-list')

    if not job_list_container_soup:
        logging.error(f"Error: Main job results list container not found for scroll {scroll_id}. Skipping job extraction for this view.")
        driver.save_screenshot(f"container_not_found_scroll_{scroll_id}.png")
        with open(f"debug_full_page_scroll_{scroll_id}.html", "w", encoding="utf-8") as f:
            f.write(current_page_html)
        return scraped_job_counter, 0

    job_cards = job_list_container_soup.find_all('div', class_=lambda x: x and 'job-search-card' in x.split())
    
    total_jobs_on_page_html = len(job_cards) # Count jobs found in the HTML parse
    logging.info(f"Found {total_jobs_on_page_html} job cards in HTML for scroll {scroll_id}.")

    current_scroll_jobs_scraped_this_parse = 0 # Count new jobs added in this particular parse
    for job_listing_soup in job_cards:
        title, company, job_location, job_link, post_time = "N/A", "N/A", "N/A", "N/A", "N/A"

        title_element = job_listing_soup.find('h3', class_='base-search-card__title')
        if title_element:
            title = title_element.get_text(strip=True)

        company_element = job_listing_soup.find('h4', class_='base-search-card__subtitle')
        if company_element:
            company = company_element.get_text(strip=True)

        location_element = job_listing_soup.find('span', class_='job-search-card__location')
        if location_element:
            job_location = location_element.get_text(strip=True)

        link_element = job_listing_soup.find('a', class_='base-card__full-link')
        if link_element and 'href' in link_element.attrs:
            job_link = link_element['href']

        post_time_element = job_listing_soup.find('time', class_='job-search-card__listdate')
        if post_time_element:
            post_time_text = post_time_element.get_text(strip=True)
            try:
                parsed_time = dateutil.parser.parse(post_time_text, fuzzy=True)
                post_time = parsed_time.strftime('%Y-%m-%d') # Only date
            except ValueError:
                logging.warning(f"Could not parse post time '{post_time_text}'. Keeping original text.")
                post_time = post_time_text

        normalized_job_link = normalize_linkedin_job_url(job_link)

        if normalized_job_link and normalized_job_link not in existing_links:
            csv_writer.writerow([title, company, job_location, normalized_job_link, post_time])
            existing_links.add(normalized_job_link)
            scraped_job_counter += 1
            current_scroll_jobs_scraped_this_parse += 1
            logging.debug(f"Scraped NEW job: '{title}' at '{company}' posted on '{post_time}'")
        elif normalized_job_link:
            logging.debug(f"Skipping duplicate job: '{title}' (already in CSV or seen in this session).")
        else:
            logging.debug(f"Skipping job with no link: '{title}'")
    logging.info(f"Added {current_scroll_jobs_scraped_this_parse} new jobs from scroll {scroll_id} to CSV. Total new jobs this session: {scraped_job_counter}.")
    return scraped_job_counter, total_jobs_on_page_html


# --- Main Scraper Function ---

def scrape_linkedin_jobs(keyword, location, output_filename=DEFAULT_OUTPUT_FILENAME, max_scroll_attempts=1000):
    scraped_job_count = 0

    options = webdriver.ChromeOptions()
    # options.add_argument('--headless') # Uncomment to run without a visible browser window
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu') # Often helps in headless environments

    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/91.0.864.59",
    ]
    options.add_argument(f"user-agent={random.choice(user_agents)}")

    logging.info("Initializing Chrome WebDriver (undetected_chromedriver)...")
    driver = None
    try:
        driver = uc.Chrome(options=options)
        logging.info("Chrome WebDriver initialized successfully.")
    except Exception as e:
        logging.critical(f"Failed to initialize Chrome WebDriver: {e}", exc_info=True)
        logging.critical("Please ensure Google Chrome is installed and updated. Also, check undetected_chromedriver installation.")
        return 0

    wait = WebDriverWait(driver, 30)

    existing_links = get_existing_job_links(output_filename)
    if existing_links:
        logging.info(f"Found {len(existing_links)} existing job links (normalized) in {output_filename}. Will append new jobs.")
    else:
        logging.info(f"No existing job links found or file is new. A new CSV file: {output_filename} will be created if it doesn't exist.")

    headers = ["Title", "Company", "Location", "Link", "Post Time"]

    csv_file = None
    csv_writer = None

    # --- Load Progress ---
    completed_scrolls = load_progress()
    logging.info(f"Resuming from scroll count: {completed_scrolls}.")
    # Start the loop from the *next* scroll attempt number
    start_scroll_attempt = completed_scrolls + 1

    try:
        csv_file = open(output_filename, 'a', newline='', encoding='utf-8')
        csv_writer = csv.writer(csv_file)

        if not os.path.exists(output_filename) or os.stat(output_filename).st_size == 0:
             csv_writer.writerow(headers)
             logging.info(f"Created new CSV file: {output_filename} with headers.")
        else:
             logging.info(f"Appending to existing CSV file: {output_filename}.")

        encoded_keyword = urllib.parse.quote_plus(keyword)
        encoded_location = urllib.parse.quote_plus(location)
        initial_url = f"https://www.linkedin.com/jobs/search?keywords={encoded_keyword}&location={encoded_location}&origin=JOBS_HOME_SEARCH_BUTTON"

        logging.info(f"Navigating to LinkedIn Jobs search: {initial_url}")
        driver.get(initial_url)

        time.sleep(random.uniform(5, 10))

        logging.info("Attempting to close any pop-ups...")
        try:
            driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.ESCAPE)
            logging.info("Sent ESC key to close pop-up.")
            time.sleep(random.uniform(1, 2))

            close_button = WebDriverWait(driver, 3).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "button.modal-client-actions__close, button[aria-label*='Dismiss'], button[aria-label*='Close']"))
            )
            if close_button:
                close_button.click()
                logging.info("Clicked pop-up close button as a fallback.")
                time.sleep(random.uniform(1, 2))
        except TimeoutException:
            logging.info("No clickable pop-up close button found after ESC (or already closed).")
            pass
        except NoSuchElementException:
            logging.warning("Could not find 'body' element to send ESC key (unlikely but possible).")
            pass
        except Exception as e:
            logging.warning(f"An unexpected error occurred during pop-up closing: {e}")
            pass

        logging.info("Waiting for general page elements to load (e.g., search bar)...")
        try:
            wait.until(EC.presence_of_element_located((By.ID, "job-search-bar-keywords")))
            logging.info("General search bar element found. Page seems loaded.")
        except TimeoutException:
            logging.error(f"Initial wait for general page element timed out after {wait._timeout} seconds.")
            raise

        # --- Initial scrape AFTER page load (if not resuming past this point) ---
        if start_scroll_attempt == 1: # Only do initial scrape if we are truly starting fresh
            logging.info("Performing initial job extraction (scroll 0/page 1).")
            scraped_job_count, last_total_jobs_on_page = extract_and_save_jobs(driver, csv_writer, existing_links, scraped_job_count, scroll_id=0)
            save_progress(1) # Mark initial load as 'scroll 1' completed
            logging.info(f"Initial load completed. Total jobs on page after first extraction: {last_total_jobs_on_page}. Total new jobs added to CSV this session: {scraped_job_count}")
        else:
            logging.info(f"Resuming from scroll {completed_scrolls}. Skipping initial job extraction.")
            scraped_job_count = len(existing_links) # Update scraped_job_count to reflect existing links
            # We need to get the current number of jobs visible on the page to properly track new content loading
            _, last_total_jobs_on_page = extract_and_save_jobs(driver, csv_writer, existing_links, scraped_job_count, scroll_id="resume_check")
            logging.info(f"Resuming with {scraped_job_count} jobs already in CSV. Jobs currently visible on screen: {last_total_jobs_on_page}")


        # --- Main scrolling loop for a fixed number of attempts ---
        logging.info(f"Starting scrolling loop for up to {max_scroll_attempts} attempts.")
        
        no_new_jobs_consecutive_scrolls = 0
        MAX_NO_NEW_JOBS_CONSECUTIVE_SCROLLS = 5 # Allow more patience before stopping

        for current_scroll_attempt in range(start_scroll_attempt, max_scroll_attempts + 1):
            logging.info(f"Attempting scroll {current_scroll_attempt} of {max_scroll_attempts}...")
            
            # Scroll the entire page to the bottom
            scrolled_to_bottom = scroll_to_bottom_human_like(driver)
            
            # Wait for content to load after scroll
            time.sleep(random.uniform(3, 7))

            # Try to click "See more jobs" button if it appears
            clicked_see_more = False
            try:
                see_more_button = WebDriverWait(driver, 5).until( # Shorter wait for button, as we're actively trying to scroll
                    EC.element_to_be_clickable((By.CSS_SELECTOR, ".infinite-scroller__show-more-button"))
                )
                if see_more_button.is_displayed():
                    # Attempt to click using JavaScript if direct click fails sometimes (e.g., obscured)
                    driver.execute_script("arguments[0].click();", see_more_button)
                    clicked_see_more = True
                    logging.info(f"Clicked 'See more jobs' button for scroll {current_scroll_attempt}.")
                    time.sleep(random.uniform(3, 6)) # Wait longer after click for jobs to load
                else:
                    logging.info(f"No 'See more jobs' button visible for scroll {current_scroll_attempt}.")
            except TimeoutException:
                logging.info(f"No 'See more jobs' button appeared within timeout for scroll {current_scroll_attempt}.")
            except Exception as e:
                logging.warning(f"Error while trying to click 'See more jobs' button for scroll {current_scroll_attempt}: {e}")

            # Extract and save jobs after this scroll/click attempt
            prev_scraped_count = scraped_job_count # Jobs added to CSV
            scraped_job_count, new_total_jobs_on_page = extract_and_save_jobs(driver, csv_writer, existing_links, scraped_job_count, scroll_id=current_scroll_attempt)

            logging.info(f"Jobs visible on page AFTER scroll {current_scroll_attempt}: {new_total_jobs_on_page}. (Was {last_total_jobs_on_page} before this scroll attempt).")

            # Determine if new content actually loaded
            content_increased = (new_total_jobs_on_page > last_total_jobs_on_page)
            jobs_added_to_csv = (scraped_job_count > prev_scraped_count)

            if content_increased or jobs_added_to_csv or clicked_see_more:
                no_new_jobs_consecutive_scrolls = 0 # Reset counter if content increased or button clicked or new jobs added
                logging.info(f"Content increased, button clicked, or new jobs added. Resetting no-new-jobs counter.")
            else:
                no_new_jobs_consecutive_scrolls += 1
                logging.info(f"No new content loaded and no new jobs added to CSV for scroll {current_scroll_attempt}. Consecutive no-new-activity: {no_new_jobs_consecutive_scrolls}")
                if no_new_jobs_consecutive_scrolls >= MAX_NO_NEW_JOBS_CONSECUTIVE_SCROLLS:
                    logging.info(f"Stopping after {MAX_NO_NEW_JOBS_CONSECUTIVE_SCROLLS} consecutive scrolls with no new activity. Assuming end of results or page is exhausted.")
                    break
            
            last_total_jobs_on_page = new_total_jobs_on_page # Update for next iteration

            # Save progress after successfully processing a scroll
            save_progress(current_scroll_attempt)

            # Add a small random delay before the next loop iteration to avoid being too fast
            time.sleep(random.uniform(1, 3))


    except TimeoutException as e:
        logging.error(f"A Selenium wait operation timed out: {e}")
        if driver:
            driver.save_screenshot("final_timeout_error.png")
            logging.info("Screenshot 'final_timeout_error.png' saved.")
    except NoSuchElementException as e:
        logging.error(f"A required web element was not found: {e}")
        if driver:
            driver.save_screenshot("element_not_found_error.png")
            logging.info("Screenshot 'element_not_found_error.png' saved.")
    except Exception as e:
        logging.critical(f"An unhandled critical error occurred during scraping: {e}", exc_info=True)
        if driver:
            driver.save_screenshot("general_error.png")
            logging.info("Screenshot 'general_error.png' saved.")
    finally:
        if driver:
            logging.info("Closing WebDriver.")
            driver.quit()
        if csv_file:
            logging.info("Closing CSV file.")
            csv_file.close()
        # Progress file is NOT removed here, allowing resumption

    return scraped_job_count

# --- Main Execution Block ---

if __name__ == "__main__":
    logging.info("--- LinkedIn Job Scraper Initiated ---")

    job_keyword = "data"       # Hardcoded keyword
    job_location = "Saudi Arabia" # Hardcoded location
    output_csv_filename = DEFAULT_OUTPUT_FILENAME # Uses the hardcoded filename
    target_scroll_attempts = 1000 # User-defined limit for scroll attempts

    logging.info(f"Searching for '{job_keyword}' jobs in '{job_location}'.")
    logging.info(f"Output will be saved to '{output_csv_filename}'.")
    logging.info(f"The script will attempt to scroll/load content up to {target_scroll_attempts} times.")

    # IMPORTANT: Delete linkedin_scraper_progress.txt and data_saudi_arabia_jobs.csv
    # if you want a fresh start and verify initial data population.
    # Otherwise, it will try to resume based on the progress file.

    start_time = time.time()
    total_new_jobs_scraped = scrape_linkedin_jobs(job_keyword, job_location, output_csv_filename, max_scroll_attempts=target_scroll_attempts)
    end_time = time.time()
    duration = end_time - start_time

    if total_new_jobs_scraped > 0:
        logging.info(f"\nScraping complete! Data saved to {output_csv_filename}")
        logging.info(f"Total NEW jobs found and saved: {total_new_jobs_scraped}")
    else:
        logging.info(f"\nNo NEW jobs found or an error occurred during scraping. Check logs for details.")
    logging.info(f"Script finished in {duration:.2f} seconds.")
    logging.info("--- LinkedIn Job Scraper Finished ---")
