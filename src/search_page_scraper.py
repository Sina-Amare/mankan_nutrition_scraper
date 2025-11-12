"""Scraper for search pages to extract all valid food IDs."""

import re
import time
from pathlib import Path
from typing import List, Set
from urllib.parse import urljoin

from playwright.sync_api import sync_playwright, Page, TimeoutError as PlaywrightTimeout
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from src.logger_config import get_logger

logger = get_logger(__name__)


class SearchPageScraper:
    """Scrapes search pages to extract all valid food IDs."""
    
    BASE_URL = "https://www.mankan.me/mag/lib/search.php"
    
    def __init__(self):
        """Initialize search page scraper."""
        self.playwright = None
        self.browser = None
        self.page = None
    
    def _init_browser(self):
        """Initialize browser."""
        if self.browser is None:
            self.playwright = sync_playwright().start()
            self.browser = self.playwright.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-blink-features=AutomationControlled"]
            )
            self.page = self.browser.new_page()
            logger.debug("Browser initialized for search page scraping")
    
    def _close_browser(self):
        """Close browser."""
        if self.page:
            self.page.close()
        if self.browser:
            self.browser.close()
        if self.playwright:
            self.playwright.stop()
    
    def extract_food_ids_from_page(self, page: Page) -> Set[int]:
        """Extract all food IDs from a search results page.
        
        Args:
            page: Playwright Page object with loaded search results
        Returns:
            Set of food IDs found on the page
        """
        food_ids = set()
        
        try:
            # Method 1: Extract from href attributes
            # Pattern: <a href="read_one.php?id=XXXX">
            links = page.query_selector_all('a[href*="read_one.php?id="]')
            
            for link in links:
                href = link.get_attribute("href")
                if href:
                    # Extract ID from href
                    match = re.search(r'id=(\d+)', href)
                    if match:
                        food_id = int(match.group(1))
                        food_ids.add(food_id)
            
            # Method 2: Also check the HTML content directly
            content = page.content()
            # Find all occurrences of read_one.php?id=XXXX
            id_matches = re.findall(r'read_one\.php\?id=(\d+)', content)
            for match in id_matches:
                food_ids.add(int(match))
            
            logger.debug(f"Extracted {len(food_ids)} food IDs from page")
            
        except Exception as e:
            logger.error(f"Error extracting food IDs from page: {e}", exc_info=True)
        
        return food_ids
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((PlaywrightTimeout, Exception)),
        reraise=False
    )
    def scrape_search_page(self, page_num: int) -> Set[int]:
        """Scrape a single search page and extract food IDs.
        
        Args:
            page_num: Page number to scrape (1-indexed)
        Returns:
            Set of food IDs found on the page
        """
        if self.page is None:
            self._init_browser()
        
        url = f"{self.BASE_URL}?keyword=&page={page_num}"
        
        try:
            response = self.page.goto(url, wait_until="domcontentloaded", timeout=30000)
            
            if response and response.status != 200:
                logger.warning(f"Page {page_num} returned status {response.status}")
                return set()
            
            # Wait for content to load
            self.page.wait_for_timeout(1000)
            
            # Extract food IDs
            food_ids = self.extract_food_ids_from_page(self.page)
            
            return food_ids
            
        except PlaywrightTimeout:
            logger.warning(f"Timeout loading search page {page_num}")
            return set()
        except Exception as e:
            logger.warning(f"Error scraping search page {page_num}: {e}")
            return set()
    
    def get_total_pages(self) -> int:
        """Get total number of search pages.
        
        Returns:
            Total number of pages, or 238 if unable to determine
        """
        if self.page is None:
            self._init_browser()
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Load first page to get pagination info
                url = f"{self.BASE_URL}?keyword=&page=1"
                self.page.goto(url, wait_until="domcontentloaded", timeout=30000)
                self.page.wait_for_timeout(2000)
                
                # Look for pagination info: "برگه 1 از 238"
                content = self.page.content()
                match = re.search(r'برگه\s+\d+\s+از\s+(\d+)', content)
                if match:
                    total_pages = int(match.group(1))
                    logger.info(f"Found {total_pages} total pages")
                    return total_pages
                
                # Fallback: check pagination links
                pagination = self.page.query_selector('.pages-info')
                if pagination:
                    text = pagination.inner_text()
                    match = re.search(r'از\s+(\d+)', text)
                    if match:
                        total_pages = int(match.group(1))
                        logger.info(f"Found {total_pages} total pages from pagination")
                        return total_pages
                
                # Try alternative: look for last page link
                last_page_link = self.page.query_selector('a.exc:has-text("آخرین")')
                if last_page_link:
                    href = last_page_link.get_attribute("href")
                    if href:
                        match = re.search(r'page=(\d+)', href)
                        if match:
                            total_pages = int(match.group(1))
                            logger.info(f"Found {total_pages} total pages from last link")
                            return total_pages
                
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} to get total pages failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2)
                    continue
        
        # Default to 238 based on user's information
        logger.warning("Could not determine total pages. Using default 238")
        return 238
    
    def scrape_all_pages(self, start_page: int = 1, end_page: int = None) -> List[int]:
        """Scrape all search pages and collect all food IDs.
        
        Args:
            start_page: Starting page number (default: 1)
            end_page: Ending page number (default: auto-detect)
        Returns:
            Sorted list of all unique food IDs
        """
        self._init_browser()
        
        try:
            # Get total pages if not specified
            if end_page is None:
                end_page = self.get_total_pages()
            
            logger.info(f"Scraping search pages {start_page} to {end_page}")
            
            all_food_ids = set()
            failed_pages = []
            
            for page_num in range(start_page, end_page + 1):
                try:
                    logger.info(f"Scraping page {page_num}/{end_page}...")
                    
                    food_ids = self.scrape_search_page(page_num)
                    
                    if food_ids:
                        all_food_ids.update(food_ids)
                        logger.info(f"Page {page_num}: Found {len(food_ids)} items (Total unique: {len(all_food_ids)})")
                    else:
                        logger.warning(f"Page {page_num}: No food IDs found")
                        failed_pages.append(page_num)
                    
                    # Save progress every 10 pages
                    if page_num % 10 == 0:
                        self.save_food_ids(sorted(list(all_food_ids)), 
                                         Path(f"data/food_ids_progress_{page_num}.txt"))
                    
                    # Delay between pages to avoid rate limiting
                    time.sleep(1.0)
                    
                except Exception as e:
                    logger.error(f"Failed to scrape page {page_num}: {e}")
                    failed_pages.append(page_num)
                    # Continue with next page
                    continue
            
            # Retry failed pages once
            if failed_pages:
                logger.info(f"Retrying {len(failed_pages)} failed pages...")
                time.sleep(5)  # Wait before retry
                
                for page_num in failed_pages[:]:  # Copy list to modify during iteration
                    try:
                        logger.info(f"Retrying page {page_num}...")
                        food_ids = self.scrape_search_page(page_num)
                        if food_ids:
                            all_food_ids.update(food_ids)
                            failed_pages.remove(page_num)
                            logger.info(f"Page {page_num} retry successful: Found {len(food_ids)} items")
                        time.sleep(1.0)
                    except Exception as e:
                        logger.error(f"Page {page_num} retry also failed: {e}")
            
            # Convert to sorted list
            sorted_ids = sorted(list(all_food_ids))
            logger.info(f"Scraping complete! Found {len(sorted_ids)} unique food IDs")
            
            if failed_pages:
                logger.warning(f"Failed to scrape {len(failed_pages)} pages: {failed_pages}")
            
            return sorted_ids
            
        finally:
            self._close_browser()
    
    def save_food_ids(self, food_ids: List[int], filepath: Path = Path("data/food_ids.txt")):
        """Save food IDs to a file.
        
        Args:
            food_ids: List of food IDs to save
            filepath: Path to save file
        """
        filepath.parent.mkdir(parents=True, exist_ok=True)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            for food_id in food_ids:
                f.write(f"{food_id}\n")
        
        logger.info(f"Saved {len(food_ids)} food IDs to {filepath}")

