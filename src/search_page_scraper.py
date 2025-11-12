"""Scraper for search pages to extract all valid food IDs using requests library."""

import re
import time
import json
from pathlib import Path
from typing import List, Set, Dict
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from src.logger_config import get_logger

logger = get_logger(__name__)


class SearchPageScraper:
    """Scrapes search pages to extract all valid food IDs using requests library."""
    
    BASE_URL = "https://www.mankan.me/mag/lib/search.php"
    CHECKPOINT_FILE = Path("data/search_page_checkpoint.json")
    
    def __init__(self):
        """Initialize search page scraper."""
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.checkpoint_file = self.CHECKPOINT_FILE
        self.checkpoint_file.parent.mkdir(parents=True, exist_ok=True)
    
    def load_checkpoint(self) -> Dict:
        """Load checkpoint data.
        
        Returns:
            Dictionary with checkpoint data
        """
        if not self.checkpoint_file.exists():
            return {
                "scraped_pages": [],
                "food_ids": [],
                "failed_pages": [],
                "last_page": 0
            }
        
        try:
            with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Error loading checkpoint: {e}. Starting fresh.")
            return {
                "scraped_pages": [],
                "food_ids": [],
                "failed_pages": [],
                "last_page": 0
            }
    
    def save_checkpoint(self, data: Dict):
        """Save checkpoint data.
        
        Args:
            data: Dictionary with checkpoint data
        """
        try:
            with open(self.checkpoint_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Error saving checkpoint: {e}")
    
    def extract_food_ids_from_html(self, html: str) -> Set[int]:
        """Extract all food IDs from HTML content.
        
        Args:
            html: HTML content of search page
        Returns:
            Set of food IDs found on the page
        """
        food_ids = set()
        
        try:
            # Method 1: Use BeautifulSoup for reliable parsing
            soup = BeautifulSoup(html, 'html.parser')
            
            # Find all search-result-box divs (each contains one food item)
            result_boxes = soup.find_all('div', class_='search-result-box')
            
            for box in result_boxes:
                # Find link inside the box
                link = box.find('a', href=re.compile(r'read_one\.php\?id=\d+'))
                if link:
                    href = link.get('href', '')
                    match = re.search(r'id=(\d+)', href)
                    if match:
                        food_ids.add(int(match.group(1)))
            
            # Method 2: Also use regex as fallback to catch any we might have missed
            id_matches = re.findall(r'read_one\.php\?id=(\d+)', html)
            for match in id_matches:
                food_ids.add(int(match))
            
            # Method 3: Find all links with read_one.php (additional safety)
            links = soup.find_all('a', href=re.compile(r'read_one\.php\?id=\d+'))
            for link in links:
                href = link.get('href', '')
                match = re.search(r'id=(\d+)', href)
                if match:
                    food_ids.add(int(match.group(1)))
            
            logger.debug(f"Extracted {len(food_ids)} food IDs from page")
            
            # Warn if we don't find exactly 8 (most pages should have 8)
            if len(food_ids) != 8 and len(food_ids) > 0:
                logger.warning(f"Found {len(food_ids)} IDs instead of expected 8")
            
        except Exception as e:
            logger.error(f"Error extracting food IDs from HTML: {e}", exc_info=True)
        
        return food_ids
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((requests.RequestException, Exception)),
        reraise=False
    )
    def scrape_search_page(self, page_num: int) -> Set[int]:
        """Scrape a single search page and extract food IDs.
        
        Args:
            page_num: Page number to scrape (1-indexed)
        Returns:
            Set of food IDs found on the page
        """
        url = f"{self.BASE_URL}?keyword=&page={page_num}"
        
        try:
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            
            # Extract food IDs from HTML
            food_ids = self.extract_food_ids_from_html(response.text)
            
            return food_ids
            
        except requests.Timeout:
            logger.warning(f"Timeout loading search page {page_num}")
            return set()
        except requests.RequestException as e:
            logger.warning(f"Error scraping search page {page_num}: {e}")
            return set()
        except Exception as e:
            logger.warning(f"Unexpected error scraping search page {page_num}: {e}")
            return set()
    
    def get_total_pages(self) -> int:
        """Get total number of search pages.
        
        Returns:
            Total number of pages, or 238 if unable to determine
        """
        max_retries = 3
        for attempt in range(max_retries):
            try:
                url = f"{self.BASE_URL}?keyword=&page=1"
                response = self.session.get(url, timeout=15)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Method 1: Look for pagination info: "برگه 1 از 238"
                text = response.text
                match = re.search(r'برگه\s+\d+\s+از\s+(\d+)', text)
                if match:
                    total_pages = int(match.group(1))
                    logger.info(f"Found {total_pages} total pages")
                    return total_pages
                
                # Method 2: Check pagination element
                pagination = soup.select_one('.pages-info')
                if pagination:
                    text = pagination.get_text()
                    match = re.search(r'از\s+(\d+)', text)
                    if match:
                        total_pages = int(match.group(1))
                        logger.info(f"Found {total_pages} total pages from pagination")
                        return total_pages
                
                # Method 3: Look for last page link
                last_link = soup.select_one('a.exc:contains("آخرین")')
                if not last_link:
                    # Try finding link with "آخرین" text
                    for link in soup.find_all('a', class_='exc'):
                        if 'آخرین' in link.get_text():
                            href = link.get('href', '')
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
    
    def scrape_all_pages(self, start_page: int = 1, end_page: int = None, resume: bool = True) -> List[int]:
        """Scrape all search pages and collect all food IDs.
        
        Args:
            start_page: Starting page number (default: 1)
            end_page: Ending page number (default: auto-detect)
            resume: Whether to resume from checkpoint (default: True)
        Returns:
            Sorted list of all unique food IDs
        """
        # Load checkpoint if resuming
        checkpoint = self.load_checkpoint() if resume else {
            "scraped_pages": [],
            "food_ids": [],
            "failed_pages": [],
            "last_page": 0
        }
        
        all_food_ids = set(checkpoint.get("food_ids", []))
        scraped_pages = set(checkpoint.get("scraped_pages", []))
        failed_pages = checkpoint.get("failed_pages", [])
        
        # Get total pages if not specified
        if end_page is None:
            end_page = self.get_total_pages()
        
        # Adjust start page if resuming - but ensure we scrape ALL pages
        # Don't use last_page, use the actual scraped_pages set to determine what's missing
        if resume and scraped_pages:
            # Find the highest page number that's been scraped
            max_scraped = max(scraped_pages)
            # Start from the next unscraped page, but ensure we go to end_page
            start_page = max(start_page, min(max_scraped + 1, end_page + 1))
            logger.info(f"Resuming: {len(scraped_pages)} pages already scraped, starting from page {start_page}")
        
        logger.info(f"Scraping search pages {start_page} to {end_page}")
        logger.info(f"Already have {len(all_food_ids)} food IDs from checkpoint")
        
        try:
            for page_num in range(start_page, end_page + 1):
                # Skip if already scraped
                if page_num in scraped_pages:
                    logger.debug(f"Skipping page {page_num} (already scraped)")
                    continue
                
                try:
                    logger.info(f"Scraping page {page_num}/{end_page}...")
                    
                    food_ids = self.scrape_search_page(page_num)
                    
                    if food_ids:
                        all_food_ids.update(food_ids)
                        scraped_pages.add(page_num)
                        logger.info(f"Page {page_num}: Found {len(food_ids)} items (Total unique: {len(all_food_ids)})")
                    else:
                        logger.warning(f"Page {page_num}: No food IDs found")
                        if page_num not in failed_pages:
                            failed_pages.append(page_num)
                    
                    # Save checkpoint every 5 pages (more frequent to avoid data loss)
                    if page_num % 5 == 0:
                        checkpoint_data = {
                            "scraped_pages": sorted(list(scraped_pages)),
                            "food_ids": sorted(list(all_food_ids)),
                            "failed_pages": failed_pages,
                            "last_page": page_num
                        }
                        self.save_checkpoint(checkpoint_data)
                        logger.info(f"Checkpoint saved at page {page_num} ({len(all_food_ids)} IDs so far)")
                    
                    # Delay between pages to avoid rate limiting
                    time.sleep(0.5)
                    
                except Exception as e:
                    logger.error(f"Failed to scrape page {page_num}: {e}")
                    if page_num not in failed_pages:
                        failed_pages.append(page_num)
                    # Continue with next page
                    continue
            
            # Retry failed pages once
            if failed_pages:
                logger.info(f"Retrying {len(failed_pages)} failed pages...")
                time.sleep(3)  # Wait before retry
                
                retry_failed = failed_pages.copy()
                for page_num in retry_failed:
                    try:
                        logger.info(f"Retrying page {page_num}...")
                        food_ids = self.scrape_search_page(page_num)
                        if food_ids:
                            all_food_ids.update(food_ids)
                            scraped_pages.add(page_num)
                            failed_pages.remove(page_num)
                            logger.info(f"Page {page_num} retry successful: Found {len(food_ids)} items")
                        else:
                            logger.warning(f"Page {page_num} retry still found no IDs")
                        time.sleep(0.5)
                    except Exception as e:
                        logger.error(f"Page {page_num} retry also failed: {e}")
                
                # Save checkpoint after retry
                checkpoint_data = {
                    "scraped_pages": sorted(list(scraped_pages)),
                    "food_ids": sorted(list(all_food_ids)),
                    "failed_pages": failed_pages,
                    "last_page": end_page
                }
                self.save_checkpoint(checkpoint_data)
            
            # Final checkpoint save
            checkpoint_data = {
                "scraped_pages": sorted(list(scraped_pages)),
                "food_ids": sorted(list(all_food_ids)),
                "failed_pages": failed_pages,
                "last_page": end_page
            }
            self.save_checkpoint(checkpoint_data)
            
            # Convert to sorted list
            sorted_ids = sorted(list(all_food_ids))
            logger.info(f"Scraping complete! Found {len(sorted_ids)} unique food IDs")
            
            if failed_pages:
                logger.warning(f"Failed to scrape {len(failed_pages)} pages: {failed_pages}")
            
            return sorted_ids
            
        finally:
            self.session.close()
    
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
