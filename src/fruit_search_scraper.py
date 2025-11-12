"""Scraper for fruit search pages to extract all valid fruit IDs using requests library."""

import re
import time
import json
from pathlib import Path
from typing import List, Set, Dict

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


class FruitSearchPageScraper:
    """Scrapes fruit search pages to extract all valid fruit IDs using requests library."""
    
    BASE_URL = "https://www.mankan.me/mag/lib/search_fruit.php"
    CHECKPOINT_FILE = Path("data/fruit_search_page_checkpoint.json")
    
    def __init__(self):
        """Initialize fruit search page scraper."""
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
                "fruit_ids": [],
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
                "fruit_ids": [],
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
    
    def extract_fruit_ids_from_html(self, html: str) -> Set[int]:
        """Extract all fruit IDs from HTML content.
        
        Args:
            html: HTML content of search page
        Returns:
            Set of fruit IDs found on the page
        """
        fruit_ids = set()
        
        try:
            # Use BeautifulSoup for reliable parsing
            soup = BeautifulSoup(html, 'html.parser')
            
            # Find all search-result-box divs (each contains one fruit item)
            result_boxes = soup.find_all('div', class_='search-result-box')
            
            for box in result_boxes:
                # Find link inside the box - pattern: read_one.php?id=X&type=fruit
                link = box.find('a', href=re.compile(r'read_one\.php\?id=\d+&type=fruit'))
                if link:
                    href = link.get('href', '')
                    match = re.search(r'id=(\d+)', href)
                    if match:
                        fruit_ids.add(int(match.group(1)))
            
            # Also use regex as fallback
            id_matches = re.findall(r'read_one\.php\?id=(\d+)&type=fruit', html)
            for match in id_matches:
                fruit_ids.add(int(match))
            
            logger.debug(f"Extracted {len(fruit_ids)} fruit IDs from page")
            
            # Warn if we don't find expected number
            if len(fruit_ids) != 8 and len(fruit_ids) > 0 and len(fruit_ids) != 1:
                logger.warning(f"Found {len(fruit_ids)} IDs instead of expected 8 (or 1 for last page)")
            
        except Exception as e:
            logger.error(f"Error extracting fruit IDs from HTML: {e}", exc_info=True)
        
        return fruit_ids
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((requests.RequestException, Exception)),
        reraise=False
    )
    def scrape_search_page(self, page_num: int) -> Set[int]:
        """Scrape a single search page and extract fruit IDs.
        
        Args:
            page_num: Page number to scrape (1-indexed)
        Returns:
            Set of fruit IDs found on the page
        """
        url = f"{self.BASE_URL}?keyword=&page={page_num}"
        
        try:
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            
            # Extract fruit IDs from HTML
            fruit_ids = self.extract_fruit_ids_from_html(response.text)
            
            return fruit_ids
            
        except requests.Timeout:
            logger.warning(f"Timeout loading fruit search page {page_num}")
            return set()
        except requests.RequestException as e:
            logger.warning(f"Error scraping fruit search page {page_num}: {e}")
            return set()
        except Exception as e:
            logger.warning(f"Unexpected error scraping fruit search page {page_num}: {e}")
            return set()
    
    def get_total_pages(self) -> int:
        """Get total number of fruit search pages.
        
        Returns:
            Total number of pages, or 14 if unable to determine
        """
        max_retries = 3
        for attempt in range(max_retries):
            try:
                url = f"{self.BASE_URL}?keyword=&page=1"
                response = self.session.get(url, timeout=15)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Look for pagination info: "برگه 1 از 14"
                text = response.text
                match = re.search(r'برگه\s+\d+\s+از\s+(\d+)', text)
                if match:
                    total_pages = int(match.group(1))
                    logger.info(f"Found {total_pages} total fruit pages")
                    return total_pages
                
                # Check pagination element
                pagination = soup.select_one('.pages-info')
                if pagination:
                    text = pagination.get_text()
                    match = re.search(r'از\s+(\d+)', text)
                    if match:
                        total_pages = int(match.group(1))
                        logger.info(f"Found {total_pages} total fruit pages from pagination")
                        return total_pages
                
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} to get total pages failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2)
                    continue
        
        # Default to 14 based on user's information
        logger.warning("Could not determine total pages. Using default 14")
        return 14
    
    def scrape_all_pages(self, start_page: int = 1, end_page: int = None, resume: bool = True) -> List[int]:
        """Scrape all fruit search pages and collect all fruit IDs.
        
        Args:
            start_page: Starting page number (default: 1)
            end_page: Ending page number (default: auto-detect)
            resume: Whether to resume from checkpoint (default: True)
        Returns:
            Sorted list of all unique fruit IDs
        """
        # Load checkpoint if resuming
        checkpoint = self.load_checkpoint() if resume else {
            "scraped_pages": [],
            "fruit_ids": [],
            "failed_pages": [],
            "last_page": 0
        }
        
        all_fruit_ids = set(checkpoint.get("fruit_ids", []))
        scraped_pages = set(checkpoint.get("scraped_pages", []))
        failed_pages = checkpoint.get("failed_pages", [])
        
        # Get total pages if not specified
        if end_page is None:
            end_page = self.get_total_pages()
        
        # Adjust start page if resuming
        if resume and scraped_pages:
            max_scraped = max(scraped_pages)
            start_page = max(start_page, min(max_scraped + 1, end_page + 1))
            logger.info(f"Resuming: {len(scraped_pages)} pages already scraped, starting from page {start_page}")
        
        logger.info(f"Scraping fruit search pages {start_page} to {end_page}")
        logger.info(f"Already have {len(all_fruit_ids)} fruit IDs from checkpoint")
        
        try:
            for page_num in range(start_page, end_page + 1):
                if page_num in scraped_pages:
                    logger.debug(f"Skipping page {page_num} (already scraped)")
                    continue
                
                try:
                    logger.info(f"Scraping fruit page {page_num}/{end_page}...")
                    
                    fruit_ids = self.scrape_search_page(page_num)
                    
                    if fruit_ids:
                        all_fruit_ids.update(fruit_ids)
                        scraped_pages.add(page_num)
                        logger.info(f"Page {page_num}: Found {len(fruit_ids)} items (Total unique: {len(all_fruit_ids)})")
                    else:
                        logger.warning(f"Page {page_num}: No fruit IDs found")
                        if page_num not in failed_pages:
                            failed_pages.append(page_num)
                    
                    # Save checkpoint every 5 pages
                    if page_num % 5 == 0:
                        checkpoint_data = {
                            "scraped_pages": sorted(list(scraped_pages)),
                            "fruit_ids": sorted(list(all_fruit_ids)),
                            "failed_pages": failed_pages,
                            "last_page": page_num
                        }
                        self.save_checkpoint(checkpoint_data)
                        logger.info(f"Checkpoint saved at page {page_num} ({len(all_fruit_ids)} IDs so far)")
                    
                    time.sleep(0.5)
                    
                except Exception as e:
                    logger.error(f"Failed to scrape page {page_num}: {e}")
                    if page_num not in failed_pages:
                        failed_pages.append(page_num)
                    continue
            
            # Retry failed pages
            if failed_pages:
                logger.info(f"Retrying {len(failed_pages)} failed pages...")
                time.sleep(3)
                
                retry_failed = failed_pages.copy()
                for page_num in retry_failed:
                    try:
                        logger.info(f"Retrying page {page_num}...")
                        fruit_ids = self.scrape_search_page(page_num)
                        if fruit_ids:
                            all_fruit_ids.update(fruit_ids)
                            scraped_pages.add(page_num)
                            failed_pages.remove(page_num)
                            logger.info(f"Page {page_num} retry successful: Found {len(fruit_ids)} items")
                        time.sleep(0.5)
                    except Exception as e:
                        logger.error(f"Page {page_num} retry also failed: {e}")
                
                checkpoint_data = {
                    "scraped_pages": sorted(list(scraped_pages)),
                    "fruit_ids": sorted(list(all_fruit_ids)),
                    "failed_pages": failed_pages,
                    "last_page": end_page
                }
                self.save_checkpoint(checkpoint_data)
            
            # Final checkpoint save
            checkpoint_data = {
                "scraped_pages": sorted(list(scraped_pages)),
                "fruit_ids": sorted(list(all_fruit_ids)),
                "failed_pages": failed_pages,
                "last_page": end_page
            }
            self.save_checkpoint(checkpoint_data)
            
            # Convert to sorted list
            sorted_ids = sorted(list(all_fruit_ids))
            logger.info(f"Scraping complete! Found {len(sorted_ids)} unique fruit IDs")
            
            if failed_pages:
                logger.warning(f"Failed to scrape {len(failed_pages)} pages: {failed_pages}")
            
            return sorted_ids
            
        finally:
            self.session.close()
    
    def save_fruit_ids(self, fruit_ids: List[int], filepath: Path = Path("data/fruit_ids.txt")):
        """Save fruit IDs to a file.
        
        Args:
            fruit_ids: List of fruit IDs to save
            filepath: Path to save file
        """
        filepath.parent.mkdir(parents=True, exist_ok=True)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            for fruit_id in fruit_ids:
                f.write(f"{fruit_id}\n")
        
        logger.info(f"Saved {len(fruit_ids)} fruit IDs to {filepath}")

