"""Robust scraper for Mankan.me with multi-strategy extraction and error handling."""

import random
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from playwright.sync_api import Browser, BrowserContext, Page, Playwright, TimeoutError as PlaywrightTimeout
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from src.checkpoint import CheckpointManager
from src.data_processor import DataProcessor
from src.logger_config import get_logger

logger = get_logger(__name__)


class RobustMankanScraper:
    """Robust scraper with multi-strategy extraction and graceful error handling."""
    
    BASE_URL = "https://www.mankan.me/mag/lib/read_one.php"
    
    def __init__(
        self,
        start_id: int = 3,
        end_id: int = 1967,
        checkpoint_manager: Optional[CheckpointManager] = None,
        request_delay: tuple = (0.5, 1.5),
        checkpoint_frequency: int = 50,
    ):
        """Initialize robust scraper."""
        self.start_id = start_id
        self.end_id = end_id
        self.checkpoint_manager = checkpoint_manager or CheckpointManager()
        self.request_delay = request_delay
        self.checkpoint_frequency = checkpoint_frequency
        
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None
        self.playwright: Optional[Playwright] = None
        
        self.data_processor = DataProcessor()
        self.scraped_data: List[Dict[str, Any]] = []
        self.completed_ids: List[int] = []
        self.failed_ids: List[int] = []
        self.skipped_ids: List[int] = []
        
        # Load existing checkpoint
        checkpoint_data = self.checkpoint_manager.load()
        self.completed_ids = checkpoint_data.get("completed_ids", [])
        self.scraped_data = checkpoint_data.get("data", [])
        
        logger.info(
            f"Robust scraper initialized: IDs {start_id}-{end_id}, "
            f"{len(self.completed_ids)} already completed"
        )
    
    def _init_browser(self):
        """Initialize Playwright browser."""
        if self.browser is None:
            from playwright.sync_api import sync_playwright
            self.playwright = sync_playwright().start()
            self.browser = self.playwright.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-blink-features=AutomationControlled"]
            )
            self.context = self.browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            )
            self.page = self.context.new_page()
            logger.debug("Browser initialized")
    
    def _close_browser(self):
        """Close browser and cleanup."""
        if self.page:
            self.page.close()
        if self.context:
            self.context.close()
        if self.browser:
            self.browser.close()
        if self.playwright:
            self.playwright.stop()
        logger.debug("Browser closed")
    
    def _random_delay(self):
        """Add random delay between requests."""
        delay = random.uniform(*self.request_delay)
        time.sleep(delay)
    
    def _is_valid_page(self, page: Page) -> bool:
        """Check if page is valid (not a PHP error or empty page).
        
        Args:
            page: Playwright Page object
        Returns:
            True if page is valid, False otherwise
        """
        try:
            # Check page title
            title = page.title()
            if not title or len(title.strip()) == 0:
                return False
            
            # Check for PHP errors
            body = page.query_selector("body")
            if body:
                body_text = body.inner_text()
                if "Fatal error" in body_text or "Uncaught" in body_text:
                    return False
                
                # Check HTML length (error pages are very short)
                html = body.inner_html()
                if len(html) < 1000:  # Valid pages are much longer
                    return False
            
            return True
        except:
            return False
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=4),
        retry=retry_if_exception_type((PlaywrightTimeout, Exception)),
        reraise=True
    )
    def fetch_food_page(self, food_id: int) -> Optional[Page]:
        """Fetch and load a food item page.
        
        Args:
            food_id: Food item ID
        Returns:
            Playwright Page object or None if page is invalid
        """
        if self.page is None:
            self._init_browser()
        
        url = f"{self.BASE_URL}?id={food_id}"
        logger.debug(f"Fetching: {url}")
        
        try:
            response = self.page.goto(url, wait_until="domcontentloaded", timeout=30000)
            
            # Check HTTP status
            if response and response.status == 404:
                logger.debug(f"ID {food_id}: 404 Not Found")
                return None
            
            self.page.wait_for_load_state("domcontentloaded")
            self.page.wait_for_timeout(300)
            
            # Validate page
            if not self._is_valid_page(self.page):
                logger.debug(f"ID {food_id}: Invalid page (PHP error or empty)")
                return None
            
            return self.page
        except PlaywrightTimeout as e:
            logger.debug(f"Timeout loading page {food_id}: {e}")
            return None
        except Exception as e:
            logger.debug(f"Error loading page {food_id}: {e}")
            return None
    
    def parse_food_name(self, page: Page) -> Optional[str]:
        """Extract food name using multiple strategies.
        
        Args:
            page: Playwright Page object
        Returns:
            Food name or None
        """
        # Strategy 1: Standard selectors
        selectors = ["h1", "h2", "h3", ".header-food h1", ".header-food h2"]
        for selector in selectors:
            try:
                element = page.query_selector(selector)
                if element:
                    text = element.inner_text().strip()
                    if text and 3 < len(text) < 200:
                        return text
            except:
                continue
        
        # Strategy 2: Search in containers
        containers = ["main", ".content", "#content", ".container", "body"]
        for container_sel in containers:
            try:
                container = page.query_selector(container_sel)
                if container:
                    heading = container.query_selector("h1, h2, h3, h4")
                    if heading:
                        text = heading.inner_text().strip()
                        if text and len(text) > 0:
                            return text
            except:
                continue
        
        return None
    
    def get_measurement_options(self, page: Page) -> List[Dict[str, str]]:
        """Extract measurement options with fallbacks.
        
        Args:
            page: Playwright Page object
        Returns:
            List of measurement option dictionaries
        """
        options = []
        
        # Strategy 1: Find select element
        selectors = [
            "select",
            "select[name*='measure']",
            "select[id*='measure']",
            "select[name*='unit']",
            "select[id*='unit']",
        ]
        
        select_element = None
        for selector in selectors:
            try:
                select_element = page.query_selector(selector)
                if select_element:
                    break
            except:
                continue
        
        # Strategy 2: Find ANY select element
        if not select_element:
            try:
                all_selects = page.query_selector_all("select")
                if all_selects:
                    select_element = all_selects[0]
            except:
                pass
        
        if select_element:
            try:
                option_elements = select_element.query_selector_all("option")
                for option in option_elements:
                    value = option.get_attribute("value") or ""
                    text = option.inner_text().strip()
                    if text:
                        options.append({"value": value, "text": text})
            except:
                pass
        
        # Fallback: default option
        if not options:
            options = [{"value": "100", "text": "100 گرم"}]
        
        return options
    
    def extract_nutritional_values(self, page: Page) -> Dict[str, Optional[float]]:
        """Extract nutritional values using multiple strategies.
        
        Args:
            page: Playwright Page object
        Returns:
            Dictionary with nutritional values
        """
        values = {
            "calories": None,
            "carbs_g": None,
            "protein_g": None,
            "fat_g": None,
            "fiber_g": None,
        }
        
        # Strategy 1: ID-based selectors
        field_map = {
            "calories": ["calory-amount", "calor", "cal"],
            "carbs_g": ["carbo-amount", "carbo", "carb"],
            "protein_g": ["protein-amount", "protein", "prot"],
            "fat_g": ["fat-amount", "fat"],
            "fiber_g": ["fiber-amount", "fiber", "fib"],
        }
        
        for field, ids in field_map.items():
            for vid in ids:
                try:
                    # Try ID selector
                    element = page.query_selector(f"#{vid}")
                    if element:
                        text = element.inner_text().strip()
                        if text:
                            numbers = re.findall(r'\d+\.?\d*', text)
                            if numbers:
                                val = float(numbers[0])
                                if 0 <= val <= 10000:
                                    values[field] = val
                                    break
                    
                    # Try attribute selector
                    element = page.query_selector(f"[id*='{vid}']")
                    if element:
                        text = element.inner_text().strip()
                        if text:
                            numbers = re.findall(r'\d+\.?\d*', text)
                            if numbers:
                                val = float(numbers[0])
                                if 0 <= val <= 10000:
                                    values[field] = val
                                    break
                except:
                    continue
        
        # Strategy 2: Text pattern matching (if values still missing)
        if any(v is None for v in values.values()):
            try:
                body = page.query_selector("body")
                if body:
                    content_text = body.inner_text()
                    patterns = {
                        "calories": r'(\d+\.?\d*)\s*(?:کالری|cal)',
                        "carbs_g": r'(\d+\.?\d*)\s*(?:کربوهیدرات|g|گرم)',
                        "protein_g": r'(\d+\.?\d*)\s*(?:پروتئین|g|گرم)',
                        "fat_g": r'(\d+\.?\d*)\s*(?:چربی|g|گرم)',
                        "fiber_g": r'(\d+\.?\d*)\s*(?:فیبر|g|گرم)',
                    }
                    
                    for field, pattern in patterns.items():
                        if values[field] is None:
                            matches = re.findall(pattern, content_text, re.IGNORECASE)
                            if matches:
                                try:
                                    val = float(matches[0])
                                    if 0 <= val <= 10000:
                                        values[field] = val
                                except:
                                    pass
            except:
                pass
        
        return values
    
    def extract_measurement_value(self, measurement_text: str, dropdown_value: Optional[str] = None) -> Optional[float]:
        """Extract numeric value from measurement.
        
        Args:
            measurement_text: Text like "100 گرم"
            dropdown_value: Value from dropdown (preferred)
        Returns:
            Numeric value or None
        """
        if dropdown_value:
            try:
                return float(dropdown_value)
            except:
                pass
        
        numbers = re.findall(r'\d+\.?\d*', measurement_text)
        if numbers:
            try:
                return float(numbers[0])
            except:
                pass
        
        return None
    
    def scrape_food_item(self, food_id: int) -> List[Dict[str, Any]]:
        """Scrape all measurement variants for a food item.
        
        Args:
            food_id: Food item ID
        Returns:
            List of data dictionaries (empty if failed)
        """
        results = []
        
        try:
            # Fetch and validate page
            page = self.fetch_food_page(food_id)
            if not page:
                return results  # Invalid page, return empty
            
            # Extract food name
            food_name = self.parse_food_name(page)
            if not food_name:
                food_name = f"Food {food_id}"  # Fallback name
            
            # Get measurement options
            measurement_options = self.get_measurement_options(page)
            if not measurement_options:
                return results  # No measurements found
            
            # Extract data for each measurement
            for measurement in measurement_options:
                try:
                    # Select measurement if dropdown exists
                    select_element = page.query_selector("select")
                    if select_element and measurement.get("value"):
                        try:
                            select_element.select_option(measurement["value"])
                            page.wait_for_timeout(200)
                        except:
                            pass  # Continue even if selection fails
                    
                    # Extract nutritional values
                    nutritional_values = self.extract_nutritional_values(page)
                    
                    # Extract measurement value
                    measurement_value = self.extract_measurement_value(
                        measurement["text"],
                        measurement.get("value")
                    )
                    
                    # Build row
                    row = {
                        "food_id": food_id,
                        "food_name": food_name,
                        "measurement_unit": measurement["text"],
                        "measurement_value": measurement_value,
                        "calories": nutritional_values["calories"],
                        "carbs_g": nutritional_values["carbs_g"],
                        "protein_g": nutritional_values["protein_g"],
                        "fat_g": nutritional_values["fat_g"],
                        "fiber_g": nutritional_values["fiber_g"],
                    }
                    
                    # Validate and clean
                    cleaned_row = self.data_processor.clean_data(row)
                    if self.data_processor.validate_row(cleaned_row):
                        results.append(cleaned_row)
                
                except Exception as e:
                    logger.debug(f"Error processing measurement for ID {food_id}: {e}")
                    continue
            
        except Exception as e:
            logger.debug(f"Error scraping food item {food_id}: {e}")
        
        return results
    
    def scrape_all(self) -> List[Dict[str, Any]]:
        """Scrape all food items with graceful error handling.
        
        Returns:
            List of all scraped data dictionaries
        """
        self._init_browser()
        
        try:
            total_ids = self.end_id - self.start_id + 1
            skipped = len(self.completed_ids)
            remaining = total_ids - skipped
            
            logger.info(
                f"Starting robust scrape: {remaining} items remaining "
                f"({skipped} already completed)"
            )
            
            for food_id in range(self.start_id, self.end_id + 1):
                # Skip if already completed
                if food_id in self.completed_ids:
                    continue
                
                # Log progress
                current_progress = len(self.completed_ids)
                progress_pct = (current_progress / total_ids * 100) if total_ids > 0 else 0
                logger.info(
                    f"[{current_progress + 1}/{total_ids}] ({progress_pct:.1f}%) "
                    f"Scraping ID {food_id}..."
                )
                
                # Scrape item
                try:
                    item_data = self.scrape_food_item(food_id)
                    
                    if item_data:
                        self.scraped_data.extend(item_data)
                        self.completed_ids.append(food_id)
                        logger.info(
                            f"✓ ID {food_id}: {len(item_data)} measurement(s) extracted "
                            f"(Total: {len(self.scraped_data)} rows)"
                        )
                    else:
                        # No data extracted - likely invalid/deleted page
                        self.skipped_ids.append(food_id)
                        logger.warning(f"⚠ ID {food_id}: No data extracted (skipped)")
                    
                    # Save checkpoint periodically
                    if len(self.completed_ids) % self.checkpoint_frequency == 0:
                        self.checkpoint_manager.save(
                            self.completed_ids,
                            self.scraped_data
                        )
                        logger.info(
                            f"💾 Checkpoint saved: {len(self.completed_ids)}/{total_ids} items "
                            f"({len(self.scraped_data)} data rows, {len(self.skipped_ids)} skipped)"
                        )
                
                except Exception as e:
                    # Log error but continue
                    self.failed_ids.append(food_id)
                    logger.error(f"✗ ID {food_id}: Error - {e}")
                    # Continue to next item (don't stop)
                
                # Random delay
                self._random_delay()
            
            # Final checkpoint
            self.checkpoint_manager.save(
                self.completed_ids,
                self.scraped_data,
                force=True
            )
            
            # Summary
            logger.info("")
            logger.info("=" * 60)
            logger.info("Robust Scraping Complete!")
            logger.info(f"  ✓ Completed: {len(self.completed_ids)} items")
            logger.info(f"  ⚠ Skipped: {len(self.skipped_ids)} items (invalid/deleted)")
            logger.info(f"  ✗ Failed: {len(self.failed_ids)} items")
            logger.info(f"  📊 Total data rows: {len(self.scraped_data)}")
            logger.info("=" * 60)
            
            if self.skipped_ids:
                skipped_str = ", ".join(map(str, self.skipped_ids[:20]))
                if len(self.skipped_ids) > 20:
                    skipped_str += f" ... ({len(self.skipped_ids)} total)"
                logger.info(f"Skipped IDs: {skipped_str}")
            if self.failed_ids:
                failed_str = ", ".join(map(str, self.failed_ids[:20]))
                if len(self.failed_ids) > 20:
                    failed_str += f" ... ({len(self.failed_ids)} total)"
                logger.info(f"Failed IDs: {failed_str}")
            
        finally:
            self._close_browser()
        
        return self.scraped_data

