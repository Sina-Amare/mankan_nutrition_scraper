"""Main scraper for Mankan.me nutritional database.

Uses Playwright to handle JavaScript-driven dropdown interactions.
"""

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


class MankanScraper:
    """Scraper for Mankan.me nutritional database."""
    
    BASE_URL = "https://www.mankan.me/mag/lib/read_one.php"
    
    def __init__(
        self,
        start_id: int = 3,
        end_id: int = 1967,
        checkpoint_manager: Optional[CheckpointManager] = None,
        request_delay: tuple = (0.5, 1.5),  # Random delay between (min, max) seconds - optimized for speed
        checkpoint_frequency: int = 50,  # Save checkpoint every N items
    ):
        """Initialize scraper.
        
        Args:
            start_id: Starting food item ID
            end_id: Ending food item ID (inclusive)
            checkpoint_manager: Checkpoint manager instance
            request_delay: Tuple of (min, max) seconds for random delay
            checkpoint_frequency: Save checkpoint every N items
        """
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
        
        # Load existing checkpoint
        checkpoint_data = self.checkpoint_manager.load()
        self.completed_ids = checkpoint_data.get("completed_ids", [])
        self.scraped_data = checkpoint_data.get("data", [])
        
        logger.info(
            f"Scraper initialized: IDs {start_id}-{end_id}, "
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
            logger.info("Browser initialized")
    
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
        logger.info("Browser closed")
    
    def _investigate_failed_page(self, food_id: int):
        """Investigate a failed page to understand its structure.
        
        Args:
            food_id: Food item ID that failed
        """
        logger.info(f"Investigating failed page ID {food_id}...")
        
        try:
            if self.page is None:
                self._init_browser()
            
            url = f"{self.BASE_URL}?id={food_id}"
            logger.info(f"Loading page: {url}")
            
            # Try to load the page
            try:
                self.page.goto(url, wait_until="domcontentloaded", timeout=30000)
                self.page.wait_for_load_state("domcontentloaded")
                self.page.wait_for_timeout(1000)  # Wait a bit longer for investigation
            except Exception as e:
                logger.error(f"Could not load page: {e}")
                return
            
            # Check page title
            title = self.page.title()
            logger.info(f"Page title: {title}")
            
            # Check if page has content
            body_text = self.page.query_selector("body")
            if body_text:
                body_html = body_text.inner_html()
                logger.info(f"Body HTML length: {len(body_html)} characters")
                
                # Check for common elements
                h1 = self.page.query_selector("h1")
                h2 = self.page.query_selector("h2")
                h3 = self.page.query_selector("h3")
                select = self.page.query_selector("select")
                
                logger.info(f"Elements found - h1: {h1 is not None}, h2: {h2 is not None}, "
                           f"h3: {h3 is not None}, select: {select is not None}")
                
                # Try to find food name with different selectors
                logger.info("Trying to find food name...")
                name_selectors = ["h1", "h2", "h3", ".food-name", "[class*='title']", 
                                "[class*='name']", "title"]
                for selector in name_selectors:
                    element = self.page.query_selector(selector)
                    if element:
                        text = element.inner_text().strip()
                        if text:
                            logger.info(f"Found food name with '{selector}': {text}")
                
                # Check for measurement dropdown
                logger.info("Checking for measurement dropdown...")
                if select:
                    options = select.query_selector_all("option")
                    logger.info(f"Found {len(options)} options in select")
                    for i, opt in enumerate(options[:5]):  # First 5
                        logger.info(f"  Option {i}: value='{opt.get_attribute('value')}', "
                                  f"text='{opt.inner_text().strip()}'")
                else:
                    logger.warning("No select element found!")
                    # Try alternative selectors
                    all_selects = self.page.query_selector_all("select")
                    logger.info(f"Total select elements on page: {len(all_selects)}")
                
                # Check for nutritional values
                logger.info("Checking for nutritional values...")
                value_ids = ["calory-amount", "carbo-amount", "protein-amount", 
                           "fat-amount", "fiber-amount"]
                for vid in value_ids:
                    element = self.page.query_selector(f"#{vid}")
                    if element:
                        text = element.inner_text().strip()
                        logger.info(f"  {vid}: {text}")
                    else:
                        logger.warning(f"  {vid}: NOT FOUND")
                
                # Save page HTML for inspection
                html_file = f"data/logs/failed_page_{food_id}.html"
                Path(html_file).parent.mkdir(parents=True, exist_ok=True)
                with open(html_file, "w", encoding="utf-8") as f:
                    f.write(self.page.content())
                logger.info(f"Page HTML saved to: {html_file}")
                
        except Exception as e:
            logger.error(f"Error during investigation: {e}")
    
    def _random_delay(self):
        """Add random delay between requests."""
        delay = random.uniform(*self.request_delay)
        time.sleep(delay)
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=4),
        retry=retry_if_exception_type((PlaywrightTimeout, Exception)),
        reraise=True
    )
    def fetch_food_page(self, food_id: int) -> Page:
        """Fetch and load a food item page.
        
        Args:
            food_id: Food item ID
        Returns:
            Playwright Page object
        Raises:
            Exception: If page load fails after retries
        """
        if self.page is None:
            self._init_browser()
        
        url = f"{self.BASE_URL}?id={food_id}"
        logger.debug(f"Fetching: {url}")
        
        try:
            # Use 'domcontentloaded' for faster loading, then wait for specific elements
            self.page.goto(url, wait_until="domcontentloaded", timeout=30000)
            # Wait for page to be interactive (more flexible than waiting for specific elements)
            self.page.wait_for_load_state("domcontentloaded")
            # Small wait for any dynamic content
            self.page.wait_for_timeout(300)  # Reduced from implicit waits
            return self.page
        except PlaywrightTimeout as e:
            logger.error(f"Timeout loading page {food_id}: {e}")
            raise
        except Exception as e:
            logger.error(f"Error loading page {food_id}: {e}")
            raise
    
    def parse_food_name(self, page: Page) -> Optional[str]:
        """Extract food name from page.
        
        Args:
            page: Playwright Page object
        Returns:
            Food name or None if not found
        """
        try:
            # Try multiple selectors for food name (expanded list)
            selectors = [
                "h1",
                "h2",
                "h3",
                ".food-name",
                "[class*='title']",
                "[class*='name']",
                "title",
                ".header-food h1",
                ".header-food h2",
                "[id*='food']",
                "[id*='title']",
            ]
            
            for selector in selectors:
                try:
                    element = page.query_selector(selector)
                    if element:
                        text = element.inner_text().strip()
                        if text and len(text) > 0 and len(text) < 200:  # Reasonable length
                            return text
                except:
                    continue
            
            # Fallback: try to find text in main content area
            main_selectors = ["main", ".content", "#content", ".container", "body"]
            for main_selector in main_selectors:
                try:
                    main_content = page.query_selector(main_selector)
                    if main_content:
                        # Look for first heading
                        heading = main_content.query_selector("h1, h2, h3, h4")
                        if heading:
                            text = heading.inner_text().strip()
                            if text and len(text) > 0:
                                return text
                except:
                    continue
            
            # Last resort: look for any text that might be a food name
            try:
                body = page.query_selector("body")
                if body:
                    all_text = body.inner_text()
                    lines = [line.strip() for line in all_text.split('\n') if line.strip()]
                    for line in lines[:10]:  # First 10 lines
                        if len(line) > 3 and len(line) < 100:
                            if not line.replace('.', '').replace(',', '').isdigit():
                                return line
            except:
                pass
            
            logger.debug("Could not find food name on page")
            return None
            
        except Exception as e:
            logger.debug(f"Error parsing food name: {e}")
            return None
    
    def get_measurement_options(self, page: Page) -> List[Dict[str, str]]:
        """Extract all measurement options from dropdown.
        
        Args:
            page: Playwright Page object
        Returns:
            List of dictionaries with 'value' and 'text' keys
        """
        options = []
        try:
            # Find the measurement dropdown (expanded selector list)
            selectors = [
                "select",
                "select[name*='measure']",
                "select[id*='measure']",
                "select[name*='unit']",
                "select[id*='unit']",
                ".measurement-select",
                "[class*='measurement'] select",
                "[class*='serving'] select",
            ]
            
            select_element = None
            for selector in selectors:
                try:
                    select_element = page.query_selector(selector)
                    if select_element:
                        logger.debug(f"Found select element with selector: {selector}")
                        break
                except:
                    continue
            
            if not select_element:
                # Try to find ANY select element
                all_selects = page.query_selector_all("select")
                if all_selects:
                    logger.debug(f"Found {len(all_selects)} select element(s), using first one")
                    select_element = all_selects[0]
                else:
                    logger.warning("Measurement dropdown not found, using default")
                    return [{"value": "100", "text": "100 گرم"}]
            
            # Get all option elements
            option_elements = select_element.query_selector_all("option")
            
            for option in option_elements:
                value = option.get_attribute("value") or ""
                text = option.inner_text().strip()
                if text:  # Skip empty options
                    options.append({"value": value, "text": text})
            
            if not options:
                logger.warning("No options found in select, using default")
                options = [{"value": "100", "text": "100 گرم"}]
            
            logger.debug(f"Found {len(options)} measurement options")
            return options
            
        except Exception as e:
            logger.error(f"Error getting measurement options: {e}")
            return [{"value": "100", "text": "100 گرم"}]
    
    def extract_measurement_value(self, measurement_text: str, dropdown_value: Optional[str] = None) -> Optional[float]:
        """Extract numeric value from measurement text or dropdown value.
        
        Args:
            measurement_text: Text like "100 گرم" or "یک عدد"
            dropdown_value: Value from dropdown option (preferred if available)
        Returns:
            Numeric value or None
        """
        # Prefer dropdown value if available (it contains the actual weight in grams)
        if dropdown_value:
            try:
                return float(dropdown_value)
            except (ValueError, TypeError):
                pass
        
        # Extract numbers from text
        numbers = re.findall(r'\d+\.?\d*', measurement_text)
        if numbers:
            try:
                return float(numbers[0])
            except ValueError:
                pass
        
        # Handle Persian text for "one" (یک) - but prefer dropdown value
        if "یک" in measurement_text or "1" in measurement_text:
            # Return None if no dropdown value, as we can't determine actual weight
            return None
        
        return None
    
    def extract_nutritional_values(self, page: Page) -> Dict[str, Optional[float]]:
        """Extract nutritional values from current page state.
        
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
        
        try:
            # Map of field names to possible selectors (expanded)
            field_selectors = {
                "calories": [
                    "#calory-amount",
                    "[id*='calor']",
                    "[id*='cal']",
                    "[class*='calor']",
                    "[class*='cal']",
                    ".calories",
                    "h3",  # Sometimes calories are in h3
                ],
                "carbs_g": [
                    "#carbo-amount",
                    "[id*='carbo']",
                    "[id*='carb']",
                    "[class*='carbo']",
                    "[class*='carb']",
                    ".carbs",
                ],
                "protein_g": [
                    "#protein-amount",
                    "[id*='protein']",
                    "[id*='prot']",
                    "[class*='protein']",
                    "[class*='prot']",
                    ".protein",
                ],
                "fat_g": [
                    "#fat-amount",
                    "[id*='fat']",
                    "[class*='fat']",
                    ".fat",
                ],
                "fiber_g": [
                    "#fiber-amount",
                    "[id*='fiber']",
                    "[id*='fib']",
                    "[class*='fiber']",
                    "[class*='fib']",
                    ".fiber",
                ],
            }
            
            for field, selectors in field_selectors.items():
                for selector in selectors:
                    try:
                        element = page.query_selector(selector)
                        if element:
                            text = element.inner_text().strip()
                            if text:
                                # Extract numeric value
                                numbers = re.findall(r'\d+\.?\d*', text)
                                if numbers:
                                    try:
                                        val = float(numbers[0])
                                        # Validate reasonable range
                                        if 0 <= val <= 10000:  # Reasonable nutritional value range
                                            values[field] = val
                                            break
                                    except ValueError:
                                        pass
                    except:
                        continue
            
            # Fallback: try to find values by text patterns (Persian keywords)
            if all(v is None for v in values.values()):
                logger.debug("Trying fallback extraction method")
                try:
                    body = page.query_selector("body")
                    if body:
                        content_text = body.inner_text()
                        # Look for patterns like numbers followed by Persian keywords
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
            
        except Exception as e:
            logger.error(f"Error extracting nutritional values: {e}")
            return values
    
    def scrape_food_item(self, food_id: int) -> List[Dict[str, Any]]:
        """Scrape all measurement variants for a food item.
        
        Args:
            food_id: Food item ID
        Returns:
            List of data dictionaries (one per measurement unit)
        """
        results = []
        
        try:
            # Fetch page
            page = self.fetch_food_page(food_id)
            
            # Extract food name
            food_name = self.parse_food_name(page)
            if not food_name:
                logger.warning(f"Could not extract food name for ID {food_id}")
                food_name = f"Unknown Food {food_id}"
            
            # Get measurement options
            measurement_options = self.get_measurement_options(page)
            
            if not measurement_options:
                logger.debug(f"No measurement options found for ID {food_id}")
                return results
            
            # Extract data for each measurement option
            for measurement in measurement_options:
                try:
                    # Select measurement option if dropdown exists
                    select_element = page.query_selector("select")
                    if select_element and measurement["value"]:
                        select_element.select_option(measurement["value"])
                        # Wait for DOM update (nutritional values to change) - optimized
                        page.wait_for_timeout(200)  # Reduced wait time
                    
                    # Extract nutritional values
                    nutritional_values = self.extract_nutritional_values(page)
                    
                    # Extract measurement value (prefer dropdown value)
                    measurement_value = self.extract_measurement_value(
                        measurement["text"],
                        dropdown_value=measurement.get("value")
                    )
                    
                    # Build data row
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
                    else:
                        logger.debug(
                            f"Invalid data for ID {food_id}, measurement: {measurement['text']}"
                        )
                
                except Exception as e:
                    logger.debug(
                        f"Error processing measurement '{measurement['text']}' "
                        f"for ID {food_id}: {e}"
                    )
                    continue
            
            if results:
                logger.info(
                    f"✓ ID {food_id} ({food_name}): {len(results)} measurement variants extracted"
                )
                for result in results:
                    logger.debug(
                        f"  - {result.get('measurement_unit')}: "
                        f"{result.get('calories')} cal, "
                        f"{result.get('carbs_g')}g carbs, "
                        f"{result.get('protein_g')}g protein"
                    )
            else:
                logger.warning(f"✗ ID {food_id}: No valid data extracted")
            
        except Exception as e:
            logger.error(f"Error scraping food item {food_id}: {e}")
        
        return results
    
    def scrape_all(self) -> List[Dict[str, Any]]:
        """Scrape all food items in range.
        
        Returns:
            List of all scraped data dictionaries
        """
        self._init_browser()
        
        try:
            total_ids = self.end_id - self.start_id + 1
            skipped = len(self.completed_ids)
            remaining = total_ids - skipped
            
            logger.info(
                f"Starting scrape: {remaining} items remaining "
                f"({skipped} already completed)"
            )
            
            for food_id in range(self.start_id, self.end_id + 1):
                # Skip if already completed
                if food_id in self.completed_ids:
                    logger.debug(f"Skipping already completed ID {food_id}")
                    continue
                
                # Log current item being scraped
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
                        logger.error(f"✗ ID {food_id}: No data extracted - STOPPING for investigation")
                        logger.error(f"Failed at ID {food_id}. Investigating page structure...")
                        self._investigate_failed_page(food_id)
                        raise ValueError(f"Failed to extract data from ID {food_id}")
                    
                    # Save checkpoint periodically
                    if len(self.completed_ids) % self.checkpoint_frequency == 0:
                        self.checkpoint_manager.save(
                            self.completed_ids,
                            self.scraped_data
                        )
                        logger.info(
                            f"💾 Checkpoint saved: {len(self.completed_ids)}/{total_ids} items "
                            f"({len(self.scraped_data)} data rows)"
                        )
                except Exception as e:
                    logger.error(f"✗ ID {food_id}: Error - {e}")
                    logger.error(f"STOPPING process at ID {food_id} for investigation")
                    raise  # Stop the process instead of continuing
                
                # Random delay between requests
                self._random_delay()
            
            # Final checkpoint save
            self.checkpoint_manager.save(
                self.completed_ids,
                self.scraped_data,
                force=True
            )
            
            logger.info(
                f"Scraping complete: {len(self.completed_ids)} items, "
                f"{len(self.scraped_data)} data rows"
            )
            
        finally:
            self._close_browser()
        
        return self.scraped_data

