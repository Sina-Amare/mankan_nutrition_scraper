"""Scraper for individual fruit pages from mankan.me."""

import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from playwright.sync_api import sync_playwright, Page, TimeoutError as PlaywrightTimeout

from src.data_processor import DataProcessor
from src.logger_config import get_logger

logger = get_logger(__name__)


class FruitScraper:
    """Scraper for fruit pages with type=fruit parameter."""
    
    BASE_URL = "https://www.mankan.me/mag/lib/read_one.php"
    
    def __init__(self):
        """Initialize fruit scraper."""
        self.playwright = None
        self.browser = None
        self.page = None
        self.data_processor = DataProcessor()
    
    def _init_browser(self):
        """Initialize browser once."""
        if self.browser is None:
            self.playwright = sync_playwright().start()
            self.browser = self.playwright.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-blink-features=AutomationControlled"]
            )
            self.page = self.browser.new_page()
            logger.debug("Browser ready for fruit scraping")
    
    def _close_browser(self):
        """Close browser."""
        if self.page:
            self.page.close()
        if self.browser:
            self.browser.close()
        if self.playwright:
            self.playwright.stop()
    
    def _is_valid_page(self, page: Page) -> bool:
        """Quick check if page is valid."""
        try:
            title = page.title()
            if not title or len(title.strip()) == 0:
                return False
            body = page.query_selector("body")
            if body:
                text = body.inner_text()
                if "Fatal error" in text or len(body.inner_html()) < 1000:
                    return False
            return True
        except:
            return False
    
    def fetch_fruit_page(self, fruit_id: int) -> Optional[Page]:
        """Fetch fruit page with type=fruit parameter.
        
        Args:
            fruit_id: Fruit item ID
        Returns:
            Playwright Page object or None if page is invalid
        """
        if self.page is None:
            self._init_browser()
        
        url = f"{self.BASE_URL}?id={fruit_id}&type=fruit"
        try:
            response = self.page.goto(url, wait_until="domcontentloaded", timeout=15000)
            if response and response.status == 404:
                return None
            self.page.wait_for_timeout(200)  # Wait for content
            if not self._is_valid_page(self.page):
                return None
            return self.page
        except:
            return None
    
    def get_fruit_name(self, page: Page) -> str:
        """Get fruit name from page.
        
        Args:
            page: Playwright Page object
        Returns:
            Fruit name
        """
        # Try multiple selectors for fruit name
        selectors = [
            "h1",
            "h2", 
            "h3",
            ".food-titel h1",
            ".titer-Result-Box h1",
            "[class*='title']",
            "[class*='name']",
        ]
        
        for selector in selectors:
            try:
                elem = page.query_selector(selector)
                if elem:
                    text = elem.inner_text().strip()
                    # Filter out invalid names
                    if text and len(text) > 2 and len(text) < 200 and not text.startswith("Fruit"):
                        # Remove common prefixes/suffixes
                        text = text.replace("کالری:", "").replace("قند:", "").replace("فیبر:", "").strip()
                        if text and len(text) > 2:
                            return text
            except:
                continue
        
        # Fallback: try to extract from page title or URL
        try:
            title = page.title()
            if title and len(title) > 3:
                return title.split("-")[0].strip() if "-" in title else title.strip()
        except:
            pass
        
        # Last resort
        fruit_id = page.url.split('id=')[-1].split('&')[0] if 'id=' in page.url else 'Unknown'
        return f"Fruit {fruit_id}"
    
    def extract_fruit_values(self, page: Page) -> Dict[str, Optional[float]]:
        """Extract fruit nutritional values (calories, sugar, fiber, salt).
        
        Args:
            page: Playwright Page object
        Returns:
            Dictionary with calories, sugar_g, fiber_g, salt_g
        """
        values = {
            "calories": None,
            "sugar_g": None,
            "fiber_g": None,
            "salt_g": None,
        }
        
        try:
            # Get page content
            body = page.query_selector("body")
            if not body:
                return values
            
            text = body.inner_text()
            html = body.inner_html()
            
            # Method 1: Extract from HTML structure (more reliable)
            # Look for the organics div structure: <div class="organics">کالری: <span class="amount">50<sub>Cal</sub></span>...
            try:
                organics_div = page.query_selector('.organics, [class*="organic"]')
                if organics_div:
                    org_text = organics_div.inner_text()
                    org_html = organics_div.inner_html()
                    
                    # Extract calories - look for pattern: کالری: 50Cal or <span class="amount">50<sub>Cal</sub></span>
                    cal_match = re.search(r'کالری[:\s]*(\d+\.?\d*)\s*Cal?', org_text, re.IGNORECASE)
                    if not cal_match:
                        # Try HTML pattern
                        cal_html_match = re.search(r'کالری[:\s]*<span[^>]*>(\d+\.?\d*)<sub>Cal', org_html, re.IGNORECASE)
                        if cal_html_match:
                            values["calories"] = float(cal_html_match.group(1))
                    else:
                        values["calories"] = float(cal_match.group(1))
                    
                    # Extract sugar (قند)
                    sugar_match = re.search(r'قند[:\s]*(\d+\.?\d*)\s*g', org_text, re.IGNORECASE)
                    if not sugar_match:
                        sugar_html_match = re.search(r'قند[:\s]*<span[^>]*>(\d+\.?\d*)<sub>g', org_html, re.IGNORECASE)
                        if sugar_html_match:
                            values["sugar_g"] = float(sugar_html_match.group(1))
                    else:
                        values["sugar_g"] = float(sugar_match.group(1))
                    
                    # Extract fiber (فیبر)
                    fiber_match = re.search(r'فیبر[:\s]*(\d+\.?\d*)\s*g', org_text, re.IGNORECASE)
                    if not fiber_match:
                        fiber_html_match = re.search(r'فیبر[:\s]*<span[^>]*>(\d+\.?\d*)<sub>g', org_html, re.IGNORECASE)
                        if fiber_html_match:
                            values["fiber_g"] = float(fiber_html_match.group(1))
                    else:
                        values["fiber_g"] = float(fiber_match.group(1))
                    
                    # Extract salt (نمک) - may not be available for fruits
                    salt_match = re.search(r'نمک[:\s]*(\d+\.?\d*)\s*g', org_text, re.IGNORECASE)
                    if not salt_match:
                        salt_html_match = re.search(r'نمک[:\s]*<span[^>]*>(\d+\.?\d*)<sub>g', org_html, re.IGNORECASE)
                        if salt_html_match:
                            values["salt_g"] = float(salt_html_match.group(1))
                    else:
                        values["salt_g"] = float(salt_match.group(1))
            except Exception as e:
                logger.debug(f"Error extracting from organics div: {e}")
            
            # Method 2: Extract from text patterns (fallback)
            # Pattern: کالری: 50Cal, قند: 8g, فیبر: 1.6g
            if values["calories"] is None:
                cal_patterns = [
                    r'کالری[:\s]*(\d+\.?\d*)\s*Cal?',
                    r'کالری[:\s]*(\d+\.?\d*)',
                    r'(\d+\.?\d*)\s*Cal\b',
                ]
                for pattern in cal_patterns:
                    match = re.search(pattern, text, re.IGNORECASE)
                    if match:
                        try:
                            values["calories"] = float(match.group(1))
                            break
                        except:
                            continue
            
            if values["sugar_g"] is None:
                sugar_patterns = [
                    r'قند[:\s]*(\d+\.?\d*)\s*g\b',
                    r'قند[:\s]*(\d+\.?\d*)',
                ]
                for pattern in sugar_patterns:
                    match = re.search(pattern, text, re.IGNORECASE)
                    if match:
                        try:
                            values["sugar_g"] = float(match.group(1))
                            break
                        except:
                            continue
            
            if values["fiber_g"] is None:
                fiber_patterns = [
                    r'فیبر[:\s]*(\d+\.?\d*)\s*g\b',
                    r'فیبر[:\s]*(\d+\.?\d*)',
                ]
                for pattern in fiber_patterns:
                    match = re.search(pattern, text, re.IGNORECASE)
                    if match:
                        try:
                            values["fiber_g"] = float(match.group(1))
                            break
                        except:
                            continue
            
            # Method 2: Try to find elements with class "amount" (from search result structure)
            # The search results show: <span class="amount">50<sub>Cal</sub></span>
            try:
                # Look for calories in span.amount elements
                amount_elements = page.query_selector_all('.amount')
                for elem in amount_elements:
                    text_content = elem.inner_text().strip()
                    # Check if it contains Cal (calories)
                    if 'Cal' in text_content or 'کالری' in elem.get_attribute('class') or '':
                        nums = re.findall(r'\d+\.?\d*', text_content)
                        if nums and values["calories"] is None:
                            values["calories"] = float(nums[0])
            except:
                pass
            
            # Method 3: Try ID-based selectors (same as food pages)
            try:
                cal_elem = page.query_selector("#calory-amount, .cal, [id*='calor']")
                if cal_elem and values["calories"] is None:
                    text = cal_elem.inner_text().strip()
                    nums = re.findall(r'\d+\.?\d*', text)
                    if nums:
                        values["calories"] = float(nums[0])
            except:
                pass
            
            logger.debug(f"Extracted fruit values: {values}")
            
        except Exception as e:
            logger.debug(f"Error extracting fruit values: {e}")
        
        return values
    
    def scrape_fruit(self, fruit_id: int) -> List[Dict[str, Any]]:
        """Scrape a single fruit item.
        
        Args:
            fruit_id: Fruit item ID
        Returns:
            List of data dictionaries (fruits typically have one row per fruit)
        """
        results = []
        
        try:
            page = self.fetch_fruit_page(fruit_id)
            if not page:
                return results
            
            fruit_name = self.get_fruit_name(page)
            values = self.extract_fruit_values(page)
            
            # Fruits typically have one measurement (100g or per piece)
            # Create a single row for the fruit
            # Set missing values to 0 for compatibility with existing Excel structure
            row = {
                "food_id": fruit_id,
                "food_name": fruit_name,
                "measurement_unit": "100 گرم",  # Default for fruits
                "measurement_value": 100.0,
                "calories": values.get("calories") or 0.0,
                "fat_g": 0.0,  # Fruits don't have fat, set to 0
                "protein_g": 0.0,  # Fruits don't have protein, set to 0
                "carbs_g": 0.0,  # Fruits don't have carbs breakdown, set to 0
                "fiber_g": values.get("fiber_g") or 0.0,
                "sugar_g": values.get("sugar_g") or 0.0,  # Sugar from fruit data
                "salt_g": values.get("salt_g") or 0.0,  # Salt from fruit data (usually 0)
            }
            
            # Clean and validate
            cleaned = self.data_processor.clean_data(row)
            if self.data_processor.validate_row(cleaned):
                results.append(cleaned)
        
        except Exception as e:
            logger.debug(f"Error scraping fruit {fruit_id}: {e}")
        
        return results
    
    def scrape_all_fruits(self, fruit_ids: List[int]) -> List[Dict[str, Any]]:
        """Scrape all fruit items.
        
        Args:
            fruit_ids: List of fruit IDs to scrape
        Returns:
            List of all scraped fruit data dictionaries
        """
        self._init_browser()
        
        scraped_data = []
        
        try:
            total = len(fruit_ids)
            logger.info(f"Scraping {total} fruits...")
            
            for idx, fruit_id in enumerate(fruit_ids, 1):
                logger.info(f"[{idx}/{total}] Scraping fruit ID {fruit_id}...")
                
                try:
                    data = self.scrape_fruit(fruit_id)
                    
                    if data:
                        scraped_data.extend(data)
                        logger.info(f"✓ Fruit ID {fruit_id}: {len(data)} row(s) extracted")
                    else:
                        logger.warning(f"⚠ Fruit ID {fruit_id}: No data extracted")
                
                except Exception as e:
                    logger.error(f"✗ Fruit ID {fruit_id}: Error - {e}")
                
                time.sleep(0.3)  # Small delay
            
            logger.info(f"Complete: {len(scraped_data)} fruit rows extracted")
        
        finally:
            self._close_browser()
        
        return scraped_data

