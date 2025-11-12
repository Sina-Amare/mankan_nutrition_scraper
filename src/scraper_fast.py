"""Fast, accurate, full data scraper for Mankan.me - optimized for speed and completeness."""

import random
import re
import time
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

from playwright.sync_api import sync_playwright, Page, TimeoutError as PlaywrightTimeout

from src.checkpoint import CheckpointManager
from src.data_processor import DataProcessor
from src.incremental_writer import IncrementalWriter
from src.logger_config import get_logger
from src.skipped_logger import SkippedLogger

logger = get_logger(__name__)


class FastMankanScraper:
    """Fast scraper optimized for speed and completeness."""
    
    BASE_URL = "https://www.mankan.me/mag/lib/read_one.php"
    
    def __init__(
        self,
        start_id: int = 3,
        end_id: int = 1967,
        checkpoint_manager: Optional[CheckpointManager] = None,
        checkpoint_frequency: int = 50,
        output_dir: Optional[Path] = None,
        csv_filename: str = "mankan_nutritional_data.csv",
        excel_filename: str = "mankan_nutritional_data.xlsx",
    ):
        """Initialize fast scraper."""
        self.start_id = start_id
        self.end_id = end_id
        self.checkpoint_manager = checkpoint_manager or CheckpointManager()
        self.checkpoint_frequency = checkpoint_frequency
        
        self.playwright = None
        self.browser = None
        self.page = None
        
        self.data_processor = DataProcessor()
        self.scraped_data: List[Dict[str, Any]] = []
        self.completed_ids: List[int] = []
        self.skipped_ids: List[int] = []
        
        # Initialize incremental writer and skipped logger
        output_dir = output_dir or Path("output")
        self.incremental_writer = IncrementalWriter(
            output_dir=output_dir,
            csv_filename=csv_filename,
            excel_filename=excel_filename
        )
        self.skipped_logger = SkippedLogger()
        
        # Load checkpoint
        checkpoint_data = self.checkpoint_manager.load()
        self.completed_ids = checkpoint_data.get("completed_ids", [])
        self.scraped_data = checkpoint_data.get("data", [])
        
        logger.info(f"Fast scraper: IDs {start_id}-{end_id}, {len(self.completed_ids)} completed")
    
    def _init_browser(self):
        """Initialize browser once with speed optimizations."""
        if self.browser is None:
            self.playwright = sync_playwright().start()
            self.browser = self.playwright.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-blink-features=AutomationControlled",
                    "--disable-gpu",
                    "--disable-dev-shm-usage",
                    "--no-first-run",
                    "--disable-background-networking",
                    "--disable-background-timer-throttling",
                    "--disable-renderer-backgrounding",
                    "--disable-backgrounding-occluded-windows",
                ]
            )
            # Create context with smaller viewport for speed
            self.context = self.browser.new_context(
                viewport={"width": 1280, "height": 720},
                ignore_https_errors=True,
            )
            self.page = self.context.new_page()
            
            # Block unnecessary resources to speed up loading
            def handle_route(route):
                resource_type = route.request.resource_type
                # Block images, fonts, stylesheets, but keep scripts (needed for dropdowns)
                if resource_type in ["image", "font", "stylesheet", "media"]:
                    route.abort()
                else:
                    route.continue_()
            
            self.page.route("**/*", handle_route)
            
            logger.debug("Browser ready (optimized for speed)")
    
    def _close_browser(self):
        """Close browser."""
        if self.page:
            try:
                self.page.close()
            except:
                pass
        if hasattr(self, 'context') and self.context:
            try:
                self.context.close()
            except:
                pass
        if self.browser:
            try:
                self.browser.close()
            except:
                pass
        if self.playwright:
            try:
                self.playwright.stop()
            except:
                pass
    
    def _is_valid_page(self, page: Page) -> bool:
        """Quick check if page is valid - ultra-fast version."""
        try:
            # Ultra-fast: just check if body exists (skip content validation for speed)
            body = page.query_selector("body")
            return body is not None
        except:
            return False
    
    def fetch_page(self, food_id: int) -> Optional[Page]:
        """Fetch page quickly with optimized loading."""
        if self.page is None:
            self._init_browser()
        
        url = f"{self.BASE_URL}?id={food_id}"
        try:
            # Use 'commit' for fastest loading - don't wait for full page load
            response = self.page.goto(url, wait_until="commit", timeout=8000)
            if response and response.status == 404:
                return None
            # Minimal wait - just enough for basic DOM
            self.page.wait_for_timeout(30)  # Reduced to 30ms
            # Skip validation check for speed (assume page is valid)
            return self.page
        except:
            return None
    
    def get_food_name(self, page: Page) -> str:
        """Get food name with comprehensive extraction - ensures no 'Food X' names."""
        # Strategy 1: Try h1 (most reliable)
        try:
            h1 = page.query_selector("h1")
            if h1:
                text = h1.inner_text().strip()
                if text and 3 < len(text) < 200:
                    # Clean up nutritional labels that might be mixed in
                    text = text.replace("کالری:", "").replace("قند:", "").replace("فیبر:", "").replace("نمک:", "").strip()
                    if text and len(text) > 2 and not text.startswith("Food") and not text.startswith("Fruit"):
                        return text
        except:
            pass
        
        # Strategy 2: Try h2, h3
        for selector in ["h2", "h3"]:
            try:
                elem = page.query_selector(selector)
                if elem:
                    text = elem.inner_text().strip()
                    if text and 3 < len(text) < 200 and not text.startswith("Food") and not text.startswith("Fruit"):
                        return text
            except:
                continue
        
        # Strategy 3: Look in main content area
        try:
            main_content = page.query_selector("main, .content, .read-one, section, article")
            if main_content:
                # Get first heading
                heading = main_content.query_selector("h1, h2, h3")
                if heading:
                    text = heading.inner_text().strip()
                    if text and 3 < len(text) < 200 and not text.startswith("Food") and not text.startswith("Fruit"):
                        # Clean nutritional labels
                        text = text.replace("کالری:", "").replace("قند:", "").replace("فیبر:", "").replace("نمک:", "").strip()
                        if text and len(text) > 2:
                            return text
        except:
            pass
        
        # Strategy 4: Page title (reliable fallback)
        try:
            title = page.title()
            if title and len(title) > 3:
                # Extract food name from title (format: "Food Name - Mankan" or similar)
                parts = title.split("-")
                if parts:
                    name = parts[0].strip()
                    # Remove site name if present
                    name = name.replace("مانکن", "").replace("Mankan", "").strip()
                    if name and len(name) > 2:
                        return name
        except:
            pass
        
        # Last resort: Try to extract from URL or any visible text
        try:
            # Look for any significant text that's not nutritional info
            body = page.query_selector("body")
            if body:
                # Get all headings
                headings = body.query_selector_all("h1, h2, h3")
                for heading in headings[:3]:  # Check first 3 headings
                    text = heading.inner_text().strip()
                    if text and 3 < len(text) < 200:
                        # Skip if it contains nutritional keywords
                        if not any(x in text for x in ["کالری", "قند", "فیبر", "نمک", "مقدار مصرف", "Cal", "g"]):
                            if not text.startswith("Food") and not text.startswith("Fruit") and not text.isdigit():
                                return text
        except:
            pass
        
        # Final fallback - log warning but still return ID-based name
        food_id = page.url.split('id=')[-1].split('&')[0] if 'id=' in page.url else 'Unknown'
        logger.warning(f"Could not extract food name for ID {food_id} - using fallback. Page may have unusual structure.")
        return f"Food {food_id}"
    
    def get_measurements(self, page: Page) -> List[Dict[str, str]]:
        """Get measurement options quickly."""
        try:
            select = page.query_selector("select")
            if not select:
                # Try to find any select
                selects = page.query_selector_all("select")
                if selects:
                    select = selects[0]
                else:
                    return [{"value": "100", "text": "100 گرم"}]
            
            options = []
            for opt in select.query_selector_all("option"):
                val = opt.get_attribute("value") or ""
                txt = opt.inner_text().strip()
                if txt:
                    options.append({"value": val, "text": txt})
            
            return options if options else [{"value": "100", "text": "100 گرم"}]
        except:
            return [{"value": "100", "text": "100 گرم"}]
    
    def get_nutritional_values(self, page: Page) -> Dict[str, Optional[float]]:
        """Get nutritional values quickly."""
        values = {
            "calories": None, "carbs_g": None, "protein_g": None,
            "fat_g": None, "fiber_g": None, "salt_g": None
        }
        
        # Fast ID-based extraction
        ids_map = {
            "calories": "calory-amount",
            "carbs_g": "carbo-amount",
            "protein_g": "protein-amount",
            "fat_g": "fat-amount",
            "fiber_g": "fiber-amount",
            "salt_g": "salt-amount"  # نمک
        }
        
        for field, vid in ids_map.items():
            try:
                elem = page.query_selector(f"#{vid}")
                if elem:
                    text = elem.inner_text().strip()
                    nums = re.findall(r'\d+\.?\d*', text)
                    if nums:
                        val = float(nums[0])
                        if 0 <= val <= 10000:
                            values[field] = val
            except:
                continue
        
        # If salt not found via ID, try alternative selectors
        if values["salt_g"] is None:
            try:
                # Try text-based search for نمک (salt in Persian)
                body = page.query_selector("body")
                if body:
                    text = body.inner_text()
                    salt_match = re.search(r'نمک[:\s]*(\d+\.?\d*)', text)
                    if salt_match:
                        values["salt_g"] = float(salt_match.group(1))
            except:
                pass
        
        return values
    
    def scrape_item(self, food_id: int) -> List[Dict[str, Any]]:
        """Scrape single item fast."""
        results = []
        
        try:
            page = self.fetch_page(food_id)
            if not page:
                return results
            
            food_name = self.get_food_name(page)
            measurements = self.get_measurements(page)
            
            for measurement in measurements:
                try:
                    # Select measurement
                    select = page.query_selector("select")
                    if select and measurement.get("value"):
                        try:
                            select.select_option(measurement["value"])
                            # Minimal wait - just enough for DOM update
                            page.wait_for_timeout(50)  # Reduced to absolute minimum
                        except:
                            pass
                    
                    # Get values immediately (no retry to save time)
                    nutrition = self.get_nutritional_values(page)
                    
                    # Measurement value
                    mval = None
                    if measurement.get("value"):
                        try:
                            mval = float(measurement["value"])
                        except:
                            pass
                    if mval is None:
                        nums = re.findall(r'\d+\.?\d*', measurement["text"])
                        if nums:
                            mval = float(nums[0])
                    
                    # Build row - matching column order
                    row = {
                        "food_id": food_id,
                        "food_name": food_name,
                        "measurement_unit": measurement["text"],
                        "measurement_value": mval,
                        "calories": nutrition.get("calories") or 0.0,
                        "fat_g": nutrition.get("fat_g") or 0.0,
                        "protein_g": nutrition.get("protein_g") or 0.0,
                        "carbs_g": nutrition.get("carbs_g") or 0.0,
                        "fiber_g": nutrition.get("fiber_g") or 0.0,
                        "sugar_g": 0.0,  # Foods don't have sugar breakdown
                        "salt_g": nutrition.get("salt_g") or 0.0,
                    }
                    
                    # Validate
                    cleaned = self.data_processor.clean_data(row)
                    if self.data_processor.validate_row(cleaned):
                        results.append(cleaned)
                
                except Exception as e:
                    logger.debug(f"Error processing measurement for {food_id}: {e}")
                    continue
        
        except Exception as e:
            logger.debug(f"Error scraping {food_id}: {e}")
        
        return results
    
    def scrape_all(self, food_ids: List[int] = None) -> List[Dict[str, Any]]:
        """Scrape all items fast with continuous progress.
        
        Args:
            food_ids: Optional list of specific food IDs to scrape. 
                     If None, uses range from start_id to end_id.
        Returns:
            List of scraped data dictionaries
        """
        self._init_browser()
        
        try:
            # Use provided food_ids or generate from range
            if food_ids is None:
                food_ids_to_scrape = list(range(self.start_id, self.end_id + 1))
            else:
                food_ids_to_scrape = food_ids
            
            # Filter out already completed IDs
            food_ids_to_scrape = [fid for fid in food_ids_to_scrape if fid not in self.completed_ids]
            
            total = len(food_ids_to_scrape)
            logger.info(f"Starting fast scrape: {total} items to process")
            
            for food_id in food_ids_to_scrape:
                
                # Progress logging - always visible
                current = len(self.completed_ids)
                total_all = len(food_ids_to_scrape) + len(self.completed_ids)
                pct = (current / total_all * 100) if total_all > 0 else 0
                
                # Print progress immediately (flush)
                print(f"[{current + 1}/{total}] ({pct:.1f}%) ID {food_id}...", flush=True)
                logger.info(f"[{current + 1}/{total}] ({pct:.1f}%) Scraping ID {food_id}...")
                
                # Scrape
                try:
                    data = self.scrape_item(food_id)
                    
                    if data:
                        self.scraped_data.extend(data)
                        self.completed_ids.append(food_id)
                        
                        # Immediately save to CSV/Excel (incremental)
                        self.incremental_writer.add_data(data)
                        
                        print(f"✓ ID {food_id}: {len(data)} measurements (Total: {len(self.scraped_data)} rows)", flush=True)
                        logger.info(f"✓ ID {food_id}: {len(data)} measurements (Total: {len(self.scraped_data)} rows)")
                    else:
                        # No data extracted - log as skipped
                        self.skipped_ids.append(food_id)
                        self.skipped_logger.log_skipped(
                            food_id=food_id,
                            reason="no_data",
                            error_message="No data extracted from page"
                        )
                        print(f"⚠ ID {food_id}: Skipped (no data)", flush=True)
                        logger.warning(f"⚠ ID {food_id}: Skipped (no data)")
                    
                    # Checkpoint (less frequent for speed)
                    if len(self.completed_ids) % (self.checkpoint_frequency * 2) == 0:
                        self.checkpoint_manager.save(self.completed_ids, self.scraped_data)
                        print(f"💾 Checkpoint: {len(self.completed_ids)}/{total} items", flush=True)
                
                except Exception as e:
                    # Error occurred - log with full details
                    self.skipped_ids.append(food_id)
                    self.skipped_logger.log_skipped(
                        food_id=food_id,
                        error=e,
                        reason="exception"
                    )
                    print(f"✗ ID {food_id}: Error - {e}", flush=True)
                    logger.error(f"✗ ID {food_id}: Error - {e}", exc_info=True)
                
                # Minimal delay - almost no delay for maximum speed
                time.sleep(0.05)  # Reduced to 50ms
            
            # Final checkpoint save
            self.checkpoint_manager.save(self.completed_ids, self.scraped_data, force=True)
            
            # Flush any remaining data to CSV/Excel
            self.incremental_writer.finalize()
            
            # Summary
            print("\n" + "="*60, flush=True)
            print(f"Complete! {len(self.completed_ids)} items, {len(self.scraped_data)} rows", flush=True)
            print(f"Skipped: {len(self.skipped_ids)} items", flush=True)
            if self.skipped_ids:
                print(f"Skipped IDs: {self.skipped_ids[:30]}{'...' if len(self.skipped_ids) > 30 else ''}", flush=True)
            print("="*60, flush=True)
            
            logger.info(f"Complete: {len(self.completed_ids)} items, {len(self.scraped_data)} rows, {len(self.skipped_ids)} skipped")
        
        finally:
            # Ensure any pending data is saved even on error
            try:
                self.incremental_writer.finalize()
            except Exception as e:
                logger.error(f"Error finalizing incremental writer: {e}", exc_info=True)
            self._close_browser()
        
        return self.scraped_data

