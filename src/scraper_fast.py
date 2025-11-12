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
        """Initialize browser once."""
        if self.browser is None:
            self.playwright = sync_playwright().start()
            self.browser = self.playwright.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-blink-features=AutomationControlled"]
            )
            self.page = self.browser.new_page()
            logger.debug("Browser ready")
    
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
    
    def fetch_page(self, food_id: int) -> Optional[Page]:
        """Fetch page quickly."""
        if self.page is None:
            self._init_browser()
        
        url = f"{self.BASE_URL}?id={food_id}"
        try:
            response = self.page.goto(url, wait_until="domcontentloaded", timeout=15000)
            if response and response.status == 404:
                return None
            self.page.wait_for_timeout(100)  # Minimal wait
            if not self._is_valid_page(self.page):
                return None
            return self.page
        except:
            return None
    
    def get_food_name(self, page: Page) -> str:
        """Get food name quickly."""
        for selector in ["h1", "h2", "h3"]:
            try:
                elem = page.query_selector(selector)
                if elem:
                    text = elem.inner_text().strip()
                    if text and 3 < len(text) < 200:
                        return text
            except:
                continue
        return f"Food {page.url.split('id=')[-1] if 'id=' in page.url else 'Unknown'}"
    
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
            "fat_g": None, "fiber_g": None
        }
        
        # Fast ID-based extraction
        ids_map = {
            "calories": "calory-amount",
            "carbs_g": "carbo-amount",
            "protein_g": "protein-amount",
            "fat_g": "fat-amount",
            "fiber_g": "fiber-amount"
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
                            page.wait_for_timeout(100)  # Minimal wait
                        except:
                            pass
                    
                    # Get values
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
                    
                    # Build row
                    row = {
                        "food_id": food_id,
                        "food_name": food_name,
                        "measurement_unit": measurement["text"],
                        "measurement_value": mval,
                        "calories": nutrition["calories"],
                        "carbs_g": nutrition["carbs_g"],
                        "protein_g": nutrition["protein_g"],
                        "fat_g": nutrition["fat_g"],
                        "fiber_g": nutrition["fiber_g"],
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
                    
                    # Checkpoint
                    if len(self.completed_ids) % self.checkpoint_frequency == 0:
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
                
                # Minimal delay
                time.sleep(0.3)  # Fixed small delay for speed
            
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

