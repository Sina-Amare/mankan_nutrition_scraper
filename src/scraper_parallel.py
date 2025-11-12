"""Parallel scraper using multiple browser instances for maximum speed."""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional

from playwright.sync_api import sync_playwright, Page

from src.checkpoint import CheckpointManager
from src.data_processor import DataProcessor
from src.incremental_writer import IncrementalWriter
from src.logger_config import get_logger
from src.skipped_logger import SkippedLogger

logger = get_logger(__name__)


class ParallelScraper:
    """Parallel scraper with multiple browser instances."""
    
    BASE_URL = "https://www.mankan.me/mag/lib/read_one.php"
    
    def __init__(
        self,
        num_workers: int = 4,
        checkpoint_manager: Optional[CheckpointManager] = None,
        output_dir: Optional[Path] = None,
        csv_filename: str = "mankan_nutritional_data.csv",
        excel_filename: str = "mankan_nutritional_data.xlsx",
    ):
        """Initialize parallel scraper.
        
        Args:
            num_workers: Number of parallel browser instances
            checkpoint_manager: Checkpoint manager
            output_dir: Output directory
            csv_filename: CSV filename
            excel_filename: Excel filename
        """
        self.num_workers = num_workers
        self.checkpoint_manager = checkpoint_manager or CheckpointManager()
        self.data_processor = DataProcessor()
        
        output_dir = output_dir or Path("output")
        self.incremental_writer = IncrementalWriter(
            output_dir=output_dir,
            csv_filename=csv_filename,
            excel_filename=excel_filename,
            batch_size=100,  # Larger batch for parallel
        )
        self.skipped_logger = SkippedLogger()
        
        # Load checkpoint
        checkpoint_data = self.checkpoint_manager.load()
        self.completed_ids = set(checkpoint_data.get("completed_ids", []))
        
        logger.info(f"Parallel scraper initialized: {num_workers} workers, {len(self.completed_ids)} completed")
    
    def _create_browser_instance(self):
        """Create a browser instance for a worker."""
        playwright = sync_playwright().start()
        browser = playwright.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-gpu",
                "--disable-dev-shm-usage",
            ]
        )
        context = browser.new_context(
            viewport={"width": 1280, "height": 720},
            ignore_https_errors=True,
        )
        page = context.new_page()
        
        # Block resources
        def handle_route(route):
            if route.request.resource_type in ["image", "font", "stylesheet", "media"]:
                route.abort()
            else:
                route.continue_()
        page.route("**/*", handle_route)
        
        return playwright, browser, context, page
    
    def _scrape_single_item(self, food_id: int, page: Page) -> List[Dict[str, Any]]:
        """Scrape a single food item (worker function)."""
        results = []
        
        try:
            # Fetch page
            url = f"{self.BASE_URL}?id={food_id}"
            response = page.goto(url, wait_until="commit", timeout=8000)
            if response and response.status == 404:
                return results
            
            page.wait_for_timeout(30)
            
            # Get food name
            food_name = self._get_food_name(page)
            
            # Get measurements
            measurements = self._get_measurements(page)
            
            # Scrape all measurements
            for measurement in measurements:
                try:
                    # Select measurement
                    select = page.query_selector("select")
                    if select and measurement.get("value"):
                        try:
                            select.select_option(measurement["value"])
                            page.wait_for_timeout(50)
                        except:
                            pass
                    
                    # Get nutritional values
                    nutrition = self._get_nutritional_values(page)
                    
                    # Measurement value
                    mval = None
                    if measurement.get("value"):
                        try:
                            mval = float(measurement["value"])
                        except:
                            pass
                    if mval is None:
                        import re
                        nums = re.findall(r'\d+\.?\d*', measurement["text"])
                        if nums:
                            mval = float(nums[0])
                    
                    # Build row
                    # IMPORTANT: For foods, sugar_g and salt_g are ALWAYS 0.0
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
                        "sugar_g": 0.0,  # Foods NEVER have sugar breakdown - always 0
                        "salt_g": 0.0,   # Foods NEVER have salt breakdown - always 0
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
    
    def _get_food_name(self, page: Page) -> str:
        """Get food name - comprehensive extraction with question pattern cleaning."""
        import re
        
        # Try h1 first
        try:
            h1 = page.query_selector("h1")
            if h1:
                text = h1.inner_text().strip()
                if text and 3 < len(text) < 200:
                    text = text.replace("کالری:", "").replace("قند:", "").replace("فیبر:", "").replace("نمک:", "").strip()
                    
                    # Remove question patterns
                    text = re.sub(r'^کالری\s+(.+?)\s+چقدر\s+است\??\s*$', r'\1', text, flags=re.IGNORECASE)
                    text = re.sub(r'^کالری\s+(.+?)\s+چقدر\??\s*$', r'\1', text, flags=re.IGNORECASE)
                    text = re.sub(r'^(.+?)\s+چقدر\s+است\??\s*$', r'\1', text, flags=re.IGNORECASE)
                    text = re.sub(r'^(.+?)\s+چند\s+کالری\s+دارد\??\s*$', r'\1', text, flags=re.IGNORECASE)
                    text = re.sub(r'^بانک\s+غذایی\s*\|\s*(.+?)$', r'\1', text, flags=re.IGNORECASE)
                    text = re.sub(r'\s+(چقدر|است|هست|چند|دارد|کالری)\??\s*$', '', text, flags=re.IGNORECASE)
                    
                    if text and len(text) > 2 and not text.startswith("Food") and not text.startswith("Fruit"):
                        return text
        except:
            pass
        
        # Try h2, h3
        for selector in ["h2", "h3"]:
            try:
                elem = page.query_selector(selector)
                if elem:
                    text = elem.inner_text().strip()
                    if text and 3 < len(text) < 200:
                        # Clean question patterns
                        text = re.sub(r'^کالری\s+(.+?)\s+چقدر\s+است\??\s*$', r'\1', text, flags=re.IGNORECASE)
                        text = re.sub(r'^(.+?)\s+چند\s+کالری\s+دارد\??\s*$', r'\1', text, flags=re.IGNORECASE)
                        text = re.sub(r'\s+(چقدر|است|هست|چند|دارد)\??\s*$', '', text, flags=re.IGNORECASE)
                        
                        if text and len(text) > 2 and not text.startswith("Food") and not text.startswith("Fruit"):
                            return text
            except:
                continue
        
        # Try page title
        try:
            title = page.title()
            if title and len(title) > 3:
                parts = title.split("-")
                if parts:
                    name = parts[0].strip().replace("مانکن", "").replace("Mankan", "").strip()
                    # Clean question patterns
                    name = re.sub(r'^کالری\s+(.+?)\s+چقدر\s+است\??\s*$', r'\1', name, flags=re.IGNORECASE)
                    name = re.sub(r'^(.+?)\s+چند\s+کالری\s+دارد\??\s*$', r'\1', name, flags=re.IGNORECASE)
                    name = re.sub(r'\s+(چقدر|است|هست|چند|دارد)\??\s*$', '', name, flags=re.IGNORECASE)
                    
                    if name and len(name) > 2:
                        return name
        except:
            pass
        
        # Fallback
        food_id = page.url.split('id=')[-1].split('&')[0] if 'id=' in page.url else 'Unknown'
        return f"Food {food_id}"
    
    def _get_measurements(self, page: Page) -> List[Dict[str, str]]:
        """Get all measurement options."""
        try:
            select = page.query_selector("select")
            if not select:
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
    
    def _get_nutritional_values(self, page: Page) -> Dict[str, Optional[float]]:
        """Get nutritional values - for FOODS only (no salt/sugar)."""
        values = {
            "calories": None, "carbs_g": None, "protein_g": None,
            "fat_g": None, "fiber_g": None
            # NOTE: salt_g and sugar_g are NOT extracted for foods - always 0.0
        }
        
        ids_map = {
            "calories": "calory-amount",
            "carbs_g": "carbo-amount",
            "protein_g": "protein-amount",
            "fat_g": "fat-amount",
            "fiber_g": "fiber-amount",
            # salt_g removed - foods don't have salt breakdown
        }
        
        for field, vid in ids_map.items():
            try:
                elem = page.query_selector(f"#{vid}")
                if elem:
                    text = elem.inner_text().strip()
                    import re
                    nums = re.findall(r'\d+\.?\d*', text)
                    if nums:
                        val = float(nums[0])
                        if 0 <= val <= 10000:
                            values[field] = val
            except:
                continue
        
        # DO NOT extract salt for foods - it should always be 0.0
        
        return values
    
    def scrape_all(self, food_ids: List[int]) -> List[Dict[str, Any]]:
        """Scrape all items in parallel."""
        # Filter out completed IDs
        food_ids_to_scrape = [fid for fid in food_ids if fid not in self.completed_ids]
        
        if not food_ids_to_scrape:
            logger.info("All items already completed")
            return []
        
        total = len(food_ids_to_scrape)
        logger.info(f"Scraping {total} items with {self.num_workers} parallel workers...")
        
        scraped_data = []
        completed = []
        skipped = []
        
        # Use ThreadPoolExecutor for parallel scraping
        with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            # Create browser instances for each worker
            browsers = []
            for _ in range(self.num_workers):
                browsers.append(self._create_browser_instance())
            
            try:
                # Submit all tasks
                future_to_id = {}
                browser_idx = 0
                
                for food_id in food_ids_to_scrape:
                    playwright, browser, context, page = browsers[browser_idx % self.num_workers]
                    future = executor.submit(self._scrape_single_item, food_id, page)
                    future_to_id[future] = food_id
                    browser_idx += 1
                
                # Process results as they complete
                for future in as_completed(future_to_id):
                    food_id = future_to_id[future]
                    try:
                        data = future.result()
                        
                        if data:
                            scraped_data.extend(data)
                            completed.append(food_id)
                            self.completed_ids.add(food_id)
                            self.incremental_writer.add_data(data)
                            
                            current = len(completed)
                            print(f"[{current}/{total}] ✓ ID {food_id}: {len(data)} measurements", flush=True)
                        else:
                            skipped.append(food_id)
                            self.skipped_logger.log_skipped(
                                food_id=food_id,
                                reason="no_data",
                                error_message="No data extracted"
                            )
                            print(f"[{len(completed)}/{total}] ⚠ ID {food_id}: Skipped", flush=True)
                        
                        # Checkpoint every 50 items
                        if len(completed) % 50 == 0:
                            self.checkpoint_manager.save(list(self.completed_ids), scraped_data)
                    
                    except Exception as e:
                        skipped.append(food_id)
                        self.skipped_logger.log_skipped(
                            food_id=food_id,
                            error=e,
                            reason="exception"
                        )
                        logger.error(f"Error scraping {food_id}: {e}")
            
            finally:
                # Close all browsers
                for playwright, browser, context, page in browsers:
                    try:
                        page.close()
                        context.close()
                    except:
                        pass
                    try:
                        browser.close()
                    except:
                        pass
                    try:
                        playwright.stop()
                    except:
                        pass
        
        # Final save
        self.incremental_writer.finalize()
        self.checkpoint_manager.save(list(self.completed_ids), scraped_data)
        
        logger.info(f"Complete: {len(completed)} items, {len(skipped)} skipped")
        return scraped_data

