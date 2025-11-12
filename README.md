# Mankan.me Nutritional Database Scraper

A production-ready web scraper for extracting nutritional data from [mankan.me](https://www.mankan.me) website. Extracts all food items (IDs 3-1967) with all their measurement variants and exports to professionally styled Excel and CSV files.

## Features

- **Complete Data Extraction**: Scrapes all food items with all measurement units (100g, 1 piece, 1 cup, etc.)
- **Checkpoint/Resume**: Automatically saves progress and can resume from last checkpoint
- **Robust Error Handling**: Retry logic with exponential backoff, graceful error handling
- **Professional Output**: Styled Excel files with headers, borders, colors, and summary statistics
- **Comprehensive Logging**: Dual logging (console + file) for debugging and monitoring
- **Data Validation**: Validates and cleans all extracted data before saving
- **Respectful Scraping**: Configurable delays between requests to be respectful to the server

## Requirements

- Python 3.8 or higher
- Playwright browser (installed automatically)

## Installation

1. **Clone or download this repository**

2. **Create a virtual environment** (recommended):
```bash
python -m venv venv
```

3. **Activate the virtual environment**:
   - Windows: `venv\Scripts\activate`
   - Linux/Mac: `source venv/bin/activate`

4. **Install dependencies**:
```bash
pip install -r requirements.txt
```

5. **Install Playwright browsers**:
```bash
playwright install chromium
```

## Usage

### Basic Usage

Scrape all food items (IDs 3-1967):
```bash
python main.py
```

### Command Line Options

```bash
python main.py [OPTIONS]
```

**Options:**
- `--start-id N`: Starting food item ID (default: 3)
- `--end-id N`: Ending food item ID (default: 1967)
- `--resume`: Resume from last checkpoint
- `--checkpoint-frequency N`: Save checkpoint every N items (default: 50)
- `--delay-min N`: Minimum delay between requests in seconds (default: 1.0)
- `--delay-max N`: Maximum delay between requests in seconds (default: 3.0)
- `--output-dir DIR`: Output directory (default: output)
- `--excel-filename NAME`: Excel output filename (default: mankan_nutritional_data.xlsx)
- `--csv-filename NAME`: CSV output filename (default: mankan_nutritional_data.csv)

### Examples

**Scrape a specific range:**
```bash
python main.py --start-id 100 --end-id 200
```

**Resume from checkpoint:**
```bash
python main.py --resume
```

**Custom delay and checkpoint frequency:**
```bash
python main.py --delay-min 2 --delay-max 5 --checkpoint-frequency 25
```

## Project Structure

```
mankan-scraper/
├── src/
│   ├── __init__.py
│   ├── scraper.py          # Main scraping logic with Playwright
│   ├── data_processor.py   # Data validation and cleaning
│   ├── excel_writer.py     # Excel styling and export
│   ├── checkpoint.py       # Checkpoint/resume functionality
│   └── logger_config.py    # Logging configuration
├── data/
│   ├── checkpoints/        # Checkpoint JSON files
│   └── logs/              # Log files
├── output/                # Final Excel and CSV files
├── tests/                 # Test files
├── requirements.txt
├── .gitignore
├── README.md
└── main.py                # Entry point
```

## Output Format

### Excel File

The Excel file contains:
- **Nutritional Data Sheet**: All scraped data with professional styling
  - Headers with blue background (#4472C4) and white text
  - Borders on all cells
  - Auto-adjusted column widths
  - Frozen header row
- **Summary Sheet**: Statistics including:
  - Completion date
  - Total food items
  - Total data rows
  - Average measurements per food
  - Measurement unit distribution

### CSV File

A CSV backup file with the same data for portability.

### Columns

- Food Name
- Measurement Unit
- Measurement Value
- Calories
- Carbs (g)
- Protein (g)
- Fat (g)
- Fiber (g)
- Food ID
- Source URL

## Checkpoint System

The scraper automatically saves progress every N items (default: 50). If interrupted, you can resume using:

```bash
python main.py --resume
```

Checkpoints are saved in `data/checkpoints/checkpoint.json` with backup files for recovery.

## Logging

Logs are written to:
- **Console**: INFO level and above
- **File**: DEBUG level and above in `data/logs/scraper_YYYY-MM-DD_HH-MM-SS.log`

## Error Handling

The scraper handles:
- Network timeouts and connection errors (with retry)
- Missing or invalid pages (404 errors)
- Missing HTML elements
- Invalid data formats
- Checkpoint corruption (with backup recovery)
- JavaScript execution failures

## Troubleshooting

### Playwright Installation Issues

If Playwright browsers fail to install:
```bash
playwright install chromium --force
```

### Import Errors

Ensure you're in the project root directory and virtual environment is activated.

### Memory Issues

For large scraping jobs, consider:
- Reducing checkpoint frequency
- Scraping in smaller ID ranges
- Closing other applications

### Browser Timeout Errors

If you encounter frequent timeouts:
- Increase delays between requests (`--delay-min` and `--delay-max`)
- Check your internet connection
- The website may be temporarily unavailable

## Development

### Running Tests

```bash
python -m pytest tests/
```

### Code Style

The code follows PEP 8 standards with type hints and comprehensive docstrings.

## Legal and Ethical Considerations

- This scraper is for educational and research purposes
- Always respect the website's terms of service
- Use appropriate delays between requests
- Do not overload the server with excessive requests
- Consider reaching out to the website owner for API access if available

## License

This project is provided as-is for educational purposes.

## Support

For issues or questions:
1. Check the logs in `data/logs/`
2. Review error messages in the console
3. Ensure all dependencies are installed correctly

## Changelog

### Version 1.0.0
- Initial release
- Full scraping functionality
- Checkpoint/resume system
- Professional Excel output
- Comprehensive error handling

