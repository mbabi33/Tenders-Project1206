# Tender Scraper (C17.py)

## Description

This Python script is a command-line tool for scraping tender information from the Georgian public procurement website (`tenders.procurement.gov.ge`).

It automates the process of searching for tenders based on a CPV code and a date range, navigates through the search result pages, and scrapes detailed information for each tender found. The scraped data is saved locally into CSV files and HTML files for further analysis.

## Features

- **Search by CPV Code:** Target specific tender categories using a CPV code.
- **Date Range Filtering:** Limit the search to a specific period.
- **Flexible Pagination:** Scrape all pages, a single page, or a specific range of pages.
- **Detailed Scraping:** For each tender, the script scrapes and saves all the information from its various detail tabs.
- **Organized Output:** Creates a dedicated project directory for each CPV code, with scraped data organized into subdirectories.
- **Robust Error Handling:** The script is designed to handle pagination issues and other potential scraping challenges.

## Usage

The script is run from the command line and accepts several arguments to control its behavior.

```bash
python C17.py -c <cpv_code> [options]
```

### Arguments

| Argument | Alias | Type | Description |
|---|---|---|---|
| `--cpv` | `-c` | `string` | **(Required)** The CPV code to search for. |
| `--date-start` | `-ds` | `string` | The start date for the search range (format: `DD.MM.YYYY`). Defaults to the first day of the previous month. |
| `--date-end` | `-de` | `string` | The end date for the search range (format: `DD.MM.YYYY`). Defaults to yesterday. |
| `--page-start` | `-ps` | `int` | The page number to start scraping from. Defaults to `1`. |
| `--page-end` | `-pe` | `int` | The page number to stop scraping at. If not provided, or if set to `0`, the script will scrape all pages until the end. |
| `--root-dir` | `-root` | `string` | The root directory to save the project folders in. Defaults to the path configured in `config.py`. |

### Special Arguments

- **`-pe 0`**: If you set the end page to `0`, the script will ignore the end page limit and scrape all available pages from the specified start page.

## Example Usage

**1. Scrape the first 2 pages for a specific CPV code and date range:**
```bash
python C17.py -c 71200000 -ds 01.01.2025 -de 01.02.2025 -ps 1 -pe 2
```

**2. Scrape pages 5 through 7 for a specific CPV code and date range:**
```bash
python C17.py -c 71200000 -ds 01.01.2024 -de 01.02.2024 -ps 5 -pe 7
```

**3. Scrape all pages from page 3 onwards for a specific CPV code:**
```bash
python C17.py -c 71200000 -ps 3 -pe 0
```

**4. Scrape all pages using the default date range:**
```bash
python C17.py -c 71200000
```
