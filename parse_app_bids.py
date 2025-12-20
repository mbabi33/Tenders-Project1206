#!/usr/bin/env python3
"""
Tender Bids Parser (parse_app_bids.py)

Parses `_app_bids.html` files to extract competition data:
- Bidders list
- Offer history (First/Last offer amounts and dates)
- Ranking

Output: `bids.db`
"""

import sqlite3
import os
import re
import argparse
import logging
from datetime import datetime
from bs4 import BeautifulSoup
from tqdm import tqdm
import config

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# --- Helper Functions ---

def parse_datetime(dt_str):
    """Converts DD.MM.YYYY HH:MM string to ISO format."""
    if not dt_str:
        return None
    dt_str = dt_str.strip()
    try:
        dt_obj = datetime.strptime(dt_str, '%d.%m.%Y %H:%M')
        return dt_obj.strftime('%Y-%m-%d %H:%M:%S')
    except ValueError:
        return None

def clean_amount(amount_str):
    """Parses Georgian number format (e.g. 1`234.56) to float."""
    if not amount_str:
        return 0.0
    # Remove backticks, commas, spaces, &nbsp;
    cleaned = amount_str.replace('`', '').replace(',', '').replace('\xa0', '').strip()
    try:
        return float(cleaned)
    except ValueError:
        return 0.0

# --- Database ---

def init_db(db_path):
    """Initialize database schema."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    # Tenders (Applications)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS tenders (
            application_id INTEGER PRIMARY KEY,
            tender_number TEXT,
            file_name TEXT
        )
    """)
    
    # Bidders (Companies)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bidders (
            bidder_id INTEGER PRIMARY KEY,
            bidder_name TEXT
        )
    """)
    
    # Bids (Relationship)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bids (
            application_id INTEGER,
            bidder_id INTEGER,
            last_offer_amount REAL,
            last_offer_date TEXT,
            first_offer_amount REAL,
            first_offer_date TEXT,
            offer_count INTEGER,
            rank INTEGER,
            PRIMARY KEY (application_id, bidder_id),
            FOREIGN KEY (application_id) REFERENCES tenders(application_id),
            FOREIGN KEY (bidder_id) REFERENCES bidders(bidder_id)
        )
    """)
    
    conn.commit()
    return conn

# --- Parsing Logic ---

def process_file(file_path, conn):
    filename = os.path.basename(file_path)
    
    # Extract ID: pg_CODE_ID_app_bids.html
    match = re.search(r'pg_([A-Z0-9]+)_(\d+)_app_bids\.html', filename)
    if not match:
        return False
    
    tender_number_prefix = match.group(1) # e.g. NAT240000262 (though strictly regex groups split it)
    # Actually regex above: group 1 is the CODE (e.g. NAT24... or just NAT... depending on strictness).
    # The sample file: pg_NAT240000262_551777_app_bids.html
    # My regex: pg_([A-Z0-9]+)_(\d+)... -> Group 1: NAT240000262, Group 2: 551777
    
    tender_code = match.group(1)
    app_id = int(match.group(2))
    
    # Save Tender
    cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO tenders (application_id, tender_number, file_name) VALUES (?, ?, ?)",
                (app_id, tender_code, filename))
    
    with open(file_path, 'r', encoding='utf-8') as f:
        soup = BeautifulSoup(f, 'lxml')
        
    # Find table
    table = soup.find('table', class_='ktable')
    if not table:
        return True # File processed, but no bids table (valid state)
        
    bids = []
    
    # The last ktable is usually the one, as per old script logic, but looking at HTML, there is only one ktable.
    # We will use the found one.
    
    tbody = table.find('tbody')
    if not tbody:
        return True

    for row in tbody.find_all('tr'):
        cols = row.find_all('td')
        if len(cols) < 4:
            continue
            
        # 1. Bidder
        bidder_link = cols[0].find('a', onclick=re.compile(r'ShowProfile'))
        if not bidder_link:
            continue
            
        bidder_id_match = re.search(r'ShowProfile\((\d+)\)', bidder_link['onclick'])
        bidder_id = int(bidder_id_match.group(1)) if bidder_id_match else 0
        
        bidder_name_span = cols[0].find('span', class_='color-1') or cols[0].find('span')
        bidder_name = bidder_name_span.get_text(strip=True) if bidder_name_span else "Unknown"
        
        # 2. Last Offer
        last_amt_tag = cols[1].find('strong')
        last_amount = clean_amount(last_amt_tag.text) if last_amt_tag else 0.0
        
        last_date_tag = cols[1].find('span', class_='date')
        last_date = parse_datetime(last_date_tag.text) if last_date_tag else None
        
        # 3. First Offer
        # Structure: "AMOUNT <br> <span class='date'>DATE</span>"
        # We can extract text excluding children or just parse the first number found
        first_date_tag = cols[2].find('span', class_='date')
        first_date = parse_datetime(first_date_tag.text) if first_date_tag else None
        
        # For amount, get text and split? Or just regex the first number
        # cols[2].get_text() -> "3`763`220.00 26.01.2024 16:37"
        full_text_first = cols[2].get_text(separator=' ', strip=True)
        # Remove date part to be safe?
        if first_date_tag:
            date_text = first_date_tag.get_text(strip=True)
            amount_text_part = full_text_first.replace(date_text, '')
        else:
            amount_text_part = full_text_first
            
        first_amount = clean_amount(amount_text_part)
        
        # 4. Count
        # "[1] ... ნახვა"
        count_text = cols[3].get_text(strip=True)
        count_match = re.search(r'\[(\d+)\]', count_text)
        offer_count = int(count_match.group(1)) if count_match else 1
        
        bids.append({
            'app_id': app_id,
            'bidder_id': bidder_id,
            'bidder_name': bidder_name,
            'last_amt': last_amount,
            'last_date': last_date,
            'first_amt': first_amount,
            'first_date': first_date,
            'count': offer_count
        })
        
    # Calculate Rank (lowest amount = rank 1)
    # Sort by last amount ascending
    bids.sort(key=lambda x: x['last_amt'])
    
    for rank, bid in enumerate(bids, 1):
        # Save Bidder
        cur.execute("INSERT OR IGNORE INTO bidders (bidder_id, bidder_name) VALUES (?, ?)", 
                   (bid['bidder_id'], bid['bidder_name']))
        
        # Save Bid
        cur.execute("""
            INSERT OR REPLACE INTO bids (
                application_id, bidder_id, last_offer_amount, last_offer_date,
                first_offer_amount, first_offer_date, offer_count, rank
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            bid['app_id'], bid['bidder_id'], bid['last_amt'], bid['last_date'],
            bid['first_amt'], bid['first_date'], bid['count'], rank
        ))
        
    conn.commit()
    return True

def main():
    parser = argparse.ArgumentParser(description="Parse Tender Bids HTML")
    parser.add_argument('-c', '--cpv', required=True, help="CPV Code")
    parser.add_argument('-root', '--root_dir', help="Custom root directory")
    args = parser.parse_args()
    
    paths = config.get_project_paths(args.cpv, html_dir_name='app_bids', db_filename='bids.db')
    html_dir = paths['HTML_DIR']
    db_path = paths['DB_NAME']
    
    if not os.path.exists(html_dir):
        logger.error(f"HTML directory not found: {html_dir}")
        return
        
    conn = init_db(db_path)
    
    files = [f for f in os.listdir(html_dir) if f.endswith('_app_bids.html')]
    logger.info(f"Found {len(files)} files.")
    
    processed = 0
    for f in tqdm(files, desc="Parsing Bids"):
        fpath = os.path.join(html_dir, f)
        if process_file(fpath, conn):
            processed += 1
            
    conn.close()
    logger.info(f"Done. Processed {processed}/{len(files)} files.")

if __name__ == "__main__":
    main()