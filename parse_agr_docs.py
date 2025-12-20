#!/usr/bin/env python3
"""
Clean Contract Parser (parse_agr_docs.py)
Parses contract HTML files (_agr_docs) into SQLite database.
Features: Robust parsing of Georgian dates/numbers, handling of amendments, payments, and document links.
"""

import sqlite3
import os
import re
import argparse
import logging
from pathlib import Path
from urllib.parse import urljoin
from datetime import datetime
from bs4 import BeautifulSoup
from tqdm import tqdm
import config

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# --- Helper Functions ---

def clean_text(text):
    """Clean extra whitespace and newlines."""
    if not text:
        return ""
    return " ".join(text.split())

def parse_number(text: str) -> float:
    """Parse Georgian number format (e.g., '1`234.56') to float."""
    if not text:
        return 0.0
    # Remove currency symbols, spaces, backticks (thousands separator)
    clean = re.sub(r'[^\d\.,]', '', text)
    clean = clean.replace(',', '.') # Standardize decimal point
    try:
        # If multiple dots exist (rare error case), keep only the last one
        if clean.count('.') > 1:
            clean = clean.replace('.', '', clean.count('.') - 1)
        return float(clean)
    except ValueError:
        return 0.0

def parse_date(text: str) -> str:
    """Parse Georgian date (DD.MM.YYYY) to ISO format (YYYY-MM-DD)."""
    if not text:
        return None
    # Extract pattern DD.MM.YYYY
    match = re.search(r'(\d{2})\.(\d{2})\.(\d{4})', text)
    if match:
        day, month, year = match.groups()
        return f"{year}-{month}-{day}"
    return None

def parse_datetime(text: str) -> str:
    """Parse Georgian datetime (DD.MM.YYYY HH:MM) to ISO format."""
    if not text:
        return None
    try:
        dt = datetime.strptime(text.strip(), '%d.%m.%Y %H:%M')
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except ValueError:
        return parse_date(text) # Fallback to date only

# --- Database Schema ---

def init_db(db_path: str):
    """Initialize the SQLite database with required tables."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # 1. Main Contract Data
    cur.execute('''
        CREATE TABLE IF NOT EXISTS tnd_agr_doc (
            tender_id INTEGER PRIMARY KEY,
            html_file_name TEXT,
            cpv_code TEXT,
            contract_number TEXT,
            contract_title TEXT,
            contract_amount REAL,
            contract_currency TEXT,
            status TEXT,
            supplier_name TEXT,
            supplier_id INTEGER,
            contract_start_date TEXT,
            contract_end_date TEXT,
            actual_end_date TEXT,
            created_date TEXT,
            total_paid_amount REAL DEFAULT 0.0,
            has_contract INTEGER DEFAULT 0
        )
    ''')

    # 2. Payments (Actual payments)
    cur.execute('''
        CREATE TABLE IF NOT EXISTS tnd_agr_payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tender_id INTEGER,
            amount REAL,
            currency TEXT,
            payment_date TEXT,
            record_date TEXT,
            type TEXT, -- 'payment' or 'advance'
            funding_source TEXT,
            FOREIGN KEY(tender_id) REFERENCES tnd_agr_doc(tender_id),
            UNIQUE(tender_id, amount, payment_date, type)
        )
    ''')

    # 3. Contract Changes (Amendments)
    cur.execute('''
        CREATE TABLE IF NOT EXISTS tnd_contract_changes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tender_id INTEGER,
            change_number TEXT,
            date TEXT,
            amount REAL,
            currency TEXT,
            file_url TEXT,
            FOREIGN KEY(tender_id) REFERENCES tnd_agr_doc(tender_id),
            UNIQUE(tender_id, change_number, date)
        )
    ''')

    # 4. Files (Attached documents)
    cur.execute('''
        CREATE TABLE IF NOT EXISTS tnd_agr_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tender_id INTEGER,
            file_type TEXT,
            url TEXT UNIQUE,
            name TEXT,
            upload_date TEXT,
            author TEXT,
            download_status TEXT DEFAULT 'pending',
            FOREIGN KEY(tender_id) REFERENCES tnd_agr_doc(tender_id)
        )
    ''')

    conn.commit()
    return conn

# --- Parsing Logic ---

def extract_metadata(soup, tender_id, filename, cpv_code):
    """Extract main contract details."""
    data = {
        'tender_id': tender_id,
        'html_file_name': filename,
        'cpv_code': cpv_code,
        'contract_number': None,
        'contract_title': None,
        'contract_amount': 0.0,
        'contract_currency': 'GEL',
        'status': None,
        'supplier_name': None,
        'supplier_id': None,
        'contract_start_date': None,
        'contract_end_date': None,
        'created_date': None,
        'has_contract': 0
    }

    agency_div = soup.find('div', id='agency_docs')
    if not agency_div:
        return data
    
    main_div = agency_div.find('div', class_='ui-state-highlight')
    if not main_div:
        return data
    
    data['has_contract'] = 1

    # Status
    status_span = main_div.find('span', class_=re.compile(r'agrfg\d+'))
    if status_span:
        data['status'] = clean_text(status_span.text)

    # Supplier
    supp_link = main_div.find('a', onclick=re.compile(r'ShowProfile'))
    if supp_link:
        # Extract ID
        sid_match = re.search(r'ShowProfile\((\d+)\)', supp_link['onclick'])
        if sid_match:
            data['supplier_id'] = int(sid_match.group(1))
        
        # Extract Name
        # Ищем strong сразу после ссылки (наиболее частый случай)
        strong = supp_link.find_next_sibling('strong')
        if not strong:
            # Если нет рядом, ищем внутри родительского контейнера ссылки
            strong = supp_link.parent.find('strong')
            
        if strong:
            data['supplier_name'] = clean_text(strong.text)

    # Dates
    text_content = main_div.get_text(separator=' ', strip=True)
    
    # Validity range
    valid_match = re.search(r'ხელშეკრულება ძალაშია:\s*(\d{2}\.\d{2}\.\d{4})\s*-\s*(\d{2}\.\d{2}\.\d{4})', text_content)
    if valid_match:
        data['contract_start_date'] = parse_date(valid_match.group(1))
        data['contract_end_date'] = parse_date(valid_match.group(2))
    
    # Creation date
    created_match = re.search(r'ხელშეკრულების თარიღი:\s*(\d{2}\.\d{2}\.\d{4})', text_content)
    if created_match:
        data['created_date'] = parse_date(created_match.group(1))

    # Number and Amount
    # Using regex on raw html or specific text block often fails due to layout changes.
    # Searching for the specific "convertme" span is most reliable for amount.
    amount_span = main_div.find('span', class_='convertme')
    if amount_span:
        # Try ID attribute first: id="1234.56-GEL-..."
        if amount_span.get('id'):
            parts = amount_span['id'].split('-')
            if len(parts) >= 2:
                data['contract_amount'] = parse_number(parts[0])
                data['contract_currency'] = parts[1]
    
    # If amount still 0, try regex on text "Number / Amount"
    if data['contract_amount'] == 0:
        num_amount_match = re.search(r'ნომერი/თანხა:\s*(.*?)\s*/\s*(.*?)(?:<|$)', str(main_div))
        if num_amount_match:
            data['contract_number'] = clean_text(num_amount_match.group(1))
            data['contract_amount'] = parse_number(num_amount_match.group(2))
    
    # If contract number still empty, try simple text search
    if not data['contract_number']:
        num_match = re.search(r'ნომერი/თანხა:\s*(.*?)\s*/', text_content)
        if num_match:
            data['contract_number'] = clean_text(num_match.group(1))

    return data

def extract_payments(soup, tender_id):
    """Extract payment history."""
    payments = []
    total_paid = 0.0

    # Find payment block
    pay_div = None
    for div in soup.find_all('div', class_='ui-state-highlight'):
        if 'ფაქტობრივი გადახდები' in div.get_text():
            pay_div = div
            break
    
    if not pay_div:
        return payments, total_paid

    # Summary Total
    summary_text = pay_div.get_text()
    paid_match = re.search(r'გადახდილი თანხა:\s*([\d`\.,]+)', summary_text)
    if paid_match:
        total_paid = parse_number(paid_match.group(1))

    # Table
    table = pay_div.find('table', class_='ktable')
    if table:
        for row in table.find_all('tr')[1:]: # Skip header
            cols = row.find_all('td')
            if len(cols) >= 5:
                amt_text = clean_text(cols[0].get_text())
                is_advance = 'ავანსი' in amt_text or 'аванс' in amt_text.lower()
                
                funding = ""
                source_span = cols[0].find('span', class_='color-2')
                if source_span:
                    funding = clean_text(source_span.get_text())

                payments.append({
                    'tender_id': tender_id,
                    'amount': parse_number(amt_text),
                    'currency': 'GEL', # Assuming GEL for now
                    'payment_date': parse_date(cols[3].get_text()),
                    'record_date': parse_date(cols[4].get_text()),
                    'type': 'advance' if is_advance else 'payment',
                    'funding_source': funding
                })
    return payments, total_paid

def extract_changes(soup, tender_id):
    """Extract contract amendments."""
    changes = []
    agency_div = soup.find('div', id='agency_docs')
    if not agency_div:
        return changes

    # Amendments are usually in subsequent highlighted divs
    divs = agency_div.find_all('div', class_='ui-state-highlight', recursive=False)
    for div in divs[1:]: # Skip first (main contract)
        header = div.find('div', style=re.compile(r'text-align:center'))
        if not header or 'ცვლილება' not in header.text:
            continue
        
        table = div.find('table')
        if not table:
            continue
            
        for row in table.find_all('tr'):
            text = row.get_text(separator=' ', strip=True)
            
            # Change Number
            num_match = re.search(r'ცვლილება\s*(\d+)', text)
            change_num = num_match.group(1) if num_match else "Unknown"

            # Date
            date_match = re.search(r'თარიღი:\s*(\d{2}\.\d{2}\.\d{4})', text)
            change_date = parse_date(date_match.group(1)) if date_match else None

            # Amount (if changed)
            amount = 0.0
            amt_match = re.search(r'ნომერი/თანხა:.*?/\s*([\d`\.,]+)', text)
            if amt_match:
                amount = parse_number(amt_match.group(1))

            # File link
            file_url = None
            link = row.find('a', href=re.compile(r'contract\.php'))
            if link:
                file_url = urljoin(config.BASE_URL, link['href'])

            changes.append({
                'tender_id': tender_id,
                'change_number': change_num,
                'date': change_date,
                'amount': amount,
                'currency': 'GEL',
                'file_url': file_url
            })
    return changes

def extract_files(soup, tender_id):
    """Extract related documents."""
    files = []
    table = soup.find('table', id='last_docs')
    if not table:
        return files
    
    for row in table.find_all('tr')[1:]:
        cols = row.find_all('td')
        if len(cols) < 4:
            continue
        
        link = cols[2].find('a')
        if not link:
            continue

        # Metadata from col 3 (Date :: Author)
        meta_text = cols[3].get_text(strip=True)
        upload_date = None
        author = None
        if '::' in meta_text:
            d, a = meta_text.split('::', 1)
            upload_date = parse_datetime(d)
            author = clean_text(a)
        else:
            upload_date = parse_datetime(meta_text)

        files.append({
            'tender_id': tender_id,
            'file_type': 'contract_doc',
            'url': urljoin(config.BASE_URL, link['href']),
            'name': clean_text(link.get_text()),
            'upload_date': upload_date,
            'author': author
        })
    return files

# --- Main Logic ---

def process_file(filepath, conn, cpv_code):
    try:
        filename = os.path.basename(filepath)
        # Extract ID from filename: pg_CODE_ID_agr_docs.html
        match = re.search(r'_(\d+)_agr_docs\.html', filename)
        if not match:
            return False
        tender_id = int(match.group(1))

        with open(filepath, 'r', encoding='utf-8') as f:
            soup = BeautifulSoup(f.read(), 'html.parser')

        # 1. Parse Data
        meta = extract_metadata(soup, tender_id, filename, cpv_code)
        payments, total_paid = extract_payments(soup, tender_id)
        meta['total_paid_amount'] = total_paid
        changes = extract_changes(soup, tender_id)
        files = extract_files(soup, tender_id)

        # 2. Save Data
        cur = conn.cursor()
        
        # Save Meta
        cur.execute('''
            INSERT OR REPLACE INTO tnd_agr_doc (
                tender_id, html_file_name, cpv_code, contract_number, contract_title,
                contract_amount, contract_currency, status, supplier_name, supplier_id,
                contract_start_date, contract_end_date, created_date, total_paid_amount, has_contract
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            meta['tender_id'], meta['html_file_name'], meta['cpv_code'], meta['contract_number'],
            meta['contract_title'], meta['contract_amount'], meta['contract_currency'],
            meta['status'], meta['supplier_name'], meta['supplier_id'],
            meta['contract_start_date'], meta['contract_end_date'], meta['created_date'],
            meta['total_paid_amount'], meta['has_contract']
        ))

        # Save Payments
        for p in payments:
            cur.execute('''
                INSERT OR IGNORE INTO tnd_agr_payments (
                    tender_id, amount, currency, payment_date, record_date, type, funding_source
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (p['tender_id'], p['amount'], p['currency'], p['payment_date'], p['record_date'], p['type'], p['funding_source']))

        # Save Changes
        for c in changes:
            cur.execute('''
                INSERT OR IGNORE INTO tnd_contract_changes (
                    tender_id, change_number, date, amount, currency, file_url
                ) VALUES (?, ?, ?, ?, ?, ?)
            ''', (c['tender_id'], c['change_number'], c['date'], c['amount'], c['currency'], c['file_url']))

        # Save Files
        for f in files:
            cur.execute('''
                INSERT OR IGNORE INTO tnd_agr_files (
                    tender_id, file_type, url, name, upload_date, author
                ) VALUES (?, ?, ?, ?, ?, ?)
            ''', (f['tender_id'], f['file_type'], f['url'], f['name'], f['upload_date'], f['author']))

        conn.commit()
        return True

    except Exception as e:
        logger.error(f"Error processing {filepath}: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description="Parse Agreement Docs HTML")
    parser.add_argument('-c', '--cpv', required=True, help="CPV Code")
    parser.add_argument('-root', '--root_dir', help="Custom root directory")
    args = parser.parse_args()

    # Get paths
    paths = config.get_project_paths(args.cpv, html_dir_name='agr_docs', db_filename='agr.db')
    html_dir = Path(paths['HTML_DIR'])
    db_path = paths['DB_NAME']

    if not html_dir.exists():
        logger.error(f"Directory not found: {html_dir}")
        return

    # Init DB
    conn = init_db(db_path)
    
    # Find files
    files = list(html_dir.glob('*agr_docs*.html'))
    logger.info(f"Found {len(files)} files in {html_dir}")
    logger.info(f"Database: {db_path}")

    # Process
    processed = 0
    for f in tqdm(files, desc="Parsing", unit="file"):
        if process_file(f, conn, args.cpv):
            processed += 1
            
    conn.close()
    logger.info(f"Finished. Successfully processed {processed}/{len(files)} files.")

if __name__ == "__main__":
    main()
