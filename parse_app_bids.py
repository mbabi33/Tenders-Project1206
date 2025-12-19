import sqlite3
from bs4 import BeautifulSoup
import os
import re
import argparse
from datetime import datetime
import config # Import the new unified config module
import logging

# --- Настройка логирования ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Database Functions ---

def init_db(db_path):
    """
    Initializes the database and creates the 'tenders', 'bidders', and 'bids' tables.
    The tables are dropped first to ensure a clean start.
    """
    logger.info(f"Инициализация базы данных: {db_path}")
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        
        # Drop existing tables
        cur.execute("DROP TABLE IF EXISTS tenders")
        cur.execute("DROP TABLE IF EXISTS bidders")
        cur.execute("DROP TABLE IF EXISTS bids")
        
        # Create tenders table
        cur.execute("""
        CREATE TABLE tenders (
            application_id INTEGER PRIMARY KEY,
            tender_number TEXT,
            file_name TEXT NOT NULL
        )
        """
        )
        
        # Create bidders table
        cur.execute("""
        CREATE TABLE bidders (
            bidder_id INTEGER PRIMARY KEY,
            bidder_name TEXT NOT NULL
        )
        """
        )
        
        # Create bids table
        cur.execute("""
        CREATE TABLE bids (
            application_id INTEGER,
            bidder_id INTEGER,
            last_offer_amount REAL,
            last_offer_datetime TEXT,
            first_offer_amount REAL,
            first_offer_datetime TEXT,
            number_of_offers INTEGER,
            final_rank INTEGER,
            PRIMARY KEY (application_id, bidder_id),
            FOREIGN KEY (application_id) REFERENCES tenders (application_id),
            FOREIGN KEY (bidder_id) REFERENCES bidders (bidder_id)
        )
        """
        )
        
    logger.info("✅ База данных и таблицы успешно созданы.")

# --- Parsing Functions ---

def parse_datetime(dt_str):
    """Converts DD.MM.YYYY HH:MM string to YYYY-MM-DD HH:MM:SS format."""
    if not dt_str:
        return None
    try:
        # Parse the date and time, then reformat
        dt_obj = datetime.strptime(dt_str, '%d.%m.%Y %H:%M')
        return dt_obj.strftime('%Y-%m-%d %H:%M:%S')
    except (ValueError, TypeError):
        logger.warning(f"⚠️ Не удалось распарсить дату/время: '{dt_str}'. Возвращено None.")
        return None

def clean_amount(amount_str):

    """Removes backticks, commas, whitespace and converts to float."""

    if not amount_str:

        return 0.0

    # Use strip() to remove leading/trailing whitespace and non-breaking spaces

    cleaned_str = amount_str.replace('`', '').replace(',', '').strip()

    return float(cleaned_str)



def parse_bids_file(file_path, db_path):
    """Parses a single _app_bids.html file and saves the data to the database."""
    filename = os.path.basename(file_path)
    logger.info(f"\n--- Обработка файла: {filename} ---")

    # 1. Extract info from filename
    match = re.search(r'pg_([A-Z]{3})(\d+)_(\d+)_app_bids\.html', filename)
    if not match:
        logger.warning(f"  -> ⚠️ Пропущено: не удалось извлечь ID из имени файла.")
        return

    tender_prefix, tender_number, application_id = match.groups()
    application_id = int(application_id)

    # 2. Save tender key to the database immediately
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute("INSERT OR IGNORE INTO tenders (application_id, tender_number, file_name) VALUES (?, ?, ?)",
                    (application_id, tender_number, filename))

    # 3. Read and parse HTML for bids
    with open(file_path, 'r', encoding='utf-8') as f:
        soup = BeautifulSoup(f, 'lxml')

    # Find all tables with class 'ktable'
    tables = soup.find_all('table', class_='ktable')

    # If no tables are found, the tender is already recorded; no bids to parse.
    if not tables:
        logger.info(f"  -> Информация: Таблица с предложениями не найдена. Тендер записан без предложений.")
        return

    # Always take the last 'ktable', as it consistently contains the detailed bid list
    bid_table = tables[-1]
    
    bids_data = []
    rows = bid_table.find('tbody').find_all('tr')

    # If no rows are found, the tender is already recorded; no bids to parse.
    if not rows:
        logger.info("  -> Информация: В файле нет предложений. Тендер записан без предложений.")
        return

    # 4. Extract bid data from each row
    for row in rows:
        cols = row.find_all('td')
        if len(cols) < 4:
            continue

        # Bidder Info
        profile_link = cols[0].find('a', onclick=True)
        bidder_id_match = re.search(r'ShowProfile\((\d+)\)', profile_link['onclick']) if profile_link else None
        bidder_id = int(bidder_id_match.group(1)) if bidder_id_match else None
        bidder_name = cols[0].find('span').get_text(strip=True) if cols[0].find('span') else 'N/A'

        # Bid Details
        last_offer_amount = clean_amount(cols[1].find('strong').get_text(strip=True))
        last_offer_datetime = parse_datetime(cols[1].find('span', class_='date').get_text(strip=True))
        
        # Robustly extract first offer amount and date
        first_offer_amount_raw = cols[2].get_text(separator='|', strip=True) # Use a separator to distinguish parts
        first_offer_datetime_str = None
        if '|' in first_offer_amount_raw:
            # Assuming format 'AMOUNT|DATE', take the first part as amount and second as date
            parts = first_offer_amount_raw.split('|')
            first_offer_amount = clean_amount(parts[0])
            first_offer_datetime_str = parts[1]
        else:
            # If no separator, assume the whole text is amount (or fallback)
            first_offer_amount = clean_amount(first_offer_amount_raw)
            
        date_span = cols[2].find('span', class_='date')
        if date_span:
            first_offer_datetime_str = date_span.get_text(strip=True)
            
        first_offer_datetime = parse_datetime(first_offer_datetime_str)
        
        num_offers_match = re.search(r'\[(\d+)\]', cols[3].get_text(strip=True))
        number_of_offers = int(num_offers_match.group(1)) if num_offers_match else 0

        bids_data.append({
            'bidder_id': bidder_id,
            'bidder_name': bidder_name,
            'last_offer_amount': last_offer_amount,
            'last_offer_datetime': last_offer_datetime,
            'first_offer_amount': first_offer_amount,
            'first_offer_datetime': first_offer_datetime,
            'number_of_offers': number_of_offers,
        })

    # 5. Calculate final rank
    bids_data.sort(key=lambda x: x['last_offer_amount'])
    for i, bid in enumerate(bids_data):
        bid['final_rank'] = i + 1

    # 6. Save bidders and bids to database
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        
        # Insert bidders and bids
        for bid in bids_data:
            cur.execute("INSERT OR IGNORE INTO bidders (bidder_id, bidder_name) VALUES (?, ?)",
                        (bid['bidder_id'], bid['bidder_name']))
            
            cur.execute("""
            INSERT OR REPLACE INTO bids (
                application_id, bidder_id, last_offer_amount, last_offer_datetime,
                first_offer_amount, first_offer_datetime, number_of_offers, final_rank
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                application_id, bid['bidder_id'], bid['last_offer_amount'], bid['last_offer_datetime'],
                bid['first_offer_amount'], bid['first_offer_datetime'], bid['number_of_offers'], bid['final_rank']
            ))
    
    logger.info(f"  -> Успешно обработано {len(bids_data)} предложений.")

# --- Main Execution ---

def main():
    """Main function to run the parser."""
    parser = argparse.ArgumentParser(description='Парсер предложений по тендерам (app_bids)')
    parser.add_argument('-c', '--cpv', type=str, required=True, help='CPV код (обязательный)')
    parser.add_argument('-root', '--root-dir', type=str, help='Корневая директория (переопределяет config.py)')
    args = parser.parse_args()
    
    cpv_code = args.cpv
    
    # Get paths using the new unified config
    PATHS = config.get_project_paths(cpv_code, html_dir_name='app_bids', db_filename='bids.db')
    db_path = PATHS['DB_NAME']
    html_dir = PATHS['HTML_DIR']

    logger.info(f"🚀 Запуск парсера предложений...")
    logger.info(f"   CPV код: {cpv_code}")
    logger.info(f"   База данных: {db_path}")
    logger.info(f"   Директория HTML: {html_dir}")
    
    if not os.path.exists(html_dir):
        logger.error(f"❌ Директория '{html_dir}' не найдена. Проверьте CPV код и путь.")
        return

    # Initialize database
    init_db(db_path)

    # Get list of files to parse
    html_files = [f for f in os.listdir(html_dir) if f.endswith('_app_bids.html')]
    
    if not html_files:
        logger.warning(f"⚠️ В директории '{html_dir}' не найдены файлы '*_app_bids.html'.")
        return

    logger.info(f"📁 Найдено файлов для обработки: {len(html_files)}")
    
    # Process each file
    for fname in html_files:
        file_path = os.path.join(html_dir, fname)
        try:
            parse_bids_file(file_path, db_path)
        except Exception as e:
            logger.error(f"  -> ❌ Ошибка при обработке файла {fname}: {e}")
    
    logger.info("\n🎉 Парсинг завершен. Данные о предложениях сохранены в БД!")

if __name__ == '__main__':
    # Before running, make sure you have BeautifulSoup and lxml installed:
    # pip install beautifulsoup4 lxml
    main()
