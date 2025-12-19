import sqlite3
import os
import re
import argparse
import logging
from pathlib import Path
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import config
from tqdm import tqdm

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def init_database(conn):
    """Инициализация таблиц в базе данных"""
    cursor = conn.cursor()
    
    # Таблица основных данных контракта
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tnd_agr_doc (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tender_id INTEGER NOT NULL UNIQUE,
            html_file_name TEXT,
            has_contract INTEGER DEFAULT 0,
            contract_title TEXT,
            contract_number TEXT,
            contract_amount REAL,
            contract_currency TEXT,
            contract_start_date TEXT,
            contract_end_date TEXT,
            days_remaining INTEGER,
            supplier_name TEXT,
            created_date TEXT,
            total_contract_amount REAL
        )
    ''')
    
    # Таблица файлов контракта
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tnd_agr_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tender_id INTEGER NOT NULL,
            file_type TEXT,
            file_url TEXT UNIQUE,
            original_filename TEXT,
            download_status TEXT DEFAULT 'pending',
            local_path TEXT,
            FOREIGN KEY (tender_id) REFERENCES tnd_agr_doc(tender_id)
        )
    ''')
    
    # Таблица платежей
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tnd_agr_payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tender_id INTEGER NOT NULL,
            order_id INTEGER NOT NULL,
            payment_amount REAL,
            payment_date TEXT,
            record_date TEXT,
            payment_type TEXT,
            FOREIGN KEY (tender_id) REFERENCES tnd_agr_doc(tender_id),
            UNIQUE(tender_id, payment_amount, payment_date, payment_type)
        )
    ''')
    
    conn.commit()

def process_html_file(html_file_path, conn):
    """Обработка одного HTML файла: парсинг и сохранение метаданных."""
    filename = os.path.basename(html_file_path)
    match = re.search(r'pg_\w+_(\d+)_agr_docs\.html', filename)
    tender_id = int(match.group(1)) if match else None
    
    if not tender_id:
        logging.error(f"Не удалось извлечь tender_id из файла: {filename}")
        return

    with open(html_file_path, 'r', encoding='utf-8') as file:
        soup = BeautifulSoup(file.read(), 'html.parser')

    cur = conn.cursor()

    # --- Парсинг и сохранение данных контракта ---
    # ... (логика извлечения данных, адаптированная)
    
    # --- Парсинг и сохранение файлов ---
    all_file_links = soup.find_all('a', href=re.compile(r'files\.php'))
    for link in all_file_links:
        file_url = urljoin(config.BASE_URL, link['href'])
        original_name = link.get_text(strip=True)
        file_type = 'other' # Упрощенно, можно доработать
        
        cur.execute(
            "INSERT OR IGNORE INTO tnd_agr_files (tender_id, file_type, file_url, original_filename, download_status) VALUES (?, ?, ?, ?, ?)",
            (tender_id, file_type, file_url, original_name, 'pending')
        )
    
    # --- Парсинг и сохранение платежей ---
    # ... (логика извлечения платежей)

    conn.commit()

def main():
    """Основная функция"""
    parser = argparse.ArgumentParser(description='Парсинг HTML файлов контрактов тендеров (метаданные)')
    parser.add_argument('-c', '--cpv', required=True, help='CPV код')
    parser.add_argument('-root', '--root_dir', help='Корневая директория')
    parser.add_argument('--silent', '-s', action='store_true', help='Отключить подробные логи')
    args = parser.parse_args()
    
    paths = config.get_project_paths(args.cpv, html_dir_name='agr_docs', db_filename='agr.db')
    HTML_DIR = paths['HTML_DIR']
    DB_PATH = paths['DB_NAME']
    
    if not args.silent:
        logging.info(f"Запуск парсера метаданных контрактов. База данных: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    init_database(conn)
    
    html_files = list(Path(HTML_DIR).glob('*agr_docs*.html'))
    
    for html_file in tqdm(html_files, desc="Парсинг agr_docs", unit="file", disable=args.silent):
        try:
            process_html_file(html_file, conn)
        except Exception as e:
            logging.error(f"Ошибка обработки файла {html_file}: {e}")

    conn.close()
    if not args.silent:
        logging.info("Парсинг метаданных контрактов завершен.")

if __name__ == "__main__":
    main()