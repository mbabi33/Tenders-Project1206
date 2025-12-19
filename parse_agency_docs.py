import os
import re
import sqlite3
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import argparse
import config
from tqdm import tqdm

def extract_tender_id(basename: str):
    """Извлекает ID тендера из имени файла."""
    m = re.search(r'_(\d+)_agency_docs\.html$', basename)
    return m.group(1) if m else None

def parse_author_date(text: str):
    """Парсит строку с датой и автором."""
    text = (text or "").strip()
    if not text:
        return None, None
    if '/' in text:
        left, right = [p.strip() for p in text.split('/', 1)]
        return left or None, right or None
    parts = text.split(None, 1)
    date = parts[0] if parts else None
    author = parts[1] if len(parts) > 1 else None
    return date, author

def process_html(filepath, conn, verbose=True):
    """
    Парсит HTML-файл, извлекает метаданные документов и дисквалификаций,
    и сохраняет их в базу данных.
    """
    basename = os.path.basename(filepath)
    tender_id = extract_tender_id(basename)
    if not tender_id:
        if verbose:
            print(f"Не удалось определить tender_id для {basename}")
        return

    cur = conn.cursor()

    with open(filepath, 'r', encoding='utf-8') as f:
        soup = BeautifulSoup(f, 'html.parser')

    agency_div = soup.find('div', id='agency_docs')
    if not agency_div:
        return

    # --- Парсинг документов ---
    table = agency_div.find('table', id='reports')
    if table and table.tbody:
        for tr in table.tbody.find_all('tr'):
            anchors = tr.find_all('a', href=True)
            if not anchors:
                continue

            tds = tr.find_all('td')
            author_date_text = tds[2].get_text(" ", strip=True) if len(tds) >= 3 else ''
            date_val, author_val = parse_author_date(author_date_text)

            for a in anchors:
                href = a['href'].strip()
                doc_url = urljoin(config.BASE_URL, href)
                original_title = a.get_text(strip=True) or os.path.basename(href)
                
                td = a.find_parent('td')
                is_invalid = 1 if td and 'obsolete1' in td.get('class', []) else 0

                # Вставляем запись в БД со статусом 'pending'
                cur.execute("""INSERT OR IGNORE INTO tnd_agency_docs
                    (tender_id, doc_title, doc_url, author, date, is_invalid, download_status)
                    VALUES (?,?,?,?,?,?,?)""",
                    (tender_id, original_title, doc_url, author_val, date_val, is_invalid, 'pending'))
    
    # --- Парсинг дисквалификаций ---
    dq_div = agency_div.find('div', class_='ui-state-highlight')
    if dq_div and dq_div.table:
        for tr in dq_div.table.find_all('tr'):
            tds = tr.find_all('td')
            if len(tds) >= 3:
                dq_date = tds[0].get_text(strip=True)
                company = tds[1].get_text(strip=True)
                reason = tds[2].get_text(strip=True)
                cur.execute("""INSERT OR IGNORE INTO tnd_disqualifications
                    (tender_id, company_name, disqualification_date, reason)
                    VALUES (?,?,?,?)""",
                    (tender_id, company, dq_date, reason))
    
    conn.commit()

def init_db(conn):
    """Инициализирует таблицы в базе данных."""
    cur = conn.cursor()
    # Таблица документов
    cur.execute("""
    CREATE TABLE IF NOT EXISTS tnd_agency_docs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tender_id   TEXT,
        doc_title   TEXT,
        doc_url     TEXT,
        local_path  TEXT,
        author      TEXT,
        date        TEXT,
        is_invalid  INTEGER DEFAULT 0,
        download_status TEXT DEFAULT 'pending',
        UNIQUE(tender_id, doc_url)
    )
    """)
    # Таблица дисквалификаций
    cur.execute("""
    CREATE TABLE IF NOT EXISTS tnd_disqualifications (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tender_id TEXT,
        company_name TEXT,
        disqualification_date TEXT,
        reason TEXT,
        UNIQUE(tender_id, company_name, disqualification_date)
    )
    """)
    conn.commit()

def main():
    parser = argparse.ArgumentParser(description='Парсинг метаданных документов агентства')
    parser.add_argument('-c', '--cpv', required=True, help='CPV код')
    parser.add_argument('-root', '--root_dir', help='Корневая директория (переопределяет config.py)')
    parser.add_argument('--silent', '-s', action='store_true', help='Отключить подробный вывод')
    args = parser.parse_args()

    # Используем agency_docs, так как organize_files.sh перемещает туда соответствующие файлы
    paths = config.get_project_paths(args.cpv, html_dir_name='agency_docs', db_filename='agency.db')
    HTML_DIR = paths['HTML_DIR']
    DB_PATH = paths['DB_NAME']
    
    if not args.silent:
        print(f"🚀 Запуск парсера метаданных документов агентства...")
        print(f"   CPV-код: {args.cpv}")
        print(f"   База данных: {DB_PATH}")
        print(f"   Директория HTML: {HTML_DIR}")

    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    html_files = [f for f in os.listdir(HTML_DIR) if f.endswith('_agency_docs.html')]
    
    if not args.silent:
        print(f"🔍 Найдено HTML файлов: {len(html_files)}")
        print("🚀 Начинаем обработку...")

    for fname in tqdm(html_files, desc="Парсинг agency_docs", unit="file", disable=args.silent):
        process_html(
            os.path.join(HTML_DIR, fname), 
            conn,
            verbose=not args.silent
        )

    conn.close()

    if not args.silent:
        print(f"✅ Обработка завершена!")
        print(f"💾 Метаданные сохранены в: {DB_PATH}")

if __name__ == "__main__":
    main()