import os
import re
import sqlite3
import argparse
from bs4 import BeautifulSoup
from tqdm import tqdm
import config
import logging

# --- Настройка логирования ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- END CONFIGURATION ---

def init_db(conn):
    """
    Инициализирует таблицы в базе данных с правильной схемой.
    """
    cur = conn.cursor()
    
    # Таблица для хранения текстовых секций
    cur.execute("""
    CREATE TABLE IF NOT EXISTS tnd_app_doc_sections (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tender_code TEXT,
        tender_db_id INTEGER,
        section_id TEXT,
        section_title TEXT,
        section_text TEXT,
        UNIQUE(tender_db_id, section_id)
    )
    """)
    
    # Таблица для хранения метаданных о файлах
    cur.execute("""
    CREATE TABLE IF NOT EXISTS tnd_app_doc_files (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tender_code TEXT,
        tender_db_id INTEGER,
        section_id TEXT,
        file_name TEXT,
        file_url TEXT,
        upload_date TEXT,
        local_path TEXT,
        download_status TEXT DEFAULT 'pending',
        UNIQUE(tender_db_id, section_id, file_name)
    )
    """)
    
    conn.commit()
    print("Database initialized successfully.")

def clean_filename(name: str) -> str:
    """
    Очищает имя файла:
    1. Заменяет пробелы и спецсимволы (включая '-') на '_'.
    2. Схлопывает несколько подряд идущих '_' в один.
    3. Убирает '_' в начале и в конце имени.
    """
    if not name:
        return ""
    
    base, ext = os.path.splitext(name)
    
    # Заменяем все, что не является буквой, цифрой или грузинским символом, на '_'
    cleaned_base = re.sub(r'[^0-9A-Za-zა-ჰ]+', '_', base)
    # Убираем лишние '_' в начале и конце
    cleaned_base = cleaned_base.strip('_')
    
    return f"{cleaned_base}{ext}"

def parse_qa_structure(soup, tender_code, tender_db_id, cur):
    """
    Парсит HTML-структуру "Типа Б" (Вопросы-Ответы).
    """
    sections = soup.find_all('section', class_='question')
    
    for section in sections:
        section_id = section.get('id', '')
        title_tag = section.find('p', class_='q')
        title = title_tag.get_text(strip=True) if title_tag else ''
        
        text_div = section.find('div', class_='a')
        text_content = text_div.get_text('\n', strip=True) if text_div else ''
        
        cur.execute("""
        INSERT OR IGNORE INTO tnd_app_doc_sections (tender_code, tender_db_id, section_id, section_title, section_text)
        VALUES (?, ?, ?, ?, ?)
        """, (tender_code, tender_db_id, section_id, title, text_content))
        
        answ_file_div = section.find('div', class_='answ-file')
        if answ_file_div:
            for a_tag in answ_file_div.find_all('a', href=True):
                original_name = a_tag.get_text(strip=True)
                cleaned_name = clean_filename(original_name)
                href = a_tag['href']
                file_url = config.BASE_URL + href if not href.startswith('http') else href
                
                cur.execute("""
                INSERT OR IGNORE INTO tnd_app_doc_files (tender_code, tender_db_id, section_id, file_name, file_url, upload_date)
                VALUES (?, ?, ?, ?, ?, ?)
                """, (tender_code, tender_db_id, section_id, cleaned_name, file_url, None))

def parse_filelist_structure(soup, tender_code, tender_db_id, cur):
    """
    Парсит HTML-структуру "Типа А" (Список файлов).
    """
    docs_table = soup.find('table', id='tender_docs')
    if not docs_table:
        return

    section_id = 'main_documentation'
    
    for row in docs_table.find('tbody').find_all('tr'):
        file_cell = row.find('td', class_='obsolete0')
        date_cell = row.find('td', class_='date')
        
        if not file_cell or not date_cell:
            continue
            
        a_tag = file_cell.find('a', href=True)
        if not a_tag:
            continue
            
        original_name = a_tag.get_text(strip=True)
        cleaned_name = clean_filename(original_name)
        href = a_tag['href']
        file_url = config.BASE_URL + href if not href.startswith('http') else href
        
        date_author_text = date_cell.get_text(strip=True)
        upload_date = date_author_text.split('::')[0].strip()
        
        cur.execute("""
        INSERT OR IGNORE INTO tnd_app_doc_files (tender_code, tender_db_id, section_id, file_name, file_url, upload_date)
        VALUES (?, ?, ?, ?, ?, ?)
        """, (tender_code, tender_db_id, section_id, cleaned_name, file_url, upload_date))

def process_html_file(filepath, conn):
    """
    Обрабатывает один HTML-файл, определяя его структуру и вызывая соответствующий парсер.
    """
    cur = conn.cursor()
    
    match = re.search(r'pg_([A-Z0-9]+)_(\d+)_app_docs\.html$', os.path.basename(filepath))
    if not match:
        print(f"  [WARNING] Could not extract IDs from filename: {os.path.basename(filepath)}")
        return
        
    tender_code, tender_db_id_str = match.groups()
    tender_db_id = int(tender_db_id_str)
    
    with open(filepath, 'r', encoding='utf-8') as f:
        soup = BeautifulSoup(f, 'html.parser')
        
    if soup.find('section', class_='question'):
        parse_qa_structure(soup, tender_code, tender_db_id, cur)
    elif soup.find('table', id='tender_docs'):
        parse_filelist_structure(soup, tender_code, tender_db_id, cur)
    else:
        print(f"  [WARNING] Unknown HTML structure in file: {os.path.basename(filepath)}")
        
    conn.commit()

def main():
    """
    Главная функция скрипта.
    """
    parser = argparse.ArgumentParser(description='Парсер документов тендера (app_docs)')
    parser.add_argument('-c', '--cpv', type=str, required=True, help='CPV код (обязательный)')
    parser.add_argument('-root', '--root-dir', type=str, help='Корневая директория (переопределяет config.py)')
    args = parser.parse_args()
    cpv_code = args.cpv

    # Получаем пути через централизованный config.py
    PATHS = config.get_project_paths(cpv_code, html_dir_name='app_docs', db_filename='docs.db')
    db_path = PATHS['DB_NAME']
    html_dir = PATHS['HTML_DIR']

    print(f"🚀 Запуск парсера документов...")
    print(f"   CPV код: {cpv_code}")
    print(f"   База данных: {db_path}")
    print(f"   Директория HTML: {html_dir}")

    if not os.path.exists(html_dir):
        print(f"[ERROR] Директория HTML не найдена: {html_dir}")
        return

    # Проверка на существование директории для БД не так важна,
    # так как get_project_paths и sqlite3.connect могут ее создать.
    
    conn = sqlite3.connect(db_path)
    
    init_db(conn)
    
    try:
        html_files = [f for f in os.listdir(html_dir) if f.endswith('_app_docs.html')]
        print(f"Найдено {len(html_files)} HTML файлов для обработки.")
    except FileNotFoundError:
        print(f"[ERROR] Не удалось прочитать файлы, директория не найдена: {html_dir}")
        conn.close()
        return

    for fname in tqdm(html_files, desc="Parsing HTML files", unit="file"):
        filepath = os.path.join(html_dir, fname)
        try:
            process_html_file(filepath, conn)
        except Exception as e:
            print(f"[ERROR] Не удалось обработать файл {fname}: {e}")
            
    conn.close()
    print("✅ Парсинг завершен!")

if __name__ == '__main__':
    main()