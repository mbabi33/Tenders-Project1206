#!/usr/bin/env python3
import sqlite3
from bs4 import BeautifulSoup
import os
import re
import argparse
import config # Import the new unified config module
import logging
from datetime import datetime

# --- Настройка логирования ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def init_db(db_path):
    """Создает новую таблицу 'tenders' для хранения 23 полей."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS tenders") # Drop existing table to ensure clean start
    cur.execute("""
    CREATE TABLE tenders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cpv_code TEXT,
        lotsType TEXT,
        tenderCodePrefix TEXT,
        tender_db_id INTEGER,
        lotsNumber TEXT UNIQUE,
        lotsUrl TEXT,
        lotStatus TEXT,
        lotsDate TEXT,
        submitStartDate TEXT,
        lotsDateEnd TEXT,
        lotsPrice REAL,
        lotsCurrency TEXT,
        lotsPayCondition TEXT,
        lotsCategory TEXT,
        classifierCodes TEXT,
        lotsDeliveryTerm TEXT,
        lotsName TEXT,
        lotsDeliveryPlace TEXT,
        purchaseQuantityVolume TEXT,
        bidStep REAL,
        guaranteeValidityDays TEXT,
        customerName TEXT,
        year INTEGER,
        source_file TEXT
    )
    """)
    conn.commit()
    conn.close()
    logger.info(f"✅ База данных '{db_path}' и таблица 'tenders' созданы.")

def parse_tender_file(file_path, db_path, cpv_code):
    """Извлекает данные из HTML-файла и сохраняет их в базу данных."""
    with open(file_path, 'r', encoding='utf-8') as f:
        soup = BeautifulSoup(f, 'html.parser')

    # --- Вспомогательные функции ---
    def get_field_text(label):
        element = soup.find(lambda tag: tag.name == 'td' and label in tag.get_text())
        if element and element.find_next_sibling('td'):
            return element.find_next_sibling('td').get_text(strip=True)
        return None

    def get_classifier_codes():
        codes = []
        label_td = soup.find('td', string=lambda t: t and 'კლასიფიკატორის კოდები' in t)
        if label_td:
            next_td = label_td.find_next_sibling('td')
            if next_td:
                for li in next_td.find_all('li'):
                    codes.append(li.get_text(strip=True))
        return ", ".join(codes) if codes else None

    # --- Извлечение данных из имени файла ---
    filename = os.path.basename(file_path)
    tenderCodePrefix, tender_db_id = None, None
    file_match = re.search(r'pg_([A-Z]{3})\d+_(\d+)_app_main\.html', filename)
    if file_match:
        tenderCodePrefix = file_match.group(1)
        tender_db_id = int(file_match.group(2))

    # --- Извлечение данных из HTML-контента ---
    lotsType = get_field_text('შესყიდვის ტიპი')
    lotsNumber = get_field_text('განცხადების ნომერი')
    lotStatus = get_field_text('შესყიდვის სტატუსი')
    customerName = get_field_text('შემსყიდველი')
    lotsDate = get_field_text('შესყიდვის გამოცხადების თარიღი')
    submitStartDate = get_field_text('წინადადებების მიღება იწყება')
    lotsDateEnd = get_field_text('წინადადებების მიღება მთავრდება')
    lotsPayCondition = get_field_text('წინადადება წარმოდგენილი უნდა იყოს')
    lotsCategory = get_field_text('შესყიდვის კატეგორია')
    lotsDeliveryTerm = get_field_text('მოწოდების ვადა')
    lotsName = get_field_text('დამატებითი ინფორმაცია')
    purchaseQuantityVolume = get_field_text('შესყიდვის რაოდენობა ან მოცულობა')
    guaranteeValidityDays = get_field_text('გარანტიის მოქმედების ვადა')
    
    pre_tag = soup.find('pre')
    lotsUrl = pre_tag.get_text(strip=True).split()[-1] if pre_tag else None

    price_raw = get_field_text('შესყიდვის სავარაუდო ღირებულება')
    lotsPrice, lotsCurrency = None, None
    if price_raw:
        price_match = re.search(r'([\d`,\']+\.?\d*)', price_raw)
        if price_match:
            lotsPrice = float(price_match.group(1).replace('`', '').replace(',', ''))
        currency_match = re.search(r'([A-Z]{3})', price_raw)
        if currency_match:
            lotsCurrency = currency_match.group(1)

    bid_step_raw = get_field_text('შეთავაზების ფასის კლების ბიჯი')
    bidStep = None
    if bid_step_raw:
        bid_match = re.search(r'([\d`,\']+\.?\d*)', bid_step_raw)
        if bid_match:
            bidStep = float(bid_match.group(1).replace('`', '').replace(',', ''))

    lotsDeliveryPlace = None
    if lotsName:
        loc_match = re.search(r'(?:სოფელ|ქალაქ|დაბა)\s*([ა-ჰ\s]+)(?:ში|ის)', lotsName)
        if loc_match:
            lotsDeliveryPlace = loc_match.group(1).strip()
        else:
            loc_match = re.search(r'([ა-ჰ\s]+)\s*\(საკადასტრო კოდი:', lotsName)
            if loc_match:
                lotsDeliveryPlace = loc_match.group(1).strip()

    classifierCodes = get_classifier_codes()
    year = None
    if lotsDate:
        try:
            # lotsDate format: DD.MM.YYYY HH:MM
            year = datetime.strptime(lotsDate.split(' ')[0], '%d.%m.%Y').year
        except ValueError:
            logger.warning(f"⚠️ Не удалось распарсить год из lotsDate: {lotsDate}. Используется текущий год.")
            year = datetime.now().year
    if year is None:
        year = datetime.now().year # Fallback to current year if lotsDate is empty

    # --- Сохранение в базу данных ---
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    try:
        cur.execute("""
        INSERT INTO tenders (
            cpv_code, lotsType, tenderCodePrefix, tender_db_id, lotsNumber, lotsUrl, lotStatus, lotsDate,
            submitStartDate, lotsDateEnd, lotsPrice, lotsCurrency, lotsPayCondition,
            lotsCategory, classifierCodes, lotsDeliveryTerm, lotsName, lotsDeliveryPlace,
            purchaseQuantityVolume, bidStep, guaranteeValidityDays, customerName, year, source_file
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            cpv_code, lotsType, tenderCodePrefix, tender_db_id, lotsNumber, lotsUrl, lotStatus, lotsDate,
            submitStartDate, lotsDateEnd, lotsPrice, lotsCurrency, lotsPayCondition,
            lotsCategory, classifierCodes, lotsDeliveryTerm, lotsName, lotsDeliveryPlace,
            purchaseQuantityVolume, bidStep, guaranteeValidityDays, customerName, year, filename
        ))
        conn.commit()
        logger.info(f"  -> Успешно обработан: {lotsNumber}")
    except sqlite3.IntegrityError:
        logger.info(f"  -> Пропущен дубликат: {lotsNumber} (tender_db_id: {tender_db_id})")
    except Exception as e:
        logger.error(f"  -> Ошибка при обработке {filename}: {e}")
    finally:
        conn.close()

def main():
    """Основная функция"""
    parser = argparse.ArgumentParser(description='Парсер основной информации о тендерах')
    parser.add_argument('-c', '--cpv', type=str, required=True, help='CPV код (обязательный)')
    # The -root argument is now handled by the config module directly
    parser.add_argument('-root', '--root-dir', type=str, help='Корневая директория (переопределяет конфиг)')
    args = parser.parse_args()
    
    cpv_code = args.cpv
    
    # Get paths using the new unified config, specifying the correct html directory
    PATHS = config.get_project_paths(cpv_code, html_dir_name='app_main')
    
    DB_NAME = PATHS['DB_NAME']
    HTML_DIR = PATHS['HTML_DIR']

    logger.info(f"🚀 Запуск парсера основной информации...")
    logger.info(f"   CPV код: {cpv_code}")
    logger.info(f"   База данных: {DB_NAME}")
    logger.info(f"   HTML файлы: {HTML_DIR}")
    
    if not os.path.exists(HTML_DIR):
        logger.error(f"❌ Директория '{HTML_DIR}' не найдена. Проверьте путь и CPV код.")
        return

    init_db(DB_NAME)

    html_files = [f for f in os.listdir(HTML_DIR) if f.endswith('_app_main.html')]
    
    if not html_files:
        logger.warning(f"⚠️ В директории '{HTML_DIR}' не найдены файлы '*_app_main.html'.")
        return

    logger.info(f"📁 Найдено app_main файлов: {len(html_files)}")
    
    processed = 0
    for fname in html_files:
        file_path = os.path.join(HTML_DIR, fname)
        logger.info(f"\n--- Обработка: {fname} ---")
        parse_tender_file(file_path, DB_NAME, cpv_code)
        processed += 1
    
    logger.info(f"\n📊 РЕЗУЛЬТАТ: Обработано {processed} из {len(html_files)} файлов")
    logger.info("🎉 Основная информация о тендерах сохранена в БД!")

if __name__ == '__main__':
    main()
