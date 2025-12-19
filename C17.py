#!/usr/bin/env python3
from urllib.parse import urlencode
from selenium import webdriver
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service
import pandas as pd
import time
import logging

import os
import argparse
import re
import config
from datetime import datetime, timedelta

# --- Настройка логирования ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Функция для определения дат по умолчанию ---
def get_default_dates():
    """
    Возвращает даты по умолчанию:
    - date_start: 1 день предыдущего месяца
    - date_end: вчерашний день
    Формат: "DD.MM.YYYY"
    """
    today = datetime.now()
    
    # Вчерашний день
    date_end = (today - timedelta(days=1)).strftime("%d.%m.%Y")
    
    # 1 день предыдущего месяца
    first_day_prev_month = today.replace(day=1) - timedelta(days=1)
    first_day_prev_month = first_day_prev_month.replace(day=1)
    date_start = first_day_prev_month.strftime("%d.%m.%Y")
    
    return date_start, date_end

# --- Парсинг аргументов командной строки ---
def parse_arguments():
    parser = argparse.ArgumentParser(description='Парсер тендеров с портала госзакупок')
    parser.add_argument('-c', '--cpv', type=str, required=True, help='CPV код (обязательный)')
    parser.add_argument('-root', '--root-dir', type=str, help='Корневая директория')
    parser.add_argument('-ds', '--date-start', type=str, help='Дата начала (формат: DD.MM.YYYY)')
    parser.add_argument('-de', '--date-end', type=str, help='Дата окончания (формат: DD.MM.YYYY)')
    parser.add_argument('-ps', '--page-start', type=int, help='Начальная страница')
    parser.add_argument('-pe', '--page-end', type=int, help='Конечная страница')
    parser.add_argument('--update', action='store_true', help='Принудительно обновлять данные, даже если они уже существуют.')
    
    args = parser.parse_args()
    
    # Устанавливаем значения по умолчанию для дат
    DATE_START, DATE_END = get_default_dates()
    DATE_FROM = args.date_start if args.date_start else DATE_START
    DATE_TILL = args.date_end if args.date_end else DATE_END
    
    # Handle page arguments
    START_PAGE = args.page_start if args.page_start is not None else 1
    
    # If -pe is 0, it means process all pages until the end
    if args.page_end == 0:
        PAGE_END_ARG = None
    else:
        PAGE_END_ARG = args.page_end
    
    return args.cpv, DATE_FROM, DATE_TILL, START_PAGE, PAGE_END_ARG, args.root_dir, args.update

# --- Функция для определения общего количества страниц ---
def extract_total_pages(pagination_info):
    """
    Извлекает общее количество страниц из pagination_info
    Пример: '52 ჩანაწერი (გვერდი: 1/13)' → возвращает 13
    """
    try:
        match = re.search(r'გვერდი:\s*(\d+)/(\d+)', pagination_info)
        if match:
            return int(match.group(2))
    except:
        pass
    return 1

# --- Получаем аргументы ---
TARGET_CPV_CODE, DATE_FROM, DATE_TILL, START_PAGE, PAGE_END_ARG, ROOT_DIR_ARG, UPDATE_FLAG = parse_arguments()



# --- Получаем пути для текущего CPV кода ---
PATHS = config.get_project_paths(TARGET_CPV_CODE)

# Распаковываем пути для удобства
BASE_DIR = PATHS['BASE_DIR']
CSV_FILE = PATHS['CSV_FILE']
LINKS_CSV_FILE = PATHS['LINKS_CSV_FILE']
OUTPUT_DIR = PATHS['HTML_DIR']
GECKODRIVER_PATH = config.GECKODRIVER_PATH
FIREFOX_PATH = config.FIREFOX_PATH

# --- WebDriver setup ---
options = Options()
options.add_argument("--headless") # Enabled --headless option
options.binary_location = FIREFOX_PATH
service = Service(executable_path=GECKODRIVER_PATH)
driver = webdriver.Firefox(service=service, options=options)
wait = WebDriverWait(driver, 20)

# --- Функции для вкладок ---
def build_tab_urls(app_id, token):
    base_url = "https://tenders.procurement.gov.ge/public/library/controller.php"
    return {
        "first_tab": f"{base_url}?action=application&app_id={app_id}&app_reg=&key={token}",
        "app_main": f"{base_url}?action=app_main&app_id={app_id}&key={token}",
        "app_docs": f"{base_url}?action=app_docs&app_id={app_id}&key={token}",
        "app_bids": f"{base_url}?action=app_bids&app_id={app_id}&key={token}",
        "agency_docs": f"{base_url}?action=agency_docs&app_id={app_id}&key={token}",
        "agr_docs": f"{base_url}?action=agr_docs&app_id={app_id}"  # без token
    }

def save_tab_pages(driver, app_id, token, page_num, all_links_global, tender_no, tender_code, tdr_start, tdr_end, tdr_status):
    """
    Переходит по всем вкладкам App ID, сохраняет HTML и собирает все ссылки на странице.
    Добавляет только новые ссылки в глобальный список.
    """
    tabs = build_tab_urls(app_id, token)
    current_tender_new_links = [] # Ссылки, найденные в текущем тендере

    # Загружаем существующие ссылки для этого tender_code, если файл существует
    existing_links_for_cpv = pd.DataFrame()
    if os.path.exists(LINKS_CSV_FILE):
        try:
            existing_links_for_cpv = pd.read_csv(LINKS_CSV_FILE, dtype={'tender_code': str, 'tender': str})
            existing_links_for_cpv = existing_links_for_cpv[existing_links_for_cpv['tender_code'] == tender_code]
            existing_links_for_cpv = existing_links_for_cpv[existing_links_for_cpv['tender'] == f"TENDER_{app_id}"]
        except Exception as e:
            logger.warning(f"⚠️ Ошибка при чтении {LINKS_CSV_FILE} для App ID {app_id}: {e}")


    for tab_name, tab_url in tabs.items():
        try:
            driver.get(tab_url)
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            time.sleep(1)

            # --- Save HTML ---
            filename = os.path.join(OUTPUT_DIR, f"pg_{tender_no}_{app_id}_{tab_name}.html")
            with open(filename, "w", encoding="utf-8") as f:
                f.write(driver.page_source)
            logger.info(f"💾 Page {page_num}: Saved {filename}")

            # --- Collect links ---
            links = driver.find_elements(By.TAG_NAME, "a")
            for a in links:
                text = a.text.strip()
                href = a.get_attribute("href")
                if href:
                    href = href.replace("library/library", "library")
                    link_data = {
                        "tender_code": tender_code,
                        "tender_name": tender_no,
                        "tender": f"TENDER_{app_id}",
                        "tab_name": tab_name,
                        "text": text,
                        "url": href,
                        "tender_start": tdr_start,
                        "tender_end": tdr_end,
                        "tender_status": tdr_status
                    }
                    current_tender_new_links.append(link_data)

        except Exception as e:
            logger.warning(f"⚠️ Page {page_num}: Failed to save tab '{tab_name}': {e}")
            continue

    # Добавляем только уникальные новые ссылки в глобальный список all_links_global
    if current_tender_new_links:
        current_tender_new_links_df = pd.DataFrame(current_tender_new_links)

        if not existing_links_for_cpv.empty:
            # Объединяем существующие и новые ссылки, удаляем дубликаты, оставляя существующие
            # Это гарантирует, что мы не будем добавлять уже существующие ссылки
            combined_df = pd.concat([existing_links_for_cpv, current_tender_new_links_df])
            # Удаляем дубликаты по 'url', 'tender', 'tab_name', 'text'
            # 'keep='first'' означает, что существующие (из existing_links_for_cpv) будут сохранены, а дубликаты из new_links_df удалены
            unique_new_links_df = combined_df.drop_duplicates(subset=['url', 'tender', 'tab_name', 'text'], keep=False)
            # Добавляем только те, которые не были в existing_links_for_cpv (т.е. новые уникальные)
            # Это может быть сложной логикой, проще добавить все новые и затем отфильтровать

            # Более простой подход: добавляем все, а затем оставляем уникальные, но только те, которых не было раньше
            # Создаем уникальный идентификатор для сравнения
            existing_links_for_cpv['link_id'] = existing_links_for_cpv['url'] + existing_links_for_cpv['tender'] + existing_links_for_cpv['tab_name'] + existing_links_for_cpv['text']
            current_tender_new_links_df['link_id'] = current_tender_new_links_df['url'] + current_tender_new_links_df['tender'] + current_tender_new_links_df['tab_name'] + current_tender_new_links_df['text']

            new_links_to_add_df = current_tender_new_links_df[~current_tender_new_links_df['link_id'].isin(existing_links_for_cpv['link_id'])]
            new_links_to_add_df = new_links_to_add_df.drop(columns=['link_id'])
            
            if not new_links_to_add_df.empty:
                all_links_global.extend(new_links_to_add_df.to_dict(orient='records'))
                logger.info(f"   -> App ID {app_id}: Добавлено {len(new_links_to_add_df)} новых уникальных ссылок.")
            else:
                logger.info(f"   -> App ID {app_id}: Новых уникальных ссылок не найдено.")

        else:
            # Если файла LINKS_CSV_FILE не было, все найденные ссылки - новые
            all_links_global.extend(current_tender_new_links)
            logger.info(f"   -> App ID {app_id}: Добавлено {len(current_tender_new_links)} ссылок (файл LINKS_CSV_FILE создан впервые).")

def perform_search():
    """
    Открывает страницу поиска, выбирает CPV код, задает даты и нажимает Search.
    Возвращает True, если страница загрузилась и таблица тендеров доступна.
    """
    driver.get("https://tenders.procurement.gov.ge/public/?lang=ge")

    # --- Select CPV code ---
    select_elem = wait.until(EC.presence_of_element_located((By.ID, "app_basecode")))
    select = Select(select_elem)
    for option in select.options:
        if TARGET_CPV_CODE in option.text:
            select.select_by_visible_text(option.text)
            break

    # --- Fill date fields ---
    driver.execute_script(f"document.getElementById('app_date_from').value='{DATE_FROM}'")
    driver.execute_script(f"document.getElementById('app_date_till').value='{DATE_TILL}'")

    # --- Click Search ---
    wait.until(EC.element_to_be_clickable((By.ID, "search_btn"))).click()
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "#content tbody tr")))
    time.sleep(1)


def load_existing_app_ids(csv_file_path):
    """
    Загружает существующие App ID и их метаданные из CSV файла, если он существует.
    Возвращает DataFrame с App ID, номером тендера, датами и статусом.
    """
    existing_df = pd.DataFrame(columns=['application_id', 'tender_num', 'tender_start', 'tender_end', 'tender_status'])
    if os.path.exists(csv_file_path):
        try:
            df = pd.read_csv(csv_file_path, dtype={'application_id': str})
            required_cols = ['application_id', 'tender_num', 'tender_start', 'tender_end', 'tender_status']
            if all(col in df.columns for col in required_cols):
                existing_df = df[required_cols]
                logger.info(f"✅ Загружено {len(existing_df)} существующих записей тендеров из {csv_file_path}")
            else:
                logger.warning(f"⚠️ CSV файл {csv_file_path} не содержит всех необходимых колонок для сравнения. Будет обработан как новый.")
        except Exception as e:
            logger.error(f"⚠️ Ошибка при чтении {csv_file_path}: {e}")
    return existing_df

from bs4 import BeautifulSoup

def parse_urls(html):
    """
    Extract App IDs and Tokens from the page without going inside tenders.
    Returns:
        page_data: list of dicts with App ID and Token
        pagination_info: text info about page
    """
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.select("#content tbody tr")

    announcement_tags = soup.select("p:-soup-contains('განცხადების ნომერი:') > strong")
    tenders_n = [tag.get_text().strip() for tag in announcement_tags]

    # dates
    start_date_tags = soup.select("p:-soup-contains('შესყიდვის გამოცხადების თარიღი:')")
    tg_starts = [tag.get_text().split(':')[-1].strip() for tag in start_date_tags]

    # Select all p tags that contain the phrase "წინდადებების მიღების ვადა:"
    # Then get the text from those tags.
    end_date_tags = soup.select("p:-soup-contains('წინდადებების მიღების ვადა:')")
    tg_ends = [tag.get_text().split(':')[-1].strip() for tag in end_date_tags]

    # ststius
    status_tags = soup.select('p.status')

    # Создать список извлеченного текста
    all_statuses = [tag.get_text(strip=True) for tag in status_tags]

    # tenders_n = soup.select("#content tbody განცხადების ნომერი: strong")
    # breakpoint()
    page_data = []

    for row in rows:
        onclick = row.get("onclick", "")
        app_id, token = "", ""
        m = re.search(r"ShowApp\((\d+),\s*'[^']*',\s*\d+,\s*'([^']+)'\)", onclick)
        if m:
            app_id, token = m.groups()

        if not app_id or not token:
            continue

        page_data.append({
            "App ID": app_id,
            "Token": token
        })

    # Pagination info
    pagination_span = soup.find("span", string=lambda s: s and "ჩანაწერი" in s)
    pagination_info = pagination_span.get_text(strip=True) if pagination_span else "Not found"

    return page_data, pagination_info, tenders_n, tg_starts, tg_ends, all_statuses

# --- Main ---
all_data = []
all_links = []

try:
    logger.info(f"🎯 Настройки парсера:")
    logger.info(f"   CPV код: {TARGET_CPV_CODE}")
    logger.info(f"   Период: {DATE_FROM} - {DATE_TILL}")
    logger.info(f"   Проект: {BASE_DIR}")
    
    # --- Загружаем уже существующие App ID и их метаданные ---
    existing_tenders_df = load_existing_app_ids(CSV_FILE)
    existing_app_ids_set = set(existing_tenders_df['application_id'].astype(str)) if not existing_tenders_df.empty else set()
    
    # --- STAGE 1: Collect all tender information from all search pages ---
    logger.info("\n--- STAGE 1: Collecting tender information from all pages ---")
    
    perform_search()
    
    page_html = driver.page_source
    _, pagination_info, _, _, _, _ = parse_urls(page_html)
    total_pages = extract_total_pages(pagination_info)
    END_PAGE = PAGE_END_ARG if PAGE_END_ARG is not None else total_pages
    if END_PAGE > total_pages:
        logger.warning(f"⚠️ Запрошенная конечная страница {END_PAGE} больше общего количества страниц {total_pages}")
        END_PAGE = total_pages
    
    logger.info(f"📊 Найдено: {pagination_info}")
    logger.info(f"📄 Страницы для обработки: {START_PAGE}-{END_PAGE} из {total_pages}")

    # This single loop handles both pagination and conditional scraping
    for current_page in range(1, END_PAGE + 1):
        
        # --- Pagination Logic: If we're past page 1, click to the next page ---
        if current_page > 1:
            logger.info(f"   -> Navigating to page {current_page}...")
            try:
                next_btn = wait.until(EC.presence_of_element_located((By.ID, "btn_next")))
                driver.execute_script("arguments[0].click();", next_btn)
                wait.until(EC.text_to_be_present_in_element((By.CSS_SELECTOR, "div.pager button.ui-button-text-icon-primary"), f"გვერდი: {current_page}/"))
                time.sleep(1)
            except Exception as e:
                logger.warning(f"⚠️ Page {current_page}: Failed to navigate. Error: {e}. Stopping pagination.")
                break
        
        # --- Scraping Logic: Only scrape if the page is in the desired range ---
        if current_page >= START_PAGE:
            logger.info(f"\n--- Processing page {current_page} ---")
            page_html = driver.page_source
            page_data, _, tenders, tenders_start, tenders_end, all_tender_statuses = parse_urls(page_html)
            
            if not page_data:
                 logger.warning(f"⚠️ Page {current_page}: No tenders found on this page. Continuing...")
                 continue

            logger.info(f"✅ Page {current_page}: Found {len(page_data)} tenders")

            for item, tender_num, tender_start, tender_end, tender_status in zip(page_data, tenders, tenders_start, tenders_end, all_tender_statuses):
                all_data.append({
                    "App ID": item["App ID"],
                    "Token": item["Token"],
                    "Tender Num": tender_num,
                    "Tender Start": tender_start,
                    "Tender end": tender_end,
                    "Tender Status": tender_status
                })


    logger.info(f"\n--- STAGE 1 COMPLETE: Collected {len(all_data)} total tenders ---")

    # --- STAGE 2: Scrape all the detail tabs for each collected tender ---
    logger.info("\n--- STAGE 2: Scraping detail tabs for each tender ---")
    if UPDATE_FLAG:
        logger.info("💡 Флаг --update активен: будут обновлены все найденные тендеры.")

    processed_count = 0
    skipped_count = 0
    for index, tender_info in enumerate(all_data):
        app_id = tender_info["App ID"]
        token = tender_info["Token"]
        tender_num = tender_info["Tender Num"]
        tender_start = tender_info["Tender Start"]
        tender_end = tender_info["Tender end"]
        tender_status = tender_info["Tender Status"]

        should_process = True
        skip_reason = ""

        if not UPDATE_FLAG:
            # Check if tender already exists in the DataFrame and if metadata has changed
            existing_tender = existing_tenders_df[existing_tenders_df['application_id'] == str(app_id)]

            if not existing_tender.empty:
                existing_tender = existing_tender.iloc[0]
                if (
                    existing_tender['tender_num'] == tender_num and
                    existing_tender['tender_start'] == tender_start and
                    existing_tender['tender_end'] == tender_end and
                    existing_tender['tender_status'] == tender_status
                ):
                    should_process = False
                    skip_reason = "уже существует и метаданные не изменились"
                else:
                    # Metadata changed, so we need to re-process and update
                    skip_reason = "существует, но метаданные изменились (обновление)"
            else:
                # New tender, not in existing_tenders_df
                skip_reason = "новый тендер"
        else:
            skip_reason = "флаг --update активен"

        if not should_process and not UPDATE_FLAG:
            logger.info(f"   -> Пропускаем тендер {tender_num} (App ID: {app_id}): {skip_reason}.")
            skipped_count += 1
            continue
        
        logger.info(f"   -> Обработка тендера {processed_count + skipped_count + 1}/{len(all_data)}: {tender_num} (App ID: {app_id}) - {skip_reason}")
        save_tab_pages(driver, app_id, token, (index + 1), all_links, tender_num, TARGET_CPV_CODE, tender_start, tender_end, tender_status)
        processed_count += 1

    logger.info("\n--- STAGE 2 COMPLETE ---")
    logger.info(f"📊 Обработано новых/обновлено тендеров: {processed_count}")
    logger.info(f"ℹ️ Пропущено существующих тендеров: {skipped_count}")

    # --- Final Save ---
    # Сохраняем ссылки (LINKS_CSV_FILE) - накопительная логика
    if all_links:
        current_links_df = pd.DataFrame(all_links)
        if os.path.exists(LINKS_CSV_FILE):
            existing_links_df = pd.read_csv(LINKS_CSV_FILE, dtype={'tender_code': str, 'tender': str, 'url': str, 'tab_name': str, 'text': str})
            
            # Объединяем, удаляем дубликаты, сохраняя только те, которых не было ранее
            # Создаем уникальный идентификатор для сравнения
            existing_links_df['link_id'] = existing_links_df['url'] + existing_links_df['tender'] + existing_links_df['tab_name'] + existing_links_df['text']
            current_links_df['link_id'] = current_links_df['url'] + current_links_df['tender'] + current_links_df['tab_name'] + current_links_df['text']

            new_links_to_save_df = current_links_df[~current_links_df['link_id'].isin(existing_links_df['link_id'])]
            new_links_to_save_df = new_links_to_save_df.drop(columns=['link_id'])
            
            if not new_links_to_save_df.empty:
                new_links_to_save_df.to_csv(LINKS_CSV_FILE, mode='a', header=False, index=False, encoding="utf-8")
                logger.info(f"💾 Добавлено {len(new_links_to_save_df)} новых уникальных ссылок в {LINKS_CSV_FILE}")
            else:
                logger.info(f"💾 Новых уникальных ссылок не найдено для добавления в {LINKS_CSV_FILE}")
        else:
            # Если файла не было, сохраняем все
            current_links_df.to_csv(LINKS_CSV_FILE, index=False, encoding="utf-8")
            logger.info(f"💾 Создан {LINKS_CSV_FILE} с {len(current_links_df)} ссылками.")
    else:
        logger.info(f"💾 Нет ссылок для сохранения в {LINKS_CSV_FILE}")

    # Сохраняем данные тендеров (CSV_FILE) - обновление/добавление
    if all_data:
        new_tender_data_df = pd.DataFrame(all_data)
        new_tender_data_df = new_tender_data_df.drop(columns=['Tender Status'])
        new_tender_data_df = new_tender_data_df.rename(columns={"App ID": "application_id", "Token": "token", "Tender Num": "tender_num", "Tender Start": "tender_start", "Tender end": "tender_end"})
        new_tender_data_df = new_tender_data_df[["tender_num", "application_id", "token", "tender_start", "tender_end"]]
        
        if not existing_tenders_df.empty:
            # Обновляем существующие записи и добавляем новые
            # Сначала удаляем старые версии обновленных тендеров
            updated_tenders_df = pd.concat([existing_tenders_df, new_tender_data_df])
            updated_tenders_df = updated_tenders_df.drop_duplicates(subset=['application_id'], keep='last')
            updated_tenders_df.to_csv(CSV_FILE, index=False, encoding="utf-8")
            logger.info(f"💾 Обновлено/добавлено {len(new_tender_data_df)} записей тендеров в {CSV_FILE}. Всего записей: {len(updated_tenders_df)}")
        else:
            # Если файла не было, сохраняем все новые
            new_tender_data_df.to_csv(CSV_FILE, index=False, encoding="utf-8")
            logger.info(f"💾 Создан {CSV_FILE} с {len(new_tender_data_df)} записями тендеров.")
    else:
        # Если all_data пуст, но файл уже существовал, мы его не трогаем.
        # Если файла не было, создаем пустой или оставляем как есть.
        if not os.path.exists(CSV_FILE):
             pd.DataFrame(columns=["tender_num", "application_id", "token", "tender_start", "tender_end"]).to_csv(CSV_FILE, index=False, encoding="utf-8")
             logger.info(f"💾 Создан пустой {CSV_FILE}.")
        else:
             logger.info(f"💾 Нет новых данных тендеров для сохранения в {CSV_FILE}")

    logger.info(f"✅ Total tenders in {CSV_FILE}: {len(updated_tenders_df) if 'updated_tenders_df' in locals() else len(new_tender_data_df) if 'new_tender_data_df' in locals() else 0}")


except Exception as e:
    logger.error(f"❌ Error: {e}")

finally:
    driver.quit()

logger.info(f"✅ {TARGET_CPV_CODE} OK - Clary_0")

