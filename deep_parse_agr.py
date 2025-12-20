import sqlite3
import re
from datetime import datetime
from bs4 import BeautifulSoup
import os
from typing import Dict, List, Optional, Tuple
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ContractParser:
    """Класс для парсинга HTML-файлов контрактов"""
    
    # Статусы контрактов и их перевод на английский
    STATUS_TRANSLATIONS = {
        "მიმდინარე ხელშეკრულება - საგარანტიო პერიოდი": "Current contract - warranty period",
        "შეუსრულებელი ხელშეკრულება": "Unfulfilled contract",
        # Можно добавить другие статусы при необходимости
    }
    
    def __init__(self, db_path: str = 'contracts.db'):
        """Инициализация парсера с подключением к базе данных"""
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.create_tables()
    
    def create_tables(self):
        """Создание таблиц в базе данных"""
        cursor = self.conn.cursor()
        
        # Таблица тендеров
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS tenders (
            tender_id INTEGER PRIMARY KEY,
            tender_code TEXT UNIQUE NOT NULL,
            file_name TEXT,
            parsed_date DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # Таблица контрактов
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS contracts (
            contract_id INTEGER PRIMARY KEY AUTOINCREMENT,
            tender_id INTEGER NOT NULL,
            contract_status_ge TEXT,
            contract_status_en TEXT,
            responsible_person TEXT,
            contract_date_updated DATE,
            supplier_name TEXT,
            supplier_id INTEGER,
            contract_number TEXT,
            contract_amount DECIMAL(12, 2),
            contract_amount_currency TEXT DEFAULT 'GEL',
            contract_effective_from DATE,
            contract_effective_to DATE,
            contract_sign_date DATE,
            FOREIGN KEY (tender_id) REFERENCES tenders(tender_id)
        )
        ''')
        
        # Таблица изменений контрактов
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS contract_amendments (
            amendment_id INTEGER PRIMARY KEY AUTOINCREMENT,
            contract_id INTEGER NOT NULL,
            amendment_number INTEGER,
            amendment_date DATE,
            amendment_amount DECIMAL(12, 2),
            pdf_link TEXT,
            counterparty TEXT,
            FOREIGN KEY (contract_id) REFERENCES contracts(contract_id)
        )
        ''')
        
        # Таблица документов
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS documents (
            document_id INTEGER PRIMARY KEY AUTOINCREMENT,
            contract_id INTEGER NOT NULL,
            doc_index INTEGER,
            doc_icon TEXT,
            doc_name TEXT,
            doc_link TEXT,
            doc_upload_date DATETIME,
            doc_upload_author TEXT,
            FOREIGN KEY (contract_id) REFERENCES contracts(contract_id)
        )
        ''')
        
        # Таблица платежей
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS payments (
            payment_id INTEGER PRIMARY KEY AUTOINCREMENT,
            contract_id INTEGER NOT NULL,
            payment_amount DECIMAL(12, 2),
            payment_is_advance BOOLEAN DEFAULT 0,
            funding_source TEXT,
            payment_year INTEGER,
            payment_quarter INTEGER,
            payment_date DATE,
            payment_record_date DATE,
            FOREIGN KEY (contract_id) REFERENCES contracts(contract_id)
        )
        ''')
        
        # Таблица сводки по платежам
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS payment_summary (
            summary_id INTEGER PRIMARY KEY AUTOINCREMENT,
            contract_id INTEGER UNIQUE NOT NULL,
            total_contract_amount DECIMAL(12, 2),
            total_paid_amount DECIMAL(12, 2),
            paid_percentage DECIMAL(5, 2),
            FOREIGN KEY (contract_id) REFERENCES contracts(contract_id)
        )
        ''')
        
        self.conn.commit()
        logger.info("Таблицы созданы успешно")
    
    def extract_tender_info_from_filename(self, filename: str) -> Tuple[Optional[str], Optional[int]]:
        """Извлечение tender_code и tender_id из имени файла"""
        # Формат: pg_NAT240000163_553417_agr_docs.html
        match = re.search(r'pg_([A-Z0-9]+)_(\d+)_', filename)
        if match:
            tender_code = match.group(1)  # NAT240000163
            tender_id = int(match.group(2))  # 553417
            return tender_code, tender_id
        return None, None
    
    def parse_currency_amount(self, amount_str: str) -> Tuple[float, str]:
        """Парсинг суммы с валютой"""
        # Пример: "1327402.54 ლარი" или "50`999.72 ლარი"
        amount_str = amount_str.replace('`', '').replace(',', '.')
        match = re.search(r'([\d\.]+)\s*(\D+)', amount_str)
        if match:
            amount = float(match.group(1))
            currency = match.group(2).strip()
            # Стандартизируем валюту
            if 'ლარ' in currency:
                currency = 'GEL'
            return amount, currency
        return 0.0, 'GEL'
    
    def parse_date(self, date_str: str) -> Optional[str]:
        """Парсинг даты в стандартный формат"""
        try:
            # Убираем время если есть
            date_str = date_str.split()[0]
            dt = datetime.strptime(date_str, '%d.%m.%Y')
            return dt.strftime('%Y-%m-%d')
        except Exception as e:
            logger.warning(f"Не удалось распарсить дату: {date_str} - {e}")
            return None
    
    def parse_datetime(self, datetime_str: str) -> Optional[str]:
        """Парсинг даты и времени"""
        try:
            dt = datetime.strptime(datetime_str, '%d.%m.%Y %H:%M')
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except Exception as e:
            logger.warning(f"Не удалось распарсить дату-время: {datetime_str} - {e}")
            return self.parse_date(datetime_str.split()[0]) if datetime_str else None
    
    def extract_supplier_id(self, onclick_str: str) -> Optional[int]:
        """Извлечение ID поставщика из onclick атрибута"""
        match = re.search(r'ShowProfile\((\d+)\)', onclick_str)
        return int(match.group(1)) if match else None
    
    def parse_contract_info(self, soup: BeautifulSoup, tender_id: int) -> Dict:
        """Парсинг основной информации о контракте"""
        contract_div = soup.find('div', class_='ui-state-highlight ui-corner-all')
        
        if not contract_div:
            logger.error("Не найден блок контракта")
            return {}
        
        # Извлекаем статус и ответственного
        status_elem = contract_div.find('span', class_='agrfg40') or contract_div.find('span', class_='agrfg30')
        status_ge = status_elem.text.strip() if status_elem else ''
        status_en = self.STATUS_TRANSLATIONS.get(status_ge, status_ge)
        
        date_elem = contract_div.find('span', class_='date')
        contract_date_updated = date_elem.text.split('::')[0].strip() if date_elem else ''
        
        # Извлекаем информацию о поставщике
        supplier_link = contract_div.find('a', onclick=lambda x: x and 'ShowProfile' in x)
        supplier_id = self.extract_supplier_id(supplier_link['onclick']) if supplier_link else None
        supplier_name = contract_div.find('strong').text.strip() if contract_div.find('strong') else ''
        
        # Извлекаем номер и сумму контракта
        contract_info_text = contract_div.get_text()
        number_match = re.search(r'ნომერი/თანხა:\s*(.+?)/\s*(.+?)\s*<', str(contract_div))
        
        contract_number = ''
        contract_amount = 0.0
        
        if number_match:
            contract_number = number_match.group(1).strip()
            amount_str = number_match.group(2)
            contract_amount, currency = self.parse_currency_amount(amount_str)
        
        # Извлекаем даты
        dates = {}
        date_patterns = {
            'ხელშეკრულება ძალაშია': 'contract_dates',
            'ხელშეკრულების თარიღი': 'sign_date'
        }
        
        for line in contract_info_text.split('\n'):
            for pattern, key in date_patterns.items():
                if pattern in line:
                    date_match = re.search(r'(\d{2}\.\d{2}\.\d{4})', line)
                    if date_match:
                        dates[key] = self.parse_date(date_match.group(1))
        
        # Разбираем диапазон дат
        effective_from = effective_to = None
        if 'contract_dates' in dates:
            date_range = dates['contract_dates']
            if ' - ' in date_range:
                from_str, to_str = date_range.split(' - ')
                effective_from = self.parse_date(from_str.strip())
                effective_to = self.parse_date(to_str.strip())
        
        return {
            'tender_id': tender_id,
            'contract_status_ge': status_ge,
            'contract_status_en': status_en,
            'responsible_person': date_elem.text.split('::')[1].strip() if date_elem and '::' in date_elem.text else '',
            'contract_date_updated': self.parse_date(contract_date_updated),
            'supplier_name': supplier_name,
            'supplier_id': supplier_id,
            'contract_number': contract_number,
            'contract_amount': contract_amount,
            'contract_effective_from': effective_from,
            'contract_effective_to': effective_to,
            'contract_sign_date': self.parse_date(dates.get('sign_date', ''))
        }
    
    def parse_amendments(self, soup: BeautifulSoup) -> List[Dict]:
        """Парсинг изменений контракта"""
        amendments = []
        
        # Ищем блок с изменениями
        amendments_header = soup.find('div', string=lambda x: x and 'ხელშეკრულების ცვლილება' in x)
        if not amendments_header:
            return amendments
        
        amendments_div = amendments_header.find_parent('div', class_='ui-state-highlight')
        if not amendments_div:
            return amendments
        
        # Парсим каждое изменение
        for row in amendments_div.find_all('tr'):
            amendment_text = row.get_text()
            
            # Номер изменения
            amendment_match = re.search(r'ცვლილება\s*(\d+)', amendment_text)
            amendment_number = int(amendment_match.group(1)) if amendment_match else len(amendments) + 1
            
            # Дата изменения
            date_match = re.search(r'ხელშეკრულების ცვლილების თარიღი:\s*(\d{2}\.\d{2}\.\d{4})', amendment_text)
            amendment_date = self.parse_date(date_match.group(1)) if date_match else None
            
            # Сумма
            amount_match = re.search(r'ნომერი/თანხა:.*?/\s*(.+?)\s*<', str(row))
            amendment_amount = 0.0
            if amount_match:
                amendment_amount, _ = self.parse_currency_amount(amount_match.group(1))
            
            # Ссылка на PDF
            pdf_link = ''
            pdf_tag = row.find('a', href=lambda x: x and 'contract.php?go=' in x)
            if pdf_tag:
                pdf_link = pdf_tag['href']
            
            # Контрагент
            counterparty = ''
            strong_tag = row.find('strong')
            if strong_tag:
                counterparty = strong_tag.text.strip()
            
            amendments.append({
                'amendment_number': amendment_number,
                'amendment_date': amendment_date,
                'amendment_amount': amendment_amount,
                'pdf_link': pdf_link,
                'counterparty': counterparty
            })
        
        return amendments
    
    def parse_documents(self, soup: BeautifulSoup) -> List[Dict]:
        """Парсинг списка документов"""
        documents = []
        
        # Ищем таблицу с документами
        docs_table = soup.find('table', id='last_docs')
        if not docs_table:
            return documents
        
        # Парсим каждую строку таблицы
        for row in docs_table.find_all('tr')[1:]:  # Пропускаем заголовок
            cols = row.find_all('td')
            if len(cols) >= 4:
                try:
                    doc_index = int(cols[0].text.strip('.'))
                    doc_icon = cols[1].find('img')['src'] if cols[1].find('img') else ''
                    
                    doc_link_tag = cols[2].find('a')
                    doc_name = doc_link_tag.text.strip() if doc_link_tag else cols[2].text.strip()
                    doc_link = doc_link_tag['href'] if doc_link_tag else ''
                    
                    date_author = cols[3].text.strip()
                    if ' :: ' in date_author:
                        date_part, author_part = date_author.split(' :: ', 1)
                        doc_upload_date = self.parse_datetime(date_part.strip())
                        doc_upload_author = author_part.strip()
                    else:
                        doc_upload_date = self.parse_datetime(date_author)
                        doc_upload_author = ''
                    
                    documents.append({
                        'doc_index': doc_index,
                        'doc_icon': doc_icon,
                        'doc_name': doc_name,
                        'doc_link': doc_link,
                        'doc_upload_date': doc_upload_date,
                        'doc_upload_author': doc_upload_author
                    })
                except Exception as e:
                    logger.warning(f"Ошибка парсинга документа: {e}")
        
        return documents
    
    def parse_payments(self, soup: BeautifulSoup) -> Tuple[List[Dict], Dict]:
        """Парсинг платежей и сводной информации"""
        payments = []
        summary = {}
        
        # Ищем блок с платежами
        payments_div = None
        for div in soup.find_all('div', class_='ui-state-highlight'):
            if div.find('p', string=lambda x: x and 'ფაქტობრივი გადახდები' in x):
                payments_div = div
                break
        
        if not payments_div:
            return payments, summary
        
        # Парсим сводную информацию
        summary_text = payments_div.get_text()
        
        # Общая сумма контракта
        total_match = re.search(r'ხელშეკრულების თანხა:\s*([\d`\.]+)\s*ლარი', summary_text)
        if total_match:
            summary['total_contract_amount'], _ = self.parse_currency_amount(total_match.group(1))
        
        # Выплаченная сумма и процент
        paid_match = re.search(r'გადახდილი თანხა:\s*([\d`\.]+)\s*ლარი\s*\((\d+)%\)', summary_text)
        if paid_match:
            summary['total_paid_amount'], _ = self.parse_currency_amount(paid_match.group(1))
            summary['paid_percentage'] = float(paid_match.group(2))
        
        # Парсим таблицу платежей
        payments_table = payments_div.find('table', class_='ktable')
        if payments_table:
            headers = [th.text.strip() for th in payments_table.find('thead').find_all('td')]
            
            for row in payments_table.find_all('tr')[1:]:  # Пропускаем заголовок
                cols = row.find_all('td')
                if len(cols) >= 5:
                    try:
                        # Сумма платежа
                        amount_text = cols[0].get_text()
                        amount, _ = self.parse_currency_amount(amount_text)
                        
                        # Проверяем, является ли платеж авансом
                        is_advance = 'ავანსი' in amount_text or 'аванс' in amount_text.lower()
                        
                        # Источник финансирования
                        funding_source = ''
                        source_tag = cols[0].find('span', class_='color-2')
                        if source_tag:
                            funding_source = source_tag.text.strip()
                        
                        # Год и квартал
                        year = int(cols[1].text.strip()) if cols[1].text.strip().isdigit() else None
                        quarter = int(cols[2].text.strip()) if cols[2].text.strip().isdigit() else None
                        
                        # Даты
                        payment_date = self.parse_date(cols[3].text.strip())
                        record_date_text = cols[4].find('br').previous_sibling.strip() if cols[4].find('br') else cols[4].text.strip()
                        payment_record_date = self.parse_date(record_date_text)
                        
                        payments.append({
                            'payment_amount': amount,
                            'payment_is_advance': is_advance,
                            'funding_source': funding_source,
                            'payment_year': year,
                            'payment_quarter': quarter,
                            'payment_date': payment_date,
                            'payment_record_date': payment_record_date
                        })
                    except Exception as e:
                        logger.warning(f"Ошибка парсинга платежа: {e}")
        
        return payments, summary
    
    def save_tender(self, tender_code: str, tender_id: int, filename: str) -> int:
        """Сохранение информации о тендере"""
        cursor = self.conn.cursor()
        
        # Проверяем, существует ли уже тендер
        cursor.execute("SELECT tender_id FROM tenders WHERE tender_id = ?", (tender_id,))
        existing = cursor.fetchone()
        
        if existing:
            logger.info(f"Тендер {tender_code} уже существует в базе")
            return tender_id
        
        cursor.execute('''
        INSERT INTO tenders (tender_id, tender_code, file_name)
        VALUES (?, ?, ?)
        ''', (tender_id, tender_code, filename))
        
        self.conn.commit()
        logger.info(f"Тендер {tender_code} сохранен")
        return tender_id
    
    def save_contract(self, contract_data: Dict) -> int:
        """Сохранение контракта и возврат его ID"""
        cursor = self.conn.cursor()
        
        cursor.execute('''
        INSERT INTO contracts (
            tender_id, contract_status_ge, contract_status_en, 
            responsible_person, contract_date_updated, supplier_name,
            supplier_id, contract_number, contract_amount, 
            contract_effective_from, contract_effective_to, contract_sign_date
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            contract_data['tender_id'],
            contract_data['contract_status_ge'],
            contract_data['contract_status_en'],
            contract_data['responsible_person'],
            contract_data['contract_date_updated'],
            contract_data['supplier_name'],
            contract_data['supplier_id'],
            contract_data['contract_number'],
            contract_data['contract_amount'],
            contract_data['contract_effective_from'],
            contract_data['contract_effective_to'],
            contract_data['contract_sign_date']
        ))
        
        contract_id = cursor.lastrowid
        self.conn.commit()
        logger.info(f"Контракт #{contract_id} сохранен")
        return contract_id
    
    def save_amendments(self, contract_id: int, amendments: List[Dict]):
        """Сохранение изменений контракта"""
        if not amendments:
            return
        
        cursor = self.conn.cursor()
        
        for amendment in amendments:
            cursor.execute('''
            INSERT INTO contract_amendments (
                contract_id, amendment_number, amendment_date,
                amendment_amount, pdf_link, counterparty
            ) VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                contract_id,
                amendment['amendment_number'],
                amendment['amendment_date'],
                amendment['amendment_amount'],
                amendment['pdf_link'],
                amendment['counterparty']
            ))
        
        self.conn.commit()
        logger.info(f"Сохранено {len(amendments)} изменений для контракта #{contract_id}")
    
    def save_documents(self, contract_id: int, documents: List[Dict]):
        """Сохранение документов"""
        if not documents:
            return
        
        cursor = self.conn.cursor()
        
        for doc in documents:
            cursor.execute('''
            INSERT INTO documents (
                contract_id, doc_index, doc_icon, doc_name,
                doc_link, doc_upload_date, doc_upload_author
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                contract_id,
                doc['doc_index'],
                doc['doc_icon'],
                doc['doc_name'],
                doc['doc_link'],
                doc['doc_upload_date'],
                doc['doc_upload_author']
            ))
        
        self.conn.commit()
        logger.info(f"Сохранено {len(documents)} документов для контракта #{contract_id}")
    
    def save_payments(self, contract_id: int, payments: List[Dict]):
        """Сохранение платежей"""
        if not payments:
            return
        
        cursor = self.conn.cursor()
        
        for payment in payments:
            cursor.execute('''
            INSERT INTO payments (
                contract_id, payment_amount, payment_is_advance,
                funding_source, payment_year, payment_quarter,
                payment_date, payment_record_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                contract_id,
                payment['payment_amount'],
                1 if payment['payment_is_advance'] else 0,
                payment['funding_source'],
                payment['payment_year'],
                payment['payment_quarter'],
                payment['payment_date'],
                payment['payment_record_date']
            ))
        
        self.conn.commit()
        logger.info(f"Сохранено {len(payments)} платежей для контракта #{contract_id}")
    
    def save_payment_summary(self, contract_id: int, summary: Dict):
        """Сохранение сводки по платежам"""
        if not summary:
            return
        
        cursor = self.conn.cursor()
        
        cursor.execute('''
        INSERT INTO payment_summary (
            contract_id, total_contract_amount,
            total_paid_amount, paid_percentage
        ) VALUES (?, ?, ?, ?)
        ''', (
            contract_id,
            summary.get('total_contract_amount', 0),
            summary.get('total_paid_amount', 0),
            summary.get('paid_percentage', 0)
        ))
        
        self.conn.commit()
        logger.info(f"Сводка по платежам сохранена для контракта #{contract_id}")
    
    def parse_file(self, filepath: str) -> bool:
        """Основной метод парсинга файла"""
        try:
            filename = os.path.basename(filepath)
            logger.info(f"Начинаю парсинг файла: {filename}")
            
            # Извлекаем информацию из имени файла
            tender_code, tender_id = self.extract_tender_info_from_filename(filename)
            if not tender_code or not tender_id:
                logger.error(f"Не удалось извлечить tender_code и tender_id из {filename}")
                return False
            
            # Читаем HTML файл
            with open(filepath, 'r', encoding='utf-8') as f:
                html_content = f.read()
            
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Сохраняем тендер
            self.save_tender(tender_code, tender_id, filename)
            
            # Парсим и сохраняем данные
            contract_data = self.parse_contract_info(soup, tender_id)
            if not contract_data:
                logger.error(f"Не удалось распарсить контракт из {filename}")
                return False
            
            contract_id = self.save_contract(contract_data)
            
            amendments = self.parse_amendments(soup)
            self.save_amendments(contract_id, amendments)
            
            documents = self.parse_documents(soup)
            self.save_documents(contract_id, documents)
            
            payments, summary = self.parse_payments(soup)
            self.save_payments(contract_id, payments)
            self.save_payment_summary(contract_id, summary)
            
            logger.info(f"Файл {filename} успешно обработан")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при обработке файла {filepath}: {e}")
            return False
    
    def run_queries(self):
        """Примеры аналитических запросов"""
        cursor = self.conn.cursor()
        
        print("\n" + "="*50)
        print("АНАЛИТИЧЕСКИЕ ЗАПРОСЫ")
        print("="*50)
        
        # 1. Сводка по всем тендерам
        print("\n1. Сводка по всем тендерам:")
        cursor.execute('''
        SELECT 
            t.tender_code,
            c.contract_status_en,
            c.supplier_name,
            c.contract_amount,
            ps.paid_percentage,
            COUNT(DISTINCT d.document_id) as doc_count,
            COUNT(DISTINCT p.payment_id) as payment_count,
            COUNT(DISTINCT a.amendment_id) as amendment_count
        FROM tenders t
        JOIN contracts c ON t.tender_id = c.tender_id
        LEFT JOIN payment_summary ps ON c.contract_id = ps.contract_id
        LEFT JOIN documents d ON c.contract_id = d.contract_id
        LEFT JOIN payments p ON c.contract_id = p.contract_id
        LEFT JOIN contract_amendments a ON c.contract_id = a.contract_id
        GROUP BY t.tender_id
        ORDER BY c.contract_amount DESC
        ''')
        
        for row in cursor.fetchall():
            print(f"  {row['tender_code']}: {row['supplier_name']}")
            print(f"    Статус: {row['contract_status_en']}")
            print(f"    Сумма: {row['contract_amount']:,.2f} GEL")
            print(f"    Оплачено: {row['paid_percentage']}%")
            print(f"    Документы: {row['doc_count']}, Платежи: {row['payment_count']}, Изменения: {row['amendment_count']}")
        
        # 2. Крупнейшие платежи
        print("\n2. 5 крупнейших платежей:")
        cursor.execute('''
        SELECT 
            t.tender_code,
            c.supplier_name,
            p.payment_amount,
            p.payment_date,
            CASE WHEN p.payment_is_advance = 1 THEN 'Аванс' ELSE 'Оплата' END as payment_type
        FROM payments p
        JOIN contracts c ON p.contract_id = c.contract_id
        JOIN tenders t ON c.tender_id = t.tender_id
        ORDER BY p.payment_amount DESC
        LIMIT 5
        ''')
        
        for row in cursor.fetchall():
            print(f"  {row['supplier_name']} ({row['tender_code']})")
            print(f"    {row['payment_amount']:,.2f} GEL - {row['payment_type']} ({row['payment_date']})")
        
        # 3. Контракты по статусам
        print("\n3. Контракты по статусам:")
        cursor.execute('''
        SELECT 
            contract_status_en,
            COUNT(*) as contract_count,
            SUM(contract_amount) as total_amount,
            AVG(ps.paid_percentage) as avg_paid_percentage
        FROM contracts c
        LEFT JOIN payment_summary ps ON c.contract_id = ps.contract_id
        GROUP BY contract_status_en
        ''')
        
        for row in cursor.fetchall():
            print(f"  {row['contract_status_en']}: {row['contract_count']} контрактов")
            print(f"    Общая сумма: {row['total_amount']:,.2f} GEL")
            print(f"    Средний % оплаты: {row['avg_paid_percentage']:.1f}%")
        
        # 4. Поставщики с наибольшим числом контрактов
        print("\n4. Топ поставщиков:")
        cursor.execute('''
        SELECT 
            supplier_name,
            COUNT(*) as contract_count,
            SUM(contract_amount) as total_amount
        FROM contracts
        GROUP BY supplier_name
        ORDER BY total_amount DESC
        LIMIT 5
        ''')
        
        for row in cursor.fetchall():
            print(f"  {row['supplier_name']}: {row['contract_count']} контрактов")
            print(f"    Общая сумма: {row['total_amount']:,.2f} GEL")
    
    def close(self):
        """Закрытие соединения с базой данных"""
        if self.conn:
            self.conn.close()
            logger.info("Соединение с базой данных закрыто")


def main():
    """Основная функция"""
    # Создаем парсер
    parser = ContractParser('contracts.db')
    
    # Пример обработки файлов
    files_to_parse = [
        'pg_NAT240000167_553925_agr_docs.html',  # Первый файл
        'pg_NAT240000163_553417_agr_docs.html',  # Второй файл
    ]
    
    # Проверяем существование файлов
    existing_files = []
    for filepath in files_to_parse:
        if os.path.exists(filepath):
            existing_files.append(filepath)
        else:
            logger.warning(f"Файл не найден: {filepath}")
    
    if not existing_files:
        logger.error("Нет файлов для обработки")
        return
    
    # Парсим каждый файл
    for filepath in existing_files:
        success = parser.parse_file(filepath)
        if success:
            logger.info(f"✓ {filepath} - успешно")
        else:
            logger.error(f"✗ {filepath} - ошибка")
    
    # Запускаем аналитические запросы
    parser.run_queries()
    
    # Закрываем соединение
    parser.close()
    
    print("\n" + "="*50)
    print("ОБРАБОТКА ЗАВЕРШЕНА")
    print("="*50)
    print(f"База данных: contracts.db")
    print(f"Обработано файлов: {len(existing_files)}")
    print("="*50)


if __name__ == "__main__":
    main()
