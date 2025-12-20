import unittest
import os
import sqlite3
from contract_parser import ContractParser


class TestContractParser(unittest.TestCase):
    def setUp(self):
        """Настройка тестов"""
        self.test_db = 'test_contracts.db'
        self.parser = ContractParser(self.test_db)
        
        # Создаем тестовый HTML файл
        self.test_html = '''<html><body>
        <div id="agency_docs">
            <div class="ui-state-highlight ui-corner-all">
                <div><span class="agrfg40">მიმდინარე ხელშეკრულება - საგარანტიო პერიოდი</span><br>
                <span class="date">ნინო ლომჯარია :: 28.11.2024</span></div>
                <table><tr><td>
                <a onclick="ShowProfile(12345)"><img src="profile.png"></a> <strong>შპს ტესტი</strong><br>
                ნომერი/თანხა: TEST123 / 100000.00 ლარი<br>
                ხელშეკრულება ძალაშია: 01.01.2024 - 31.12.2024<br>
                <span>ხელშეკრულების თარიღი: 01.01.2024</span>
                </td></tr></table>
            </div>
        </div></body></html>'''
        
        with open('test_file.html', 'w', encoding='utf-8') as f:
            f.write(self.test_html)
    
    def test_extract_tender_info(self):
        """Тест извлечения информации из имени файла"""
        filename = "pg_NAT240000167_553925_agr_docs.html"
        code, tid = self.parser.extract_tender_info_from_filename(filename)
        self.assertEqual(code, "NAT240000167")
        self.assertEqual(tid, 553925)
    
    def test_parse_currency(self):
        """Тест парсинга валюты"""
        amount, currency = self.parser.parse_currency_amount("100000.00 ლარი")
        self.assertEqual(amount, 100000.0)
        self.assertEqual(currency, "GEL")
    
    def tearDown(self):
        """Очистка после тестов"""
        self.parser.close()
        if os.path.exists(self.test_db):
            os.remove(self.test_db)
        if os.path.exists('test_file.html'):
            os.remove('test_file.html')


if __name__ == '__main__':
    unittest.main()
