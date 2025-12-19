#!/usr/bin/env python3
"""
Конфигурационный модуль для парсера тендеров
Автоматическое создание структуры папок на основе CPV кода
"""

import os
import argparse

# Базовый ROOT_DIR по умолчанию. Может быть переопределен аргументом -root
DEFAULT_ROOT_DIR = '/home/nerow/BIKI/ML_Projects/N17-2025ABC'

# Другие глобальные настройки
GECKODRIVER_PATH = "/bin/geckodriver"
FIREFOX_PATH = "/bin/firefox"
BASE_URL = 'https://tenders.procurement.gov.ge/public/'

def get_root_dir():
    """
    Определяет ROOT_DIR:
    - если передан аргумент -root, использует его
    - иначе использует DEFAULT_ROOT_DIR
    """
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('-root', '--root-dir', type=str, help='Корневая директория для проектов')
    args, _ = parser.parse_known_args()
    
    if args.root_dir:
        return os.path.abspath(args.root_dir)
    else:
        return DEFAULT_ROOT_DIR

def get_project_paths(cpv_code, html_dir_name='html_tabs', db_filename='tenders.db'):
    """
    Создает и возвращает все пути для конкретного CPV кода.

    Args:
        cpv_code (str): CPV код (например: "71200000")
        html_dir_name (str): Имя подпапки для HTML файлов (например, 'app_main' или 'html_tabs')
        db_filename (str): Имя файла базы данных (например, 'tenders.db', 'docs.db')

    Returns:
        dict: Словарь с путями

    Raises:
        SystemExit: Если cpv_code не указан
    """
    if not cpv_code:
        print("❌ CPV код обязателен!")
        raise SystemExit(1)
    
    # Определяем ROOT_DIR
    ROOT_DIR = get_root_dir()
    
    # Создаем базовую структуру путей
    BASE_DIR = os.path.join(ROOT_DIR, f"T_{cpv_code}")
    
    paths = {
        'ROOT_DIR': ROOT_DIR,
        'BASE_DIR': BASE_DIR,
        'DB_NAME': os.path.join(BASE_DIR, db_filename),
        'CSV_FILE': os.path.join(BASE_DIR, 'data_urls.csv'),
        'LINKS_CSV_FILE': os.path.join(BASE_DIR, 'data_links.csv'),
        'HTML_DIR': os.path.join(BASE_DIR, html_dir_name),
        'DOC_FILES_DIR': os.path.join(BASE_DIR, 'DOWN_doc_files'),
        'AGR_FILES_DIR': os.path.join(BASE_DIR, 'DOWN_agr_files'),
        'AGENCY_FILES_DIR': os.path.join(BASE_DIR, 'DOWN_agency_files'),
        'RESULTS_DIR': os.path.join(BASE_DIR, 'Results'),
        'CPV_CODE': cpv_code
    }
    
    # Создаем все необходимые папки
    folders_to_create = [
        paths['BASE_DIR'],
        paths['HTML_DIR'], 
        paths['DOC_FILES_DIR'],
        paths['AGR_FILES_DIR'],
        paths['AGENCY_FILES_DIR'],
        paths['RESULTS_DIR']
    ]
    
    for folder in folders_to_create:
        os.makedirs(folder, exist_ok=True)
    
    return paths

# Пример использования (для тестирования)
if __name__ == '__main__':
    print("Тестирование с 'html_tabs' и БД по умолчанию:")
    try:
        paths = get_project_paths("71200000", html_dir_name='html_tabs')
        print("✅ Пути успешно созданы:")
        for key, value in paths.items():
            print(f"   {key}: {value}")
    except SystemExit:
        print("❌ Тест провален: CPV код не указан")

    print("\nТестирование с 'app_main' и кастомной БД 'my_docs.db':")
    try:
        paths = get_project_paths("71200000", html_dir_name='app_main', db_filename='my_docs.db')
        print("✅ Пути успешно созданы:")
        for key, value in paths.items():
            print(f"   {key}: {value}")
    except SystemExit:
        print("❌ Тест провален: CPV код не указан")
