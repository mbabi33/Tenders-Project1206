import os
import re
import sqlite3
import argparse
import requests
from tqdm import tqdm
from urllib.parse import urljoin
import csv
import config

def download_file(file_url, local_path, timeout=30):
    """
    Скачивает файл по URL и сохраняет его локально.
    """
    try:
        # Создаем директорию, если ее нет
        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        with requests.get(file_url, stream=True, timeout=timeout) as r:
            r.raise_for_status()  # Вызывает исключение для плохих статусов (4xx, 5xx)
            with open(local_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        return True
    except requests.exceptions.RequestException as e:
        print(f"  [ERROR] Ошибка при скачивании {file_url}: {e}")
        return False
    except IOError as e:
        print(f"  [ERROR] Ошибка при сохранении файла {local_path}: {e}")
        return False

def export_manifest_to_csv(conn, project_root):
    """
    Экспортирует метаданные скачанных файлов в CSV-файл.
    """
    manifest_path = os.path.join(project_root, 'manifest_app_doc.csv')
    cur = conn.cursor()
    
    # Выбираем все скачанные файлы
    cur.execute("SELECT id, file_name, local_path FROM tnd_app_doc_files WHERE download_status = 'downloaded' ORDER BY id")
    rows = cur.fetchall()
    
    if not rows:
        print("  [INFO] Нет скачанных файлов для экспорта в манифест.")
        return

    with open(manifest_path, 'w', newline='', encoding='utf-8') as csvfile:
        csv_writer = csv.writer(csvfile)
        # Записываем заголовок
        csv_writer.writerow(['id', 'file_name', 'local_path'])
        # Записываем данные
        for row in rows:
            csv_writer.writerow(row)
            
    print(f"  [INFO] Манифест успешно экспортирован в {manifest_path}")

def main():
    """
    Главная функция скрипта для скачивания файлов.
    """
    parser = argparse.ArgumentParser(description='Скачиватель файлов документации тендеров')
    parser.add_argument('-c', '--cpv', type=str, required=True, help='CPV код (обязательный)')
    parser.add_argument('-root', '--root-dir', type=str, help='Корневая директория (переопределяет config.py)')
    parser.add_argument('-batch_size', type=int, help='Количество тендеров для обработки за один запуск (по умолчанию все)')
    parser.add_argument('--date-from', type=str, help='Начальная дата для фильтрации (ГГГГ-ММ-ДД)')
    parser.add_argument('--date-to', type=str, help='Конечная дата для фильтрации (ГГГГ-ММ-ДД)')
    args = parser.parse_args()
    
    cpv_code = args.cpv
    batch_size = args.batch_size

    # Получаем пути через централизованный config.py
    PATHS = config.get_project_paths(cpv_code, db_filename='docs.db')
    db_path = PATHS['DB_NAME']
    download_dir = PATHS['DOC_FILES_DIR']
    project_root = PATHS['BASE_DIR'] # Используем BASE_DIR для размещения манифеста

    print(f"🚀 Запуск скачивателя файлов...")
    print(f"   CPV код: {cpv_code}")
    print(f"   База данных: {db_path}")
    print(f"   Директория для скачивания: {download_dir}")

    if not os.path.exists(db_path):
        print(f"[ERROR] Файл базы данных не найден: {db_path}")
        return
    
    # os.makedirs(download_dir, exist_ok=True) # Эта проверка уже есть внутри get_project_paths

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # --- Шаг 1: Получаем список tender_db_id для обработки ---
    tender_ids_to_process = []
    tender_select_sql = "SELECT DISTINCT tender_db_id FROM tnd_app_doc_files WHERE download_status = 'pending'"
    tender_params = []

    # Добавляем фильтр по дате, если он указан
    if args.date_from and args.date_to:
        tender_select_sql += " AND upload_date BETWEEN ? AND ?"
        tender_params.extend([args.date_from, args.date_to])
        print(f"   [INFO] Активен фильтр по дате: с {args.date_from} по {args.date_to}")

    tender_select_sql += " ORDER BY tender_db_id"

    if batch_size is not None:
        tender_select_sql += " LIMIT ?"
        tender_params.append(batch_size)
    
    cur.execute(tender_select_sql, tender_params)
    tender_ids_to_process = [row[0] for row in cur.fetchall()]

    if not tender_ids_to_process:
        print("  [INFO] Не найдено тендеров с ожидающими файлами для обработки.")
        conn.close()
        return

    print(f"  [INFO] Будет обработано {len(tender_ids_to_process)} тендеров.")

    # --- Шаг 2: Формируем SQL-запрос для выборки файлов для этих тендеров ---
    file_select_sql = "SELECT id, file_url, tender_code, tender_db_id, section_id, file_name FROM tnd_app_doc_files WHERE download_status = 'pending'"
    file_params = []

    # Добавляем фильтр по выбранным tender_db_id
    placeholders = ','.join('?' * len(tender_ids_to_process))
    file_select_sql += f" AND tender_db_id IN ({placeholders})"
    file_params.extend(tender_ids_to_process)
    
    file_select_sql += " ORDER BY tender_db_id, id" # Сортируем для предсказуемости

    cur.execute(file_select_sql, file_params)
    files_to_download = cur.fetchall()

    if not files_to_download:
        print("  [INFO] Не найдено файлов для скачивания по заданным критериям.")
        conn.close()
        return

    print(f"📁 Найдено {len(files_to_download)} файлов для скачивания.")

    downloaded_count = 0
    failed_count = 0

    # --- Цикл скачивания ---
    for file_record in tqdm(files_to_download, desc="Скачивание файлов", unit="file"):
        file_id, file_url, tender_code, tender_db_id, section_id, file_name = file_record
        
        # Формируем локальный путь: {id}{расширение}
        _, file_extension = os.path.splitext(file_name)
        local_filename = f"{file_id}_{tender_code}_{tender_db_id}{file_extension}"
        local_path = os.path.join(download_dir, local_filename)

        # Скачиваем файл
        if download_file(file_url, local_path):
            downloaded_count += 1
            # Обновляем статус в БД
            cur.execute("UPDATE tnd_app_doc_files SET download_status = 'downloaded', local_path = ? WHERE id = ?",
                        (local_path, file_id))
        else:
            failed_count += 1
            # Обновляем статус в БД
            cur.execute("UPDATE tnd_app_doc_files SET download_status = 'failed' WHERE id = ?",
                        (file_id,))
        conn.commit() # Коммитим каждую операцию, чтобы прогресс сохранялся

    conn.close()
    print("\n✅ Скачивание завершено!")
    print(f"📊 Скачано успешно: {downloaded_count}")
    print(f"❌ Ошибок при скачивании: {failed_count}")

    # --- Сохранение ID обработанных тендеров для следующих скриптов ---
    if tender_ids_to_process:
        batch_file_path = os.path.join(project_root, '.last_batch_ids.txt')
        print(f"  [INFO] Сохранение {len(tender_ids_to_process)} ID обработанных тендеров в {batch_file_path}")
        with open(batch_file_path, 'w') as f:
            for tender_id in tender_ids_to_process:
                f.write(f"{tender_id}\n")

    # --- Экспорт манифеста ---
    # Открываем новое соединение для экспорта, так как предыдущее уже закрыто
    export_manifest_to_csv(sqlite3.connect(db_path), project_root)

if __name__ == '__main__':
    main()