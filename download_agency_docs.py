#!/usr/bin/env python3
import os
import re
import sqlite3
import argparse
import requests
from tqdm import tqdm
from urllib.parse import urljoin
import mimetypes
import csv
import config

def get_file_extension(response, url, title):
    """Определяет расширение файла из Content-Type, URL или заголовка."""
    content_type = response.headers.get('content-type', '').split(';')[0].strip()
    if content_type:
        guessed_ext = mimetypes.guess_extension(content_type)
        if guessed_ext:
            return guessed_ext

    # Пытаемся извлечь из URL
    _, url_ext = os.path.splitext(url.split('?')[0])
    if url_ext and len(url_ext) < 6:
        return url_ext

    # Пытаемся извлечь из оригинального названия
    _, title_ext = os.path.splitext(title)
    if title_ext and len(title_ext) < 6:
        return title_ext

    return '' # Если не удалось определить

def download_file(file_url, local_path, timeout=30):
    """
    Скачивает файл по URL, сохраняет его и возвращает объект ответа для анализа.
    """
    try:
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with requests.get(file_url, stream=True, timeout=timeout) as r:
            r.raise_for_status()
            with open(local_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
            return r, True
    except requests.exceptions.RequestException as e:
        print(f"  [ERROR] Ошибка при скачивании {file_url}: {e}")
        return None, False
    except IOError as e:
        print(f"  [ERROR] Ошибка при сохранении файла {local_path}: {e}")
        return None, False

def export_manifest_to_csv(conn, project_root):
    """
    Экспортирует метаданные скачанных файлов в CSV-файл.
    """
    manifest_path = os.path.join(project_root, 'manifest_agency_doc.csv')
    cur = conn.cursor()
    
    cur.execute("SELECT id, tender_id, local_path FROM tnd_agency_docs WHERE download_status = 'downloaded' ORDER BY id")
    rows = cur.fetchall()
    
    if not rows:
        return

    with open(manifest_path, 'w', newline='', encoding='utf-8') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(['id', 'tender_id', 'local_path'])
        csv_writer.writerows(rows)
            
    print(f"  [INFO] Манифест успешно экспортирован в {manifest_path}")

def main():
    parser = argparse.ArgumentParser(description='Скачиватель файлов документов агентства')
    parser.add_argument('-c', '--cpv', required=True, help='CPV код')
    parser.add_argument('-root', '--root_dir', help='Корневая директория (переопределяет config.py)')
    parser.add_argument('-batch_size', type=int, help='Количество тендеров для обработки, если не используется --use-last-batch')
    parser.add_argument('--use-last-batch', action='store_true', help='Использовать ID тендеров из последнего запуска app_docs_downloader')
    args = parser.parse_args()

    paths = config.get_project_paths(args.cpv, db_filename='agency.db')
    DB_PATH = paths['DB_NAME']
    DOWNLOAD_DIR = paths['AGENCY_FILES_DIR']
    PROJECT_ROOT = paths['BASE_DIR']

    print(f"🚀 Запуск скачивателя документов агентства...")
    print(f"   База данных: {DB_PATH}")
    print(f"   Директория для скачивания: {DOWNLOAD_DIR}")

    if not os.path.exists(DB_PATH):
        print(f"[ERROR] Файл базы данных не найден: {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # --- Шаг 1: Получаем список tender_db_id для обработки ---
    tender_ids_to_process = []
    if args.use_last_batch:
        batch_file_path = os.path.join(PROJECT_ROOT, '.last_batch_ids.txt')
        if os.path.exists(batch_file_path):
            print(f"  [INFO] Используется файл последней пачки: {batch_file_path}")
            with open(batch_file_path, 'r') as f:
                tender_ids_to_process = [line.strip() for line in f if line.strip()]
        else:
            print(f"  [ERROR] Файл последней пачки не найден: {batch_file_path}")
            conn.close()
            return
    else:
        # Старая логика, если --use-last-batch не указан
        tender_select_sql = "SELECT DISTINCT tender_id FROM tnd_agency_docs WHERE download_status = 'pending' ORDER BY tender_id"
        params = [args.batch_size] if args.batch_size else []
        if args.batch_size:
            tender_select_sql += " LIMIT ?"
        
        cur.execute(tender_select_sql, params)
        tender_ids_to_process = [row[0] for row in cur.fetchall()]

    if not tender_ids_to_process:
        print("  [INFO] Нет тендеров с ожидающими файлами для обработки.")
        conn.close()
        return

    print(f"  [INFO] Будет обработано {len(tender_ids_to_process)} тендеров.")

    # Шаг 2: Выборка файлов для этих тендеров
    placeholders = ','.join('?' * len(tender_ids_to_process))
    file_select_sql = f"SELECT id, tender_id, doc_url, doc_title FROM tnd_agency_docs WHERE download_status = 'pending' AND tender_id IN ({placeholders}) ORDER BY tender_id, id"
    
    cur.execute(file_select_sql, tender_ids_to_process)
    files_to_download = cur.fetchall()

    if not files_to_download:
        print("  [INFO] Не найдено файлов для скачивания по заданным критериям.")
        conn.close()
        return

    print(f"📁 Найдено {len(files_to_download)} файлов для скачивания.")
    
    # Цикл скачивания
    for file_id, tender_id, doc_url, doc_title in tqdm(files_to_download, desc="Скачивание файлов", unit="file"):
        
        # Временный путь для скачивания
        temp_path = os.path.join(DOWNLOAD_DIR, f"temp_{file_id}")
        
        # Скачиваем файл
        response, success = download_file(doc_url, temp_path)
        
        if success:
            # Определяем расширение и формируем финальное имя
            extension = get_file_extension(response, doc_url, doc_title)
            local_filename = f"{file_id}_{args.cpv}_{tender_id}{extension}"
            local_path = os.path.join(DOWNLOAD_DIR, local_filename)
            
            # Переименовываем временный файл
            try:
                os.rename(temp_path, local_path)
                cur.execute("UPDATE tnd_agency_docs SET download_status = 'downloaded', local_path = ? WHERE id = ?", (local_path, file_id))
            except OSError as e:
                print(f"  [ERROR] Ошибка переименования файла {temp_path}: {e}")
                cur.execute("UPDATE tnd_agency_docs SET download_status = 'failed' WHERE id = ?", (file_id,))
                if os.path.exists(temp_path): os.remove(temp_path) # Чистим за собой
        else:
            cur.execute("UPDATE tnd_agency_docs SET download_status = 'failed' WHERE id = ?", (file_id,))
            if os.path.exists(temp_path): os.remove(temp_path) # Чистим за собой
        
        conn.commit()

    conn.close()
    print("\n✅ Скачивание завершено!")

    # Экспорт манифеста
    export_manifest_to_csv(sqlite3.connect(DB_PATH), PROJECT_ROOT)

if __name__ == "__main__":
    main()
