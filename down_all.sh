#!/bin/bash

# --- НАСТРОЙКИ ---
set -e # Останавливать скрипт при любой ошибке

# Путь к корневой директории проекта
ROOT_DIR="/home/nerow/BIKI/ML_Projects/N10_2024t"
# Код CPV
CPV_CODE="45200000"
# Размер пакета для скачивания файлов (для ведущего загрузчика)
BATCH_SIZE=10 # Указанный вами размер
# --- КОНЕЦ НАСТРОЕК ---


echo "--- ЗАПУСК РЕЖИМА СКАЧИВАНИЯ ФАЙЛОВ (связанный режим) ---"

echo "  -> Запуск ведущего загрузчика app_docs (скачивает свою пачку и сохраняет ID тендеров)..."
python download_app_doc_files.py -root "$ROOT_DIR" -c "$CPV_CODE" -batch_size "$BATCH_SIZE"

echo "  -> Запуск ведомого загрузчика agency_docs (использует ID тендеров из предыдущего шага)..."
python download_agency_docs.py -root "$ROOT_DIR" -c "$CPV_CODE" --use-last-batch

echo "  -> Запуск ведомого загрузчика agr_docs (использует ID тендеров из предыдущего шага)..."
python download_agr_docs.py -root "$ROOT_DIR" -c "$CPV_CODE" --use-last-batch

echo "--- СКАЧИВАНИЕ ФАЙЛОВ ЗАВЕРШЕНО УСПЕШНО ---"