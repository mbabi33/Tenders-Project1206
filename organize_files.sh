#!/bin/bash

# Скрипт для организации файлов из исходного каталога в подкаталоги по категориям.
# Использует групповое перемещение `mv` для эффективности.
# Имеет опциональный флаг --compress для предварительного архивирования.

# --- Конфигурация и обработка аргументов ---

# Первый аргумент - рабочий каталог.
WORK_DIR=$1
# Второй аргумент - необязательный флаг сжатия.
COMPRESS_FLAG=$2

# --- Валидация ---

if [[ -z "$WORK_DIR" ]]; then
  echo "Ошибка: Не указан рабочий каталог."
  echo "Использование: $0 <путь_к_рабочему_каталогу> [--compress]"
  exit 1
fi

SOURCE_DIR="${WORK_DIR}/html_tabs"
if [[ ! -d "$SOURCE_DIR" ]]; then
  echo "Ошибка: Исходный каталог не найден по пути '${SOURCE_DIR}'"
  exit 1
fi

# --- Логика сжатия (если указан флаг) ---

if [[ "$COMPRESS_FLAG" == "--compress" ]]; then
  echo "Активирован режим сжатия."
  ARCHIVE_PATH="${WORK_DIR}/html_tabs_archive_$(date +%F).tar.gz"
  
  echo "Начинаю сжатие содержимого папки '${SOURCE_DIR}'..."
  echo "Архив будет сохранен в '${ARCHIVE_PATH}'"

  tar -czf "$ARCHIVE_PATH" -C "$SOURCE_DIR" .

  if [[ $? -eq 0 ]]; then
    echo "Сжатие исходной папки успешно завершено."
    echo "---"
  else
    echo "Внимание: В процессе сжатия произошла ошибка. Организация файлов будет продолжена, но архив может быть не создан."
  fi
fi

# --- Подготовка каталогов для организации ---

DEST_NAMES=("app_main" "app_bids" "app_docs" "agency_docs" "agr_docs")
for dir_name in "${DEST_NAMES[@]}"; do
  mkdir -p "${WORK_DIR}/${dir_name}"
done

echo "Начинаю организацию (перемещение) файлов из '${SOURCE_DIR}'..."

# Включаем nullglob, чтобы glob-шаблоны раскрывались в пустую строку, если нет совпадений.
shopt -s nullglob

# --- Групповое перемещение по шаблонам ---

# Шаблон 1: *app_main*
files_to_move=( "$SOURCE_DIR"/*app_main* )
if [ ${#files_to_move[@]} -gt 0 ]; then
    echo "Перемещаю ${#files_to_move[@]} файлов в app_main..."
    mv "${files_to_move[@]}" "${WORK_DIR}/app_main/"
fi

# Шаблон 2: *app_bids*
files_to_move=( "$SOURCE_DIR"/*app_bids* )
if [ ${#files_to_move[@]} -gt 0 ]; then
    echo "Перемещаю ${#files_to_move[@]} файлов в app_bids..."
    mv "${files_to_move[@]}" "${WORK_DIR}/app_bids/"
fi

# Шаблон 3: *app_docs*
files_to_move=( "$SOURCE_DIR"/*app_docs* )
if [ ${#files_to_move[@]} -gt 0 ]; then
    echo "Перемещаю ${#files_to_move[@]} файлов в app_docs..."
    mv "${files_to_move[@]}" "${WORK_DIR}/app_docs/"
fi

# Шаблон 4: *_agency_docs.html
files_to_move=( "$SOURCE_DIR"/*_agency_docs.html )
if [ ${#files_to_move[@]} -gt 0 ]; then
    echo "Перемещаю ${#files_to_move[@]} файлов в agency_docs..."
    mv "${files_to_move[@]}" "${WORK_DIR}/agency_docs/"
fi

# Шаблон 5: *_agr_docs.html
files_to_move=( "$SOURCE_DIR"/*_agr_docs.html )
if [ ${#files_to_move[@]} -gt 0 ]; then
    echo "Перемещаю ${#files_to_move[@]} файлов в agr_docs..."
    mv "${files_to_move[@]}" "${WORK_DIR}/agr_docs/"
fi

# Выключаем nullglob, чтобы вернуть стандартное поведение оболочки.
shopt -u nullglob

echo "Организация завершена."
