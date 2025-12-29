

import pandas as pd
import os

def get_classification_map():
    """
    Возвращает список кортежей с категориями и их ключевыми словами.
    Порядок в списке определяет приоритет (более специфичные правила выше).
    """
    return [
        ('ШАБЛОНЫ', ['შაბლონი']),
        ('ДОГОВОРЫ', ['ხელშეკრულება']),
        ('ПРОТОКОЛЫ_И_АКТЫ', ['ოქმი']),
        ('СМЕТЫ_И_ТАБЛИЦЫ', ['ხარჯთაღრიცხვა', 'ღირებულებ', 'სახარჯთაღრიცხვო']),
        ('ЧЕРТЕЖИ_И_ПРОЕКТЫ', [
            'ნახაზი', 'ნახაზები', 'gegma', 'კონსტრუქცია', 'konstruqcia', 
            'პროფილი', 'პროექტი', 'გრაფიკული', 'ganivi', 'grdzivi', 
            'mockoba', 'Naxazebi', 'naxazi', 'ელექტროობა', 'საინჟინრო'
        ]),
        # --- Низкоприоритетные ключевые слова ---
        ('СМЕТЫ_И_ТАБЛИЦЫ', ['ცხრილი']),
        ('ПРОЧИЕ_ДОКУМЕНТЫ', ['დანართი', 'გრაფიკი', 'გარანტია', 'გამოცდილება', 'უწყისები']),
    ]

def classify_filename(filename):
    """
    Классифицирует имя файла на основе предопределенного набора правил.
    """
    if not isinstance(filename, str):
        return 'НЕОПРЕДЕЛЕННЫЙ'

    filename_lower = filename.lower()
    
    classification_map = get_classification_map()
    
    # Поиск по ключевым словам с учетом приоритета
    for category, keywords in classification_map:
        for keyword in keywords:
            if keyword in filename_lower:
                return category
    
    # Если ключевых слов не найдено, но файл — таблица, считаем его сметой
    if filename_lower.endswith(('.xlsx', '.xls')):
        return 'СМЕТЫ_И_ТАБЛИЦЫ'

    return 'НЕОПРЕДЕЛЕННЫЙ'

def main():
    """
    Основная функция для чтения, классификации и сохранения данных.
    """
    # Определение путей
    input_csv_path = os.path.join('ML_DATA', 'file_names.csv')
    output_csv_path = os.path.join('ML_DATA', 'file_names_classified.csv')
    
    print(f"Чтение данных из '{input_csv_path}'...")
    
    try:
        # Указываем разделитель, так как в именах файлов могут быть запятые
        df = pd.read_csv(input_csv_path, sep=',')
    except FileNotFoundError:
        print(f"ОШИБКА: Файл '{input_csv_path}' не найден.")
        return

    # Проверка наличия необходимой колонки
    if 'file_name' not in df.columns:
        print(f"ОШИБКА: В файле '{input_csv_path}' отсутствует колонка 'file_name'.")
        return

    print("Выполняется классификация файлов...")
    df['category'] = df['file_name'].apply(classify_filename)
    
    print(f"Сохранение результатов в '{output_csv_path}'...")
    df.to_csv(output_csv_path, index=False, encoding='utf-8', sep=',')
    
    print("\n--- Статистика по категориям ---")
    print(df['category'].value_counts())
    
    print(f"\nГотово! Результаты сохранены в '{output_csv_path}'.")

if __name__ == '__main__':
    main()
