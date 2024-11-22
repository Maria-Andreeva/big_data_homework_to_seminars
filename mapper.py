import sys

for line in sys.stdin:
    try:
        # Разбиваем строку на колонки
        columns = line.strip().split(',')
        # Извлекаем цену (price)
        price = columns[columns.index("price")]
        if price.isdigit():
            print(f"{price}\t1")
    except:
        # Игнорируем строки с ошибками
        continue
