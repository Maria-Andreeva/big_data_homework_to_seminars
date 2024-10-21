import json
from pathlib import Path
from functools import reduce

# Mapper: считывает файл и возвращает кортеж (n, mean, M2)
def mapper(path):
    if path.is_file() and path.suffix == '.json':
        with open(path, 'r') as f:
            info = json.load(f)
        score = float(info['movieIMDbRating'])
        # Для одного файла: n=1, mean=score, M2=0 (потому что 1 элемент)
        return 1, score, 0.0
    return 0, 0.0, 0.0

# Reducer: объединяет результаты двух кортежей (n1, mean1, M2_1) и (n2, mean2, M2_2)
def reducer(data1, data2):
    n1, mean1, M2_1 = data1
    n2, mean2, M2_2 = data2

    # Если один из наборов пустой, просто возвращаем другой
    if n1 == 0:
        return data2
    if n2 == 0:
        return data1

    # Общий размер данных
    n_total = n1 + n2

    # Разница в средних
    delta = mean2 - mean1

    # Объединённое среднее
    mean_total = mean1 + delta * n2 / n_total

    # Объединённое M2
    M2_total = M2_1 + M2_2 + delta**2 * n1 * n2 / n_total

    return n_total, mean_total, M2_total

# Применение map-reduce к набору данных из файлов
# Передаем начальное значение (0, 0.0, 0.0) для обработки пустого набора
file_paths = list(Path('imdb-user-reviews').glob('**/*'))  # Сначала собираем список файлов

if file_paths:
    n, mean, M2 = reduce(reducer, map(mapper, file_paths), (0, 0.0, 0.0))  # Указываем начальное значение
else:
    n, mean, M2 = 0, 0.0, 0.0

# Вывод результата
if n > 0:
    print(mean, (M2 / n) ** 0.5)
else:
    print("Нет данных для вычисления.")
