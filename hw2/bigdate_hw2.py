import zipfile
import os
import pandas as pd

# Шаг 1: Распаковка архива с данными
zip_file_path = 'new-york-city-airbnb-open-data.zip'  # Укажите путь к архиву

# Проверяем, существует ли файл архива
if os.path.exists(zip_file_path):
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall()  # Извлекаем файлы в текущую директорию
    print("Файлы успешно распакованы.")
else:
    print(f"Файл {zip_file_path} не найден.")

# Шаг 2: Загрузка данных в Pandas DataFrame
csv_file_path = 'AB_NYC_2019.csv'  # Имя распакованного CSV-файла

if os.path.exists(csv_file_path):
    df = pd.read_csv(csv_file_path)
    print("Данные успешно загружены.")
else:
    print(f"Файл {csv_file_path} не найден.")

# Выводим первые строки для ознакомления с данными
print(df.head())

# Шаг 3: Расчет среднего и дисперсии с помощью Pandas
mean_price = df['price'].mean()
variance_price = df['price'].var()

print(f"Среднее значение цены: {mean_price}")
print(f"Дисперсия цены: {variance_price}")

# Шаг 4: Реализация MapReduce в Python для расчета среднего и дисперсии

# Шаг 4.1: Функция Mapper
def mapper(data):
    """Функция маппера для расчета суммы цен и количества элементов"""
    for price in data['price']:
        yield price, 1

# Шаг 4.2: Функция Reducer для среднего значения
def reducer_mean(mapped_data):
    """Функция редьюсера для расчета среднего"""
    sum_price = 0
    count = 0
    for price, cnt in mapped_data:
        sum_price += price * cnt
        count += cnt
    return sum_price, count

# Шаг 4.3: Вызываем Mapper и Reducer для расчета среднего
mapped_data = list(mapper(df))
sum_price, count = reducer_mean(mapped_data)
mean_price_mapreduce = sum_price / count

print(f"Среднее значение (MapReduce): {mean_price_mapreduce}")

# Шаг 4.4: Функция Reducer для дисперсии
def reducer_variance(mapped_data, mean):
    """Функция редьюсера для расчета дисперсии"""
    sum_squared_diff = 0
    count = 0
    for price, cnt in mapped_data:
        sum_squared_diff += ((price - mean) ** 2) * cnt
        count += cnt
    variance = sum_squared_diff / count
    return variance

# Шаг 4.5: Вызываем Reducer для расчета дисперсии
variance_price_mapreduce = reducer_variance(mapped_data, mean_price_mapreduce)

print(f"Дисперсия (MapReduce): {variance_price_mapreduce}")

# Шаг 5: Сравнение результатов
print("\nСравнение результатов:")
print(f"Среднее значение (Pandas): {mean_price}")
print(f"Среднее значение (MapReduce): {mean_price_mapreduce}")
print(f"Дисперсия (Pandas): {variance_price}")
print(f"Дисперсия (MapReduce): {variance_price_mapreduce}")
