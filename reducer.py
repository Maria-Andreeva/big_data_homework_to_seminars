import sys
from math import pow

sum_price = 0
sum_squared_price = 0
count = 0

for line in sys.stdin:
    try:
        price, count_inc = line.strip().split('\t')
        price = float(price)
        count_inc = int(count_inc)

        sum_price += price
        sum_squared_price += price ** 2
        count += count_inc
    except:
        continue

if count > 0:
    mean = sum_price / count
    variance = (sum_squared_price / count) - pow(mean, 2)
    print(f"Mean: {mean}, Variance: {variance}")
