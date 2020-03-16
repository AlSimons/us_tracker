import csv
import os
import sys

BASE_DIR = '.'

in_csv = open(os.path.join(BASE_DIR,
                           'COVID-19\\csse_covid_19_data\\csse_covid_19_time_series\\time_series_19-covid-Confirmed.csv'),
              newline='')

reader = csv.reader(in_csv)

dates = reader.__next__()[4:]
print(dates)
accumulator = [0] * len(dates)
print(accumulator)
for line in reader:
    if line[1] == 'US':
        counts = line[4:]
        for n in range(len(dates)):
            accumulator[n] += int(counts[n])

# print(line)
# print(counts)
print(accumulator)

new = [0] * len(dates)
for n in range(1, len(dates)):
    new[n] = accumulator[n] - accumulator[n-1]

with open("output.txt", 'w') as f:
    for n in range(len(dates)):
        print("{}\t{}\t{}".format(dates[n], accumulator[n],new[n]), file=f)