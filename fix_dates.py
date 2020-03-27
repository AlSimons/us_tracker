#!/usr/env/ python
file_path = r'COVID-19\csse_covid_19_data\csse_covid_19_time_series\time_series_covid19_recovered_global.csv'
with open(file_path) as f:
    lines = f.readlines()
line_elements = lines[0].strip().split(',')
dates = line_elements[4:]
for n in range(len(dates)):
    dates[n] = dates[n][:-2]

with open(file_path + '_new', 'w') as o:
    print(','.join(line_elements[:4] + dates), file=o)
    for line in lines[2:]:
        print(line, file=o, end='')
