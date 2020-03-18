import csv
import os
import sys


BASE_DIR = '.'
FIXED_DIRS = r'COVID-19\csse_covid_19_data\csse_covid_19_time_series'
COUNTRY = 'US'


def process_file(country, file_path):
    with open(os.path.join(BASE_DIR, FIXED_DIRS, file_path)) as in_csv:
        reader = csv.reader(in_csv)

        dates = reader.__next__()[4:]
        accumulator = [0] * len(dates)
        for line in reader:
            if line[1] == country:
                counts = line[4:]
                for n in range(len(dates)):
                    accumulator[n] += int(counts[n])

    new_cases = [0] * len(dates)
    for n in range(1, len(dates)):
        new_cases[n] = accumulator[n] - accumulator[n - 1]
    return dates, new_cases, accumulator


def main():
    # Confirmed cases
    confirmed_dates, confirmed_new_cases, confirmed_cases = \
        process_file(COUNTRY, 'time_series_19-covid-Confirmed.csv')
    deaths_dates, new_deaths, cumulative_deaths = \
        process_file(COUNTRY, 'time_series_19-covid-Deaths.csv')
    recovered_dates, new_recovered, cumulative_recovered = \
        process_file(COUNTRY, 'time_series_19-covid-Recovered.csv')
    if (confirmed_dates[-1] != deaths_dates[-1] or
            confirmed_dates[-1] != recovered_dates[-1]):
        sys.exit("Error: dates don't match. Exiting.\n{}\n{}\n{}".format(
                 confirmed_dates[-1], deaths_dates[-1], recovered_dates[-1]))

    with open("output.txt", 'w') as f:
        print("Confirmed\t\tDeaths\t\tRecovered", file=f)
        print("Date\tCumulative\tNew\tCumulative\tNew\tCumulative\tNew",
              file=f)
        for n in range(len(confirmed_dates)):
            print("{}\t{}\t{}\t{}\t{}\t{}\t{}".format(confirmed_dates[n],
                                      confirmed_cases[n],
                                      confirmed_new_cases[n],
                                      cumulative_deaths[n],
                                      new_deaths[n],
                                      cumulative_recovered[n],
                                      new_recovered[n]), file=f)

if __name__ == '__main__':
    main()
