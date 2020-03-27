import argparse
import csv
import os
import sys


BASE_DIR = '.'
FIXED_DIRS = r'COVID-19\csse_covid_19_data\csse_covid_19_time_series'


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--country', default="US",
                        help="The name of the country to summarize")
    parser.add_argument('-s', '--state',
                        help="State to monitor if country is US")
    args = parser.parse_args()
    return args


def process_file(country, state, file_path):
    with open(os.path.join(BASE_DIR, FIXED_DIRS, file_path)) as in_csv:
        reader = csv.reader(in_csv)

        dates = reader.__next__()[4:]
        accumulator = [0] * len(dates)
        for line in reader:
            if line[1] == country:
                if state and country == 'US' and not line[0].endswith(state):
                    continue
                counts = line[4:]
                for n in range(len(dates)):
                    if counts[n] == '':
                        continue
                    accumulator[n] += int(counts[n])

    new_cases = [0] * len(dates)
    for n in range(1, len(dates)):
        new_cases[n] = accumulator[n] - accumulator[n - 1]
    return dates, new_cases, accumulator


def main():
    args = parse_args()

    # Confirmed cases
    country = args.country
    state = args.state

    confirmed_dates, confirmed_new_cases, confirmed_cases = \
        process_file(country, state, 'time_series_covid19_confirmed_global.csv')

    deaths_dates, new_deaths, cumulative_deaths = \
        process_file(country, state, 'time_series_covid19_deaths_global.csv')

    recovered_dates, new_recovered, cumulative_recovered = \
        process_file(country, state, 'time_series_covid19_recovered_global.csv')

    if (confirmed_dates[-1] != deaths_dates[-1] or
            confirmed_dates[-1] != recovered_dates[-1]):
        sys.exit("Error: dates don't match. Exiting.\n{}\n{}\n{}".format(
                 confirmed_dates[-1], deaths_dates[-1], recovered_dates[-1]))

    title = country if not state else state

    with open("output.txt", 'w') as f:
        print("{}\tConfirmed\t\tDeaths\t\tRecovered".format(title), file=f)
        print("Date\tCumulative\tNew\tCumulative\tNew\tCumulative\tNew",
              file=f)
        for n in range(len(confirmed_dates)):
            print("{}\t{}\t{}\t{}\t{}\t{}\t{}".format(
                confirmed_dates[n],
                confirmed_cases[n],
                confirmed_new_cases[n],
                cumulative_deaths[n],
                new_deaths[n],
                cumulative_recovered[n],
                new_recovered[n]), file=f)


if __name__ == '__main__':
    main()
