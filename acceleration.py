#
# Minimal edits from compare_regions.py.  Print out the days between each
# million case mark
import datetime
import pandas as pd
import sys


def simplify_column_dates(d):
    if 'Country' in d:
        return d
    # The year is always in 2 digits, so we don't have to format it.
    parts = d.split('/')
    month = '{:02d}'.format(int(parts[0]))
    day = '{:02d}'.format(int(parts[1]))
    return '/'.join([month, day, parts[2]])


def get_covid_data():
    # Read in the CSV
    if len(sys.argv) > 1 and sys.argv[1] == 'global':
        file_name = \
            r'csse_covid_19_time_series\time_series_covid19_confirmed_global.csv'
    else:
        file_name = \
            r'csse_covid_19_time_series\time_series_covid19_confirmed_US.csv'
    covid_data = pd.read_csv(file_name, index_col=6)
    # print("Initial", covid_data.shape)

    # Get rid of all the unneeded columns (all those not in the time series)
    if len(sys.argv) > 1 and sys.argv[1] == 'global':
        covid_data = covid_data.drop(['Province/State', 'Lat', 'Long',], axis=1)
    else:
        covid_data = covid_data.drop(
            ['UID', 'iso2', 'iso3', 'code3', 'FIPS',
             'Lat', 'Long_', 'Combined_Key', 'Admin2'], axis=1)
    # print("Dropped columns", covid_data.shape)

    # Simplify the column names
    covid_data.columns = [simplify_column_dates(x)
                          for x in covid_data.columns]

    # Add up all the entries
    if len(sys.argv) > 1 and sys.argv[1] == 'global':
        covid_data = covid_data.agg(['sum'])
        covid_data = covid_data.drop(['Country/Region'], axis=1)
    else:
        covid_data = covid_data.groupby('Country_Region').agg(['sum'])
        # That step changed the column headers from [date, date, ...]
        # to [(date, "sum"), (date, "sum").  Return to simply dates.
        covid_data.columns = [x[0] for x in covid_data.columns]
    print("After aggregation", covid_data.shape)

    return covid_data


def set_pandas_options():
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', None)


def find_milestones(covid_data):
    milestones = [
        1,
        100,
        1000,
        10000,
        100000,
        1000000,
        # We'll handle milestones > 1M differently.
    ]
    previous_ordinal = 0
    milestone_n = 0
    millions = 1
    milestone = milestones[milestone_n]
    for n in range(len(covid_data.columns)):
        count = covid_data.iloc[0, n]
        try:
            if count >= milestone:
                mmdd = covid_data.columns[n]
                ordinal = to_ordinal_date(mmdd)
                if previous_ordinal == 0:
                    print("{}\t{:,}".format(mmdd, count))
                else:
                    print("{}\t{:,}\t{} days".format(mmdd, count,
                                                     ordinal - previous_ordinal))
                previous_ordinal = ordinal
                if milestone < 1000000:
                    milestone_n += 1
                    milestone = milestones[milestone_n]
                else:
                    millions += 1
                    milestone = millions * 1000000
        except TypeError:
            # In global mode, the first column is a string of all the location
            # names.
            continue
    ordinal = to_ordinal_date(covid_data.columns[-1])
    if ordinal != previous_ordinal:
        print("Current:\t{:,}\t{} days".format(covid_data.iloc[0, -1],
                                       ordinal - previous_ordinal))


def to_ordinal_date(mmddyy):
    date_template = '%m/%d/%y'
    return datetime.datetime.strptime(mmddyy, date_template).\
        date().toordinal()


def main():
    set_pandas_options()
    covid_data = get_covid_data()
    find_milestones(covid_data)


if __name__ == '__main__':
    main()
