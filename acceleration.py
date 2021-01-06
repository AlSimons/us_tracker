#
# Minimal edits from compare_regions.py.  Print out the days between each
# million case mark
import datetime
import pandas as pd


def simplify_column_dates(d):
    if 'Country' in d:
        return d
    parts = d.split('/')
    month = '{:02d}'.format(int(parts[0]))
    day = '{:02d}'.format(int(parts[1]))
    return '/'.join([month, day])


def get_covid_data():
    # Read in the CSV
    file_name = \
        r'csse_covid_19_time_series\time_series_covid19_confirmed_US.csv'
    covid_data = pd.read_csv(file_name, index_col=6)
    # print("Initial", covid_data.shape)

    # Get rid of all the unneeded columns (all those not in the time series)
    covid_data = covid_data.drop(
        ['UID', 'iso2', 'iso3', 'code3', 'FIPS',
         'Lat', 'Long_', 'Combined_Key', 'Admin2'], axis=1)
    # print("Dropped columns", covid_data.shape)

    # Simplify the column names
    covid_data.columns = [simplify_column_dates(x)
                          for x in covid_data.columns]

    # Add up all the entries for the US
    covid_data = covid_data.groupby('Country_Region').agg(['sum'])
    print("After aggregation", covid_data.shape)

    # That step changed the column headers from [date, date, ...]
    # to [(date, "sum"), (date, "sum").  Return to simply dates.
    covid_data.columns = [x[0] for x in covid_data.columns]
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


def to_ordinal_date(mmdd):
    full_date = mmdd + "/2020"
    date_template = '%m/%d/%Y'
    return datetime.datetime.strptime(full_date, date_template).\
        date().toordinal()


def main():
    set_pandas_options()
    covid_data = get_covid_data()
    find_milestones(covid_data)


if __name__ == '__main__':
    main()
