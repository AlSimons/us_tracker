import argparse
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--area',
                        help="Graph only the specified area, e.g, \"New York\"")
    parser.add_argument(
        '-c', '--countries', action='store_true',
        help="Compare countries instead of states"
    )
    parser.add_argument(
        '-d', '--days', type=int, default=7,
        help="Number of days (D) to use for running average. Default=7.")
    parser.add_argument('--log', action='store_true',
                        help="Plot with log Y.")
    parser.add_argument(
        '-l', '--lowest', action='store_true',
        help="Display the lowest death rate regions. (default: highest)")
    parser.add_argument(
        '-n', '--num_states', default=10, type=int,
        help="The number of states to display. (default: 10)")
    parser.add_argument('-s', '--start-date',
                        help="Earliest date to plot (expands scale if early peaks are omitted.")
    args = parser.parse_args()
    return args


def compute_daily_change(covid_data, args):
    """
    Compute the increase in counts over  over a period of days determined
    by the constant INCREASE_INTERVAL, above.  If the user wanted
    population adjusted values, that normalization has already been done.
    :param covid_data: A dataframe with all the covid data.
    :return: covid_data
    """
    c_data = covid_data

    for c in range(len(c_data.columns) - 1, args.days, -1):
        c_data[c_data.columns[c]] = \
            c_data[c_data.columns[c]] - c_data[c_data.columns[c - args.days]]
    return c_data


def simplify_column_dates(d):
    parts = d.split('/')
    try:
        month = '{:02d}'.format(int(parts[0]))
        day = '{:02d}'.format(int(parts[1]))
        return '/'.join([month, day])
    except ValueError:
        return d


def get_covid_data(args):
    # Read in the CSV
    if args.countries:
        cases_file = r'csse_covid_19_time_series\time_series_covid19_confirmed_global.csv'
        deaths_file = r'csse_covid_19_time_series\time_series_covid19_deaths_global.csv'
    else:
        cases_file = r'csse_covid_19_time_series\time_series_covid19_confirmed_US.csv'
        deaths_file = r'csse_covid_19_time_series\time_series_covid19_deaths_US.csv'

    covid_cases = pd.read_csv(cases_file, index_col=6)
    covid_deaths = pd.read_csv(deaths_file, index_col=6)

    # print("Initial columns", covid_cases.columns)
    # print("Initial covid cases shape", covid_cases.shape)
    # print("Initial covid deaths shape", covid_deaths.shape)

    if args.countries:
        to_drop = ['Lat', 'Long', 'Province/State']
    else:
        to_drop = ['UID', 'iso2', 'iso3', 'code3', 'FIPS', 'Admin2',
                   'Country_Region', 'Lat', 'Long_', 'Combined_Key']
    covid_cases = covid_cases.drop(to_drop, axis=1)
    if not args.countries:
        # The US deaths file has an extra "population" column.
        to_drop.append('Population')
    covid_deaths = covid_deaths.drop(to_drop, axis=1)

    # print("Cases 1\n", covid_cases.columns)
    # print("Deaths 1\n", covid_deaths.columns)
    if args.countries:
        groupby = 'Country/Region'
    else:
        groupby = 'Province_State'
    covid_cases = covid_cases.groupby(groupby).agg(['sum'])
    covid_deaths = covid_deaths.groupby(groupby).agg(['sum'])
    # print("After aggregation cases", covid_cases.shape)
    # print("After aggregation deaths", covid_deaths.shape)

    # Drop the non-states, except for DC and Puerto Rico
    if not args.countries:
        non_states = ['American Samoa', 'Diamond Princess', 'Grand Princess',
                      'Guam', 'Northern Mariana Islands', 'Virgin Islands']
        covid_cases.drop(non_states, inplace=True)
        covid_deaths.drop(non_states, inplace=True)


    # Simplify the column names
    covid_cases.columns = [simplify_column_dates(x[0])
                           for x in covid_cases.columns]
    covid_deaths.columns = [simplify_column_dates(x[0])
                            for x in covid_deaths.columns]

    # Get rid of data before April (mostly zero), or the start date from the
    # command line (getting rid of early peaks lets the scale expand).
    if args.start_date:
        start_date = args.start_date
    else:
        start_date = '04/01'
    covid_cases = covid_cases.drop([x for x in covid_cases.columns
                                    if x[0] < start_date], axis=1)
    covid_deaths = covid_deaths.drop([x for x in covid_deaths.columns
                                      if x[0] < start_date], axis=1)

    # The files have total counts. We need to turn them into daily changes.
    covid_cases = compute_daily_change(covid_cases, args)
    covid_deaths = compute_daily_change(covid_deaths, args)

    return covid_cases, covid_deaths


def filter_by_area(df, area):
    return df.loc[area]


def compute_rate(cases, deaths):
    ret = pd.DataFrame(deaths * 100 / cases).transpose()
    return ret

def plot(covid_death_rate_data, args):
    """
    Plot the death rate over time.
    :param covid_death_rate_data:
    :param args:
    :return:
    """
    if args.area:
        covid_death_rate_data = filter_by_area(covid_death_rate_data, args.area)

    covid_death_rate_data.plot(title="Death rate (running average over {} days)".
                               format(args.days), logy=args.log)
    plt.show()


def set_pandas_options():
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', None)


def main():
    set_pandas_options()
    args = parse_args()
    covid_cases, covid_deaths = get_covid_data(args)
    rates = 100.0 * covid_deaths / covid_cases

    if args.lowest:
        ascending = True
    else:
        ascending = False
    rates = rates.sort_values(by=rates.columns[-1],
                              ascending=ascending)
    rates = rates.iloc[:args.num_states]
    rates = rates.transpose()

    plot(rates, args)


if __name__ == '__main__':
    main()
