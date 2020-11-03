import argparse
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-c', '--countries', action='store_true',
        help="Compare countries instead of states"
    )
    parser.add_argument(
        '-d', '--days', type=int, default=7,
        help="Number of days (D) to use with --percent-increase and "
             "--increase. Default=7.")
    parser.add_argument(
        '-D', '--deaths', action='store_true',
        help="Plot death data instead of case data."
    )
    parser.add_argument(
        '-f', '--filter-smallest-states', default=20,
        help="Filter out the N smallest states. Only applied before doing "
             "percentages. Default=20.")
    parser.add_argument(
        '-i', '--increase-count', action='store_true',
        help="Graph the top N states based on increased count in the past D "
             "days. Default graph based on raw case count.")
    parser.add_argument(
        '-l', '--lowest', action='store_true',
        help="Display the least infected regions, not most infected.")
    parser.add_argument(
        '-n', '--num_states', default=10, type=int,
        help="The number of states to display.")
    parser.add_argument(
        '-p', '--percent-increase', action='store_true',
        help="Graph the top N states based on percent increase in the past D "
             "days. Default graph based on raw case count.")
    parser.add_argument(
        '-P', '--per-population', action='store_true',
        help="Use population-adjusted data (cases per-10k people")
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
    """
    for c in range(len(c_data.columns) - 1, 1, -1):
        c_data[c_data.columns[c]] = \
            c_data[c_data.columns[c]] - c_data[c_data.columns[c - 1]]
    """
    return c_data


def simplify_column_dates(d):
    parts = d.split('/')
    month = '{:02d}'.format(int(parts[0]))
    day = '{:02d}'.format(int(parts[1]))
    return '/'.join([month, day])


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

    print("Initial columns", covid_cases.columns)
    # print("Initial covid cases shape", covid_cases.shape)
    # print("Initial covid deaths shape", covid_deaths.shape)

    if args.countries:
        to_drop = ['Lat', 'Long_', 'Province_State']
    else:
        to_drop = ['UID', 'iso2', 'iso3', 'code3', 'FIPS', 'Admin2',
                   'Country_Region', 'Lat', 'Long_', 'Combined_Key']
    covid_cases = covid_cases.drop(to_drop, axis=1)
    covid_deaths = covid_deaths.drop(to_drop + ['Population'], axis=1)

    # Get rid of data before March.  Mostly zero.
    covid_cases = covid_cases.drop([x for x in covid_cases.columns
                                    if x[0] < '03/15'], axis=1)
    covid_deaths = covid_deaths.drop([x for x in covid_deaths.columns
                                      if x[0] < "03/15"], axis=1)

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

    # Simplify the column names
    covid_cases.columns = [simplify_column_dates(x[0])
                           for x in covid_cases.columns]
    covid_deaths.columns = [simplify_column_dates(x[0])
                            for x in covid_deaths.columns]

    # The files have total counts. We need to turn them into daily changes.
    covid_cases = compute_daily_change(covid_cases, args)
    covid_deaths = compute_daily_change(covid_deaths, args)

    return covid_cases, covid_deaths


def filter_by_state(df, state):
    return df.loc[state]


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
    covid_death_rate_data.plot(title="Death rate (running average over {} days".
                               format(args.days), logy=True)
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

    output_df = None
    for state in ['Montana', 'Arizona', 'Connecticut', 'New York', 'Wisconsin',
                  'North Dakota', 'South Dakota', 'Puerto Rico', 'Florida',
                  'Texas', 'California']:
        cases = filter_by_state(covid_cases, state)
        deaths = filter_by_state(covid_deaths, state)
        rate = compute_rate(cases, deaths).transpose()
        rate['copy_index'] = rate.index
        if output_df is None:
            output_df = rate
        else:
            print(output_df.index)
            output_df = pd.merge(output_df, rate, how='outer', on=['copy_index'])
    output_df.set_index(['copy_index'], inplace=True)
    # output_df.drop(['copy_index'], axis=1)
    plot(output_df, args)


if __name__ == '__main__':
    main()
