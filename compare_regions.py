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

    if args.countries and args.percent_increase:
        parser.error(
            "--countries and --percent-increase are mutually exlusive."
        )
    if args.percent_increase and args.increase_count:
        parser.error(
            "--increase-count --and percent-increase are mutually exclusive.")
    return args


def simplify_column_dates(d):
    parts = d.split('/')
    month = '{:02d}'.format(int(parts[0]))
    day = '{:02d}'.format(int(parts[1]))
    return '/'.join([month, day])


def get_covid_data(args):
    # Read in the CSV
    if args.deaths:
        if args.countries:
            file_name = \
                r'csse_covid_19_time_series\time_series_covid19_deaths_global.csv'
        else:
            file_name = \
                r'csse_covid_19_time_series\time_series_covid19_deaths_US.csv'
    else:
        if args.countries:
            file_name = \
                r'csse_covid_19_time_series\time_series_covid19_confirmed_global.csv'
        else:
            file_name = \
                r'csse_covid_19_time_series\time_series_covid19_confirmed_US.csv'
    covid_data = pd.read_csv(file_name, index_col=6)
    # print("Initial", covid_data.shape)

    # Get rid of all the unneeded columns (all those not in the time series)
    if args.countries:
        covid_data = covid_data.drop(['Lat', 'Long', 'Province/State'], axis=1)
    else:
        covid_data = covid_data.drop(
            ['UID', 'iso2', 'iso3', 'code3', 'FIPS', 'Country_Region',
             'Lat', 'Long_', 'Combined_Key', 'Admin2'], axis=1)
    # print("Dropped columns", covid_data.shape)

    # The deaths time series file also has a Population column/
    if args.deaths and not args.countries:
        covid_data = covid_data.drop(['Population'], axis=1)

    # Drop the non-states, except for DC and Puerto Rico
    if not args.countries:
        covid_data.drop(
            ['American Samoa', 'Diamond Princess', 'Grand Princess', 'Guam',
             'Northern Mariana Islands', 'Virgin Islands'], inplace=True)
        print("Dropped non-states", covid_data.shape)

    # Simplify the column names
    covid_data.columns = [simplify_column_dates(x)
                          for x in covid_data.columns]

    # Get rid of data before March.  Mostly zero.
    covid_data.drop([x for x in covid_data.columns if x < "03/01"], axis=1,
                    inplace=True)

    # Add up all the entries for a state / territory
    if args.countries:
        covid_data = covid_data.groupby('Country/Region').agg(['sum'])
    else:
        covid_data = covid_data.groupby('Province_State').agg(['sum'])
    # print("After aggregation", covid_data.shape)

    return covid_data


def adjust_population(covid_data):
    # Get the population info for the states (& DC, Puerto Rico)
    pops = pd.read_csv(r'..\..\us_tracker\populations.csv', index_col=0)

    # The population file contains non-US states. Remove the extra.
    pops = pops[pops.Category == 'US State']
    pops.sort_index(inplace=True)

    # We want cases per-10,000 people, so extract the population series and
    # adjust the population counts.
    pops = pops.Population / 10000

    # Now make our data population-based
    covid_data = covid_data.divide(pops, axis=0)
    return covid_data


def compute_count_increase(covid_data, args):
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


def compute_percent_increase(covid_data, args):
    """
    Compute the percent increase in counts, over a period of days determined
    by the constant INCREASE_INTERVAL, above.  If the user wanted
    population adjusted values, that normalization has already been done.
    :param covid_data: A dataframe with all the covid data.
    :param args: The command line arguments
    :return: covid_data
    """
    c_data = covid_data

    c_data = filter_smallest_states(c_data, args)
    for c in range(len(c_data.columns) - 1, args.days, -1):
        c_data[c_data.columns[c]] = \
            100.0 * (c_data[c_data.columns[c]] - c_data[c_data.columns[c - args.days]]) / \
            c_data[c_data.columns[c - args.days]]
    # Because we may have just done division by zero, we need to replace the
    # generated NaNs with zeros.
    c_data = c_data.fillna(0).replace([np.inf, -np.inf], 0)
    # The very early days of the pandemic saw huge percentage increases, which
    # make the rest of the plot unusable because of scaling, even logarithmic
    # scaling.  We'll drop all of March and April
    c_data = c_data.drop(c_data.columns[0:61], axis=1)
    c_data = c_data.sort_values(by=c_data.columns[-1], ascending=False)
    print(c_data[c_data.columns[-1]])
    return c_data


def filter_smallest_states(covid_data, args):
    covid_data = covid_data.sort_values(by=covid_data.columns[-1], ascending=False)
    print(covid_data[covid_data.columns[-1]])
    covid_data = covid_data[:-args.filter_smallest_states]
    print(covid_data[covid_data.columns[-1]])
    return covid_data


def plot(covid_data, args):
    adjusted = ""
    if args.per_population:
        adjusted = "population adjusted (per-10,000) "

    if args.deaths:
        count_type = "deaths "
    else:
        count_type = "cases "

    title = "BUG: Didn't set the title"

    covid_data = covid_data.sort_values(by=covid_data.columns[-1],
                                        ascending=False)

    # To rank by PERCENT increase
    if args.percent_increase:
        title = "Percent {}increase of {} over {} days".format(
            adjusted, count_type, args.days)

    # To rank by raw increase
    elif args.increase_count:
        title = "Highest {}increase of {} over {} days".format(
            adjusted, count_type, args.days)

    # To rank by raw counts
    elif args.lowest:
        title = "Lowest {}{}count".format(adjusted, count_type)
    else:
        title = "Highest {}{}count".format(adjusted, count_type)

    if args.lowest:
        covid_data = covid_data.iloc[-args.num_states:]
    else:
        covid_data = covid_data.iloc[:args.num_states]

    # It seems that pandas time series plotting wants the states as columns
    # and the time series points as indexes.
    covid_data = covid_data.transpose()

    covid_data.plot(title=title)
    plt.show()


def set_pandas_options():
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', None)


def main():
    set_pandas_options()
    args = parse_args()
    covid_data = get_covid_data(args)
    if args.per_population:
        covid_data = adjust_population(covid_data)
    if args.increase_count:
        covid_data = compute_count_increase(covid_data, args)
    if args.percent_increase:
        covid_data = compute_percent_increase(covid_data, args)
    plot(covid_data, args)


if __name__ == '__main__':
    main()
