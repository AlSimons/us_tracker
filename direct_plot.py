#!/usr/bin/env python3

# Standard library
import argparse
import datetime
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import os
import re
import statistics
import sys

# Project imports
from mysql_credentials import username, password

# SQL Alchemy imports
from sqlalchemy import create_engine, text, MetaData, Table, func, select


# Number of days to include in the X days rolling average plots
ROLLING_AVERAGE_DAYS = 7

# Max days to display in the plots
DISPLAY_DAYS = 200

# Compare current death count to confirmed cases X days ago to compute death
# rate percentage, since death usually lags diagnosis. Most important for areas
# with rapidly increasing case loads, which would otherwise show an artificially
# low death rate.
DEATHS_LAG = 10

DATABASE_NAME = r'mysql+mysqlconnector://{}:{}@localhost/covid_data'
POPULATION_FILE = r'us_tracker\populations.csv'


class ColumnInfo:
    def __init__(self, field,
                 computed_type, depended_field):
        self.field = field
        self.computed_type = computed_type
        self.depended_field = depended_field


class Day:
    columns = None
    all_days = []
    day_hash = {}
    past_accelerations = []

    def __init__(self, date,
                 conf_num, deaths_num,
                 parent_conf_num = 0, parent_deaths_num = 0):
        self.date = date
        Day.all_days.append(self)
        Day.day_hash[date] = self
        for column in Day.columns:
            setattr(self, column.field, 0)
        self.conf_num = conf_num
        self.deaths_num = deaths_num
        self.parent_conf_num = parent_conf_num
        self.parent_deaths_num = parent_deaths_num

    @staticmethod
    def set_columns(_columns):
        Day.columns = _columns

    @staticmethod
    def set_data(location_data, parent_data):
        # Sanity check, since we're going to walk the two result lists
        # in parallel.
        if parent_data is not None and len(location_data) != len(parent_data):
            sys.exit("How are the lengths of the two results different: {} {}?". \
                     format(len(location_data), len(parent_data)))
        for n in range(len(location_data)):
            # OK, let's create days!
            if parent_data is not None:
                Day(ordinal_date_to_string(location_data[n][0], True),
                    location_data[n][1], location_data[n][2],
                    parent_data[n][1], parent_data[n][2])
            else:
                Day(ordinal_date_to_string(location_data[n][0], True),
                    location_data[n][1], location_data[n][2])

    def compute_vel_acc(self, prev, dependence_type):
        # This handles velocity and acceleration, not smoothed acceleration.
        for column in Day.columns:
            if column.computed_type == dependence_type:
                target_field = column.field
                dep_field = column.depended_field
                try:
                    cur_dep = getattr(self, dep_field)
                except AttributeError:
                    cur_dep = 0
                try:
                    prev_dep = getattr(prev, dep_field)
                except AttributeError:
                    prev_dep = 0
                setattr(self, target_field, cur_dep - prev_dep)

    def compute_pct(self):
        for column in Day.columns:
            if column.computed_type == 'pct':
                target_field = column.field
                depended_field = column.depended_field
                depended_field2 = column.depended_field2
                try:
                    value = getattr(self, depended_field)
                except AttributeError:
                    value = 0
                base = getattr(self, depended_field2)
                try:
                    pct = round(100.0 * (value / base), 1)
                except ZeroDivisionError:
                    pct = ''
                setattr(self, target_field, pct)

    @staticmethod
    def compute_pcts_velocities_accelerations():
        for n in range(1, len(Day.all_days)):
            today = Day.all_days[n]
            yesterday = Day.all_days[n-1]
            today.compute_pct()
            today.compute_vel_acc(yesterday, 'vel')
            today.compute_vel_acc(yesterday, 'acc')

    @staticmethod
    def compute_rolling_average():
        for column in Day.columns:
            if column.computed_type == 'rolling_average':
                Day.past_accelerations = []
                for day in Day.all_days:
                    try:
                        avg = getattr(day, column.depended_field)
                    except AttributeError:
                        avg = 0
                    Day.past_accelerations.append(avg)

                    setattr(day, column.field,
                            round(statistics.mean(
                                Day.past_accelerations[-ROLLING_AVERAGE_DAYS:]),
                                2))

    def __str__(self):
        out = '{}\t'.format(self.date)
        for column in Day.columns:
            out += str(getattr(self, column.field)) + '\t'
            out += str(self.parent_conf_num) + '\t'
            out += str(self.parent_deaths_num)
        return out


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-1', '--admin1',
                        help="Specify admin-1 level group (e.g., country)")
    parser.add_argument(
        '-2', '--admin2',
        help="Specify admin-2 level group (e.g., state / province)")
    parser.add_argument(
        '-3', '--admin3',
        help="Specify admin-3 level group (e.g., city / county)")
    parser.add_argument('--parent-1',
                        help="Country to use for percentage computation")
    parser.add_argument('--parent-2',
                        help="State to use for percentage computation")
    parser.add_argument('--parent-global', action="store_true",
                        help="Compute percentage of global cases.")
    args = parser.parse_args()
    return args


def get_population(focus):
    with open(POPULATION_FILE) as f:
        for line in f:
            parts = line.strip().split(',')
            if parts and parts[0] == focus:
                return int(parts[1])
    # Use zero as the "no data" flag, since no entity will have a population
    # of zero
    return 0


def define_columns():
    columns_defs = [
        ColumnInfo('conf_num', '', ''),
        ColumnInfo('daily_conf', 'vel', 'conf_num'),
        ColumnInfo('avg_daily_conf', 'rolling_average', 'daily_conf'),
        ColumnInfo('deaths_num', '', ''),
        ColumnInfo('daily_deaths', 'vel', 'deaths_num'),
        ColumnInfo('avg_daily_deaths', 'rolling_average', 'daily_deaths'),
    ]
    return columns_defs


def one_plot(dates, values, focus, position, title, color):
    plt.subplot(position)
    plt.title(focus + " " + title)
    plt.xticks(rotation=90)
    # Set x-axis major ticks to weekly interval, on Mondays
    #### plt.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=mdates.MONDAY))
    # Format x-tick labels as 3-letter month name and day number
    #### plt.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'));
    plt.plot(dates, values, color + '-')
    axes = plt.gca()
    axes.set_ylim(bottom=0)


def plot_it(focus, parent_focus):
    dates = []
    conf_vel = []
    conf_num = []
    conf_accel = []
    deaths = []
    deaths_vel = []
    deaths_acc = []
    # Percent of parent
    conf_pop = []
    deaths_pop = []

    doing_parents = Day.all_days[-1].parent_conf_num > 0

    for day in Day.all_days[-DISPLAY_DAYS:]:
        dates.append(day.date)
        conf_num.append(day.conf_num)
        conf_vel.append(day.daily_conf)
        conf_accel.append(day.avg_daily_conf)
        deaths.append(day.deaths_num)
        deaths_vel.append(day.daily_deaths)
        deaths_acc.append(day.avg_daily_deaths)
        if doing_parents:
            if day.parent_conf_num > 0:
                conf_pop.append((day.conf_num / day.parent_conf_num) * 100)
            else:
                conf_pop.append(0.0)
            if day.parent_deaths_num > 0:
                deaths_pop.append((day.deaths_num / day.parent_deaths_num) *
                                  100)
            else:
                deaths_pop.append(0.0)

            # Layout of the charts depends on whether we are doing parents.
            # We're still in "if doing_parents:"
            conf_num_pos = 241
            conf_daily_pos = 242
            conf_roll_avg_pos = 243
            conf_pop_pos = 244
            deaths_num_pos = 245
            deaths_daily_pos = 246
            deaths_roll_avg_pos = 247
            deaths_pop_pos = 248
        else:
            # No display of parents percentage
            conf_num_pos = 231
            conf_daily_pos = 232
            conf_roll_avg_pos = 233
            deaths_num_pos = 234
            deaths_daily_pos = 235
            deaths_roll_avg_pos = 236

    focus_population = get_population(focus)

    if doing_parents:
        parent_population = get_population(parent_focus)
        if parent_population > 0 and focus_population > 0:
            percent_of_parent_population = \
                100 * focus_population / parent_population
            percent_of_parent_pop_str = " ({}%)".\
                format(round(percent_of_parent_population, 2))
        else:
            percent_of_parent_pop_str = ""

    plt.figure(figsize=(19, 9), num='{} COVID-19 History ({} days)'.
               format(focus, len(dates)))
    plt.subplots_adjust(hspace=.27, wspace=.16,
                        top=.97, bottom=.07,
                        left=.06, right=.97)

    if focus_population > 0:
        confirmed_percentage = " {}%".\
            format(round(100 * conf_num[-1] / focus_population, 2))
    else:
        confirmed_percentage = ""

    one_plot(dates, conf_num, focus, conf_num_pos, "{:,} Confirmed Cases {}".
             format(conf_num[-1], confirmed_percentage), 'g')
    # The number will print with a minus sign if < 0, so we only need to
    # add the "+" if >= 0.
    sign = "+" if conf_vel[-1] > 0 else ""
    one_plot(dates, conf_vel, focus, conf_daily_pos,
             "{:,} Daily New Cases {}{}%".
             format(conf_vel[-1], sign,
                    round(100 * conf_vel[-1] / conf_num[-2], 2)),
             'y')
    sign = "+" if conf_accel[-1] > conf_accel[-2] else ""
    one_plot(dates, conf_accel, focus, conf_roll_avg_pos,
             "{:,} {} Day Avg New Cases {}{}%".
             format(conf_accel[-1],
                    min(len(dates), ROLLING_AVERAGE_DAYS),
                    sign,
                    round(100 *
                          (conf_accel[-1] - conf_accel[-2]) / conf_accel[-2],
                          2)), 'k')
    if doing_parents:
        one_plot(dates, conf_pop, focus, conf_pop_pos,
                 "{}% of {} cases{}".
                 format(round(conf_pop[-1], 2), parent_focus,
                        percent_of_parent_pop_str), 'k')
    try:
        one_plot(dates, deaths, focus, deaths_num_pos, "{:,} Deaths Cumulative: {}%".
                 format(deaths[-1],
                        round((deaths[-1] /
                               conf_num[-(DEATHS_LAG + 1)]) * 100, 1)),
                 'r')
    except IndexError:
        print("No data found for {}".format(focus))
        sys.exit()
    sign = "+" if deaths_vel[-1] > deaths_vel[-2] else ""
    one_plot(dates, deaths_vel, focus, deaths_daily_pos, "{:,} Daily New Deaths {}{}%".
             format(deaths_vel[-1], sign,
                    round(100 * deaths_vel[-1] / deaths[-2], 2)), 'b')
    try:
        deaths_inc_pct_sign = "+" if deaths_acc[-1] > deaths_acc[-2] else ""
        deaths_inc_pct_str = "{}{}%".format(
            deaths_inc_pct_sign, round(
                100 * (deaths_acc[-1] - deaths_acc[-2]) / deaths_acc[-2], 2))
    except ZeroDivisionError:
        deaths_inc_pct_str = ""

    one_plot(dates, deaths_acc, focus, deaths_roll_avg_pos,
             "{} {} Day Rolling Avg Deaths {}".
             format(deaths_acc[-1],
                    min(len(dates), ROLLING_AVERAGE_DAYS),
                    deaths_inc_pct_str), 'k')
    if doing_parents:
        one_plot(dates, deaths_pop, focus, deaths_pop_pos,
                 "{}% of {} deaths{}".
                 format(round(deaths_pop[-1], 2), parent_focus,
                        percent_of_parent_pop_str), 'k')
    plt.show()


def get_level_focus(args):
    focuses = [None, None, None]
    worldwide = True
    if args.admin1:
        # Summarize a country
        focuses[0] = args.admin1
        worldwide = False
    if args.admin2:
        # Summarize a state / province
        focuses[1] = args.admin2
        worldwide = False
    if args.admin3:
        # Summarize a city / county
        focuses[2] = args.admin3
        worldwide = False
    return worldwide, focuses


def get_parent_level_focus(args):
    if args.parent_global:
        return None, 'Global'

    if args.parent_1:
        # Summarize a country
        return 'Country', args.parent_1

    if args.parent_2:
        # Summarize a state / province
        return 'State', args.parent_2

    # No parent display requested.
    return None, None


engine = None


def get_engine():
    global engine
    if engine is None:
        engine = create_engine(DATABASE_NAME.format(username, password),
                               echo=False)
    return engine


def get_tables_and_connection():
    eng = get_engine()
    metadata = MetaData()
    location_table = \
        Table('location', metadata, autoload=True,
              autoload_with=engine)
    datum_table = \
        Table('datum', metadata, autoload=True,
              autoload_with=engine)
    conn = engine.connect()
    return location_table, datum_table, eng


def ordinal_date_to_string(ordinal, mmdd=False):
    date_string = str(datetime.datetime.fromordinal(ordinal).date())
    if mmdd:
        date_string = date_string[5:]
    return date_string


def get_data_from_db(focuses, worldwide, parent_level, parent_focus):
    """
    First we get a list of all the jhu_keys in the requested focus.
    Then we will get all the data for those location keys.
    :param focuses: a list of [country, admin1, admin2] describing the location
        we want to plot.  For instance, ['US', None, None], or
        [None, 'Arizona', None].
    :param worldwide: True if we are going for global statistics.
    :param parent_level:
    :param parent_focus:
    :return:
    """
    location, datum, conn = get_tables_and_connection()
    engine = get_engine()
    focus_texts = []
    base_query_text = """
    SELECT 
            datum.ordinal_date AS odate, 
            SUM(datum.confirmed) AS conf,
            SUM(datum.deaths) AS deaths
        FROM datum, location WHERE 
    """

    # Build the location's where based on the focuses
    if focuses[0] is not None:
        focus_texts.append('country = "{}"'.format(focuses[0]))
    if focuses[1] is not None:
        focus_texts.append('admin1 = "{}"'.format(focuses[1]))
    if focuses[2] is not None:
        focus_texts.append('admin2 = "{}"'.format(focuses[2]))

    if focus_texts:
        where_clause = ' AND '.join(focus_texts) + ' AND '
    else:
        where_clause = ''

    query_tail = """
        location.jhu_key = datum.location_jhu_key
    GROUP BY ordinal_date ORDER BY ordinal_date
    """
    query_text = base_query_text + where_clause + query_tail

    # The query result is a generator like object.  We will need to do
    # a few things that require a list.
    location_result = list(conn.execute(text(query_text)))

    # Now work on the parent where clause
    if parent_focus is None:
        # Nothing left to do.
        return location_result, None

    if parent_focus == 'Global':
        where_clause = ''
    elif parent_level == 'Country':
        where_clause = 'country = "{}"'.format(parent_focus)
    elif parent_level == 'State':
        where_clause = 'admin1 = "{}"'.format(parent_focus)
    else:
        sys.exit("Huh? Can't figure out parent")

    if where_clause:
        where_clause += ' AND '

    # Is is likely that the parent has more (earlier) data than the location.
    # Only get the parent data from the start of the location's data
    date_limit_text = "ordinal_date >= {} AND ".format(location_result[0][0])
    query_text = base_query_text + where_clause + \
                 date_limit_text + query_tail
    parent_result = list(conn.execute(text(query_text)))

    return location_result, parent_result


def main():
    args = parse_args()
    columns = define_columns()
    Day.set_columns(columns)

    worldwide, focuses = get_level_focus(args)

    parent_level, parent_focus = get_parent_level_focus(args)

    location_data, parent_data = get_data_from_db(focuses, worldwide,
                                                  parent_level, parent_focus)
    Day.set_data(location_data, parent_data)

    Day.compute_pcts_velocities_accelerations()
    Day.compute_rolling_average()

    # Use the finest grain focus that was specified
    focus = 'Global'
    for n in range(2, -1, -1):
        if focuses[n] is not None:
            focus = focuses[n]
            break
    plot_it(focus, parent_focus)


if __name__ == '__main__':
    main()
