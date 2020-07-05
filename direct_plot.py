#!/usr/bin/env python3


import argparse
import csv
import matplotlib.pyplot as plt
import os
import re
import statistics
import sys

# Number of days to include in the X days rolling average plots
ROLLING_AVERAGE_DAYS = 7

# Max days to display in the plots
DISPLAY_DAYS = 120

# Compare current death count to confirmed cases X days ago to compute death
# rate percentage, since death usually lags diagnosis. Most important for areas
# with rapidly increasing case loads, which would otherwise show an artificially
# low death rate.
DEATHS_LAG = 10

DATA_DIRECTORY = r'COVID-19\csse_covid_19_data\csse_covid_19_daily_reports'
POPULATION_FILE = r'us_tracker\populations.csv'


class ColumnInfo:
    def __init__(self, field,
                 computed_type, depended_field,
                 depended_field2='', input_column='',):
        self.field = field
        self.computed_type = computed_type
        self.depended_field = depended_field
        self.depended_field2 = depended_field2
        self.input_column = input_column


class Day:
    columns = None
    all_days = []
    day_hash = {}
    past_accelerations = []

    def __init__(self, date):
        self.date = date
        Day.all_days.append(self)
        Day.day_hash[date] = self
        for column in Day.columns:
            setattr(self, column.field, 0)

    @staticmethod
    def add_line(date, line_dict):
        if date not in Day.day_hash.keys():
            Day(date)
        day = Day.day_hash[date]
        for column in Day.columns:
            if column.input_column and \
                    column.input_column in line_dict.keys() and \
                    line_dict[column.input_column]:
                prev = getattr(day, column.field)
                try:
                    # For some strange reason, JHU are recording some statistics
                    # as floats with "nnn.0" Therefore have to do int(float())
                    # instead of simply int(). Sigh.
                    setattr(day, column.field,
                            int(float(line_dict[column.input_column])) + prev)
                except Exception as e:
                    print("Failed for field {}: {}\n    Line was: {}".format(
                        column.input_column, e, line_dict))

    @staticmethod
    def set_columns(_columns):
        Day.columns = _columns

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
                                Day.past_accelerations[-ROLLING_AVERAGE_DAYS:]), 2))

    def __str__(self):
        out = '{}\t'.format(self.date)
        for column in Day.columns:
            out += str(getattr(self, column.field)) + '\t'
        return out[:-1]


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
    args = parser.parse_args()
    parser.add_argument('--parent-1',
                        help="Country to use for percentage computation")
    parser.add_argument('--parent-2',
                        help="State to use for percentage computation")
    return args


def get_files(from_dir):
    """
    From the list of all files in the given directory, find the ones that
    match mm-dd-yyyy.csv, and sort them by date. (os.listdir() returns in
    arbitrary order.

    TODO: create a real sort, which changes mm-dd-yyyy into yyyymmdd to perform
    the sort!  It'll work for now, because the year hasn't yet changed.
    """
    all_files = os.listdir(from_dir)
    filtered = []
    for f in all_files:
        if re.match(r'\d{2}-\d{2}-\d{4}\.csv', f):
            filtered.append(f)
    # The limiting expression below makes sure that if the data are available
    # we can do the smoothing of acceleration for even the first displayed day.
    return sorted(filtered)[-(ROLLING_AVERAGE_DAYS + DISPLAY_DAYS):]


def get_population(focus):
    with open(POPULATION_FILE) as f:
        for line in f:
            parts = line.strip().split(',')
            if parts[0] == focus:
                return int(parts[1])
    # Use zero as the "no data" flag, since no entity will have a population
    # of zero
    return 0


def process_file(path, file, focuses, worldwide):
    date = file[:-4]
    with open(os.path.join(path, file)) as f:
        reader = csv.DictReader(f)
        fields = reader.fieldnames
        if not worldwide:
            # If we're not filtering (getting Global stats), don't need
            # to do this.

            # The column headers in the daily report files are not completely
            # uniform, but close enough that we can figure out which ones to
            # use.  Initially, levels is ['Country', 'State', 'Admin2']. We
            # fix it here.
            levels = ['Country', 'State', 'Admin2']
            for n in range(len(levels)):
                found = False
                for field in fields:
                    if levels[n] in field:
                        levels[n] = field
                        found = True
                        break
                if not found:
                    # Need to find everything specified
                    return

        for line in reader:
            # First, gather the collective / parent information.  We're only
            # collecting case data.

            # Filter, if we're filtering
            need_line = True
            if not worldwide:
                for n in range(len(levels)):
                    if focuses[n] is not None and line[levels[n]] != focuses[n]:
                        need_line = False
                        break
            if need_line:
                Day.add_line(date, line)


def define_columns():
    columns_defs = [
        ColumnInfo('conf_num', '', '', '', 'Confirmed'),
        ColumnInfo('daily_conf', 'vel', 'conf_num',),
        ColumnInfo('avg_daily_conf', 'rolling_average', 'daily_conf',),
        ColumnInfo('deaths_num', '', '', '', 'Deaths',),
        ColumnInfo('daily_deaths', 'vel', 'deaths_num',),
        ColumnInfo('avg_daily_deaths', 'rolling_average', 'daily_deaths',),
    ]
    return columns_defs


def one_plot(dates, values, focus, position, title, color):
    plt.subplot(position)
    plt.title(focus + " " + title)
    plt.xticks(rotation=90)
    plt.plot(dates, values, color + '-')


def plot_it(focus):
    dates = []
    conf_vel = []
    conf_num = []
    conf_accel = []
    deaths = []
    deaths_vel = []
    deaths_acc = []
    for day in Day.all_days[-DISPLAY_DAYS:]:
        dates.append(day.date)
        conf_num.append(day.conf_num)
        conf_vel.append(day.daily_conf)
        conf_accel.append(day.avg_daily_conf)
        deaths.append(day.deaths_num)
        deaths_vel.append(day.daily_deaths)
        deaths_acc.append(day.avg_daily_deaths)
    dates = [x[:-5] for x in dates]

    focus_population = get_population(focus)

    plt.figure(figsize=(15, 9), num='{} COVID-19 History ({} days)'.
               format(focus, len(dates)))
    plt.subplots_adjust(hspace=.27, wspace=.16,
                        top=.97, bottom=.07,
                        left=.06, right=.97)

    if focus_population > 0:
        confirmed_percentage = " {}%".\
            format(round(100 * conf_num[-1] / focus_population, 2))
    else:
        confirmed_percentage = ""

    one_plot(dates, conf_num, focus, 231, "{:,} Confirmed Cases {}".
             format(conf_num[-1], confirmed_percentage), 'g')
    one_plot(dates, conf_vel, focus, 232, "{:,} Daily New Cases +{}%".
             format(conf_vel[-1], round(100 * conf_vel[-1] / conf_num[-2], 2)),
             'y')
    one_plot(dates, conf_accel, focus, 233,
             "{} Day Rolling Average Daily New Cases".
             format(min(len(dates), ROLLING_AVERAGE_DAYS)), 'k')
    try:
        one_plot(dates, deaths, focus, 234, "{:,} Deaths Cumulative: {}%".
                 format(deaths[-1],
                        round((deaths[-1] /
                               conf_num[-(DEATHS_LAG + 1)]) * 100, 1)),
                 'r')
    except IndexError:
        print("No data found for {}".format(focus))
        sys.exit()
    one_plot(dates, deaths_vel, focus, 235, "{:,} Daily New Deaths +{}%".
             format(deaths_vel[-1],
                    round(100 * deaths_vel[-1] / deaths[-2], 2)), 'b')
    one_plot(dates, deaths_acc, focus, 236,
             "{} Day Rolling Average Daily Deaths".
             format(min(len(dates), ROLLING_AVERAGE_DAYS)), 'k')
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
    parents = [None, None]
    parent_worldwide = True
    if args.parent_1:
        # Summarize a country
        parents[0] = args.parent_1
        parent_worldwide = False
    if args.parent_2:
        # Summarize a state / province
        parents[1] = args.parent_2
        parent_worldwide = False
    return parent_worldwide, parents


def main():
    args = parse_args()
    columns = define_columns()
    Day.set_columns(columns)

    all_daily_files = get_files(DATA_DIRECTORY)

    worldwide, focuses = get_level_focus(args)

    for f in all_daily_files:
        process_file(DATA_DIRECTORY, f, focuses, worldwide)

    Day.compute_pcts_velocities_accelerations()
    Day.compute_rolling_average()

    focus = 'Global'
    for n in range(2, -1, -1):
        if focuses[n] is not None:
            focus = focuses[n]
            break
    # Use the finest grain focus that was specified
    plot_it(focus)


if __name__ == '__main__':
    main()
