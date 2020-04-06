#!/usr/bin/env python3


import argparse
import csv
import matplotlib.pyplot as plt
import os
import re
import statistics


class ColumnInfo:
    def __init__(self, header1, header2, field,
                 computed_type, depended_field,
                 depended_field2='', input_column='',):
        self.header1 = header1
        self.header2 = header2
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
                    setattr(day, column.field,
                            int(line_dict[column.input_column]) + prev)
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
    def compute_smoothed_acceleration():
        smooth_days = 10
        for column in Day.columns:
            if column.computed_type == 'sm_acc':
                Day.past_accelerations = []
                for day in Day.all_days:
                    try:
                        acc = getattr(day, column.depended_field)
                    except AttributeError:
                        acc = 0
                    Day.past_accelerations.append(
                        acc)
                    Day.past_accelerations = \
                        Day.past_accelerations[-smooth_days:]
                    setattr(day, column.field,
                            round(statistics.mean(Day.past_accelerations), 0))

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
    if (args.admin1 and args.admin2) or \
            (args.admin1 and args.admin3) or \
            (args.admin2 and args.admin3):
        parser.error("Only one of --admin1, --admin2, or --admin3 may be used")
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
    return sorted(filtered)[-22:]


def process_file(path, file, level, focus):
    date = file[:-4]
    with open(os.path.join(path, file)) as f:
        reader = csv.DictReader(f)
        fields = reader.fieldnames
        found = False
        if level is not None:
            # If we're not filtering (getting Global stats), don't need
            # to do this.
            for field in fields:
                if level in field:
                    level = field
                    found = True
                    break
            if not found:
                return

        for line in reader:
            # Filter, if we're filtering
            if level and line[level] != focus:
                continue
            Day.add_line(date, line)


def define_columns():
    columns_defs = [
        ColumnInfo("Confirmed", "Number", 'conf_num',
                   '', '', '', 'Confirmed'),
        ColumnInfo("Confirmed", "Velocity", 'conf_vel',
                   'vel', 'conf_num',),
        ColumnInfo("Confirmed", "Acceleration", 'conf_acc',
                   'acc', 'conf_vel',),
        ColumnInfo("Confirmed", "Smoothed Accel", 'conf_sm_acc',
                   'sm_acc', 'conf_acc',),

        ColumnInfo("Deaths", "Number", 'deaths_num',
                   '', '', '', 'Deaths',),
        ColumnInfo("Deaths", "Percent", 'deaths_pct',
                   'pct', 'deaths_num', 'conf_num',),
        ColumnInfo("Deaths", "Velocity", 'deaths_vel',
                   'vel', 'deaths_num',),
        ColumnInfo("Deaths", "Acceleration", 'deaths_acc',
                   'acc', 'deaths_vel',),
        ColumnInfo("Deaths", "Smoothed Accel", 'deaths_sm_acc',
                   'sm_acc', 'deaths_acc',),

        ColumnInfo("Recovered", "Number", 'recovered_num',
                   '', '', '', 'Recovered'),
        ColumnInfo("Recovered", "Percent", 'recovered_pct',
                   'pct', 'recovered_num', 'conf_num',),
        ColumnInfo("Recovered", "Velocity", 'recovered_vel',
                   'vel', 'recovered_num',),
        ColumnInfo("Recovered", "Acceleration", 'recovered_acc',
                   'acc', 'recovered_vel',),
        ColumnInfo("Recovered", "Smoothed Accel", 'recovered_sm_acc',
                   'sm_acc', 'recovered_acc'),

        ColumnInfo("Active", "Number", 'active_num',
                   '', '', '', 'Active'),
        ColumnInfo("Active", "Percent", 'active_pct',
                   'pct', 'active_num', 'conf_num',),
        ColumnInfo("Active", "Velocity", 'active_vel',
                   'vel', 'active_num',),
        ColumnInfo("Active", "Acceleration", 'active_acc',
                   'acc', 'active_vel',),
        ColumnInfo("Active", "Smoothed Accel", 'active_sm_acc',
                   'sm_acc', 'active_acc',),
    ]
    return columns_defs


def write_header(focus, column_defs, o):
    line1 = "{}\t" + "\t".join(x.header1 for x in column_defs)
    line2 = "Date\t" + "\t".join(x.header2 for x in column_defs)
    print(line1.format(focus),
          file=o)
    print(line2, file=o)


def write_it(focus, columns):
    found_one = False
    with open('output.txt', 'w') as o:
        write_header(focus, columns, o)
        for day in Day.all_days:
            conf = day.conf_num
            if conf == 0 and not found_one:
                continue
            found_one = True
            print(day, file=o)


def plot_it(focus, columns):
    dates = []
    conf_vel = []
    conf_num = []
    conf_accel = []
    deaths = []
    for day in Day.all_days[1:]:
        dates.append(day.date)
        conf_num.append(day.conf_num)
        conf_vel.append(day.conf_vel)
        conf_accel.append(day.conf_sm_acc)
        deaths.append(day.deaths_num)
    dates = [x[:-5] for x in dates]
    fig = plt.figure(figsize=(10, 9))
    plt.subplots_adjust(hspace=.3)
    plt.subplot(221)
    plt.title(focus + " Confirmed Cases")
    plt.xticks(rotation=90)
    plt.plot(dates, conf_num, 'g-')
    plt.subplot(223)
    plt.title(focus + " Deaths")
    plt.xticks(rotation=90)
    plt.plot(dates, deaths,  'r-')
    plt.subplot(222)
    plt.title(focus + " Daily New Cases")
    plt.xticks(rotation=90)
    plt.plot(dates, conf_vel,  'k-')
    plt.subplot(224)
    plt.title(focus + " New Case Delta")
    plt.xticks(rotation=90)
    plt.plot(dates, conf_accel,  'm-')
    plt.show()


def get_level_focus(args):
    focus = 'Global'
    level = None
    if args.admin1:
        # Summarize a country
        level = 'Country'
        focus = args.admin1
    elif args.admin2:
        # Summarize a state / province
        level = 'State'
        focus = args.admin2
    elif args.admin3:
        # Summarize a city / county
        level = 'Admin2'
        focus = args.admin3
    else:
        # Summarize global situation
        pass
    return level, focus


def main():
    args = parse_args()
    columns = define_columns()
    Day.set_columns(columns)

    from_dir = r'COVID-19\csse_covid_19_data\csse_covid_19_daily_reports'
    all_daily_files = get_files(from_dir)

    level, focus = get_level_focus(args)

    for f in all_daily_files:
        process_file(from_dir, f, level, focus)

    Day.compute_pcts_velocities_accelerations()
    Day.compute_smoothed_acceleration()

    plot_it(focus, columns)
    write_it(focus, columns)


if __name__ == '__main__':
    main()
