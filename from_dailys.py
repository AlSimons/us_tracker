#!/usr/bin/env python3


import argparse
import csv
import os
import re
import statistics

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-1', '--admin1',
                        help="Specify admin-1 level group (e.g., country)")
    parser.add_argument('-2', '--admin2',
                        help="Specify admin-2 level group (e.g., state / province)")
    parser.add_argument('-3', '--admin3',
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
    return sorted(filtered)


def process_file(path, file, level, focus):
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
                return {'Day': file,
                        'Confirmed': 0,
                        'Deaths': 0,
                        'Recovered': 0,
                        'Active': 0}
        confirmed = 0
        deaths = 0
        recovered = 0
        active = 0
        for line in reader:
            # Filter, if we're filtering
            if level and line[level] != focus:
                continue
            try:
                confirmed += int(line['Confirmed'])
            except:
                pass
            try:
                deaths += int(line['Deaths'])
            except:
                pass
            try:
                recovered += int(line['Recovered'])
            except:
                pass
            try:
                active += int(line['Active'])
            except:
                pass

    return {'Day': file,
            'Confirmed': confirmed,
            'Deaths': deaths,
            'Recovered': recovered,
            'Active': active}

def compute_trajectory(days):
    days[0]['Velocity'] = 0
    days[0]['Acceleration'] = 0
    for n in range(1, len(days)):
        days[n]['Velocity'] = days[n]['Confirmed'] - days[n-1]['Confirmed']
        days[n]['Acceleration'] = days[n]['Velocity'] - days[n-1]['Velocity']

    # Now the smoothed acceleration, to remove some of the jitter.
    five_days = []
    for n in range(len(days)):
        five_days.append(days[n]['Acceleration'])
        five_days = five_days[-5:]
        days[n]['Smooth Acceleration'] = statistics.mean(five_days)


def write_header(focus, o):
    print('{}\tConfirmed\tConfirmed\tConfirmed\tConfirmed\tDeaths\t\tRecovered\t\tActive\t'.format(focus),
          file=o)
    print('Date\tNumber\tVelocity\tAcceleration\tSmooth Acc\tNumber\tPercent\tNumber\tPercent\tNumber\tPercent',
          file=o)


def write_it(focus, days):
    with open('output.txt', 'w') as o:
        write_header(focus, o)
        for day in days:
            conf = day['Confirmed']
            velocity = day['Velocity']
            acceleration = day['Acceleration']
            smooth_acceleration = round(day['Smooth Acceleration'], 0)
            deaths = day['Deaths']
            recovered = day['Recovered']
            active = day['Active']
            try:
                deaths_pct = round(deaths / conf, 4)
                recovered_pct = round(recovered / conf, 4)
                active_pct = round(active / conf, 4)
            except ZeroDivisionError:
                deaths_pct = ''
                recovered_pct = ''
                active_pct = ''
            print('{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}'.format(
                day['Day'][:-4],
                conf,
                velocity,
                acceleration,
                smooth_acceleration,
                deaths,
                deaths_pct,
                recovered,
                recovered_pct,
                active,
                active_pct
            ), file=o)


def main():
    args = parse_args()
    from_dir = r'COVID-19\csse_covid_19_data\csse_covid_19_daily_reports'
    all_daily_files = get_files(from_dir)
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

    days = []
    for f in all_daily_files:
        days.append(process_file(from_dir, f, level, focus))

    compute_trajectory(days)

    write_it(focus, days)


if __name__ == '__main__':
    main()