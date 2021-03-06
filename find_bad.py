#! /usr/bin/env python3

import argparse
from csv import DictReader
import os
import sys

BASE_PATH = r'COVID-19\csse_covid_19_data\csse_covid_19_daily_reports'


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-k', '--key', default='Confirmed',
                        help="Which of Confirmed, Deaths, Active, Recovered to process [Confirmed]")
    parser.add_argument('-t', '--threshold', type=int, default=1000,
                        help="Differential needed to trigger a report [1000]")
    parser.add_argument('day1',
                        help="Earlier of two days (no default)")
    parser.add_argument('day2',
                        help="Later of two days (no default)")
    args = parser.parse_args()
    return args


def read_file(filename, column):
    """
    Extracts the specified column's data from every reporting jurisdiction for a day. Note that each
    jurisdiction in the file has a unique combined key; this is used later to combine data from multiple files.
    :param filename: Name of a daily file, not the full file path.
    :param column: Which column to process
    :return: A dict: {combined_key: column-datum,}
    """
    file_path = os.path.join(BASE_PATH, filename)

    result = {}
    with open(file_path) as f:
        reader = DictReader(f)
        for row in reader:
            result[row['Combined_Key']] = int(row[column])
    return result


def main():
    args = parse_args()
    day1 = read_file(args.day1, args.key)
    day2 = read_file(args.day2, args.key)
    print("Processing {}, with threshold {}".format(args.key, args.threshold))
    for k in day1.keys():
        try:
            diff = day2[k] - day1[k]
            if diff > args.threshold or diff < -args.threshold:
                print(k, diff, day1[k], day2[k])
        except KeyError:
            # A jurisdiction didn't exist in day2.  Report it and move on.
            print("Couldn't find combined key {} in {}.".format(k, args.day2))


if __name__ == '__main__':
    main()