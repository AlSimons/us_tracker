import re
import os
import datetime
import sys


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
    return sorted(filtered, key=filename_to_ordinal_date)


def filename_to_ordinal_date(filename):
    date_template = '%m-%d-%Y'
    return datetime.datetime.strptime(filename[:-4], date_template).\
        date().toordinal()


def ordinal_date_to_string(ordinal):
    return str(datetime.datetime.fromordinal(ordinal).date())


def main():
    if sys.argv[1] == 'from':
        for ord_date in sys.argv[2:]:
            print(ord_date, ordinal_date_to_string(int(ord_date)))
    elif sys.argv[1] == 'to':
        for date_string in sys.argv[2:]:
            print(date_string, "is",
                  filename_to_ordinal_date(date_string + ".csv"))
    else:
        print("First arg must be either 'from' or 'to'", file=sys.stderr)


if __name__ == '__main__':
    main()
