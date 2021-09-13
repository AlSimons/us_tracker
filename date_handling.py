import datetime
import sys


def string_date_to_ordinal_date(string_date, date_template):
    return datetime.datetime.strptime(string_date, date_template). \
        date().toordinal()


def bc_date_to_ordinal_date(string_date):
    date_template = '%Y-%m-%d'
    return string_date_to_ordinal_date(string_date, date_template)


def filename_to_ordinal_date(filename):
    date_template = '%m-%d-%Y'
    return string_date_to_ordinal_date(filename[:-4], date_template)


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
