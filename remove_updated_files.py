"""
Sometimes JHU updates data files, possibly going back several months. This
program determines what dates have been updated by reading the file
updated_files.txt, and removes the data from each of those days, preparing the
way for a reload of just the changed dates.  Note that it does nothing to the
last_date table.  The load_database.py program has a new option, --update,
which selectively loads based on whether there are any data for a date.

The file updated_files.txt should be created by copy/paste from a git pull
listing sent to the terminal.  It may, but need not, include the current /
most-recent date's line.
"""
import mariadb
import os

from file_handling import filename_to_ordinal_date
from mysql_credentials import username, password

db_conn = None
database = 'covid_data'


def get_ordinal_date_from_filename(line):
    """
    A git listing line looks like:
        .../csse_covid_19_daily_reports/08-31-2020.csv     |    2 +-
    from this line we will extract 08-31-2020, and then turn it into an
    ordinal date.
    :param line: a line from the updated_files.txt file.
    :return: The ordinal date for the file name.
    """
    path = line.strip().split()[0]
    filename = os.path.split(path)[1]
    ordinal_date = filename_to_ordinal_date(filename)
    print(filename, ordinal_date)
    return ordinal_date


def get_cursor():
    global db_conn
    if db_conn is None:
        db_conn = mariadb.connect(host='localhost',
                                  database='',
                                  user=username,
                                  password=password)
        cur = db_conn.cursor()
        cur.execute('USE {}'.format(database))
    return db_conn.cursor()


def delete_data_for_ordinal_date(ordinal_date):
    c = get_cursor()
    c.execute("""DELETE FROM datum WHERE ordinal_date=?""", (ordinal_date,))
    db_conn.commit()


def main():
    with open('updated_files.txt') as f:
        for line in f.readlines():
            ordinal_date = get_ordinal_date_from_filename(line)
            delete_data_for_ordinal_date(ordinal_date)


if __name__ == '__main__':
    main()