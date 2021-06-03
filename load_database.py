#! /usr/bin/env python3

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql import text
from database_schema import Base, Location, Datum, LastDate, Loaded
from file_handling import get_files, filename_to_ordinal_date, \
    ordinal_date_to_string
from state_abbreviations import state_abbreviations
from timer import Timer

# Note: NEVER put this file into git!
from mysql_credentials import username, password

import argparse
import csv
from datetime import datetime
import os
import sys


DATABASE_NAME = 'mysql+mysqlconnector://{}:{}@localhost/covid_data'
JHU_DATA_DIRECTORY = r'..\COVID-19\csse_covid_19_data\csse_covid_19_daily_reports'


class LineDatum(object):
    def __init__(self, line):
        # Sometimes these column values are blank. Use zero.
        try:
            self.confirmed = int(line['Confirmed'])
        except ValueError:
            self.confirmed = 0
        try:
            # Arrgh.  Consistency, folks.  10-28-20 has Deaths as a float. I
            # guess part of a person died somewhere.
            self.deaths = int(float(line['Deaths']))
        except ValueError:
            self.deaths = 0
        try:
            self.recovered = int(line['Recovered'])
        except ValueError:
            self.recovered = 0
        try:
            self.active = int(line['Active'])
        except (ValueError, KeyError):
            self.active = 0
        try:
            self.incidence_rate = float(line['Incidence_rate'])
        except(ValueError, KeyError):
            self.incidence_rate = 0.0
        try:
            self.case_fatality_ratio = round(
                float(line['Case-Fatality_Ratio']), 4)
        except (ValueError, KeyError):
            self.case_fatality_ratio = 0.0


class DbDatum(object):
    all_data = {}

    def __init__(self, dbrec):
        self.id = dbrec[0]
        self.ordinal_date = dbrec[1]
        self.location_jhu_key = dbrec[2]
        self.confirmed = dbrec[3]
        self.deaths = dbrec[4]
        self.recovered = dbrec[5]
        self.active = dbrec[6]
        self.incidence_rate = dbrec[7]
        self.case_fatality_ratio = dbrec[8]

        # JHU is not consistent with location capitalization, e.g.,
        # Desoto, Florida versus DeSota, Florida.
        DbDatum.all_data[self.location_jhu_key.upper()] = self

    @staticmethod
    def clear():
        DbDatum.all_data = {}


def get_all_db_records_for_day(session, ordinal_date):
    query = text("""SELECT * FROM datum WHERE ordinal_date = :od""")
    records = session.bind.execute(query, od=ordinal_date).fetchall()

    for rec in records:
        DbDatum(rec)

    data = DbDatum.all_data
    DbDatum.clear()
    return data


args = None


def parse_args():
    global args
    parser = argparse.ArgumentParser()
    parser.add_argument('--load-missing-files', action='store_true',
                        help="Refill JHU data that have been removed due to "
                             "an update.")
    parser.add_argument('-r', '--refresh', action='store_true',
                        help="Delete and reload data for dates that have "
                             "updated data.")

    args = parser.parse_args()


def datum_from_line(line, location, ordinal_date):
    """
    Create a datum from a line of file data.
    :param line: A dict representing a line of data from the file
    :param location: the jhu_key for the line being processed.
    :param ordinal_date: The ordinal date for the file
    :return: a new database Datum object
    """
    d = Datum()
    line_datum = LineDatum(line)

    d.ordinal_date = ordinal_date
    d.location = location

    d.active = line_datum.active
    d.case_fatality_ratio = line_datum.case_fatality_ratio
    d.confirmed = line_datum.confirmed
    d.deaths = line_datum.deaths
    d.incidence_rate = line_datum.incidence_rate
    d.recovered = line_datum.recovered

    return d


def process_line(session, levels, line, ordinal_date):
    """
    Insert a new datum from one line of a file.
    :param session: The SQLalchemy session
    :param levels: The specified location levels
    :param line: a dict representing a line of data from the file
    :param ordinal_date: The ordinal date of the file
    :return: None
    :side_effect: The database is updated with new data.
    """
    # First get the location record.
    try:
        location = get_location(session, levels, line)
    except IndexError as e:
        print("Exception encountered in processing input line.", file=sys.stderr)
        print("The line was\n   ", line, file=sys.stderr)
        print("The exception was\n   ", e, file=sys.stderr)
        # Skip further processing of this line
        return

    # Now get the rest of the information from the line.

    d = datum_from_line(line, location, ordinal_date)
    location.data.append(d)

    # Commit is performed after each file is processed, not per-line.


def update_latest_ordinal_date(session, ordinal_date):
    """
    We don't want to reprocess a file that has already been loaded, so as
    each file is completed we record the date of the just completed file.
    When we're scanning the directory for files to load, we skip any before
    or on this date.
    :param session: The sqlalchemy orm session.
    :param ordinal_date: The ordinal date of the just completed file
    :return: None
    """
    try:
        # This should succeed except for the very first time we process a file.
        last_date = session.query(LastDate).filter(LastDate.ordinal_date).one()
        last_date.ordinal_date = ordinal_date
        last_date.date_string = ordinal_date_to_string(ordinal_date)
    except NoResultFound:
        last_date = LastDate()
        last_date.ordinal_date = ordinal_date
        last_date.date_string = ordinal_date_to_string(ordinal_date)
        session.add(last_date)

    # No commit().  Only at the end of processing a file.


def fix_admin1(admin1):
    """
    Early files didn't have an admin2 field.  When JHU wanted to go to a finer
    granularity than the US state, they combined the admin2 and state into
    the state column, for instance, "Boston, MA".  This routine is used to
    split those elements out.  The state was almost always abbreviated, so
    we also expand the state to the full spelling to match the representation in
    later files.
    :param admin1:
    :return: admin2, (expanded) admin1
    """
    admin2, abbreviated_admin1 = [x.strip() for x in admin1.split(',')]

    if abbreviated_admin1 in state_abbreviations:
        return admin2, state_abbreviations[abbreviated_admin1]

    if '(' in abbreviated_admin1:
        abbreviated_admin1, parenthetical = [x.strip() for x in
                                             abbreviated_admin1.split('(')]
        if abbreviated_admin1 in state_abbreviations:
            return admin2, state_abbreviations[abbreviated_admin1] + \
                ' (' + parenthetical

    # A real oddball in 3-14 and 3-15-2020.
    if admin2 == 'Virgin Islands' and abbreviated_admin1 == 'U.S.':
        return None, admin2

    # Known instances that are fine, so don't warn.
    known_non_state_fixups = [
        'Ascension and Tristan da Cunha',
        'Sint Eustatius and Saba',
    ]

    if abbreviated_admin1 not in known_non_state_fixups:
        print("Not found:", admin2, "::", abbreviated_admin1)

    return None, admin1


def fix_jhu_key(jhu_key):
    # Some JHU Combined Keys are built wrong, and have leading commas.
    # Get rid of them.
    while jhu_key[0] == ',':
        jhu_key = jhu_key[1:]
    return jhu_key


def get_location(session, levels, line, just_jhu_key=False):
    country = line[levels[0]]
    admin1 = line[levels[1]]
    if levels[2] is not None:
        admin2 = line[levels[2]]
    else:
        admin2 = None

    if not admin2 and ',' in admin1:
        admin2, admin1 = fix_admin1(admin1)

    if 'Combined_Key' in line:
        jhu_key = line['Combined_Key']
        if jhu_key[1] == ',':
            jhu_key = fix_jhu_key(jhu_key)
    else:
        # Have to construct it from Admin2, State, and Country
        # if they exist and are non-null. Also change empty strings to
        # None, to get nulls in the DB
        if not admin1:
            # Not admin1 implies not admin2
            jhu_key = country
            admin1 = None
            admin2 = None
        elif not admin2:
            jhu_key = ', '.join([admin1, country])
            admin2 = None
        else:
            jhu_key = ', '.join([admin2, admin1, country])

    if just_jhu_key:
        return jhu_key

    # Change empty strings to None, to get null in DB
    if admin1 == '':
        admin1 = None
    if admin2 == '':
        admin2 = None

    # Do query to try to get location record here.

    try:
        location = session.query(Location).\
            filter(Location.jhu_key == jhu_key).one()
        return location

    except NoResultFound:
        # Continue processing this routine.
        pass

    # This location was not in the database.  Need to add it.
    location = Location()
    location.country = country
    location.admin1 = admin1
    location.admin2 = admin2
    location.jhu_key = jhu_key
    # Don't try to insert a null string FIPS. sqlite takes it; mysql doesn't.
    if 'FIPS' in line and line['FIPS']:
        try:
            location.fips = int(line['FIPS'])
        except ValueError:
            # If the int conversion didn't work, simply skip it.
            pass
    session.add(location)
    #
    # Note: no commit().  We only commit after completing a daily report file.
    #
    return location


def fix_inconsistent_levels(fields):
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
            # Need to find Country and State. Admin2 is optional.
            # Very early files don't have it.
            if n != 2:
                raise ValueError(
                    "Couldn't find required location column header {} in {}".
                        format(levels[n], fields))
            else:
                levels[n] = None  # No Admin2 this file.
    return levels


def process_one_jhu_file(session, filename, ordinal_date):
    filepath = os.path.join(JHU_DATA_DIRECTORY, filename)
    with open(filepath) as f:
        reader = csv.DictReader(f)
        fields = reader.fieldnames

        levels = fix_inconsistent_levels(fields)

        for line in reader:
            process_line(session, levels, line, ordinal_date)

    # After the file is completed, update the latest ordinal_date_processed.
    update_latest_ordinal_date(session, ordinal_date)
    record_file_mtime(session, filename)

    # Commit all the changes from this file.  Avoids partial loads if the
    # program is interrupted during a file.
    session.commit()


def initial_load_jhu_files(session):
    # Get the files to process
    files = get_files(JHU_DATA_DIRECTORY)

    # Get the last date already processed.  Don't want to re-do any data.
    try:
        last_ordinal_date_processed = session.query(LastDate).one().ordinal_date
    except NoResultFound:
        # Nothing in the DB so far means we haven't processed any files.
        last_ordinal_date_processed = 0

    # For each file get the associated date.
    for filename in files:
        ordinal_date = filename_to_ordinal_date(filename)
        # Have we already processed this one?
        if ordinal_date <= last_ordinal_date_processed:
            if not args.load_missing_files:
                continue
            if not date_is_missing_data(session, ordinal_date):
                continue

        # The [:-7] on the datetime.now() strips off the microseconds
        print("Processing", ordinal_date_to_string(ordinal_date),
              str(datetime.now())[:-7])
        # clean_partial_load(ordinal_date)
        process_one_jhu_file(session,
                             filename,
                             ordinal_date)
    print(str(datetime.now())[:-7])


def create_database(engine):
    Base.metadata.create_all(engine)


def needs_refreshing(session, filename):
    """
    We track the modification dates of all the loaded files. This routine
    checks the filetime (mtime saved in the database), and compares it to the
    mtime of the file currently on disk. If the disk time is newer, return
    True to indicate that the file should be reloaded.
    :param session: The SQLalchemy session
    :param filename: The file we are processing
    :return: True if the file is newer than the database entries.
    """
    filepath = os.path.join(JHU_DATA_DIRECTORY, filename)
    # Get the time of the file currently on disk.
    current_mtime = os.path.getmtime(filepath)
    string_current_mtime = f'{current_mtime}'

    # May already be recorded (_is_ recorded, unless we had a bug somewhere)
    try:
        loaded = session.query(Loaded). \
            filter(Loaded.filename == filename).one()
    except NoResultFound:
        # Can't find a record for this file.  Reload it.
        return True

    if loaded.filetime > string_current_mtime:
        # This "can't" happen.  Bomb the program.
        raise ValueError

    return loaded.filetime < string_current_mtime


def refresh_files(session):
    """
    JHU frequently changes a line or two in old files, to correct existing
    data.  Check for files which have been modified since initially loaded,
    and updates those.
    :param session: The SQLalchemy session
    :return: None
    :side_effect: The database MAY BE updated.
    """
    files = get_files(JHU_DATA_DIRECTORY)
    any_refreshed = False
    for filename in files:
        if not needs_refreshing(session, filename):
            continue
        any_refreshed = True
        refresh_lines(session, filename)
        record_file_mtime(session, filename)
        # Commit the changes from this file.
        session.commit()
    if not any_refreshed:
        print("All files were up to date.")


def refresh_lines(session, filename):
    """
    We have a file that has been replaced / updated since it was loaded.
    Process the file line by line, check each line's data against that in the
    database, and update only those lines that have changed.
    :param session: The SQLalchemy session.
    :param filename: The file to process.
    :return: None
    :side_effect: the database will be updated.
    """
    ordinal_date = filename_to_ordinal_date(filename)
    filepath = os.path.join(JHU_DATA_DIRECTORY, filename)

    db_data = get_all_db_records_for_day(session, ordinal_date)

    with open(filepath) as f:
        reader = csv.DictReader(f)
        fields = reader.fieldnames

        levels = fix_inconsistent_levels(fields)

        for line in reader:
            # Using the ORM appears to make this conceptually simple task of
            # comparing a record in the DB to the corresponding record in the
            # file inordinately hard.  Just doing it in straight SQL.

            location = get_location(session, levels, line, just_jhu_key=True)
            new_datum = LineDatum(line)
            try:
                stored_datum = db_data[location.upper()]
            except KeyError:
                continue

            # Collect all the messages for this line.
            msg = []
            if new_datum.active != stored_datum.active:
                msg.append("Act: {} > {}".format(
                    stored_datum.active, new_datum.active))
                stored_datum.active = new_datum.active
            if new_datum.case_fatality_ratio != stored_datum.case_fatality_ratio:
                msg.append("CFR: {} > {}".format(
                    stored_datum.case_fatality_ratio,
                    new_datum.case_fatality_ratio))
                stored_datum.case_fatality_ratio = new_datum.case_fatality_ratio
            if new_datum.confirmed != stored_datum.confirmed:
                msg.append("Conf: {} > {}".format(
                    stored_datum.confirmed, new_datum.confirmed))
                stored_datum.confirmed = new_datum.confirmed
            if new_datum.deaths != stored_datum.deaths:
                msg.append("Dths: {} > {}".format(
                    stored_datum.deaths, new_datum.deaths))
                stored_datum.deaths = new_datum.deaths
            if new_datum.incidence_rate != stored_datum.incidence_rate:
                msg.append("Inci: {} > {}".format(
                    stored_datum.incidence_rate, new_datum.incidence_rate))
                stored_datum.incidence_rate = new_datum.incidence_rate
            if new_datum.recovered != stored_datum.recovered:
                msg.append("Rec: {} > {}".format(
                    stored_datum.recovered, new_datum.recovered))
                stored_datum.recovered = new_datum.recovered
            if msg:
                print(filename, location, "; ".join(msg))
                update = text("""UPDATE datum
                                 SET active=:act,
                                     case_fatality_ratio=:cfr,
                                     confirmed=:conf,
                                     deaths=:dead,
                                     incidence_rate=:inc,
                                     recovered=:rec
                                 WHERE id=:id"""
                              )
                params = {"id": stored_datum.id,
                          "act": stored_datum.active,
                          "cfr": stored_datum.case_fatality_ratio,
                          "conf": stored_datum.confirmed,
                          "dead": stored_datum.deaths,
                          "inc": stored_datum.incidence_rate,
                          "rec": stored_datum.recovered
                         }
                session.bind.execute(update, params)


def record_file_mtime(session, filename):
    """
    We want to track the modification times (mtimes) of the files that are
    loaded so that we can tell when a file has been updated and needs to be
    reloaded.
    :param session: The SQLalchemy session
    :param filename: The file we are processing
    :return: None.
    :side_effect: The database is updated with the specified files mtime.
    """
    filepath = os.path.join(JHU_DATA_DIRECTORY, filename)
    # Get the time of the file currently on disk.
    current_mtime = os.path.getmtime(filepath)
    string_current_mtime = f'{current_mtime}'

    if needs_refreshing(session, filename):
        # May already be recorded (unless this is the initial load)
        try:
            loaded = session.query(Loaded). \
                filter(Loaded.filename == filename).one()
            # We know we need to update the time, because needs_loading said so.
            loaded.filetime = string_current_mtime
            session.add(loaded)
            session.commit()
        except NoResultFound:
            # Either initial load, or for some reason the record was missing.
            loaded = Loaded()
            loaded.filename = filename
            loaded.filetime = string_current_mtime

        session.add(loaded)
        session.commit()


def main():
    timer = Timer().start()

    parse_args()
    engine = create_engine(DATABASE_NAME.format(username, password), echo=False)
    create_database(engine)

    Session = sessionmaker(bind=engine)
    session = Session()

    initial_load_jhu_files(session)

    refresh_files(session)

    timer.stop()
    print("Entire database load took:", timer)


if __name__ == '__main__':
    main()
