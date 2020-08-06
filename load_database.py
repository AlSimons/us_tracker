from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.orm.exc import NoResultFound
from database_schema import Base, Location, Datum, LastDate
from file_handling import get_files, filename_to_ordinal_date, \
    ordinal_date_to_string
from state_abbreviations import state_abbreviations

from mysql_credentials import username, password
from datetime import datetime

import csv
from datetime import datetime
import os
import sys


DATABASE_NAME = 'mysql+mysqlconnector://{}:{}@localhost/covid_data'
JHU_DATA_DIRECTORY = r'..\COVID-19\csse_covid_19_data\csse_covid_19_daily_reports'


def process_line(session, levels, line, ordinal_date):

    # First get the location record.
    location = get_location(session, levels, line)

    # Now get the rest of the information from the line.
    # We know that this one will be unique, so construct the record from
    # scratch.
    d = Datum()
    d.ordinal_date = ordinal_date
    # Sometimes these column values are blank. Use zero.
    try:
        d.confirmed = int(line['Confirmed'])
    except ValueError:
        d.confirmed = 0
    try:
        d.deaths = int(line['Deaths'])
    except ValueError:
        d.deaths = 0
    try:
        d.recovered = int(line['Recovered'])
    except ValueError:
        d.recovered = 0
    try:
        d.active = int(line['Active'])
    except (ValueError, KeyError):
        d.active = 0
    try:
        d.incidence_rate = float(line['Incidence_rate'])
    except(ValueError, KeyError):
        d.incidence_rate = 0.0
    try:
        d.case_fatality_ration = line['Case-Fatality_Ratio']
    except (ValueError, KeyError):
        d.case_fatality_ration = 0.0

    location.data.append(d)

    session.commit()


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
    session.commit()


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

    # A real oddball in 3-14 and 3-15.
    if admin2 == 'Virgin Islands' and abbreviated_admin1 == 'U.S.':
        return None, admin2

    # A known instance that is fine, so don't warn about it.
    if abbreviated_admin1 != 'Sint Eustatius and Saba':
        print("Not found:", abbreviated_admin1)

    return None, admin1


def fix_jhu_key(jhu_key):
    # Some JHU Combined Keys are built wrong, and have leading commas.
    # Get rid of them.
    while jhu_key[1] == ',':
        jhu_key = jhu_key[1:]
    return jhu_key


def get_location(session, levels, line):
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
    session.commit()
    return location


def process_one_jhu_file(session, filepath, ordinal_date):
    with open(filepath) as f:
        reader = csv.DictReader(f)
        fields = reader.fieldnames
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
                        "Couldn't find required location column header {} in {}".\
                            format(levels[n], fields))
                else:
                    levels[n] = None  # No Admin2 this file.

        for line in reader:
            process_line(session, levels, line, ordinal_date)

    # After the file is completed, update the latest ordinal_date_processed.
    update_latest_ordinal_date(session, ordinal_date)


def read_jhu_files(session):
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
            continue

        print("Processing", ordinal_date_to_string(ordinal_date), datetime.now())
        process_one_jhu_file(session,
                             os.path.join(JHU_DATA_DIRECTORY, filename),
                             ordinal_date)
    print(datetime.now())


def create_database(engine):
    Base.metadata.create_all(engine)


def main():
    engine = create_engine(DATABASE_NAME.format(username, password), echo=False)
    create_database(engine)

    Session = sessionmaker(bind=engine)
    session = Session()

    read_jhu_files(session)


if __name__ == '__main__':
    main()