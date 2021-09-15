import csv
import io
import requests
from date_handling import bc_date_to_ordinal_date
from sqlalchemy.orm.exc import NoResultFound
from database_schema import Base, Location, Datum, LastDate, Loaded


def load_bc_data(session):
    """
    JHU doesn't gather regional data for BC.
    BC makes a CSV file available daily with only case data.
    It has new cases daily, not cumulative cases. It also has "smoothed"
    cases, which are 7 day running averages, which we will ignore.
    Since there are only new case counts, we'll build the running totals.

    BC data are categorized by HA (Health Authority) of which there are 5,
    and within HAs by HSDA (Health Service Delivery Area).

    We will use Country Canada, Admin 1 British Columbia, and Admin 2 the
    HSDA.  Each HA also has an "All" pseudo-HSDA, for which we'll use Admin 2
    "<HA>-All

    The file is currently (9/11/2021) in date order.  We assume that it will
    remain so; the program depends on it.

    :return: None
    :side effects: The database is updated.
    """
    # Yeah,longer than 68 characters.  So shoot me.
    url = 'http://www.bccdc.ca/Health-Info-Site/Documents/BCCDC_COVID19_Regional_Summary_Data.csv'

    r = requests.get(url)
    buff = io.StringIO(r.text)
    reader = csv.DictReader(buff)
    for row in reader:
        process_bc_row(session, row)

    # We're done with the BC regional data.  Commit.
    session.commit()


total_counts = {}


def process_bc_row(session, row):
    """
    Handle one line from the BC regional summary data web download.
    :param session: an sqlalchemy session
    :param row: An OrderedDict of one HSDA for one day.
        Keys:
         - Date (M/D/YYYY)
         - Province Ignored (always "BC")
         - HA Ignored except for HSDA "All", in which case we use it to
           create a pseudo HSDA of "<HA>-All".
         - HSDA
         - Cases_Reported Count of new cases since last row.
         - Cases_Reported_Smoothed Ignored
    :return: None
    :side_effects: Database is updated.
    """
    date = row['Date']
    ha = row['HA']
    hsda = row['HSDA']
    new_count = int(row['Cases_Reported'])
    ordinal_date = bc_date_to_ordinal_date(date)

    if hsda == 'All':
        # Special case: there is a row in each date's data with HA All and
        # HSDA All.  Completely skip it.
        if ha == 'All':
            return

        hsda = ha + '-All'

    # We have to build our cumulative counts from ero, so do this for every
    # record.
    try:
        current_total = total_counts[hsda]
    except KeyError:
        current_total = 0
    total_counts[hsda] = current_total + new_count

    # However, only enter records into the DB if they are not already there.
    # We'll try to retrieve the record and just return if it is found.
    # If it is not found, we'll insert it.

    # Get our location record.
    location = get_bc_location_record(session, hsda)

    try:
        result = session.query(Datum).filter(
            Datum.location_jhu_key == location.jhu_key).filter(
            Datum.ordinal_date == ordinal_date).one()

        # If we get here, the record is already in the DB. Simply return.
        return
    except NoResultFound:
        pass

    # Now we have to insert it.
    datum = Datum()
    datum.ordinal_date = ordinal_date
    datum.confirmed = total_counts[hsda]
    datum.location = location
    datum.location_jhu_key = location.jhu_key
    datum.active = 0
    datum.deaths = 0
    datum.recovered = 0
    datum.incidence_rate = 0.0
    datum.case_fatality_ratio = 0.0
    session.add(datum)


def get_bc_location_record(session, hsda):
    """
    Get the location record for a BC HSDA.

    We overload the "JHU key", to be a similar key with BC HSDA names,
    even though the data don't come from JHU.

    :param session: An sqlalchemy session
    :param hsda: A HSDA name string
    :return: A location record.
    """
    jhu_key = ', '.join([hsda, 'British Columbia', 'Canada'])

    try:
        location = session.query(Location). \
            filter(Location.jhu_key == jhu_key).one()
        return location
    except NoResultFound:
        # Continue this routine.
        pass

    # This location was not in the database.  Need to add it.
    location = Location()
    location.country = 'Canada'
    location.admin1 = 'British Columbia'
    location.admin2 = hsda
    location.jhu_key = jhu_key
    session.add(location)
    #
    # Note: no commit().  We only commit after loading the entire CSV.
    #
    return location
