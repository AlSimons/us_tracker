#!/usr/bin/env python3

# Standard library
import argparse
import datetime
import decimal
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import statistics
import sys

# Project imports
from groups import groups
from mysql_credentials import username, password

# SQL Alchemy imports
from sqlalchemy import create_engine, text, MetaData, Table

DATABASE_NAME = r'mysql+mysqlconnector://{}:{}@localhost/covid_data'
POPULATION_FILE = r'us_tracker\populations.csv'


class ColumnInfo:
    def __init__(self, field,
                 computed_type, depended_field, depended_field2=None):
        self.field = field
        self.computed_type = computed_type
        self.depended_field = depended_field
        self.depended_field2 = depended_field2


class Day:
    columns = None
    all_days = []
    day_hash = {}
    past_accelerations = []

    def __init__(self, date,
                 conf_num, deaths_num,
                 parent_conf_num=0, parent_deaths_num=0):
        self.date = date
        Day.all_days.append(self)
        Day.day_hash[date] = self
        for column in Day.columns:
            setattr(self, column.field, 0)
        self.conf_num = conf_num
        self.deaths_num = deaths_num
        self.parent_conf_num = parent_conf_num
        self.parent_deaths_num = parent_deaths_num

    @staticmethod
    def set_columns(_columns):
        Day.columns = _columns

    @staticmethod
    def fill_data_list(data_list):
        """
        One of the data lists [(date, cases, deaths),...] has missing dates.
        Fill in with the previous days data.
        :param data_list: List of triples, (ordinal_date, cases, deaths)
        :return: A new filled in list.
        """
        # Now walk the lists.  If a date is missing in either, add an item with
        # the previous day's data. Since we're messing with the list, we can't
        # simply walk over range(len()). The simplest way around this is to
        # create two new lists, appending as we go: new_ld for location data,
        # new_pd for parent data.
        filled_list = []
        prev_date = data_list[0][0] - 1
        prev_cases = 0
        prev_deaths = 0
        for datum in data_list:
            if datum[0] != prev_date + 1:
                # Need to add some elements
                for ordinal_date in range(prev_date + 1, datum[0]):
                    filled_list.append((ordinal_date, prev_cases, prev_deaths))

            # Now OK to continue
            filled_list.append(datum)
            prev_date = datum[0]
            prev_cases = datum[1]
            prev_deaths = datum[2]

        return filled_list

    @staticmethod
    def fixup_missing_location_data(location_data: list, parent_data: list) -> \
            (list, list):
        """Sometimes data are incomplete, for instance, New York City has
        data for four days, and then not for 16, and then solid. Where there
        are missing data, fill in with the previous day's data.
        :param location_data: A list of triples (ordinal_day, cases, deaths).
        :param parent_data: Same for the parent.
        :return A collection of two repaired lists, of the same triples.
        """
        # First ensure that the lists start on the same day.  If the parent
        # list starts after the location list, something is bad wrong. Bail
        # so we can see when it happens and debug it.
        location_first_date = location_data[0][0]
        if location_first_date != parent_data[0][0]:
            if location_first_date < parent_data[0][0]:
                sys.exit("Error. Location data earlier than parent data.")
            for n in range(len(parent_data)):
                if location_first_date > parent_data[n][0]:
                    continue
                # Nuance: It should not be possible for a parent to be missing
                # a date that a location has.
                if location_first_date != parent_data[n][0]:
                    sys.exit("Parent_data doesn't contain location data.")
                # Truncate the parent data to start at the start of the
                # location data.
                parent_data = parent_data[n:]
                break

        # Sanity check.  They should end on the same date.
        if location_data[-1][0] != parent_data[-1][0]:
            sys.exit("Location and parent_data don't end on the same date")
        # Note! Their lengths are likely not equal at this point, but
        # the parent data list should not be shorter.
        if len(parent_data) < len(location_data):
            sys.exit("The parent data list is shorter than loc data.")
        # Phew!
        location_data = Day.fill_data_list(location_data)
        parent_data = Day.fill_data_list(parent_data)

        # NOW the lists should be the same length!
        if len(location_data) != len(parent_data):
            sys.exit("Lists are not the same length after filling.")
        return location_data, parent_data

    @staticmethod
    def set_data(location_data, parent_data):
        # Sanity check, since we're going to walk the two result lists
        # in parallel.
        if parent_data is not None and (
                len(location_data) != len(parent_data) or
                location_data[0][0] != parent_data[0][0] or
                location_data[-1][0] != parent_data[-1][0]):
            location_data, parent_data = Day.fixup_missing_location_data(
                location_data, parent_data)

        for n in range(len(location_data)):
            # OK, let's create days!
            if parent_data is not None:
                Day(ordinal_date_to_string(location_data[n][0]),
                    location_data[n][1], location_data[n][2],
                    parent_data[n][1], parent_data[n][2])
            else:
                Day(ordinal_date_to_string(location_data[n][0]),
                    location_data[n][1], location_data[n][2])

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

    def compute_active_today(self, healed_day):
        for column in Day.columns:
            if column.computed_type == 'active':
                active_field = column.field
                confirmed_field = column.depended_field
                deaths_field = column.depended_field2
                try:
                    current_confirmed = getattr(self, confirmed_field)
                except AttributeError:
                    current_confirmed = 0
                try:
                    healed_cases = getattr(healed_day, confirmed_field)
                except AttributeError:
                    healed_cases = 0
                try:
                    deaths = getattr(self, deaths_field)
                except AttributeError:
                    deaths = 0
                try:
                    past_deaths = getattr(healed_day, deaths_field)
                except AttributeError:
                    past_deaths = 0
                # To compute the current active cases at any point in time,
                # first we assume that any case that existed 14 or more days
                # ago is no longer active, and remove those cases; they have
                # either healed or died. Then we have to remove the deaths
                # which have occurred within the past 14 days (the deaths
                # before then were removed by removing ALL cases older than
                # 14 days).
                recent_deaths = deaths - past_deaths
                active = current_confirmed - (healed_cases + recent_deaths)

                setattr(self, active_field, active)

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
    def compute_active():
        # Assume that the active cases are
        # confirmed - deaths - confirmed 14 days ago
        for n in range(15, len(Day.all_days)):
            today = Day.all_days[n]
            healed_day = Day.all_days[n-14]
            today.compute_active_today(healed_day)

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
                                Day.past_accelerations[-args.days:]),
                                2))

    def __str__(self):
        out = '{}\t'.format(self.date)
        for column in Day.columns:
            out += str(getattr(self, column.field)) + '\t'
            out += str(self.parent_conf_num) + '\t'
            out += str(self.parent_deaths_num)
        return out


args = None


def parse_args():
    global args
    parser = argparse.ArgumentParser()
    parser.add_argument('-1', '--admin1',
                        help="Specify admin-1 level group (e.g., country)")
    parser.add_argument(
        '-2', '--admin2',
        help="Specify admin-2 level group (e.g., state / province)")
    parser.add_argument(
        '-3', '--admin3',
        help="Specify admin-3 level group (e.g., city / county)")
    parser.add_argument('-a', '--active-cases', action='store_true',
                        help="Display active cases instead of confirmed cases.")
    parser.add_argument('-c', '--cumulative-cases', action='store_true',
                        help="Display cumulaltive cases instead of active cases.")
    parser.add_argument('-d', '--days', type=int, default=7,
                        help="Number of days for rolling averages [7]")
    parser.add_argument('-D', '--display-days', type=int, default=365,
                        help="Number of days to display [365]")
    parser.add_argument('-g', '--use-groups', action='store_true',
                        help='Plot a group of entities, e.g., Europe')
    parser.add_argument('-G', '--group-name', type=str,
                        help="Name for a groups of entities, e.g., Europe")
    parser.add_argument('-l', '--death-lag', type=int, default=10,
                        help="Number of days to skew the death stats when "
                             "determining death rate. [10]")
    parser.add_argument('--parent-1',
                        help="Country to use for percentage computation")
    parser.add_argument('--parent-2',
                        help="State to use for percentage computation")
    parser.add_argument('--parent-global', action="store_true",
                        help="Compute percentage of global cases.")
    parser.add_argument('-s', '--smooth-data', action='store_true',
                        help="Smooth data spikes caused by missing a day's"
                             "data")
    parser.add_argument('-y', '--y-log', action='store_true',
                        help="Use logarithmic scale for Y axis.")
    args = parser.parse_args()
    if args.use_groups and not args.group_name:
        parser.error("Using --use-groups requires using --group-name")
    if not args.cumulative_cases:
        args.active_cases = True
    return args


def get_population(focus):
    # Capture a local copy of focus, in case we need to change it.
    local_focus = focus
    if type(local_focus) == str:
        local_focus = [local_focus]
    # The record keeping for NYC changed. At first it was a single entity, then
    # they started tracking it by county (borough).  So for pulling the
    # stats, we have to look for both the "New York City" entries, and also
    # the entries for each borough, so it has to be a group.
    # BUT! When computing the populations, we only want to look up NYC, without
    # adding in all the boroughs (again).  If we do that, the computed
    # population is greater than that for the whole state.
    if local_focus[0] == 'New York City':
        local_focus = ['New York City']
    population = 0
    found = set()
    with open(POPULATION_FILE) as f:
        for line in f:
            parts = line.strip().split(',')
            if parts and parts[0] in local_focus:
                population += int(parts[1])
                found.add(parts[0])
    # Now report the populations we were not able to find.
    header_out = False
    for place in local_focus:
        if place not in found:
            if not header_out:
                print("Population(s) not found:")
                header_out = True
            print("   ", place)

    # Use zero as the "no data" flag, since no entity will have a population
    # of zero
    return population


def define_columns():
    columns_defs = [
        ColumnInfo('conf_num', '', ''),
        # The files have an "active" column which we don't use because it isn't
        # maintained by all jurisdictions. So we create a new column,
        # "computed_active"
        ColumnInfo('computed_active', 'active', 'conf_num', 'deaths_num'),
        ColumnInfo('daily_conf', 'vel', 'conf_num'),
        ColumnInfo('avg_daily_conf', 'rolling_average', 'daily_conf'),
        ColumnInfo('deaths_num', '', ''),
        ColumnInfo('daily_deaths', 'vel', 'deaths_num'),
        ColumnInfo('avg_daily_deaths', 'rolling_average', 'daily_deaths'),
    ]
    return columns_defs


def one_plot(dates, values, focus, position, title, color):
    plt.subplot(position)
    if type(focus) == str:
        plt.title(focus + " " + title)
    else:
        plt.title(args.group_name + " " + title)

    # For some displays, we may want to look in log scale.
    y_bottom_lim = 0
    if args.y_log:
        y_bottom_lim = 99999999999
        max = -1
        for x in values:
            if x > max:
                max = x
            if x < y_bottom_lim:
                y_bottom_lim = x
        if max > 2500:
            if y_bottom_lim < 1:
                y_bottom_lim = 1
            plt.yscale('log');

    plt.xticks(rotation=90)
    # Set x-axis major ticks to weekly interval, on Mondays
    plt.gca().xaxis.set_major_locator(
        mdates.WeekdayLocator(byweekday=mdates.MONDAY, interval=2))
    # Format x-tick labels as 3-letter month name and day number
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
    plt.plot(dates, values, color + '-')
    plt.grid(axis='y')
    axes = plt.gca()
    axes.set_ylim(bottom=y_bottom_lim)


def plot_it(focus, parent_focus):
    dates = []
    conf_vel = []
    conf_num = []
    conf_accel = []
    active_num = []
    deaths = []
    deaths_vel = []
    deaths_acc = []
    # Percent of parent
    conf_pop = []
    deaths_pop = []

    doing_parents = Day.all_days[-1].parent_conf_num > 0

    for day in Day.all_days[-args.display_days:]:
        dates.append(day.date)
        # For plotting, we overload the conf_num list to report active cases,
        # instead of cumulative cases.
        conf_num.append(day.conf_num)
        active_num.append(day.computed_active)
        conf_vel.append(day.daily_conf)
        conf_accel.append(day.avg_daily_conf)
        deaths.append(day.deaths_num)
        deaths_vel.append(day.daily_deaths)
        deaths_acc.append(day.avg_daily_deaths)
        if doing_parents:
            if day.parent_conf_num > 0:
                conf_pop.append((day.conf_num / day.parent_conf_num) * 100)
            else:
                conf_pop.append(0.0)
            if day.parent_deaths_num > 0:
                deaths_pop.append((day.deaths_num / day.parent_deaths_num) *
                                  100)
            else:
                deaths_pop.append(0.0)

            # Layout of the charts depends on whether we are doing parents.
            # We're still in "if doing_parents:"
            conf_num_pos = 241
            conf_daily_pos = 242
            conf_roll_avg_pos = 243
            conf_pop_pos = 244
            deaths_num_pos = 245
            deaths_daily_pos = 246
            deaths_roll_avg_pos = 247
            deaths_pop_pos = 248
        else:
            # No display of parents percentage
            conf_num_pos = 231
            conf_daily_pos = 232
            conf_roll_avg_pos = 233
            deaths_num_pos = 234
            deaths_daily_pos = 235
            deaths_roll_avg_pos = 236

    focus_population = get_population(focus)

    if doing_parents:
        parent_population = get_population(parent_focus)
        if parent_population > 0 and focus_population > 0:
            percent_of_parent_population = \
                100 * focus_population / parent_population
            percent_of_parent_pop_str = " ({}%)".\
                format(round(percent_of_parent_population, 2))
        else:
            percent_of_parent_pop_str = ""

    if type(focus) == str:
        focus_for_title = focus
    else:
        focus_for_title = args.group_name
    if args.active_cases:
        focus_for_title += " Active"
    plt.figure(figsize=(19, 9), num='{} COVID-19 History ({} days) Pop: {:,}'.
               format(focus_for_title, len(dates), focus_population))
    plt.subplots_adjust(hspace=.27, wspace=.16,
                        top=.97, bottom=.07,
                        left=.06, right=.97)

    if args.active_cases:
        cases = active_num[-1]
        case_list = active_num
        case_type = "Active"
    else:
        cases = conf_num[-1]
        case_list = conf_num
        case_type = "Confirmed"

    # Compute the percentage of the population that is either confirmed or
    # active.
    if focus_population > 0:
        if not args.active_cases:
            confirmed_percentage = " {}%".\
                format(round(100 * cases / focus_population, 2))
        else:
            # Active cases
            confirmed_percentage = "(1/{})".\
                format(round(focus_population / cases, 0))
    else:
        confirmed_percentage = ""

    one_plot(dates, case_list, focus, conf_num_pos, "{:,} {} Cases {}".
             format(cases, case_type, confirmed_percentage), 'g')

    # The number will print with a minus sign if < 0, so we only need to
    # add the "+" if >= 0.
    sign = "+" if conf_vel[-1] > 0 else ""
    one_plot(dates, conf_vel, focus, conf_daily_pos,
             "{:,} Daily New Cases {}{}%".
             format(conf_vel[-1], sign,
                    round(100 * conf_vel[-1] / conf_num[-2], 2)),
             'y')
    sign = "+" if conf_accel[-1] > conf_accel[-2] else ""
    if conf_accel[-2] != 0:
        inc_pct_str = "{}{}%". \
            format(sign,
                   round(100 *
                         (conf_accel[-1] - conf_accel[-2]) / conf_accel[-2],
                         2))
    else:
        inc_pct_str = ""
    one_plot(dates, conf_accel, focus, conf_roll_avg_pos,
             "{:,} {} Day Avg New Cases {}".
             format(conf_accel[-1],
                    min(len(dates), args.days),
                    inc_pct_str), 'k')
    if doing_parents:
        one_plot(dates, conf_pop, focus, conf_pop_pos,
                 "{}% of {} cases{}".
                 format(round(conf_pop[-1], 2), parent_focus,
                        percent_of_parent_pop_str), 'k')
    try:
        one_plot(dates, deaths, focus, deaths_num_pos,
                 "{:,} Deaths Cumulative: {}%".
                 format(deaths[-1],
                        round((deaths[-1] /
                               conf_num[-(args.death_lag + 1)]) * 100, 1)),
                 'r')
    except IndexError:
        print("No data found for {}".format(focus))
        sys.exit()
    sign = "+" if deaths[-1] > deaths[-2] else ""
    one_plot(dates, deaths_vel, focus, deaths_daily_pos,
             "{:,} Daily New Deaths {}{}%".
             format(deaths_vel[-1], sign,
                    round(100 *
                          (deaths[-1] - deaths[-2]) / deaths[-2], 2)), 'b')
    try:
        deaths_inc_pct_sign = "+" if deaths_acc[-1] > deaths_acc[-2] else ""
        deaths_inc_pct_str = "{}{}%".format(
            deaths_inc_pct_sign, round(
                100 * (deaths_acc[-1] - deaths_acc[-2]) / deaths_acc[-2], 2))
    except (ZeroDivisionError, decimal.InvalidOperation):
        # Stuff coming out of the database (at least under MySQL) are of type
        # Decimal (not ints). Therefore, we have to take care of its version
        # of ZeroDivisionError.  Why they didn't use the base ZeroDivisionError
        # remains a mystery!
        deaths_inc_pct_str = ""

    one_plot(dates, deaths_acc, focus, deaths_roll_avg_pos,
             "{} {} Day Avg Deaths {}".
             format(deaths_acc[-1],
                    min(len(dates), args.days),
                    deaths_inc_pct_str), 'k')
    if doing_parents:
        one_plot(dates, deaths_pop, focus, deaths_pop_pos,
                 "{}% of {} deaths{}".
                 format(round(deaths_pop[-1], 2), parent_focus,
                        percent_of_parent_pop_str), 'k')
    plt.show()


def get_level_focus():
    focuses = [None, None, None]
    if args.admin1:
        # Summarize a country
        if args.use_groups and args.admin1 in groups['admin1']:
            focuses[0] = groups['admin1'][args.admin1]
        else:
            focuses[0] = args.admin1
    if args.admin2:
        # Summarize a state / province
        if args.use_groups and args.admin2 in groups['admin2']:
            focuses[1] = groups['admin2'][args.admin2]
        else:
            focuses[1] = args.admin2
    if args.admin3:
        # Summarize a city / county
        if args.use_groups and args.admin3 in groups['admin3']:
            focuses[2] = groups['admin3'][args.admin3]
        else:
            focuses[2] = args.admin3
    return focuses


def get_parent_level_focus():
    if args.parent_global:
        return None, 'Global'

    if args.parent_1:
        # Summarize a country
        return 'Country', args.parent_1

    if args.parent_2:
        # Summarize a state / province
        return 'State', args.parent_2

    # No parent display requested.
    return None, None


engine = None


def get_engine():
    global engine
    if engine is None:
        engine = create_engine(DATABASE_NAME.format(username, password),
                               echo=False)
    return engine


def get_tables_and_connection():
    eng = get_engine()
    metadata = MetaData()
    location_table = \
        Table('location', metadata, autoload=True,
              autoload_with=engine)
    datum_table = \
        Table('datum', metadata, autoload=True,
              autoload_with=engine)
    return location_table, datum_table, eng


def ordinal_date_to_string(ordinal, to_dt=True):
    date_string = str(datetime.datetime.fromordinal(ordinal).date())
    if to_dt:
        dt = datetime.datetime.strptime(date_string, '%Y-%m-%d')
        return dt
    # If just the string was wanted...
    return date_string


def build_focus_where_clause(field, focus):
    if type(focus) == str:
        clause = '{} = "{}"'.format(field, focus)
    else:
        wrapped_focus = []
        for n in range(len(focus)):
            # Have to wrap the strings in quotes for SQL
            wrapped_focus.append('"' + focus[n] + '"')
        clause = '{} in ({})'.format(field, ', '.join(wrapped_focus))

    return clause


def get_data_from_db(focuses, parent_level, parent_focus):
    # FIXME
    # FIXME
    # FIXME
    # FIXME
    # FIXME YIKES!  Not using prepared statements!
    # FIXME
    # FIXME
    # FIXME
    """
    First we get a list of all the jhu_keys in the requested focus.
    Then we will get all the data for those location keys.
    :param focuses: a list of [country, admin1, admin2] describing the location
        we want to plot.  For instance, ['US', None, None], or
        [None, 'Arizona', None].
    :param parent_level:
    :param parent_focus:
    :return:
    """
    location, datum, conn = get_tables_and_connection()
    focus_texts = []
    case_type = 'confirmed'

    base_query_text = """
    SELECT 
            datum.ordinal_date AS odate, 
            SUM(datum.{}) AS conf,
            SUM(datum.deaths) AS deaths
        FROM datum, location WHERE 
    """.format(case_type)

    # Build the location's where based on the focuses
    if focuses[0] is not None:
        focus_where = build_focus_where_clause('country', focuses[0])
        focus_texts.append(focus_where)
    if focuses[1] is not None:
        focus_where = build_focus_where_clause('admin1', focuses[1])
        focus_texts.append(focus_where)
    if focuses[2] is not None:
        focus_where = build_focus_where_clause('admin2', focuses[2])
        focus_texts.append(focus_where)

    if focus_texts:
        where_clause = ' AND '.join(focus_texts) + ' AND '
    else:
        where_clause = ''

    query_tail = """
        location.jhu_key = datum.location_jhu_key
    GROUP BY ordinal_date ORDER BY ordinal_date
    """
    query_text = base_query_text + where_clause + query_tail

    # The query result is a generator like object.  We will need to do
    # a few things that require a list.
    location_result = list(conn.execute(text(query_text)))

    # Now work on the parent where clause
    if parent_focus is None:
        # Nothing left to do.
        return location_result, None

    if parent_focus == 'Global':
        where_clause = ''
    elif parent_level == 'Country':
        where_clause = 'country = "{}"'.format(parent_focus)
    elif parent_level == 'State':
        where_clause = 'admin1 = "{}"'.format(parent_focus)
    else:
        sys.exit("Huh? Can't figure out parent")

    if where_clause:
        where_clause += ' AND '

    # Is is likely that the parent has more (earlier) data than the location.
    # Only get the parent data from the start of the location's data
    date_limit_text = "ordinal_date >= {} AND ".format(location_result[0][0])
    query_text = \
        base_query_text + where_clause + date_limit_text + query_tail
    parent_result = list(conn.execute(text(query_text)))

    return location_result, parent_result


def main():
    parse_args()
    columns = define_columns()
    Day.set_columns(columns)

    focuses = get_level_focus()

    parent_level, parent_focus = get_parent_level_focus()

    location_data, parent_data = get_data_from_db(focuses,
                                                  parent_level, parent_focus)
    if args.smooth_data:
        location_data = smooth_data(location_data)
        parent_data = smooth_data(parent_data)

    Day.set_data(location_data, parent_data)

    Day.compute_pcts_velocities_accelerations()
    Day.compute_rolling_average()
    if args.active_cases:
        Day.compute_active()

    # Use the finest grain focus that was specified
    focus = 'Global'
    for n in range(2, -1, -1):
        if focuses[n] is not None:
            focus = focuses[n]
            break
    plot_it(focus, parent_focus)


if __name__ == '__main__':
    main()
