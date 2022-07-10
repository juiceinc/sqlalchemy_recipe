from datetime import date, datetime
from dateutil.relativedelta import relativedelta


def date_offset(dt, offset, **offset_params):
    if offset in ("prior", "previous", "last"):
        dt -= relativedelta(**offset_params)
        return dt
    elif offset == "next":
        dt += relativedelta(**offset_params)
        return dt
    elif offset in ("current", "this"):
        return dt
    else:
        raise ValueError("Unknown intelligent date offset")


def convert_to_start_datetime(dt):
    """Convert a date or datetime to the first moment of the day"""
    # Convert a date or datetime to the first moment of the day
    # only if the datetime is the first moment of the day
    if isinstance(dt, (date, datetime)):
        dt = datetime(dt.year, dt.month, dt.day)
    return dt


def convert_to_end_datetime(dt):
    """Convert a date or datetime to the last moment of the day"""
    if isinstance(dt, (date, datetime)):
        dt = datetime(dt.year, dt.month, dt.day)
        dt += relativedelta(days=1, microseconds=-1)
    return dt


def convert_to_eod_datetime(dt):
    """Convert a date or datetime to the last moment of the day,
    only convert datetimes if they are the first moment of the day,"""
    if isinstance(dt, datetime):
        if dt.hour == 0 and dt.minute == 0 and dt.second == 0:
            dt += relativedelta(days=1, microseconds=-1)
    elif isinstance(dt, date):
        dt = datetime(dt.year, dt.month, dt.day)
        dt += relativedelta(days=1, microseconds=-1)
    return dt


def calc_date_range(offset, units, dt):
    """Create an intelligent date range using offsets, units and a starting date

    Args:

        offset: An offset
            current|this options for the current period
            prior|previous|last options for the previous period
            next options the next period
        units: The kind of date range to create
            year: A full year
            ytd: The year up to the provided date
            qtr: The full quarter the date belongs to
            month: The full month of the provided date
            mtd: The month up to the provided date
            day: The provided date
        dt
            The date that will be used for calculations

    Returns:

        A tuple of dates constructed using the offsets and units
    """
    offset = str(offset).lower()
    units = str(units).lower()

    # TODO: Add a week unit
    if units == "year":
        dt = date_offset(dt, offset, years=1)
        return date(dt.year, 1, 1), date(dt.year, 12, 31)
    elif units == "ytd":
        dt = date_offset(dt, offset, years=1)
        return date(dt.year, 1, 1), dt
    elif units == "qtr":
        dt = date_offset(dt, offset, months=3)
        qtr = (dt.month - 1) // 3  # Calculate quarter as 0,1,2,3
        return (
            date(dt.year, qtr * 3 + 1, 1),
            date(dt.year, qtr * 3 + 3, 1) + relativedelta(months=1, days=-1),
        )
    elif units == "month":
        dt = date_offset(dt, offset, months=1)
        start_dt = date(dt.year, dt.month, 1)
        return start_dt, start_dt + relativedelta(months=1, days=-1)
    elif units == "mtd":
        dt = date_offset(dt, offset, months=1)
        return date(dt.year, dt.month, 1), dt
    elif units == "day":
        dt = date_offset(dt, offset, days=1)
        return dt, dt
    else:
        raise ValueError("Unknown intelligent date units")
