# pylint: disable = C0103, C0301

"""
The 'biaDates.py' module provides procedures that
perform date-related calculations in the app.
"""

from datetime import date, datetime, time, timedelta
import numpy as np


def calculate_recon_times(day: date, off_days: list) -> dict:
	"""
	Calculates reconciliation times for a calendar day.

	Params:
	-------
	day:
		The calendar day for which the
		ultimo date will be determined.

	off_days:
		List of out-of-office dates as stated
		in the company's fiscal year calendar.

	Returns:
	--------
	Calculated reconciliation time parameters:
	- 'fiscal_year': `int` \n
		Fiscal year of the day.
	- 'fiscal_period': `int` \n
		Fiscal period (month) of the day.
	- 'reconciliation_date': `datetime.date` \n
		Reconciliation date for the day.
	- 'conversion_date': `datetime.date` \n
		Currency conversion date for the day.
	"""

	if not is_ultimo_plus_one(day, off_days):
		if is_start_of_month(day):
			while not np.is_busday(day, holidays = off_days):
				day += timedelta(-1)
			fisc_year = day.year + 1
			fisc_period = day.month
			recon_date = day
		else:
			fisc_year = day.year + 1
			fisc_period = day.month
			recon_date = day
	else:

		fisc_period = day.month - 1
		fisc_year = day.year

		if fisc_period == 0:
			fisc_period = 12

		if fisc_period != 12:
			fisc_year += 1

		last_day_prev_mon = start_of_month(day) - timedelta(1)

		while not np.is_busday(last_day_prev_mon, holidays = off_days):
			last_day_prev_mon -= timedelta(1)

		recon_date = last_day_prev_mon

	if recon_date == day:
		exch_rate_date = recon_date
	else:
		# recon_date = u+1; exch_rate_date = ultimo
		exch_rate_date = get_ultimo_date(day, off_days) # recon_date

	# result = {
	# 	"fiscal_year": fisc_year,
	# 	"fiscal_period": fisc_period,
	# 	"reconciliation_date": recon_date,
	# 	"conversion_date": exch_rate_date
	# }

	# FIXME: upravit tak, aby zohladnilo po novom aj 
	# datumy sviatkov medzi u a u+1, ak sa GL rozhodne
	# pustit rekonciliaciu v takychto dnoch
	result = {
		"fiscal_year": 2024,
		"fiscal_period": 10,
		"reconciliation_date": date(2023, 10, 31),
		"conversion_date": date(2023, 10, 31)
	}

	return result

def calculate_export_date(day: date, off_days: list, boundary: str = "lower") -> date:
	"""
	Calculates posting days for which FBL3N data will be exported.

	Params:
	-------
	day:
		A reference day for which the
		export dates will becalculated.

	off_days:
		List of out-of-office dates as stated
		in the company's fiscal year calendar.

	boundary:
		Export date interval boundary. Default 'lower'. \n
		If 'lower' is provided, then the first date of
		the reconciliation month be calculated. \n
		If 'upper' is provided, then the last date of
		the reconciliation month will be calculated.

	Returns:
	--------
	Posting date stored as a datetime.date object.
	"""

	result = calculate_export_dates(day, off_days)

	if boundary == "lower":
		exp_date = result[0]
	elif boundary == "upper":
		exp_date = result[1]
	else:
		raise ValueError("Argument 'kind' has incorrect value!")

	return exp_date

def calculate_export_dates(day: date, off_days: list) -> tuple:
	"""
	Calculates the first and the last posting days
	for which FBL3N data will be exported.

	Params:
	-------
	day:
		A reference day for which the
		export dates will becalculated.

	off_days:
		List of out-of-office dates as stated
		in the company's fiscal year calendar.

	Returns:
	--------
	A tuple of (first posting date, last posting date) stored as datetime.date objects.
	"""

	last_day_prev_mon = start_of_month(day) - timedelta(1)
	first_day_prev_mon = start_of_month(last_day_prev_mon)
	uplus_one = is_ultimo_plus_one(day, off_days)

	if uplus_one:
		lower = start_of_month(first_day_prev_mon - timedelta(1))
		upper = last_day_prev_mon
	else:
		lower = first_day_prev_mon
		upper = day

	# FIXME: opravit kalkulaciu tak aby zohladnilo aj predultimove datumy
	return (date(2023, 9, 1), date(2023, 10, 31))
	# return (lower, upper)

def end_of_month(day: date) -> date:
	"""
	Calculates last day of a month.

	Params:
	-------
	day:
		Day of a month for which the last day is calculated.

	Returns:
	--------
	Last date of the month.
	"""

	next_mon = day.replace(day=28) + timedelta(days=4)
	first_day_next_mon = next_mon - timedelta(days=next_mon.day)

	return first_day_next_mon

def is_start_of_month(day: date) -> bool:
	"""
	Checks if a day is the first day of month.

	Params:
	-------
	day:
		Day of a month for which the
		first day is calculated.

	Returns:
	--------
	The first date of a month.
	"""
	return day.day == 1

def start_of_month(day: date) -> date:
	"""
	Calculates first day of a month.

	Params:
	-------
	day:
		Day of a month for which the
		first day is calculated.

	Returns:
	--------
	The first date of a month.
	"""
	return day.replace(day = 1)

def get_date(n_days: int = 0, n_weeks: int = 0) -> date:
	"""
	Calculates a date by adding days
	and/or weeks to a current date.

	Params:
	-------
	n_days:
		Offset in days.

	n_weeks:
		Offset in weeks.

	Returns:
	--------
	An offsetted date.
	"""

	offset = datetime.date(datetime.now())
	offset += timedelta(days = n_days)
	offset += timedelta(weeks = n_weeks)

	return offset

def get_current_date() -> date:
	"""
	Returns a current date.

	Returns:
	--------
	A datetime.date object representing a current date.
	"""

	curr_date = get_date(0)

	return curr_date

def get_current_time() -> time:
	"""
	Returns current time.

	Returns:
	--------
	A datetime.time object representing a current time.
	"""

	curr_time = datetime.now().time()

	return curr_time

def is_ultimo_plus_one(day: date, off_days: list) -> bool:
	"""
	Checks whether a day is an ultimo + 1 day.

	Params:
	-------
	day:
		Day of a month for which the
		first day is calculated.

	off_days:
		List of out-of-office dates as stated
		in the company's fiscal year calendar.

	Returns:
	--------
	True if a day is an Ultimo + 1 day, False if it's not.
	"""

	first_day_month = start_of_month(day)
	first_workday = first_day_month

	while not np.is_busday(first_workday, holidays = off_days):
		first_workday += timedelta(1)

	if first_workday == day:
		return True

	return False

def get_ultimo_date(uplusone: date, off_days: list) -> date:
	"""
	Calculates an ultimo date for a given ultimo plus one date.

	Params:
	-------
	uplusone:
		The ultimo plus one date.

	off_days:
		List of out-of-office dates as stated
		in the company's fiscal year calendar.

	Returns:
	--------
	A calculated ultimo date.
	"""

	ultimo = uplusone - timedelta(1) # basically one day before U + 1

	# check if the ultimo date is a business day, if it is a
	# weekend day or a holiday, then calculate the ultimo as
	# the last business day of a given month
	while not np.is_busday(ultimo, holidays = off_days):
		ultimo -= timedelta(1)

	return ultimo
