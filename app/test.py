from datetime import date, datetime
from engine import biaDates2 as dates
import yaml
import sys
from os.path import join
import pandas as pd


def test_date_calculator():
	"""
	Tests the correctness of the calculated 
	reconciliation dates and times.
	"""

	print("Arranging unit text ...", end = "")

	cfg_path = join(sys.path[0], "appconfig.yaml")

	with open(cfg_path, encoding = "utf-8") as fs:
		cfg = yaml.safe_load(fs)

	# sanitize dates
	calc_holidays = []
	calendar_year = datetime.now().year

	for holiday in cfg["reconciliation"]["holidays"]:
		calc_holidays.append(date(calendar_year, holiday.month, holiday.day))

	# initializing testing periods
	start_date = datetime.strptime("01-01-2023", "%d-%m-%Y")
	days_in_year = pd.date_range(start_date, periods = 365)

	print("done.")

	print("Running test cases ...")
	n_failed = 0

	# dates.calculate_reconciliation_times(date(2023, 11, 2), calc_holidays)
	# dates.calculate_export_dates(date(2023, 11, 2), calc_holidays)

	for calendar_day in days_in_year:

		day = calendar_day.date()
		calcs = dates.calculate_reconciliation_times(day, calc_holidays)
		recon_date = calcs["reconciliation_date"]

		try:
			# recon runs on holidays during work days
			if day == date(2023, 1, 2):
				assert recon_date == date(2022, 12, 30)
			elif day == date(2023, 1, 6):
				assert recon_date == date(2023, 1, 6)
			elif day == date(2023, 6, 1):
				assert recon_date == date(2023, 5, 31)
			elif day == date(2023, 4, 7):
				assert recon_date == date(2023, 4, 7)
			elif day == date(2023, 4, 10):
				assert recon_date == date(2023, 4, 10)
			elif day == date(2023, 5, 1):
				assert recon_date == date(2023, 4, 28)
			elif day == date(2023, 5, 8):
				assert recon_date == date(2023, 5, 8)
			elif day == date(2023, 7, 5):
				assert recon_date == date(2023, 7, 5)
			elif day == date(2023, 5, 31):
				assert recon_date == date(2023, 5, 31)
			elif day == date(2023, 8, 29):
				assert recon_date == date(2023, 8, 29)
			elif day == date(2023, 9, 1):
				assert recon_date == date(2023, 8, 31)
			elif day == date(2023, 9, 17):
				assert recon_date == date(2023, 9, 17)
			elif day == date(2023, 12, 24):
				assert recon_date == date(2023, 12, 24)
			elif day == date(2023, 12, 25):
				assert recon_date == date(2023, 12, 25)
			elif day == date(2023, 12, 26):
				assert recon_date == date(2023, 12, 26)
			# recon runs on first work days of weeks
			elif day == date(2023, 2, 1):
				assert recon_date == date(2023, 1, 31)
			elif day == date(2023, 3, 1):
				assert recon_date == date(2023, 2, 28)
			elif day == date(2023, 4, 3):
				assert recon_date == date(2023, 3, 31)
			elif day == date(2023, 5, 2):
				assert recon_date == date(2023, 4, 28)
			elif day == date(2023, 6, 1):
				assert recon_date == date(2023, 5, 31)
			elif day == date(2023, 7, 1):
				assert recon_date == date(2023, 6, 30)
			elif day == date(2023, 7, 3):
				assert recon_date == date(2023, 6, 30)
			elif day == date(2023, 8, 1):
				assert recon_date == date(2023, 7, 31)
			elif day == date(2023, 9, 2):
				assert recon_date == date(2023, 8, 31)
			elif day == date(2023, 10, 2):
				assert recon_date == date(2023, 9, 29)
			elif day == date(2023, 11, 2):
				assert recon_date == date(2023, 10, 31)
			elif day == date(2023, 10, 31):
				assert recon_date == date(2023, 10, 31)
			elif day == date(2023, 12, 1):
				assert recon_date == date(2023, 11, 30)
			elif day == date(2023, 11, 1):
				assert recon_date == date(2023, 10, 31)
			elif day == date(2023, 9, 4):
				assert recon_date == date(2023, 8, 31)
			# runs on
			elif day == date(2023, 1, 1):
				assert recon_date == date(2022, 12, 30)
			elif day == date(2023, 10, 1):
				assert recon_date == date(2023, 9, 29)
			elif day == date(2023, 5, 31):
				assert recon_date == date(2023, 5, 31)
			elif day == date(2023, 4, 1):
				assert recon_date == date(2023, 3, 31)
			elif day == date(2023, 4, 2):
				assert recon_date == date(2023, 3, 31)
			elif day == date(2023, 7, 2):
				assert recon_date == date(2023, 6, 30)
			elif day == date(2023, 9, 3):
				assert recon_date == date(2023, 8, 31)
			else:
				assert recon_date == day
		except Exception:
			n_failed += 1
			print("Entered day:", day, "Calculated day:", recon_date)			

	if n_failed == 0:
		print("All test cases passed.")
	else:
		print("Test cases failed:", n_failed)

test_date_calculator()
