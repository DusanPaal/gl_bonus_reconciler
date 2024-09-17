# pylint: disable = C0103, C0123, C0301, C0302, E0401, E0611, W0603, W0703, W1203

"""
The 'biaController.py' module represents the middle layer
of the application design that connects the top and bottom
layers via service procedures, facilitating data management
and control flow between the two layers.
"""

from datetime import datetime, timedelta, date
from glob import glob
import logging
from logging import config
from os import mkdir, remove, scandir
from os.path import exists, join, isfile
from shutil import rmtree
import sys

from win32com.client import CDispatch
import yaml

import engine.biaDatabase as db
import engine.biaDates2 as dat
import engine.biaFBL3N as fbl3n
import engine.biaFS10N as fs10n
import engine.biaMail as mail
import engine.biaProcessor as proc
import engine.biaRecovery as rec
import engine.biaReport as report
import engine.biaSAP as sap
import engine.biaSE16 as se16
import engine.biaUtils as utils
import engine.biaPortal as ptl
import engine.biaZSD25 as zsd25

_pg_conn = None
_logger = logging.getLogger("master")

def initialize_logger() -> None:
	"""
	Initializes application global logger.

	The logger is configured using logging paramters
	that are loaded form an external file. Then, either
	a new log file is created, or the content of an existing
	one is cleared. Lastly, the logger header is printed
	to the empty log file.
	"""

	# load logging configuration
	cfg_path = join(sys.path[0], "logging.yaml")

	with open(cfg_path, encoding = "utf-8") as stream:
		cfg_text = stream.read()

	log_cfg = yaml.safe_load(cfg_text)
	config.dictConfig(log_cfg)

	# compile path to the log file
	nth = 1

	while True:

		log_name = f"log_{str(nth).zfill(3)}.log"
		log_path = join(sys.path[0], "logs", log_name)

		if not isfile(log_path):
			break

		nth += 1

	# change log file handler in the logging config
	prev_file_handler = _logger.handlers.pop(1)
	new_file_handler = logging.FileHandler(log_path, encoding = "utf-8")
	new_file_handler.setFormatter(prev_file_handler.formatter)
	_logger.addHandler(new_file_handler)

	# create an empty / clear an existing log file
	with open(log_path, 'w', encoding = "utf-8"):
		pass

	# write log header
	header = {
		"Application name": "GL Bonus Reconciler",
		"Application version": "1.1.20220817",
		"Log date": get_current_time("%d-%b-%Y")
	}

	for i, (key, val) in enumerate(header.items(), start = 1):
		line = f"{key}: {val}"
		if i == len(header):
			line += "\n"
		_logger.info(line)

def get_current_time(fmt: str) -> str:
	"""
	Returns a formatted current date.

	Params:
	-------
	fmt:
		The string that controls
		the ouptut date format.

	Returns:
	--------
	A string representing the current date.
	"""

	ctime = dat.get_current_date().strftime(fmt)

	return ctime

def load_app_config(file_path: str) -> dict:
	"""
	Reads and parses a file containing
	application configuration parameters.

	Params:
	-------
	file_path:
		Path to a .yml/.yaml file with
		configuration parameters.

	Returns:
	--------
	Application configuration parameters. \n
	If data loading fails, then None is returned.
	"""

	_logger.info("Configuring application ...")

	try:
		with open(file_path, encoding = "utf-8") as stream:
			txt = stream.read().replace("$appdir$", sys.path[0])
			cfg = yaml.safe_load(txt)
	except Exception as exc:
		_logger.critical(f"Loading data form 'appconfig.yaml' failed! Details: {exc}")
		return None

	calc_holidays = []
	calendar_year = datetime.now().year

	for holiday in cfg["reconciliation"]["holidays"]:
		calc_holidays.append(date(calendar_year, holiday.month, holiday.day))

	cfg["reconciliation"]["holidays"] = calc_holidays

	return cfg

def get_active_countries(data_cfg: dict, rules: dict, cocd: str = None) -> dict:
	"""
	Returns a list of active countries mapped to their company codes.

	If a company code is provided, then only parameters for that\n
	company code's country will be checked and returned.

	Params:
	-------
	tmp_dir:
		Path to the root directory containing temporary folders and files.

	rules:
		A dict of countries mapped to their specific reconciliation parameters.

	cocd:
		Company code (default None).

	Returns:
	--------
	Names of active countries mapped to their respective company codes.
	"""

	_logger.info("Searching active countries to process ...")

	tmp_dir = data_cfg["temp_dir"]
	countries = {}
	created = False
	cocd_valid = False

	# create list of countries to process
	for cntry in rules:

		if cocd is not None and rules[cntry]["company_code"] != cocd:
			continue

		cocd_valid = True

		if not rules[cntry]["active"]:
			_logger.warning(f"'{cntry}' excluded from reconciliation "
			"as per settings defined in 'rules.yaml'.")
			continue

		if not _ensure_subdirs(tmp_dir, rules[cntry]["company_code"]):
			_logger.error(f"'{cntry}' excluded form reconciliation. "
			"Reason: Subdirectory for temporary data missing!")
			continue

		created = True
		countries.update({cntry: rules[cntry]["company_code"]})

	if cocd is not None and not cocd_valid:
		_logger.error(f"The user-entered company code {cocd} doesn't match "
		"any of the countries listed in 'rules.yaml'")
		return None

	if not created:
		_logger.critical("Could not create subfolders in temp!")
		return None

	if len(countries) == 0:
		return None

	return countries

def get_user_params(msg_cfg: dict, email_id: str) -> dict:
	"""
	Extracts sender info and reconciliation parameters
	from user email.

	Params:
	-------
	msg_cfg:
		Application 'messages' configuration parameters.

	email_id:
		MS Exchange ID of the user email.

	Returns:
	--------
	Reconciliation parameters such as user email, user name, \n
	and the company code, for which reconciliation should be performed.\n
	"""

	_logger.info("Extracting user input parameters ...")

	user_req = msg_cfg["requests"]

	try:
		creds = mail.get_credentials(user_req["account"])
	except (FileNotFoundError, mail.ParamNotFoundError) as exc:
		_logger.exception(exc)
		return None

	acc = mail.get_account(user_req["mailbox"], creds, user_req["server"])
	msg = mail.get_message(acc, email_id)

	if msg is None:
		return None

	params = mail.extract_user_data(msg)
	_logger.debug(f"Parameters: {params}")

	if not params["email"].endswith("@ledvance.com"):
		_logger.error(f"Invalid sender: {msg.sender}! Only company email addresses are accepted!")
		return None

	return params

def load_reconciliation_rules(recon_cfg: dict) -> dict:
	"""
	Reads and parses file that contains country-specific \n
	parameters used for data export and reconciliation.

	Params:
	-------
	recon_cfg:
		Application 'reconciliation' configuration params.

	Returns:
	--------
	Country names mapped to their respective reconciliation rules.
	"""

	_logger.info("Loading reconciliation rules ...")

	file_path = recon_cfg["rules_path"]

	try:
		with open(file_path, 'r', encoding = "UTF-8") as stream:
			rules = yaml.safe_load(stream)
	except Exception as exc:
		_logger.critical(exc)
		return None

	return rules

def _ensure_subdirs(tmp_dir: str, cocd: str) -> bool:
	"""
	Checks whether a subdir named by a specific
	company code exists in the root temporary
	directory. If the subdir is not found, the
	procedure attempts to create one. Returns
	True, if the subdir is found or created,
	False if the creation fails.
	"""

	if not exists(tmp_dir):
		_logger.critical(f"Folder not found at the specified path: {tmp_dir}")
		return False

	sub_dir = join(tmp_dir, cocd)

	if exists(sub_dir):
		return True

	try:
		mkdir(sub_dir)
	except Exception as exc:
		_logger.exception(exc)
		return False

	mkdir(join(sub_dir, "exp"))
	mkdir(join(sub_dir, "bin"))

	return True

def connect_to_sap(sap_cfg: dict) -> CDispatch:
	"""
	Manages opening application connection
	to the SAP GUI scripting engine.

	Params:
	-------
	sap_cfg:
		SAP GUI configuration paramters.

	Returns:
	--------
	An initialized GuiSession object.
	"""

	_logger.info("Logging to SAP ...")

	sess = sap.login(sap_cfg["gui_path"], sap.SYS_P25)

	return sess

def disconnect_from_sap(sess: CDispatch) -> CDispatch:
	"""
	Manages disconnecting from
	the SAP GUI scripting engine.

	Params:
	-------
	sess:
		A SAP GuiSession object.

	Returns:
	--------
	None.
	"""

	_logger.info("Logging out from SAP GUI application ...")

	sap.logout(sess)

	return sess

def connect_to_database(db_cfg: dict) -> bool:
	"""
	Manages connecting to the database engine.

	Params:
	-------
	db_cfg:
		Application 'database' configuration parameters.

	Returns:
	--------
	True if a connection to database is successfully opened, False if not.
	"""

	global _pg_conn

	_logger.info("Connecting to database ...")
	_logger.debug(f"Connection params: {db_cfg}'")

	_pg_conn = db.connect(
		db_cfg["host"], db_cfg["port"],
		db_cfg["name"], db_cfg["user"],
		db_cfg["password"]
	)

	if _pg_conn is None:
		_logger.critical("Could not connect to the database!")
		return False

	return True

def disconnect_from_database() -> None:
	"""
	Closes an open connection to
	the database engine and releases
	any allocated resources.
	"""

	global _pg_conn

	_logger.info("Closing connection to database ...")
	db.disconnect(_pg_conn)
	del _pg_conn

def initialize_recovery(rec_cfg: dict, countries: list, rules: dict) -> bool:
	"""
	Manages the initialization of application recovery functionality.

	Params:
	-------
	rec_cfg:
		Application 'recovery' configuration parameters.

	countries:
		List of countries to which the recovery mechanism will be applied.

	rules:
		Country names mapped to their respective reconciliation rules.

	Returns:
	--------
	True if recovery initialization succeeds, False if it fails.
	"""

	rec_path = join(rec_cfg["recovery_dir"], rec_cfg["recovery_name"])

	try:
		rec.initialize(rec_path, countries, rules)
	except Exception as exc:
		_logger.critical(str(exc))
		return False

	return True

def export_fbl3n_data(data_cfg: dict, recon_cfg: dict, rules: dict, cntry: str, sess: CDispatch) -> bool:
	"""
	Manages data export from GL accounts into a local text file.

	Params:
	-------
	sap_cfg:
		Applicaton 'sap' configuration parameters.

	data_cfg:
		Application 'data' configuration parameters.

	recon_cfg:
		Application 'reconciliation' configuration parameters.

	rules:
		Country names mapped to their respective reconciliation rules.

	cntry:
		Name of the country to reconcile.

	sess:
		A SAP GuiSession object.

	Returns:
	--------
	True if data export succeeds, False if it fails.
	"""

	if rec.get_state(cntry, "fbl3n_data_exported"):
		_logger.warning("Skipping FBL3N data export. "
		"Reason: Data already exported in the previous run.")
		return True

	cocd = rules[cntry]["company_code"]
	accs = list(map(str, rules[cntry]["accounts"]))
	exp_name = data_cfg["fbl3n_data_export_name"].replace("$cocd$", cocd)
	exp_path = join(data_cfg["temp_dir"], cocd, "exp", exp_name)
	layout = data_cfg["fbl3n_layout"]
	off_days = recon_cfg["holidays"]
	curr_date = dat.get_current_date()
	from_date, to_date = dat.calculate_export_dates(curr_date, off_days)
	exported = False

	fbl3n.start(sess)

	_logger.info(f"Exporting FBL3N data from date: {from_date.strftime('%d.%m.%Y')} to date: {to_date.strftime('%d.%m.%Y')} ...")
	n_attempts = 3
	nth = 0

	try:
		fbl3n.export(exp_path, cocd, accs, from_date, to_date, layout)
	except fbl3n.SapRuntimeError as exc:
		_logger.exception(exc)
		_logger.critical("Data export from FBL3N failed!")
		_logger.info(f"Attempt ({nth} of {n_attempts}) to handle the error ...")

		while nth < n_attempts:
			try:
				_logger.info("Exporting data from FBL3N ...")
				fbl3n.export(exp_path, cocd, accs, from_date, to_date, layout)
			except fbl3n.SapRuntimeError as exc2:
				_logger.exception(exc2)
				_logger.critical("Data export from FBL3N failed!")
				_logger.info(f"Attempt ({nth} of {n_attempts}) to handle the error ...")
				nth += 1
			else:
				break

	except fbl3n.NoDataFoundWarning as wng:
		_logger.warning(wng)
	except Exception as exc:
		_logger.exception(exc)
	finally:
		fbl3n.close()

	if nth < n_attempts:
		_logger.info("Data export completd.")
		rec.save_state(cntry, "fbl3n_data_exported", True)
		exported = True

	return exported

def process_fbl3n_data(db_cfg: dict, data_cfg: dict, recon_cfg: dict,
					   rules: dict, cntry: str) -> bool:
	"""
	Manages reading, parsing and evaluation and storing
	of the exported FBL3N data to database.

	Params:
	-------
	data_cfg:
		Application 'data' configuration parameters.

	recon_cfg:
		Application 'reconciliation' configuration parameters.

	rules:
		Country names mapped to their respective reconciliation rules.

	cntry:
		Name of the country to reconcile.

	Returns:
	--------
	True if data processing succeeds, False if it fails.
	"""

	cocd = rules[cntry]["company_code"]

	exp_name = data_cfg["fbl3n_data_export_name"].replace("$cocd$", cocd)
	bin_name = data_cfg["fbl3n_data_bin_name"].replace("$cocd$", cocd)

	tmp_dir = data_cfg["temp_dir"]
	dat_dir = join(tmp_dir, cocd)
	bin_path = join(dat_dir, "bin", bin_name)
	exp_path = join(dat_dir, "exp", exp_name)
	off_days = recon_cfg["holidays"]
	curr_date = dat.get_current_date()
	schema = db_cfg["schema"]
	money_curr = db_cfg["lc_monetary"]

	if rec.get_state(cntry, "fbl3n_data_processed"):
		_logger.warning("FBL3N data preprocessing skipped. "
		"Reason: Data already preprocessed in the previous run.")
		proc.store_to_accum(proc.read_binary_file(bin_path), cntry, "fbl3n_data")
	else:

		try:
			preproc = proc.convert_fbl3n_data_opt(exp_path)
		except Exception as exc:
			_logger.exception(exc)
			return False

		if preproc is None:
			return False

		proc.store_to_accum(preproc, cntry, "fbl3n_data")
		proc.store_to_binary(preproc, bin_path)
		rec.save_state(cntry, "fbl3n_data_processed", True)

	# update database on the processed FBL3N data
	if rec.get_state(cntry, "db_updated"):
		_logger.info("Database updating skipped since data was already stored in the previous run.")
	else:

		date_from = dat.calculate_export_dates(curr_date, off_days)[0]

		_logger.info("Updating database records ...")
		_logger.info(f" - Deleting old records from database where posting date >= {date_from.strftime('%d.%m.%Y')} ...")

		if not db.delete_data(_pg_conn, cocd, schema, date_from):
			return False

		_logger.info(" - Resetting table sequencer ...")
		db.reset_sequence(_pg_conn, schema, cocd)

		data = proc.get_from_accum(cntry, "fbl3n_data")

		_logger.info(" - Storing records to database ...")
		if not db.store_data(_pg_conn, schema, cocd,data, money_curr):
			return False

		rec.save_state(cntry, "db_updated", True)

	return True

def load_data_to_database(file_path: str, db_params: dict, cocd: str) -> bool:
	"""
	TBA ...
	"""

	_logger.info("Converting exported FBL3N data ...")
	data = proc.convert_fbl3n_data(file_path)
	_logger.info("Data conversion completed.")

	_logger.info("Storing data records to database ...")
	if not db.store_data(_pg_conn, db_params["schema"], cocd, data, db_params["lc_monetary"]):
		return False

	_logger.info("Records successfully stored.")

	_logger.info("Resetting table sequencer ...")
	if not db.reset_sequence(_pg_conn, db_params["schema"], cocd):
		return False

	_logger.info("Sequencer was reset.")

	return True

def export_se16_kote_data(data_cfg: dict, rules: dict, cntry: str, sess: CDispatch) -> bool:
	"""
	Manages data export from the SE16 KOTE table into a local text file.

	Params:
	-------
	data_cfg:
		Application 'data' configuration parameters.

	rules:
		Country names mapped to their respective reconciliation rules.

	cntry:
		Name of the country to reconcile.

	sess:
		A SAP GuiSession object.

	Returns:
	--------
	True if data export succeeds, False if it fails.
	"""

	cocd = rules[cntry]["company_code"]
	sal_org_hq = rules[cntry]["sales_organization_glob"]
	sales_offs = rules[cntry]["sales_offices"]
	dat_dir = join(data_cfg["temp_dir"], cocd)
	exp_name = data_cfg["se16_kote_data_export_name"].replace("$cocd$", cocd)
	exp_path = join(dat_dir, "exp", exp_name)
	exported = False

	if rec.get_state(cntry, "se16_kote_data_exported"):
		_logger.warning("SE16 KOTE890 data export skipped. Reason: Data already exported in the previous run.")
		return True

	se16.start(sess)

	_logger.info("Exporting data from 'KOTE890' table ...")

	try:
		se16.export(exp_path, se16.Tables.KOTE, sal_org_hq, tuple(sales_offs))
	except Exception as exc:
		_logger.error(str(exc))
	else:
		rec.save_state(cntry, "se16_kote_data_exported", True)
		exported = True
	finally:
		se16.close()

	return exported

def export_se16_kona_data(data_cfg: dict, rules: dict, cntry: str, sess: CDispatch) -> bool:
	"""
	Manages data export from the SE16 KONA table into a local text file.

	Params:
	-------
	data_cfg:
		Application 'data' configuration parameters.

	rules:
		Country names mapped to their respective reconciliation rules.

	cntry:
		Name of the country to reconcile.

	sess:
		A SAP GuiSession object.

	Returns:
	--------
	True if data export succeeds, False if it fails.
	"""

	cocd = rules[cntry]["company_code"]
	sal_org_hq = rules[cntry]["sales_organization_glob"]
	dat_dir = join(data_cfg["temp_dir"], cocd)
	exp_name = data_cfg["se16_kona_data_export_name"].replace("$cocd$", cocd)
	exp_path = join(dat_dir, "exp", exp_name)
	agr_nums = proc.get_se16_agreements(proc.get_from_accum(cntry, "kote_data"))

	if rec.get_state(cntry, "se16_kona_data_exported"):
		_logger.warning("SE16 KONA data export skipped. "
		"Reason: Data already exported in the previous run.")
		return True

	se16.start(sess)

	_logger.info("Exporting data from 'KONA' table ...")

	try:
		se16.export(exp_path, se16.Tables.KONA, sal_org_hq, agreements = agr_nums)
	except se16.NoDataFoundWarning as wng:
		_logger.warning(wng)
		rec.save_state(cntry, "se16_no_kona_data", True)
		success = True
	except Exception as exc:
		_logger.exception(exc)
		success = False
	else:
		rec.save_state(cntry, "se16_kona_data_exported", True)
		success = True
	finally:
		se16.close()

	return success

def process_se16_kote_data(data_cfg: dict, rules: dict, cntry: str):
	"""
	Manages reading, parsing, and evaluation of the SE16 KOTE
	table data stored in a local text file.

	Params:
	-------
	data_cfg:
		Application 'data' configuration parameters.

	rules:
		Country names mapped to their respective reconciliation rules.

	cntry:
		Name of the country to reconcile.

	Returns:
	--------
	True if data processing succeeds, False if it fails.
	"""

	cocd = rules[cntry]["company_code"]

	exp_name = data_cfg["se16_kote_data_export_name"].replace("$cocd$", cocd)
	bin_name = data_cfg["se16_kote_data_bin_name"].replace("$cocd$", cocd)

	exp_path = join(data_cfg["temp_dir"], cocd, "exp", exp_name)
	bin_dir = join(data_cfg["temp_dir"], cocd, "bin")
	bin_path = join(bin_dir, bin_name)

	if rec.get_state(cntry, "se16_kote_data_processed"):
		_logger.warning("SE16 KOTE890 data processing skipped. "
		"Reason: Data aready processed in the previous run.")
		proc.store_to_accum(proc.read_binary_file(bin_path), cntry, "kote_data")
		return

	preproc = proc.convert_se16_kote(exp_path)

	proc.store_to_binary(preproc, bin_path)
	proc.store_to_accum(preproc, cntry, "kote_data")
	rec.save_state(cntry, "se16_kote_data_processed", True)

def process_se16_kona_data(data_cfg: dict, rules: dict, cntry: str):
	"""
	Manages reading, parsing, and evaluation of the SE16 KONA
	table data stored in a local text file.

	Params:
	-------
	data_cfg:
		Application 'data' configuration parameters.

	rules:
		Country names mapped to their respective reconciliation rules.

	cntry:
		Name of the country to reconcile.

	Returns:
	--------
	True if data processing succeeds, False if it fails.
	"""

	cocd = rules[cntry]["company_code"]

	exp_name = data_cfg["se16_kona_data_export_name"].replace("$cocd$", cocd)
	bin_name = data_cfg["se16_kona_data_bin_name"].replace("$cocd$", cocd)

	exp_path = join(data_cfg["temp_dir"], cocd, "exp", exp_name)
	bin_dir = join(data_cfg["temp_dir"], cocd, "bin")
	bin_path = join(bin_dir, bin_name)

	if rec.get_state(cntry, "se16_kona_data_processed"):
		_logger.warning("SE16 KONA data processing skipped. "
		"Reason: Data aready processed in the previous run.")
		proc.store_to_accum(proc.read_binary_file(bin_path), cntry, "kona_data")
		return

	if rec.get_state(cntry, "se16_no_kona_data"):
		_logger.warning("SE16 KONA data processing skipped. "
		"Reason: Data search returned no results.")
		proc.store_to_accum(None, cntry, "kona_data")
		return

	preproc = proc.convert_se16_kona(exp_path)

	proc.store_to_binary(preproc, bin_path)
	proc.store_to_accum(preproc, cntry, "kona_data")
	rec.save_state(cntry, "se16_kona_data_processed", True)

def export_zsd25_local_data(data_cfg: dict, rules: dict, cntry: str, sess: CDispatch) -> bool:
	"""
	Manages data export of local entity bonus data
	from ZSD25_T125 transaction into a local text file.

	Params:
	-------
	data_cfg:
		Application 'data' configuration parameters.

	rules:
		Country names mapped to their respective reconciliation rules.

	cntry:
		Name of the country to reconcile.

	sess:
		A SAP GuiSession object.

	Returns:
	--------
	True if data export succeeds, False if it fails.
	"""

	cocd = rules[cntry]["company_code"]
	sal_org = rules[cntry]["sales_organization_loc"]
	agree_stats = ("A", "B", "C", "")

	exp_dir = join(data_cfg["temp_dir"], cocd, "exp")
	exp_name = data_cfg["zsd25_local_data_export_name"].replace("$cocd$", cocd)
	exp_path = join(exp_dir, exp_name)
	layout = data_cfg["zsd25_layout"]
	exported = True

	if layout != "BONUS_RECON":
		_logger.warning(f"Expected layout name: 'BONUS_RECON' but '{layout}' used.")

	if rec.get_state(cntry, "zsd25_loc_data_exported"):
		_logger.warning("ZSD25 local data export skipped. "
		"Reason: Data already exported in the previous run.")
		return True

	zsd25.start(sess)

	_logger.info("Exporting local entity bonus data from ZSD25_T125 ...")

	try:
		zsd25.export(exp_path, True, layout, sal_org, agree_stats)
	except Exception as exc:
		_logger.exception(exc)
	else:
		rec.save_state(cntry, "zsd25_loc_data_exported", True)
	finally:
		zsd25.close()

	return exported

def export_zsd25_global_data(data_cfg: dict, rules: dict, cntry: str, sess: CDispatch) -> bool:
	"""
	Manages data export of headquarter bonus data
	from the ZSD25_T125 transaction into a local text file.

	Params:
	-------
	data_cfg:
		Application 'data' configuration parameters.

	rules:
		Country names mapped to their respective reconciliation rules.

	cntry:
		Name of the country to reconcile.

	sess:
		A SAP GuiSession object.

	Returns:
	--------
	True if data export succeeds, False if it fails.
	"""

	cocd = rules[cntry]["company_code"]
	sal_offs = rules[cntry]["sales_offices"]
	sal_org_glob = rules[cntry]["sales_organization_glob"]
	agree_stats = ("A", "B", "C", "")

	exp_dir = join(data_cfg["temp_dir"], cocd, "exp")
	exp_name = data_cfg["zsd25_global_data_export_name"].replace("$cocd$", cocd)
	exp_path = join(exp_dir, exp_name)
	layout = data_cfg["zsd25_layout"]

	if layout != "BONUS_RECON":
		_logger.warning(f"Expected layout name: 'BONUS_RECON' but '{layout}' used.")

	if rec.get_state(cntry, "se16_no_kona_data"):
		_logger.warning("Head quarter bonus data export from ZSD25 skipped since "
		"the SE16 KONA data search returned no results.")
		return True

	if rec.get_state(cntry, "zsd25_glob_data_exported"):
		_logger.warning("Head quarter bonus data export from ZSD25 skipped since "
		"the data already exported in the previous run.")
		return True

	if rec.get_state(cntry, "zsd25_no_glob_data"):
		_logger.warning("Head quarter bonus data export from ZSD25 skipped since "
		"the previous ZSD25 search returned no data for this type of bonus.")
		return True

	zsd25.start(sess)

	# with KONA data available, get all the agreement numbers
	agr_nums = proc.get_se16_agreements(proc.get_from_accum(cntry, "kona_data"))
	exported = True

	_logger.info("Exporting head quarter bonus data from ZSD25_T125 ...")

	try:
		zsd25.export(exp_path, True, layout, sal_org_glob, agree_stats, agr_nums, tuple(sal_offs))
	except zsd25.NoDataFoundWarning as wng:
		# no global data found does not mean failure!
		_logger.warning(wng)
		rec.save_state(cntry, "zsd25_no_glob_data", True)
	except Exception as exc:
		_logger.exception(exc)
	else:
		rec.save_state(cntry, "zsd25_glob_data_exported", True)
	finally:
		zsd25.close()

	return exported

def process_zsd25_local_data(data_cfg: dict, rules: dict, cntry: str):
	"""
	Manages reading, parsing, and evaluation of the ZSD25
	local entity bonus data stored in a local text file.

	Params:
	-------
	data_cfg:
		Application 'data' configuration parameters.

	rules:
		Country names mapped to their respective reconciliation rules.

	cntry:
		Name of the country to reconcile.

	Returns:
	--------
	True if data processing succeeds, False if it fails.
	"""

	cocd = rules[cntry]["company_code"]

	exp_dir = data_cfg["exports_dir"].replace("$cocd$", cocd)
	bin_dir = data_cfg["binaries_dir"].replace("$cocd$", cocd)

	exp_name = data_cfg["zsd25_local_data_export_name"].replace("$cocd$", cocd)
	bin_name = data_cfg["zsd25_local_data_bin_name"].replace("$cocd$", cocd)
	cond_bin_name = data_cfg["zsd25_local_conditions_data_bin_name"].replace("$cocd$", cocd)

	exp_path = join(exp_dir, exp_name)
	bin_path = join(bin_dir, bin_name)
	cond_bin_path = join(bin_dir, cond_bin_name)

	if rec.get_state(cntry, "zsd25_loc_data_processed"):
		_logger.warning("ZSD25 local data processing skipped. "
		"Reason: Data aready processed in the previus run.")
		proc.store_to_accum(proc.read_binary_file(bin_path), cntry, "loc_bonus_data")
		proc.store_to_accum(proc.read_binary_file(cond_bin_path), cntry, "loc_conditions")
		return

	preproc, preproc_conds = proc.convert_zsd25_loc_data(exp_path)

	proc.store_to_accum(preproc, cntry, "loc_bonus_data")
	proc.store_to_accum(preproc_conds, cntry, "loc_conditions")

	proc.store_to_binary(preproc, bin_path)
	proc.store_to_binary(preproc_conds, cond_bin_path)

	rec.save_state(cntry, "zsd25_loc_data_processed", True)

def process_zsd25_global_data(data_cfg: dict, rules: dict, cntry: str):
	"""
	Manages reading, parsing, and evaluation of the ZSD25 headquarter
	bonus data stored in a local text file.

	Params:
	-------
	data_cfg:
		Application 'data' configuration parameters.

	rules:
		Country names mapped to their respective reconciliation rules.

	cntry:
		Name of the country to reconcile.

	Returns:
	--------
	True if data processing succeeds, False if it fails.
	"""

	cocd = rules[cntry]["company_code"]
	sal_org_loc = rules[cntry]["sales_organization_loc"]
	exp_name = data_cfg["zsd25_global_data_export_name"].replace("$cocd$", cocd)
	bin_name = data_cfg["zsd25_global_data_bin_name"].replace("$cocd$", cocd)
	dat_dir = join(data_cfg["temp_dir"], cocd)
	exp_path = join(dat_dir, "exp", exp_name)
	bin_path = join(dat_dir, "bin", bin_name)

	if rec.get_state(cntry, "se16_no_kona_data"):
		_logger.warning("ZSD25 head quarter bonus data processing skipped since "
		"no records were found in SE16 KONA table for this type of bonus.")
		proc.store_to_accum(None, cntry, "glob_bonus_data")
		return

	if rec.get_state(cntry, "zsd25_no_glob_data"):
		_logger.warning("ZSD25 head quarter bonus data processing skipped since "
		"no records were found for this type of bonus.")
		proc.store_to_accum(None, cntry, "glob_bonus_data")
		return

	if rec.get_state(cntry, "zsd25_glob_data_processed"):
		_logger.warning("ZSD25 head quarter bonus data processing skipped since "
		"the data was already processed in the previous run.")
		proc.store_to_accum(proc.read_binary_file(bin_path), cntry, "glob_bonus_data")
		return

	preproc = proc.convert_zsd25_glob_data(exp_path, sal_org_loc)

	proc.store_to_accum(preproc, cntry, "glob_bonus_data")
	proc.store_to_binary(preproc, bin_path)
	rec.save_state(cntry, "zsd25_glob_data_processed", True)

def export_fs10n_data(data_cfg: dict, rules: dict, recon_cfg: dict, cntry: str, sess: CDispatch) -> bool:
	"""
	Manages accounting data export from the FS10N transaction into a local text file.

	Params:
	-------
	data_cfg:
		Application 'data' configuration parameters.

	rules:
		Country names mapped to their respective reconciliation rules.

	recon_cfg:
		Application 'reconciliation' configuration parameters.

	cntry:
		Name of the country to reconcile.

	sess:
		A SAP GuiSession object.

	Returns:
	--------
	True if data export succeeds, False if it fails.
	"""

	# get the fiscal year for the month being reconciled
	result = dat.calculate_reconciliation_times(dat.get_current_date(), recon_cfg["holidays"])
	fisc_year = result["fiscal_year"]

	# commence data export
	cocd = rules[cntry]["company_code"]
	accs = rules[cntry]["accounts"]
	dat_dir = join(data_cfg["temp_dir"], cocd)
	bas_exp_name = data_cfg["fs10n_data_export_name"]

	fs10n.start(sess)

	# perform data export separately for each account being reconciled
	for acc in list(map(str, accs)):

		exp_name = bas_exp_name.replace("$cocd$", cocd).replace("$acc$", acc)
		exp_path = join(dat_dir, "exp", exp_name)

		if rec.get_state(cntry, "fs10n_data_exported", acc) == "data_nonexist":
			_logger.warning(f"FS10N data export for account {acc} skipped since "
			"the data search returned no results in the previous run.")
			continue

		if rec.get_state(cntry, "fs10n_data_exported", acc):
			_logger.warning(f"FS10N data export for account {acc} skipped since "
			"tha data was already exported in the previous run.")
			continue

		_logger.info(f"Exporting FS10N data for account: {acc} ...")

		try:
			fs10n.export(exp_path, acc, cocd, fisc_year)
		except fs10n.NoDataFoundWarning:
			rec.save_state(cntry, "fs10n_data_exported", "data_nonexist", acc)
		except Exception as exc:
			# continuing the country recon has no point
			# if any of the accounts remains unprocessed
			_logger.exception(exc)
			fs10n.close()
			return False
		else:
			rec.save_state(cntry, "fs10n_data_exported", True, acc)

	fs10n.close()

	return True

def process_fs10n_data(data_cfg: dict, rules: dict, cntry: str) -> None:
	"""
	Manages reading, parsing, and evaluation of the FS10N accounting
	data stored in a local text file.

	Params:
	-------
	data_cfg:
		Application 'data' configuration parameters.

	rules:
		Country names mapped to their respective reconciliation rules.

	cntry:
		Name of the country to reconcile.

	Returns:
	--------
	True if data processing succeeds, False if it fails.
	"""

	accs = rules[cntry]["accounts"]
	cocd = rules[cntry]["company_code"]
	dat_dir = join(data_cfg["temp_dir"], cocd)
	bas_exp_name = data_cfg["fs10n_data_export_name"].replace("$cocd$", cocd)
	bas_bin_name = data_cfg["fs10n_data_bin_name"].replace("$cocd$", cocd)

	for acc in list(map(str, accs)):

		exp_name = bas_exp_name.replace("$acc$", acc)
		bin_name = bas_bin_name.replace("$acc$", acc)
		exp_path = join(dat_dir, "exp", exp_name)
		bin_path = join(dat_dir, "bin", bin_name)

		if rec.get_state(cntry, "fs10n_data_exported", acc) == "data_nonexist":
			_logger.warning(f"FS10N data conversion not applicable for account {acc} since "
			"the data search returned no results.")
			proc.store_to_accum(None, cntry, "fs10n_data", acc)
			continue

		if rec.get_state(cntry, "fs10n_data_processed", acc):
			_logger.warning(f"FS10N data conversion not applicable for account {acc} since "
			"the data was already converted in the previous run.")
			proc.store_to_accum(proc.read_binary_file(bin_path), cntry, "fs10n_data", acc)
			continue

		_logger.info(f"Converting FS10N data exported from account: {acc} ...")
		preproc = proc.convert_fs10n_data(exp_path)

		proc.store_to_accum(preproc, cntry, "fs10n_data", acc)
		proc.store_to_binary(preproc, bin_path)
		rec.save_state(cntry, "fs10n_data_processed", True, acc)

def reconcile(
		db_cfg: dict, data_cfg: dict, recon_cfg: dict,
		rules: dict, cntry: str, user_params: dict) -> bool:
	"""
	Manages final data calculations, referred to as the reconciliation process.

	Params:
	-------
	db_cfg:
		Applicatin 'databasep configuration parameters.

	data_cfg:
		Application 'data' configuration parameters.

	rules:
		Country names mapped to their respective reconciliation rules.

	recon_cfg:
		Application 'reconciliation' configuration parameters.

	cntry:
		Name of the country to reconcile.

	Returns:
	--------
	True if the country reconciliation succeeds, False if it fails.
	"""

	_logger.info("Performing reconcilition operations ...")

	off_days = recon_cfg["holidays"]
	recon_time = dat.get_current_time()
	recon_time_fmt = recon_time.strftime("%I:%M:%S %p")
	result = dat.calculate_reconciliation_times(dat.get_current_date(), off_days)
	_logger.info(f"Reconciliation times: {result}")

	fisc_year = result["fiscal_year"]
	fisc_period = result["fiscal_period"]
	recon_date = result["reconciliation_date"]
	fx_rate_date = result["conversion_date"]

	recon_date_fmt = recon_date.strftime("%d-%b-%Y")
	accs = list(map(str, rules[cntry]["accounts"]))
	cocd = rules[cntry]["company_code"]
	loc_curr = rules[cntry]["local_currency"]
	curr_date = dat.get_current_date()
	fx_rate = None
	is_uplusone = curr_date == dat.get_ultimo_plus_one(curr_date, off_days)
	_logger.info(f"Date is an ultimo plus one day: {is_uplusone}")

	if user_params is not None and "fx_rate" in user_params:
		fx_rate = user_params["fx_rate"]
		_logger.warning(f" A user-entered exchange rate {fx_rate} will be used for calculations.")
	else:
		try:
			fx_rate = ptl.get_exchange_rate(fx_rate_date, loc_curr)
		except TimeoutError as exc:
			_logger.error(f"Handling error: '{str(exc)}' ...")
			fx_rate = ptl.get_exchange_rate(fx_rate_date, loc_curr, resp_timeout = 60)
		except ptl.ResponseError as exc:
			_logger.error(f"Handling error: '{str(exc)}' ...")
		except Exception as exc:
			_logger.error(f"Unhandled exception: '{str(exc)}'")
			return False

	# tricky part - on some days there may be no rates available
	# on the web portal at the time of recon yet. If the recon was
	# triggred by a user request, then get the rates published on
	# the previous day and include the information to the user notification.
	if fx_rate is None and not is_uplusone:

		MAX_PAST_DAYS = 5 # keep it at a minimum of 5 due to possible occurence of holidays
		past_days = 1

		# try to get an exchange rate from recent days
		while fx_rate is None and past_days <= MAX_PAST_DAYS:

			prev_day = fx_rate_date - timedelta(past_days)
			past_days += 1

			try:
				fx_rate = ptl.get_exchange_rate(prev_day, loc_curr)
			except TimeoutError as exc:
				_logger.error(f"Handling error: '{str(exc)}' ...")
				fx_rate = ptl.get_exchange_rate(fx_rate_date, loc_curr, resp_timeout = 60)
			except ptl.ResponseError as exc:
				_logger.error(f"Handling error: '{str(exc)}' ...")
				past_days += 1
			except Exception as exc:
				_logger.error(f"Unhandled exception: '{str(exc)}'")
				return False

		if fx_rate is None:
			warn_msg_upone = f"""The exchange rate was not available on the portal for the given day {recon_date_fmt} at {recon_time_fmt}.\n
								 Please, send a trigger mail with a manually entered fx rate to retry the reconciliation."""

			_logger.warning(f"The exchange rate was not available on the portal for the given day {recon_date_fmt}.")
			rec.save_state(cntry, "user_error", f"The exchange rate was not available on the portal for the given day {recon_date_fmt} at {recon_time_fmt}.")
			return False

		prev_day_fmt = prev_day.strftime("%d-%b-%Y")
		warn_msg_nonupone = f"""The exchange rate was not available on the portal for the given day {recon_date_fmt} at {recon_time_fmt}.\n
								An exchange rate as per {prev_day_fmt} was used instead. If you wish to use a different rate, please,
								send a trigger mail with a manually entered FX rate to retry the reconciliation."""

		_logger.warning(f"The exchange rate was not available on the portal for the given day {recon_date_fmt} "
						f"at {recon_time_fmt}. An exchange rate as per {prev_day_fmt} was used instead.")

		rec.save_state(cntry, "user_warning", warn_msg_nonupone)

	elif fx_rate is None and is_uplusone:
		_logger.error(f" Currency conversion failed! The exchange rate was not available on the portal for the given day {recon_date_fmt}")
		warn_msg_upone = f"""The exchange rate was not available on the portal for the given day {recon_date_fmt} at {recon_time_fmt}.
							 Please, send a trigger mail with a manually entered FX rate to retry the reconciliation."""
		rec.save_state(cntry, "user_error", warn_msg_upone)

		return False

	_logger.debug(f"Exchange rate: {fx_rate}")

	bin_dir_path = join(data_cfg["temp_dir"], cocd, "bin")

	loc_calc_bin_name = data_cfg["zsd25_local_calcs_bin_name"].replace("$cocd$", cocd)
	loc_calc_bin_path = join(bin_dir_path, loc_calc_bin_name)

	glob_calc_bin_name = data_cfg["zsd25_global_calcs_bin_name"].replace("$cocd$", cocd)
	glob_calc_bin_path = join(bin_dir_path, glob_calc_bin_name)

	bon_summ_bin_name = data_cfg["bonus_data_summary_bin_name"].replace("$cocd$", cocd)
	bon_summ_bin_path = join(bin_dir_path, bon_summ_bin_name)

	year_summ_bin_name = data_cfg["yearly_accounts_summary_bin_name"].replace("$cocd$", cocd)
	year_summ_bin_path = join(bin_dir_path, year_summ_bin_name)

	schema = db_cfg["schema"]

	if rec.get_state(cntry, "yearly_summary_retrieved"):
		_logger.warning("Yearly account data summary already retrieved in the previous run.")
		proc.store_to_accum(proc.read_binary_file(year_summ_bin_path), cntry, "yearly_acc_summ")
	else:
		try:
			_logger.info("Retrieving yearly data summary from database ...")
			yearly_summ = db.get_yearly_summary(_pg_conn, schema, cocd)
			assert not yearly_summ.empty, "Argument 'yearly_data' contains no records!"
		except Exception as exc:
			_logger.exception(exc)
			return False
		else:
			proc.store_to_accum(yearly_summ, cntry, "yearly_acc_summ")
			proc.store_to_binary(yearly_summ, year_summ_bin_path)
			rec.save_state(cntry, "yearly_summary_retrieved", True)

	_logger.info("Creating period overview ...")
	period_overview = proc.create_period_overview(proc.get_from_accum(cntry, "yearly_acc_summ"))
	proc.store_to_accum(period_overview, cntry, "period_overview")
	# do not dump this data as data recalc is not computationally demanding

	_logger.info("Retrieving text summaries from database ...")
	for acc in accs:

		txt_bin_name = data_cfg["text_summary_bin_name"].replace("$cocd$", cocd).replace("$acc$", acc)
		txt_bin_path = join(bin_dir_path, txt_bin_name)

		if rec.get_state(cntry, "text_summary_retrieved", acc):
			_logger.warning(f" Data summarization for account {acc} skipped. "
			"Reason: Data already summarized in the previous run.")
			proc.store_to_accum(proc.read_binary_file(txt_bin_path), cntry, "text_summs", acc)
		else:
			try:
				txt_summ = db.get_text_summary(_pg_conn, schema, cocd, acc)
			except Exception as exc:
				_logger.exception(exc)
				return False

			proc.store_to_binary(txt_summ, txt_bin_path)
			proc.store_to_accum(txt_summ, cntry, "text_summs", acc)
			rec.save_state(cntry, "text_summary_retrieved", True, acc)

	if rec.get_state(cntry, "zsd25_no_glob_data"):
		_logger.warning("Data calculations for head quarter bonuses skipped. "
		"Reason: ZSD25 data search returned no results.")
		proc.store_to_accum(None, cntry, "glob_bonus_calcs")
	elif rec.get_state(cntry, "se16_no_kona_data"):
		_logger.warning("Data calculations for head quarter bonuses skipped. "
		"Reason: SE16 KONA data search returned no results.")
		proc.store_to_accum(None, cntry, "glob_bonus_calcs")
	elif rec.get_state(cntry, "zsd25_glob_data_calculated"):
		_logger.warning("Data calculations for head quarter bonuses skipped. "
		"Reason: Calculations aready performed in the previous run.")
		proc.store_to_accum(proc.read_binary_file(glob_calc_bin_path), cntry, "glob_bonus_calcs")
	else:
		glob_calc = proc.calculate_hq_bonus_data(
			proc.get_from_accum(cntry, "text_summs"),
			proc.get_from_accum(cntry, "glob_bonus_data"),
			loc_curr, fx_rate
		)

		rec.save_state(cntry, "zsd25_glob_data_calculated", True)
		proc.store_to_accum(glob_calc, cntry, "glob_bonus_calcs")
		proc.store_to_binary(glob_calc, glob_calc_bin_path)

	if rec.get_state(cntry, "zsd25_loc_data_calculated"):
		_logger.warning(" Data calculations for local entity bonuses skipped. "
		"Reason: Calculations already performed in the previous run.")
		loc_calc = proc.read_binary_file(loc_calc_bin_path)
	else:

		loc_calc = proc.calculate_le_bonus_data(
			proc.get_from_accum(cntry, "text_summs"),
			proc.get_from_accum(cntry, "loc_bonus_data"),
			loc_curr, fx_rate
		)

	if cocd == "1001":
		loc_calc, hq_compared, le_compared = proc.consolidate_zsd25_data(
			loc_calc, proc.get_from_accum(cntry, "glob_bonus_calcs")
		)
		proc.store_to_accum(hq_compared, cntry, "glob_agreement_comparison")
		proc.store_to_accum(le_compared, cntry, "loc_agreement_comparison")
	else:
		proc.store_to_accum(None, cntry, "glob_agreement_comparison")
		proc.store_to_accum(None, cntry, "loc_agreement_comparison")

	rec.save_state(cntry, "zsd25_loc_data_calculated", True)
	proc.store_to_binary(loc_calc, loc_calc_bin_path)
	proc.store_to_accum(loc_calc, cntry, "loc_bonus_calcs")

	check_text_summs = proc.check_agreement_states(
		proc.get_from_accum(cntry, "text_summs"),
		proc.get_from_accum(cntry, "loc_bonus_data"),
		proc.get_from_accum(cntry, "glob_bonus_data")
	)

	proc.store_to_accum(check_text_summs, cntry, "check_text_summs")

	ledger_summary = proc.summarize(
		proc.get_from_accum(cntry, "check_text_summs"),
		proc.get_from_accum(cntry, "loc_bonus_calcs"),
		proc.get_from_accum(cntry, "glob_bonus_calcs"),
		proc.get_from_accum(cntry, "fs10n_data"),
		accs, fisc_period
	)

	proc.store_to_accum(ledger_summary, cntry, "final_summary")
	proc.store_to_binary(ledger_summary, bon_summ_bin_path)

	info = proc.compile_recon_info(
		cntry, cocd, fx_rate, loc_curr, fisc_year, fisc_period, accs,
		rules[cntry]["sales_offices"],
		rules[cntry]["sales_organization_glob"],
		rules[cntry]["sales_organization_loc"],
		recon_date, recon_time
	)

	proc.store_to_accum(info, cntry, "info")

	return True

def generate_report(rep_cfg: dict, rules: dict, cntry: str) -> bool:
	"""
	Manages the generation of user reports from the data
	generated in the process of reconciliation.

	Params:
	-------
	rep_cfg:
		Application 'reports' configuration parameters.

	rules:
		Country names mapped to their respective reconciliation rules.

	cntry:
		Name of the reconciled country.

	Returns:
	--------
	True if the report is successfully generated, otherwise False.
	"""

	_logger.info("Generating report ...")

	cocd = rules[cntry]["company_code"]

	loc_dir_path = rep_cfg["local_dir"]
	bas_rep_name = rep_cfg["report_name"]
	rep_path = join(loc_dir_path, bas_rep_name.replace("$cocd$", cocd))

	try:
		report.create(rep_path,
			kote_data = proc.get_from_accum(cntry, "kote_data"),
			kona_data = proc.get_from_accum(cntry, "kona_data"),
			glob_bonus_data = proc.get_from_accum(cntry, "glob_bonus_data"),
			loc_bonus_data = proc.get_from_accum(cntry, "loc_bonus_data"),
			loc_bonus_calcs = proc.get_from_accum(cntry, "loc_bonus_calcs"),
			loc_conditions_data = proc.get_from_accum(cntry, "loc_conditions"),
			glob_bonus_calcs = proc.get_from_accum(cntry, "glob_bonus_calcs"),
			final_summary = proc.get_from_accum(cntry, "final_summary"),
			period_overview = proc.get_from_accum(cntry, "period_overview"),
			check_text_summs = proc.get_from_accum(cntry, "check_text_summs"),
			hq_comparison = proc.get_from_accum(cntry, "glob_agreement_comparison"),
			le_comparison = proc.get_from_accum(cntry, "loc_agreement_comparison"),
			info = proc.get_from_accum(cntry, "info")
		)
	except Exception as exc:
		_logger.exception(exc)
		return False

	return True

def send_notification(
		msg_cfg: dict, rep_cfg: dict, rules: dict = None,
		cntry: str = None, user_params: dict = None) -> bool:
	"""
	Manages sending of user notification.

	If reconciliation was successfully performed and the report was generated, \n
	this will be then attached to the user notfication. If the reconciliation \n
	wasn't completed due to an error, no report will be attached, but a notification \n
	detailing the nature of the error will be sent to the user.

	Params:
	-------
	msg_cfg:
		Application 'messages' configuration parameters.

	rep_cfg:
		Application 'reports' configuration parameters.

	rules:
		Country names mapped to their respective reconciliation rules.

	cntry:
		Name of the reconciled country.

	user_params:
		Reconciliation parameters extracted from \n
		the user email that triggered the processing.

	Returns:
	--------
	True if the notification is successfully sent, False if not.
	"""

	notif_cfg = msg_cfg["notifications"]

	if not notif_cfg["send"]:
		_logger.warning("Sending of notifications to users is disabled in 'appconfig.yaml'.")
		return

	_logger.info("Sending notification to user ...")

	warn_msg = ""
	err_msg = ""

	if user_params is None:

		cocd = rules[cntry]["company_code"]
		recipients = [usr["mail"] for usr in rules[cntry]["accountants"] if usr["send_message"]]

		if len(recipients) == 0:
			_logger.warning("A user notification will not be sent. "
			"Reason: Sending of notification to users is disabled in 'rules.yaml'.")
			return

		# so far, errors and warnings applicable
		# for user-triggered reconciliations only
		warn_msg = rec.get_state(cntry, "user_warning")
		err_msg = rec.get_state(cntry, "user_error")

	else:

		cocd = user_params["company_code"]
		recipients = user_params["email"]

		if cocd is None:
			err_msg = "The company code you've provided is incorrect!"
		elif "fx_rate" in user_params and user_params["fx_rate"] is None:
			err_msg = "The FX rate you've provided is incorrect!"

	# For simplicity, either warning or error message generation is
	# implemented. This decision can be changed in the future, if needed.
	assert not (warn_msg != "" and err_msg != ""), "Unsupported scenario!"
	# assert warn_msg != err_msg == "" consider this technique

	if warn_msg != "":
		templ_name = notif_cfg["templates"]["warning"]
	elif err_msg != "":
		templ_name = notif_cfg["templates"]["error"]
	else:
		templ_name = notif_cfg["templates"]["general"]

	templ_dir = notif_cfg["template_dir"]
	templ_path = join(templ_dir, templ_name)

	_logger.info("Loading notification template ...")

	try:
		with open(templ_path, 'r', encoding = "utf-8") as stream:
			html_body = stream.read()
	except Exception as exc:
		_logger.exception(exc)
		return

	if cntry is None:
		html_body = html_body.replace("$country$", "your country")
	else:
		html_body = html_body.replace("$country$", cntry)

	if warn_msg != "":
		rep_path = join(rep_cfg["local_dir"], rep_cfg["report_name"].replace("$cocd$", cocd))
		html_body = html_body.replace("$warn_msg$", warn_msg)
		msg = mail.create_message(notif_cfg["sender"], recipients, notif_cfg["subject"], html_body, rep_path)
	elif err_msg != "":
		html_body = html_body.replace("$error_msg$", err_msg)
		msg = mail.create_message(notif_cfg["sender"], recipients, notif_cfg["subject"], html_body)
	else: # recon OK
		rep_path = join(rep_cfg["local_dir"], rep_cfg["report_name"].replace("$cocd$", cocd))
		msg = mail.create_message(notif_cfg["sender"], recipients, notif_cfg["subject"], html_body, rep_path)

	try:
		mail.send_smtp_message(msg, notif_cfg["host"], notif_cfg["port"])
	except mail.UndeliveredWarning as wng:
		_logger.warning(wng)
	except Exception as exc:
		_logger.exception(exc)
		return False

	return True

def set_completed(cntry: str) -> None:
	"""
	Sets reconciliation for
	a country as complete.

	Params:
	-------
	cntry:
		The name of the country.

	Returns:
	--------
	None.
	"""

	rec.save_state(cntry, "reconciled", True)

def is_reconciled(cntry: str) -> bool:
	"""
	Checks if reconciliation for
	a country is complete.

	Params:
	-------
	cntry:
		Name of the country.

	Returns:
	--------
	True, if reconciliation for a country
	has been successfully completed, False if not.
	"""

	reconciled = rec.get_state(cntry, "reconciled")

	return reconciled

def delete_reports(rep_cfg: dict) -> None:
	"""
	Deletes user excel reports.

	Params:
	-------
	rep_cfg:
		Application 'reports' configuration parameters.

	Returns:
	--------
	None.
	"""

	dir_path = rep_cfg["local_dir"]
	file_paths = glob(join(dir_path, "*.xlsx*"))

	if len(file_paths) == 0:
		_logger.warning("No old user report was found.")
		return

	_logger.info("Deleting old user reports ...")
	for file_path in file_paths:
		try:
			remove(file_path)
		except Exception as exc:
			_logger.exception(exc)

def remove_temp_files(dir_path: str) -> None:
	"""
	Deletes temporary files generated
	during application runtime. \n
	Subfolders located in the root directory
	will not be removed.

	Params:
	-------
	dir_path:
		Path to the directory containing
		temporary folders and files.

	Returns:
	--------
	None.
	"""

	file_paths = glob(join(dir_path, "**", "*.*"), recursive = True)

	if len(file_paths) == 0:
		_logger.warning("Could not clear the temp folder "
		"since there were no files contained.")
		return

	_logger.info("Deleting temporary folders and files ...")

	for file_path in file_paths:
		try:
			remove(file_path)
		except Exception as exc:
			_logger.exception(exc)

	for subdir_path in [entry.path for entry in scandir(dir_path) if entry.is_dir()]:
		try:
			rmtree(subdir_path, ignore_errors = True)
		except Exception as exc:
			_logger.exception(exc)

def clear_recovery_states() -> None:
	"""
	Clears application recovery data.

	Params:
	-------
	None.

	Returns:
	--------
	None
	"""

	_logger.info("Clearing recovery states ...")

	rec.clear()

def clear_data_processor() -> None:
	"""
	Clears the content of the processor's
	data accumulator.

	Params:
	-------
	None.

	Returns:
	--------
	None
	"""

	_logger.info("Clearing data processor ...")

	proc.clear()

def run_benchmarks(bench_cfg: dict) -> None:
	"""
	Runs application performance benchmarks.

	Params:
	-------
	bench_cfg:
		Application 'benchmarks' configuration parameters.

	Returns:
	--------
	None.
	"""

	N_ROUNDS = 7
	run_date = datetime.now().strftime("%d%b%Y")
	data_dir = bench_cfg["data_dir"]
	result_dir = bench_cfg["result_dir"]
	result_name = bench_cfg["result_name"].replace("$rundate$", run_date)
	result_path = join(result_dir, result_name)

	utils.clear_results_log(result_path)
	utils.benchmark_dat_loading(join(data_dir, "sample.dat"), N_ROUNDS)
	utils.benchmark_txt_loading(join(data_dir, "sample.txt"), N_ROUNDS)
	utils.benchmark_feather_loading(join(data_dir, "sample.feather"), N_ROUNDS)
	print(f"Results stored to file '{result_path}'.")
