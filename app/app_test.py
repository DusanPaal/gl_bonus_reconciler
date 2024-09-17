# pylint: disable = C0103, C0123, C0301, E0401, E1121, R0911, R0912, R0915, W0703, W1203

"""
Description:
Integration test for the application
The 'app.py' module contains the main entry of the program
that controls application data and operation flow by using
more specialized servicing procedures from the biaServies.py
module.

Version history:

1.0.20211004
    - Initial version.

1.0.20220729
    biaMail.py:
        - Improved handling and logging of exceptional states after
          calling smtp_conn.sendmail() procedure in 'biaMail.py'.
        - Connection timeout increased from 7 to 30 secs to avoid
          timeouts when using low-speed internet.
    biaDatabase.py:
        - String queries to the PostgreSQL database are replaced by
          SQLAlchemy Core API constructs.
        - Improved data storing performance by replacing sequential record
          insertion with bulk record insertion into the database.

1.1.20220809
    Added a benchmarking feature to support profiling of performance-critical
    application regions such as loading and conversion of data files.
    app.py:
        - Added component execution control to the 'main()' procedure.
    biaServices.py:
        - Added 'run_benchmarks()' procedure.
    appconfig.yaml:
        - Added 'benchmarks' parameter section.

1.1.20220817
    biaSE16.py:
        - Added docstrings to enum classes.
        - export() procedure: Simplified parameter names, added new assertions for param checking.
    biaSE16.py:
        - export() procedure: Simplified parameter names, added new assertions for param checking,
                              tansaction now returns to initial window when no data is found.

"""

import logging
from os.path import join
import sys
import engine.biaController as ctrlr

_logger = logging.getLogger("master")

def main(args: dict) -> int:
    """
    Serves as the starting point and overall control for the application. \n
    If application launch was invoked by user email, all \n
    the necessary reconciliation params are fetched from \n
    the user message referenced by the email ID. If no \n
    parameters are provided, then reconciliation for all \n
    active countries will be performed as per settings in \n
    'rules.yaml'.

    Params:
    -------
    action:
        Action to be performed by the app \n
        (e.g. reconciliation, benchmarking, etc.).

    email_id:
        String identification code of the user
        message that has triggered reconciliation.

    debug:
        Indicates whether application
        should be run in debug mode.

    Returns:
    --------
    Program completion state.
    """

    logger_ok = ctrlr.initialize_logger(
        cfg_path = join(sys.path[0], "logging.yaml"),
        log_path = join(sys.path[0], "log.log"),
        debug = bool(args["debug"]),
        header = {
            "Application name": "GL Bonus Reconciler",
            "Application version": "1.1.20220817",
            "Log date": ctrlr.get_current_time("%d-%b-%Y")
        }
    )

    if not logger_ok:
        return 1

    _logger.info("=== Initialization ===")
    _logger.debug(f"Arguments passed to main(): {args}")

    cfg = ctrlr.load_app_config(join(sys.path[0], "appconfig.yaml"))

    if cfg is None:
        _logger.critical("Loading of configuration data failed.")
        return 2

    rules = ctrlr.load_reconciliation_rules(cfg["reconciliation"])

    if rules is None:
        _logger.critical("Loading of the reconciliation rules failed.")
        return 3

    if args["email_id"] is None:
        countries = ctrlr.get_active_countries(cfg["data"], rules)
        usr_params = None
    else:

        usr_params = ctrlr.get_user_params(cfg["messages"], args["email_id"])

        if usr_params is None:
            return 4

        if usr_params["incomplete"]:
            _logger.critical("The user message contains incorrect or no parameter(s).")
            ctrlr.send_notification(cfg["messages"], cfg["reports"], user_params = usr_params)
            return 5

        countries = ctrlr.get_active_countries(cfg["data"], rules, usr_params["company_code"])

    if countries is None: # common check for user-invoked and automatic application launch
        _logger.error("No active countries were discovered.")
        return 6

    if not ctrlr.initialize_recovery(cfg["recovery"], list(countries.keys()), rules):
        return 7

    sess = ctrlr.connect_to_sap(cfg["sap"])

    if sess is None:
        _logger.critical("Could not connect to the SAP GUI scripting engine.")
        return 8

    if not ctrlr.connect_to_database(cfg["database"]):
        _logger.info("=== Cleanup ===")
        ctrlr.disconnect_from_sap(sess)
        return 9

    ctrlr.delete_reports(cfg["reports"])
    _logger.info("=== Success ===\n")

    _logger.info("=== Processing ===")
    processed = 0

    for cntry in countries:

        if ctrlr.is_reconciled(cntry):
            _logger.warning(f"{cntry} skipped since already reconciled in the previous run.")
            continue

        _logger.info(f"Reconciling '{cntry}' ...")

        if not ctrlr.export_se16_kote_data(cfg["data"], rules, cntry, sess):
            _logger.error("Data export failed. Stopping country reconciliation ...")
            continue

        ctrlr.process_se16_kote_data(cfg["data"], rules, cntry)

        if not ctrlr.export_se16_kona_data(cfg["data"], rules, cntry, sess):
            _logger.error("Data export failed. Stopping country reconciliation ...")
            continue

        ctrlr.process_se16_kona_data(cfg["data"], rules, cntry)

        if not ctrlr.export_zsd25_global_data(cfg["data"], rules, cntry, sess):
            _logger.error("Data export failed. Stopping country reconciliation ...")
            continue

        ctrlr.process_zsd25_global_data(cfg["data"], rules, cntry)

        if not ctrlr.export_zsd25_local_data(cfg["data"], rules, cntry, sess):
            _logger.error("Data export failed. Stopping country reconciliation ...")
            continue

        ctrlr.process_zsd25_local_data(cfg["data"], rules, cntry)

        if not ctrlr.export_fs10n_data(cfg["data"], rules, cfg["reconciliation"], cntry, sess):
            _logger.error("Data export failed. Stopping country reconciliation ...")
            continue

        ctrlr.process_fs10n_data(cfg["data"], rules, cntry)

        if not ctrlr.export_fbl3n_data(cfg["data"], cfg["reconciliation"], rules, cntry, sess):
            _logger.error("Data export failed. Stopping country reconciliation ...")
            continue

        if not ctrlr.process_fbl3n_data(cfg["database"], cfg["data"], cfg["reconciliation"], rules, cntry):
            _logger.error("Data processing failed. Stopping country reconciliation ...")
            continue

        if not ctrlr.reconcile(cfg["database"], cfg["data"], cfg["reconciliation"], rules, cntry, usr_params):
            _logger.error(f"Reconciliation of '{cntry}' failed.\n")
            ctrlr.send_notification(cfg["messages"], cfg["reports"], rules, cntry)
            continue

        if not ctrlr.generate_report(cfg["reports"], rules, cntry):
            _logger.error("Generating of user report failed!")
            continue

        processed += 1
        ctrlr.send_notification(cfg["messages"], cfg["reports"], rules, cntry, usr_params)
        ctrlr.set_completed(cntry)
        _logger.info(f"Reconciliation of '{cntry}' completed.")
        _logger.info("---------------------------------------")

    _logger.info("=== Success ===\n")

    _logger.info("=== Cleanup ===")
    if processed == 0 and args["email_id"] is None:
        # File cleanup skipped in this scenario since temp data
        # will be needed for further error investigation
        ctrlr.disconnect_from_sap(sess)
        return 10

    ctrlr.remove_temp_files(cfg["data"]["temp_dir"])
    ctrlr.disconnect_from_sap(sess)
    ctrlr.disconnect_from_database()
    ctrlr.clear_recovery_states()
    ctrlr.clear_data_processor()
    _logger.info("=== Success ===\n")

    if processed == 0 and args["email_id"] is not None:
        # in this scenario, return after a complete
        # cleanup has been performed
        return 11

    return 0

if __name__ == "__main__":

    arguments = {
        "action": "reconcile",
        "debug": True,
        "database": "remote",
        "email_id": "<DB6PR02MB30780CFCCBCD36B13EA8FAE9F527A@DB6PR02MB3078.eurprd02.prod.outlook.com>"
    }

    exit_code = main(arguments)

    _logger.info(f"=== System shutdown with return code: {exit_code} ===")
    logging.shutdown()
    sys.exit(exit_code)
