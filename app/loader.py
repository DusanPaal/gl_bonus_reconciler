# pylint: disable = W1203, C0103

"""The module for loading accounting data by country into the database."""

import logging
import sys
from glob import glob
from os.path import join

import engine.biaServices as svc
from engine.biaServices import DatabaseType

log = logging.getLogger("master")

def main(args: dict) -> int:
    """Serves as the starting point and overall control
    for the application.

    If application launch was invoked by user email, all
    the necessary reconciliation params are fetched from
    the user message referenced by the email ID. If no
    parameters are provided, then reconciliation for all
    active countries will be performed as per settings in
    'rules.yaml'.

    Returns:
    --------
    Program completion state.
    """

    db_access = DatabaseType.REMOTE

    logger_ok = svc.initialize_logger(
        cfg_path = join(sys.path[0], "logging.yaml"),
        log_path = join(sys.path[0], "log.log"),
        debug = bool(args["debug"]),
        header = {
            "Application name": "GL Bonus Reconciler",
            "Application version": "1.1.20220817",
            "Log date": svc.get_current_time("%d-%b-%Y")
        }
    )

    if not logger_ok:
        return 1

    log.info("=== Initialization ===")
    log.debug(f"Arguments passed to main(): {args}")

    cfg = svc.load_app_config(join(sys.path[0], "appconfig.yaml"))

    if cfg is None:
        log.critical("Loading of configuration data failed.")
        return 2

    if not svc.connect_to_database(cfg["database"], db_access):
        return 9

    log.info("=== Processing ===")

    src_folder = join(sys.path[0], "data", args["company_code"])
    file_paths = []

    file_paths += glob(join(src_folder, "*.dat"))
    file_paths += glob(join(src_folder, "*.txt"))
    n_files = len(file_paths)

    for nth, file_path in enumerate(file_paths, start = 1):

        log.info(f"Storing data from file ({nth} of {n_files}): '{file_path}' ...")
        svc.load_data_to_database(file_path, cfg["database"][db_access.value], args["company_code"])

        log.info("Data successfully stored.")
        log.info("---------------------------------------")

    log.info("=== Processing OK ===\n")

    log.info("=== Cleanup ===\n")
    svc.disconnect_from_database()
    log.info("=== Cleanup OK ===\n")

    return 0

if __name__ == "__main__":

    exit_code = main({
        "database": "remote",
        "debug": True,
        "company_code": "0061"
    })

    log.info(f"=== System shutdown with return code: {exit_code} ===")
    logging.shutdown()
    sys.exit(exit_code)
