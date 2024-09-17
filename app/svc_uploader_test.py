# pylint: disable = C0103, W1203, W0718

from glob import iglob
from os.path import join
import logging
import sys
from engine import biaServices as svc

log = logging.getLogger("master")

def main() -> int:
    """
    Serves as the starting point and overall control for the application.

    when the application is invoked by a user email, any necessary
    matching parameters are retrieved from the user message referenced
    by the email ID. If no parameters are provided, matching is performed
    for all active countries according to the settings in 'rules.yaml'.

    Returns:
    --------
    An int representing the program completion status.
    """

    try:
        svc.initialize_logger(
            cfg_path = join(sys.path[0], "logging.yaml"),
            log_path = join(sys.path[0], "log.log"),
            debug = True,
            header = {
                "Application name": "GL Bonus Reconciler",
                "Application version": "1.1.20220817",
                "Log date": svc.get_current_time("%d-%b-%Y")
            }
        )
    except Exception as exc:
        print(f"CRITICAL: {str(exc)}")
        return 1

    cocd = "0065"
    dir_path = fr"C:\bia\ledvance_gl_bonus_reconciler\app\data\{cocd}"
    cfg = svc.load_app_config(join(sys.path[0], "appconfig.yaml"))
    db_cdg = cfg["database"][svc.DatabaseType.REMOTE.value]
    svc.connect_to_database(cfg["database"], svc.DatabaseType.REMOTE)

    try:
        for file_path in iglob(join(dir_path, "*.txt")):
            svc.load_data_to_database(file_path, db_cdg, cocd)
    except Exception as exc:
        log.exception(exc)
        return 2

    return 0

if __name__ == "__main__":
    exit_code = main()
    log.info(f"=== System shutdown with return code: {exit_code} ===")
    logging.shutdown()
    sys.exit(exit_code)
