# pylint: disable = C0103, W0602, W0603, W1203

"""
The 'biaRecovery.py' module provides interface for setup
and management of the application recovery functionality.
The main purpose of the functionality is saving runtime
states of processed entities.
"""

from enum import Enum
import json
import logging
from os.path import exists
from typing import Union

_logger = logging.getLogger("master")
_ent_states: dict = None
_rec_path: str = None

class states(Enum):
    """
    Enum of possible application
    states to save during runtime.
    """

    RECONCILED = "reconciled"
    DB_UPDATED = "db_updated"
    INFO = "info"
    FBL3N_DATA_EXPORTED = "fbl3n_data_exported"
    FBL3N_DATA_PROCESSED = "fbl3n_data_processed"
    SE16_NO_KONA_DATA = "se16_no_kona_data"
    SE16_KONA_DATA_EXPORTED = "se16_kona_data_exported"
    SE16_KOTE_DATA_EXPORTED ="se16_kote_data_exported"
    SE16_KONA_DATA_PROCESSED = "se16_kona_data_processed"
    SE16_KOTE_DATA_PROCESSED = "se16_kote_data_processed"
    ZSD25_HQ_DATA_EXPORTED = "zsd25_hq_data_exported"
    ZSD25_HQ_DATA_PROCESSED = "zsd25_hq_data_processed"
    ZSD25_HQ_DATA_CALCULATED = "zsd25_hq_data_calculated"
    ZSD25_LE_DATA_EXPORTED = "zsd25_le_data_exported"
    ZSD25_LE_DATA_PROCESSED = "zsd25_le_data_processed"
    ZSD25_LE_DATA_CALCULATED = "zsd25_le_data_calculated"
    ZSD25_NO_HQ_DATA = "zsd25_no_hq_data_available"
    YEARLY_SUMMARY_RETRIEVED = "yearly_summary_retrieved"
    TEXT_SUMMARY_RETRIEVED = "text_summary_retrieved"
    FS10N_DATA_EXPORTED = "fs10n_data_exported"
    FS10N_DATA_PROCESSED = "fs10n_data_processed"
    USER_WARNING = "user_warning"
    USER_ERROR = "user_error"

def initialize(rec_path: str, countries: list, rules: dict):
    """
    Initializes the application recovery functionality. A check for any previous \n
    application failure is performed. If a failure is deteted, then the recovery \n
    file containing saved processing checkpoints will be loaded. If no failure \n
    is found, then a new recovery file with default entity states is created.

    Params:
    -------
    rec_path:
        Path to the file containing checkpoints for application recovery
        shoud the app crash or be terminated due to a critical error.

    countries:
        List of countries to which the recovery mechanism will be applied.

    rules:
        Data processing rules for reconciled countries.

    Returns:
    --------
    None.
    """

    # do not check the existence of the file here
    #  as the file may actually not be created yet
    if len(countries) == 0:
        raise ValueError("No country name provided!")

    global _ent_states
    global _rec_path

    _rec_path = rec_path

    if not exists(_rec_path):
        clear_states()

    with open(_rec_path, 'r', encoding = "utf-8") as stream:
        ent_states = json.loads(stream.read())

    # use previous states to recover app
    if len(ent_states) != 0:
        _ent_states = ent_states
        return

    # no failure - init app with default states
    _ent_states = reset_states(countries, rules)

def clear():
    """
    Clears all recovery states
    and deallocates its  memory
    resources.

    Params:
    -------
    None.

    Returns:
    --------
    None.
    """

    global _rec_path
    global _ent_states

    _logger.info("Clearing application recovery ...")

    clear_states()

    _rec_path = None
    _ent_states = None

def clear_states():
    """
    Clears recovery data
    for reconciled countries.

    Params:
    -------
    None.

    Returns:
    --------
    None.
    """

    global _ent_states

    _ent_states = {}

    with open(_rec_path, 'w', encoding = "utf-8") as stream:
        json.dump(_ent_states, stream)

def reset_states(countries: list, rules: dict) -> dict:
    """
    Sets default values to recovery data
    for countries being reconciled.

    Params:
    -------
    countries:
        List of countries being reconciled.

    rules:
        Data processing rules for reconciled countries.

    Returns:
    --------
    Default processing checkpoints
    for each reconciled country.
    """

    if len(countries) == 0:
        raise ValueError("No country name provided!")

    new_ent_states = {}

    for cntry in countries:

        new_ent_states[cntry] = {}
        new_ent_states[cntry]["reconciled"] = False
        new_ent_states[cntry]["db_updated"] = False
        new_ent_states[cntry]["info"] = False
        new_ent_states[cntry]["fbl3n_data_exported"] = False
        new_ent_states[cntry]["fbl3n_data_processed"] = False
        new_ent_states[cntry]["se16_no_kona_data"] = False
        new_ent_states[cntry]["se16_kona_data_exported"] = False
        new_ent_states[cntry]["se16_kote_data_exported"] = False
        new_ent_states[cntry]["se16_kona_data_processed"] = False
        new_ent_states[cntry]["se16_kote_data_processed"] = False
        new_ent_states[cntry]["zsd25_glob_data_exported"] = False
        new_ent_states[cntry]["zsd25_glob_data_processed"] = False
        new_ent_states[cntry]["zsd25_glob_data_calculated"] = False
        new_ent_states[cntry]["zsd25_loc_data_exported"] = False
        new_ent_states[cntry]["zsd25_loc_data_processed"] = False
        new_ent_states[cntry]["zsd25_loc_data_calculated"] = False
        new_ent_states[cntry]["zsd25_no_glob_data"] = False
        new_ent_states[cntry]["yearly_summary_retrieved"] = False
        new_ent_states[cntry]["text_summary_retrieved"] = {}
        new_ent_states[cntry]["fs10n_data_exported"] = {}
        new_ent_states[cntry]["fs10n_data_processed"] = {}
        new_ent_states[cntry]["user_warning"] = ""
        new_ent_states[cntry]["user_error"] = ""

        for acc in rules[cntry]["accounts"]:
            new_ent_states[cntry]["text_summary_retrieved"][str(acc)] = False
            new_ent_states[cntry]["fs10n_data_exported"][str(acc)] = False
            new_ent_states[cntry]["fs10n_data_processed"][str(acc)] = False

    with open(_rec_path, 'w', encoding = "utf-8") as stream:
        json.dump(new_ent_states, stream, indent = 4)

    return new_ent_states

def save_state(country: str, key: str, val: Union[bool,str], acc: Union[int, str] = None):
    """
    Stores new value to recovery data defined by country and parameter name. \n
    If account is provided, then the new value will be set to data defined by \n
    country, parameter name and the account number.

    Params:
    -------
    cntry:
        Name of the country for which a processing state will be saved.

    key:
        Parameter name for which the new state will be saved.

    val:
        A state to save. \n
        A valid value is a string or a bool.

    acc:
        Account number, for which a processing state will be saved.

    Returns:
    --------
    None.
    """

    global _ent_states

    if acc is None:
        _logger.debug(
            "Saving processing state: "
            f"country = '{country}'; key = '{key}'; val = {val}"
        )
        _ent_states[country][key] = val
    else:
        _logger.debug(
            "Saving processing state: "
            f"country = '{country}'; key = '{key}'; acc = '{acc}'; val = {val}"
        )
        _ent_states[country][key][str(acc)] = val

    with open(_rec_path, 'w', encoding = "utf-8") as stream:
        json.dump(_ent_states, stream, indent = 4)

def get_state(country: str, key: str, acc: Union[int, str] = None) -> Union[bool,str]:
    """
    Returns processing state for a given country and parameter.

    If account is provided, then the new value will be set to data \n
    defined by country, parameter name and the account number.

    Params:
    -------
    country:
        Country for which the recovery value willl be searched.

    key:
        Name of the parameter for which the value will be searched.

    acc:
        Account number, for which the the recovery value will be searched.

    Returns:
    --------
    A string or bool representing a processing state.
    """

    if acc is None:
        state = _ent_states[country][key]
    else:
        state = _ent_states[country][key][str(acc)]

    return state
