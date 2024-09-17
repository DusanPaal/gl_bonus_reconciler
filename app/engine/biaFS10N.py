# pylint: disable = C0103, W0603

"""
The 'biaFS10N.py' module automates data searching, loading and export
from the standard SAP FS10N transaction table to a plain text file.
"""

import logging
from os.path import exists, isfile, split
from typing import Union
from win32com.client import CDispatch

# custom warnings
class NoDataFoundWarning(Warning):
    """
    Raised when there are no open
    items available on account.
    """

# custom exceptions
class DataWritingError(Exception):
    """
    Raised when writing of accounting
    data to file fails.
    """

class FolderNotFoundError(Exception):
    """
    Raised when the folder to which
    data should be exported doesn't exist.
    """

class TransactionNotStartedError(Exception):
    """
    Raised when attempting to use a procedure
    before starting the transaction.
    """

_sess = None
_main_wnd = None
_stat_bar = None

_logger = logging.getLogger("master")

# keyboard to SAP virtual keys mapping
_vkeys = {
    "Enter":        0,
    "F3":           3,
    "F8":           8,
    "CtrlS":        11,
    "F12":          12,
    "CtrlShiftF9":  45
}

def _is_popup_dialog() -> bool:
    """
    Checks if the active window
    is a popup dialog window.
    """

    if _sess.ActiveWindow.type == "GuiModalWindow":
        return True

    return False

def _get_popup_text() -> str:
    """
    Returns text message
    contained in a SAP pop-up
    window.
    """

    txt = _sess.ActiveWindow.children(1).children(1).text

    return txt

def _close_popup_dialog(confirm: bool):
    """
    Confirms or delines a pop-up dialog.
    """

    if _sess.ActiveWindow.text == "Information":
        if confirm:
            _main_wnd.SendVKey(_vkeys["Enter"]) # confirm
        else:
            _main_wnd.SendVKey(_vkeys["F12"])   # decline
        return

    btn_caption = "Yes" if confirm else "No"

    for child in _sess.ActiveWindow.Children:
        for grandchild in child.Children:
            if grandchild.Type != "GuiButton":
                continue
            if btn_caption != grandchild.text.strip():
                continue
            grandchild.Press()
            return

def _set_company_code(val: str):
    """
    Sets company code value in
    the initial search window.
    """

    if not (len(val) == 4 and val.isnumeric()):
        raise ValueError(f"Invalid company code used: {val}. "
        "A valid value is a 4-digit number (e.g. '0075').")

    _main_wnd.FindByName("SO_BUKRS-LOW", "GuiCTextField").text = val

def _set_gl_account(val: str):
    """
    Sets GL account value in
    the initial search window.
    """

    if not (len(val) == 8 and val.isnumeric()):
        raise ValueError(f"Invalid GL account used: {val}. "
        "A valid value is an 8-digit number (e.g. '66791580')")

    _main_wnd.FindByName("SO_SAKNR-LOW", "GuiCTextField").text = val

def _set_fiscal_year(val: int):
    """
    Sets fiscal year value in
    the initial search window.
    """

    if not 2020 <= val <= 2030:
        raise ValueError(f"Invalid fiscal year used: {val}!"
        "A valid value must be within the range of years (inlcuding): 2020 - 2030.")

    _main_wnd.FindByName("GP_GJAHR", "GuiTextField").text = str(val)

def _select_export_format(idx: int):
    """
    Selects data export format
    based on its index in the
    option menu.
    """

    # function accepts indexed option rather than name in order to avoid any
    # export option order deviations in different SAP versions.

    grid_view = _main_wnd.FindById("usr/cntlFDBL_BALANCE_CONTAINER/shellcont/shell")
    grid_view.PressToolbarContextButton("&MB_EXPORT")
    grid_view.SelectContextMenuItem("&PC")
    optins_wnd = _sess.FindById("wnd[1]")
    optins_wnd.FindAllByName("SPOPLI-SELFLAG", "GuiRadioButton")(idx).Select()

def _load_data():
    """
    Simulates pressing
    the 'Execute' button.
    """

    _main_wnd.SendVKey(_vkeys["F8"])

    if _is_popup_dialog():
        msg = _get_popup_text()
        _close_popup_dialog(confirm = True)
        raise NoDataFoundWarning(msg)

def _export_to_file(file_path: str, enc: str = "4120"):
    """
    Exports loaded accounting data to a text file.
    """

    folder_path, file_name = split(file_path)

    if not exists(folder_path):
        raise FolderNotFoundError(f"Export folder not found: {folder_path}")

    if not file_path.endswith(".txt"):
        raise ValueError(f"Invalid file type: {file_path}. "
        "Only '.txt' file types are supported.")

    _select_export_format(0)                    # select unconverted text data export format
    _main_wnd.SendVKey(_vkeys["Enter"])         # confirm

    _sess.FindById("wnd[1]").FindByName("DY_PATH", "GuiCTextField").text = folder_path
    _sess.FindById("wnd[1]").FindByName("DY_FILENAME", "GuiCTextField").text = file_name
    _sess.FindById("wnd[1]").FindByName("DY_FILE_ENCODING", "GuiCTextField").text = enc

    _main_wnd.SendVKey(_vkeys["CtrlS"])         # replace an exiting file
    _main_wnd.SendVKey(_vkeys["F3"])     # Load main mask

    # double check if data export succeeded
    if not isfile(file_path):
        raise DataWritingError(f"Failed to export data to file: {file_path}")

def start(sess: CDispatch):
    """
    Starts FS10N transaction.

    Params:
    ------
    sess:
        A GuiSession object.

    Returns:
    -------
    None.
    """

    global _sess
    global _main_wnd
    global _stat_bar

    if _sess is not None:
        _logger.info("Restarting FS10N ...")
    else:
        _logger.info("Starting FS10N ...")
        _sess = sess
        _main_wnd = _sess.findById("wnd[0]")
        _stat_bar = _main_wnd.findById("sbar")

    _sess.StartTransaction("FS10N")

def close():
    """
    Closes a running FS10N transaction.

    Params:
    -------
    None.

    Returns:
    --------
    None.

    Raises:
    -------
    TransactionNotStartedError:
        When attempting to close \n
        FS10N when it's not running.
    """

    global _sess
    global _main_wnd
    global _stat_bar

    if _sess is None:
        raise TransactionNotStartedError("Cannot close FS10N when it's "
        "actually not running! Use the biaFS10N.start() procedure to run "
        "the transaction first of all.")

    _logger.info("Closing FS10N ...")

    _sess.EndTransaction()

    if _is_popup_dialog():
        _close_popup_dialog(confirm = True)

    _sess = None
    _main_wnd = None
    _stat_bar = None

def export(file_path: str, account: Union[int, str], company_code: str, fisc_year: Union[int, str]):
    """
    Loads FS10N data based on defined search params
    and exports the data into a plain text file.

    Params:
    -------
    file_path:
        Path to the text file to which
        the loaded data will be exported.

    account:
        A GL account number. \n
        A valid account is an 8-digit string \n
        or integer (e.g. '66010030' or 66010030).

    company_code:
        Company code for which the data will be exported. \n
        A valid code is a 4-digit string (e.g. '1001').

    fisc_year:
        The fiscal year for which the records will be loaded. \n
        A valid year is a 4-digit string or integer (e.g. '2021', 2020) \n
        in a range 2020 - 2030 (including).

    Returns:
    --------
    None.

    Raises:
    -------
    NoDataFoundWarning:
        If there are no open items available on account(s).

    DataWritingError:
        If writing of accounting data to a file fails.

    FolderNotFoundError:
        When the folder to which data should be exported doesn't exist.

    TransactionNotStartedError:
        When attempting to use the procedure before starting FBL3N.
    """

    if _sess is None:
        raise TransactionNotStartedError("Cannot export accounting data from FS10N "
        "when it's actually not running! Use the biaFS10N.start() procedure to run "
        "the transaction first of all.")

    _set_gl_account(account)
    _set_company_code(company_code)
    _set_fiscal_year(int(fisc_year))
    _load_data()
    _export_to_file(file_path)
