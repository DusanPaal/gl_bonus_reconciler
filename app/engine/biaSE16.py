# pylint: disable = C0123, C0103, C0301, C0302, E0401, E0611, R1711, W0603, W0703, W1203

"""
The 'biaSE16.py' module automates data searching, loading and export
from the standard SAP SE16 transaction table to a plain text file.
"""

from enum import Enum
from os.path import exists, isfile, split
from typing import Union
from pyperclip import copy as copy_to_clipboard
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

class SapRuntimeError(Exception):
    """
    Raised when an unhanded general SAP
    error occurs during communication with
    the transaction.
    """

class TransactionNotStartedError(Exception):
    """
    Raised when attempting to use a procedure
    before starting the transaction.
    """

_sess = None
_main_wnd = None
_stat_bar = None

# keyboard to SAP virtual keys mapping
_vkeys = {
    "Enter":       0,
    "F3":          3,
    "F8":          8,
    "CtrlS":       11,
    "F12":         12,
    "ShiftF4":     16,
    "ShiftF12":    24,
    "CtrlF9":      33,
    "CtrlShiftF9": 45
}

class Tables(Enum):
    """
    List of SE16 tables containing
    source data for reconciliation.
    """
    KOTE = "KOTE890"
    KONA = "KONA"

def _is_popup_dialog() -> bool:
    """
    Checks if the active window
    is a popup dialog window.
    """

    if _sess.ActiveWindow.type == "GuiModalWindow":
        return True

    return False

def _confirm():
    """Simulates pressign the Enter buton."""
    _main_wnd.SendVKey(_vkeys["Enter"])

def _decline():
    """Simulates pressign the F12 buton."""
    _main_wnd.SendVKey(_vkeys["F12"])

def _close_popup_dialog(confirm: bool):
    """
    Confirms or delines a pop-up dialog.
    """

    if _sess.ActiveWindow.text == "Information":
        if confirm:
            _confirm()
        else:
            _decline()
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

def _set_table_name(val: str):
    """
    Sets table value in the initial search window.
    """

    _main_wnd.findByName("DATABROWSE-TABLENAME", "GuiCTextField").text = val

def _set_sales_offs(vals: Union[list, tuple]):
    """
    Sets sales office values in the initial search window.
    """

    if len(vals) == 0:
        raise ValueError("No sales office code provided!")

    for val in vals:
        if not (len(str(val)) == 4 and str(val).isnumeric()):
            raise ValueError(f"Invalid sales office code: {val}."
            "A valid value is a 4-digit number (e.g. '0075').")

    # open selection table for company codes
    _main_wnd.findByName("%_I4_%_APP_%-VALU_PUSH", "GuiButton").press()

    _main_wnd.SendVKey(_vkeys["ShiftF4"])   # clear any previous values
    copy_to_clipboard("\r\n".join(vals))    # copy accounts to clipboard
    _main_wnd.SendVKey(_vkeys["ShiftF12"])  # confirm selection
    copy_to_clipboard("")                   # clear the clipboard
    _main_wnd.SendVKey(_vkeys["F8"])        # confirm"Invalid parameter combination used!"

def _set_agreements(vals: Union[list, tuple]):
    """
    Sets sales offices as data search criteria
    in the initial search window.
    """

    if len(vals) == 0:
        raise ValueError("No agrement number provided!")

    for val in vals:
        if not (len(str(val).lstrip('0')) == 8 and str(val).isnumeric()):
            raise ValueError(f"Invalid agreement number: {val}."
            "A valid value is an 8-digit number (e.g. '72000223').")

    vals = list(map(str, vals))

    # open selection table for company codes
    _main_wnd.findByName("%_I1_%_APP_%-VALU_PUSH", "GuiButton").press()

    _main_wnd.SendVKey(_vkeys["ShiftF4"])   # clear any previous values
    copy_to_clipboard("\r\n".join(vals))    # copy accounts to clipboard
    _main_wnd.SendVKey(_vkeys["ShiftF12"])  # confirm selection
    copy_to_clipboard("")                   # clear the clipboard
    _main_wnd.SendVKey(_vkeys["F8"])        # confirm

def _set_sales_org(val: str, tbl_name: str):
    """
    Sets sales organization code as data
    search criteria in the initial search window.
    """

    if not (len(val) == 4 and val.isnumeric()):
        raise ValueError(f"Invalid sales organization code: {val}. "
        "A valid value is a 4-digit number (e.g. '0075').")

    if tbl_name == "KOTE890":
        fld_id = "I3-LOW"
    elif tbl_name == "KONA":
        fld_id = "I2-LOW"

    _main_wnd.FindByName(fld_id, "GuiCTextField").text = val

def _clear_hit_limit() -> None:
    """Clears the default maximum hit limit."""

    _main_wnd.FindByName("MAX_SEL", "GuiTextField").text = ""

def _select_data_format(idx: int) -> None:
    """
    Selects data export format from the export options
    dialog based on the option index on the list.
    """

    option_wnd = _sess.FindById("wnd[1]")
    option_wnd.FindAllByName("SPOPLI-SELFLAG", "GuiRadioButton")(idx).Select()

def _load_data() -> None:
    """Simulates pressing the 'Execute' button."""

    _main_wnd.SendVKey(_vkeys["F8"]) # load data

    # handle situation when no entries were found
    if _stat_bar.text.strip() == "No table entries found for specified key":
        raise NoDataFoundWarning(_stat_bar.text)

    if _stat_bar.text.strip() != "":
        raise SapRuntimeError(_stat_bar.text)

def _export_to_file(file_path: str, enc: str = "4120") -> None:
    """Exports loaded accounting data to a text file."""

    folder_path, file_name = split(file_path)

    if not exists(folder_path):
        raise FolderNotFoundError(f"Export folder not found: {folder_path}")

    if not file_path.endswith(".txt"):
        raise ValueError(f"Invalid file type: {file_path}. "
        "Only '.txt' file types are supported.")

    # open local data file export dialog,
    # and set plain text data export format
    _main_wnd.SendVKey(_vkeys["CtrlShiftF9"])
    _select_data_format(0)
    _confirm()

    _sess.FindById("wnd[1]").FindByName("DY_PATH", "GuiCTextField").text = folder_path
    _sess.FindById("wnd[1]").FindByName("DY_FILENAME", "GuiCTextField").text = file_name
    _sess.FindById("wnd[1]").FindByName("DY_FILE_ENCODING", "GuiCTextField").text = enc

    _main_wnd.SendVKey(_vkeys["CtrlS"])  # replace an exiting file
    _main_wnd.SendVKey(_vkeys["F3"])     # Load main mask

    # double check if data export succeeded
    if not isfile(file_path):
        raise DataWritingError(f"Failed to export data to file: {file_path}")

def start(sess: CDispatch) -> None:
    """
    Starts SE16 transaction.

    Params:
    ------
    sess: A GuiSession object.

    Returns:
    -------
    None.
    """

    global _sess
    global _main_wnd
    global _stat_bar

    _sess = sess
    _main_wnd = _sess.findById("wnd[0]")
    _stat_bar = _main_wnd.findById("sbar")

    _sess.StartTransaction("SE16")

def close() -> None:
    """
    Closes a running SE16 transaction.
    Attempt to close SE16 that is not
    running is ignored.
    """

    global _sess
    global _main_wnd
    global _stat_bar

    if _sess is None:
        return

    _sess.EndTransaction()

    if _is_popup_dialog():
        _close_popup_dialog(confirm = True)

    _sess = None
    _main_wnd = None
    _stat_bar = None

def export(
        file_path: str, table: Tables, sales_org: str,
        sales_offs: Union[list, tuple] = None,
        agreements: Union[list, tuple] = None) -> None:
    """
    Loads and exports agreement data from KONA/KOTE table.

    If any records are found, the loaded data is exported into
    a plain text file defined by a valid path.

    Params:
    -------
    file_path:
        Path to the exported text file.

    table:
        Name of the data table.

    sales_org:
        Sales organization numerical code.

    sales_offs:
        List of sales office numerical codes (optional).

    agreements:
        Agreement numbers to search (optional).

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

    SapRuntimeError:
        If an unhanded general SAP error occurs
        during communication with the transaction.

    TransactionNotStartedError:
        When attempting to use the procedure before starting FBL3N.
    """

    if _sess is None:
        raise TransactionNotStartedError("Cannot export accounting data from SE16 "
        "when it's actually not running! Use the biaSE16.start() procedure to run "
        "the transaction first of all.")

    if table == Tables.KONA and not (agreements is not None and sales_offs is None):
        raise ValueError(
            "Parameter combination not permitted: "
            f"'table' = {table}; 'agreements' = {agreements}; sales_offs = '{sales_offs}'"
        )

    if table == Tables.KOTE and not (agreements is None and sales_offs is not None):
        raise ValueError(
            "Parameter combination not permitted: "
            f"'table' = {table}; 'agreements' = {agreements}; sales_offs = '{sales_offs}'"
        )

    _set_table_name(table.value)
    _confirm()
    _set_sales_org(sales_org, table.value)

    if sales_offs is not None:
        _set_sales_offs(sales_offs)

    if agreements is not None:
        _set_agreements(agreements)

    _clear_hit_limit()
    _load_data()
    _export_to_file(file_path)
