# pylint: disable = C0103, R0913, W0603

"""
The 'biaFBL3N.py' module automates the standard SAP GUI FBL3N transaction
in order to load and export data located on customer accounts to a plain
text file.
"""

from datetime import date
from os.path import exists, isfile, split
from pyperclip import copy as copy_to_clipboard
from win32com.client import CDispatch

# custom warnings
class NoDataFoundWarning(Warning):
    """
    Raised when there are no open
    items available on account.
    """

# custom exceptions
class AbapRuntimeError(Exception):
    """
    Raised when SAP 'ABAP Runtime Error'
    occurs during communication with
    the transaction.
    """

class ConnectionLostError(Exception):
    """
    Raised when a connection to SAP
    is lost as a result of a network error.
    """

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

class ItemsLoadingError(Exception):
    """
    Raised when loading of open
    items fails.
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
    "F9":          9,
    "CtrlS":       11,
    "F12":         12,
    "ShiftF4":     16,
    "ShiftF12":    24,
    "CtrlF1":      25,
    "CtrlF8":      32,
    "CtrlShiftF6": 42
}

def _is_sap_runtime_error(main_wnd: CDispatch) -> bool:
    """
    Checks if a SAP ABAP runtime error exists.
    """

    if main_wnd.text == "ABAP Runtime Error":
        return True

    return False

def _is_error_message(sbar: CDispatch) -> bool:
    """
    Checks if a status bar message
    is an error message.
    """

    if sbar.messageType == "E":
        return True

    return False

def _is_popup_dialog() -> bool:
    """
    Checks if the active window
    is a popup dialog window.
    """

    if _sess.ActiveWindow.type == "GuiModalWindow":
        return True

    return False

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
    Enters company code into the 'Company code'
    field located on the main transaction window.
    """

    if not (len(val) == 4 and val.isnumeric()):
        raise ValueError(f"Invalid company code used: {val}. "
        "A valid value is a 4-digit number (e.g. '0075').")

    if _main_wnd.findAllByName("SD_BUKRS-LOW", "GuiCTextField").count > 0:
        _main_wnd.findByName("SD_BUKRS-LOW", "GuiCTextField").text = val
    elif _main_wnd.findAllByName("SO_WLBUK-LOW", "GuiCTextField").count > 0:
        _main_wnd.findByName("SO_WLBUK-LOW", "GuiCTextField").text = val

def _set_layout(val: str):
    """
    Enters layout name into the 'Layout' field
    located on the main transaction window.
    """
    _main_wnd.findByName("PA_VARI", "GuiCTextField").text = val

def _set_accounts(vals: list):

    if len(vals) == 0:
        raise ValueError("No GL account provided!")

    for val in vals:
        if not (len(str(val)) == 8 and str(val).isnumeric()):
            raise ValueError(f"Invalid GL account used: {val} "
            "A valid value is an 8-digit number (e.g. '66791580')")

    # remap vals to str since accounts may be passed in as ints
    accs = list(map(str, vals))

    # open selection table for company codes
    _main_wnd.findByName("%_SD_SAKNR_%_APP_%-VALU_PUSH", "GuiButton").press()

    _main_wnd.SendVKey(_vkeys["ShiftF4"])   # clear any previous values
    copy_to_clipboard("\r\n".join(accs))    # copy accounts to clipboard
    _main_wnd.SendVKey(_vkeys["ShiftF12"])  # confirm selection
    copy_to_clipboard("")                   # clear the clipboard
    _main_wnd.SendVKey(_vkeys["F8"])        # confirm

def _choose_line_item_selection(option: str):
    """
    Selects the kind of items to load.
    """

    if option == "all_items":
        _main_wnd.findByName("X_AISEL", "GuiRadioButton").select()
    elif option == "open_items":
        _main_wnd.findByName("X_OPSEL", "GuiRadioButton").select()
    elif option == "cleared_items":
        _main_wnd.findByName("X_CLSEL", "GuiRadioButton").select()
    else:
        assert False, "Unrecognized selection option!"

def _set_posting_dates(first: date, last: date):
    """
    Enters first and last posting date into the
    fields of the 'All items' option located on
    the main transaction window.
    """

    date_from = first.strftime("%d.%m.%Y")
    date_to = last.strftime("%d.%m.%Y")

    _main_wnd.FindByName("SO_BUDAT-LOW", "GuiCTextField").text = date_from
    _main_wnd.FindByName("SO_BUDAT-HIGH", "GuiCTextField").text = date_to

def _toggle_worklist(activate: bool):
    """
    Activates or deactivates the 'Use worklist'
    option in the transaction main search mask.
    """

    used = _main_wnd.FindAllByName("PA_WLSAK", "GuiCTextField").Count > 0

    if (activate or used) and not (activate and used):
        _main_wnd.SendVKey(_vkeys["CtrlF1"])

def _select_data_format(idx: int) -> None:
    """
    Selects data export format from the export options
    dialog based on the option index on the list.
    """

    option_wnd = _sess.FindById("wnd[1]")
    option_wnd.FindAllByName("SPOPLI-SELFLAG", "GuiRadioButton")(idx).Select()

def _load_items() -> None:
    """Simulates pressing the 'Execute'
    button that triggers data loading.
    """

    try:
        _main_wnd.SendVKey(_vkeys["F8"])
    except Exception as exc:
        raise SapRuntimeError(f"Attempt to load items failed: {str(exc)}") from exc

    # In some situations a SAP crash can be caught
    # only when trying to execute a next statement
    # after pressing the 'Execute' button. Check:

    try:
        msg = _stat_bar.Text
    except Exception as exc:
        raise ConnectionLostError("Connection to SAP lost due to an network error.") from exc

    if _is_sap_runtime_error(_main_wnd):
        raise SapRuntimeError("SAP runtime error!")

    if "items displayed" not in msg:
        raise NoDataFoundWarning(msg)

    if "No items selected" in msg:
        raise NoDataFoundWarning("No items found for the given selection criteria.")

    if "The current transaction was reset" in msg:
        raise SapRuntimeError("FBL3N was unexpectedly terminated!")

    if _is_error_message(_stat_bar):
        raise ItemsLoadingError(msg)

    if _main_wnd.text == 'ABAP Runtime Error':
        raise AbapRuntimeError("Data loading failed due to an ABAP runtime error.")

def _export_to_file(file_path: str, enc: str = "4120"):
    """
    Exports loaded accounting data into a text file.
    """

    folder_path, file_name = split(file_path)

    if not exists(folder_path):
        raise FolderNotFoundError(f"Export folder not found: {folder_path}")

    if not file_path.endswith(".txt"):
        raise ValueError(f"Invalid file type: {file_path}. "
        "Only '.txt' file types are supported.")

    _main_wnd.SendVKey(_vkeys["F9"])     # open local data file export dialog
    _select_data_format(0)               # set plain text data export format
    _main_wnd.SendVKey(_vkeys["Enter"])  # confirm

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
    Starts FBL3N transaction.

    Params:
    ------
    sess:
        A SAP GuiSession object.
    """

    global _sess
    global _main_wnd
    global _stat_bar

    _sess = sess
    _main_wnd = _sess.findById("wnd[0]")
    _stat_bar = _main_wnd.findById("sbar")

    _sess.StartTransaction("FBL3N")

def close() -> None:
    """
    Closes a running FBL3N transaction.

    Raises:
    -------
    TransactionNotStartedError:
        When attempting to close \n
        FBL3N when it's not running.
    """

    global _sess
    global _main_wnd
    global _stat_bar

    if _sess is None:
        raise TransactionNotStartedError("Cannot close FBL3N when it's "
        "actually not running! Use the biaFBL3N.start() procedure to run "
        "the transaction first of all.")

    _sess.EndTransaction()

    if _is_popup_dialog():
        _close_popup_dialog(confirm = True)

    _sess = None
    _main_wnd = None
    _stat_bar = None

def export(file_path: str, cocd: str, gl_accs: list,
           from_day: date, to_day: date, layout: str = None):
    """
    Loads and exports data from GL accounts into a file.

    Loads accounting data based on GL accounts and posting date range (from-to). \n
    If any records are found, the loaded data is exported into a plain text file defined \n
    by a valid path. The format of the exported data is defined by the name of the used \n
    layout.

    Params:
    -------
    file_path:
        Path to the text file to which
        the loaded data will be exported.

    cocd:
        Company code for which the data will be exported. \n
        A valid code is a 4-digit string (e.g. '1001').

    gl_accs:
        GL account numbers for which the data export will be performed. \n
        A valid code is a 8-digit string or integer (e.g. '66010030' or 66010030).

    from_day:
        Posting date from which (incl.) accounting data will be loaded.

    to_day:
        Posting date to which (incl.) accounting data will be loaded.

    layout:
        Name of the layout that defines the format \n
        of the loaded/exported data. If None is used (default), \n
        then the default transacton value will be used.

    Returns:
    --------
    None.

    Raises:
    -------
    NoDataFoundWarning:
        If there are no open items available on account(s).

    AbapRuntimeError:
        If a SAP 'ABAP Runtime Error' occurs during transaction runtime.

    ConnectionLostError:
        If  a connection to SAP is lost
        as a result of a network error.

    DataWritingError:
        If writing of accounting data to a file fails.

    FolderNotFoundError:
        When the folder to which data should be exported doesn't exist.

    ItemsLoadingError:
        If loading of open items fails.

    SapRuntimeError:
        If an unhanded general SAP error occurs \n
        during communication with the transaction.

    TransactionNotStartedError:
        When attempting to use the procedure before starting FBL3N.
    """

    if _sess is None:
        raise TransactionNotStartedError("Cannot export accounting data from FBL3N "
        "when it's actually not running! Use the biaFBL3N.start() procedure to run "
        "the transaction first of all.")

    if layout is not None:
        _set_layout(layout)

    _toggle_worklist(activate = False)
    _set_company_code(cocd)
    _set_accounts(gl_accs)
    _choose_line_item_selection(option = "all_items")
    _set_posting_dates(from_day, to_day)
    _load_items()
    _export_to_file(file_path)
