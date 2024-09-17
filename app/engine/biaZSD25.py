# pylint: disable = C0103, R0913, W0603

"""
The 'biaZSD25.py' module automates data searching, loading, and export
from the standard SAP ZSD25_T125 transaction to a local text file.
"""

import logging
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

_logger = logging.getLogger("master")

# keyboard to SAP virtual keys mapping
_vkeys = {
    "Enter":       0,
    "F3":          3,
    "F8":          8,
    "CtrlS":       11,
    "F12":         12,
    "ShiftF4":     16,
    "ShiftF12":    24,
    "CtrlF5":      29,
    "CtrlShiftF6": 42,
    "CtrlShiftF9": 45
}

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

def _set_sales_org(val: str):
    """
    Enters sales organization value
    into the corresponding field on
    the transaction initial window.
    """

    if not (len(val) == 4 and val.isnumeric()):
        raise ValueError(f"Invalid sales organization: {val}. "
        "The value shoud be a 4-digit string (e.g. '0075').")

    _main_wnd.FindByName("S_VKORG-LOW", "GuiCTextField").text = val

def _set_agreements(vals: list):
    """
    Enters agreement numbers
    into the corresponding field on
    the transaction initial window.
    """

    for val in vals:
        if not (len(str(val).lstrip('0')) == 8 and str(val).isnumeric()):
            raise ValueError(f"Invalid agreement number: {val}."
            "A valid value is an 8-digit number (e.g. '72000223').")

    vals = list(map(str, vals))

    # open selection table
    _main_wnd.findByName("%_S_KNUMA_%_APP_%-VALU_PUSH", "GuiButton").press()

    _main_wnd.SendVKey(_vkeys["ShiftF4"])   # clear any previous values
    copy_to_clipboard("\r\n".join(vals))    # copy accounts to clipboard
    _main_wnd.SendVKey(_vkeys["ShiftF12"])  # confirm selection
    copy_to_clipboard("")                   # clear the clipboard
    _main_wnd.SendVKey(_vkeys["F8"])        # confirm

def _set_agreement_states(vals: list):
    """
    Enters agreement state chars
    into the corresponding field on
    the transaction initial window.
    """

    for val in vals:
        if not val in ('A', 'B', 'C', ""):
            raise ValueError(f'Invalid agreement state: {val}! '
            'Valid values are: "A", "B", "C", "".')

    # open selection table
    _main_wnd.findByName("%_S_BOSTA_%_APP_%-VALU_PUSH", "GuiButton").press()

    _main_wnd.SendVKey(_vkeys["ShiftF4"])    # clear any previous values
    copy_to_clipboard("\r\n".join(vals))     # copy accounts to clipboard
    _main_wnd.SendVKey(_vkeys["ShiftF12"])   # confirm selection
    copy_to_clipboard("")                   # clear the clipboard

    if "" in vals:
        idx = len(vals) - 1
        _sess.findById("wnd[1]").findAllByName("RSCSEL_255-SOP_I", "GuiButton")(idx).press()
        _main_wnd.SendVKey(_vkeys["Enter"])

    _main_wnd.SendVKey(_vkeys["F8"]) # confirm

def _set_display_conditions(select: bool):
    """
    Places a check mark to the 'Didsplay conditions'
    option on the transaction initial window if
    arguemnt 'slected' is True; otherwise the field
    remains unchecked.
    """

    _main_wnd.findByName("P_DETAIL", "GuiCheckBox").selected = select

    return

def _set_validity_end_date():
    """
    Sets 'Validity end date before'
    default field value one year ahead.
    """

    prev_date = _main_wnd.findByName("P_ABRDAT", "GuiCTextField").text
    prev_year = int(prev_date[-4:])
    new_date = "".join([prev_date[:6], str(prev_year + 1)])
    _main_wnd.findByName("P_ABRDAT", "GuiCTextField").text = new_date

def _set_layout(val: str):
    """
    Enters layout name intothe 'Layout' field
    located on the main transaction window.
    """

    _main_wnd.findByName("P_VARI", "GuiCTextField").text = val

def _set_variable_key_filter(vals: list):
    """
    Applies a filter on 'Variable' field to
    display only records containing the specific
    values stored in 'vals' argument.
    """

    if len(vals) == 0:
        raise ValueError("No sales office code provided!")

    for val in vals:
        if not (len(str(val)) == 4 and str(val).isnumeric()):
            raise ValueError(f"Invalid sales office code: {val}."
            "A valid value is a 4-digit string (e.g. '0075').")

    keys = [f"*Sales Office {val}*" for val in vals]

    # show filter selection dialog
    _main_wnd.SendVKey(_vkeys["CtrlF5"])

    # add key to filter list
    _main_wnd.SendVKey(_vkeys["CtrlShiftF6"])

    # select the key
    fld_container = _sess.findById("wnd[1]").findAllByName("shell", "GuiShell")[1]
    lst_row_idx = fld_container.RowCount - 1
    curr_row_idx = 0

    while curr_row_idx <= lst_row_idx:

        fld_tech_name = fld_container.GetCellValue(curr_row_idx, "FIELDNAME")

        if fld_tech_name == "VARKEY_T":
            fld_container.selectedRows = curr_row_idx
            # adds key to filter list
            _sess.findById("wnd[1]").findByName("APP_WL_SING", "GuiButton").press()
            break

        curr_row_idx += 1

    # press define values button
    _sess.findById("wnd[1]").findByName("600_BUTTON", "GuiButton").press()
    _sess.findById("wnd[2]").findByName("%_%%DYN001_%_APP_%-VALU_PUSH", "GuiButton").press()

    _main_wnd.SendVKey(_vkeys["ShiftF4"])   # clear any previous values
    copy_to_clipboard("\r\n".join(keys))    # copy accounts to clipboard
    _main_wnd.SendVKey(_vkeys["ShiftF12"])  # confirm selection
    copy_to_clipboard("")                   # clear the clipboard

    # add filter vals
    idx = len(keys)
    _sess.findById("wnd[3]").findAllByName("RSCSEL_255-SOP_I", "GuiButton").elementAt(idx).press()

    _main_wnd.SendVKey(_vkeys["Enter"]) # confirm filter option
    _main_wnd.SendVKey(_vkeys["F8"])    # confirm
    _main_wnd.SendVKey(_vkeys["Enter"]) # confirm filter values

def _select_data_format(idx: int):
    """
    Selects data export format from
    the export options dialog based
    on the option index on the list.
    """

    option_wnd = _sess.FindById("wnd[1]")
    option_wnd.FindAllByName("SPOPLI-SELFLAG", "GuiRadioButton")(idx).Select()

def _load_data():
    """
    Simulates pressing
    the 'Execute' button.
    """

    _main_wnd.SendVKey(_vkeys["F8"])

    # check errors
    if _is_error_message(_stat_bar):
        raise SapRuntimeError(f"Could not load transaction data. {_stat_bar.Text}")

    # data successfully loaded
    if not _is_popup_dialog():
        return

    # no entries found
    msg = _get_popup_text()
    _close_popup_dialog(confirm = True)
    _main_wnd.SendVKey(_vkeys["F3"])

    if msg.strip() == "Keine Daten gefunden!":
        raise NoDataFoundWarning("No data found for the used search criteria.")

    # unhandled error
    raise SapRuntimeError(msg)

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

    _main_wnd.SendVKey(_vkeys["CtrlShiftF9"])   # open local data file export dialog
    _select_data_format(0)                      # set plain text data export format
    _main_wnd.SendVKey(_vkeys["Enter"])         # confirm

    _sess.FindById("wnd[1]").FindByName("DY_PATH", "GuiCTextField").text = folder_path
    _sess.FindById("wnd[1]").FindByName("DY_FILENAME", "GuiCTextField").text = file_name
    _sess.FindById("wnd[1]").FindByName("DY_FILE_ENCODING", "GuiCTextField").text = enc

    _main_wnd.SendVKey(_vkeys["CtrlS"])  # replace an exiting file
    _main_wnd.SendVKey(_vkeys["F3"])     # Load main mask

    # double check if data export succeeded
    if not isfile(file_path):
        raise DataWritingError(f"Failed to export data to file: {file_path}")

def start(sess: CDispatch):
    """
    Starts ZSD25_T125 transaction.

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

    _sess = sess
    _main_wnd = _sess.findById("wnd[0]")
    _stat_bar = _main_wnd.findById("sbar")

    _sess.StartTransaction("ZSD25_T125")

def close():
    """
    Closes a running ZSD25_T125 transaction.

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
        ZSD25_T125 when it's not running.
    """

    global _sess
    global _main_wnd
    global _stat_bar

    if _sess is None:
        raise TransactionNotStartedError("Cannot close ZSD25_T125 when it's "
        "actually not running! Use the biaZSD25.start() procedure to run "
        "the transaction first of all.")

    _sess.EndTransaction()

    if _is_popup_dialog():
        _close_popup_dialog(confirm = True)

    _sess = None
    _main_wnd = None
    _stat_bar = None

def export(file_path: str, conditions: bool, layout: str, sales_org: str,
           states: Union[list, tuple], agreements: Union[list, tuple] = None,
           sales_offs: Union[list, tuple] = None):
    """
    Loads and exports agreement data into a file.

    Loads agreement data based on agreement states, agreement numbers, \n
    sales organization and sales offices. If any records are found the loaded \n
    data is then exported into a plain text file defined by a valid path. \n
    The format of the exported data is defined by the name of the used \n
    layout. The resulting format may differ if displaying of conditions is turend on.

    Params:
    -------
    file_path:
        Path to the text file to which
        the loaded data will be exported.

    conditions:
        Indicates whether sales conditions
        should be displayed in the loaded data.

    layout:
        Name of the layout applied to the loaded data.

    sales_org:
        Sales organization numerical code. \n
        A valid code is a 4-digit number stored
        as a string (e.g. '0001').

    states:
        Agreement states. \n
        A combination of at least one of
        the following values: 'A', 'B', 'C', "".

    agreements:
        List of agreement numbers (optional).

    sales_offs:
        A ist of sales office numerical codes. \n
        A valid code is a 4-digit number stored
        as a string (e.g. '0001').

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
        raise TransactionNotStartedError("Cannot export accounting data from ZSD25_T125 "
        "when it's actually not running! Use the biaZSD25.start() procedure to run "
        "the transaction first of all.")

    comb_a = agreements is None and sales_offs is None
    comb_b = agreements is None or sales_offs is None

    if not (comb_a or not comb_b):
        raise ValueError(
            "Parameter combination not permitted: "
            f"agreements = {agreements}; sales_offs = {sales_offs}!"
        )

    # sales org must be entered before
    # entering any agreement numbers!!
    _set_sales_org(sales_org)

    if agreements is not None:
        _set_agreements(agreements)

    _set_agreement_states(states)
    _set_display_conditions(conditions)
    _set_validity_end_date()
    _set_layout(layout)
    _load_data()

    if sales_offs is not None:
        _set_variable_key_filter(sales_offs)

    _export_to_file(file_path)
