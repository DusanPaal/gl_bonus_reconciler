# pylint: disable = C0103, C0301, E0110, R0912

"""
The 'biaReport.py' module contains procedures
for generation of user Excel reconciliation
reports.
"""

from datetime import datetime
import pandas as pd
from pandas import DataFrame, ExcelWriter, Series
from xlsxwriter.format import Format
from xlsxwriter.worksheet import Worksheet

_SPACE = " "
_UNDERSCORE = "_"
_HEADER_ROW_IDX = 1

def _write_to_excel(
        wrtr: ExcelWriter, data: DataFrame, sht_name: str,
        print_index: bool = False, print_header: bool = True) -> None:
    """
    Writes data into a worksheet of an excel workbook.

    Params:
    -------
    wrtr:
        Excel writer object.

    data:
        A DataFrame object containing records (data) to write.

    sht_name:
        Name of the worksheet to which data will be written.

    print_index:
        Indicates whether a DataFrame row indexes should
        be written along with the contained data.

    print_header:
        Indicates whether DataFrame columns should
        be written along with the contained data.
    """

    def replace_char(char, repl, axis) -> list:
        result = [str(rec).replace(char, repl) if rec is not None else None for rec in axis]
        return result

    data.index.names = replace_char(_UNDERSCORE, _SPACE, data.index.names)
    data.columns = replace_char(_UNDERSCORE, _SPACE, data.columns)

    if data.index.inferred_type == "string":
        data.index = replace_char(_SPACE, _UNDERSCORE, data.index)

    data.to_excel(wrtr, sht_name, index = print_index, header = print_header)

    data.columns = replace_char(_SPACE, _UNDERSCORE, data.columns)
    data.index.names = replace_char(_SPACE, _UNDERSCORE, data.index.names)

    if data.index.inferred_type == "string":
        data.index = replace_char(_SPACE, _UNDERSCORE, data.index)

def _col_to_rng(
        data: DataFrame, first_col: str, last_col: str = None,
        row: int = -1, last_row: int = -1) -> str:
    """
    Converts data position in a DataFrame object into excel range notation (e.g. 'A1:D1', 'B2:G2').
    If 'last_col' is None, then only single-column range will be generated (e.g. 'A:A', 'B1:B1').
    If 'row' is '-1', then the generated range will span all the column(s) rows (e.g. 'A:A', 'E:E').
    If 'last_row' is provided, then the generated range will include all data records up to the last row (including).

    Params:
    -------
    data: Data for which colum names should be converted to a range.
    first_col: Name of the first column.
    last_col: Name of the last column.
    row: Index of the row for which the range will be generated.
    last_row: Index of the last data row which location will be considered in the resulting range.

    Returns:
    ---------
    Excel data range notation.
    """

    if isinstance(first_col, str):
        first_col_idx = data.columns.get_loc(first_col)
    elif isinstance(first_col, int):
        first_col_idx = first_col
    else:
        assert False, "Argument 'first_col' has invalid type!"

    first_col_idx += 1
    prim_lett_idx = first_col_idx // 26
    sec_lett_idx = first_col_idx % 26

    lett_a = chr(ord('@') + prim_lett_idx) if prim_lett_idx != 0 else ""
    lett_b = chr(ord('@') + sec_lett_idx) if sec_lett_idx != 0 else ""
    lett = "".join([lett_a, lett_b])

    if last_col is None:
        last_lett = lett
    else:

        if isinstance(last_col, str):
            last_col_idx = data.columns.get_loc(last_col)
        elif isinstance(last_col, int):
            last_col_idx = last_col
        else:
            assert False, "Argument 'last_col' has invalid type!"

        last_col_idx += 1
        prim_lett_idx = last_col_idx // 26
        sec_lett_idx = last_col_idx % 26

        lett_a = chr(ord('@') + prim_lett_idx) if prim_lett_idx != 0 else ""
        lett_b = chr(ord('@') + sec_lett_idx) if sec_lett_idx != 0 else ""
        last_lett = "".join([lett_a, lett_b])

    if row == -1:
        rng = ":".join([lett, last_lett])
    elif first_col == last_col and row != -1 and last_row == -1:
        rng = f"{lett}{row}"
    elif first_col == last_col and row != -1 and last_row != -1:
        rng = ":".join([f"{lett}{row}", f"{lett}{last_row}"])
    elif first_col != last_col and row != -1 and last_row == -1:
        rng = ":".join([f"{lett}{row}", f"{last_lett}{row}"])
    elif first_col != last_col and row != -1 and last_row != -1:
        rng = ":".join([f"{lett}{row}", f"{last_lett}{last_row}"])
    else:
        assert False, "Undefined argument combination!"

    return rng

def _get_rng(data: DataFrame, col_name: str, val: object) -> str:
    """
    Converts data position in a DataFrame object column
    containing a specific value into excel cell range
    notation (e.g. 'A2:A4' is returned if first DataFrame
    column contains the specific value in the second and
    fourth row).

    Note that if any value other than 'val' appears in between
    the firsta and last row, this will be included in the range,
    as well. Therefore, it is recommended to sort the column data
    before generating the excel range.
    """

    if data[data[col_name] == val].index.empty:
        return None

    first_row = data[data[col_name] == val].index.min() + 2
    last_row = data[data[col_name] == val].index.max() + 2
    col_idx = data.columns.get_loc(col_name) + 1

    first_row = int(first_row)
    last_row = int(last_row)

    col_char = chr(ord('@') + col_idx)
    rng = ":".join([f"{col_char}{first_row}", f"{col_char}{last_row}"])

    return rng

def _get_col_width(vals: Series, col_name: str, add_width: int = 0) -> int:
    """
    Returns an iteger representing the width of a column calculated
    as the maximum number of characters contained in column name and
    column values plus additional points provided with the 'add_width'
    argument (default 0 points).
    """

    ALPHA = 1 # additional offset factor

    if col_name.isnumeric():
        return 14 + add_width

    if col_name == "Agreement":
        return 11 + add_width

    if col_name in ("Valid_From", "Valid_To"):
        return 11 + add_width

    if col_name == "Payments":
        return 12 + add_width

    data_vals = vals.astype("string").dropna().str.len()
    data_vals = list(data_vals)
    data_vals.append(len(str(col_name)))
    width = max(data_vals) + ALPHA + add_width

    return width

def _to_excel_serial(data: DataFrame, date_flds: list) -> DataFrame:
    """
    Converts a datetime object into excel-compatible
    date integer serial format.
    """

    serialized = data.copy()

    for fld_name in date_flds:

        if fld_name not in serialized.columns:
            continue

        serialized[fld_name] = serialized[fld_name].apply(
            lambda x: (x - datetime(1899, 12, 30).date()).days if not pd.isna(x) else x
        )

    return serialized

def _set_format(fmt: Format, sht: Worksheet, rng: str) -> None:
    """Applies visual formatting to a cell range."""
    sht.conditional_format(rng, {"type": "no_errors", "format": fmt})

def _create_kote_sheet(wrtr: ExcelWriter, data: DataFrame, sht_name: str, **formats) -> None:
    """
    Creates a new worksheet named 'KOTE890' in the user report, writes data
    into the sheet and applies column formatting defined in 'formats' argument.
    """

    data["Client"] = pd.to_numeric(data["Client"])
    data["Sales_Organization"] = pd.to_numeric(data["Sales_Organization"])
    data["Sales_Office"] = pd.to_numeric(data["Sales_Office"])

    # some customer numbers are not numerical (e.g. Germany);
    # convert to numeric only those which are
    data["Customer"] = data["Customer"].astype("object")
    mask = data["Customer"].str.isnumeric()
    data.loc[mask, "Customer"] = pd.to_numeric(data.loc[mask, "Customer"])

    date_fields = ("Valid_To", "Valid_From")
    data = _to_excel_serial(data, date_fields)

    # replace underscores in header with spaces, write data to an excel sheet
    # then replace header spaces back with underscores for better code readability
    _write_to_excel(wrtr, data, sht_name)

    # get the workbook sheet and apply field formats
    sht = wrtr.sheets[sht_name]

    for idx, col in enumerate(data.columns):

        col_width = _get_col_width(data[col], col)

        if col == "Client":
            col_fmt = formats["clients"]
        elif col in ("Sales_Organization", "Sales_Office"):
            col_fmt = formats["codes"]
        elif col == "Sales_Office":
            col_fmt = formats["codes"]
        elif col == "Condition_Record_Number":
            col_fmt = formats["conditions"]
        elif col == "Agreement":
            col_fmt = formats["agreements"]
        elif col in ("Valid_To", "Valid_From"):
            col_fmt = formats["date"]
        else:
            col_fmt = formats["align"]

        # apply new column format
        sht.set_column(idx, idx, col_width, col_fmt)

    rng = _col_to_rng(data, "Client", "Condition_Record_Number", _HEADER_ROW_IDX)
    _set_format(formats["header"], sht, rng)
    sht.freeze_panes(_HEADER_ROW_IDX, 0)

def _create_kona_sheet(wrtr: ExcelWriter, data: DataFrame, sht_name: str, **formats) -> None:
    """
    Creates a new worksheet named 'KONA' in the user report, writes data
    into the sheet and applies column formatting defined in 'formats' argument.
    """

    # if there was no data found in SAP, indicate this by writing
    # a message to the sheet so that users are aware of the reason
    if data is None:

        data = DataFrame.from_dict({
            "Reason_for_missing_data": ["No relevant records found."]
        })

        _write_to_excel(wrtr, data, sht_name)
        sht = wrtr.sheets[sht_name]

        # adjust column width to fit the content
        for idx, col in enumerate(data.columns):
            sht.set_column(idx, idx, _get_col_width(data[col], col))

        return

    # convert field values to appropriate data types
    data["Client"] = pd.to_numeric(data["Client"])
    data["Sales_Organization"] = pd.to_numeric(data["Sales_Organization"])
    data["Sales_Office"] = pd.to_numeric(data["Sales_Office"])

    date_fields = ("Created_On", "Changed_On", "Valid_To", "Valid_From")
    data = _to_excel_serial(data, date_fields)
    _write_to_excel(wrtr, data, sht_name, False)

    # get the workbook sheet and apply field formats
    sht = wrtr.sheets[sht_name]

    for idx, col in enumerate(data.columns):

        col_width = _get_col_width(data[col], col)

        if col == "Client":
            col_fmt = formats["clients"]
        elif col in date_fields:
            col_fmt = formats["date"]
        elif col in ("Sales_Organization", "Sales_Office"):
            col_fmt = formats["codes"]
        elif col == "Agreement":
            col_fmt = formats["agreements"]
        else:
            col_fmt = formats["align"]

        # apply new column format
        sht.set_column(idx, idx, col_width, col_fmt)

    rng = _col_to_rng(data, "Client", "Settlement_Periods", _HEADER_ROW_IDX)
    _set_format(formats["header"], sht, rng)
    sht.freeze_panes(_HEADER_ROW_IDX, 0)

def _create_zsd25_sheet(wrtr: ExcelWriter, data: DataFrame, sht_name: str, **formats) -> None:
    """
    Creates a new worksheet in the user report, writes
    ZSD25 data into the sheet and formats the columns.
    """

    if data is None:

        data = DataFrame.from_dict({
            "Reason_for_missing_data": ["No relevant records found."]
        })

        _write_to_excel(wrtr, data, sht_name, False)
        sht = wrtr.sheets[sht_name]

        # adjust column width to fit the content
        for idx, col in enumerate(data.columns):
            sht.set_column(idx, idx, _get_col_width(data[col], col))

        return

    monetary_fields = (
        "Condition_Based_Value",
        "Condition_Value",
        "Accruals", "Open_Accruals",
        "Accruals_Reversed",
        "Payments", "Open_Value",
    )

    date_fields = ("Valid_To", "Valid_From")
    data = _to_excel_serial(data, date_fields)
    _write_to_excel(wrtr, data, sht_name, False)

    # get the workbook sheet and apply field formats
    sht = wrtr.sheets[sht_name]
    rng = _col_to_rng(data, data.columns[0], data.columns[-1], _HEADER_ROW_IDX)
    _set_format(formats["header"], sht, rng)

    for idx, col in enumerate(data.columns):

        col_width = _get_col_width(data[col], col)

        if col == "Condition_Rate":
            col_fmt = formats["condition_rate"]
        elif col in monetary_fields:
            col_fmt = formats["money"]
        elif col in date_fields:
            col_fmt = formats["date"]
        else:
            col_fmt = formats["align"]

        # apply new column format
        sht.set_column(idx, idx, col_width, col_fmt)

    sht.freeze_panes(_HEADER_ROW_IDX, 0)

def _create_zsd25_loc_calc_sheet(wrtr: ExcelWriter, data: DataFrame, sht_name: str, **formats) -> None:
    """
    Creates a new worksheet named 'Local Entity Bonuses' in the user report, writes data
    into the sheet and applies column formatting defined in 'formats' argument.
    """

    accs = [col for col in data.columns if col.isnumeric()]

    data = data[[
        "Rebate_Recipient", "Name", "Country", "Agreement_Type_Code",
        "Agreement", "Status", "Description_Of_Agreement",
        "Condition_Value","Payments", "Open_Accruals",
        "Corr_to_LC", "LC_Open_Accr"] + accs + ["Difference", "Currency",
        "Arrangement_Calendar", "Valid_From", "Valid_To"
    ]]

    monetary_fields = (
        "Condition_Value", "Condition_Rate",
        "Payments", "Open_Accruals", "Corr_to_LC",
        "LC_Open_Accr", "Difference"
    )

    blue_head_fields = ("Corr_to_LC", "LC_Open_Accr", "Difference")
    date_fields = ("Valid_To", "Valid_From")

    data = _to_excel_serial(data, date_fields)
    _write_to_excel(wrtr, data, sht_name, False)

    # get the workbook sheet and apply field formats
    sht = wrtr.sheets[sht_name]

    # adjust column width to fit the content
    for idx, col in enumerate(data.columns):

        col_width = _get_col_width(data[col], col, add_width = 3.5)

        if col.isnumeric() or col in blue_head_fields:
            header_fmt = formats["sl_header"] # blue
        else:
            header_fmt = formats["gl_header"] # orange

        if col.isnumeric() or col in monetary_fields:
            col_fmt = formats["money"]
        else:
            col_fmt = formats["align"]

        if col in date_fields:
            col_fmt = formats["date"]

        # apply new column format
        sht.set_column(idx, idx, col_width, col_fmt)

        # apply new header format
        rng = _col_to_rng(data, col, row = _HEADER_ROW_IDX)
        _set_format(header_fmt, sht, rng)

    # mark open accruals > 0
    for idx in data[data["Open_Accruals"] > 0].index:
        rng = _col_to_rng(data, "Open_Accruals", row = idx + 2)
        _set_format(formats["warnings"], sht, rng)

    # mark non-zero difference
    for idx in data[data["Difference"] != 0].index:
        rng = _col_to_rng(data, "Difference", row = idx + 2)
        _set_format(formats["warnings"], sht, rng)

    # freeze data header row and set autofiler on all fields
    sht.freeze_panes(_HEADER_ROW_IDX, 0)
    rng = _col_to_rng(data, "Rebate_Recipient", "Valid_To", row = _HEADER_ROW_IDX)
    sht.autofilter(rng)

def _create_zsd25_glob_calc_sheet(wrtr: ExcelWriter, data: DataFrame, sht_name: str, **formats):
    """
    Creates a new worksheet named 'HQ Bonuses' in the user report, writes data
    into the sheet and applies column formatting defined in 'formats' argument.
    """

    if data is None:

        data = DataFrame.from_dict({
            "Reason_for_missing_data": ["No relevant records found."]
        })

        _write_to_excel(wrtr, data, sht_name, False)
        sht = wrtr.sheets[sht_name]

        # adjust column width to fit the content
        for idx, col in enumerate(data.columns):
            sht.set_column(idx, idx, _get_col_width(data[col], col))

        return

    accs = [col for col in data.columns if col.isnumeric()]

    data = data[[
        "Rebate_Recipient", "Name", "Country", "Agreement_Type_Code",
        "Agreement", "Status", "Description_Of_Agreement",
        "Condition_Based_Value", "Payments", "Open_Accruals",
        "Corr_to_LC", "LC_Open_Accr"] + accs + ["Difference",
        "Currency", "Arrangement_Calendar", "Valid_From", "Valid_To"
    ]]

    monetary_fields = (
        "Condition_Based_Value",
        "Payments",
        "Open_Accruals",
        "Corr_to_LC",
        "LC_Open_Accr",
        "Difference",
        "Open_Accruals"
    )

    date_fields = ("Valid_To", "Valid_From")
    blue_head_fields = ("Corr_to_LC", "LC_Open_Accr", "Difference")

    data = _to_excel_serial(data, date_fields)
    _write_to_excel(wrtr, data, sht_name, False)

    # get the workbook sheet and apply field formats
    sht = wrtr.sheets[sht_name]

    # detect and apply column data/header formats
    for idx, col in enumerate(data.columns):

        col_width = _get_col_width(data[col], col, add_width = 3.5)

        if col.isnumeric() or col in blue_head_fields:
            header_fmt = formats["sl_header"] # blue
        else:
            header_fmt = formats["gl_header"] # orange

        if col.isnumeric() or col in monetary_fields:
            col_fmt = formats["money"]
        else:
            col_fmt = formats["align"]

        if col in date_fields:
            col_fmt = formats["date"]

        # apply new column format
        sht.set_column(idx, idx, col_width, col_fmt)

        # apply new header format
        rng = _col_to_rng(data, col, row = _HEADER_ROW_IDX)
        _set_format(header_fmt, sht, rng)

    # mark open accruals > 0
    for idx in data[data["Open_Accruals"] > 0].index:
        rng = _col_to_rng(data, "Open_Accruals", row = idx + 2)
        _set_format(formats["warnings"], sht, rng)

    # mark non-zero difference
    for idx in data.query("Difference != 0 and Difference.notna()").index:
        rng = _col_to_rng(data, "Difference", row = idx + 2)
        _set_format(formats["warnings"], sht, rng)

    # freeze data header row and set autofiler on all fields
    sht.freeze_panes(_HEADER_ROW_IDX, 0)
    header_rng = _col_to_rng(data, "Rebate_Recipient", "Valid_To", row = _HEADER_ROW_IDX)
    sht.autofilter(header_rng)

def _create_period_overview_sheet(wrtr: ExcelWriter, data: DataFrame, sht_name: str, **formats) -> None:
    """
    Creates a new worksheet named 'Period overview' in the user report, writes data
    into the sheet and applies column formatting defined in 'formats' argument.
    """

    FISCAL_YEAR_COL_IDX = 0
    PERIOD_COL_IDX = 1
    FIRST_COL_IDX = 0
    LAST_COL_IDX = data.columns.size + 1

    def ranger(x, y) -> str:
        return "".join([f"A{y}:", chr(ord('@') + data.columns.get_loc(x) + 3), str(y)])

    _write_to_excel(wrtr, data, sht_name, print_index = True)

    # get the workbook sheet and apply field formats
    sht = wrtr.sheets[sht_name]

    for col in data.columns:
        sht.set_column(_col_to_rng(data, col), None, formats["align"])

    rng = _col_to_rng(data, FIRST_COL_IDX, LAST_COL_IDX, _HEADER_ROW_IDX)
    _set_format(formats["header"], sht, rng)

    # fix the width for these 2 fields since the vals have same length acrocc reports
    sht.set_column(FISCAL_YEAR_COL_IDX, FISCAL_YEAR_COL_IDX, width = 11)
    sht.set_column(PERIOD_COL_IDX, PERIOD_COL_IDX, width = 7)

    # adjust column width to fit the content
    for col_idx, col in enumerate(data.columns, start = 2):
        col_width = _get_col_width(data[col], col) + 2 # plus 2 pts for a better visual in report
        sht.set_column(col_idx, col_idx, col_width, formats["money"])

    # format pivot table footer
    footer_idx = data.shape[0] + 1
    _set_format(formats["footer"], sht, ranger("Grand_Total", footer_idx))

    # freeze header row
    sht.freeze_panes(_HEADER_ROW_IDX, 0)

def _create_final_summary_sheet(wrtr: ExcelWriter, data: DataFrame, sht_name: str, **formats) -> None:
    """
    Creates a new worksheet named 'Summary' in the user report, writes data
    into the sheet and applies column formatting defined in 'formats' argument.
    """

    data.to_excel(wrtr, sht_name, index = False)
    sht = wrtr.sheets[sht_name]

    for col in data.columns:
        sht.set_column(_col_to_rng(data, col), None, formats["align"])

    for col in data.columns:
        if col.isnumeric() or col == "Difference":
            sht.set_column(_col_to_rng(data, col), None, formats["money"])

    for col in data.columns:
        for row_idx in (1, 4, 6):
            rng = _col_to_rng(data, col, row = row_idx)
            _set_format(formats["header"], sht, rng)

        for row_idx in (2, 3, 7, 8):
            rng = _col_to_rng(data, col, row = row_idx)
            _set_format(formats["light_blue"], sht, rng)

        rng = _col_to_rng(data, col, row = 5)
        _set_format(formats["light_blue_bolt"], sht, rng)

    for idx, col in enumerate(data.columns):
        col_width = _get_col_width(data[col], col)

        if col.isnumeric() or col == "Difference":
            col_width += 2

        sht.set_column(idx, idx, col_width)

def create_account_sheet(
        wrtr: ExcelWriter, acc: int,
        data: DataFrame, sht_name: str, **formats) -> None:
    """
    Creates a new worksheet in the user report. Then, data will be written into
    the sheet and column formatting defined in 'formats' param will be applied.
    """

    # reorder fields
    data = data[[
        "Status", "Text", "Condition", "Category",
        "Customer", "Agreement", "Note", "LC_Amount_Sum"
    ]]

    _write_to_excel(wrtr, data, acc)
    sht = wrtr.sheets[sht_name]

    for idx, col in enumerate(data.columns):

        col_width = _get_col_width(data[col], col, add_width = 3.5)

        if col == "LC_Amount_Sum":
            col_format = formats["money"]
        else:
            col_format = formats["align"]

        sht.set_column(idx, idx, col_width, col_format)

    rng = _col_to_rng(data, "Status", "LC_Amount_Sum", _HEADER_ROW_IDX)
    _set_format(formats["header"], sht, rng)

    # apply specific cell format to status marked with "CHECK"
    chk_rng = _get_rng(data, "Status", "CHECK")

    if chk_rng is not None:
        _set_format(formats["check_agreement"], sht, chk_rng)

    # apply specific cell format to status marked with "incorect text"
    wrong_txt_rng = _get_rng(data, "Status", "x")

    if wrong_txt_rng is not None:
        _set_format(formats["incorrect_text"], sht, wrong_txt_rng)

    sht.freeze_panes(_HEADER_ROW_IDX, 0)
    header_rng = _col_to_rng(data, "Status", "LC_Amount_Sum", row = _HEADER_ROW_IDX)
    sht.autofilter(header_rng)

def _create_hq_comparison_sheet(
        wrtr: ExcelWriter, data: DataFrame,
        sht_name: str, **formats) -> None:
    """
    Creates a new worksheet named 'HQ Compare'
    in the user report, writes data into the
    sheet and applies column formatting defined
    in 'formats' argument.
    """

    _write_to_excel(wrtr, data, sht_name)

    sht = wrtr.sheets[sht_name]
    rng = _col_to_rng(data, "HQ_Agreements", "Overview", _HEADER_ROW_IDX)
    _set_format(formats["dark_gray_header"], sht, rng)

    for idx, col in enumerate(data.columns):
        width = _get_col_width(data[col], col)
        sht.set_column(idx, idx, width, formats["align"])

    sht.freeze_panes(_HEADER_ROW_IDX, 0)

def _create_le_comparison_sheet(
        wrtr: ExcelWriter, data: DataFrame,
        sht_name: str, **formats) -> None:
    """
    Creates new worksheets named 'Local Compare'
    and 'Result' in the user report, writes data
    into both sheets and applies column formatting
    defined in 'formats' argument.
    """

    _write_to_excel(wrtr, data, sht_name)
    sht = wrtr.sheets[sht_name]

    dark_header_flds = (
        "Local_Entity_Bonuses_Agreements", "HQ_Agreements",
        "Overview", "Amount_Compared"
    )

    for idx, col in enumerate(data.columns):

        sht.set_column(_col_to_rng(data, col), None, formats["align"])

        if col in dark_header_flds:
            rng = _col_to_rng(data, col, row = _HEADER_ROW_IDX)
            _set_format(formats["dark_gray_header"], sht, rng)
        else:
            rng = _col_to_rng(data, col, row = _HEADER_ROW_IDX)
            _set_format(formats["light_gray_header"], sht, rng)

        col_width = _get_col_width(data[col], col)

        if col == "Difference_Local_Entity_Bonuses_Agreement":
            sht.set_column(idx, idx, col_width, formats["money"])
        else:
            sht.set_column(idx, idx, col_width)

    sht.freeze_panes(_HEADER_ROW_IDX, 0)

def _create_info_sheet(
        wrtr: ExcelWriter, data: DataFrame,
        sht_name: str, **formats) -> None:
    """
    Creates a new worksheet named 'Info' in
    the user report, writes data into the
    sheet and applies column formatting defined
    in 'formats' argument.
    """

    data.loc["Company_code", :] = pd.to_numeric(data.loc["Company_code", :])
    data.loc["Sales_organization_global", :] = pd.to_numeric(data.loc["Sales_organization_global", :])
    data.loc["Sales_organization_local", :] = pd.to_numeric(data.loc["Sales_organization_local", :])
    data.loc["Sales_offices", :] = pd.to_numeric(data.loc["Sales_offices", :])
    data.loc["Time", 0] = data.loc["Time", 0].strftime("%H:%M:%S")
    data.loc["Date", 0] = (data.loc["Date", 0] - datetime(1899, 12, 30).date()).days

    data = data.reset_index().rename({"index": "Parameter"}, axis = 1)
    data["Parameter"] = data["Parameter"].str.replace("_", " ")
    _write_to_excel(wrtr, data, sht_name, print_header = False)
    data["Parameter"] = data["Parameter"].str.replace(" ", "_")

    sht = wrtr.sheets[sht_name]

    for col in data.columns:
        sht.set_column(_col_to_rng(data, col), None, formats["align"])

    rng = _col_to_rng(data, first_col = 0, last_col = 0, row = 1, last_row = data.shape[0])
    _set_format(formats["header"], sht, rng)

    data.index = data.index.astype("int")

    code_field_rows = (
        data[data["Parameter"] == "Company_code"].index[0] + 1,
        data[data["Parameter"] == "Sales_offices"].index[0] + 1,
        data[data["Parameter"] == "Sales_organization_global"].index[0] + 1,
        data[data["Parameter"] == "Sales_organization_local"].index[0] + 1
    )

    first_col_idx = int(data.columns.min()) + 1
    last_col_idx = data.columns.size - 1

    for idx in code_field_rows:
        rng = _col_to_rng(data, first_col_idx, last_col_idx, row = idx)
        _set_format(formats["codes"], sht, rng)

    _set_format(formats["time"], sht,
        _col_to_rng(data, first_col = 1, last_col = 1,
            row = data[data["Parameter"] == "Time"].index[0] + 1
    ))

    _set_format(formats["money"], sht,
        _col_to_rng(data, first_col = 1, last_col = 1,
            row = data[data["Parameter"] == "Exchange_rate"].index[0] + 1
    ))

    _set_format(formats["date"], sht,
        _col_to_rng(data, first_col = 1, last_col = 1,
            row = data[data["Parameter"] == "Date"].index[0] + 1
    ))

    _set_format(formats["values"], sht,
        _col_to_rng(data, first_col = 1, last_col = last_col_idx,
            row = _HEADER_ROW_IDX, last_row = data.shape[0]
    ))

    for idx, col in enumerate(data.columns):
        sht.set_column(idx, idx, _get_col_width(data[col], col) + 1)

def create(file_path: str, **data_sets: dict) -> None:
    """
    Creates a local user report in .xlsx file format
    from data resulting reconciliation process outcome.

    Params:
    -------
    file_path:
        Path to the report file.

    data_sets:
        A dict of DataFrame objects containing reconciliation
        data (values), mapped to textual descriptors (keys).

    Returns:
    --------
    None.
    """

    accs = data_sets["check_text_summs"].keys()

    with ExcelWriter(file_path, engine = "xlsxwriter") as wrtr:

        report = wrtr.book # pylint: disable=E1101

        # define visual formats which will be applied to particular fields
        money_fmt = report.add_format({"num_format": "#,##0.00", "align": "center"})
        code_fmt = report.add_format({"num_format": "0000", "align": "center"})
        categ_fmt = report.add_format({"num_format": "000", "align": "center"})
        int_10_fmt = report.add_format({"num_format": "0"*10, "align": "center"})
        date_fmt = report.add_format({"num_format": "dd.mm.yyyy", "align": "center"})
        align_fmt = report.add_format({"align": "center"})

        orange_fmt = report.add_format({
            "align": "center",
            "bg_color": "#F06B00",
            "font_color": "white",
            "bold": True,
            "italic": True
        })

        blue_header_fmt = report.add_format({
            "align": "center",
            "bg_color": "blue",
            "font_color": "white",
            "bold": True,
            "italic": True
        })

        dark_gray_header_fmt = report.add_format({
            "align": "center",
            "bg_color": "#595959",
            "font_color": "white",
            "bold": True,
            "italic": True
        })

        light_gray_header_fmt = report.add_format({
            "align": "center",
            "bg_color": "#D9D9D9",
            "font_color": "white",
            "bold": True,
            "italic": True
        })

        light_blue_fmt = report.add_format({
            "align": "center",
            "bg_color": "#DDEBF7"
        })

        light_blue_bolt_fmt = report.add_format({
            "align": "center",
            "bg_color": "#DDEBF7",
            "bold": True
        })

        pvt_footer_fmt = report.add_format({
            "align": "center",
            "bg_color": "#5EEB84",
            "bold": True
        })

        check_fmt = report.add_format({"bg_color": "#FFE699"})
        incorr_txt_fmt = report.add_format({"bg_color": "#FFCCCC"})
        warning_fmt = report.add_format({"bg_color": "#F7C39F"})
        cond_rate_fmt = report.add_format({"num_format": "0.000", "align": "center"})

        _create_info_sheet(wrtr, data_sets["info"], "Info",
            align = align_fmt, time = align_fmt, codes = code_fmt, money = money_fmt,
            header = orange_fmt, values = light_blue_bolt_fmt, date = date_fmt
        )

        _create_kote_sheet(wrtr, data_sets["kote_data"], "KOTE890",
            codes = code_fmt, header = orange_fmt, align = align_fmt, agreements = int_10_fmt,
            conditions = int_10_fmt, clients = categ_fmt, category = categ_fmt, date = date_fmt
        )

        _create_kona_sheet(wrtr, data_sets["kona_data"], "KONA",
            codes = code_fmt, header = orange_fmt, align = align_fmt,
            agreements = int_10_fmt, clients = categ_fmt, date = date_fmt
        )

        _create_period_overview_sheet(wrtr, data_sets["period_overview"], "Period overview",
            align = align_fmt, header = orange_fmt,
            money = money_fmt, footer = pvt_footer_fmt
        )

        _create_zsd25_sheet(wrtr, data_sets["glob_bonus_data"], "ZSD25 HQ",
            align = align_fmt, header = orange_fmt, money = money_fmt,
            condition_rate = cond_rate_fmt, date = date_fmt
        )

        _create_zsd25_sheet(wrtr, data_sets["loc_bonus_data"], "ZSD25 Local Entity",
            align = align_fmt, header = orange_fmt, money = money_fmt,
            condition_rate = cond_rate_fmt, date = date_fmt
        )

        _create_zsd25_sheet(wrtr, data_sets["loc_conditions_data"], "ZSD25 Local Entity Conditions",
            align = align_fmt, header = orange_fmt, money = money_fmt,
            condition_rate = cond_rate_fmt, date = date_fmt
        )

        _create_zsd25_loc_calc_sheet(wrtr, data_sets["loc_bonus_calcs"], "Local Entity Bonuses",
            align = align_fmt, sl_header = blue_header_fmt, gl_header = orange_fmt,
            warnings = warning_fmt, money = money_fmt, date = date_fmt
        )

        _create_zsd25_glob_calc_sheet(wrtr, data_sets["glob_bonus_calcs"], "HQ Bonuses",
            align = align_fmt, sl_header = blue_header_fmt, gl_header = orange_fmt,
            warnings = warning_fmt, money = money_fmt, date = date_fmt
        )

        _create_final_summary_sheet(wrtr, data_sets["final_summary"], "Summary",
            align = align_fmt, header = orange_fmt, money = money_fmt,
            light_blue = light_blue_fmt, light_blue_bolt = light_blue_bolt_fmt
        )

        for acc in accs:
            create_account_sheet(wrtr, acc, data_sets["check_text_summs"][acc], str(acc),
            align = align_fmt, header = orange_fmt, money = money_fmt,
            check_agreement = check_fmt, incorrect_text = incorr_txt_fmt
        )

        if data_sets["hq_comparison"] is not None:
            _create_hq_comparison_sheet(wrtr, data_sets["hq_comparison"], "HQ Compare",
            align = align_fmt, dark_gray_header = dark_gray_header_fmt
        )

        if data_sets["le_comparison"] is not None:
            for sht_name in ("Local Compare", "Result"):
                _create_le_comparison_sheet(wrtr, data_sets["le_comparison"], sht_name,
                align = align_fmt, dark_gray_header = dark_gray_header_fmt,
                light_gray_header = light_gray_header_fmt, money = money_fmt
        )
