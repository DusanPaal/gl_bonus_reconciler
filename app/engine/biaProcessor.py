# pylint: disable = C0123, C0103, C0301, C0302, E0401, E0611, W0602, W0703, W1203

"""
The 'biaProcessor.py' contains procedures for performing
direct operations on accounting data such as parsing, cleaning,
conversion, evaluation and querying.
"""

from datetime import date
from enum import Enum
from io import StringIO
import logging
from multiprocessing import Pool
import re
from typing import Any, Union

import numpy as np
import pandas as pd
from pandas import DataFrame, Series
from pandas.api.types import infer_dtype

from engine.biaUtils import deprecated

class Marks(Enum):
    """
    'Status' values of items contained
    in the GL account summary data.
    """
    INVALD_TEXT_FMT = "x"
    AGREEMENT_CLOSED = "CHECK"

class FileTypes(Enum):
    """
    File types used for storing
    data exported from a transaction.
    """
    TXT = "txt"
    DAT = "dat"

_logger = logging.getLogger("master")

_accum = {
    "fbl3n_data": {},
    "text_summs": {},
    "check_text_summs": {},
    "kona_data": {},
    "kote_data": {},
    "loc_bonus_data": {},
    "glob_bonus_data": {},
    "glob_bonus_calcs": {},
    "loc_bonus_calcs": {},
    "glob_agreement_comparison": {},
    "loc_agreement_comparison": {},
    "loc_conditions": {},
    "fs10n_data": {},
    "final_summary": {},   # comparison of general- and subledger account balances
    "yearly_acc_summ": {}, # raw data fot period overview pivot
    "period_overview": {},
    "info": {}
}

_SE16_KOTE_HEADER = (
    "Client",
    "Application",
    "Condition_Type",
    "Sales_Organization",
    "Sales_Office",
    "Customer",
    "Valid_To",
    "Agreement",
    "Valid_From",
    "Condition_Record_Number"
)

_SE16_KONA_HEADER = (
    "Client",
    "Agreement",
    "Sales_Organization",
    "Distribution_Channel",
    "Division",
    "Sales_Office",
    "Sales_Group",
    "Agreement_Type",
    "Agreement_Category",
    "Application",
    "Created_By",
    "Created_On",
    "Time",
    "Changed_By",
    "Changed_On",
    "Time_Of_Change",
    "Rebate_Recipient",
    "Currency",
    "Maximum_Rebate",
    "Category",
    "Purchase_Organization",
    "Condition_Granter",
    "Verification_Levels",
    "Agreement_Status",
    "Valid_From",
    "Valid_To",
    "Condition_Type_Group",
    "Description_Of_Agreement",
    "Payment_Method",
    "Addition_Value_Days",
    "Fixed_Value_Date",
    "Terms_Of_Payment",
    "Higher-Level_Agreement",
    "Settlement_Calendar",
    "Arrangemet_Calendar",
    "Pers_Resp_Bv_Comp_1",
    "Bv_Comparison_Date_1",
    "Busvol_Final_Settl",
    "Pgr",
    "Promotion",
    "Scope_Of_Statement",
    "Company_Code",
    "Predecessor",
    "Updt",
    "Bdtyp",
    "Settlement_Periods",
    "Sett_Par",
    "Rest",
    "Trgrp",
    "Trigcond",
    "Agg_Level_1",
    "Agg_Level_2",
    "Summ_Lev",
    "Bvol_Comppartsettl",
    "Pers_Resp_Bv_Comp_2",
    "Bv_Comparison_Date_2",
    "Contract_Type_1",
    "Contract_No_1",
    "Flow_Type_1",
    "Ind_Enhanced_Rebate_With_Variable_Key",
    "Indicates_Indirect_Settlement",
    "Indicates_Periodic_Rebate_Settlement",
    "Contract_Type_2",
    "Contract_No_2",
    "Flow_Type_2"
)

_FBL3N_HEADER = (
    "Fiscal_Year",
    "Period",
    "GL_Account",
    "Assignment",
    "Document_Number",
    "Business_Area",
    "Document_Type",
    "Document_Date",
    "Posting_Date",
    "Posting_Key",
    "LC_Amount",
    "Tax_Code",
    "Clearing_Document",
    "Text"
)

_ZSD25_HEADER = (
    "Agreement",
    "Rebate_Recipient",
    "Name",
    "City",
    "Country",
    "Condition_Type",
    "Variable_Key",
    "Condition_Rate",
    "Condition_Based_Value",
    "Status",
    "Description_Of_Agreement",
    "Agreement_Type_Code",
    "Category_A",
    "Category_B",
    "Condition_Value",
    "Accruals",
    "Accruals_Reversed",
    "Payments",
    "Open_Value",
    "Open_Accruals",
    "Currency",
    "Arrangement_Calendar",
    "Settlement_Periods",
    "Agreement_Type_Name",
    "Valid_From",
    "Valid_To",
    "Sales_Office_Code",
    "Sales_Office_Name",
    "Sales_Group_Code",
    "Sales_Group_Name",
    "Payer",
    "Customer_Hierarchy_01",
    "Customer_Hierarchy_02",
    "Customer_Hierarchy_03",
    "Customer_Hierarchy_04",
    "Customer_Hierarchy_05",
    "Agreement_Status",
    "Sales_Organization",
    "Customer_Group",
    "Deletion_Indicator",
    "Scales",
    "Texts"
)

_FS10N_HEADER = (
    "Period",
    "Debit",
    "Credit",
    "Balance",
    "Cummulative_Balance"
)

def clear():
    """
    Clears data stored in the accumulator.

    Params:
    -------
    None.

    Returns:
    --------
    None.
    """

    for val in _accum.values():
        val.clear()

    return

def store_to_accum(data: Union[None, DataFrame], country: str, key: str, acc: Union[str, int] = None):
    """
    Stores data to the processor accumulator.

    The accumulator is a nested dict organized into a tree-like hierarchy (top to bottom): \n
        Country -> Data Descriptor [-> GL Account] -> Data. \n
    If GL account is None, then data descriptor (key) represents the lowest level \n
    in the storage hierarchy.

    Params:
    -------
    data:
        Data to store. \n
        If None is used (default value), then None
        is stored into the accumulator.

    country:
        Name of country under which the data is stored in the accumulator.

    key:
        Data descriptor under which the data is stored in accumulator.

    acc:
        GL account under which the data is stored in accumulator. \n
        If None is used (default value) then the GL account will not be \n
        considered when storig data to the accumlator. If a GL account \n
        is used, then the data will be stored under the given account.

    Returns:
    --------
    None.
    """

    global _accum

    if acc is not None and not str(acc).isnumeric():
        raise ValueError(f"The account used has incorrect value: {acc}")

    if country in _accum[key] and acc is None:
        raise RuntimeError("Cannot modify data that is already stored in the accumulator!")

    if country in _accum[key] and acc is not None:
        if acc in _accum[key][country]:
            raise RuntimeError("Cannot modify data that is already stored in the accumulator!")

    if acc is None:
        _accum[key][country] = data
    else:
        if country not in _accum[key]:
            _accum[key][country] = {}

        _accum[key][country][acc] = data

    return

def get_from_accum(country: str, key: str, acc: Union[str, int] = None) -> DataFrame:
    """
    Fetches data stored in the processor accumulator.

    The accumulator is a nested dict organized into a tree-like hierarchy (top to bottom): \n
        Country -> Data Descriptor [-> GL Account] -> Data. \n
    If GL account is None, then data descriptor (key) represents the lowest level \n
    in the storage hierarchy.

    Params:
    -------
    country:
        Name of country under which the data is stored in accumulator.

    key:
        Data descriptor under which the data is stored in accumulator.

    acc:
        GL account under which the data is stored in accumulator. \n
        If None is used (default value) then GL account will not be \n
        considered when retrieving data from the accumlator. \n
        If a valid account value is passed (8-digit string), then the data \n
        stored under that given account will be retrieved.

    Returns:
    ---------
    A DataFrame object representig the stored data.
    """

    if acc is not None and not str(acc).isnumeric():
        raise ValueError(f"The account used has incorrect value: {acc}")

    if acc is None:
        data = _accum[key][country]
    else:
        data = _accum[key][country][acc]

    return data

def parse_amount(num: str, ndigits: int = 2) -> float:
    """
    Parses a string amount formatted
    as the standard SAP numeric format
    into a rounded float literal.
    """

    sign = "-"
    val = num

    if val.startswith("-"):
        val = val.lstrip("-")
    elif val.endswith("-"):
        val = val.rstrip("-")
    else:
        sign = ""

    tokens = re.split(r"\D", val)

    if len(tokens) == 1:
        val = tokens[0]
    else:
        # concat tokens
        val = "".join(tokens[:-1])
        val = sign + val + "." + tokens[-1]

    conv = pd.to_numeric(val)
    rounded = round(conv, ndigits)

    return float(rounded)

def _parse_amounts(vals: Series) -> Series:
    """
    Parses string amounts formatted
    as the standard SAP numeric format
    into rounded float literals.
    """

    replaced = vals.str.replace(".", "", regex = False).str.replace(",", ".", regex = False)
    replaced = replaced.mask(replaced.str.endswith("-"), "-" + replaced.str.rstrip("-"))
    converted = pd.to_numeric(replaced).astype("float64")

    return converted

def _parse_dates(vals: Series) -> Series:
    """
    Parses string dates formatted
    in the standard SAP date format
    into a series of datetime.date
    objects.
    """

    parsed = pd.to_datetime(vals, dayfirst = True, errors = "coerce").dt.date

    return parsed

def _parse_data(data: DataFrame) -> DataFrame:
    """
    Parses records stored in a DataFrame
    object and casts the resulting fields
    to appropriate data types.
    """

    # remove leading and trailing spaces from string fields
    # and replace empty strings with nan where appropriate
    for col in data.columns:
        if infer_dtype(data[col]) == "string":
            data[col] = data[col].str.strip()

    # replace empty strings with null indicating either unused or missing
    # (possibly as a result of a mistake during item posting) field values
    data.loc[(data["Text"] == ""), "Text"] = pd.NA                   # missing val
    data.loc[(data["Assignment"] == ""), "Assignment"] = pd.NA       # missing val
    data.loc[(data["Tax_Code"] == ""), "Tax_Code"] = pd.NA           # unused val
    data.loc[(data["Business_Area"] == ""), "Business_Area"] = pd.NA # unused val

    # open items have no clearing document, coerce filling empty recs with
    # null, otherwise convert to int where clearing document exsits
    data["Clearing_Document"] = pd.to_numeric(
        data["Clearing_Document"],
        errors = "coerce"
    ).astype("Int64")

    # pre-convert data fields to appropriate data types
    data["Fiscal_Year"] = data["Fiscal_Year"].astype("UInt16")
    data["GL_Account"] = data["GL_Account"].astype("UInt32")
    data["Period"] = data["Period"].astype("UInt8")
    data["Document_Number"] = data["Document_Number"].astype("Int64")
    data["LC_Amount"] = _parse_amounts(data["LC_Amount"])
    data["Document_Date"] = _parse_dates(data["Document_Date"])
    data["Posting_Date"] = _parse_dates(data["Posting_Date"])
    data["Posting_Key"] = pd.to_numeric(data["Posting_Key"])

    # extract accounting params separated by a semicolon
    # from 'Text' field into separate fields
    data["Tokens"] = data["Text"].str.split(";")

    data = data.assign(
        Condition = pd.NA,
        Category = pd.NA,
        Customer = pd.NA,
        Agreement = pd.NA,
        Note = pd.NA
    )

    # get only token records containig at least 4 values
    idx = data[data["Tokens"].str.len() >= 4].index

    data.loc[idx, "Condition"] = data.loc[idx, "Tokens"].str.get(0)
    data.loc[idx, "Category"] = data.loc[idx, "Tokens"].str.get(1)
    data.loc[idx, "Customer"] = data.loc[idx, "Tokens"].str.get(2)
    data.loc[idx, "Agreement"] = data.loc[idx, "Tokens"].str.get(3)
    data.loc[idx, "Note"] = data.loc[idx, "Tokens"].str.get(4) # returns none if token has no such index

    data.drop("Tokens", axis = 1, inplace = True) # remove splitted text tokens from data

    cond_idx = data[data["Condition"].notna()].index
    categ_idx = data[data["Category"].notna()].index
    note_idx = data[data["Note"].notna()].index

    if not cond_idx.empty:
        data.loc[cond_idx, "Condition"] = data.loc[cond_idx, "Condition"].str.strip()

    if not categ_idx.empty:
        data.loc[categ_idx, "Category"] = data.loc[categ_idx, "Category"].str.strip()

    if not note_idx.empty:
        data.loc[note_idx, "Note"] = data.loc[note_idx, "Note"].str.strip()

    incorr_cond_qry = data.query("Condition.notna() and Condition.str.len() != 4")
    incorr_categ_qry = data.query("Category.notna() and Category.str.len() != 2")

    # replace incorrect extracted entries with nan
    data.loc[incorr_cond_qry.index, "Condition"] = pd.NA
    data.loc[incorr_categ_qry.index, "Category"] = pd.NA

    # convert the extracted data to appropriate data types
    data["Condition"] = data["Condition"].astype("category")
    data["Category"] = data["Category"].astype("category")
    data["Note"] = data["Note"].astype("string")

    data["Customer"] = pd.to_numeric(data["Customer"], errors = "coerce").astype("UInt32")
    data["Agreement"] = pd.to_numeric(data["Agreement"], errors = "coerce").astype("UInt32")

    return data

def read_binary_file(file_path: str) -> DataFrame:
    """
    Reads the content of a binary file
    stored in .feather format.

    Params:
    -------
    file_path:
        Path to the file to read.

    Returns:
    --------
    A DataFrame object containing the file data.
    """

    _logger.debug(f"Reading file: '{file_path}'")

    if not file_path.endswith(".feather"):
        raise ValueError(f"Unsupported file type used: {file_path}")

    data = pd.read_feather(file_path, use_threads = True)

    if "fs10n" in file_path:
        data["Cummulative_Balance"] = data["Cummulative_Balance"].apply(lambda x: pd.NA if x is None else x)
        data["Balance"] = data["Balance"].apply(lambda x: pd.NA if x is None else x)

    return data

def read_textual_file(file_path: str) -> str:
    """
    Reads the content of a textual file
    stored in .txt or .dat format.
    """

    _logger.debug(f"Reading file: '{file_path}'")

    if not file_path.endswith((".dat", ".txt")):
        raise ValueError(f"Unsupported file format: {file_path}")

    with open(file_path, 'r', encoding = "utf-8") as stream:
        txt = stream.read()

    return txt

def _clean_text(txt: str, patt: str) -> str:
    """
    Removes irrelevant lines from file text.
    """

    # get all data lines containing accounting items
    matches = re.findall(patt, txt, re.M)
    replaced = list(map(lambda x: x[1:-1].strip(), matches))
    preproc = "\n".join(replaced)

    return preproc

def _parse_fbl3n_data(text: str, file_type: FileTypes, header: list,
                      multiproc: bool) -> DataFrame:
    """
    Parses a cleaned FBL5N text.
    """

    WORKER_COUNT = 5
    MAX_ROWS_SNG = 1000
    HEADER_ROW_IDX = 0
    buff = StringIO(text)

    if file_type == FileTypes.DAT:
        data = pd.read_csv(buff,
            sep = '\t', engine = "pyarrow",
            header = HEADER_ROW_IDX, dtype = {
                "Assignment": "string",
                "Text": "string",
                "Tx": "string",
                "BusA": "string"
            }
        )

        data.drop("Crcy", axis = 1, inplace = True)

        data.columns = header

    elif file_type == FileTypes.TXT:
        data = pd.read_csv(buff, names = header,
            sep = '|', engine = "pyarrow", dtype = {
                "Assignment": "string",
                "Text": "string",
                "Tax_Code": "string",
                "Business_Area": "string"
            }
        )

    if data.shape[0] <= MAX_ROWS_SNG or not multiproc:
        _logger.debug("Parsing data ...")
        try:
            parsed = _parse_data(data)
        except Exception as exc:
            _logger.exception(exc)
            return None
    else:

        # split data into smaller manageable chunks
        _logger.debug(f"Splitting data into {WORKER_COUNT} chunks ...")
        data_chunks = np.array_split(data, WORKER_COUNT)

        # init pool of workers and let them process the data chunks
        _logger.debug(f"Creating {WORKER_COUNT} parsing workers ...")
        with Pool(WORKER_COUNT) as data_pool:
            _logger.debug("Parsing data ...")
            parsed = data_pool.map(_parse_data, data_chunks)

        # combine the data parts returned by workers
        _logger.debug("Concatenating data chunks ...")
        parsed = pd.concat(parsed)

    return parsed

@deprecated("Use a memory-optimized version convert_fbl3n_data_opt() instead.")
def convert_fbl3n_data(file_path: str, multiproc: bool = True) -> DataFrame:
    """
    Converts data exported form FBL5N into a DataFrame object.

    Params:
    -------
    file_path:
        Path to the file containing exported FBL3N data.

    multiproc:
        Indicates whether multiprocessing should be used to convert data.

    Returns:
    --------
    A DataFrame object. The result of data conversion.
    """

    _logger.info("Converting FBL3N data ...")

    content = read_textual_file(file_path)

    if file_path.endswith(".txt"):
        file_type = FileTypes.TXT
        text = _clean_text(content, patt = r"^\|\s+\d{4}\|.*$")
    elif file_path.endswith(".dat"):
        file_type = FileTypes.DAT
        text = content

    try:
        parsed = _parse_fbl3n_data(text, file_type, list(_FBL3N_HEADER), multiproc)
    except Exception as exc:
        _logger.exception(exc)
        return None

    del text

    if parsed.empty:
        return None

    # text might contain floats as a result of reading
    # files without explicitly specified dtypes
    assert infer_dtype(parsed["Text"]) == "string"
    assert infer_dtype(parsed["Assignment"]) == "string"
    assert infer_dtype(parsed["Tax_Code"]) == "string"
    assert infer_dtype(parsed["Business_Area"]) == "string"

    # check & reset data index
    if parsed.index.has_duplicates:
        parsed.reset_index(inplace = True, drop = True)

    categorical = (
        "Document_Type",
        "Condition",
        "Category",
        "Posting_Key",
        "Assignment",
        "Business_Area",
        "Tax_Code"
    )

    # finally, convert the respective data fields to (or back to)
    # categories. This is needed particularly following data concatenation
    _logger.debug(f"Converting the following data fields to categorical: {'; '.join(categorical)}'")
    for col in categorical:
        parsed[col] = parsed[col].astype("category")

    # validate extracted categories by comparing
    # the values with teh list of official categs
    categs = (
        "B1", "B2", "B3", "B4",
        "B5", "B6", "B7", "B8",
        "BO", "C1", "C2", "C3",
        "D1", "DS", "E1", "EM",
        "FE", "FS", "S1", "SE",
        "YJ", "EU", "GR"
    )

    if not parsed["Category"].cat.categories.isin(categs).all():
        undef_cats = set(parsed['Category'].cat.categories) - set(categs)
        _logger.warning(f"Undefined categories found: {', '.join(undef_cats)}",)

    # ensure all amounts with pstkey = 50 are negative
    # since .dat files store LC amounts as absolute vals
    mask = ((parsed["Posting_Key"] == 50) & (parsed["LC_Amount"] > 0))
    parsed.loc[mask, "LC_Amount"] = -1 * parsed.loc[mask, "LC_Amount"]

    # resetting index will remove any duplicated index vals
    # as a result of data concatenation
    _logger.debug("Resetting data index ...")
    parsed.reset_index(inplace = True, drop = True)

    _logger.debug("Checking data field types ...")
    assert parsed["Fiscal_Year"].dtype == "UInt16"
    assert parsed["GL_Account"].dtype == "UInt32"
    assert parsed["Customer"].dtype == "UInt32"
    assert parsed["Agreement"].dtype == "UInt32"
    assert parsed["Period"].dtype == "UInt8"
    assert parsed["Document_Number"].dtype == "Int64"
    assert parsed["Clearing_Document"].dtype == "Int64"
    assert parsed["LC_Amount"].dtype == "float64"
    assert parsed["Text"].dtype == "string"
    assert parsed["Assignment"].dtype == "category"
    assert parsed["Business_Area"].dtype == "category"
    assert parsed["Document_Type"].dtype == "category"
    assert parsed["Tax_Code"].dtype == "category"
    assert parsed["Condition"].dtype == "category"
    assert parsed["Category"].dtype == "category"
    assert parsed["Posting_Key"].dtype == "category"
    assert parsed["Document_Date"].dtype in ("object", "datetime64[ns]")
    assert parsed["Posting_Date"].dtype in ("object", "datetime64[ns]")
    _logger.debug("Passed.")

    _logger.debug("Checking data integrity ...")
    assert parsed["Document_Type"].notna().all()
    assert parsed["Posting_Key"].notna().all()
    _logger.debug("Passed.")

    return parsed

def convert_se16_kote(file_path: str) -> DataFrame:
    """
    Converts data exported from KOTE890
    table into a DataFrame object.

    Params:
    -------sss
    file_path:
        Path to the file containing the exported data.

    Returns:
    --------
    A DataFrame object. The result of data conversion.
    """

    _logger.info("Converting 'KOTE890' data ...")

    raw_txt = read_textual_file(file_path)
    prep_txt = _clean_text(raw_txt, patt = r"^\|\s*\d{3}\|.*\|$")

    parsed = pd.read_csv(StringIO(prep_txt),
        sep = '|',
        names = _SE16_KOTE_HEADER,
        keep_default_na = False,
        dtype = {
            "Agreement": "UInt32",
            "Condition_Record_Number": "UInt32",
            "Client": "string",
            "Application": "string",
            "Condition_Type": "string",
            "Sales_Organization": "string",
            "Sales_Office": "string",
            "Customer": "string"
        }
    )

    # if parsed correctly, the resulting data will not be empty
    assert not parsed.empty, "Parsing failed!"

    categorical = (
        "Client",
        "Customer",
        "Application",
        "Sales_Office",
        "Condition_Type",
        "Sales_Organization"
    )

    # remove leading and trailing whitespaces from the string data
    for col in categorical:
        parsed[col] = parsed[col].str.strip()
        parsed[col] = parsed[col].astype("category")

    # parse date fields
    parsed["Valid_To"] = _parse_dates(parsed["Valid_To"])
    parsed["Valid_From"] = _parse_dates(parsed["Valid_From"])

    return parsed

def convert_se16_kona(file_path: str) -> DataFrame:
    """
    Converts data exported from KONA
    table into a DataFrame object.

    Params:
    -------
    file_path:
        Path to the file containing the exported data.

    Returns:
    --------
    A DataFrame object. The result of data conversion.
    """

    _logger.info("Converting 'KONA' data ...")

    cols_to_use = [
        "Client",
        "Agreement",
        "Sales_Organization",
        "Distribution_Channel",
        "Division",
        "Sales_Office",
        "Sales_Group",
        "Agreement_Type",
        "Agreement_Category",
        "Application",
        "Created_By",
        "Created_On",
        "Time",
        "Changed_By",
        "Changed_On",
        "Time_Of_Change",
        "Rebate_Recipient",
        "Currency",
        "Maximum_Rebate",
        "Category",
        "Agreement_Status",
        "Valid_From",
        "Valid_To",
        "Condition_Type_Group",
        "Description_Of_Agreement",
        "Addition_Value_Days",
        "Arrangemet_Calendar",
        "Company_Code",
        "Predecessor",
        "Settlement_Periods"
    ]

    raw_txt = read_textual_file(file_path)

    prep_txt = _clean_text(raw_txt, patt = r"^\|\s*\d{3}\|.*\|$")
    prep_txt = prep_txt.replace("CARAT-Direktbonus|Hengstenberg GmbH", "CARAT-Direktbonus/Hengstenberg GmbH")
    prep_txt = prep_txt.replace("Umsatzziel- oder Wachstumsbonus|LEJ GmbH", "Umsatzziel- oder Wachstumsbonus/LEJ GmbH")

    # parse the text data
    parsed = pd.read_csv(StringIO(prep_txt),
        sep = '|',
        names = _SE16_KONA_HEADER,
        dtype = "string",
        keep_default_na = False
    )

    parsed = parsed[cols_to_use]

    # if parsed correctly, the resulting data will not be empty
    assert not parsed.empty, "Parsing failed!"

    # remove leading and trailing whitespaces from the string data
    for col in parsed.columns:
        parsed[col] = parsed[col].str.strip()

    # parse and convert date fields
    # skip parsing of the 'Rebate_Recipient' field as some vals (Germany) are not numeric
    parsed["Valid_To"] = _parse_dates(parsed["Valid_To"])
    parsed["Valid_From"] = _parse_dates(parsed["Valid_From"])
    parsed["Created_On"] = _parse_dates(parsed["Created_On"])
    parsed["Changed_On"] = _parse_dates(parsed["Changed_On"])
    parsed["Agreement"] = parsed["Agreement"].astype("UInt32")
    parsed["Addition_Value_Days"] = pd.to_numeric(parsed["Addition_Value_Days"]).astype("UInt32")
    parsed["Predecessor"] = pd.to_numeric(parsed["Predecessor"]).astype("UInt32")
    parsed["Maximum_Rebate"] = _parse_amounts(parsed["Maximum_Rebate"])

    categ_cols = (
        "Client",
        "Sales_Organization",
        "Distribution_Channel",
        "Division",
        "Sales_Office",
        "Agreement_Type",
        "Agreement_Category",
        "Application",
        "Created_By",
        "Changed_By",
        "Currency",
        "Category",
        "Agreement_Status",
        "Condition_Type_Group",
        "Arrangemet_Calendar",
        "Company_Code",
        "Settlement_Periods"
    )

    for col in categ_cols:
        parsed[col] = parsed[col].astype("category")

    return parsed

def convert_zsd25_loc_data(file_path: str) -> tuple:
    """
    Converts local entity bonus data exported
    from ZSD25_T125 into a DataFrame object.

    Params:
    -------
    file_path:
        Path to the file containing the exported data.

    Returns:
    --------
    A DataFrame object. The result of data conversion.
    """

    _logger.info("Converting ZSD25 local entity bonus data ...")

    raw_txt = read_textual_file(file_path)
    prep_txt = _clean_text(raw_txt, patt = r"^\|\s?\d{8}\s?\|.*\|$")

    parsed = pd.read_csv(
        StringIO(prep_txt),
        sep = '|',
        names = _ZSD25_HEADER,
        dtype = "string",
        keep_default_na = False
    )

    for col in parsed.columns:
        parsed[col] = parsed[col].str.strip()

    parsed.loc[(parsed["Name"] == ""), "Name"] = pd.NA
    parsed["Agreement"] = pd.to_numeric(parsed["Agreement"])
    parsed["Status"] = parsed["Status"].astype("category")
    parsed["Agreement_Type_Code"] = parsed["Agreement_Type_Code"].astype("category")
    parsed["Payments"] = _parse_amounts(parsed["Payments"])
    parsed["Open_Accruals"] = _parse_amounts(parsed["Open_Accruals"])
    parsed["Accruals_Reversed"] = _parse_amounts(parsed["Accruals_Reversed"])
    parsed["Accruals"] = _parse_amounts(parsed["Accruals"])
    parsed["Open_Value"] = _parse_amounts(parsed["Open_Value"])
    parsed["Condition_Based_Value"] = _parse_amounts(parsed["Condition_Based_Value"])
    parsed["Condition_Value"] = _parse_amounts(parsed["Condition_Value"])
    parsed["Valid_From"] = _parse_dates(parsed["Valid_From"])
    parsed["Valid_To"] = _parse_dates(parsed["Valid_To"])

    # Germany: some vals are non-numeric, these will be converted separately
    if parsed["Rebate_Recipient"].str.isnumeric().all():
        parsed["Rebate_Recipient"] = pd.to_numeric(parsed["Rebate_Recipient"]).astype("UInt32")

    # clean and parse 'Condition rate' column before any further manipulation
    cleaned_cond_rate = parsed["Condition_Rate"].str.replace(r"\s+\%", "", regex = True)
    parsed["Condition_Rate"] = _parse_amounts(cleaned_cond_rate)

    parsed_conds = parsed.copy()

    # save data subset containing agreement number (key) and Condition rate (value)
    # for later joining with the data from which redundant rows were removed
    mask = parsed["Condition_Rate"].notna()
    agree_to_cond_rate = parsed[mask][["Agreement", "Condition_Rate"]].drop_duplicates()

    # replace empty strings with NA in column 'Country'
    # to facilitate identifying redundant rows to drop
    dropped = parsed.drop(index = parsed[parsed["Country"] == ""].index)
    dropped = dropped.drop("Condition_Rate", axis = 1)

    # join the 'Conditional rate' values from the saved subset
    # with the cleaned dataset based on 'Agreement' numbers
    joined = dropped.merge(agree_to_cond_rate, on = "Agreement", how = "left")

    # reorder fields so that the 'Condition rate'
    # appears at the same place as in head quarter data
    col_idx = joined.columns.get_loc("Condition_Based_Value")
    cond_rate = joined.pop("Condition_Rate")
    joined.insert(col_idx, "Condition_Rate", cond_rate)

    return (joined, parsed_conds)

def convert_zsd25_glob_data(file_path: str, sales_org: str) -> DataFrame:
    """
    Converts head quarter bonus data exported
    from ZSD25_T125 into a DataFrame object.

    Params:
    -------
    file_path:
        Path to the file containing the exported data.

    sales_org:
        Number of the local sales organization.

    Returns:
    --------
    A DataFrame object. The result of data conversion.
    """

    allowed_le_sorgs = (
        "0075", "0076", "0073", "0072",
        "0074", "0057", "0063", "2051",
        "2053", "2054", "0078", "0059",
        "0067", "0052", "0010", "0001"
    )

    if not sales_org in allowed_le_sorgs:
        raise ValueError(f"The sales organization code is incorrect: {sales_org}")

    _logger.info("Converting ZSD25 head quarter bonus data ...")

    raw_txt = read_textual_file(file_path)
    prep_txt = _clean_text(raw_txt, patt = r"^\|\s?\d{8}\s?\|.*\|$")

    parsed = pd.read_csv(StringIO(prep_txt),
        sep = "|",
        names = _ZSD25_HEADER,
        dtype = "string",
        keep_default_na = False
    )

    assert not parsed.empty, "Parsing failed!"

    for col in parsed.columns:
        parsed[col] = parsed[col].str.strip()

    parsed["Agreement"] = pd.to_numeric(parsed["Agreement"]).astype("UInt32")
    parsed["Status"] = parsed["Status"].astype("category")
    parsed["Agreement_Type_Code"] = parsed["Agreement_Type_Code"].astype("category")
    parsed["Condition_Based_Value"] = _parse_amounts(parsed["Condition_Based_Value"])
    parsed["Payments"] = _parse_amounts(parsed["Payments"])
    parsed["Open_Accruals"] = _parse_amounts(parsed["Open_Accruals"])
    parsed["Accruals_Reversed"] = _parse_amounts(parsed["Accruals_Reversed"])
    parsed["Accruals"] = _parse_amounts(parsed["Accruals"])
    parsed["Open_Value"] = _parse_amounts(parsed["Open_Value"])
    parsed["Condition_Value"] = _parse_amounts(parsed["Condition_Value"])
    parsed["Valid_From"] = _parse_dates(parsed["Valid_From"])
    parsed["Valid_To"] = _parse_dates(parsed["Valid_To"])

    # Germany: some vals are non-numeric, these will be converted separately
    if parsed["Rebate_Recipient"].str.isnumeric().all():
        parsed["Rebate_Recipient"] = pd.to_numeric(parsed["Rebate_Recipient"]).astype("UInt32")

    # clean and parse 'Condition rate' column before any further manipulation
    cleaned_cond_rate = parsed["Condition_Rate"].str.replace(r"\s+\%", "", regex = True)
    parsed["Condition_Rate"] = _parse_amounts(cleaned_cond_rate)

    # remove rows with empty valid data or open accruals
    cleaned = parsed.drop(index = parsed.query("Open_Accruals.isna()").index)

    # reset the messy data indices resulting from agreement removal
    cleaned.reset_index(inplace = True, drop = True)

    # indicate local sales organization where text is missing
    mask = (cleaned["Variable_Key"] == "")
    cleaned.loc[mask, "Variable_Key"] = f"For {sales_org}"

    return cleaned

def convert_fs10n_data(file_path: str) -> DataFrame:
    """
    Converts data expoted from FS10N into a DataFrame object.

    Params:
    -------
    file_path:
        Path to the file containing exported FS10N data.

    Returns:
    --------
    A DataFrame object. The result of data conversion.
    """

    raw_txt = read_textual_file(file_path)
    _logger.debug(f"FS10N raw data:\n{raw_txt}")
    prep_txt = _clean_text(raw_txt, r"\|[\d,T].*\|")

    # parse the text data
    parsed = pd.read_csv(StringIO(prep_txt),
        sep = "|",
        names = _FS10N_HEADER,
        dtype = "string",
        keep_default_na = False
    )

    for col in parsed.columns:
        parsed[col] = parsed[col].str.strip()

    parsed["Debit"] = _parse_amounts(parsed["Debit"])
    parsed["Credit"] = _parse_amounts(parsed["Credit"])
    parsed["Balance"] = _parse_amounts(parsed["Balance"])
    parsed["Cummulative_Balance"] = _parse_amounts(parsed["Cummulative_Balance"])

    # first and last data rows represent non-relevant data
    parsed.drop(index = parsed.index.max(), inplace = True)

    parsed["Period"] = np.arange(1, parsed.shape[0] + 1)
    parsed.set_index("Period", inplace=True)

    for col in parsed:
        parsed[col].fillna(pd.NA, inplace = True)

    return parsed

def create_period_overview(yearly_data: DataFrame) -> DataFrame:
    """
    Generates a static pivot table summarizing yearly
    accounting data. The tabl will be placed onto the
    'Period overview' worksheet of the report.

    Params:
    -------
    yearly_data:
        A DataFrame object containing accounting
        data summarized by fiscal years.

    Returns:
    --------

    """

    # create pivot table
    pivotted = pd.pivot_table(
        yearly_data,
        values = "LC_Amount",
        index = ["Fiscal_Year", "Period"],
        columns = "GL_Account",
        aggfunc = np.sum,
        margins = True,
        margins_name = "Grand Total",
        dropna = True
    )

    for col in pivotted.columns:
        pivotted[col] = pivotted[col].astype("float64")

    return pivotted

def calculate_le_bonus_data(txt_summs: DataFrame, le_data: DataFrame, loc_curr: str, ex_rate: float) -> DataFrame:
    """
    Generates a local entity bonus data table that will be placed
    onto the 'Local Entity Bonuses' worksheet of the user report.

    Params:
    -------
    txt_summs:
        A DataFrame object containing accounting
        data summarizd by 'Text' field values.

    le_data:
        Converted local entity bonus data.

    loc_curr:
        Local currency.

    ex_rate:
        Exchange rate from EUR to a local currency.

    Returns:
    --------
    A DataFrame object containing the calculated local entity bonus data.
    """

    _logger.info("Performing data calculations for local entity bonuses ...")

    data = le_data.copy()

    data = data[[
        "Rebate_Recipient",
        "Name",
        "Country",
        "Agreement_Type_Code",
        "Agreement",
        "Status",
        "Description_Of_Agreement",
        "Condition_Value",
        "Payments",
        "Open_Accruals",
        "Currency",
        "Arrangement_Calendar",
        "Valid_From",
        "Valid_To"
    ]]

    deduped = data.drop_duplicates(subset = "Agreement")
    deduped.reset_index(inplace = True, drop = True)

    # calculate currency corrections
    subset = deduped.assign(
        Corr_to_LC = 0.0,
        LC_Open_Accr = pd.NA,
        Difference = 0.0
    )

    if ex_rate != 1.0:
        mask = (subset["Currency"] != loc_curr)
        subset.loc[mask, "Corr_to_LC"] = subset.loc[mask, "Open_Accruals"] * ex_rate - subset.loc[mask, "Open_Accruals"]

    subset["LC_Open_Accr"] = subset["Open_Accruals"] + subset["Corr_to_LC"]

    for acc in txt_summs.keys():

        txt_summ = txt_summs[acc].copy()
        txt_summ = txt_summ[["Agreement", "LC_Amount_Sum"]]
        summed = txt_summ.groupby("Agreement").sum().reset_index()

        subset = subset.merge(summed,
            left_on = "Agreement",
            right_on = "Agreement",
            how = "left",
        )

        subset.rename({"LC_Amount_Sum": acc}, inplace = True, axis = 1)
        subset[acc] = subset[acc].fillna(0)
        subset["Difference"] = subset["Difference"] + subset[acc]

    subset["Difference"] = subset["Difference"] - subset["LC_Open_Accr"]
    subset["Difference"] = subset["Difference"].round(2)

    return subset

def calculate_hq_bonus_data(txt_summs: dict, hq_data: DataFrame, loc_curr: str, ex_rate: float) -> DataFrame:
    """
    Generates a head quarter bonus data table that will be placed onto the 'Head Quarter Bonuses'
    worksheet of the user report.

    Params:
    -------
    txt_summs:
        A dict containing DataFrame ojects with
        accounting data summarizd by 'Text' field values.

    hq_data:
        Converted head quarer bonus data.

    loc_curr:
        Local currency.

    ex_rate:
        Exchange rate from EUR to a local currency.

    Returns:
    --------
    A DataFrame object containing the calculated head quarter bonus data.
    """

    _logger.info("Performing data calculations for head quarter bonuses ...")

    subset = hq_data.copy()

    subset = subset[[
        "Rebate_Recipient",
        "Name",
        "Country",
        "Agreement_Type_Code",
        "Agreement",
        "Status",
        "Description_Of_Agreement",
        "Condition_Based_Value",
        "Payments",
        "Open_Accruals",
        "Currency",
        "Arrangement_Calendar",
        "Valid_From",
        "Valid_To"
    ]]

    # prep data for processing
    name_mask = (subset["Name"] != "")
    subset.loc[name_mask, "Condition_Based_Value"] = 0.0
    subset.loc[name_mask, "Payments"] = 0.0
    subset.loc[name_mask, "Open_Accruals"] = 0.0

    # calculate currency corrections
    subset = subset.assign(
        Corr_to_LC = 0.0,
        Difference = 0.0
    )

    accr_sums = subset[["Agreement", "Open_Accruals"]].copy()
    accr_sums = accr_sums.groupby("Agreement").sum()
    accr_sums.rename({"Open_Accruals": "LC_Open_Accr"}, axis = 1, inplace = True)

    subset = subset.merge(accr_sums, on = "Agreement")
    calc_mask = ~subset["Agreement"].duplicated()
    gl_accs = list(txt_summs.keys())

    if ex_rate != 1.0:
        # this should not be applicable for countries where local currency is other than EUR
        _logger.warning(f"The local currency is {loc_curr}, an exchange rate {ex_rate} will be used for calculations!")
        mask = ((subset["Currency"] != loc_curr) & calc_mask)
        subset.loc[mask, "Corr_to_LC"] = subset.loc[mask, "Open_Accruals"] * ex_rate - subset.loc[mask, "Open_Accruals"]

    subset.loc[calc_mask, "LC_Open_Accr"] = subset.loc[calc_mask, "LC_Open_Accr"] + subset.loc[calc_mask, "Corr_to_LC"]

    for acc in gl_accs:

        txt_summ = txt_summs[acc].copy()
        txt_summ = txt_summ[["Agreement", "LC_Amount_Sum"]]
        summed = txt_summ.groupby("Agreement").sum().reset_index()

        subset = subset.merge(
            summed,
            left_on = "Agreement",
            right_on = "Agreement",
            how = "left"
        )

        subset.rename({"LC_Amount_Sum": acc}, inplace = True, axis = 1)
        subset[acc] = subset[acc].fillna(0) # inplace = True) inplace nefunguje
        subset.loc[calc_mask, "Difference"] = subset.loc[calc_mask, "Difference"] + subset.loc[calc_mask, acc]

    subset.loc[calc_mask, "Difference"] = subset.loc[calc_mask, "Difference"] - subset.loc[calc_mask, "LC_Open_Accr"]
    subset.loc[calc_mask, "Difference"] = subset.loc[calc_mask, "Difference"].round(2)

    # clean up data
    subset.loc[~calc_mask, ["LC_Open_Accr", "Difference"] + gl_accs] = pd.NA

    return subset

def check_agreement_states(txt_summs: dict, le_bon: DataFrame, hq_bon: DataFrame) -> dict:
    """
    For each GL account reconciled, validates item text format and, where agreement
    number is found in item text, the agreement status is checked as well. If item
    text format is not valid, then 'x' flag is written into the 'Status' field. If a
    closed agreement is found in items with non-zero amount balance, then 'CHECK' flag
    is written into the 'Status' field.

    Params:
    -------
    txt_summs:
        A dict of GL accounts (keys) mapped to their
        'Amount' subtotals summed on item texts (values).

    le_bon:
        Local entity bonus data exported from ZSD25.

    hq_bon:
        Head quarter bonus data exported from ZSD25.

    Returns:
    --------
    A dict of GL accounts mapped to the subtotals with 'Status' field.
    """

    _logger.info("Checking open agreement states ...")

    checked = {}

    for acc in txt_summs.keys():

        data = txt_summs[acc].assign(
            Status = "",
        )

        # mark non-zero amount items where identification
        # params are missing due to incorrect textual format
        qry = data.query(
            "(Condition.isna() or Category.isna() "
            "or Customer.isna() or Agreement.isna()) "
            "and LC_Amount_Sum != 0"
        )

        data.loc[qry.index, "Status"] = Marks.INVALD_TEXT_FMT.value

        # identify agreements closed in ZSD25 but still open on GL accounts
        open_agreements = list(le_bon["Agreement"].unique())

        if hq_bon is not None:
            open_agreements += list(hq_bon["Agreement"].unique())

        qry = data.query(f"LC_Amount_Sum != 0 and Agreement not in {open_agreements} and Status != '{Marks.INVALD_TEXT_FMT.value}'")
        data.loc[qry.index, "Status"] = Marks.AGREEMENT_CLOSED.value
        sorted_data = data.sort_values("Status", ascending = False)
        sorted_data.reset_index(inplace = True)

        checked[acc] = sorted_data

    return checked

def summarize(txt_summs: dict, le_calcs: DataFrame, hq_calcs: DataFrame, gl_data: dict, accs: list, period: int) -> DataFrame:
    """
    Summarizes general ledger and subledger data into a table that will be placed onto 'Summary' sheet of the user report.

    Params:
    -------
    txt_summs:
        A dict of GL accounts mapped to their 'Amount' subtotals summed on item texts.

    le_calcs:
        Calculated local entity bonus data.

    hq_calcs:
        Calculated head quarter bonus data.

    gl_data:
        A dict of GL accounts mapped to their 'Amount' cummulative balance exported from FS10N.

    accs:
        List of reconciled GL accounts.

    period:
        Fiscal period for which reconciliation is performed.

    Returns:
    --------
    A DataFrame object contianing summarized bonus data.
    """

    assert 1 <= period <= 15, "Argument 'period' has incorrect value!"

    _logger.info("Summarizing general ledger and subledger bonus data ...")

    gl_accs = list(map(str, accs))
    fs10n = gl_data.copy()
    fields = {}

    for acc in gl_accs:
        fields[acc] = pd.NA

    fields.update({"Difference": pd.NA})

    data = DataFrame(fields,
        index = [
            "Local_Entity_Bonuses",
            "HQ_Bonuses",
            "Sum",
            "GL_Balance",
            "Difference",
            "Status:_x",
            "Status:_CHECK"
        ]
    )

    cols_to_sum = gl_accs.copy()
    cols_to_sum.append("Difference")
    data.index.rename("Summary", inplace = True)

    for acc in gl_accs:

        # skip accounts that contian no data for the period being reconciled
        if acc not in fs10n:
            data[acc] = 0.0
            _logger.debug(
                f"Summarization for account {acc} skipped. "
                f"Reason: No data available for the reconciled period: {period}.")
            continue

        if fs10n[acc] is None:
            cumm_balance = 0
        else:
            cumm_balance = fs10n[acc].loc[period, "Cummulative_Balance"]
            if pd.isna(cumm_balance):
                cumm_balance = 0

        data.loc["Local_Entity_Bonuses", acc] = le_calcs[acc].sum()
        data.loc["HQ_Bonuses", acc] = hq_calcs[acc].sum() if hq_calcs is not None else 0
        data.loc["GL_Balance", acc] = cumm_balance
        data.loc["Status:_x", acc] = txt_summs[acc].query(f"Status == '{Marks.INVALD_TEXT_FMT.value}'")["LC_Amount_Sum"].sum()
        data.loc["Status:_CHECK", acc] = txt_summs[acc].query(f"Status == '{Marks.AGREEMENT_CLOSED.value}'")["LC_Amount_Sum"].sum()

    data.loc["Local_Entity_Bonuses", "Difference"] = le_calcs["Difference"].sum()
    data.loc["HQ_Bonuses", "Difference"] = hq_calcs["Difference"].sum() if hq_calcs is not None else pd.NA

    data.loc["Sum", cols_to_sum] = data.loc[["Local_Entity_Bonuses", "HQ_Bonuses"], cols_to_sum].sum()
    data.loc["Difference", gl_accs] = data.loc["GL_Balance", gl_accs] - data.loc["Sum", gl_accs]
    data.loc["Status:_x", "Difference"] = data.loc["Status:_x", gl_accs].sum()
    data.loc["Status:_CHECK", "Difference"] = data.loc["Status:_CHECK", gl_accs].sum()

    # rounding after summarization
    for col in data.columns:
        mask = data[col].notna()
        data.loc[mask, col] = data.loc[mask, col].astype("float64").round(2)

    data.reset_index(inplace = True)
    data["Summary"] = data["Summary"].str.replace("_", " ", regex = False)

    return data

def compile_recon_info(cntry: str, cocd: str, exch_rate: float, curr: str, fisc_year: int, period: int, accs: list,
                       sal_offs: list, sal_org_hq: str, sal_org_le: str, recon_date: date, recon_time: date) -> DataFrame:
    """
    Creates reconciliation info data that will be placed onto 'Info' sheet of the user report.

    Params:
    -------
    cntry:
        Name of the reconciled country.

    cocd:
        Company code of the reconciled country.

    exch_rate:
        Exchange rate used for currency conversion to EUR.

    curr:
        Local currency.

    period:
        Period for which reconciliation is performed.

    fisc_year:
        Fiscal year for which reconciliation is performed.

    accs:
        List of GL accounts bein greconciled.

    sal_offs:
        Sales office codes used for reconciliation.

    sal_org_hq:
        Sales organization code for head quarter.

    sal_org_le:
        Code of local sales organization.

    recon_date:
        Date on which reconciliation is performed.

    recon_time:
        Time on which reconciliation is performed.

    Returns:
    --------
    A DataFrame object contianing reconciliation info data.
    """

    assert 1 <= period < 15, "Argument 'period' has incorrect value!"

    data = DataFrame.from_dict({
        "Country": cntry,
        "Company_code": cocd,
        "Exchange_rate": exch_rate,
        "Local_currency": curr,
        "Period": period,
        "Fiscal_year": fisc_year,
        "GL_accounts": accs,
        "Sales_offices": sal_offs,
        "Sales_organization_global": sal_org_hq,
        "Sales_organization_local": sal_org_le,
        "Date": recon_date,
        "Time": recon_time
        },
        orient = "index"
    )

    data[0] = data[0].apply(lambda x: x if type(x) is list else [x])

    data = data.apply(
        lambda x: Series(x[0]), axis = 1, result_type = "expand"
    )

    data.loc["Exchange_rate", :] = pd.to_numeric(data.loc["Exchange_rate", :]).astype("float32")
    data.loc["Period", :] = pd.to_numeric(data.loc["Period", :]).astype("UInt8")
    data.loc["Fiscal_year", :] = pd.to_numeric(data.loc["Fiscal_year", :]).astype("UInt16")
    data.loc["GL_accounts", :] = pd.to_numeric(data.loc["GL_accounts", :]).astype("UInt64")
    data.loc["Sales_offices", :] = data.loc["Sales_offices", :].astype("category")
    data.loc["Sales_organization_global", :] = data.loc["Sales_organization_global", :].astype("category")
    data.loc["Sales_organization_local", :] = data.loc["Sales_organization_local", :].astype("category")

    data.fillna(pd.NA, inplace = True)

    return data

def _get_status_msg(hq_ageem: int, le_agreems: Series) -> str:
    """
    Generates 'Status' text associated with agreements.
    """

    if hq_ageem is pd.NA:
        msg = "no match"
    elif hq_ageem in tuple(le_agreems.dropna()):
        msg = f"Is in HQ and Local Agreements. Agreement Nr. {hq_ageem}"
    else:
        msg = f"Is in HQ Agreements only. Agreement Nr. {hq_ageem}"

    return msg

def _get_difference_hq_status(le_diff: float, le_agreem: int, hq_agreems: Series) -> Any:
    """
    Generates 'HQ Diff' value associated with agreements.
    """

    if le_agreem is pd.NA:
        stat = ""
    elif le_agreem in tuple(hq_agreems.dropna()):
        stat = le_diff
    else:
        stat = f"Agreement Nr. {le_agreem} is just in local overview."

    return stat

def _get_overview_val(le_agreem: int, hq_agreems: Series, le_agreems: Series) -> str:
    """
    Generates 'Overview' text associated with agreements.
    """

    if le_agreem is pd.NA:
        val = ""
    elif le_agreem in tuple(hq_agreems.dropna()) and le_agreem in tuple(le_agreems.dropna()):
        val = "HQ and Local"
    elif le_agreem in tuple(le_agreems.dropna()) and not le_agreem in tuple(hq_agreems.dropna()):
        val = "In Local Overview"
    else:
        val = "In HQ overview"

    return val

def _get_amount_compared_val(le_diff: float, hq_diff: float) -> Any:
    """
    Generates 'Amount Compared' value associated with agreements.
    """

    if le_diff is pd.NA or hq_diff is pd.NA:
        val = ""
    elif not str(hq_diff).isnumeric():
        val = 'X'
    else:
        val = hq_diff - le_diff

    return val

def consolidate_zsd25_data(le_calcs: DataFrame, hq_calcs: DataFrame) -> tuple:
    """
    Consolidates local and head quarter baonus calculations for Germany.

    Params:
    -------
    le_calcs:
        Calculated data for local entity bonuses.

    hq_calcs:
        Calculated data for head quarter bonuses.

    Returns:
    --------
    A tuple of head quarter comparison data, local entity comparison data

    (deduped, hq_compare, local_compare
    """

    _logger.info("Consolidating ZSD25 data ...")

    # generate 'HQ Compare' data
    loc_bon_agreems = le_calcs["Agreement"].unique()
    glob_bon_agreems = hq_calcs["Agreement"].unique()
    loc_bon_row_count = loc_bon_agreems.shape[0]
    glob_bon_row_count = glob_bon_agreems.shape[0]
    max_row_count = max(loc_bon_row_count, glob_bon_row_count)

    hq_compare = DataFrame(
        columns = ["HQ_Agreements", "LE_Agreements"],
        index = pd.Index(range(0, max_row_count))
    )

    hq_compare.loc[:loc_bon_row_count - 1, "LE_Agreements"] = loc_bon_agreems
    hq_compare.loc[:glob_bon_row_count -1, "HQ_Agreements"] = glob_bon_agreems

    hq_compare["HQ_Agreements"] = hq_compare["HQ_Agreements"].astype("UInt64")
    hq_compare["LE_Agreements"] = hq_compare["LE_Agreements"].astype("UInt64")

    # remove all agreements from le bonus calcs
    # that are also located in hq bonus calcs
    dual_vals = le_calcs["Agreement"].apply(
        lambda x: x in glob_bon_agreems
    )

    idx = dual_vals[dual_vals == True].index
    deduped = le_calcs.drop(index = idx)
    deduped.reset_index(inplace = True, drop = True)

    hq_compare = hq_compare.assign(
        Overview = hq_compare.apply(
            lambda x: _get_status_msg(x["HQ_Agreements"], hq_compare["LE_Agreements"]), axis = 1
        )
    )

    loc_calc_row_count = le_calcs.shape[0]
    glob_calc_row_count = hq_calcs.shape[0]
    max_row_count = max(loc_calc_row_count, glob_calc_row_count)

    # generate local compare data
    local_compare = DataFrame(
        columns = [
            "LE_Agreem",
            "LE_Diff",
            "HQ_Agreem"
        ],
        index = pd.Index(range(0, max_row_count))
    )

    local_compare.loc[:loc_calc_row_count -1, "LE_Diff"] = le_calcs["Difference"]
    local_compare.loc[:loc_calc_row_count - 1, "LE_Agreem"] = le_calcs["Agreement"]
    local_compare.loc[:glob_calc_row_count -1, "HQ_Agreem"] = hq_calcs["Agreement"]

    local_compare["LE_Diff"].fillna(pd.NA, inplace = True)
    local_compare["LE_Agreem"].fillna(pd.NA, inplace = True)
    local_compare["HQ_Agreem"].fillna(pd.NA, inplace = True)

    local_compare = local_compare.assign(

        HQ_Diff = local_compare.apply(
            lambda x: _get_difference_hq_status(x["LE_Diff"], x["LE_Agreem"], local_compare["HQ_Agreem"]), axis = 1
        ),

        Overview = local_compare.apply(
            lambda x: _get_overview_val(x["LE_Agreem"], local_compare["HQ_Agreem"], local_compare["LE_Agreem"]), axis = 1
        ),

    )

    local_compare = local_compare.assign(
        Amount_Compared = local_compare.apply(
            lambda x: _get_amount_compared_val(x["LE_Diff"], x["HQ_Diff"]), axis = 1
        )
    )

    local_compare.rename({
        "HQ_Agreem": "HQ_Agreements",
        "LE_Agreem": "Local_Entity_Bonuses_Agreements",
        "LE_Diff": "Difference_Local_Entity_Bonuses_Agreement",
        "HQ_Diff": "Difference_HQ_Agreement"
    }, inplace = True, axis = 1)

    return (deduped, hq_compare, local_compare)

def get_se16_agreements(data: DataFrame) -> tuple:
    """
    Returns a tuple of non-duplicated agreement
    numbers contained in SE16 data 'Agreement' field.

    Params:
    -------
    data:
        A DataFrame object containing SE16 data.

    Returns:
    --------
    A tuple of non-duplicated agreement numbers.
    """

    nums = data["Agreement"].unique()

    return tuple(nums)

def store_to_binary(data: DataFrame, file_path: str) -> None:
    """
    Stores a DataFrame object into a local file in 'pickle' format.

    Params:
    -------
    data:
        A DataFrame object containing sccounting data to store.

    file_path:
        Path to the binary file to which data will be stored.

    Returns:
    --------
    None.
    """

    if not file_path.endswith(".feather"):
        raise ValueError(f"Invalid file type used: {file_path}")

    reset = data.reset_index(drop = True)
    reset.columns = [str(col) for col in reset.columns]

    _logger.debug(f"Dumping data to file: '{file_path}'")
    reset.to_feather(file_path)

def _clean_text_opt(txt: list, patt: str) -> str:
    """
    Removes irrelevant lines from text.
    """

    # get all data lines containing accounting items
    # delete unused objects manually in case of a GC miss
    matches = re.findall(patt, txt[0], re.M)
    replaced = list(map(lambda x: x[1:-1].strip(), matches))
    del matches
    cleaned = "\n".join(replaced)
    del replaced

    return cleaned

def _parse_data_opt(data: DataFrame) -> DataFrame:
    """
    Parses records stored in a DataFrame
    object and casts the resulting fields
    to appropriate data types.
    """

    # replace empty strings with null indicating either unused or missing
    # (possibly as a result of a mistake during item posting) field values
    data.loc[(data["Text"] == ""), "Text"] = pd.NA                    # missing val
    data.loc[(data["Assignment"] == ""), "Assignment"] = pd.NA        # missing val
    data.loc[(data["Tax_Code"] == ""), "Tax_Code"] = pd.NA            # unused val
    data.loc[(data["Business_Area"] == ""), "Business_Area"] = pd.NA  # unused val

    # open items have no clearing document, coerce filling empty recs with
    # null, otherwise convert to int where clearing document exsits
    data["Clearing_Document"] = pd.to_numeric(
        data["Clearing_Document"],
    errors = "coerce").astype("UInt64")

    data["LC_Amount"] = data["LC_Amount"].str.replace(".", "", regex = False).str.replace(",", ".", regex = False)
    data["LC_Amount"] = data["LC_Amount"].mask(data["LC_Amount"].str.endswith("-"), "-" + data["LC_Amount"].str.rstrip("-"))
    data["LC_Amount"] = pd.to_numeric(data["LC_Amount"]).astype("float64")

    data["Document_Date"] = pd.to_datetime(data["Document_Date"], dayfirst = True).dt.date
    data["Posting_Date"] = pd.to_datetime(data["Posting_Date"], dayfirst = True).dt.date
    data["Posting_Key"] = pd.to_numeric(data["Posting_Key"])

    # extract accounting params separated by a semicolon
    # from 'Text' field into separate fields
    data["Tokens"] = data["Text"].str.split(";")

    # do no use assign() to add new fields as this
    # creates a new dataframe by copying
    data["Condition"] = pd.NA
    data["Category"] = pd.NA
    data["Customer"] = pd.NA
    data["Agreement"] = pd.NA
    data["Note"] = pd.NA

    # get only token records containig at least 4 values
    # and immediately convert the extracted vals to appropriate
    # dtypes to save as much memory as possible
    idx = data[data["Tokens"].str.len() >= 4].index

    data.loc[idx, "Condition"] = data.loc[idx, "Tokens"].str.get(0)
    data["Condition"] = data["Condition"].str.strip(" ")
    data["Condition"] = data["Condition"].astype("category")

    data.loc[idx, "Category"] = data.loc[idx, "Tokens"].str.get(1)
    data["Category"] = data["Category"].str.strip(" ")

    invaid_categs = (data["Category"].str.len() > 2)

    if invaid_categs.notna().any():
        # erase vals where category text length is >= 2,
        # which certainly not a valid category value.
        # The erased categories will not be listed in the user report, though.
        invalid_vals = data.loc[invaid_categs, "Category"].unique()
        invalid_vals = "; ".join(invalid_vals)

        _logger.warning(
            "Invalid category values found. The values will be removed form the data "
            f"and won't be included in the final user report: {invalid_vals}"
        )
        data.loc[invaid_categs, "Category"] = pd.NA

    data["Category"] = data["Category"].astype("category")

    data.loc[idx, "Customer"] = data.loc[idx, "Tokens"].str.get(2)

    # save memory immediately
    data["Customer"] = pd.to_numeric(data["Customer"], errors = "coerce").astype("UInt32")

    data.loc[idx, "Agreement"] = data.loc[idx, "Tokens"].str.get(3)
    data["Agreement"] = pd.to_numeric(data["Agreement"], errors = "coerce").astype("UInt32")

    # returns none if token has no such index
    data.loc[idx, "Note"] = data.loc[idx, "Tokens"].str.get(4)

    # remove splitted text tokens from data
    data.drop("Tokens", axis = 1, inplace = True)

    cond_idx = data[data["Condition"].notna()].index
    categ_idx = data[data["Category"].notna()].index
    note_idx = data[data["Note"].notna()].index

    if not cond_idx.empty:
        data.loc[cond_idx, "Condition"] = data.loc[cond_idx, "Condition"].str.strip()

    if not categ_idx.empty:
        data.loc[categ_idx, "Category"] = data.loc[categ_idx, "Category"].str.strip()

    if not note_idx.empty:
        data.loc[note_idx, "Note"] = data.loc[note_idx, "Note"].str.strip()
        data.loc[(data["Note"] == ""), "Note"] = pd.NA # missing vals

    incorr_cond_qry = data.query("Condition.notna() and Condition.str.len() != 4")
    incorr_categ_qry = data.query("Category.notna() and Category.str.len() != 2")

    # replace incorrect extracted entries with nan
    data.loc[incorr_cond_qry.index, "Condition"] = pd.NA
    data.loc[incorr_categ_qry.index, "Category"] = pd.NA

    # convert the extracted data to appropriate data types
    data["Note"] = data["Note"].astype("string")

    return data

def convert_fbl3n_data_opt(file_path: str, multiproc: bool = False, n_workers: int = 5) -> DataFrame:
    """
    Converts data exported form FBL5N into a DataFrame object.

    Params:
    -------
    file_path:
        Path to the file containing FBL3N data.

    n_workers:
        Indicates number of workers for parallel data processing (optimal 5).

    Returns:
    --------
    A DataFrame object. The result of data conversion.
    """

    MAX_ROWS_SNG = 1000

    text = read_textual_file(file_path)
    text = _clean_text_opt([text], patt = r"^\|\s+\d{4}\|.*$")

    data = pd.read_csv(StringIO(text),
        names = list(_FBL3N_HEADER),
        sep = '|',
        engine = "pyarrow",
        # Fields that originally contain text might contain floats
        # which get falsely parsed to a numeric dtype. Other time,
        # these fields contain no value. Hence these fields need
        # to be explicitly converted to strings at this stage.
        dtype = {
            "Fiscal_Year": "UInt16",
            "Period": "UInt8",
            "GL_Account": "UInt32",
            "Assignment": "string",
            "Document_Number": "UInt64",
            "Business_Area": "string",
            "Document_Type": "category",
            "Posting_Key": "UInt8",
            "LC_Amount": "string",
            "Tax_Code": "string",
            "Clearing_Document": "string",
            "Text": "string"
        }
    )

    # remove leading and trailing spaces from string fields
    str_columns = [
        "Assignment", "Business_Area", "Tax_Code",
        "Text", "LC_Amount", "Clearing_Document"
    ]

    for col in str_columns:
        data[col] = data[col].str.strip()

    # if there' small number of rows
    # to process, use single processing
    if data.shape[0] <= MAX_ROWS_SNG:
        multiproc = False

    if not multiproc:
        parsed = _parse_data_opt(data)
        assert parsed is data, "Object references don't match!"
    else:

        assert n_workers >= 2, "Argument 'n_workers' has incorrect value!"

        # split data into smaller manageable chunks
        data_chunks = np.array_split(data, n_workers)
        del data

        # init pool of workers and let them process the data chunks
        with Pool(n_workers) as data_pool:
            parsed = data_pool.map(_parse_data_opt, data_chunks)
            del data_chunks

        # combine the data parts returned by workers
        parsed = pd.concat(parsed, copy = False)

    if parsed.empty:
        return None

    # check & reset data index
    if parsed.index.has_duplicates:
        parsed.reset_index(inplace = True, drop = True)

    categorical = (
        "Document_Type",
        "Condition",
        "Category",
        "Posting_Key",
        "Assignment",
        "Business_Area",
        "Tax_Code",
        "GL_Account",       # added
        "Document_Number",  # added
        "Fiscal_Year"       # added
    )

    # finally, convert the respective data fields to (or back to)
    # categories. This is needed particularly following data concatenation
    for col in categorical:
        parsed[col] = parsed[col].astype("category")

    # validate extracted categories by comparing
    # the values with teh list of official categs
    categs = (
        "B1", "B2", "B3", "B4",
        "B5", "B6", "B7", "B8",
        "BO", "C1", "C2", "C3",
        "D1", "DS", "E1", "EM",
        "FE", "FS", "S1", "SE",
        "YJ", "EU", "GR"
    )

    if not parsed["Category"].cat.categories.isin(categs).all():
        undef_cats = set(parsed['Category'].cat.categories) - set(categs)
        _logger.warning(f"Undefined categories found: '{'; '.join(undef_cats)}'")

    # ensure all amounts with pstkey = 50 are negative
    # since .dat files store LC amounts as absolute vals
    mask = ((parsed["Posting_Key"] == 50) & (parsed["LC_Amount"] > 0))
    parsed.loc[mask, "LC_Amount"] = -1 * parsed.loc[mask, "LC_Amount"]

    # resetting index will remove any duplicated index vals
    # as a result of data concatenation
    parsed.reset_index(inplace = True, drop = True)

    assert parsed["Fiscal_Year"].dtype == "category"
    assert parsed["GL_Account"].dtype == "category"
    assert parsed["Customer"].dtype == "UInt32"
    assert parsed["Agreement"].dtype == "UInt32"
    assert parsed["Period"].dtype == "UInt8"
    assert parsed["Document_Number"].dtype == "category"
    assert parsed["Clearing_Document"].dtype == "UInt64"
    assert parsed["LC_Amount"].dtype == "float64"
    assert parsed["Document_Type"].dtype == "category"
    assert parsed["Condition"].dtype == "category"
    assert parsed["Category"].dtype == "category"
    assert parsed["Posting_Key"].dtype == "category"
    assert parsed["Document_Date"].dtype in ("object", "datetime64[ns]")
    assert parsed["Posting_Date"].dtype in ("object", "datetime64[ns]")
    assert parsed["Text"].dtype == "string"
    assert parsed["Assignment"].dtype  == "category"
    assert parsed["Tax_Code"].dtype  == "category"
    assert parsed["Business_Area"].dtype  == "category"

    assert parsed["Document_Type"].notna().all()
    assert parsed["Posting_Key"].notna().all()

    return parsed
