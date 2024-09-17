# pylint: disable = C0103, C0123, C0301, E0401, E0611, W0703, W1203

"""
the 'biaDatabase.py' module provides procedures that mediate
application communication with the PostgreSQL database engine.
The database contains accounting data exported form SAP BSIS
(open items) and BSAS (cleared items) tables. The data in the
databese is organized into tables, each storing records for
a given company code. Each table consists of the following
fields:
    Record_ID (integer) - ID number of each stored record.
    GL_Account (integer) - General ledger account on which the document is posted.
    Business_Area (character) - Business area for which the document was posted.
    Fiscal_Year (integer) - Fiscal year of the document. May not correspond to calendar year.
    Period (smallint) - Period for which the document was created.
    Document_Number (bigint) - SAP number of the document.
    Document_Date (date) - Date for which the document was created.
    Posting_Date (date) - Date on which the document was created.
    Document_Type (character) - Internal type of the document.
    Assignment (character varying) - Document assignment.
    Tax_Code (character) - Tax code of the document.
    LC_Amount (money) - Amount of teh document in local curency.
    Posting_Key (smallint) - Posting key used when booking the document.
    Clearing_Document (bigint) - Number of a clearing document.
    Text (text) - Text description assigned to document.
    Condition (character) - Condition of the agreement to which the document belongs
    Category (character) - Category of the agreement to which the document belongs
    Customer (integer) - Customer number (account) that identifies bonus recipient.
    Agreement (integer) - Number of the agreement to which the document belongs.
    Note (character varying) - An occasional user note in the 'Text', aways placed
                               after the 'Agreement' number.
"""

from datetime import date
from io import StringIO
import logging
from typing import Union

import pandas as pd
from pandas import DataFrame
import sqlalchemy as sqal
from sqlalchemy import Numeric, asc, cast, delete, distinct, func, select, and_, text
from sqlalchemy.engine.base import Connection
from sqlalchemy.sql.schema import MetaData, Table

class NoConnectionError(Exception):
    """
    Raised when attempting to perform
    a database operation when no connection
    to the database exists.
    """

# database object namings
_DB_TABLE_BASE_NAME = "accounting_data_$cocd$"
_TEXT_SUMMARY_BASE_NAME = "text_summary_$cocd$_$acc$"
_YEARLY_SUMMARY_BASE_NAME = "yearly_summary_$cocd$"

# constants shared acros module procedures
_FETCH_DATA_FROM_DATE = "01.01.2022"

# global logger
_logger = logging.getLogger("master")

# list currencies that reflect the of LC_MONETARY
# settings used by current postgres databases
_lc_monetary = ("EUR", "USD")

def connect(host: str, port: int, db_name: str, user: str, passw: str) -> Connection:
    """
    Creates connection to the database engine.

    Params:
    -------
    host:
        Name of the database hosting server.

    port:
        Number of the port used for server connection.

    db_name:
        Name of the database containing data tables.

    user:
        User name used for connection to database engine.

    passw:
        User password used for connection to database engine.

    Returns:
    --------
    A sqlalchemy.Connection object that
    represents the connection to the database.
    """

    params = f"postgresql+psycopg2://{user}:{passw}@{host}:{port}/{db_name}"

    try:
        engine = sqal.create_engine(params)
    except Exception as exc:
        _logger.exception(exc)
        return None

    try:
        conn = engine.connect()
    except Exception as exc:
        _logger.exception(exc)
        return None

    return conn

def disconnect(conn: Connection):
    """
    Disconnects from a database engine.

    Params:
    -------
    conn:
        A sqlalchemy.Connection object
        that represents connection
        to the remote database.

    Returns:
    --------
    None.

    Raises:
    -------
    NoConnectionError:
        When trying to close a database
        connection that doesn't exist.
    """

    if conn is None:
        raise NoConnectionError("Trying to disconnect from a non-existing connection!")

    conn.close()

    return

def delete_data(conn: Connection, cocd: str, schema: str, pst_date: date) -> bool:
    """
    Removes all records from a database table where posting date is greater
    or equal to a given posting date.

    Params:
    -------
    conn: A Connection type object that mediates connection to the remote database.
    schema: The name of the schema to which the sequence belongs.
    cocd: Company code of the country for which record will be stored.
    pst_date: Posting date from which (incl.) recodrs will be deleted.

    Returns:
    --------
    True if records were successfully deleted, otherwise False.
    """

    tbl_name = _DB_TABLE_BASE_NAME.replace("$cocd$", cocd)
    del_date_fmt = pst_date.strftime("%Y-%m-%d")

    tbl = Table(tbl_name, MetaData(), autoload_with = conn, schema = schema)
    del_stmt = delete(tbl).where(tbl.c.Posting_Date >= del_date_fmt)

    try:
        del_result = conn.execute(del_stmt)
        conn.connection.commit()
    except Exception as exc:
        conn.connection.rollback()
        _logger.exception(exc)
        return False

    if del_result.rowcount == 0:
        _logger.warning("No records were deleted using the specified criteria!")

    return True

def reset_sequence(conn: Connection, schema: str, cocd: str) -> bool:
    """
    Sets a table sequencer value to the sequencer max currrent
    value plus one (max + 1).

    Params:
    -------
    conn: A Connection type object that mediates connection to the remote database.
    schema: The name of the schema to which the sequence belongs.
    cocd: Company code of the country for which record will be stored.

    Returns:
    --------
    True if records were successfully deleted, otherwise False.
    """

    tbl_name = _DB_TABLE_BASE_NAME.replace("$cocd$", cocd)
    tbl = Table(tbl_name, MetaData(), autoload_with = conn, schema = schema)

    max_id_stmt = select(func.max(tbl.c["Record_ID"]))

    try:
        max_id_res = conn.execute(max_id_stmt)
        max_id = max_id_res.fetchall()[0][0]
    except Exception as exc:
        _logger.exception(exc)
        return False

    reset_seq_stmt = text(f"SELECT setval(\'{schema}.\"{tbl_name}_Record_ID_seq\"\', {max_id + 1})")

    try:
        conn.execute(reset_seq_stmt)
        conn.connection.commit()
    except Exception as exc:
        conn.connection.rollback()
        _logger.exception(exc)
        return False

    return True

def store_data(conn: Connection, schema: str, cocd: str, data: DataFrame, money_curr: str) -> bool:
    """
    Stores data records contained in a DataFrame object into a database table.

    Params:
    -------
    conn: A Connection type object that mediates connection to the remote database.
    schema: The name of the schema to which the sequence belongs.
    cocd: Company code of the country for which record will be stored.
    data: A DataFrame object containing data records to store.
    money_curr: Currency for which the money type of a postgres database was set.

    Returns:
    --------
    True if records were successfully stored, otherwise False.
    """

    if money_curr not in _lc_monetary:
        raise ValueError(f"Argument 'money_curr' has incorrect value: {money_curr}")

    src_data = data.copy()
    src_data["LC_Amount"] = src_data["LC_Amount"].astype("string")

    if money_curr == "EUR":
        src_data["LC_Amount"] = src_data["LC_Amount"].str.replace(".", ",", regex = False)
        src_data["LC_Amount"] = src_data["LC_Amount"] + " €"
    elif money_curr == "USD":
        src_data["LC_Amount"] = src_data["LC_Amount"].str.replace(" €", "", regex = False)
        negats = src_data["LC_Amount"].str.contains("-")
        posits = ~negats
        src_data.loc[negats, "LC_Amount"] = src_data.loc[negats, "LC_Amount"].str.replace("-", "", regex = False)
        src_data.loc[negats, "LC_Amount"] = "($" + src_data.loc[negats, "LC_Amount"] + ")"
        src_data.loc[posits, "LC_Amount"] = "$" + src_data.loc[posits, "LC_Amount"]
        src_data["LC_Amount"] = src_data["LC_Amount"].str.replace("\xa0", "", regex = False)
        src_data["LC_Amount"] = src_data["LC_Amount"].str.replace(",", ".", regex = False)

    tbl_name = _DB_TABLE_BASE_NAME.replace("$cocd$", cocd)
    s_buff = StringIO()
    src_data.to_csv(s_buff, header = False, index = False)
    s_buff.seek(0)

    columns = ", ".join([f'"{c}"' for c in data.columns])
    table = Table(tbl_name, MetaData(), autoload_with = conn, schema = schema)

    if table.schema:
        full_tbl_name = f"{table.schema}.{table.name}"
    else:
        full_tbl_name = table.name

    stmt = f"COPY {full_tbl_name} ({columns}) FROM STDIN WITH CSV"

    with conn.connection.cursor() as cur:
        try:
            cur.copy_expert(sql = stmt, file = s_buff)
            conn.connection.commit()
        except Exception as exc:
            _logger.exception(exc)
            conn.connection.rollback()
            return False

    return True

def get_text_summary(conn: Connection, schema: str, cocd: str, acc: Union[str, int]) -> DataFrame:
    """
    Creates subtotals to 'LC_Amount' at the 'Text' table field for a given GL account.

    Params:
    -------
    conn: A Connection type object that mediates connection to the remote database.
    schema: The name of the schema to which the sequence belongs.
    cocd: Company code of the country for which record will be stored.
    acc: GL account number.

    Returns:
    --------
    A DataFrame object containing the summarized data.
    """

    if not str(acc).isnumeric():
        raise ValueError(f"Argument 'acc' has incorrect value: {acc}")

    tbl_name = _DB_TABLE_BASE_NAME.replace("$cocd$", cocd)
    mw_name = _TEXT_SUMMARY_BASE_NAME.replace("$cocd$", cocd).replace("$acc$", str(acc))
    tbl = Table(tbl_name, MetaData(), autoload_with = conn, schema = schema)

    try:
        mw_data = Table(mw_name, MetaData(), autoload_with = conn, schema = schema)
    except Exception as exc:
        _logger.exception(exc)
        return None

    cols = tbl.columns

    tb_stmt = select(
        func.coalesce(cols.Text, "(blank)"),
        cols.Agreement,
        cols.Category,
        cols.Condition,
        cols.Customer,
        cols.Note,
        cast(func.sum(cols.LC_Amount), Numeric(12, 2))
    ).where(
        and_(cols.GL_Account == acc, cols.Posting_Date >= _FETCH_DATA_FROM_DATE)
    ).group_by(
        cols.Text,
        cols.Agreement,
        cols.Category,
        cols.Condition,
        cols.Customer,
        cols.Note
    ).order_by(asc(cols.Text))

    try:
        tb_res = conn.execute(tb_stmt)
        tb_data = DataFrame(tb_res.fetchall())
    except Exception as exc:
        _logger.exception(exc)
        return None

    mw_stmt = mw_data.select()

    try:
        mw_res = conn.execute(mw_stmt)
        mw_data = DataFrame(mw_res.fetchall())
    except Exception as exc:
        _logger.exception(exc)
        return None

    if not tb_data.empty: # for some accs no postings have been made since the posting date of the last item in the materialized view
        tb_data.columns = ["Text", "Agreement", "Category", "Condition", "Customer", "Note", "LC_Amount_Sum"]
        tb_data["Agreement"] = pd.to_numeric(tb_data["Agreement"]).astype("UInt32")
        tb_data["Customer"] = pd.to_numeric(tb_data["Customer"]).astype("UInt32")
        tb_data["Note"] = tb_data["Note"].astype("string")
        tb_data["LC_Amount_Sum"] = pd.to_numeric(tb_data["LC_Amount_Sum"]).astype("float64")

    mw_data.columns = ["Text", "Agreement", "Category", "Condition", "Customer", "Note", "LC_Amount_Sum"]
    mw_data["Agreement"] = pd.to_numeric(mw_data["Agreement"]).astype("UInt32")
    mw_data["Customer"] = pd.to_numeric(mw_data["Customer"]).astype("UInt32")
    mw_data["Note"] = mw_data["Note"].astype("string")
    mw_data["LC_Amount_Sum"] = pd.to_numeric(mw_data["LC_Amount_Sum"]).astype("float64")

    concat = pd.concat([tb_data, mw_data], verify_integrity = True, ignore_index=True)

    result = concat.groupby(
        by = ["Text", "Agreement", "Category", "Condition", "Customer", "Note"], dropna = False
    ).sum().reset_index().sort_values("Text")

    result["LC_Amount_Sum"] = result["LC_Amount_Sum"].round(2)
    result["Category"] = result["Category"].astype("object").astype("category")
    result["Condition"] = result["Condition"].astype("object").astype("category")

    return result

def get_yearly_summary(conn: Connection, schema: str, cocd: str) -> DataFrame:
    """
    Creates subtotals to 'LC_Amount' at 'GL_Account', 'Fiscal_Year'
    and 'Period' fields. The subtotals will be placed onto the
    'Period Overview' sheet in the user report.

    Params:
    -------
    conn: A Connection type object that mediates connection to the remote database.
    schema: The name of the schema to which the sequence belongs.
    cocd: Company code of the country for which the summary will be generated.

    Returns:
    --------
    A DataFrame object containing the summarized data.
    """

    tbl_name = _DB_TABLE_BASE_NAME.replace("$cocd$", cocd)
    mw_name = _YEARLY_SUMMARY_BASE_NAME.replace("$cocd$", cocd)

    tbl = Table(tbl_name, MetaData(), autoload_with = conn, schema = schema)

    try:
        mw_data = Table(mw_name, MetaData(), autoload_with = conn, schema = schema)
    except Exception as exc:
        _logger.exception(exc)
        return None

    cols = tbl.columns

    tb_stmt = select(
        cols.GL_Account,
        cols.Fiscal_Year,
        cols.Period,
        cast(func.sum(cols.LC_Amount), Numeric(12, 2))
    ).where(
        tbl.c.Posting_Date >= _FETCH_DATA_FROM_DATE
    ).group_by(
        cols.GL_Account,
        cols.Fiscal_Year,
        cols.Period
    )

    try:
        tb_res = conn.execute(tb_stmt)
        tb_data = DataFrame(tb_res.fetchall())
    except Exception as exc:
        _logger.exception(exc)
        return None

    mw_stmt = mw_data.select()

    try:
        mw_res = conn.execute(mw_stmt)
        mw_data = DataFrame(mw_res.fetchall())
    except Exception as exc:
        _logger.exception(exc)
        return None

    tb_data.columns = ["GL_Account", "Fiscal_Year", "Period", "LC_Amount"]
    mw_data.columns = ["GL_Account", "Fiscal_Year", "Period", "LC_Amount"]

    concat = pd.concat([tb_data, mw_data])
    result = concat.groupby(["GL_Account", "Fiscal_Year", "Period"]).sum().reset_index()

    return result

def copy(loc_conn: Connection, rem_conn: Connection, loc_schema: str,
         rem_schema: str, rem_monetary: str, cocd: str) -> bool:
    """
    Copies content of a table stored in a local database and places
    the data into a table stored in a remote database. teh tables are
    identified based on a specific company code.

    Params:
    -------
    loc_conn: A Connection type object that mediates connection to the local database.
    rem_conn: A Connection type object that mediates connection to the remote database.
    loc_schema: The name of the schema in the local database to which the sequence belongs.
    rem_schema The name of the schema in the remote database to which the sequence belongs.
    rem_monetary: Currency for which the money type of the remote postgres database was set.
    cocd: Company code that identifies the table to copy.

    Returns:
    --------
    True if data is successfully copied, False if not.
    """

    if rem_monetary not in _lc_monetary:
        raise ValueError(f"Argument 'money_curr' has incorrect value: {rem_monetary}")

    tbl_name = _DB_TABLE_BASE_NAME.replace("$cocd$", cocd)
    src_tbl = Table(tbl_name, MetaData(), autoload_with = loc_conn, schema = loc_schema)
    cols = src_tbl.columns

    crits_stmt = select(
        distinct(cols.GL_Account),
        cols.Fiscal_Year
    ).order_by(
        cols.GL_Account,
        cols.Fiscal_Year
    )

    src_res = loc_conn.execute(crits_stmt)
    src_data = DataFrame(src_res.fetchall())

    for acc, year in src_data.itertuples(index = False):

        _logger.info(f" Migrating account {acc}; fiscal year {year} ...")
        select_stmt = src_tbl.select().where(
            and_(src_tbl.c.GL_Account == acc, src_tbl.c.Fiscal_Year == year)
        )

        src_res = loc_conn.execute(select_stmt)
        src_data = DataFrame(src_res.fetchall())

        src_data["Fiscal_Year"] = src_data["Fiscal_Year"].astype("UInt16")
        src_data["GL_Account"] = src_data["GL_Account"].astype("UInt32")
        src_data["Customer"] = src_data["Customer"].astype("UInt32")
        src_data["Agreement"] = src_data["Agreement"].astype("UInt32")
        src_data["Period"] = src_data["Period"].astype("UInt8")
        src_data["Document_Number"] = src_data["Document_Number"].astype("Int64")
        src_data["Clearing_Document"] = src_data["Clearing_Document"].astype("Int64")
        src_data["Text"] = src_data["Text"].astype("string")
        src_data["Assignment"] = src_data["Assignment"].astype("category")
        src_data["Business_Area"] = src_data["Business_Area"].astype("category")
        src_data["Document_Type"] = src_data["Document_Type"].astype("category")
        src_data["Tax_Code"] = src_data["Tax_Code"].astype("category")
        src_data["Condition"] = src_data["Condition"].astype("category")
        src_data["Category"] = src_data["Category"].astype("category")
        src_data["Posting_Key"] = src_data["Posting_Key"].astype("category")

        stored = store_data(rem_conn, src_data, cocd, rem_schema, rem_monetary)

    return stored
