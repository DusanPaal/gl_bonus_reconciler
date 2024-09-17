# pylint: disable = C0103, W1203

"""
The 'biaWeb.py' module mediates fetching of currency
exchange rate data from the Ledvance FxRates portal
(web link: https://itsp01funcapps0b2e4.blob.core.windows.net/itsp01cdn/v1/FXrates/FXrates.html).
"""

from datetime import date, datetime, timedelta
import logging
import requests
import urllib3
from requests.exceptions import ReadTimeout

class ResponseError(Exception):
    """
    When exchange rate is not
    found for a given currency.
    """

urllib3.disable_warnings()

_allowed_currs = [
    "GBP", "EUR", "CHF", "CZK",
    "HUF", "PLN", "NOK", "SEK",
    "RUB", "DKK"
]

_logger = logging.getLogger("master")

def get_exchange_rate(valid_on: date, to_curr: str, from_curr: str = "EUR", resp_timeout: int = 15) -> float:
    """
    Returns the exchange rate for converting an amount \n
    from the source currency to the target currency.

    Params:
    -------
    valid_on:
        Date for which the conversion will be performed (normally ultimo date).

    to_curr:
        Currency to which the conversion will be performed.

    from_curr:
        Currency from which the conversion will be performed.

    Returns:
    --------
    The exchange rate representing to/from ratio of the compared \n
    currencies if the target currency exists on the list, otherwise None.

    Raises:
    -------
    TimeoutError:
        When waiting for response from the portal server times out.

    ResponseError:
        When the response received form the portal server is an error.
        This happens as a result of attempting to receive data for a
        given day when there's no data published for that particular day.
    """

    if not date(2022, 8, 1) < valid_on < datetime.now().date() + timedelta(1):
        raise ValueError(f"Argument 'valid_on' has incorrect value: {valid_on}")

    if to_curr not in _allowed_currs:
        raise ValueError(f"Argument 'to_curr' has incorrect value: {to_curr}")

    if from_curr not in _allowed_currs:
        raise ValueError(f"Argument 'from_curr' has incorrect value: {from_curr}")

    _logger.debug(
        "Exchange rate params: "
        f"from = '{from_curr}', "
        f"to = '{to_curr}', "
        f"day = {valid_on.strftime('%d.%m.%Y')}"
    )

    if from_curr == to_curr:
        return 1.0

    payload = """[{
        "name":"tsdate",
        "type":"Date",
        "value":"$date$"
    }, {
        "name":"fromto",
        "type":"VarChar",
        "value":"to"
    }, {
        "name":"currency",
        "type":"VarChar",
        "value":"$curr$"
    }, {
        "name":"notation",
        "type":"VarChar",
        "value":"MN"
    }]""".replace("$curr$", to_curr).replace("$date$", valid_on.strftime("%Y-%m-%d"))

    try:
        resp = requests.post(
            url = "https://app-mchcla02.mch.osram.de:4127/fxrates/getfxrate",
            headers = {"content-type": "application/json"},
            verify = False,
            data = payload,
            timeout = resp_timeout
        )
    except ReadTimeout as exc:
        raise TimeoutError(str(exc)) from exc

    if not resp.ok:
        raise ResponseError(resp.reason)

    data = dict(resp.json())

    for itm in data["data"]:
        if itm["CurFrom"] == from_curr:
            return itm["MidRate"]

    return None
