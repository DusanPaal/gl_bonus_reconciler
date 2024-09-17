# pylint: disable = C0103, W0703, W1203

"""
The 'biaMail.py' module creates and sends of emails directly via SMTP server.
It also uses the exchangelib library to connect to the Exchange server via
Exchange Web Services (EWS) in order to retrieve messages and save message
attachment under a specified account.

Version history:
1.0.20220112 - Initial version.
1.1.20220614 - Added retrieving message objects from the Exchange server accounts and
               saving message attachments to a local file.
"""

from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import logging
import os
from os.path import exists, isfile, join, split
import re
import socket
from smtplib import SMTP
from typing import Union

import pandas as pd
import exchangelib as xlib
from exchangelib import (
    Account, Build, Configuration,
     Identity, Message, Version
)

# custom message classes
class SmtpMessage(MIMEMultipart):
    """
    A wrapper for MIMEMultipart
    objects that are sent via
    an SMTP server.
    """

class Credentials(xlib.OAuth2Credentials):
    """
    A wrapper class around an
    OAuth2Credentials object.
    """

# custom warnings
class MultipleMessagesWarning(Warning):
    """
    When multiple messages with
    the same message ID are found.
    """

class UndeliveredWarning(Warning):
    """
    Raised when message is not
    delivered to all recipients.
    """

# custom exceptions
class FolderNotFoundError(Exception):
    """
    Raised when a directory is
    requested but doesn't exist.
    """

class AttachmentSavingError(Exception):
    """
    Raised when any exception is
    caught durng writing of attachment
    data to a file.
    """

class AttachmentNotFoundError(Exception):
    """
    Raised when a file attachment
    is requested but doesn't exist.
    """

class ParamNotFoundError(Exception):
    """
    Raised when a parameter required
    for creating credentials is not
    found in the source file.
    """

class InvalidSmtpHostError(Exception):
    """
    Raised when an invalid host name
    is used for SMTP connection.
    """

_logger = logging.getLogger("master")

def contains_attachment(msg: Message, ext: str = None) -> bool:
    """
    Checks if a message contains any attachments.

    Params:
    -------
    msg:
        An exchangelib.Message object representing an email.

    ext:
        File extension, that determines which attachment types to check. \n
        If None is used (default value), then any attachment type will be \n
        considered. If a file extension is used (e.g. '.pdf'), then attachments \n
        having that particular file type will be considered only.

    Returns:
    --------
    True if the message contains an attachment, False if not.
    """

    # check input
    if not isinstance(msg, Message):
        raise TypeError(f"Argument 'msg' has invalid type: {type(msg)}")

    if ext is not None and not isinstance(ext, str):
        raise TypeError(f"Argument 'ext' has invalid type: {type(ext)}")

    # perform evaluation
    if ext is None and len(msg.attachments) > 0:
        return True

    for att in msg.attachments:
        if att.name.lower().endswith(ext):
            return True

    return False

def _sanitize_emails(addr: Union[str,list]) -> list:
    """
    Trims email addresses and validates \n
    the correctness of their email format \n
    according to the company's standard.
    """

    mails = []
    validated = []

    if isinstance(addr, str):
        mails = [addr]
    elif isinstance(addr, list):
        mails = addr
    else:
        raise TypeError(f"Argument 'addr' has invalid type: {type(addr)}")

    for mail in mails:

        stripped = mail.strip()
        validated.append(stripped)

        # check if email is Ledvance-specific
        match = re.search(r"\w+\.\w+@ledvance.com", stripped)

        if match is not None:
            continue

        _logger.warning(f"Possibly invalid email address used: '{stripped}'.")

    return validated

def _attach_file(email: SmtpMessage, att_paths: list) -> SmtpMessage:
    """
    Attaches file(s) to a message.
    """

    for att_path in att_paths:

        if not isfile(att_path):
            raise AttachmentNotFoundError(f"Attachment not found: {att_path}")

        with open(att_path, "rb") as file:
            payload = file.read()

        # The content type "application/octet-stream" means
        # that a MIME attachment is a binary file
        part = MIMEBase("application", "octet-stream")
        part.set_payload(payload)
        encoders.encode_base64(part)

        # get file name
        file_name = split(att_path)[1]

        # Add header
        part.add_header(
            "Content-Disposition",
            f"attachment; filename = {file_name}"
        )

        # Add attachment to the message
        # and convert it to a string
        email.attach(part)

    return email

def create_message(from_addr: str, to_addr: Union[str,list], subj: str,
                   body: str, att: Union[str,list] = None) -> SmtpMessage:
    """
    Creates a message with or without attachment(s).

    Params:
    -------
    from_addr:
        Email address of the sender.

    to_addr:
        Email address(es) of recipient(s). \n
        If a single email address
        is used, the message will be sent to that specific address. \n
        If multiple addresses are used, then the message will be sent
        to all of the recipients.

    subj:
        Message subject.

    body:
        Message body in HTML format.

    att:
        Any valid path(s) to message atachment file(s). \n
        If None is used (default value), then the message will be created \n
        without any attachment. If a file path is passed, then this file \n
        will be attached to the message. If multiple file paths are passed, \n
        then these will be attached as multiple attachments to the message.

    Returns:
    --------
    A SmtpMessage object representing the message.

    Raises:
    -------
    AttachmentNotFoundError:
        If any of the attachment paths used is not found.
    """

    if not isinstance(to_addr, str) and len(to_addr) == 0:
        raise ValueError("No message recipients provided in 'to_addr' argument!")

    # sanitize input
    recips = _sanitize_emails(to_addr)

    # process
    email = SmtpMessage()
    email["Subject"] = subj
    email["From"] = from_addr
    email["To"] = ";".join(recips)
    email.attach(MIMEText(body, "html"))

    if att is None:
        return email

    if isinstance(att, list):
        att_paths = att
        email = _attach_file(email, att_paths)
    elif isinstance(att, str):
        att_paths = [att]
        email = _attach_file(email, att_paths)
    else:
        raise TypeError(f"Argument 'att' has invalid type: {type(att)}")

    return email

def send_smtp_message(msg: SmtpMessage, host: str, port: int):
    """
    Sends a message using an SMTP server.

    Params:
    ------
    msg:
        A SmtpMessage object representing the email to send.

    host:
        Name of the SMTP host server.

    port:
        Number o the SMTP server port.

    Returns:
    --------
    None.

    Raises:
    -------
    UndeliveredWarning:
        When message fails to reach all the required recipients.

    InvalidSmtpHostError:
        When an invalid host name is used for SMTP connection.

    TimeoutError:
        When attemt to connect to the SMTP servr times out.
    """

    # check input
    if not isinstance(msg, SmtpMessage):
        raise TypeError(f"Argument 'msg' has invalid type {type(msg)}")

    if not isinstance(host, str):
        raise TypeError(f"Argument 'host' has invalid type: {type(host)}")

    if not isinstance(port, int):
        raise TypeError(f"Argument 'port' has invalid type: {type(port)}" )

    try:
        with SMTP(host, port, timeout = 30) as smtp_conn:
            smtp_conn.set_debuglevel(0) # off = 0; verbose = 1; timestamped = 2
            send_errs = smtp_conn.sendmail(msg["From"], msg["To"].split(";"), msg.as_string())
    except socket.gaierror as exc:
        raise InvalidSmtpHostError(f"Invalid SMTP host name: {host}") from exc
    except TimeoutError as exc:
        raise TimeoutError("Attempt to connect to the SMTP servr timed out! Possible reasons: "
        "Slow internet connection or an incorrect port number used.") from exc

    if len(send_errs) != 0:
        undelivered = ';'.join(send_errs.keys())
        raise UndeliveredWarning(f"Message undelivered to: {undelivered}")

def save_attachments(msg: Message, folder_path: str, ext: str = None) -> list:
    """
    Saves email attachments with a specific type to a local file.

    Params:
    -------
    msg:
        An excahngelib.Message object representig the email.

    folder_path:
        Path to the folder where attachments will be stored.

    ext:
        File extension that determines which attachment types to download. \n
        If None is used (default value), then attachments of any file type will \n
        be downloaded. If a file extension is used (e.g. '.pdf'), then only \n
        attachmnets having that particular file type will be downloaded.

    Returns:
    --------
    A list of paths to the stored attachments.

    Rasises:
    --------
    FolderNotFoundError:
        When 'folder_path' argument refers to an non-existitg folder.

    AttachmentSavingError:
        When writing attachemnt data to a file fails.
    """

    if not exists(folder_path):
        raise FolderNotFoundError(f"Folder does not exist: {folder_path}")

    file_paths = []

    for att in msg.attachments:

        file_path = join(folder_path, att.name)

        if not (ext is None or file_path.lower().endswith(ext)):
            continue

        try:
            with open(file_path, 'wb') as a_file:
                a_file.write(att.content)
        except Exception as exc:
            raise AttachmentSavingError from exc

        if not isfile(file_path):
            raise AttachmentSavingError(f"Error writing attachment data to file: {file_path}")

        file_paths.append(file_path)

    return file_paths

def get_attachments(msg: Message, ext: str = None) -> list:
    """
    Fetches attachments from an email.

    Params:
    -------
    msg:
        An excahngelib.Message object representig the email.

    ext:
        File extension that determines which attachment types to fetch. \n
        If None is used (default value), then attachments of any file type will \n
        be fetched. If a file extension is used (e.g. '.pdf'), then only \n
        attachmnets having that particular file type will be fetched.

    Returns:
    --------
    A list of email attachments.
    """

    atts = []

    for att in msg.attachments:

        if not (ext is None or att.name.lower().endswith(ext)):
            continue

        atts.append(att.content)

    return atts

def get_credentials(acc_name: str) -> Credentials:
    """
    Returns credentails for a given account.

    Params:
    -------
    acc_name:
        Name of the account for which
        the credentails will be obtained.

    Returns:
    --------
    A Credentials object.

    Raises:
    -------
    FileNotFoundError:
        When the path to the file containing
        account credentails is invalid.

    ParamNotFoundError:
        When a credentails parameter is
        not found in the credentials file.
    """

    # check input
    if not isinstance(acc_name, str):
        raise TypeError(f"Argument 'acc_name' has invalid type: {type(acc_name)}")

    # process
    cred_dir = join(os.environ["APPDATA"], "bia")
    cred_path = join(cred_dir, f"{acc_name.lower()}.token.email.dat")

    if not isfile(cred_path):
        raise FileNotFoundError(f"Credentials file not found: {cred_path}")

    with open(cred_path, 'r', encoding = "utf-8") as file:
        lines = file.readlines()

    params = dict(
        client_id = None,
        client_secret = None,
        tenant_id = None,
        identity = Identity(primary_smtp_address = acc_name)
    )

    for line in lines:

        if ":" not in line:
            continue

        tokens = line.split(":")
        param_name = tokens[0].strip()
        param_value = tokens[1].strip()

        if param_name == "Client ID":
            key = "client_id"
        elif param_name == "Client Secret":
            key = "client_secret"
        elif param_name == "Tenant ID":
            key = "tenant_id"

        params[key] = param_value

    # verify loaded parameters
    if params["client_id"] is None:
        ParamNotFoundError("Parameter 'client_id' not found!")

    if params["client_secret"] is None:
        ParamNotFoundError("Parameter 'client_secret' not found!")

    if params["tenant_id"] is None:
        ParamNotFoundError("Parameter 'tenant_id' not found!")

    # params OK, create credentials
    creds = Credentials(
        params["client_id"],
        params["client_secret"],
        params["tenant_id"],
        params["identity"]
    )

    return creds

def get_account(mailbox: str, creds: Credentials, x_server: str) -> Account:
    """
    Logs into an account associated with
    a shared mailbox using valid credentials.

    Params:
    -------
    mailbox:
        Name of the shared mailbox.

    creds:
        Account credentials.

    x_server:
        Name of the MS Exchange server.

    Returns:
    --------
    An exchangelib.Account object.
    """

    build = Build(major_version = 15, minor_version = 20)

    cfg = Configuration(creds,
        server = x_server,
        auth_type = xlib.OAUTH2,
        version = Version(build)
    )

    cfg = Configuration(server = x_server, credentials = creds)

    acc = Account(mailbox,
        config = cfg,
        autodiscover = False,
        access_type = xlib.IMPERSONATION
    )

    return acc

def get_message(acc: Account, email_id: str) -> Message:
    """
    Uses a specific account to fetch an email
    with a specific IDfrom a mailbox.

    Params:
    -------
    acc:
        An exchangelib.Account object.

    email_id:
        The string ID of the message.

    Returns:
    --------
    An exchangelib.Message object representing the email. \n
    If no email is found, then None is returned.

    Raises:
    -------
    MultipleMessagesWarning:
        When more than one message
        is found for a given message ID.
    """

    # sanitize input
    if not (email_id.startswith("<") and email_id.endswith(">")):
        email_id = f"<{email_id}>"

    # process
    emails = acc.inbox.walk().filter(message_id = email_id).only(
        'subject', 'text_body', 'headers', 'sender',
        'attachments', 'datetime_received', 'message_id'
    )

    if emails.count() == 0:
        return None

    if emails.count() > 1:
        raise MultipleMessagesWarning(
            "Found more than one message "
            f"with message ID: {email_id}"
        )

    return emails[0]

def _parse_amount(num: str, ndigits: int = 2) -> float:
    """
    Parses string amounts represented
    in the standard SAP format.
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

def extract_user_data(msg: Message) -> dict:
    """
    Extracts specific params from a user email.

    Params:
    -------
    msg:
        An excahngelib.Message object representig the email.

    Returns:
    --------
    A dict of extracted params and the respective data:
    - email: Email address of the
    - name: First name of the message sender.
    - surname: Last name of the message sender.
    - company_code: Company code for which reconciliatin should be perfored.
    - incomplete: True, if user didn't provide all the required params, otherwise False.
    """

    email_addr = msg.sender.email_address
    sender_name = msg.sender.name.split(",")[1].strip()
    sender_surname = msg.sender.name.split(",")[0]

    cocd = None
    cocd_patt = r"Company code:\s*(?P<cocd>\d{4})"
    cocd_match = re.search(cocd_patt, msg.text_body, re.I|re.M)

    if cocd_match is not None:
        cocd = cocd_match.group("cocd")

    params = {
        "email": email_addr,
        "name": sender_name,
        "surname": sender_surname,
        "company_code": cocd,
        "incomplete": False
    }

    # check if there's any fx rate value supplied
    if re.search("fx rate", msg.text_body, re.I|re.M) is not None:
        rate = None
        rate_patt = r"FX Rate:\s*(?P<rate>(\d*\.)?\d+[,.]\d+)"
        rate_match = re.search(rate_patt, msg.text_body, re.I|re.M)
        if rate_match is not None:
            rate = _parse_amount(rate_match.group("rate"))
            rate = None if rate <= 0 else rate # exchange rate cannot be negative or zero
        params.update({"fx_rate": rate})

    if params["company_code"] is None or ("fx_rate" in params and params["fx_rate"] is None):
        params.update({"incomplete": True})

    return params
