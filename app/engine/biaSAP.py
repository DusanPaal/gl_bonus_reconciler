# pylint: disable = C0103, E0611, W0703, W1203

"""
The 'biaSAP.py' module uses win32com (package: pywin32) to connect
to SAP GUI scripting engine and login/logout to/from the defined
SAP system.
"""

from enum import Enum
from os.path import isfile
import subprocess
import win32com.client
from win32com.client import CDispatch
from win32ui import FindWindow
from win32ui import error as WinError

class Systems(Enum):
    """
    SAP systems and their full names.
    """
    P25 = "OG ERP: P25 Productive SSO"
    Q25 = "OG ERP: Q25 Quality Assurance SSO"

class LoginError(Exception):
    """
    Raised when logign to the SAP
    GUI scriptng engine fails.
    """

SYS_P25 = "OG ERP: P25 Productive SSO"
SYS_Q25 = "OG ERP: Q25 Quality Assurance SSO"

def _window_exists(name: str) -> bool:
    """Checks wheter SAP GUI process is running."""

    try:
        FindWindow(None, name)
    except WinError:
        return False
    else:
        return True

def login(exe_path: str, sys_name: str) -> CDispatch:
    """
    Logs into SAP GUI and connects
    to its scripting engine.

    Params:
    -------
    exe_path:
        Path to the SAP GUI executable.

    sys_name:
        Name of the SAP system to log in.

    Returns:
    --------
    An initialized SAP GuiSession object.

    Rasies:
    -------
    LoginError:
        Raised when logign to the SAP
        GUI scriptng engine fails.
    """

    if not isfile(exe_path):
        raise FileNotFoundError(f"SAP GUI executable not found: {exe_path}!")

    if not sys_name in (SYS_P25, SYS_Q25):
        raise ValueError("Invalid system name! Login to SAP using module constants!")

    if _window_exists("SAP Logon 750"):
        sap_gui_auto = win32com.client.GetObject('SAPGUI')
    else:

        try:
            proc = subprocess.Popen(exe_path)
        except Exception as exc:
            raise LoginError("Could not start SAP GUI application.") from exc

        try:
            proc.communicate(timeout = 8)
        except Exception:
            try:
                sap_gui_auto = win32com.client.GetObject('SAPGUI')
            except Exception as exc:
                raise LoginError("Could not bind SapGuiAuto reference.") from exc

    try:
        app = sap_gui_auto.GetScriptingEngine
    except Exception as exc:
        raise LoginError("Could not connect to SAP scripting engine.") from exc

    try:
        if app.Children.Count == 0:
            conn = app.OpenConnection(sys_name, True)
        else:
            conn = app.Children(0)
    except Exception as exc:
        raise LoginError("Could not create connection to SAP.") from exc

    try:
        sess = conn.Children(0)
    except Exception as exc:
        raise LoginError("Could not create new session.") from exc

    return sess

def logout(sess: CDispatch):
    """
    Logs out from an active SAP GUI system.

    Params:
    -------
    sess:
        An initialized SAP GuiSession object.

    Returns:
    --------
    None.
    """

    if sess is None:
        raise ValueError("Trying to close a connection that is actually not open!")

    sess.findById("wnd[0]/tbar[0]/okcd").text = "/nex"
    sess.findById("wnd[0]").sendVKey(0)
