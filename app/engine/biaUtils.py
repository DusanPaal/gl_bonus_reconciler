"""
The 'biaUtils.py' module provides following functionalities:
    - application performance measurement
    - memory consumption monitoring
    - data reading/loading times
    - decorators for procedure deprecation marking

The module is not an integral part of application, it's purpose
is to help developers in optimizing program execution time and space
requirements, as well as provide decorators for marking deprecated
procedures.
"""

# pylint: disable=C0123, C0103, C0301, C0302, E0401, E0611, W1203, W0703, W0603

import datetime as dt
from datetime import datetime
import functools
import inspect
from io import StringIO
import os
import re
import warnings

import numpy as np
import pandas as pd
import psutil

_r_path: str = None

def deprecated(reason: str):
    """
    This is a decorator which can be used to mark functions
    as deprecated. It will result in a warning being emitted
    when the function is used.
    """

    string_types = (type(b''), type(u''))

    if isinstance(reason, string_types):

        def decorator(obj1):

            if inspect.isclass(obj1):
                fmt1 = "Call to deprecated class: {name} ({reason})."
            else:
                fmt1 = "Call to deprecated function: {name} ({reason})."

            @functools.wraps(obj1)
            def new_func1(*args, **kwargs):
                warnings.simplefilter("always", DeprecationWarning)
                warnings.warn(
                    fmt1.format(name = obj1.__name__, reason = reason),
                    category = DeprecationWarning,
                    stacklevel = 2
                )
                warnings.simplefilter("default", DeprecationWarning)
                return obj1(*args, **kwargs)

            return new_func1

        return decorator

    if inspect.isclass(reason) or inspect.isfunction(reason):

        obj2 = reason

        if inspect.isclass(obj2):
            fmt2 = "Call to deprecated class: {name}."
        else:
            fmt2 = "Call to deprecated function: {name}."

        @functools.wraps(obj2)
        def new_func2(*args, **kwargs):
            warnings.simplefilter("always", DeprecationWarning)
            warnings.warn(
                fmt2.format(name = obj2.__name__),
                category = DeprecationWarning,
                stacklevel = 2
            )
            warnings.simplefilter("default", DeprecationWarning)
            return obj2(*args, **kwargs)

        return new_func2

    raise TypeError(repr(type(reason)))

def clear_results_log(f_path: str):
    """
    Clears file containing previous
    benchmark results if such exists,
    otherwise creates a new results file.

    Params:
        f_path: Path to the file containing benchmark results.

    Returns: None.
    """

    global _r_path
    _r_path = f_path

    with open(_r_path, 'w', encoding = "UTF-8") as b_log:
        b_log.write("GL Bonus Reconciler Benchmarks v. 1.0.20220625\n\n")

    return

def _write(msg: str):
    """
    Clears file containing previous
    benchmark results if such exists,
    otherwise creates a new results file.

    Params:
        msg: Message to write.

    Returns: None.
    """

    print(msg)

    with open(_r_path, 'a', encoding = "UTF-8") as b_log:
        b_log.write(msg + "\n")

    return

def timer_start() -> datetime:
    """
    Returns current time.
    Params: None.
    Returns: A datetime object representig current time.
    """

    stime = dt.datetime.now()

    return stime

def timer_elapsed(stime: datetime) -> int:
    """
    Returns elapsed time in seconds.

    Params:
        stime: Time value from which the elapsed time is calculated.

    Returns: An integer representing number of seconds elapsed.
    """

    ctime = dt.datetime.now()
    elapsed = ctime - stime
    secs = elapsed.total_seconds()

    return int(round(secs))

def get_ram_usage() -> int:
    """
    Returns memory in MB used
    by a running python process.

    Params: None.
    Returns: Memory usage in MB.
    """

    pid = os.getpid()
    python_process = psutil.Process(pid)
    rss = python_process.memory_info().rss
    mem_use = rss/2**20

    return mem_use

def _get_txt_load_params(text: str, header: list, n_rounds: int, engine: str = None) -> tuple:
    """
    Returns processing time and memory usage while parsing a buffered text using a specific engine.

    Params:
        text: Text to parse.
        header: List of field names of the resulting data table.
        n_rounds: Number of test runs to perform.
        engine: Name of the used engine. If no engine name is provided, then the pandas default parsing
                engine will be used.

    Returns: A tuple of average parsing time, standard deviation of parsing times,
             average memory usage, standard deviation of memory usage.
    """

    times = []
    mem_usage = []

    for i in range(1, n_rounds + 1):
        print(f"   Round # {i} running ...")
        start = dt.datetime.now()
        _ = pd.read_csv(StringIO(text),
            engine = engine,
            sep = '|',
            low_memory = True, # considering that app-azr-rob has only 8GB RAM leave this on for cross-engine coparison
            names = header,
            dtype = {
                "Assignment": "string",
                "Text": "string",
                "Tax_Code": "string",
                "Business_Area": "string"
            }
        )
        elapsed = dt.datetime.now() - start
        times.append(elapsed.total_seconds())
        mem_usage.append(get_ram_usage())

    avg_time = np.average(times).round(2)
    sd_time = np.std(times).round(2)

    avg_ram = np.average(mem_usage).round(2)
    sd_ram = np.std(mem_usage).round(2)
    max_ram = np.max(mem_usage).round(2)

    _write(f"  Average load time: {avg_time} ± {sd_time} sec")
    _write(f"  Average RAM used: {avg_ram} ± {sd_ram} MB")
    _write(f"  Peak RAM used: {max_ram} MB")

    return (avg_time, sd_time, avg_ram, sd_ram, max_ram)

def _preprocess_text(txt: str, patt: str) -> str:
    """
    Extracts relevant data lines
    representing accounting items
    from a text.
    """

    # get all data lines containing accounting items
    matches = re.findall(patt, txt, re.M)
    replaced = list(map(lambda x: x[1:-1], matches))
    preproc = "\n".join(replaced)

    return preproc

def benchmark_txt_loading(f_path: str, n_rounds: int):
    """
    Runs performance benchmark testing load time and memomry
    usage while reading .txt files.

    Params:
        f_path: Path to a sample *.txt file.
        n_rounds: Number of tests to perform.

    Returns: None.
    """

    header = [
        "Fiscal_Year", "Period", "GL_Account",
        "Assignment", "Document_Number",
        "Business_Area", "Document_Type",
        "Document_Date", "Posting_Date",
        "Posting_Key", "LC_Amount", "Tax_Code",
        "Clearing_Document", "Text"
    ]

    _write("Benchmark # 2: Data loading from a *.txt file using pandas.read_csv()")
    with open(f_path, 'r', encoding = "UTF-8") as t_file:
        txt = t_file.read()
        prep = _preprocess_text(txt, patt = r"^\|\s+\d{4}\|.*$")

    for i, eng in enumerate(("pyarrow", None, "c", "python"), start = 1):
        _write(f" Test # {i}: Engine = '{eng}'")
        _get_txt_load_params(prep, header, n_rounds, eng)

    return

def benchmark_dat_loading(f_path: str, n_rounds: int) -> tuple:
    """
    Runs performance benchmark testing load time and memomry
    usage while reading .dat files.

    Params:
        f_path: Path to a sample .dat file.
        n_rounds: Number of tests to perform.

    Returns: None.
    """

    _write("Benchmark # 1: Data loading from a *.dat file using pandas.read_table()")

    times = []
    mem_usage = []

    for mem_map in (True, False):

        if mem_map:
            _write(" Test # 1: Memory map used")
        else:
            _write(" Test # 2: Memory map unused")

        for i in range(1, n_rounds + 1):
            print(f"   Round # {i} running ...")
            start = dt.datetime.now()
            _ = pd.read_table(f_path,
                low_memory = False,
                header = 0, # first data line
                memory_map = mem_map,
                dtype = {
                    "Assignment": "string",
                    "Text": "string",
                    "Tx": "string",
                    "BusA": "string"
                }
            )
            elapsed = dt.datetime.now() - start
            times.append(elapsed.total_seconds())
            mem_usage.append(get_ram_usage())

        avg_time = np.average(times).round(2)
        sd_time = np.std(times).round(2)

        avg_ram = np.average(mem_usage).round(2)
        sd_ram = np.std(mem_usage).round(2)
        max_ram = np.max(mem_usage).round(2)

        _write(f"  Average load time: {avg_time} ± {sd_time} sec")
        _write(f"  Average RAM used: {avg_ram} ± {sd_ram} MB")
        _write(f"  Peak RAM used: {max_ram} MB")

    return (avg_time, sd_time, avg_ram, sd_ram, max_ram)

def benchmark_feather_loading(f_path: str, n_rounds: int):
    """
    Runs performance benchmark testing load time and memomry
    usage while reading *.feather files.

    Params:
        f_path: Path to a sample *.feather file.
        n_rounds: Number of tests to perform.

    Returns: None.
    """

    _write("Benchmark # 3: Data loading from a *.feather file using pandas.read_feather()")

    for thr in (True, False):

        if thr:
            _write(" Test # 1: Threads on")
        else:
            _write(" Test # 2: Threads off")

        times = []
        mem_usage = []

        for i in range(1, n_rounds + 1):
            print(f"   Round # {i} running ...")
            start = dt.datetime.now()
            _ = pd.read_feather(f_path, use_threads = thr)
            elapsed = dt.datetime.now() - start
            times.append(elapsed.total_seconds())
            mem_usage.append(get_ram_usage())

        avg_time = np.average(times).round(2)
        sd_time = np.std(times).round(2)

        avg_ram = np.average(mem_usage).round(2)
        sd_ram = np.std(mem_usage).round(2)
        max_ram = np.max(mem_usage).round(2)

        _write(f"  Average load time: {avg_time} ± {sd_time} sec")
        _write(f"  Average RAM used: {avg_ram} ± {sd_ram} MB")
        _write(f"  Peak RAM used: {max_ram} MB")

    return
