from typing import Union

import gen_thrift.common.ttypes as common


def _days(length: int) -> common.Window:
    return common.Window(length=length, timeUnit=common.TimeUnit.DAYS)


def _hours(length: int) -> common.Window:
    return common.Window(length=length, timeUnit=common.TimeUnit.HOURS)


def _minutes(length: int) -> common.Window:
    return common.Window(length=length, timeUnit=common.TimeUnit.MINUTES)


def _from_str(s: str) -> common.Window:
    """
    converts strings like "30d", "2h", "15m" etc into common.Window

    Args:
        s (str): Duration string in format "<number>(d|h|m)" where d=days, h=hours, m=minutes

    Returns:
        common.Window: Window object with specified duration

    Raises:
        ValueError: If string format is invalid
    """

    if not s or len(s) < 2:
        raise ValueError(f"Invalid duration format: {s}")

    # Get the numeric value and unit
    value = s[:-1]
    unit = s[-1].lower()

    try:
        length = int(value)
        if length <= 0:
            raise ValueError(f"Duration must be positive: {s}")

        if unit == "d":
            return _days(length)
        elif unit == "h":
            return _hours(length)
        elif unit == "m":
            return _minutes(length)
        else:
            raise ValueError(f"Invalid time unit '{unit}'. Must be 'd' for days, 'h' for hours, or 'm' for minutes")

    except ValueError as e:
        if "invalid literal for int()" in str(e):
            raise ValueError(f"Invalid numeric value in duration: {value}") from e
        raise e from None


def normalize_window(w: Union[common.Window, str]) -> common.Window:
    """
    Normalizes a window specification to a common.Window object.

    Accepts either a Window object directly or a string like "30d", "24h", or "15m".
    This is used across the codebase (e.g., in GroupBy aggregations and TrainingSpec).

    Args:
        w: Either a common.Window object or a string like "7d", "24h", "15m"

    Returns:
        common.Window: The normalized window object

    Raises:
        TypeError: If the input is neither a string nor a Window object
    """
    if isinstance(w, str):
        return _from_str(w)
    elif isinstance(w, common.Window):
        return w
    else:
        raise TypeError(
            f"Window should be either a string like '7d', '24h', '15m', or a Window type, "
            f"got {type(w).__name__}"
        )


DAY_MILLIS = 24 * 60 * 60 * 1000
HOUR_MILLIS = 60 * 60 * 1000
MINUTE_MILLIS = 60 * 1000
DAILY_PARTITION_FORMAT = "yyyy-MM-dd"
SUB_DAILY_PARTITION_FORMAT = "yyyy-MM-dd HH:mm"


def window_millis(w: Union[common.Window, str]) -> int:
    window = normalize_window(w)
    if window.timeUnit == common.TimeUnit.DAYS:
        return window.length * DAY_MILLIS
    if window.timeUnit == common.TimeUnit.HOURS:
        return window.length * HOUR_MILLIS
    if window.timeUnit == common.TimeUnit.MINUTES:
        return window.length * MINUTE_MILLIS
    raise ValueError(f"Unsupported TimeUnit for partition interval: {window.timeUnit}")


def default_partition_format(partition_interval: Union[common.Window, str]) -> str:
    return SUB_DAILY_PARTITION_FORMAT if window_millis(partition_interval) < DAY_MILLIS else DAILY_PARTITION_FORMAT


def output_table_info(
    partition_interval: Union[common.Window, str] = None,
    partition_column: str = "ds",
    partition_format: str = None,
) -> common.TableInfo:
    if partition_interval is None:
        return None
    interval = normalize_window(partition_interval)
    return common.TableInfo(
        partitionColumn=partition_column,
        partitionFormat=partition_format or default_partition_format(interval),
        partitionInterval=interval,
    )
