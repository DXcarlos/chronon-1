import datetime
from typing import Optional, Tuple, Union

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
            raise ValueError(
                f"Invalid time unit '{unit}'. Must be 'd' for days, 'h' for hours, or 'm' for minutes"
            )

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


def from_millis(millis: int) -> common.Window:
    if millis % DAY_MILLIS == 0:
        return _days(millis // DAY_MILLIS)
    if millis % HOUR_MILLIS == 0:
        return _hours(millis // HOUR_MILLIS)
    if millis % MINUTE_MILLIS == 0:
        return _minutes(millis // MINUTE_MILLIS)
    raise ValueError(f"Window duration must be minute-aligned, found {millis}ms")


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
    return (
        SUB_DAILY_PARTITION_FORMAT
        if window_millis(partition_interval) < DAY_MILLIS
        else DAILY_PARTITION_FORMAT
    )


def regular_subdaily_schedule(schedule_expression: str) -> Optional[Tuple[int, int]]:
    """Return (interval_ms, offset_ms) for regular sub-daily schedules."""
    from croniter import croniter

    if not schedule_expression or schedule_expression.strip().lower() in (
        "",
        "none",
        "null",
        "@daily",
        "@never",
    ):
        return None

    schedule_expression = schedule_expression.strip()
    if schedule_expression.startswith("@"):
        raise ValueError(
            "Only @daily and @never aliases are supported; use a 5-field cron expression otherwise."
        )

    test_start = datetime.datetime(2024, 1, 1, 0, 0)
    horizon_end = test_start + datetime.timedelta(days=7)
    cron = croniter(schedule_expression, test_start - datetime.timedelta(seconds=1))
    runs = []
    for _ in range(2000):
        next_run = cron.get_next(datetime.datetime)
        if next_run >= horizon_end:
            break
        runs.append(next_run)

    if len(runs) < 2:
        return None

    max_executions_in_day = 0
    for day_offset in range(7):
        day_start = test_start + datetime.timedelta(days=day_offset)
        day_end = day_start + datetime.timedelta(days=1)
        executions_in_day = sum(day_start <= run < day_end for run in runs)
        max_executions_in_day = max(max_executions_in_day, executions_in_day)

    if max_executions_in_day <= 1:
        return None

    deltas = [int((runs[i] - runs[i - 1]).total_seconds() * 1000) for i in range(1, len(runs))]
    interval_ms = deltas[0]
    if any(delta != interval_ms for delta in deltas):
        raise ValueError(
            "Sub-daily schedules must have a constant interval across a 7 day UTC horizon."
        )
    if interval_ms <= 0 or interval_ms > DAY_MILLIS:
        raise ValueError("Sub-daily schedule interval must be between 1 minute and 1 day.")
    if interval_ms % MINUTE_MILLIS != 0:
        raise ValueError("Sub-daily schedule interval must be minute-aligned.")
    if DAY_MILLIS % interval_ms != 0:
        raise ValueError("Sub-daily schedule interval must divide a UTC day evenly.")

    epoch = datetime.datetime(1970, 1, 1, 0, 0)
    offsets = {int((run - epoch).total_seconds() * 1000) % interval_ms for run in runs}
    if len(offsets) != 1:
        raise ValueError(
            "Sub-daily schedule must have a constant offset across a 7 day UTC horizon."
        )
    return interval_ms, offsets.pop()


def output_table_info(
    partition_interval: Union[common.Window, str] = None,
    partition_offset: Union[common.Window, str] = None,
    partition_column: str = "ds",
    partition_format: str = None,
    schedule: str = None,
) -> common.TableInfo:
    inferred_schedule = regular_subdaily_schedule(schedule) if schedule else None
    if partition_interval is None and inferred_schedule is None:
        return None
    interval = (
        normalize_window(partition_interval)
        if partition_interval is not None
        else from_millis(inferred_schedule[0])
    )
    offset = (
        normalize_window(partition_offset)
        if partition_offset is not None
        else from_millis(inferred_schedule[1])
        if inferred_schedule is not None
        else None
    )
    return common.TableInfo(
        partitionColumn=partition_column,
        partitionFormat=partition_format or default_partition_format(interval),
        partitionInterval=interval,
        partitionOffset=offset,
    )
