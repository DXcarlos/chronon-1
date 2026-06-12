import datetime
from typing import Optional, Union

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


def regular_subdaily_schedule(schedule_expression: str, partition_offset_ms: int = 0) -> Optional[int]:
    """Validate a cron expression and return its data interval in millis for regular sub-daily
    schedules, or None for daily-or-coarser (or absent) schedules.

    Grids are declared, never inferred from the cron: the partition grid is midnight-aligned
    unless an explicit ``partition_offset`` shifts it. The cron fire phase only contributes a
    derived processing delay relative to that declared grid, which must be constant across a
    7 day UTC horizon and strictly less than the partition interval. Day-restricted or
    irregular crons are rejected.
    """
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

    validate_cron_delay(runs, interval_ms, partition_offset_ms)
    return interval_ms


def validate_cron_delay(
    runs, partition_interval_ms: int, partition_offset_ms: int = 0
) -> int:
    """Validate that all cron fire times sit at a constant delay over the declared partition
    grid (``partition_interval_ms`` phased by ``partition_offset_ms``) and return that delay.

    The delay is execution-domain only — it never moves the grid. A fire at 09:20 over a
    midnight-aligned 3h grid is the 09:00 boundary plus a 20 minute delay.
    """
    epoch = datetime.datetime(1970, 1, 1, 0, 0)
    delays = {
        (int((run - epoch).total_seconds() * 1000) - partition_offset_ms) % partition_interval_ms
        for run in runs
    }
    if len(delays) != 1:
        raise ValueError(
            "Sub-daily schedule fire times must sit at a constant delay over the declared "
            f"partition grid (interval {partition_interval_ms}ms, offset {partition_offset_ms}ms); "
            "declare a matching partition_offset or use a regular cron."
        )
    delay_ms = delays.pop()
    if delay_ms >= partition_interval_ms:
        raise ValueError(
            "Derived cron delay must be strictly less than the partition interval, found "
            f"{delay_ms}ms >= {partition_interval_ms}ms."
        )
    return delay_ms


def is_subdaily(
    partition_interval: Union[common.Window, str] = None, schedule: str = None
) -> bool:
    """True when the output grain is sub-daily — either via an explicit partition interval
    below one day or a regular sub-daily schedule it would be inferred from."""
    if partition_interval is not None:
        return window_millis(partition_interval) < DAY_MILLIS
    if schedule:
        return regular_subdaily_schedule(schedule) is not None
    return False


def output_table_info(
    partition_interval: Union[common.Window, str] = None,
    partition_offset: Union[common.Window, str] = None,
    partition_column: str = "ds",
    partition_format: str = None,
    schedule: str = None,
) -> common.TableInfo:
    # The offset is never inferred from the cron fire phase; it defaults to zero (midnight-
    # aligned grid) and only an explicit partition_offset moves the grid. The cron fire phase
    # is treated as a derived processing delay relative to the declared grid.
    offset_ms = window_millis(partition_offset) if partition_offset is not None else 0
    cron_interval_ms = regular_subdaily_schedule(schedule, offset_ms) if schedule else None
    if partition_interval is None and cron_interval_ms is None:
        if partition_offset is not None:
            raise ValueError(
                "partition_offset requires a partition_interval or a regular sub-daily schedule."
            )
        return None
    interval = (
        normalize_window(partition_interval)
        if partition_interval is not None
        else from_millis(cron_interval_ms)
    )
    interval_ms = window_millis(interval)
    if cron_interval_ms is not None and cron_interval_ms % interval_ms != 0:
        raise ValueError(
            f"partition_interval ({interval_ms}ms) must evenly divide the cron data interval "
            f"({cron_interval_ms}ms) of schedule '{schedule}'."
        )
    if offset_ms >= interval_ms:
        raise ValueError(
            f"partition_offset ({offset_ms}ms) must be strictly less than the partition "
            f"interval ({interval_ms}ms)."
        )
    offset = normalize_window(partition_offset) if partition_offset is not None else None
    return common.TableInfo(
        partitionColumn=partition_column,
        partitionFormat=partition_format or default_partition_format(interval),
        partitionInterval=interval,
        partitionOffset=offset,
    )
