import ai.chronon.api.common.ttypes as common


def _days(length: int) -> common.Window:
    return common.Window(length=length, timeUnit=common.TimeUnit.DAYS)


def _hours(length: int) -> common.Window:
    return common.Window(length=length, timeUnit=common.TimeUnit.HOURS)


def _from_str(s: str) -> common.Window:
    """
    converts strings like "30d", "2h" etc into common.Window

    Args:
        s (str): Duration string in format "<number>(d|h)" where d=days, h=hours

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
        else:
            raise ValueError(
                f"Invalid time unit '{unit}'. Must be 'd' for days or 'h' for hours"
            )

    except ValueError as e:
        if "invalid literal for int()" in str(e):
            raise ValueError(f"Invalid numeric value in duration: {value}") from e
        raise e from None
