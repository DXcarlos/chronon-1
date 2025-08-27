import logging
import sys
from datetime import datetime

TIME_COLOR = "\033[36m"  # Cyan
LEVEL_COLORS = {
    logging.DEBUG: "\033[36m",  # Cyan
    logging.INFO: "\033[32m",  # Green
    logging.WARNING: "\033[33m",  # Yellow
    logging.ERROR: "\033[31m",  # Red
    logging.CRITICAL: "\033[41m",  # White on Red
}
FILE_COLOR = "\033[35m"  # Purple
RESET = "\033[0m"


class ColorFormatter(logging.Formatter):
    def format(self, record):
        time_str = datetime.fromtimestamp(record.created).strftime("%H:%M:%S")
        level_color = LEVEL_COLORS.get(record.levelno)

        return (
            f"{TIME_COLOR}{time_str}{RESET} "
            f"{level_color}{record.levelname}{RESET} "
            f"{FILE_COLOR}{record.filename}:{record.lineno}{RESET} - "
            f"{record.getMessage()}"
        )


def get_logger(log_level=logging.INFO):
    logger = logging.getLogger(__name__)

    # no need to reset if a handler already exists
    if not logger.hasHandlers():
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(ColorFormatter())

        logger.addHandler(handler)
        logger.setLevel(log_level)

    return logger


def red(text):
    return f"\033[1;91m{text}\033[0m"


def green(text):
    return f"\033[1;92m{text}\033[0m"


def require(cond, message):
    if not cond:
        print(f"X: {message}")
        sys.exit(1)


def done(cond, message):
    print(f"DONE: {message}")
