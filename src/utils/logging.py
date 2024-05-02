import logging
from logging import Logger  # Import the Logger class directly from logging
from datetime import datetime
import os


log_dir = './logs'
current_date = datetime.now().strftime('%Y-%m-%d')
log_file = os.path.join(log_dir, f'{current_date}.txt')

if not os.path.isfile(log_file):
    with open(log_file, 'w') as f:
        f.write('Log file created\n')

def setup_logging() -> None:
    """Configure logging to write messages to a file named 'test.txt'.

    This function sets up the logging system to log messages with INFO level
    and above to the file 'test.txt' in append mode.

    Returns:
        None
    """
    logging.basicConfig(
        level=logging.INFO,
        filename=log_file,  # Specify the log file name
        filemode='a',         # Append to the log file if it already exists
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

def get_logger(name: str) -> Logger:
    """Get a logger instance with the specified name.

    This function retrieves a logger instance using the provided name. The logger
    will inherit the logging configuration set up by setup_logging().

    Args:
        name (str): The name of the logger.

    Returns:
        Logger: A logger instance configured to write to 'test.txt'.
    """
    setup_logging()
    logger: Logger = logging.getLogger(name)
    return logger
