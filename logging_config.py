import logging
import colorlog

def setup_logger(name):
    # Create a logger object
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # Create a console handler
    handler = colorlog.StreamHandler()
    handler.setLevel(logging.DEBUG)

    # Create a formatter that includes colors
    formatter = colorlog.ColoredFormatter(
        "%(log_color)s%(message)s",
        log_colors={
            'DEBUG': 'white',
            'INFO': 'green',
            'ERROR': 'red',
        }
    )

    # Add the formatter to the handler
    handler.setFormatter(formatter)

    # Add the handler to the logger
    if not logger.handlers:
        logger.addHandler(handler)

    return logger