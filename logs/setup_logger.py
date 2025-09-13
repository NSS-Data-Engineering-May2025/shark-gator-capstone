import logging
import os

def setup_logger(folder_name, log_file_name):
    # configure logging
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    log_directory = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs', folder_name)
    os.makedirs(log_directory, exist_ok=True)  # Create directory if it doesn't exist
    log_file = os.path.join(log_directory, f'{log_file_name}.log')

    # Create file handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger