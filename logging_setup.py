import logging

def setup_logging(level=logging.INFO):
    # Create a custom logger
    logger = logging.getLogger()
    logger.setLevel(level)

    # Create handlers
    console_handler = logging.StreamHandler()
    file_handler = logging.FileHandler('execution.log')
    
    # Set the log level for handlers
    console_handler.setLevel(level)
    file_handler.setLevel(level)
    
    # Create formatters and add them to handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)
    
    # Add handlers to the logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    return logger
