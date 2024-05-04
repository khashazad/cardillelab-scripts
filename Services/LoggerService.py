import logging


class LoggerService:
    infoLogger = None
    errorLogger = None

    @staticmethod
    def create_logger():
        if LoggerService.infoLogger is None:
            # Create an info logger
            LoggerService.infoLogger = logging.getLogger("info_logger")
            LoggerService.infoLogger.setLevel(logging.INFO)

            # Create an error logger
            LoggerService.errorLogger = logging.getLogger("error_logger")
            LoggerService.errorLogger.setLevel(logging.ERROR)

            # Create a file handler for info logs
            info_file_handler = logging.FileHandler("Logs/info.log")
            info_file_handler.setLevel(logging.INFO)

            # Create a file handler for error logs
            error_file_handler = logging.FileHandler("Logs/error.log")
            error_file_handler.setLevel(logging.ERROR)

            # Create a formatter
            formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

            # Add the formatter to the file handlers
            info_file_handler.setFormatter(formatter)
            error_file_handler.setFormatter(formatter)

            # Add the file handlers to the loggers
            LoggerService.infoLogger.addHandler(info_file_handler)
            LoggerService.errorLogger.addHandler(error_file_handler)

    @staticmethod
    def log_info(message):
        LoggerService.create_logger()
        if LoggerService.infoLogger:
            LoggerService.infoLogger.info(message)

    @staticmethod
    def log_error(message):
        LoggerService.create_logger()
        if LoggerService.errorLogger:
            LoggerService.errorLogger.error(message)
