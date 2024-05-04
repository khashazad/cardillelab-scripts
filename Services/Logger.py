import logging


class Logger:
    infoLogger = None
    errorLogger = None

    def _setup_info_logger(self, filename, format=True):
        self.infoLogger = logging.getLogger("info_logger")
        self.infoLogger.setLevel(logging.INFO)

        info_file_handler = logging.FileHandler(filename)
        info_file_handler.setLevel(logging.INFO)

        if format is True:
            formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

            info_file_handler.setFormatter(formatter)

        self.infoLogger.addHandler(info_file_handler)

    def _setup_error_logger(self, filename, format=True):
        self.errorLogger = logging.getLogger("error_logger")
        self.errorLogger.setLevel(logging.ERROR)

        error_file_handler = logging.FileHandler(filename)
        error_file_handler.setLevel(logging.ERROR)

        if format is True:
            formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

            error_file_handler.setFormatter(formatter)

        self.errorLogger.addHandler(error_file_handler)

    def __init__(
        self, info_filename="info.csv", error_filename="error.csv", format=True
    ):
        self._setup_info_logger(info_filename, format)
        self._setup_error_logger(error_filename, format)

    def log_info(self, message):
        if self.infoLogger is not None:
            self.infoLogger.info(message)

    def log_error(self, message):
        if self.errorLogger is not None:
            self.errorLogger.error(message)
