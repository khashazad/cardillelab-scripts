import enum
import os

# constant
PATH_SCRIPT_DIRECTORY = os.path.dirname(os.path.realpath(__file__)) + "/.."
PATH_ASSETS_INSERT_DB = PATH_SCRIPT_DIRECTORY + r"/Assets/assetsToInsert.csv"
PATH_ASSETS_ALL = PATH_SCRIPT_DIRECTORY + r"/Assets/allAssets.csv"
PATH_ASSETS_FISHNET2 = PATH_SCRIPT_DIRECTORY + r"/Assets/fishnet2"
PATH_DOWNLOAD_FOLDER = PATH_SCRIPT_DIRECTORY + r"/Downloads"

# PATH_DBTEST_FOLDER = PATH_SCRIPT_DIRECTORY + r"/testDB"
# PATH_DB_Assets_FOLDER = r"/Volumes/Lakes Data Second Copy"
PATH_DB_Assets_FOLDER = r"F:/"


BUFFERS = [1, 10, 15, 20, 30, 40, 60, 90, 100, 125, 250]

# BUFFERS = [1]

QA_PIXEL_VALUES = [
    "1",
    "21824",
    "21826",
    "21888",
    "21890",
    "22080",
    "22144",
    "22280",
    "23888",
    "23952",
    "24088",
    "24216",
    "24344",
    "24472",
    "30048",
    "54596",
    "54852",
    "55052",
    "56856",
    "56984",
    "57240",
]


class Datasets(enum.Enum):
    LANDSAT8 = enum.auto()
    SENTINEL1 = enum.auto()


class Collections(enum.Enum):
    Collection1 = enum.auto()
    Collection2 = enum.auto()
    Collection3 = enum.auto()


class Operations(enum.Enum):
    DOWNLOAD = enum.auto()
    DELETE = enum.auto()
