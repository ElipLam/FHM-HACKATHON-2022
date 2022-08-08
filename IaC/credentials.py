from configparser import ConfigParser
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BASE_PATH = Path(__file__).resolve().parents[0]
IAC_FILE_PATH = str(Path(BASE_PATH, "credentials.ini"))


def get_credentials(file_name, section):
    config = ConfigParser()
    config.read(file_name)
    dct = {}
    if config.has_section(section):
        for (v, k) in config.items(section):
            dct[v] = k
    return dct


# aws_section = "aws.amazon.com"
# print(get_credentials(IAC_FILE_PATH, aws_section))
