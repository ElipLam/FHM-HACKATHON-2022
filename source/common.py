from datetime import datetime
import pandas as pd
from pathlib import Path
import requests
import glob
import json
from progress.bar import IncrementalBar
import time

ROOT = Path(__file__).resolve().parents[1]
BASE_PATH = Path(__file__).resolve().parents[0]
IMAGES_FOLDER = "images"
CRAWLED_DATA_FOLDER = "crawled_data"
IAC_FOLDER = "IaC"
LAKE_FOLDER = "lake"
NEW_LAKE_FOLDER = "new_lake"
SOURCE_FOLDER = "source"


def get_filename_extension(file_path: str):
    """return name and extension of file."""

    result = file_path.split("\\")[-1]
    result = result.split(".")
    file_name = result[0]
    extension = result[-1]
    return file_name, extension


def extract_timestamp(timestamp):
    """extract timestamp to year, month, day."""

    dt_object = datetime.fromtimestamp(timestamp)
    year = dt_object.year
    month = dt_object.month
    day = dt_object.day
    return (year, month, day)


def change_value_downloaded(file_path, field_name, id, value):
    filename, extension = get_filename_extension(file_path)
    if extension == "parquet":
        df = pd.read_parquet(file_path)
        df.loc[df[field_name] == id, "downloaded"] = value
        df.to_parquet(file_path, index=False)

    if extension == "csv":
        df = pd.read_csv(file_path)
        df.loc[df[field_name] == id, "downloaded"] = value
        df.to_csv(file_path, index=False)


def change_all_values_downloaded(file_path, value):
    filename, extension = get_filename_extension(file_path)
    if extension == "parquet":
        df = pd.read_parquet(file_path)
        df["downloaded"] = value
        df.to_parquet(file_path, index=False)

    if extension == "csv":
        df = pd.read_csv(file_path)
        df["downloaded"] = value
        df.to_csv(file_path, index=False)


def crawl_data(url, output_path, interval=1):
    download_start_time = time.time()
    response = requests.get(url, headers=None)
    with open(output_path, "wb") as f:
        f.write(response.content)
    download_end_time = time.time()
    elapsed_time = download_end_time - download_start_time
    if elapsed_time < interval:
        time.sleep(interval - elapsed_time)


def change_multi_rows_values_downloaded(
    # def reset_download_error_files(
    check_file,
    list_files: list,
    field_name,
    value,
    message="Reseting check downloaded of error files...",
):
    if list_files != []:
        bar = IncrementalBar(
            message,
            max=len(list_files),
            suffix="%(index)d/%(max)d | %(elapsed_td)s",
        )
        errors_id = [int(get_filename_extension(err)[0]) for err in list_files]
        # print(list_files)
        out_put = check_file
        print(out_put)
        check_download_df = pd.read_parquet(out_put)
        for err_id in errors_id:
            check_download_df.loc[
                check_download_df[field_name] == err_id, "downloaded"
            ] = value
            bar.next()
        check_download_df.to_parquet(out_put, index=False)
        bar.finish()
    pass


def json_to_dataframe(input_path, columns, message):
    """Return a pandas dataframe and list of error file path."""
    error_files = []
    data_dict = {col: [] for col in columns}
    index = []
    path_list = glob.glob(input_path)
    bar = IncrementalBar(
        message,
        max=len(path_list),
        suffix="%(index)d/%(max)d | %(elapsed_td)s",
    )
    for json_path in path_list:
        try:
            idx, _ = get_filename_extension(json_path)
            with open(json_path, "rb") as f:
                data = json.load(f)
                for col in columns:
                    data_dict[col].append(data[col])
                index.append(idx)
        except Exception as e:
            error_files.append(json_path)
        finally:
            bar.next()
    df = pd.DataFrame(data_dict, index=index)
    if error_files == []:
        flag = True
    else:
        flag = False
    return flag, df, error_files


if __name__ == "__main__":
    print("You are in", BASE_PATH)
    pass
