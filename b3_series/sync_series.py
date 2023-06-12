from datetime import datetime
from time import perf_counter, time

from b3_api.historical_series_available import DataSeries, historical_series_available
from b3_api.historical_series_download import historical_series_download

from b3_series.config import Config
from b3_series.io import list_series, remove_serie, save_serie


def _find_missing_annual_series(existing_files: list[str]) -> list[DataSeries]:
    current_year = datetime.now().year

    available_series = historical_series_available()

    return [
        serie
        for serie in available_series.annual_series
        if serie.file_name not in existing_files and int(serie.name) < current_year
    ]


def _find_missing_monthly_series(existing_files: list[str]) -> list[DataSeries]:
    current_year = str(datetime.now().year)
    current_month = str(datetime.now().month).zfill(2)
    current_month_file_name = f"COTAHIST_M{current_month}{current_year}.ZIP"

    available_series = historical_series_available()

    def file_not_exists(x):
        return x.file_name not in existing_files

    def is_current_month(x):
        return current_month_file_name != x.file_name

    def is_current_year(x):
        return current_year in x.file_name

    def accept(x):
        return file_not_exists(x) and is_current_month(x) and is_current_year(x)

    return [serie for serie in available_series.monthly_series if accept(serie)]


def _find_missing_daily_series(existing_files: list[str]) -> list[DataSeries]:
    current_month = f"{str(datetime.now().month).zfill(2)}{str(datetime.now().year)}"
    available_series = historical_series_available()

    return [
        serie
        for serie in available_series.daily_series
        if serie.file_name not in existing_files and current_month in serie.file_name
    ]


def _download_serie_content(serie: str) -> bytes:
    print(f"Downloading serie {serie}...")
    start_time = perf_counter()

    content = historical_series_download(filename=serie)

    end_time = perf_counter()
    print(f"Downloaded serie {serie}. Time elapsed: {end_time - start_time}")

    return content


def _download_series(series: list[str], timeout_in_minutes: int = 4) -> list[str]:
    """
    Download the data series.

    Args:
        series (list[str]): A list of data series to download.
        timeout_in_minutes (int, optional): Timeout duration in minutes. Defaults to 4.

    Returns:
        list[str]: A list of completed data series.

    The function downloads the data series one by one. It sets a timeout
    based on the provided duration (default is 4 minutes) and gracefully stops
    downloading if the timeout is reached. The function keeps track of the completed
    downloads and returns it.
    """
    completed = []
    timeout = time() + 60 * timeout_in_minutes

    # download the missing series one by one
    for serie in series:
        current_time = time()
        if current_time > timeout:
            print(f"Timeout of {timeout_in_minutes} minutes reached. Stopping.")
            break

        content = _download_serie_content(serie)
        save_serie(serie, content)
        completed.append(serie)

    return completed


def _cleanup_duplicated_data():
    existing_files = list_series()
    current_year = str(datetime.now().year)
    current_month = str(datetime.now().month).zfill(2)

    import re

    general_pattern = re.compile(r"COTAHIST_[D|M]\d+", re.IGNORECASE)
    daily_pattern = re.compile(
        r"COTAHIST_D\d{2}" + current_month + current_year, re.IGNORECASE
    )
    month_pattern = re.compile(r"COTAHIST_M\d{2}" + current_year, re.IGNORECASE)

    for file in existing_files:
        if general_pattern.match(file):
            if file.upper().startswith("COTAHIST_M") and not month_pattern.match(file):
                print(f"Removing file {file}...")
                remove_serie(file)
            if file.upper().startswith("COTAHIST_D") and not daily_pattern.match(file):
                print(f"Removing file {file}...")
                remove_serie(file)


def series_sync(config=Config()):
    existing_files = list_series(config)
    missing_series = []

    missing_series += _find_missing_annual_series(existing_files)
    missing_series += _find_missing_monthly_series(existing_files)
    missing_series += _find_missing_daily_series(existing_files)

    # convert to a list of file names
    missing_series = [serie.file_name for serie in missing_series]

    # download the missing series
    completed = _download_series(missing_series)

    # print the results
    print(f"Downloaded {len(completed)} series.")
    print(f"Missing {len(missing_series) - len(completed)} series.")

    _cleanup_duplicated_data()

    return len(missing_series)
