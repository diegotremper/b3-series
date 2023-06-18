from time import perf_counter, time

from b3_series.config import Config
from b3_series.io import get_absolute_series_path, list_series
from b3_series.pandas import load_compressed_series


def _replace_zip_name_with_parquet(zip_name: str):
    import re

    res = re.sub("(?i)" + re.escape("zip"), lambda m: "parquet", zip_name)
    return str(res)


def _convert_zip_to_parquet(zip_name: str, config=Config()) -> str:

    print(f"Converting {zip_name} to parquet...")
    start_time = perf_counter()

    parquet_name = _replace_zip_name_with_parquet(zip_name)
    zip_path = get_absolute_series_path(zip_name, config)
    parquet_path = get_absolute_series_path(parquet_name, config)

    df = load_compressed_series(zip_path)

    df.to_parquet(parquet_path, compression="gzip")

    end_time = perf_counter()
    print(f"Converted {zip_name} to parquet. Time elapsed: {end_time - start_time}")

    return parquet_path


def _convert_parquets(
    zip_names: list[str], timeout_in_minutes=4, config=Config()
) -> list[str]:
    converted_parquets = []
    timeout = time() + 60 * timeout_in_minutes

    for zip_name in zip_names:
        current_time = time()
        if current_time > timeout:
            print(
                f"Timeout of {timeout_in_minutes} minutes reached. Stopping conversion."
            )
            break

        parquet_name = _convert_zip_to_parquet(zip_name, config)
        converted_parquets.append(parquet_name)

    return converted_parquets


def sync_parquets(config=Config()):
    existing_series = list_series(config)

    # filter existing_series to only for zip files
    zip_series = [serie for serie in existing_series if serie.lower().endswith(".zip")]

    # for each zip file, check if the parquet file exists
    missing_parquets = []
    for zip_serie in zip_series:
        parquet_serie = _replace_zip_name_with_parquet(zip_serie)
        if parquet_serie not in existing_series:
            missing_parquets.append(zip_serie)

    # for each missing parquet file, load the zip file and convert it to parquet
    converted_parquets = _convert_parquets(missing_parquets, 3, config)

    # print the results
    print(f"Converted {len(converted_parquets)} parquets.")
    print(f"{len(missing_parquets) - len(converted_parquets)} remaining.")
