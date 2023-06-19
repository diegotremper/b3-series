from pathlib import Path
from time import perf_counter

from b3_series.config import Config


def get_absolute_series_path(series: str, config=Config()) -> str:
    return f"{config.fs_path}/{series}"


def list_series(format=None, config=Config()) -> list[str]:
    tmp_folder = Path(config.fs_path)
    tmp_folder.exists() or tmp_folder.mkdir()

    existing_files = [file.name for file in tmp_folder.iterdir()]

    if (format is not None) and (format != ""):
        existing_files = [
            file for file in existing_files if file.lower().endswith(format.lower())
        ]

    return existing_files


def remove_series(series: str, config=Config()):
    path = get_absolute_series_path(series, config)
    Path(path).unlink()


def save_series(series: str, content: bytes, config=Config()):
    print(f"Saving series {series}...")
    start_time = perf_counter()

    with open(get_absolute_series_path(series, config), "wb") as f:
        f.write(content)

    end_time = perf_counter()
    print(f"Saved series {series}. Time elapsed: {end_time - start_time}")
