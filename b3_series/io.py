from pathlib import Path
from time import perf_counter

from b3_series.config import Config


def get_absolute_series_path(serie: str, config=Config()) -> str:
    return f"{config.fs_path}/{serie}"


def list_series(config=Config()) -> list[str]:
    tmp_folder = Path(config.fs_path)
    tmp_folder.exists() or tmp_folder.mkdir()

    existing_files = [file.name for file in tmp_folder.iterdir()]
    return existing_files


def remove_serie(serie: str, config=Config()):
    path = get_absolute_series_path(serie, config)
    Path(path).unlink()


def save_serie(serie: str, content: bytes, config=Config()):
    print(f"Saving serie {serie}...")
    start_time = perf_counter()

    with open(get_absolute_series_path(serie, config), "wb") as f:
        f.write(content)

    end_time = perf_counter()
    print(f"Saved serie {serie}. Time elapsed: {end_time - start_time}")
