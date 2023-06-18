import os

from b3_series.config import Config
from b3_series.sync_parquets import (
    _convert_zip_to_parquet,
    _replace_zip_name_with_parquet,
    sync_parquets,
)


def test_replace_zip_name_with_parquet():
    assert (
        _replace_zip_name_with_parquet("COTAHIST_A2000.ZIP") == "COTAHIST_A2000.parquet"
    )
    assert (
        _replace_zip_name_with_parquet("COTAHIST_A2000.zip") == "COTAHIST_A2000.parquet"
    )
    assert (
        _replace_zip_name_with_parquet("COTAHIST_A2000.ZiP") == "COTAHIST_A2000.parquet"
    )
    assert (
        _replace_zip_name_with_parquet("COTAHIST_A2000.zip") == "COTAHIST_A2000.parquet"
    )


def test_convert_zip_to_parquet(request):
    filename = request.module.__file__
    test_dir, _ = os.path.splitext(filename)

    path = _convert_zip_to_parquet("COTAHIST_A2000.ZIP", Config(fs_path=test_dir))

    assert path == "{}/{}".format(test_dir, "COTAHIST_A2000.parquet")
    assert os.path.exists(path)
    os.unlink(path)


def test_sync_parquets(request, mocker):
    filename = request.module.__file__
    test_dir, _ = os.path.splitext(filename)

    sync_parquets(Config(fs_path=test_dir))

    out = "{}/{}".format(test_dir, "COTAHIST_A2000.parquet")
    assert os.path.exists(out)
    os.unlink(out)
