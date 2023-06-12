from datetime import datetime

from b3_api.historical_series_available import AvailableSeries, DataSeries, SerieType

from b3_series.sync_series import (
    _find_missing_annual_series,
    _find_missing_daily_series,
    _find_missing_monthly_series,
)


def test_find_missing_annual_series(mocker):
    sample = AvailableSeries(
        annual_series=[
            DataSeries(
                name="2000", type=SerieType.ANNUAL, file_name="COTAHIST_A2000.ZIP"
            )
        ],
    )

    mocker.patch(
        "b3_series.sync_series.historical_series_available", return_value=sample
    )

    missing_annual_series = _find_missing_annual_series([])
    assert len(missing_annual_series) == 1
    assert missing_annual_series[0].name == "2000"
    assert missing_annual_series[0].type == SerieType.ANNUAL
    assert missing_annual_series[0].file_name == "COTAHIST_A2000.ZIP"

    missing_annual_series = _find_missing_annual_series(["COTAHIST_A2000.ZIP"])
    assert len(missing_annual_series) == 0


def test_find_missing_monthly_series(mocker):
    sample = AvailableSeries(
        monthly_series=[
            DataSeries(
                name="Jan/2023",
                type=SerieType.MONTHLY,
                file_name="COTAHIST_M012023.ZIP",
            )
        ],
    )

    mocker.patch(
        "b3_series.sync_series.historical_series_available", return_value=sample
    )

    missing_monthly_series = _find_missing_monthly_series([])
    assert len(missing_monthly_series) == 1
    assert missing_monthly_series[0].name == "Jan/2023"
    assert missing_monthly_series[0].type == SerieType.MONTHLY
    assert missing_monthly_series[0].file_name == "COTAHIST_M012023.ZIP"

    missing_monthly_series = _find_missing_monthly_series(["COTAHIST_M012023.ZIP"])
    assert len(missing_monthly_series) == 0


def test_find_missing_daily_series(mocker):
    current_month = str(datetime.now().month).zfill(2)
    current_year = str(datetime.now().year)

    sample_filename = f"COTAHIST_D01{current_month}{current_year}.ZIP"

    sample = AvailableSeries(
        daily_series=[
            DataSeries(
                name=f"01/{current_month}/{current_year}",
                type=SerieType.DAILY,
                file_name=sample_filename,
            )
        ],
    )

    mocker.patch(
        "b3_series.sync_series.historical_series_available", return_value=sample
    )

    missing_daily_series = _find_missing_daily_series([])
    assert len(missing_daily_series) == 1
    assert missing_daily_series[0].name == f"01/{current_month}/2023"
    assert missing_daily_series[0].type == SerieType.DAILY
    assert missing_daily_series[0].file_name == sample_filename

    missing_daily_series = _find_missing_daily_series([sample_filename])
    assert len(missing_daily_series) == 0
