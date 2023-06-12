import os

from b3_series.pandas import load_compressed_series


def test_load_compressed_series(request):
    filename = request.module.__file__
    test_dir, _ = os.path.splitext(filename)
    serie_path = "{}/{}".format(test_dir, "sample.zip")

    df = load_compressed_series(serie_path)
    assert df.shape == (3, 26)
    assert df["sigla_acao"][0] == "GEPA3"
    assert df["sigla_acao"][1] == "GEPA4"
    assert df["sigla_acao"][2] == "GFSA3"
