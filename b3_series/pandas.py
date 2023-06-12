from datetime import datetime

import pandas as pd


def empty_dataframe() -> pd.DataFrame:
    return pd.DataFrame(columns=[column["name"] for column in dataframe_columns()])


def _parse_date(date: str) -> datetime:
    """
    Parse date from B3 file
    """
    if date != "00000000" and date != "99991231":
        return datetime.strptime(date, "%Y%m%d")
    else:
        return None


def load_compressed_serie(path: str) -> pd.DataFrame:
    names = []
    colspecs = []
    for column in dataframe_columns():
        names.append(column["name"])
        colspecs.append(column["tuple"])

    return pd.read_fwf(
        path,
        compression="zip",
        colspecs=colspecs,
        names=names,
        skiprows=1,
        decimal=",",
        thousands=".",
        skipfooter=1,
        converters={
            "codigo_negociacao": lambda x: x.strip(),
            "nome_resumido": lambda x: x.strip(),
            "especificacao_papel": lambda x: x.strip(),
            "indicador_correcao_preco": lambda x: x.strip(),
            "codigo_isin": lambda x: x.strip(),
            "data_pregao": _parse_date,
            "data_vencimento": _parse_date,
            "preco_abertura": lambda x: float(x) / 100,
            "preco_maximo": lambda x: float(x) / 100,
            "preco_minimo": lambda x: float(x) / 100,
            "preco_medio": lambda x: float(x) / 100,
            "preco_ultimo": lambda x: float(x) / 100,
            "preco_melhor_oferta_compra": lambda x: float(x) / 100,
            "preco_melhor_oferta_venda": lambda x: float(x) / 100,
            "preco_exercicio": lambda x: float(x) / 100,
            "preco_pontos": lambda x: float(x) / 100,
            "volume_titulos_negociados": lambda x: float(x) / 100,
        },
        encoding="ISO-8859-1",
    )


def dataframe_columns() -> list[dict]:
    """
    Columns of the B3 public data files.

    Returns:
        list[dict]: List of columns
            Each element of the list is a dict with the following keys:
                - name: Name of the column
                - description: Description of the column
                - type: Type of the column
                - tuple: Tuple with the start and end positions of the column
    """
    return [
        {
            "name": "tipo_registro",
            "description": "Tipo de registro",
            "type": "int",
            "tuple": (0, 2),
        },
        {
            "name": "data_pregao",
            "description": "Data do pregão",
            "type": "date",
            "tuple": (2, 10),
        },
        {
            "name": "codbdi",
            "description": "Código BDI",
            "type": "int",
            "tuple": (10, 12),
        },
        {
            "name": "sigla_acao",
            "description": "Sigla da ação",
            "type": "str",
            "tuple": (12, 24),
        },
        {
            "name": "tipo_mercado",
            "description": "Tipo de mercado",
            "type": "str",
            "tuple": (24, 27),
        },
        {
            "name": "nome_resumido",
            "description": "Nome resumido da empresa emissora do papel",
            "type": "str",
            "tuple": (27, 39),
        },
        {
            "name": "especificacao_papel",
            "description": "Especificação do papel",
            "type": "str",
            "tuple": (39, 49),
        },
        {
            "name": "prazo_termo",
            "description": "Prazo em dias do termo",
            "type": "int",
            "tuple": (49, 52),
        },
        {
            "name": "moeda",
            "description": "Moeda de referência em reais ou dólares",
            "type": "str",
            "tuple": (52, 56),
        },
        {
            "name": "preco_abertura",
            "description": "Preço de abertura do papel-mercado no pregão",
            "type": "float",
            "tuple": (56, 69),
        },
        {
            "name": "preco_maximo",
            "description": "Preço máximo do papel-mercado no pregão",
            "type": "float",
            "tuple": (69, 82),
        },
        {
            "name": "preco_minimo",
            "description": "Preço mínimo do papel-mercado no pregão",
            "type": "float",
            "tuple": (82, 95),
        },
        {
            "name": "preco_medio",
            "description": "Preço médio do papel-mercado no pregão",
            "type": "float",
            "tuple": (95, 108),
        },
        {
            "name": "preco_ultimo",
            "description": "Preço do último negócio do papel-mercado no pregão",
            "type": "float",
            "tuple": (108, 121),
        },
        {
            "name": "preco_melhor_oferta_compra",
            "description": "Preço da melhor oferta de compra do papel-mercado no pregão",
            "type": "float",
            "tuple": (121, 134),
        },
        {
            "name": "preco_melhor_oferta_venda",
            "description": "Preço da melhor oferta de venda do papel-mercado no pregão",
            "type": "float",
            "tuple": (134, 147),
        },
        {
            "name": "numero_negocios",
            "description": "Número de negócios efetuados com o papel-mercado no pregão",
            "type": "int",
            "tuple": (147, 152),
        },
        {
            "name": "quantidade_titulos_negociados",
            "description": "Quantidade total de títulos negociados neste papel-mercado",
            "type": "int",
            "tuple": (152, 170),
        },
        {
            "name": "volume_titulos_negociados",
            "description": "Volume total de títulos negociados neste papel-mercado",
            "type": "float",
            "tuple": (170, 188),
        },
        {
            "name": "preco_exercicio",
            "description": "Preço de exercício para o mercado de opções ou valor do contrato para o mercado de termo secundário",
            "type": "float",
            "tuple": (188, 201),
        },
        {
            "name": "indicador_correcao_preco",
            "description": "Indicador de correção de preços de exercícios ou valores de contrato para os mercados de opções ou termo secundário",
            "type": "str",
            "tuple": (201, 202),
        },
        {
            "name": "data_vencimento",
            "description": "Data do vencimento para os mercados de opções ou termo secundário",
            "type": "date",
            "tuple": (202, 210),
        },
        {
            "name": "fator_cotacao",
            "description": "Fator de cotação do papel",
            "type": "int",
            "tuple": (210, 217),
        },
        {
            "name": "preco_pontos",
            "description": "Preço de pontos do papel",
            "type": "float",
            "tuple": (217, 230),
        },
        {
            "name": "codigo_isin",
            "description": "Código do papel no sistema ISIN ou código interno do papel",
            "type": "str",
            "tuple": (230, 242),
        },
        {
            "name": "numero_distribuicao",
            "description": "Número de distribuição do papel",
            "type": "int",
            "tuple": (242, 245),
        },
    ]
