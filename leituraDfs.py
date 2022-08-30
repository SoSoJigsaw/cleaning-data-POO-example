import pandas as pd
import glob
from cleaningData import tratamento_dfs
from conexaoBD import povoar_banco


def leitura_dfs2020():

    # Path dos dataframes
    files = glob.glob(r"/*.csv")

    i = 1
    for f in files:
        df = pd.read_csv(f, sep=';', encoding='latin-1', on_bad_lines='skip')
        df1 = pd.read_csv(f, sep=';', encoding='latin-1', skiprows=[0, 1, 2, 3, 4, 5, 6, 7], on_bad_lines='skip')

        # Criando colunas do cabeçalho vertical
        df['CODIGO (WMO)'] = df.loc[2][1]

        # Dando merge nos dataframes
        df = df1.assign(key=1).merge(df[['CODIGO (WMO)']].assign(key=1), on='key').drop('key', axis=1)
        del df1

        # Tratamento dos dados e povoamento do banco de dados
        povoar_banco(tratamento_dfs(df))


def leitura_dfs2021():

    # Path dos dataframes
    files = glob.glob(r"/*.csv")

    i = 1
    for f in files:
        df = pd.read_csv(f, sep=';', encoding='latin-1', on_bad_lines='skip')
        df1 = pd.read_csv(f, sep=';', encoding='latin-1', skiprows=[0, 1, 2, 3, 4, 5, 6, 7], on_bad_lines='skip')

        # Criando colunas do cabeçalho vertical
        df['CODIGO (WMO)'] = df.loc[2][1]

        # Dando merge nos dataframes
        df = df1.assign(key=1).merge(df[['CODIGO (WMO)']].assign(key=1), on='key').drop('key', axis=1)
        del df1

        # Tratamento dos dados e povoamento do banco de dados
        povoar_banco(tratamento_dfs(df))


def leitura_dfs2022():

    # Path dos dataframes
    files = glob.glob(r"/*.csv")

    i = 1
    for f in files:
        df = pd.read_csv(f, sep=';', encoding='latin-1', on_bad_lines='skip')
        df1 = pd.read_csv(f, sep=';', encoding='latin-1', skiprows=[0, 1, 2, 3, 4, 5, 6, 7], on_bad_lines='skip')

        # Criando colunas do cabeçalho vertical
        df['CODIGO (WMO)'] = df.loc[2][1]

        # Dando merge nos dataframes
        df = df1.assign(key=1).merge(df[['CODIGO (WMO)']].assign(key=1), on='key').drop('key', axis=1)
        del df1

        # Tratamento dos dados e povoamento do banco de dados
        povoar_banco(tratamento_dfs(df))

