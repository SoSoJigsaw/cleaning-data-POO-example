import numpy as np
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
import psycopg2

# Leitura dos dataframes pelo pandas
df = pd.read_csv(r'C:\Users\Felipe Sobral\Downloads\Dfs 2022\INMET_SE_ES_A612_VITORIA_01-01-2022_A_30-06-2022.CSV',
                 sep=';', encoding='latin-1', on_bad_lines='skip')
df1 = pd.read_csv(r'C:\Users\Felipe Sobral\Downloads\Dfs 2022\INMET_SE_ES_A612_VITORIA_01-01-2022_A_30-06-2022.CSV',
                  sep=';', encoding='latin-1', skiprows=[0, 1, 2, 3, 4, 5, 6, 7], on_bad_lines='skip')

# Criando a colunas do cabeçalho vertical
df['CODIGO (WMO)'] = df.loc[2][1]

# Dando merge nos dataframes
df = df1.assign(key=1).merge(df[['CODIGO (WMO)']].assign(key=1), on='key').drop('key', axis=1)
del df1

# Apagando as linhas duplicadas
df = df.drop_duplicates(subset=['Data', 'Hora UTC'], keep='first')

# Resolvendo o problema dos números que começam com vírgula
for col in df.columns:
    mask = (df[col] == ',9')
    df.loc[mask, col] = '0,9'

    mask = (df[col] == ',8')
    df.loc[mask, col] = '0,8'

    mask = (df[col] == ',7')
    df.loc[mask, col] = '0,7'

    mask = (df[col] == ',6')
    df.loc[mask, col] = '0,6'

    mask = (df[col] == ',5')
    df.loc[mask, col] = '0,5'

    mask = (df[col] == ',4')
    df.loc[mask, col] = '0,4'

    mask = (df[col] == ',3')
    df.loc[mask, col] = '0,3'

    mask = (df[col] == ',2')
    df.loc[mask, col] = '0,2'

    mask = (df[col] == ',1')
    df.loc[mask, col] = '0,1'

    mask = (df[col] == ',0')
    df.loc[mask, col] = '0'

# Ajustando Data ao padrão Br
df['Data'] = pd.to_datetime(df['Data'], errors='coerce')
df['Data'] = df['Data'].dt.strftime("%d/%m/%Y")

# Juntando a Data à Hora
df["DATAHORA DE CAPTAÇÃO"] = df["Data"] + ' ' + df["Hora UTC"]
df['DATAHORA DE CAPTAÇÃO'] = df['DATAHORA DE CAPTAÇÃO'].replace({'0000 UTC': '00', '0100 UTC': '01',
                                                                 '0200 UTC': '02', '0300 UTC': '03',
                                                                 '0400 UTC': '04', '0500 UTC': '05',
                                                                 '0600 UTC': '06', '0700 UTC': '07',
                                                                 '0800 UTC': '08', '0900 UTC': '09',
                                                                 '1000 UTC': '10', '1100 UTC': '11',
                                                                 '1200 UTC': '12', '1300 UTC': '13',
                                                                 '1400 UTC': '14', '1500 UTC': '15',
                                                                 '1600 UTC': '16', '1700 UTC': '17',
                                                                 '1800 UTC': '18', '1900 UTC': '19',
                                                                 '2000 UTC': '20', '2100 UTC': '21',
                                                                 '2200 UTC': '22', '2300 UTC': '23',
                                                                 }, regex=True)

# Apagando colunas
if 'Unnamed: 19' in df.columns:
    df.drop('Unnamed: 19', inplace=True, axis=1)
if ',,,,,,' in df.columns:
    df.drop(',,,,,,', inplace=True, axis=1)

df.drop('Data', inplace=True, axis=1)
df.drop('Hora UTC', inplace=True, axis=1)

# Reorganizando a ordem das colunas
df = df[['CODIGO (WMO)',
         'DATAHORA DE CAPTAÇÃO',
         'PRECIPITAÇÃO TOTAL, HORÁRIO (mm)',
         'PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)',
         'PRESSÃO ATMOSFERICA MAX.NA HORA ANT. (AUT) (mB)',
         'PRESSÃO ATMOSFERICA MIN. NA HORA ANT. (AUT) (mB)',
         'RADIACAO GLOBAL (Kj/m²)',
         'TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)',
         'TEMPERATURA DO PONTO DE ORVALHO (°C)',
         'TEMPERATURA MÁXIMA NA HORA ANT. (AUT) (°C)',
         'TEMPERATURA MÍNIMA NA HORA ANT. (AUT) (°C)',
         'TEMPERATURA ORVALHO MAX. NA HORA ANT. (AUT) (°C)',
         'TEMPERATURA ORVALHO MIN. NA HORA ANT. (AUT) (°C)',
         'UMIDADE REL. MAX. NA HORA ANT. (AUT) (%)',
         'UMIDADE REL. MIN. NA HORA ANT. (AUT) (%)',
         'UMIDADE RELATIVA DO AR, HORARIA (%)',
         'VENTO, DIREÇÃO HORARIA (gr) (° (gr))',
         'VENTO, RAJADA MAXIMA (m/s)',
         'VENTO, VELOCIDADE HORARIA (m/s)']]

# Apagando vírgulas em sequência errôneas
df = df.replace({',,,,,,,,,,,,,': ''}, regex=True)
df = df.replace({',,,,,,,,,,,,': ''}, regex=True)

# Apagando vírgulas de valores floating
df = df.replace({',': '.'}, regex=True)

# Redeterminando valores Nan para NULL
df.fillna("NaN", inplace=True)

# Convertendo a datetime
df['DATAHORA DE CAPTAÇÃO'] = pd.to_datetime(df['DATAHORA DE CAPTAÇÃO'], errors='raise', dayfirst=True)

# Config para visualizar todas as colunas
pd.set_option('display.max_columns', None)


print(df.head(30))
