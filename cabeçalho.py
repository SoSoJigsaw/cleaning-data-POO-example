import pandas as pd


df = pd.read_csv(r'/home/sobral/Downloads/2022/INMET_SE_ES_A612_VITORIA_01-01-2022_A_30-06-2022.CSV',
                 sep=';', encoding='latin-1', on_bad_lines='skip', header=None)

df['REGIAO'] = df.loc[0][1]
df['UF'] = df.loc[1][1]
df['ESTACAO'] = df.loc[2][1]
df['CODIGO (WMO)'] = df.loc[3][1]
df['LATITUDE'] = df.loc[4][1]
df['LONGITUDE'] = df.loc[5][1]
df['ALTITUDE'] = df.loc[6][1]
df['DATA DE FUNDACAO'] = df.loc[7][1]

print(df.head(10))
