import pandas as pd
import glob

files = glob.glob(r"/*.csv")

i = 1
for f in files:
    df = pd.read_csv(f, sep=';', encoding='latin-1', on_bad_lines='skip')
    df1 = pd.read_csv(f, sep=';', encoding='latin-1', skiprows=[0, 1, 2, 3, 4, 5, 6, 7], on_bad_lines='skip')

    df['CODIGO (WMO)'] = df.loc[2][1]

    df = df1.assign(key=1).merge(df[['CODIGO (WMO)']].assign(key=1), on='key').drop('key', axis=1)
    del df1

    df.to_csv(fr"\2020-{i}.csv", index=False)
    i += 1
