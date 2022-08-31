import sqlalchemy
from sqlalchemy import create_engine
import psycopg2


def povoar_banco(df):

    # Conectando ao banco de dados
    db = create_engine('postgresql://sobral:123456@[localhost]/IACIT')

    # Inserindo os dados no banco de dados
    for index, row in df.iterrows():
        if db.execute(f"select exists(select 1 from RADIACAO_GLOBAL where cod_wmo='{row['CODIGO (WMO)']}' "
                      f"and datahora_captacao='{row['DATAHORA DE CAPTAÇÃO']}')").scalar() == False:
            db.execute(f"INSERT INTO RADIACAO_GLOBAL (radiacao_global, datahora_captacao) VALUES "
                       f"('{row['RADIACAO GLOBAL (Kj/m²)']}', '{row['DATAHORA DE CAPTAÇÃO']}')")
        else:
            pass
        try:
            db.execute(f"INSERT INTO VENTO (vento_direcao_horario, vento_rajada_max, datahora_captacao, "
                       f"vento_velocidade) VALUES "
                       f"('{row['VENTO, DIREÇÃO HORARIA (gr) (° (gr))']}', '{row['VENTO, RAJADA MAXIMA (m/s)']}', "
                       f"'{row['DATAHORA DE CAPTAÇÃO']}', '{row['VENTO, VELOCIDADE HORARIA (m/s)']}')")
        except sqlalchemy.exc.IntegrityError:
            print('Erro de integridade')
            pass
        try:
            db.execute(f"INSERT INTO PRESSAO_ATMOSFERICA (pressao_atm_estacao, pressao_atm_max, "
                       f"pressao_atm_min, datahora_captacao) VALUES "
                       f"('{row['PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)']}', "
                       f"'{row['PRESSÃO ATMOSFERICA MAX.NA HORA ANT. (AUT) (mB)']}', "
                       f"'{row['PRESSÃO ATMOSFERICA MIN. NA HORA ANT. (AUT) (mB)']}', "
                       f"'{row['DATAHORA DE CAPTAÇÃO']}')")
        except sqlalchemy.exc.IntegrityError:
            print('Erro de integridade')
            pass
        try:
            db.execute(f"INSERT INTO PRECIPITACAO (precipitacao_total, datahora_captacao) VALUES "
                       f"('{row['PRECIPITAÇÃO TOTAL, HORÁRIO (mm)']}', '{row['DATAHORA DE CAPTAÇÃO']}')")
        except sqlalchemy.exc.IntegrityError:
            print('Erro de integridade')
            pass
        try:
            db.execute(f"INSERT INTO UMIDADE (umidade_relativa_min, datahora_captacao, "
                       f"umidade_relativa_max, umidade_relativa_ar) VALUES "
                       f"('{row['UMIDADE REL. MIN. NA HORA ANT. (AUT) (%)']}', '{row['DATAHORA DE CAPTAÇÃO']}', "
                       f"'{row['UMIDADE REL. MAX. NA HORA ANT. (AUT) (%)']}', "
                       f"'{row['UMIDADE RELATIVA DO AR, HORARIA (%)']}')")
        except sqlalchemy.exc.IntegrityError:
            print('Erro de integridade')
            pass
        try:
            db.execute(f"INSERT INTO TEMPERATURA (temperatura_max, datahora_captacao, "
                       f"temperatura_ponto_orvalho, temperatura_orvalho_max, temperatura_orvalho_min, "
                       f"temperatura_min, temperatura_ar) VALUES "
                       f"('{row['TEMPERATURA MÁXIMA NA HORA ANT. (AUT) (°C)']}', '{row['DATAHORA DE CAPTAÇÃO']}', "
                       f"'{row['TEMPERATURA DO PONTO DE ORVALHO (°C)']}', "
                       f"'{row['TEMPERATURA ORVALHO MAX. NA HORA ANT. (AUT) (°C)']}', "
                       f"'{row['TEMPERATURA ORVALHO MIN. NA HORA ANT. (AUT) (°C)']}', "
                       f"'{row['TEMPERATURA MÍNIMA NA HORA ANT. (AUT) (°C)']}', "
                       f"'{row['TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)']}')")
        except sqlalchemy.exc.IntegrityError:
            print('Erro de integridade')
            pass


def estacao_banco(df):

    # Conectando ao banco de dados
    db = create_engine('postgresql://postgres:123456@[localhost]/db_iacit_api')

    for index, row in df.iterrows():
        try:
            db.execute(f"INSERT INTO estacao (cod_wmo, estacao_nome, estacao_regiao, estacao_estado, "
                       f"estacao_longitude, estacao_latitude, estacao_altitude, estacao_datafundacao, "
                       f"estacao_status) VALUES "
                       f"('{row['CODIGO (WMO)']}', '{row['ESTACAO']}', "
                       f"'{row['REGIAO']}', '{row['UF']}', "
                       f"{row['LONGITUDE']}, {row['LATITUDE']}, {row['ALTITUDE']}, "
                       f"'{row['DATA DE FUNDACAO']}', 'A')")
        except sqlalchemy.exc.IntegrityError:
            print('Erro de integridade')
            pass