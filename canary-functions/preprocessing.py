import pandas as pd

def moving_average_filter(data, columns, window_size=3):
    filtered_data = data.copy()
    for column in columns:
        filtered_data[column] = data[column].rolling(window=window_size, min_periods=1).mean()
    return filtered_data



def roll(df,columnas_filtro):
    data_filt = moving_average_filter(df, columnas_filtro, window_size=3)
    data_filt['P1/I_Pic'] = data_filt['P1/I_Pic'].rolling('80s',closed='both').max()
    data_filt['P2/I_Pic2'] = data_filt['P2/I_Pic2'].rolling('80s',closed='both').max()
    data_filt['Desf/I_MotorDesf'] = data_filt['Desf/I_MotorDesf'].rolling('80s',closed='both').max()
    data_filt['Conductor - Apagado'] = data_filt['Conductor - Apagado'].rolling('5min',center=True).max()
    data_filt['Conductor - Apagado'] = data_filt['Conductor - Apagado'].rolling('5min',center=True).max()

# Identificar periodos donde la señal es 0 y expandir 5 minutos
    condition = (data_filt['Picadora 1 - Apagada'] == 0)
    data_filt['Picadora 1 - Apagada'] = condition.rolling('5min',min_periods=1,center=True).max()

# Combinar periodos separados por menos de 15 minutos
    data_filt['Picadora 1 - Apagada'] = data_filt['Picadora 1 - Apagada'].rolling('15min',min_periods=1).max()

# Expandir 5 minutos hacia adelante
    data_filt['Picadora 1 - Apagada'] = data_filt['Picadora 1 - Apagada'].shift(-5,freq='min').fillna(False)

#--------------------------------------------------------------------------------------------------------------

# Identificar periodos donde la señal es 0 y expandir 5 minutos
    condition = (data_filt['Picadora 2 - Apagada'] == 0)
    data_filt['Picadora 2 - Apagada'] = condition.rolling('5min',min_periods=1,center=True).max()

# Combinar periodos separados por menos de 15 minutos
    data_filt['Picadora 2 - Apagada'] = data_filt['Picadora 2 - Apagada'].rolling('15min',min_periods=1).max()

# Expandir 5 minutos hacia adelante
    data_filt['Picadora 2 - Apagada'] = data_filt['Picadora 2 - Apagada'].shift(-5,freq='min').fillna(False)

# ---------------------------------------------------------------------------------------------------------------

# Identificar periodos donde la señal es 0 y expandir 5 minutos
    condition = (data_filt['Desfibrador - Apagado'] == 0)
    data_filt['Desfibrador - Apagado'] = condition.rolling('5min',min_periods=1,center=True).max()

# Combinar periodos separados por menos de 15 minutos
    data_filt['Desfibrador - Apagado'] = data_filt['Desfibrador - Apagado'].rolling('15min',min_periods=1).max()

# Expandir 5 minutos hacia adelante
    data_filt['Desfibrador - Apagado'] = data_filt['Desfibrador - Apagado'].shift(-5,freq='min').fillna(False)

    datav2 = data_filt
    datav2 = datav2.drop(columns=['Desfibrador - Apagado - Estado'], errors='ignore')
    datav2 = datav2.fillna(0)
    datav2 = datav2.ffill(axis=0)
    condition = (datav2['Picadora 1 - Apagada'] == 1) | (datav2['Picadora 2 - Apagada'] == 1)
    datav2.loc[condition] = datav2.loc[condition].fillna(0)
    df = datav2
    df = df.drop(['Picadora 1 - Apagada','Picadora 2 - Apagada', 'Desfibrador - Apagado','Conductor - Apagado'], axis=1)

# Reglas de Limpieza
    df['P1/I_Pic'] = df['P1/I_Pic'].clip(0, 200)
    df['P1/TT_LL'] = df['P1/TT_LL'].clip(0, 200)
    df['P1/TT_PicR'] = df['P1/TT_PicR'].clip(0, 200)
    df['P1/TT_PicS'] = df['P1/TT_PicS'].clip(0, 200)
    df['P1/TT_PicT'] = df['P1/TT_PicT'].clip(0, 200)
    df['P1/VT_PicLL'] = df['P1/VT_PicLL'].clip(0, 20)
    df['P1/TT_MotorUH'] = df['P1/TT_MotorUH'].clip(0, 200)
    df['P1/TT_LA'] = df['P1/TT_LA'].clip(0, 200)
    df['P1/TensionUH'] = df['P1/TensionUH'].clip(0, 10)
    df['P1/TT_ChumLL'] = df['P1/TT_ChumLL'].clip(0, 200)
    df['P2/TT_ChumPic2LA'] = df['P2/TT_ChumPic2LA'].clip(0, 200)
    df['P2/TT_ChumPic2LL'] = df['P2/TT_ChumPic2LL'].clip(0, 200)
    df['P2/I_Pic2'] = df['P2/I_Pic2'].clip(0, 1500)
    df['P2/TT_Pic2R'] = df['P2/TT_Pic2R'].clip(0, 200)
    df['P2/TT_Pic2S'] = df['P2/TT_Pic2S'].clip(0, 200)
    df['P2/TT_Pic2T'] = df['P2/TT_Pic2T'].clip(0, 200)
    df['P2/TensionPic2'] = df['P2/TensionPic2'].clip(0, 1000)
    df['P2/VT_Pic2LL'] = df['P2/VT_Pic2LL'].clip(0, 20)
    df['P2/TT_ReductorAceite'] = df['P2/TT_ReductorAceite'].clip(0, 200)
    df['P2/TensionPic2Red'] = df['P2/TensionPic2Red'].clip(0, 10)
    df['Desf/I_MotorDesf'] = df['Desf/I_MotorDesf'].clip(0, 400)
    df['Desf/TT_MotorDesfR'] = df['Desf/TT_MotorDesfR'].clip(0, 200)
    df['Desf/TT_MotorDesfS'] = df['Desf/TT_MotorDesfS'].clip(0, 200)
    df['Desf/TT_MotorDesfT'] = df['Desf/TT_MotorDesfT'].clip(0, 200)
    df['Desf/TT_MotorLA'] = df['Desf/TT_MotorLA'].clip(0, 200)
    df['Desf/TT_MotorLL'] = df['Desf/TT_MotorLL'].clip(0, 200)
    df['Desf/TT_ChumLA'] = df['Desf/TT_ChumLA'].clip(0, 200)
    df['Desf/TT_ChumLL'] = df['Desf/TT_ChumLL'].clip(0, 200)
    df['Desf/TT_MotorAceite'] = df['Desf/TT_MotorAceite'].clip(0, 200)
    df['Desf/VT_MotorLL'] = df['Desf/VT_MotorLL'].clip(0, 20)
    df['Conductor/I_EC101'] = df['Conductor/I_EC101'].clip(20, 65)
    df['Conductor/I_EC102'] = df['Conductor/I_EC102'].clip(5, 60)
    df['Conductor/I_EC103'] = df['Conductor/I_EC103'].clip(0, 45)
    df['Conductor/I_MC101'] = df['Conductor/I_MC101'].clip(0, 200)
    df['Conductor/I_MC102'] = df['Conductor/I_MC102'].clip(0, 50)
    df['Conductor/ST_EC101'] = df['Conductor/ST_EC101'].clip(0, 1300)
    #Drop de filas con valores nulos
    df = df.dropna()
    return df