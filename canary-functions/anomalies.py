import pandas as pd
def calculate_anomaly_score_1(df, df_umbrales, importancia=0.3):
    n = df_umbrales.loc['peso'].sum()
    df_temp = df.copy() 
    # Iterar sobre cada columna en df
    for column in df.columns:
        # Obtener los umbrales superior e inferior para la columna actual
        lower_bound = df_umbrales.loc['mean_minus_std', column]
        upper_bound = df_umbrales.loc['mean_plus_std', column]

        # Aplicar clip para limitar los valores de la columna actual
        df_temp[column] = df[column].clip(lower=lower_bound, upper=upper_bound)
        df = df_temp.copy()
    anomaly_scores = pd.DataFrame(index=df.index)


    for column in df.columns:
        # Calcula la desviación dependiendo si está por encima o por debajo del promedio
        above_mean = df[column] > df_umbrales.loc['mean', column]
        # Si está por encima del promedio, usa mean_plus_std
        normalized_deviation_above = importancia*100*df_umbrales.loc['peso', column]*(
            abs((df[column] - df_umbrales.loc['mean', column]) / (df_umbrales.loc['mean_plus_std', column] - df_umbrales.loc['mean', column]))
        ).where(above_mean, 0)  # Solo aplica cuando está por encima del promedio

        # Si está por debajo del promedio, usa mean_minus_std
        normalized_deviation_below = importancia*100*df_umbrales.loc['peso', column]*(
            abs((df_umbrales.loc['mean', column] - df[column]) / (df_umbrales.loc['mean', column] - df_umbrales.loc['mean_minus_std', column]))
        ).where(~above_mean, 0)  # Solo aplica cuando está por debajo del promedio

        # Suma las desviaciones normalizadas de ambos casos
        normalized_deviation = normalized_deviation_above + normalized_deviation_below

        # Aplica una ponderación si es necesario o suma todas las desviaciones
        anomaly_scores[column] = normalized_deviation

    # Score total de anomalía por cada minuto
    anomaly_scores['total_score'] = anomaly_scores.sum(axis=1)/n

    return anomaly_scores

def calculate_anomaly_score_2(df, df_umbrales, importancia=0.7, b=10**(-8), a=5):
    n = df_umbrales.loc['peso'].sum()
    anomaly_scores = pd.DataFrame(index=df.index)


    for column in df.columns:
        # Calcula la desviación dependiendo si está por encima o por debajo del promedio
        above_2std = df[column] > df_umbrales.loc['mean_plus_std', column]
        below_2std = df[column] < df_umbrales.loc['mean_minus_std', column]

        # Si está por encima de 2std
        temp_above = (
            100*(abs((df[column] - df_umbrales.loc['mean_plus_std', column]) / (df_umbrales.loc['high', column] - df_umbrales.loc['mean_plus_std', column])))
        ).where(above_2std, 0)
        #Si temp_above es mayor a 110, se le asigna 110
        temp_above = temp_above.where(temp_above<110,110)
        normalized_deviation_above = df_umbrales.loc['peso', column]*((n*b)*(temp_above**a))

        # Si está por debajo del promedio, usa mean_minus_2std
        temp_below=(
            100*(abs((df[column] - df_umbrales.loc['mean_minus_std', column]) / (df_umbrales.loc['low', column] - df_umbrales.loc['mean_minus_std', column])))
        ).where(below_2std, 0)
        #If temp_below is greater than 110, it is assigned 110
        temp_below = temp_below.where(temp_below<110,110)

        normalized_deviation_below = df_umbrales.loc['peso', column]*((n*b)*(temp_below**a))

        # Suma las desviaciones normalizadas de ambos casos
        normalized_deviation = normalized_deviation_above + normalized_deviation_below

        # Aplica una ponderación si es necesario o suma todas las desviaciones
        anomaly_scores[column] = normalized_deviation

    # Score total de anomalía por cada minuto (puedes modificar esto para ponderar diferentes señales)
    anomaly_scores['total_score'] = anomaly_scores.sum(axis=1)

    #dividir todo anomaly_scores por n con excepcón de la columna total_score
    anomaly_scores = anomaly_scores.div(30)


    #Multiplicar por importancia a todas las columnas excepto la columna total_score
    anomaly_scores = anomaly_scores.mul(importancia)
    anomaly_scores['total_score'] = anomaly_scores['total_score'].div(importancia)

    return anomaly_scores

