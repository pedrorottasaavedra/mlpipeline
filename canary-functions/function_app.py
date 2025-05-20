import azure.functions as func
import pandas as pd
import numpy as np
import requests
import json
import logging
from io import StringIO
from datetime import datetime, timedelta
from preprocessing import roll
from anomalies import calculate_anomaly_score_1,calculate_anomaly_score_2
import json

starTime_str = "2024-02-6T00:00:00"
endTime_str = "2024-02-7T00:00:00"


tags = [
        "PEAAAUCOI211.Extraccion_data.Preparacion Caña.Picadora 1.Motor.TT_LL Value Y",
        "PEAAAUCOI211.Extraccion_data.Preparacion Caña.Picadora 1.Motor.I_Pic Value Y",
        "PEAAAUCOI211.Extraccion_data.Preparacion Caña.Picadora 1.Motor.TT_PicR Value Y",
        "PEAAAUCOI211.Extraccion_data.Preparacion Caña.Picadora 1.Motor.TT_PicS Value Y",
        "PEAAAUCOI211.Extraccion_data.Preparacion Caña.Picadora 1.Motor.TT_PicT Value Y",
        "PEAAAUCOI211.Extraccion_data.Preparacion Caña.Picadora 1.Motor.VT_PicLL Value Y",
        "PEAAAUCOI211.Extraccion_data.Preparacion Caña.Picadora 1.UH.TT_MotorUH Value Y",
        "PEAAAUCOI211.Extraccion_data.Preparacion Caña.Picadora 1.Motor.TT_LA Value Y",
        "PEAAAUCOI211.Extraccion_data.Preparacion Caña.Picadora 1.UH.TensionUH Value Y",
        "PEAAAUCOI211.Extraccion_data.Preparacion Caña.Picadora 1.Picador de Caña.TT_ChumLL Value Y",
        "PEAAAUCOI211.Extraccion_data.Preparacion Caña.Picadora 2.Picador de Caña.TT_ChumPic2LA Value Y",
        "PEAAAUCOI211.Extraccion_data.Preparacion Caña.Picadora 2.Picador de Caña.TT_ChumPic2LL Value Y",
        "PEAAAUCOI211.Extraccion_data.Preparacion Caña.Picadora 2.Motor.I_Pic2 Value Y",
        "PEAAAUCOI211.Extraccion_data.Preparacion Caña.Picadora 2.Motor.TT_Pic2R Value Y",
        "PEAAAUCOI211.Extraccion_data.Preparacion Caña.Picadora 2.Motor.TT_Pic2S Value Y",
        "PEAAAUCOI211.Extraccion_data.Preparacion Caña.Picadora 2.Motor.TT_Pic2T Value Y",
        "PEAAAUCOI211.Extraccion_data.Preparacion Caña.Picadora 2.Motor.TensionPic2 Value Y",
        "PEAAAUCOI211.Extraccion_data.Preparacion Caña.Picadora 2.Motor.VT_Pic2LL Value Y",
        "PEAAAUCOI211.Extraccion_data.Preparacion Caña.Picadora 2.UH.TT_ReductorAceite Value Y",
        "PEAAAUCOI211.Extraccion_data.Preparacion Caña.Picadora 2.UH.TensionPic2Red Value Y",
        "PEAAAUCOI211.Extraccion_data.Preparacion Caña.Desfibrador.Motor.I_MotorDesf Value Y",
        "PEAAAUCOI211.Extraccion_data.Preparacion Caña.Desfibrador.Motor.TT_MotorDesfR Value Y",
        "PEAAAUCOI211.Extraccion_data.Preparacion Caña.Desfibrador.Motor.TT_MotorDesfS Value Y",
        "PEAAAUCOI211.Extraccion_data.Preparacion Caña.Desfibrador.Motor.TT_MotorDesfT Value Y",
        "PEAAAUCOI211.Extraccion_data.Preparacion Caña.Desfibrador.Motor.TT_MotorLA Value Y",
        "PEAAAUCOI211.Extraccion_data.Preparacion Caña.Desfibrador.Motor.TT_MotorLL Value Y",
        "PEAAAUCOI211.Extraccion_data.Preparacion Caña.Desfibrador.Picador de Caña.TT_ChumLA Value Y",
        "PEAAAUCOI211.Extraccion_data.Preparacion Caña.Desfibrador.Picador de Caña.TT_ChumLL Value Y",
        "PEAAAUCOI211.Extraccion_data.Preparacion Caña.Desfibrador.UH.TT_MotorAceite Value Y",
        "PEAAAUCOI211.Extraccion_data.Preparacion Caña.Desfibrador.Motor.VT_MotorLL Value Y",
        "PEAAAUCOI211.Extraccion_data.Preparacion Caña.Conductores.I_EC101 ValueY",
        "PEAAAUCOI211.Extraccion_data.Preparacion Caña.Conductores.ST_EC101 ValueY",
        "PEAAAUCOI211.Extraccion_data.Preparacion Caña.Conductores.I_MC101 ValueY",
        "PEAAAUCOI211.Extraccion_data.Preparacion Caña.Conductores.I_MC102 ValueY",
        "PEAAAUCOI211.Extraccion_data.Preparacion Caña.Conductores.I_EC102 ValueY",
        "PEAAAUCOI211.Extraccion_data.Preparacion Caña.Conductores.I_EC103 ValueY",
        "PEAAAUCOI211.Extraccion_data.Preparacion Caña.Picadora 1.Motor.EstadoOP Value Y",
        "PEAAAUCOI211.Extraccion_data.Preparacion Caña.Picadora 2.Motor.EstadoOPPic2 Value Y",
        "PEAAAUCOI211.Extraccion_data.Preparacion Caña.Desfibrador.Motor.EstadoDesf Value Y",
        "PEAAAUCOI211.Calc.Preparacion Caña.Conductores.EC101 - Apagado"]

# Columnas renombradas para el modelo
columnas = [
    'P1/TT_LL', 'P1/I_Pic', 'P1/TT_PicR', 'P1/TT_PicS', 'P1/TT_PicT',
    'P1/VT_PicLL', 'P1/TT_MotorUH', 'P1/TT_LA', 'P1/TensionUH', 'P1/TT_ChumLL',
    'P2/TT_ChumPic2LA', 'P2/TT_ChumPic2LL', 'P2/I_Pic2', 'P2/TT_Pic2R', 'P2/TT_Pic2S',
    'P2/TT_Pic2T', 'P2/TensionPic2', 'P2/VT_Pic2LL', 'P2/TT_ReductorAceite', 'P2/TensionPic2Red',
    'Desf/I_MotorDesf', 'Desf/TT_MotorDesfR', 'Desf/TT_MotorDesfS', 'Desf/TT_MotorDesfT',
    'Desf/TT_MotorLA', 'Desf/TT_MotorLL', 'Desf/TT_ChumLA', 'Desf/TT_ChumLL',
    'Desf/TT_MotorAceite', 'Desf/VT_MotorLL', 'Conductor/I_EC101', 'Conductor/ST_EC101',
    'Conductor/I_MC101', 'Conductor/I_MC102', 'Conductor/I_EC102', 'Conductor/I_EC103',
    'Picadora 1 - Apagada', 'Picadora 2 - Apagada', 'Desfibrador - Apagado', 'Conductor - Apagado'
]
# Columnas filtro
columnas_filtro = ['P1/TT_LL',
 'P1/I_Pic',
 'P1/TT_PicR',
 'P1/TT_PicS',
 'P1/TT_PicT',
 'P1/VT_PicLL',
 'P1/TT_MotorUH',
 'P1/TT_LA',
 'P1/TensionUH',
 'P1/TT_ChumLL',
 'P2/TT_ChumPic2LA',
 'P2/TT_ChumPic2LL',
 'P2/TT_Pic2R',
 'P2/TT_Pic2S',
 'P2/TT_Pic2T',
 'P2/TensionPic2',
 'P2/VT_Pic2LL',
 'P2/TT_ReductorAceite',
 'P2/TensionPic2Red',
 'Desf/TT_MotorDesfR',
 'Desf/TT_MotorDesfS',
 'Desf/TT_MotorDesfT',
 'Desf/TT_MotorLA',
 'Desf/TT_MotorLL',
 'Desf/TT_ChumLA',
 'Desf/TT_ChumLL',
 'Desf/TT_MotorAceite',
 'Desf/VT_MotorLL',
 'Conductor/I_EC101',
 'Conductor/ST_EC101',
 'Conductor/I_MC101',
 'Conductor/I_MC102',
 'Conductor/I_EC102',
 'Conductor/I_EC103']

PRIMARY_KEY = "7fOJeTF3b2D2bRJKr7hDhTLS7wl6cuiBXst7ZwOLB5aEszuspFLXJQQJ99BEAAAAAAAAAAAAINFRAZML4Kev"
ENDPOINT_URL = "https://ec101-endpoint.eastus.inference.ml.azure.com/score"

start_time = datetime.strptime(starTime_str, "%Y-%m-%dT%H:%M:%S")
end_time = datetime.strptime(endTime_str, "%Y-%m-%dT%H:%M:%S")

app = func.FunctionApp()
@app.route(route="getCanaryAndPredict", auth_level=func.AuthLevel.ANONYMOUS)
def getCanaryAndPredict(req: func.HttpRequest) -> func.HttpResponse:
    try:
        df_umbrales = pd.read_csv('data/umbrales_EC101(in).csv', index_col=0)
        #threshold = pd.read_csv('data/threshold_EC101(in).csv')
        aggregate_name = 'TimeAverage'
        base_url = "http://axiom.agroauroradata.com:55235/api/v2/getTagData2"
        access_token = "23538a79-c688-40a0-a4ca-5053bc4837da"
        params = [
            ("accessToken", access_token),
            ("startTime", start_time.isoformat()),
            ("endTime", end_time.isoformat()),
            ("aggregateName", aggregate_name),
            ("aggregateInterval", "00:0:30"),
            ] + [("tags", tag) for tag in tags]
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        data = response.json()
        tag_dfs = {}
        api_data = data['data']
        for tag in api_data:
            valores = api_data[tag]
            timestamps = [v["t"] for v in valores]
            valores_numericos = [v["v"] for v in valores]
            tag_dfs[tag] = pd.DataFrame({tag: valores_numericos}, index=pd.to_datetime(timestamps))

# 2. Combinar todos los DataFrames en uno solo
        df = pd.concat(tag_dfs.values(), axis=1)
        df.columns = columnas
#3. preprocesamiento
        df = roll(df,columnas_filtro)

#4. detección de anomalías
        anomaly_scores_1 = calculate_anomaly_score_1(df, df_umbrales, importancia=0.3)
        anomaly_scores_2 = calculate_anomaly_score_2(df, df_umbrales, importancia=1, b=10**(-4), a=3)
        #anomaly_scores = (anomaly_scores_1 + anomaly_scores_2)

        # Enviar a endpoint ML
        payload = json.loads(df.to_json(orient='columns'))
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {PRIMARY_KEY}"
        }
        ml_response = requests.post(ENDPOINT_URL, headers=headers, json=payload)
        ml_output = ml_response.json()

        # Simular cálculo de scored con threshold y output ml (reemplazar con lógica real)
        pred_x= ml_output["payload"]["prediction"]
        pred_df = pd.DataFrame(pred_x)    
        return func.HttpResponse(pred_df.to_json(orient="records"), mimetype="application/json")

    except Exception as e:
        logging.exception("Error en getCanaryAndPredict")
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)   