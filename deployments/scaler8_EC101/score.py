import os
import json
import joblib
import pandas as pd
import arrow
import pytz

def init():
    global model
    model_path = os.path.join(os.getenv("AZUREML_MODEL_DIR"), "scaler8_EC101.pkl")
    model = joblib.load(model_path)

def run(raw_data):
    try:
        # Parse input JSON
        data = json.loads(raw_data)
        df = pd.DataFrame([data])  # convertir a DataFrame

        # Ejemplo de preprocesamiento: agregar hora local y variable extra
        #df["hora_local"] = arrow.utcnow().to("America/Lima").format("HH:mm")
        #df["es_turno_noche"] = df["hora_local"].apply(lambda h: int(h.split(":")[0]) >= 20)

        # Predicción
        pred = model.predict(df)

        return {
            "prediccion": pred.tolist(),
            "variables_entrada": df.to_dict(orient="records")
        }

    except Exception as e:
        return { "error": str(e) }
