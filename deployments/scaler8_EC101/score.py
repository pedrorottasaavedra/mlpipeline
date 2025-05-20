import os
import json
import numpy as np
import pandas as pd
import joblib
import tensorflow as tf

def init():
    global scaler, model, threshold

    model_dir = os.getenv("AZUREML_MODEL_DIR")

    # Cargar scaler
    scaler_path = os.path.join(model_dir, "ec101_model", "scaler8_EC101.pkl")
    scaler = joblib.load(scaler_path)

    # Cargar modelo .keras
    model_path = os.path.join(model_dir, "ec101_model", "model8_EC101.keras")
    model = tf.keras.models.load_model(model_path)

    # Threshold fijo
    threshold = 0.4197862394224356

def run(raw_data):
    try:
        # Si raw_data ya es un dict (como ocurre con Azure ML), no lo vuelvas a cargar
        if isinstance(raw_data, str):
            data_json = json.loads(raw_data)
        else:
            data_json = raw_data

        df = pd.DataFrame(data_json)  # Asegúrate de que el JSON tenga clave "data"

        # Escalar datos
        X = scaler.transform(df)
        X = X.reshape(X.shape[0], 1, X.shape[1])

        # Predicción (reconstrucción)
        X_pred = model.predict(X)
        X_pred = X_pred.reshape((X_pred.shape[0], X_pred.shape[2]))

        return {
            "payload": {
                "prediction": X_pred.tolist()
            }
        }

    except Exception as e:
        return {
            "error": str(e)
        }

