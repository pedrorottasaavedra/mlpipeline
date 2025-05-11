#from azureml.core import Workspace, Environment, Model, InferenceConfig
from azureml.core.webservice import AciWebservice, Webservice
from azureml.core import Workspace, Environment, Model
#from azureml.core.environment import InferenceConfig
from azureml.core.model import InferenceConfig


import os

# Conexión al Workspace
ws = Workspace.get(
    name="aml-agroaurora",
    subscription_id="a8eb1562-1c76-4e19-b35f-a2d1093900d7",
    resource_group="rg-agroaurora-ml"
)

# 1. Cargar modelo ya registrado
model = Model(ws, name="scaler8_EC101")  # debe coincidir con el nombre que registraste

# 2. Usar entorno registrado
env = Environment.get(ws, name="agroaurora-env")

# 3. Crear configuración de inferencia
inference_config = InferenceConfig(
    entry_script="deployments/scaler8_EC101/score.py",
    environment=env
)

# 4. Configurar endpoint
deployment_config = AciWebservice.deploy_configuration(
    cpu_cores=1,
    memory_gb=1,
    auth_enabled=True
)

# 5. Desplegar endpoint (si ya existe, se puede actualizar con .update() en otro script)
service_name = "scaler8-ec101-endpoint"
service = Model.deploy(
    workspace=ws,
    name=service_name,
    models=[model],
    inference_config=inference_config,
    deployment_config=deployment_config,
    overwrite=True  # ⚠️ reemplaza si ya existe
)

service.wait_for_deployment(show_output=True)

print("✅ Endpoint desplegado en:", service.scoring_uri)
