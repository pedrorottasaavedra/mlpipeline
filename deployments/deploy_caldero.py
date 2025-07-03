from azure.identity import DefaultAzureCredential
from azure.ai.ml import MLClient
from azure.ai.ml.entities import (
    ManagedOnlineEndpoint,
    ManagedOnlineDeployment,
    CodeConfiguration
)
import os

# 1. Autenticación e identificación del workspace
subscription_id = "a8eb1562-1c76-4e19-b35f-a2d1093900d7"
resource_group = "rg-agroaurora-ml"
workspace_name = "aml-agroaurora"

ml_client = MLClient(
    credential=DefaultAzureCredential(),
    subscription_id=subscription_id,
    resource_group_name=resource_group,
    workspace_name=workspace_name
)

# 2. Nombre del endpoint
endpoint_name = "caldero-endpoint"

# 3. Crear endpoint si no existe
try:
    endpoint = ml_client.online_endpoints.get(endpoint_name)
    print(f"✅ Endpoint '{endpoint_name}' ya existe.")
except Exception:
    print(f"🚀 Creando nuevo endpoint '{endpoint_name}'...")
    endpoint = ManagedOnlineEndpoint(
        name=endpoint_name,
        description="Endpoint para modelo caldero versión 2",
        auth_mode="key"
    )
    ml_client.begin_create_or_update(endpoint).result()

# 4. Usar entorno existente
environment = ml_client.environments.get(name="env-calderov2", version="1")

# 5. Crear despliegue
deployment_name = "default"

deployment = ManagedOnlineDeployment(
    name=deployment_name,
    endpoint_name=endpoint_name,
    model="azureml:caldero_model_completo:2",
    environment=environment,
    instance_type="Standard_DS2_v2",  # ⛔ Importante: DS2_v2 puede ser muy pequeño
    instance_count=1,
    code_configuration=CodeConfiguration(
        code="./deployments/caldero",
        scoring_script="score.py"
    )
)

print("⏳ Creando despliegue...")
ml_client.begin_create_or_update(deployment).result()
print("✅ Despliegue creado")

# 6. Obtener URL del endpoint
endpoint = ml_client.online_endpoints.get(endpoint_name)
print(f"🌐 Endpoint URL: {endpoint.scoring_uri}")
