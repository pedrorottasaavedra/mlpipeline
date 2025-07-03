from azure.identity import DefaultAzureCredential
from azure.ai.ml import MLClient
from azure.ai.ml.entities import (
    ManagedOnlineEndpoint,
    ManagedOnlineDeployment,
    Environment,
    CodeConfiguration
)
import datetime
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

# 2. Nombre único para el endpoint (se puede reutilizar si ya existe)
endpoint_name = "molino-endpoint"

try:
    endpoint = ml_client.online_endpoints.get(endpoint_name)
    print(f"✅ Endpoint '{endpoint_name}' ya existe.")
except Exception:
    print(f"🚀 Creando nuevo endpoint '{endpoint_name}'...")
    endpoint = ManagedOnlineEndpoint(
        name=endpoint_name,
        description="Endpoint para modelo molino",
        auth_mode="key"
    )
    ml_client.begin_create_or_update(endpoint).result()

# 3. Configuración del entorno (puedes actualizarlo según tu `conda.yaml`)
environment = Environment(
    name="env-ec101",
    image="mcr.microsoft.com/azureml/openmpi4.1.0-ubuntu20.04:latest",
    conda_file="conda.yml",
    description="Entorno para EC101 con keras, sklearn y pandas"
)

# 4. Crear despliegue
deployment_name = "default"

deployment = ManagedOnlineDeployment(
    name=deployment_name,
    endpoint_name=endpoint_name,
    model=f"azureml:molinoo_model_completo:1",  # Asegúrate del número de versión
    environment=environment,
    instance_type="Standard_DS2_v2",
    instance_count=1,
    code_configuration=CodeConfiguration(
        code="./deployments/molino",
        scoring_script="score.py"
    )
)

print("⏳ Creando despliegue...")
ml_client.begin_create_or_update(deployment).result()
print("✅ Despliegue creado")

# Obtener endpoint actualizado (por si fue creado recién)
endpoint = ml_client.online_endpoints.get(endpoint_name)

# Imprimir URL del endpoint
print(f"🌐 Endpoint URL: {endpoint.scoring_uri}")
