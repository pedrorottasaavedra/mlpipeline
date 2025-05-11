from azureml.core import Model
from azureml.core import Workspace

import os

#ws = "aml-agroaurora"	
ws = Workspace.get(
    name="aml-agroaurora",                      # Nombre exacto del workspace
    subscription_id="a8eb1562-1c76-4e19-b35f-a2d1093900d7",  # Tu ID de suscripción
    resource_group="rg-agroaurora-ml"           # Nombre del grupo de recursos
)

model_dir = "models"
for fname in os.listdir(model_dir):
 if fname.endswith(".pkl"):
  modelPath = os.path.join(model_dir, fname)
  modelName = os.path.splitext(fname)[0]
  Model.register(workspace=ws,model_name=os.path.splitext(fname)[0],model_path=modelPath,description=f"Modelo {modelName} subido a la nube",tags={"framework": "tensorflow"})
 else: 
  pass
print("subidos todos")
        
