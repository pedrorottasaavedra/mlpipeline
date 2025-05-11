from azureml.core import Workspace, Environment

ws = Workspace.get(
    name="aml-agroaurora",
    subscription_id="a8eb1562-1c76-4e19-b35f-a2d1093900d7",
    resource_group="rg-agroaurora-ml"
)

env = Environment.from_conda_specification(name="agroaurora-env", file_path="env.yml")
env.register(workspace=ws)

print("✅ Entorno registrado:", env.name)
