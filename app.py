import os
import yaml
import aws_cdk as cdk
from stacks.debezium_stack import DebeziumHAStack

app = cdk.App()

# Select environment via: cdk deploy -c env=staging
# Defaults to production.
env_name = app.node.try_get_context("env") or "production"

config_path = os.path.join(os.path.dirname(__file__), "config", f"{env_name}.yaml")
if not os.path.exists(config_path):
    raise FileNotFoundError(
        f"Config file not found: {config_path}\n"
        f"Create config/{env_name}.yaml to deploy this environment."
    )

with open(config_path) as f:
    config: dict = yaml.safe_load(f)

# Strip any whitespace from multiline YAML strings (e.g. msk_bootstrap_servers)
for key, val in config.items():
    if isinstance(val, str):
        config[key] = val.replace("\n", "").replace(" ", "").strip(",")

DebeziumHAStack(
    app,
    f"DebeziumHAStack-{env_name}",
    config=config,
    env=cdk.Environment(
        account=config.get("account"),
        region=config.get("region", "ap-south-1"),
    ),
)

app.synth()
