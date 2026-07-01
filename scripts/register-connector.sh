#!/usr/bin/env bash
# Register (or update) a Debezium connector via ECS Exec.
# No ALB needed — curl runs inside a running Fargate task directly.
#
# Prerequisites:
#   aws cli v2, session-manager-plugin, jq
#   IAM permissions: ecs:ExecuteCommand, ssm:StartSession
#
# Usage:
#   ECS_CLUSTER=debezium-cdc-cluster \
#   ECS_SERVICE=debezium-cdc-connect \
#   bash scripts/register-connector.sh [path/to/connector.json]
#
# Defaults to connectors/mysql-source.json if no file is specified.

set -euo pipefail

CLUSTER="${ECS_CLUSTER:?Set ECS_CLUSTER to your cluster name}"
SERVICE="${ECS_SERVICE:?Set ECS_SERVICE to your service name}"
CONNECTOR_FILE="${1:-$(dirname "$0")/../connectors/mysql-source.json}"
REGION="${AWS_REGION:-ap-south-1}"

CONNECTOR_NAME=$(python3 -c "import json; print(json.load(open('${CONNECTOR_FILE}'))['name'])")
CONNECTOR_JSON=$(cat "${CONNECTOR_FILE}")

echo "Cluster  : ${CLUSTER}"
echo "Service  : ${SERVICE}"
echo "Connector: ${CONNECTOR_NAME}"
echo "File     : ${CONNECTOR_FILE}"
echo ""

# ── Find a running ECS task ────────────────────────────────────────────────────
TASK_ARN=$(aws ecs list-tasks \
  --cluster "${CLUSTER}" \
  --service-name "${SERVICE}" \
  --desired-status RUNNING \
  --region "${REGION}" \
  --query 'taskArns[0]' \
  --output text)

if [[ -z "${TASK_ARN}" || "${TASK_ARN}" == "None" ]]; then
  echo "ERROR: No running tasks found in ${SERVICE}. Is the service healthy?"
  exit 1
fi

echo "Using task: ${TASK_ARN}"
echo ""

run_in_task() {
  aws ecs execute-command \
    --cluster "${CLUSTER}" \
    --task "${TASK_ARN}" \
    --container "debezium-connect" \
    --region "${REGION}" \
    --interactive \
    --command "$1"
}

# ── Health check ───────────────────────────────────────────────────────────────
run_in_task "curl -sf http://localhost:8083/"

# ── Upsert: PUT if exists, POST if new ────────────────────────────────────────
EXISTING_STATUS=$(run_in_task "curl -sw '%{http_code}' -o /dev/null http://localhost:8083/connectors/${CONNECTOR_NAME}" 2>/dev/null || true)

if [[ "${EXISTING_STATUS}" == "200" ]]; then
  echo "Connector exists — updating config via PUT..."
  CONFIG_ONLY=$(python3 -c "import json; d=json.load(open('${CONNECTOR_FILE}')); print(json.dumps(d['config']))")
  run_in_task "curl -sf -X PUT -H 'Content-Type: application/json' -d '${CONFIG_ONLY}' http://localhost:8083/connectors/${CONNECTOR_NAME}/config"
else
  echo "Registering new connector via POST..."
  run_in_task "curl -sf -X POST -H 'Content-Type: application/json' -d '${CONNECTOR_JSON}' http://localhost:8083/connectors"
fi

echo ""
echo "Connector status (after ~3s):"
sleep 3
run_in_task "curl -sf http://localhost:8083/connectors/${CONNECTOR_NAME}/status"
