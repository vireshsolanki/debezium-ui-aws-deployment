# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a **Debezium Kafka CDC (Change Data Capture) Pipeline** infrastructure-as-code project using AWS CDK. It deploys a production-ready system for real-time data synchronization from relational databases (MySQL, PostgreSQL) to Kafka topics using Debezium connectors.

## Architecture

```
Source Database (MySQL/PostgreSQL)
    ↓ (Change Capture via Debezium)
Kafka Connect Workers (Auto-scaling ECS Fargate, 2+ on-demand, rest Spot)
    ↓
Apache Kafka (AWS MSK with IAM auth)
    ↓
Debezium UI (Optional, ECS task behind ALB)
    ↓ (CDC topics: prefix-database-table)
Kafka Sinks (Elasticsearch, MongoDB, BigQuery, Snowflake, etc.)
```

### Key Components

- **Debezium Connect Workers**: ECS Fargate tasks running Kafka Connect with Debezium connectors. Uses custom Docker image with MSK IAM auth library.
- **Kafka Connect State Topics**: Four internal topics for cluster coordination (debezium-configs, debezium-offsets, debezium-status, debezium-schema-history).
- **Application Load Balancer (ALB)**: Internal-facing ALB (no internet-facing) for HTTPS termination and traffic routing. Requires ACM certificate.
- **Debezium UI Dashboard**: Optional web interface for connector management and monitoring (runs on ECS, exposed via ALB).
- **Auto-scaling**: CPU/memory-based scaling with Fargate Spot instances (cheaper) + 2 on-demand base tasks (MySQL binlog anchor — prevent lag).
- **CloudWatch Alarms**: Monitors for all-workers-down (72h binlog window = critical) and below-minimum worker count.

## Development Commands

### Setup

```bash
# Create Python virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install CDK and dependencies
npm install -g aws-cdk
pip install -r requirements.txt
```

### CDK Deployments

```bash
# Synthesize CloudFormation without deploying (generates cdk.out/)
cdk synth

# Show differences from current stack
cdk diff -c env=production

# Deploy to production AWS account
cdk deploy -c env=production
# Or deploy to staging (requires config/staging.yaml)
cdk deploy -c env=staging

# Destroy stack (removes all AWS resources)
cdk destroy -c env=production
```

### Connector Management

```bash
# Register or update a connector (via ECS Exec into a running task)
# Prerequisites: ECS_CLUSTER, ECS_SERVICE, AWS_REGION env vars
export ECS_CLUSTER=debezium-cdc-cluster
export ECS_SERVICE=debezium-cdc-connect
export AWS_REGION=ap-south-1
bash scripts/register-connector.sh connectors/mysql-source.json
```

### Monitoring & Debugging

```bash
# View Connect worker logs (ECS Fargate tasks)
aws logs tail /ecs/debezium-cdc/connect --follow

# View Debezium UI logs
aws logs tail /ecs/debezium-cdc/ui --follow

# List ECS tasks
aws ecs list-tasks --cluster debezium-cdc-cluster --service-name debezium-cdc-connect

# Describe a specific service (running count, scaling status)
aws ecs describe-services --cluster debezium-cdc-cluster --services debezium-cdc-connect

# Get CloudWatch alarms status
aws cloudwatch describe-alarms --alarm-names debezium-cdc-workers-down debezium-cdc-workers-below-min
```

## Project Structure

```
.
├── app.py                               # CDK app entry point — loads config, instantiates stack
├── cdk.json                             # CDK CLI configuration (watch patterns, context values)
├── cdk.context.json                     # CDK cached context values (VPC lookups, AZs, etc.)
├── requirements.txt                     # Python dependencies (aws-cdk-lib, pyyaml, boto3)
│
├── config/
│   ├── production.yaml.example          # Template for production deployment
│   └── production.yaml                  # Actual config (add to .gitignore — contains secrets)
│
├── stacks/
│   ├── __init__.py
│   └── debezium_stack.py               # Main CDK Stack — defines all AWS resources
│
├── connectors/
│   └── mysql-source.json               # Example Debezium connector config (name, connection, topics)
│
├── scripts/
│   └── register-connector.sh            # Helper script for registering connectors via ECS Exec
│
├── docker/
│   └── Dockerfile                       # Custom Debezium Connect image with MSK IAM auth library
│
├── debezium-plugin/                     # (Optional) Custom Debezium plugins directory
└── cdk.out/                             # Generated CloudFormation templates (don't commit)
```

## Configuration Management

All deployment parameters come from YAML files in `config/`:

```yaml
# config/production.yaml
account: "123456789012"                  # AWS account ID
region: "ap-south-1"                     # AWS region
vpc_id: "vpc-xxxxxxx"                    # Existing VPC to deploy into
private_subnet_ids:                      # 3+ subnets (one per AZ)
  - "subnet-aaa"
  - "subnet-bbb"
  - "subnet-ccc"

msk_cluster_arn: "arn:aws:kafka:..."    # Pre-existing MSK cluster
msk_bootstrap_servers: "b-1.xxx:9098,..." # MSK brokers (IAM auth, port 9098)

naming_prefix: "debezium-cdc"            # Resource naming convention
connect_group_id: "debezium-cdc-connect" # Kafka Connect group ID
cdc_database: "your_database"            # Source database name
cdc_tables:                              # Tables to capture
  - "table_one"
  - "table_two"

domain_name: "debezium.yourdomain.com"   # (Optional) custom domain for ALB
acm_certificate_arn: "arn:aws:acm:..."   # ACM certificate for HTTPS

# ECS task sizing
debezium_worker_count: 3                 # Initial task count
debezium_worker_min: 2                   # Auto-scale floor (keep ≥2 for binlog safety)
debezium_worker_max: 10                  # Auto-scale ceiling
debezium_cpu: 1024                       # Task CPU units (1 vCPU)
debezium_memory_mib: 2048                # Task memory (2 GB)

# Debezium & UI versions
debezium_image_tag: "2.7"                # Debezium version (impacts docker build)
ui_image_tag: "latest"                   # Debezium UI version
deploy_ui: true                          # Deploy UI dashboard

# Topic names (must be created beforehand in MSK)
kafka_topics:
  config: "debezium-configs"
  offsets: "debezium-offsets"
  status: "debezium-status"

paused: false                            # Set true to scale all tasks to 0 (cost save)
environment: "production"                # Environment tag

tags:                                    # Custom AWS tags
  Product: "YourProduct"
  ManagedBy: "CDK"
```

### Pre-deployment Checklist

1. **AWS Prerequisite Resources**:
   - VPC with 3+ private subnets in different AZs
   - MSK cluster (IAM auth enabled, SASL_SSL on port 9098)
   - RDS MySQL instance (5.7+) or PostgreSQL
   - ACM certificate for domain
   - AWS Secrets Manager secret for DB credentials (created by stack, then update manually)

2. **Kafka State Topics** (must be created before deploying connectors):
   ```bash
   kafka-topics.sh --bootstrap-server <MSK_BROKER>:9098 \
     --create --topic debezium-configs --partitions 1 --replication-factor 3
   kafka-topics.sh --bootstrap-server <MSK_BROKER>:9098 \
     --create --topic debezium-offsets --partitions 25 --replication-factor 3
   kafka-topics.sh --bootstrap-server <MSK_BROKER>:9098 \
     --create --topic debezium-status --partitions 5 --replication-factor 3
   kafka-topics.sh --bootstrap-server <MSK_BROKER>:9098 \
     --create --topic debezium-schema-history --partitions 1 --replication-factor 3
   ```

3. **Secrets Manager** (created automatically by CDK as `{prefix}/mysql-credentials`):
   ```bash
   aws secretsmanager update-secret \
     --secret-id debezium-cdc/mysql-credentials \
     --secret-string '{"username":"user","password":"pass"}'
   ```

## Key Files Deep Dive

### `app.py`
- **Role**: CDK app entry point
- **Key logic**: Loads environment-specific YAML config, validates file exists, instantiates `DebeziumHAStack`
- **Important**: Strips whitespace from multiline YAML strings (for `msk_bootstrap_servers`)

### `stacks/debezium_stack.py`
- **Role**: Defines all AWS resources (ECS, ALB, Security Groups, IAM, CloudWatch, etc.)
- **Key sections**:
  - **Security Groups**: Three groups (ALB, Connect, UI) with fine-grained ingress rules
  - **IAM Roles**: Task role (MSK permissions) + Execution role (ECR pulls, CloudWatch logs, SSM)
  - **ECS Cluster**: With ContainerInsights v2 enabled
  - **Connect Task Definition**: Custom Docker image, environment variables (Kafka Connect config)
  - **Connect Service**: Fargate with capacity provider strategies (base 2 on-demand, scale-out via Spot)
  - **Auto-scaling**: CPU (70%) and memory (80%) based scaling
  - **ALB**: Internal-facing, HTTPS termination, path routing
  - **UI Task Definition** (optional): Separate task for Debezium UI dashboard
  - **CloudWatch Alarms**: All-workers-down (CRITICAL) + below-minimum (WARNING)

### `docker/Dockerfile`
- **Role**: Custom Debezium Connect image with MSK IAM auth
- **What it does**: Builds on `quay.io/debezium/connect:{VERSION}`, adds AWS MSK IAM auth JAR to classpath
- **Version control**: Debezium version passed via build arg from config (`debezium_image_tag`)

### `connectors/mysql-source.json`
- **Role**: Example Debezium connector configuration
- **Critical fields**:
  - `database.hostname`, `database.port`, `database.user` (via Secrets Manager)
  - `database.include.list`, `table.include.list`: Scope of CDC
  - `topic.prefix`: Determines Kafka topic names (e.g., `dz-database-table`)
  - `snapshot.mode`: Initial snapshot strategy
  - `schema.history.internal.kafka.topic`: Schema history tracking

### `scripts/register-connector.sh`
- **Role**: Helper for connector registration via ECS Exec (no ALB needed)
- **Prerequisites**: AWS CLI v2, session-manager-plugin, jq
- **Logic**: Finds running Fargate task, curls Connect REST API to POST/PUT connector config
- **Upsert behavior**: Checks if connector exists; PUT if exists, POST if new

## Important Architectural Decisions

1. **MSK IAM Auth**: Uses AWS MSK IAM authentication (not plaintext/SCRAM). Requires custom Docker image with `aws-msk-iam-auth` JAR.

2. **Capacity Provider Strategy**:
   - **Base 2 on-demand**: Ensures ≥2 workers always running (MySQL binlog is 72-hour window; if all workers die for >72h, binlog is purged and CDC falls behind)
   - **Spot for scale-out**: ~70% cheaper than on-demand
   - Rebalancing during Spot interruption is automatic via Kafka Connect heartbeat/session timeout

3. **Internal ALB**: Not internet-facing; accessed via VPC peering or private link from VPC. HTTPS termination is VPC-internal (no public internet exposure).

4. **CloudWatch Alarms**: Two critical alerts:
   - All workers down: 3 evaluation periods × 1 min = ~3 min to detect, indicates 72h binlog loss risk
   - Below minimum: 5 evaluation periods × 1 min = ~5 min to detect, indicates HA degradation

5. **UI Optional**: Debezium UI is optional (can be disabled to save Fargate costs). Connector registration can be done entirely via script + ECS Exec.

## Common Development Tasks

### Add a New Connector
1. Create `connectors/my-source.json` with Debezium connector config
2. Ensure Kafka topics exist (`debezium-configs`, etc.)
3. Update DB credentials in Secrets Manager if needed
4. Register via: `bash scripts/register-connector.sh connectors/my-source.json`

### Scale Up/Down Workers
1. Edit `config/production.yaml`: adjust `debezium_worker_count`, `debezium_worker_min`, `debezium_worker_max`
2. Run: `cdk diff -c env=production` to preview changes
3. Run: `cdk deploy -c env=production`

### Pause/Resume Deployment (Cost Optimization)
1. Edit `config/production.yaml`: set `paused: true`
2. Run: `cdk deploy -c env=production`
3. All ECS tasks scale to 0, MSK cluster remains available (MSK charges still apply)

### Update Debezium Version
1. Edit `config/production.yaml`: change `debezium_image_tag` (e.g., "2.8")
2. Run: `cdk deploy -c env=production`
3. ECS will perform rolling restart with new image

### Investigate High Lag
1. Check Connect worker logs: `aws logs tail /ecs/debezium-cdc/connect --follow`
2. Check Kafka lag via Debezium UI or AWS CLI
3. If CPU/memory high: increase `debezium_cpu`, `debezium_memory_mib` in config
4. If not enough workers: increase `debezium_worker_count`, `debezium_worker_max`

### Debug Connector Failure
1. Check ECS task logs: `aws logs tail /ecs/debezium-cdc/connect --follow`
2. ECS Exec into task: `aws ecs execute-command --cluster ... --task ... --container debezium-connect --interactive`
3. Inside task, curl Connect API: `curl http://localhost:8083/connectors/my-connector/status`
4. Check Secrets Manager for DB credentials: `aws secretsmanager get-secret-value --secret-id debezium-cdc/mysql-credentials`

## Deployment Targets

- **Production**: `cdk deploy -c env=production` (uses `config/production.yaml`)
- **Staging**: Create `config/staging.yaml`, then `cdk deploy -c env=staging`
- **Local testing**: `cdk synth` (generates CloudFormation in `cdk.out/`) without deploying

## Security Considerations

- **No public endpoints**: ALB is internal-facing only
- **IAM roles**: Least-privilege; Connect tasks can only access their MSK cluster and state topics
- **Secrets Manager**: DB credentials stored in AWS Secrets Manager, not in code or config
- **Encryption in transit**: ALB → ECS via VPC, ECS → MSK via TLS (SASL_SSL)
- **Network isolation**: Private subnets, security groups restrict traffic to MSK ports

## Gotchas & Troubleshooting

1. **72-hour binlog window**: MySQL binlog is purged after 72 hours. If all Connect workers die, CDC falls behind and cannot recover. Mitigated by keeping `debezium_worker_min ≥ 2`.

2. **State topics immutable**: Never rename or delete `debezium-configs`, `debezium-offsets`, `debezium-status`, `debezium-schema-history` topics — Kafka Connect relies on them for state.

3. **MSK IAM auth failures**: Common cause of connector startup failures. Ensure:
   - MSK cluster has IAM auth enabled (not just SCRAM)
   - ECS task role has correct IAM permissions
   - MSK IAM auth JAR is in Kafka Connect classpath (check custom Docker image)

4. **ALB requires certificate**: If deploying with custom domain, ACM certificate must exist in AWS and ARN must be in config.

5. **Whitespace in YAML**: Multiline strings like `msk_bootstrap_servers` are automatically stripped in `app.py` to avoid parsing issues.

## Testing & Validation

```bash
# Validate CDK template syntax
cdk synth -c env=production 2>&1 | grep -E "error|Error" || echo "✓ Syntax OK"

# Check CloudFormation diff before deploying
cdk diff -c env=production | head -50

# Check ECS task health after deployment
aws ecs describe-services --cluster debezium-cdc-cluster --services debezium-cdc-connect | jq '.services[0].deployments'

# Verify MSK connectivity from task
aws ecs execute-command \
  --cluster debezium-cdc-cluster \
  --task <TASK_ARN> \
  --container debezium-connect \
  --interactive \
  --command "/bin/bash -c 'echo test | nc -v b-1.msk.aws.com 9098'"
```
