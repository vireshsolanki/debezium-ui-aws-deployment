# Debezium Kafka CDC Pipeline

A production-ready AWS CDK infrastructure-as-code deployment for **Change Data Capture (CDC)** using **Debezium**, **Apache Kafka (MSK)**, and **Kafka Connect**. This solution enables real-time data synchronization from relational databases (MySQL, PostgreSQL, etc.) to any CDC sink (data lakes, warehouses, event streams, etc.).

## 🎯 Overview

This project provides:

- **Debezium Connectors** → Captures CDC events from MySQL, PostgreSQL, and other databases
- **Apache Kafka (AWS MSK)** → Event streaming and state management
- **Debezium UI** → Web dashboard for connector management and monitoring
- **Auto-scaling ECS** → Kafka Connect workers scale based on lag and load
- **TLS/HTTPS** → Production-ready security with ACM certificates
- **Infrastructure as Code** → AWS CDK for reproducible, cloud-agnostic deployments

### Architecture

```
Source Database (MySQL, PostgreSQL, etc.)
    ↓ (Change capture)
Debezium Connector
    ↓
Apache Kafka (MSK)
    ↓ (CDC Topics: prefix-database-table)
Debezium UI (Management & Monitoring Dashboard)
    ↓
Any Sink: Data Lake, Warehouse, Event Stream, etc.
  ├── Elasticsearch
  ├── MongoDB
  ├── Snowflake
  ├── BigQuery
  ├── Apache Doris
  ├── Pinot
  └── Custom sink
```

## 📋 Prerequisites

- **AWS Account** with IAM permissions for EC2, ECS, MSK, RDS, Secrets Manager, CloudWatch
- **AWS CLI v2** configured with default credentials
- **Python 3.9+** with pip
- **AWS CDK CLI**: `npm install -g aws-cdk`
- **Existing AWS Infrastructure**:
  - VPC with private subnets (3+ AZs recommended)
  - MSK cluster (IAM auth, port 9098 SASL_SSL)
  - RDS MySQL instance (5.7+ or 8.0)
  - ACM certificate (for domain)

## 🚀 Quick Start

### 1. Clone & Install

```bash
git clone <repo-url>
cd debezium-kafka-cdc

# Create and activate Python venv
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure Deployment

Copy the example config:

```bash
cp config/production.yaml.example config/production.yaml
```

Edit `config/production.yaml` with your AWS details:

```yaml
account: "123456789012"
region: "ap-south-1"
vpc_id: "vpc-xxxxxxx"
private_subnet_ids:
  - "subnet-aaaaa"  # AZ-a
  - "subnet-bbbbb"  # AZ-b
  - "subnet-ccccc"  # AZ-c

msk_cluster_arn: "arn:aws:kafka:ap-south-1:123456789012:cluster/your-msk/xxxx"
msk_bootstrap_servers: "b-1.your-msk.xxxx.c4.kafka.ap-south-1.amazonaws.com:9098,..."

naming_prefix: "debezium-cdc"
connect_group_id: "debezium-cdc-connect"
cdc_database: "your_database"
cdc_tables:
  - "table_one"
  - "table_two"

domain_name: "debezium.yourdomain.com"
acm_certificate_arn: "arn:aws:acm:ap-south-1:123456789012:certificate/xxxxx"

tags:
  Product: "YourProduct"
  ManagedBy: "CDK"
```

### 3. Deploy Infrastructure

```bash
# Synthesize the CloudFormation template
cdk synth

# Review changes (optional)
cdk diff -c env=production

# Deploy to AWS
cdk deploy -c env=production

# Confirm: Press 'y' to proceed
```

The deployment creates:
- ECS Fargate cluster with Debezium Connect workers
- Load Balancer with HTTPS (auto-redirect from HTTP)
- Security groups, IAM roles, CloudWatch logs
- Secrets Manager for credentials
- Auto-scaling policies based on Kafka lag

### 4. Configure MySQL Credentials

After deployment, update the MySQL secret in AWS Secrets Manager:

```bash
aws secretsmanager update-secret \
  --secret-id debezium-cdc/mysql-credentials \
  --secret-string '{
    "username": "your_db_user",
    "password": "your_db_password"
  }' \
  --region ap-south-1
```

### 5. Register Debezium Connector

Create `connectors/mysql-source.json`:

```json
{
  "name": "mysql-source",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "database.hostname": "your-rds.c1m3w8x.ap-south-1.rds.amazonaws.com",
    "database.port": "3306",
    "database.user": "${secretManager:debezium-cdc/mysql-credentials:username}",
    "database.password": "${secretManager:debezium-cdc/mysql-credentials:password}",
    "database.server.id": "223344",
    "database.include.list": "your_database",
    "table.include.list": "your_database.table_one,your_database.table_two",
    "topic.prefix": "dz",
    "topic.creation.enable": true,
    "topic.creation.default.replication.factor": 3,
    "topic.creation.default.partitions": 3,
    "snapshot.mode": "initial",
    "include.schema.changes": true,
    "schema.history.internal.kafka.topic": "debezium-schema-history",
    "schema.history.internal.kafka.bootstrap.servers": "b-1.your-msk.xxxx.c4.kafka.ap-south-1.amazonaws.com:9098,..."
  }
}
```

Register the connector:

```bash
# Set environment variables
export ECS_CLUSTER=debezium-cdc-cluster
export ECS_SERVICE=debezium-cdc-connect
export AWS_REGION=ap-south-1

# Register via ECS Exec
bash scripts/register-connector.sh connectors/mysql-source.json
```

Or via the Debezium UI (https://debezium.yourdomain.com):
- Navigate to "Create connector"
- Paste the JSON config
- Click "Deploy"

## 📊 Monitoring

### CloudWatch Logs

```bash
# View Connect worker logs
aws logs tail /debezium-cdc/connect --follow

# View Debezium UI logs
aws logs tail /debezium-cdc/ui --follow
```

### Kafka Topics

```bash
# List Debezium state topics
aws kafka list-topics-within-cluster \
  --cluster-arn arn:aws:kafka:ap-south-1:...

# Check topic offsets and lag
aws kafka describe-cluster \
  --cluster-arn arn:aws:kafka:ap-south-1:...
```

### ECS Metrics

```bash
# View task count and scaling
aws ecs describe-services \
  --cluster debezium-cdc-cluster \
  --services debezium-cdc-connect
```

## 📁 Project Structure

```
.
├── app.py                          # CDK app entry point
├── cdk.json                        # CDK configuration
├── requirements.txt                # Python dependencies
├── config/
│   ├── production.yaml.example     # Example production config
│   └── production.yaml             # Actual config (add to .gitignore)
├── stacks/
│   └── debezium_stack.py          # Main CDK stack
├── connectors/
│   └── mysql-source.json          # Debezium connector config
├── scripts/
│   └── register-connector.sh       # Connector registration helper
├── docker/                         # Custom Docker images (if any)
└── README.md                       # This file
```

## ⚙️ Configuration Reference

### Key Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `debezium_worker_count` | 3 | Initial task count |
| `debezium_worker_min` | 2 | Minimum tasks (auto-scale floor) |
| `debezium_worker_max` | 10 | Maximum tasks (auto-scale ceiling) |
| `debezium_image_tag` | "2.7" | Debezium Connect version |
| `debezium_cpu` | 1024 | CPU units per task (1 vCPU) |
| `debezium_memory_mib` | 2048 | RAM per task (2 GB) |
| `deploy_ui` | true | Deploy Debezium UI dashboard |
| `paused` | false | Set to `true` to scale to 0 |

### Kafka Topics

State topics must be created before deployment:

```bash
# Create via Kafka CLI (adjust broker endpoints)
kafka-topics.sh --bootstrap-server <MSK_BROKER>:9098 \
  --create --topic debezium-configs --partitions 1 --replication-factor 3
kafka-topics.sh --bootstrap-server <MSK_BROKER>:9098 \
  --create --topic debezium-offsets --partitions 25 --replication-factor 3
kafka-topics.sh --bootstrap-server <MSK_BROKER>:9098 \
  --create --topic debezium-status --partitions 5 --replication-factor 3
kafka-topics.sh --bootstrap-server <MSK_BROKER>:9098 \
  --create --topic debezium-schema-history --partitions 1 --replication-factor 3
```

## 🔐 Security

- **Encryption in Transit**: TLS 1.3 via ALB + ACM
- **Encryption at Rest**: RDS and MSK encryption enabled
- **Secrets Management**: AWS Secrets Manager for DB credentials
- **IAM Roles**: Least-privilege task roles
- **Network Isolation**: Private subnets, security groups for traffic control
- **Audit Logging**: CloudWatch Logs for compliance

## 🐛 Troubleshooting

### Connector Fails to Start

1. **Check task logs**:
   ```bash
   aws logs tail /debezium-cdc/connect --follow
   ```

2. **Verify credentials**:
   ```bash
   aws secretsmanager get-secret-value --secret-id debezium-cdc/mysql-credentials
   ```

3. **Check MSK connectivity**:
   - Ensure security group allows port 9098
   - Verify IAM auth is enabled on MSK

### High Lag / Slow Sync

1. **Increase workers** in `config/production.yaml`:
   ```yaml
   debezium_worker_count: 5
   debezium_worker_max: 15
   ```

2. **Increase task resources**:
   ```yaml
   debezium_cpu: 2048          # 2 vCPU
   debezium_memory_mib: 4096   # 4 GB
   ```

3. **Check MySQL binlog**:
   ```sql
   SHOW BINARY LOGS;
   SHOW VARIABLES LIKE 'binlog_%';
   ```

### Connector Status

```bash
# Via Debezium UI (https://debezium.yourdomain.com/connectors)
# Or via ECS Exec:
export ECS_CLUSTER=debezium-cdc-cluster
export ECS_SERVICE=debezium-cdc-connect

# Check via scripts/register-connector.sh
bash scripts/register-connector.sh  # Shows connector status
```

## 📝 Deployment Modes

### Production

```bash
cdk deploy -c env=production
```

### Staging

Create `config/staging.yaml`:

```bash
cp config/production.yaml config/staging.yaml
# Edit for staging AWS account/region
cdk deploy -c env=staging
```

### Pause/Resume (Cost Optimization)

```yaml
# Set in config
paused: true
```

```bash
cdk deploy -c env=production
# All ECS tasks scale to 0, MSK cluster remains available
```

## 🛠️ Development

### Local Testing

```bash
# Synthesize without deploying
cdk synth

# Validate template
cdk diff
```

### Update Debezium Version

Edit `config/production.yaml`:

```yaml
debezium_image_tag: "2.8"  # New version
```

Redeploy:

```bash
cdk deploy -c env=production
```

ECS will rolling-restart tasks with the new image.

## 📚 Additional Resources

- [Debezium Docs](https://debezium.io/documentation/)
- [MySQL Connector Guide](https://debezium.io/documentation/reference/stable/connectors/mysql.html)
- [AWS CDK Reference](https://docs.aws.amazon.com/cdk/)
- [Apache Kafka Documentation](https://kafka.apache.org/)
- [Apache Doris](https://doris.apache.org/) - Example sink
- [Elasticsearch](https://www.elastic.co/) - Example sink
- [MongoDB](https://www.mongodb.com/) - Example sink

## 📄 License

MIT License - See LICENSE file

## 🤝 Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/your-feature`)
3. Commit changes (`git commit -am 'Add feature'`)
4. Push to branch (`git push origin feature/your-feature`)
5. Open a Pull Request

## ⚠️ Important Notes

- **Secrets Security**: Never commit `config/production.yaml` with real credentials
- **Binlog Window**: Keep `debezium_worker_min >= 2` (MySQL binlog is 72 hours by default)
- **State Topics**: Do NOT rename or delete Kafka state topics (`debezium-*`)
- **Costs**: MSK and Fargate charges apply; pause deployments when not in use

## 📞 Support

- Check existing [Issues](https://github.com/yourusername/repo/issues)
- Review [Troubleshooting](#-troubleshooting) section
- Check Debezium community [forums](https://groups.google.com/forum/#!forum/debezium)

---

**Built with ❤️ using AWS CDK, Debezium, and Kafka**
