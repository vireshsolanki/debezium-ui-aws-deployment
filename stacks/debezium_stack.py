from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    CfnOutput,
    Tags,
    aws_ec2 as ec2,
    aws_ecs as ecs,
    aws_iam as iam,
    aws_logs as logs,
    aws_cloudwatch as cloudwatch,
    aws_elasticloadbalancingv2 as elbv2,
)
from constructs import Construct
import os



class DebeziumHAStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, *, config: dict, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ── Config ────────────────────────────────────────────────────────────
        vpc_id          = config["vpc_id"]
        subnet_ids      = config["private_subnet_ids"]
        msk_cluster_arn = config["msk_cluster_arn"]
        msk_bootstrap   = config["msk_bootstrap_servers"]
        paused          = config.get("paused",                False)
        worker_count    = 0 if paused else config.get("debezium_worker_count", 3)
        worker_min      = 0 if paused else config.get("debezium_worker_min",   2)
        worker_max      = 0 if paused else config.get("debezium_worker_max",   10)
        cpu             = config.get("debezium_cpu",          1024)
        memory_mib      = config.get("debezium_memory_mib",   2048)
        image_tag       = config.get("debezium_image_tag",    "2.7")
        group_id        = config.get("connect_group_id",      "debezium-kafka-cdc-cluster")
        topics          = config.get("kafka_topics", {})
        environment     = config.get("environment",           "production")
        deploy_ui       = config.get("deploy_ui",             True)
        ui_image_tag    = config.get("ui_image_tag",          "latest")
        cert_arn        = config.get("acm_certificate_arn")
        domain_name     = config.get("domain_name")

        # ── Naming convention: debezium-kafka-cdc-{component} ─────────────────
        # Controlled via naming_prefix in production.yaml
        prefix = config.get("naming_prefix", "debezium-kafka-cdc")

        def name(component: str) -> str:
            return f"{prefix}-{component}"

        # ── Stack-level tags — auto-propagate to every resource ───────────────
        for key, value in config.get("tags", {}).items():
            Tags.of(self).add(key, value)
        Tags.of(self).add("Environment", environment)

        # ── VPC ───────────────────────────────────────────────────────────────
        vpc = ec2.Vpc.from_lookup(self, "VPC", vpc_id=vpc_id)

        private_subnets = [
            ec2.Subnet.from_subnet_id(self, f"Subnet{i}", sid)
            for i, sid in enumerate(subnet_ids)
        ]

        # ── Security Groups ───────────────────────────────────────────────────
        alb_sg = ec2.SecurityGroup(
            self, "AlbSG",
            vpc=vpc,
            security_group_name=name("alb-sg"),
            description="Debezium ALB - UI + Connect REST management",
            allow_all_outbound=True,
        )
        alb_sg.add_ingress_rule(
            peer=ec2.Peer.ipv4(vpc.vpc_cidr_block),
            connection=ec2.Port.tcp(80),
            description="HTTP redirect to HTTPS",
        )
        alb_sg.add_ingress_rule(
            peer=ec2.Peer.ipv4(vpc.vpc_cidr_block),
            connection=ec2.Port.tcp(443),
            description="HTTPS Debezium UI",
        )
        alb_sg.add_ingress_rule(
            peer=ec2.Peer.ipv4(vpc.vpc_cidr_block),
            connection=ec2.Port.tcp(8083),
            description="HTTP Kafka Connect REST API internal VPC only",
        )

        connect_sg = ec2.SecurityGroup(
            self, "ConnectSG",
            vpc=vpc,
            security_group_name=name("connect-sg"),
            description="Debezium Kafka Connect workers",
            allow_all_outbound=True,
        )
        connect_sg.add_ingress_rule(
            peer=connect_sg,
            connection=ec2.Port.tcp(8083),
            description="Worker-to-worker REST forwarding during rebalance",
        )
        connect_sg.add_ingress_rule(
            peer=alb_sg,
            connection=ec2.Port.tcp(8083),
            description="ALB to Connect REST API",
        )

        ui_sg = ec2.SecurityGroup(
            self, "UISG",
            vpc=vpc,
            security_group_name=name("ui-sg"),
            description="Debezium UI container",
            allow_all_outbound=True,
        )
        ui_sg.add_ingress_rule(
            peer=alb_sg,
            connection=ec2.Port.tcp(8080),
            description="ALB to UI container",
        )

        # ── IAM Roles ─────────────────────────────────────────────────────────
        task_role = iam.Role(
            self, "ConnectTaskRole",
            role_name=name("task-role"),
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
        )

        task_role.add_to_policy(iam.PolicyStatement(
            sid="MSKClusterConnect",
            actions=[
                "kafka-cluster:Connect",
                "kafka-cluster:AlterCluster",
                "kafka-cluster:DescribeCluster",
            ],
            resources=[msk_cluster_arn],
        ))

        arn_base     = ":".join(msk_cluster_arn.split(":")[:5])
        cluster_path = msk_cluster_arn.split(":cluster/")[1]

        task_role.add_to_policy(iam.PolicyStatement(
            sid="MSKTopicAccess",
            actions=[
                "kafka-cluster:CreateTopic",
                "kafka-cluster:DeleteTopic",
                "kafka-cluster:DescribeTopic",
                "kafka-cluster:AlterTopic",
                "kafka-cluster:WriteData",
                "kafka-cluster:ReadData",
                "kafka-cluster:DescribeTopicDynamicConfiguration",
                "kafka-cluster:AlterTopicDynamicConfiguration",
            ],
            resources=[f"{arn_base}:topic/{cluster_path}/*"],
        ))

        task_role.add_to_policy(iam.PolicyStatement(
            sid="MSKGroupAccess",
            actions=[
                "kafka-cluster:AlterGroup",
                "kafka-cluster:DescribeGroup",
            ],
            resources=[f"{arn_base}:group/{cluster_path}/*"],
        ))

        task_role.add_to_policy(iam.PolicyStatement(
            sid="ECSExec",
            actions=[
                "ssmmessages:CreateControlChannel",
                "ssmmessages:CreateDataChannel",
                "ssmmessages:OpenControlChannel",
                "ssmmessages:OpenDataChannel",
            ],
            resources=["*"],
        ))

        exec_role = iam.Role(
            self, "ExecRole",
            role_name=name("exec-role"),
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AmazonECSTaskExecutionRolePolicy"
                )
            ],
        )

        # ── CloudWatch Log Groups ─────────────────────────────────────────────
        connect_logs = logs.LogGroup(
            self, "ConnectLogs",
            log_group_name=f"/ecs/{prefix}/connect",
            retention=logs.RetentionDays.ONE_DAY,
            removal_policy=RemovalPolicy.DESTROY,
        )
        ui_logs = logs.LogGroup(
            self, "UILogs",
            log_group_name=f"/ecs/{prefix}/ui",
            retention=logs.RetentionDays.ONE_DAY,
            removal_policy=RemovalPolicy.DESTROY,
        )

        # ── ECS Cluster ───────────────────────────────────────────────────────
        cluster = ecs.Cluster(
            self, "Cluster",
            cluster_name=name("cluster"),
            vpc=vpc,
            container_insights_v2=ecs.ContainerInsights.ENABLED,
            enable_fargate_capacity_providers=True,
        )

        # ── Connect Task Definition ───────────────────────────────────────────
        connect_task_def = ecs.FargateTaskDefinition(
            self, "ConnectTaskDef",
            family=name("connect"),
            cpu=cpu,
            memory_limit_mib=memory_mib,
            task_role=task_role,
            execution_role=exec_role,
        )

        connect_container = connect_task_def.add_container(
            "debezium-connect",
            image=ecs.ContainerImage.from_asset(
                os.path.join(os.path.dirname(__file__), "..", "docker"),
                build_args={"DEBEZIUM_VERSION": image_tag},
            ),
            logging=ecs.LogDrivers.aws_logs(
                stream_prefix="connect",
                log_group=connect_logs,
            ),
            environment={
                # ── Kafka Connect cluster ──────────────────────────────────
                "BOOTSTRAP_SERVERS":                  msk_bootstrap,
                "GROUP_ID":                           group_id,
                "CONFIG_STORAGE_TOPIC":               topics.get("config",  "debezium-configs"),
                "CONFIG_STORAGE_REPLICATION_FACTOR":  "3",
                "OFFSET_STORAGE_TOPIC":               topics.get("offsets", "debezium-offsets"),
                "OFFSET_STORAGE_REPLICATION_FACTOR":  "3",
                "OFFSET_STORAGE_PARTITIONS":          "25",
                "STATUS_STORAGE_TOPIC":               topics.get("status",  "debezium-status"),
                "STATUS_STORAGE_REPLICATION_FACTOR":  "3",
                "STATUS_STORAGE_PARTITIONS":          "5",

                # ── Converters ─────────────────────────────────────────────
                "KEY_CONVERTER":                      "org.apache.kafka.connect.json.JsonConverter",
                "VALUE_CONVERTER":                    "org.apache.kafka.connect.json.JsonConverter",
                "KEY_CONVERTER_SCHEMAS_ENABLE":       "false",
                "VALUE_CONVERTER_SCHEMAS_ENABLE":     "false",

                # ── MSK IAM auth (worker / producer / consumer) ────────────
                "CONNECT_SECURITY_PROTOCOL":          "SASL_SSL",
                "CONNECT_SASL_MECHANISM":             "AWS_MSK_IAM",
                "CONNECT_SASL_JAAS_CONFIG":           "software.amazon.msk.auth.iam.IAMLoginModule required;",
                "CONNECT_SASL_CLIENT_CALLBACK_HANDLER_CLASS": "software.amazon.msk.auth.iam.IAMClientCallbackHandler",

                "CONNECT_PRODUCER_SECURITY_PROTOCOL": "SASL_SSL",
                "CONNECT_PRODUCER_SASL_MECHANISM":    "AWS_MSK_IAM",
                "CONNECT_PRODUCER_SASL_JAAS_CONFIG":  "software.amazon.msk.auth.iam.IAMLoginModule required;",
                "CONNECT_PRODUCER_SASL_CLIENT_CALLBACK_HANDLER_CLASS": "software.amazon.msk.auth.iam.IAMClientCallbackHandler",

                "CONNECT_CONSUMER_SECURITY_PROTOCOL": "SASL_SSL",
                "CONNECT_CONSUMER_SASL_MECHANISM":    "AWS_MSK_IAM",
                "CONNECT_CONSUMER_SASL_JAAS_CONFIG":  "software.amazon.msk.auth.iam.IAMLoginModule required;",
                "CONNECT_CONSUMER_SASL_CLIENT_CALLBACK_HANDLER_CLASS": "software.amazon.msk.auth.iam.IAMClientCallbackHandler",

                # ── HA / rebalance ─────────────────────────────────────────
                "CONNECT_HEARTBEAT_INTERVAL_MS":      "3000",
                "CONNECT_SESSION_TIMEOUT_MS":         "60000",
                "CONNECT_REBALANCE_TIMEOUT_MS":       "60000",

                # ── Throughput ─────────────────────────────────────────────
                "CONNECT_MAX_BATCH_SIZE":             "2048",
                "CONNECT_MAX_QUEUE_SIZE":             "8192",
                "CONNECT_POLL_INTERVAL_MS":           "100",
                "CONNECT_PRODUCER_BATCH_SIZE":        "131072",
                "CONNECT_PRODUCER_LINGER_MS":         "20",
                "CONNECT_PRODUCER_BUFFER_MEMORY":     "33554432",
                "CONNECT_PRODUCER_COMPRESSION_TYPE":  "lz4",

                "CONNECT_REST_ADVERTISED_PORT":       "8083",
            },
            health_check=ecs.HealthCheck(
                command=["CMD-SHELL", "curl -f http://localhost:8083/connectors || exit 1"],
                interval=Duration.seconds(30),
                timeout=Duration.seconds(5),
                retries=3,
                start_period=Duration.seconds(120),
            ),
        )
        connect_container.add_port_mappings(ecs.PortMapping(container_port=8083))

        # ── Connect ECS Service ───────────────────────────────────────────────
        connect_service = ecs.FargateService(
            self, "ConnectService",
            service_name=name("connect"),
            cluster=cluster,
            task_definition=connect_task_def,
            desired_count=worker_count,
            vpc_subnets=ec2.SubnetSelection(subnets=private_subnets),
            security_groups=[connect_sg],
            assign_public_ip=False,
            enable_execute_command=True,
            capacity_provider_strategies=[
                ecs.CapacityProviderStrategy(
                    capacity_provider="FARGATE",
                    base=2,      # 2 on-demand always running — binlog anchor
                    weight=1,
                ),
                ecs.CapacityProviderStrategy(
                    capacity_provider="FARGATE_SPOT",
                    base=0,
                    weight=3,    # scale-out goes to Spot (~70% cheaper)
                ),
            ],
            circuit_breaker=ecs.DeploymentCircuitBreaker(rollback=True),
            min_healthy_percent=67,
            max_healthy_percent=200,
        )

        # ── Connect Auto Scaling ──────────────────────────────────────────────
        scalable = connect_service.auto_scale_task_count(
            min_capacity=worker_min,
            max_capacity=worker_max,
        )
        scalable.scale_on_cpu_utilization(
            "CpuScaling",
            target_utilization_percent=70,
            scale_in_cooldown=Duration.seconds(300),
            scale_out_cooldown=Duration.seconds(60),
        )
        scalable.scale_on_memory_utilization(
            "MemoryScaling",
            target_utilization_percent=80,
            scale_in_cooldown=Duration.seconds(300),
            scale_out_cooldown=Duration.seconds(60),
        )

        # ── Connect Target Group ──────────────────────────────────────────────
        connect_tg = elbv2.ApplicationTargetGroup(
            self, "ConnectTG",
            vpc=vpc,
            target_group_name=name("connect-tg"),
            port=8083,
            protocol=elbv2.ApplicationProtocol.HTTP,
            target_type=elbv2.TargetType.IP,
            targets=[connect_service],
            health_check=elbv2.HealthCheck(
                path="/connectors",
                interval=Duration.seconds(30),
                healthy_threshold_count=2,
                unhealthy_threshold_count=3,
                timeout=Duration.seconds(5),
            ),
            deregistration_delay=Duration.seconds(30),
        )

        # ── CloudWatch Alarms (72h binlog window) ─────────────────────────────
        running_task_metric = cloudwatch.Metric(
            namespace="ECS/ContainerInsights",
            metric_name="RunningTaskCount",
            dimensions_map={
                "ClusterName": cluster.cluster_name,
                "ServiceName": connect_service.service_name,
            },
            period=Duration.minutes(1),
            statistic="Minimum",
        )

        cloudwatch.Alarm(
            self, "AllWorkersDownAlarm",
            alarm_name=name("workers-down"),
            alarm_description=(
                "CRITICAL: All Debezium workers are down — CDC stopped. "
                "MySQL binlog purge window is 72h. Recover immediately."
            ),
            metric=running_task_metric,
            threshold=1,
            comparison_operator=cloudwatch.ComparisonOperator.LESS_THAN_THRESHOLD,
            evaluation_periods=3,
            datapoints_to_alarm=3,
            treat_missing_data=cloudwatch.TreatMissingData.BREACHING,
        )

        cloudwatch.Alarm(
            self, "WorkersBelowMinAlarm",
            alarm_name=name("workers-below-min"),
            alarm_description=(
                f"WARNING: Workers below minimum ({worker_min}). "
                "HA degraded — one more failure stops CDC."
            ),
            metric=running_task_metric,
            threshold=worker_min,
            comparison_operator=cloudwatch.ComparisonOperator.LESS_THAN_THRESHOLD,
            evaluation_periods=5,
            datapoints_to_alarm=5,
            treat_missing_data=cloudwatch.TreatMissingData.BREACHING,
        )

        # ── ALB ───────────────────────────────────────────────────────────────
        # Created before UI so the ALB DNS token is available for KAFKA_CONNECT_URIS
        alb = elbv2.ApplicationLoadBalancer(
            self, "ALB",
            load_balancer_name=name("alb"),
            vpc=vpc,
            internet_facing=False,
            security_group=alb_sg,
            vpc_subnets=ec2.SubnetSelection(subnets=private_subnets),
        )

        # ── Debezium UI ───────────────────────────────────────────────────────
        ui_tg = None
        if deploy_ui:
            _host = domain_name or alb.load_balancer_dns_name
            # Port 8083 listener routes ALL paths to Connect TG — no path-routing whack-a-mole.
            # Plain HTTP is fine here; this is internal VPC traffic only.
            connect_base_url = f"http://{_host}:8083"

            ui_task_def = ecs.FargateTaskDefinition(
                self, "UITaskDef",
                family=name("ui"),
                cpu=512,
                memory_limit_mib=1024,
                execution_role=exec_role,
            )

            # Container must be added BEFORE creating the target group so CDK
            # can resolve the default containerName / port for load balancer registration.
            ui_task_def.add_container(
                "debezium-ui",
                image=ecs.ContainerImage.from_registry(
                    f"debezium/debezium-ui:{ui_image_tag}"
                ),
                logging=ecs.LogDrivers.aws_logs(
                    stream_prefix="ui",
                    log_group=ui_logs,
                ),
                environment={
                    "KAFKA_CONNECT_URIS": connect_base_url,
                },
                port_mappings=[ecs.PortMapping(container_port=8080)],
                health_check=ecs.HealthCheck(
                    command=["CMD-SHELL", "curl -f http://localhost:8080/ || exit 1"],
                    interval=Duration.seconds(30),
                    timeout=Duration.seconds(5),
                    retries=3,
                    start_period=Duration.seconds(60),
                ),
            )

            ui_service = ecs.FargateService(
                self, "UIService",
                service_name=name("ui"),
                cluster=cluster,
                task_definition=ui_task_def,
                desired_count=0 if paused else 1,
                vpc_subnets=ec2.SubnetSelection(subnets=private_subnets),
                security_groups=[ui_sg],
                assign_public_ip=False,
                circuit_breaker=ecs.DeploymentCircuitBreaker(rollback=True),
                min_healthy_percent=100,
                max_healthy_percent=200,
                capacity_provider_strategies=[
                    ecs.CapacityProviderStrategy(
                        capacity_provider="FARGATE_SPOT",
                        weight=1,
                    ),
                ],
            )

            ui_tg = elbv2.ApplicationTargetGroup(
                self, "UITG",
                vpc=vpc,
                target_group_name=name("ui-tg"),
                port=8080,
                protocol=elbv2.ApplicationProtocol.HTTP,
                target_type=elbv2.TargetType.IP,
                targets=[ui_service],
                health_check=elbv2.HealthCheck(
                    path="/",
                    interval=Duration.seconds(30),
                    healthy_threshold_count=2,
                    unhealthy_threshold_count=3,
                    timeout=Duration.seconds(5),
                ),
                deregistration_delay=Duration.seconds(15),
            )

        # Listener 1 — port 80 → permanent redirect to HTTPS
        alb.add_listener(
            "HttpListener",
            port=80,
            open=False,
            default_action=elbv2.ListenerAction.redirect(
                protocol="HTTPS",
                port="443",
                permanent=True,
            ),
        )

        # Listener 2 — port 443 HTTPS → UI only (no path routing needed)
        default_tg = ui_tg if (deploy_ui and ui_tg) else connect_tg
        alb.add_listener(
            "HttpsListener",
            port=443,
            open=False,
            certificates=[elbv2.ListenerCertificate.from_arn(cert_arn)] if cert_arn else [],
            ssl_policy=elbv2.SslPolicy.RECOMMENDED_TLS,
            default_action=elbv2.ListenerAction.forward([default_tg]),
        )

        # Listener 3 — port 8083 HTTP → Connect REST API (all paths, no TLS needed inside VPC)
        # KAFKA_CONNECT_URIS points here so the Quarkus UI backend can call any Connect endpoint
        # without ALB path-routing restrictions.
        alb.add_listener(
            "ConnectRestListener",
            port=8083,
            open=False,
            protocol=elbv2.ApplicationProtocol.HTTP,
            default_action=elbv2.ListenerAction.forward([connect_tg]),
        )

        # ── Outputs ───────────────────────────────────────────────────────────
        base_url = f"https://{domain_name}" if domain_name else f"https://{alb.load_balancer_dns_name}"

        CfnOutput(self, "Step1DNSRecord",
            value=alb.load_balancer_dns_name,
            description=f"STEP 1 - Create DNS CNAME: {domain_name or 'your-domain'} to this value",
        )

        CfnOutput(self, "Step2OpenUI",
            value=f"{base_url}/",
            description="STEP 2 - Open Debezium UI and register your MySQL connector",
        )

        CfnOutput(self, "ConnectRestApi",
            value=f"{base_url}/connectors",
            description="Kafka Connect REST API endpoint",
        )

        CfnOutput(self, "ECSClusterName",
            value=cluster.cluster_name,
        )

        CfnOutput(self, "ECSExecDebug",
            value=(
                f"TASK=$(aws ecs list-tasks --cluster {cluster.cluster_name} "
                f"--service-name {connect_service.service_name} --query 'taskArns[0]' --output text) && "
                f"aws ecs execute-command --cluster {cluster.cluster_name} "
                f"--task $TASK --container debezium-connect --interactive "
                f"--command 'curl -sf http://localhost:8083/connectors'"
            ),
            description="ECS Exec - direct Connect REST access for debugging",
        )
