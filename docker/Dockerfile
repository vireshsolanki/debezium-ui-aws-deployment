ARG DEBEZIUM_VERSION=2.7
FROM quay.io/debezium/connect:${DEBEZIUM_VERSION}

# Add AWS MSK IAM authentication library to Kafka Connect's classpath.
# Required because MSK IAM auth uses a custom SASL mechanism not bundled
# with the default Kafka client.
ARG MSK_IAM_AUTH_VERSION=2.2.0
USER root

RUN curl -fsSL \
    "https://github.com/aws/aws-msk-iam-auth/releases/download/v${MSK_IAM_AUTH_VERSION}/aws-msk-iam-auth-${MSK_IAM_AUTH_VERSION}-all.jar" \
    -o /kafka/libs/aws-msk-iam-auth-${MSK_IAM_AUTH_VERSION}-all.jar

USER 1001
