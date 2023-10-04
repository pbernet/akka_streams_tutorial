#!/bin/sh

# Setup Elasticsearch
# Default Elasticsearch endpoint: "es-local.us-east-1.es.localhost.localstack.cloud:4566"
awslocal es create-elasticsearch-domain --domain-name es-local

# Setup S3
awslocal s3 mb s3://kinesis-activity-backup-local

# Setup Firehose
awslocal firehose create-delivery-stream --delivery-stream-name activity-to-elasticsearch-local --elasticsearch-destination-configuration "RoleARN=arn:aws:iam::000000000000:role/Firehose-Reader-Role,DomainARN=arn:aws:es:us-east-1:000000000000:domain/es-local,IndexName=activity,TypeName=activity,S3BackupMode=AllDocuments,S3Configuration={RoleARN=arn:aws:iam::000000000000:role/Firehose-Reader-Role,BucketARN=arn:aws:s3:::kinesis-activity-backup-local}"

echo "Wait for Elasticsearch setup to complete..."