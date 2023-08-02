#!/bin/sh

# To parse JSON output
#apt-get install jq -y
#sleep 10

# We need the returned endpoint
# Default: "es-local.us-east-1.es.localhost.localstack.cloud:4566"
awslocal es create-elasticsearch-domain --domain-name es-local

# Setup S3
awslocal s3 mb s3://kinesis-activity-backup-local

# Setup Firehose
awslocal firehose create-delivery-stream --delivery-stream-name activity-to-elasticsearch-local --elasticsearch-destination-configuration "RoleARN=arn:aws:iam::000000000000:role/Firehose-Reader-Role,DomainARN=arn:aws:es:us-east-1:000000000000:domain/es-local,IndexName=activity,TypeName=activity,S3BackupMode=AllDocuments,S3Configuration={RoleARN=arn:aws:iam::000000000000:role/Firehose-Reader-Role,BucketARN=arn:aws:s3:::kinesis-activity-backup-local}"

# Wait for Elasticsearch to be reachable. Does not work
#until [awslocal es describe-elasticsearch-domain --domain-name es-local | jq ".DomainStatus.Processing" = 'false']
#do
#  echo "Elasticsearch is not up yet. Waiting..."
#  sleep 5
#done

echo "Initialized."