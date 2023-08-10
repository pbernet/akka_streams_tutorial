#!/bin/sh

awslocal sqs create-queue --queue-name mysqs-queue --region us-east-1
echo "Initialized."