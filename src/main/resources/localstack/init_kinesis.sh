#!/bin/sh

awslocal kinesis create-stream --stream-name kinesisDataStreamProvisioned --shard-count 1
echo "Initialized."