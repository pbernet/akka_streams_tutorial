#!/bin/sh

awslocal kinesis create-stream --stream-name testDataStreamProvisioned --shard-count 1
echo "Initialized."