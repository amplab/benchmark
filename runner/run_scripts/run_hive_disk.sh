#!/bin/bash
IMPALA_HOSTS=ec2-72-44-48-9.compute-1.amazonaws.com,\
ec2-184-73-2-80.compute-1.amazonaws.com,\
ec2-54-226-72-108.compute-1.amazonaws.com,\
ec2-23-20-84-156.compute-1.amazonaws.com,\
ec2-54-225-38-165.compute-1.amazonaws.com
IMPALA_IDENTITY_FILE=~/.ssh/patkey.pem
NUM_TRIALS=5
RUN_DIR=..

queries=(1a)
out_file=hive_disk_`date +%s`

for i in "${queries[@]}"
do
  $RUN_DIR/run-query.sh \
    --impala \
    --query-num=$i \
    --impala-use-hive \
    --clear-buffer-cache \
    --num-trials=$NUM_TRIALS \
    --impala-hosts=$IMPALA_HOSTS \
    --impala-identity-file=$IMPALA_IDENTITY_FILE >> $out_file
done
