#!/bin/bash
SHARK_HOST=ec2-184-73-94-24.compute-1.amazonaws.com
SHARK_IDENTITY_FILE=~/.ssh/patkey.pem
NUM_TRIALS=2
RUN_DIR=..

queries=(1a)
out_file=shark_disk_`date +%s`

for i in "${queries[@]}"
do
  $RUN_DIR/run-query.sh \
    --shark \
    --query-num=$i \
    --shark-no-cache \
    --clear-buffer-cache \
    --reduce-tasks=500 \
    --num-trials=$NUM_TRIALS \
    --shark-host=$SHARK_HOST \
    --shark-identity-file=$SHARK_IDENTITY_FILE >> $out_file
done


