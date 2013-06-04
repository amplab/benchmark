#!/bin/bash
REDSHIFT_HOST=mycluster.ciw9wxhvytg3.us-east-1.redshift.amazonaws.com
REDSHIFT_USERNAME=username
REDSHIFT_PASSWORD=password
REDSHIFT_DATABASE=dev
NUM_TRIALS=5
RUN_DIR=..

queries=(1a)
out_file=redshift_`date +%s`

for i in "${queries[@]}"
do
  $RUN_DIR/run-query.sh \
    --redshift \
    --query-num=$i \
    --num-trials=$NUM_TRIALS \
    --redshift-host=$REDSHIFT_HOST \
    --redshift-username=$REDSHIFT_USERNAME \
    --redshift-database=$REDSHIFT_DATABASE \
    --redshift-password=$REDSHIFT_PASSWORD >> $out_file
done

