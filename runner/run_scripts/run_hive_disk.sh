#!/bin/bash

# Copyright 2013 The Regents of The University California
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
