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

SHARK_HOST=ec2-184-73-94-24.compute-1.amazonaws.com
SHARK_IDENTITY_FILE=~/.ssh/patkey.pem
NUM_TRIALS=2
RUN_DIR=..
queries=(1a)
out_file=shark_`date +%s`

for i in "${queries[@]}"
do
  $RUN_DIR/run-query.sh \
    --shark \
    --query-num=$i \
    --reduce-tasks=500 \
    --num-trials=$NUM_TRIALS \
    --shark-host=$SHARK_HOST \
    --shark-identity-file=$SHARK_IDENTITY_FILE >> $out_file
done


