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

