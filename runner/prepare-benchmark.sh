#!/bin/sh

cd "`dirname $0`"
PYTHONPATH="./deps/:$PYTHONPATH" python ./prepare_benchmark.py $@
