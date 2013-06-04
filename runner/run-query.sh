#!/bin/sh

cd "`dirname $0`"
PYTHONPATH="./deps/:$PYTHONPATH" python ./run_query.py $@
