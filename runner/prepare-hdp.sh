#!/bin/sh

cd "`dirname $0`"
PYTHONPATH="./deps/boto-2.4.1.zip/boto-2.4.1:$PYTHONPATH" python ./prepare_hdp.py $@
