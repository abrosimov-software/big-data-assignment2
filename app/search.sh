#!/bin/bash
if [ $# -lt 1 ]; then
    echo "Usage: $0 \"your search query\""
    echo "Please provide a search query as an argument."
    exit 1
fi

echo "Searching for: $*"

if [ -d ".venv" ]; then
    source .venv/bin/activate
fi

export PYSPARK_DRIVER_PYTHON=$(which python) 

export PYSPARK_PYTHON=./.venv/bin/python

spark-submit \
    --master yarn \
    --deploy-mode client \
    --archives .venv.tar.gz#.venv \
    --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./.venv/bin/python \
    --conf spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON=$(which python) \
    --conf spark.yarn.submit.waitAppCompletion=true \
    app/query.py "$@"

if [ -d ".venv" ]; then
    deactivate
fi