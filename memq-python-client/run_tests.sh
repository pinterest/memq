#!/usr/bin/env bash

set -o verbose

WD=$(pwd)
echo "current working directory: $WD"
if [ -z "$PYTHONPATH" ]; then
    PYTHONPATH=.
else
    PYTHONPATH=$PYTHONPATH:.
fi
echo "using PYTHONPATH: $PYTHONPATH"

python3 setup.py test