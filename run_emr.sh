#!/usr/bin/env bash
dir=$(cd -P -- "$(dirname -- "$0")" && pwd -P)
cd ${dir}

bash setup.sh
source env/bin/activate
python3 emr.py --indicies $1
if [ $? -eq 0 ]; then
    exit 0
else
    exit 1
fi 
