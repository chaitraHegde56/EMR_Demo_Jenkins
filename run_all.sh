#!/bin/bash



dir=$(cd -P -- "$(dirname -- "$0")" && pwd -P)
#cd ${dir}
echo $dir/build
cd ${dir/build}
make build
cd ${dir}

bash run_emr.sh

