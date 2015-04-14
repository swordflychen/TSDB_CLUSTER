#!/bin/bash

echo "stop tsdb begin..."

work_path=$(grep work_path ./config.json | awk -F "\"" '{print $4}')
kill $(cat ${work_path}/tsdb.pid) 

echo "stop tsdb end"
