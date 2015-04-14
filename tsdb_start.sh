#!/bin/bash
echo "start tsdb begin..."

nohup ./tsdb ./config.json &

echo "start tsdb end."
