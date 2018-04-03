#!/bin/bash

mvn clean
./build.sh example
mvn package
mvn exec:java -Dexec.args="--local --sleep 3600000 --resource /topology.yaml"