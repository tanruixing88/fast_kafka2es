#!/usr/bin/env bash
mvn clean
mvn compile
mvn exec:java -Dexec.mainClass="Main" -Dexec.args="-c conf/someone.yaml" -DLOG_HOME=/home/web_server/work/fast_kafka2es/logs
