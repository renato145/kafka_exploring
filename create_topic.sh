#!/bin/bash
CMD="kafka-topics.sh --zookeeper zookeeper:2181 --topic $1 --create --partitions ${2:-3} --replication-factor ${3:-1}"
echo running \"$CMD\"
# docker exec kafka_server $CMD

