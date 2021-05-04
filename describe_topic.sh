#!/bin/bash
CMD="kafka-topics.sh --zookeeper zookeeper:2181 --topic $1 --describe"
echo running \"$CMD\"
docker exec kafka_server $CMD

