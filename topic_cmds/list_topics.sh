#!/bin/bash
CMD="kafka-topics.sh --zookeeper zookeeper:2181 --list"
echo running \"$CMD\"
docker exec kafka_server $CMD
