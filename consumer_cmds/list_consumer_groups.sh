#!/bin/bash
CMD="kafka-consumer-groups.sh --bootstrap-server 127.0.0.1:9092 --list"
echo running \"$CMD\"
docker exec kafka_server $CMD

