#!/bin/bash
CMD="kafka-console-producer.sh --broker-list 127.0.0.1:9092 --topic $1"
echo running \"$CMD\"
docker exec -it kafka_server $CMD

