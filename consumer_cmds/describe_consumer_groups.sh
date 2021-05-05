#!/bin/bash
CMD="kafka-consumer-groups.sh --bootstrap-server 127.0.0.1:9092 --describe --group $1"
echo running \"$CMD\"
docker exec -it kafka_server $CMD

