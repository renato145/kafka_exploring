#!/bin/bash
CMD="kafka-consumer-groups.sh --bootstrap-server 127.0.0.1:9092 --group $1 --reset-offsets --to-earliest --execute --topic $2"
echo running \"$CMD\"
docker exec -it kafka_server $CMD

