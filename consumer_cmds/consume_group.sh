#!/bin/bash
CMD="kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic $1 --group $2"
echo running \"$CMD\"
docker exec -it kafka_server $CMD

