#!/usr/bin/zsh

docker-compose up -d
# need to wait for broker to be completely up
sleep 10
docker exec broker \
kafka-topics --bootstrap-server broker:9092 \
             --create \
             --topic ASx3\
             --partitions 3

