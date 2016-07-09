#!/bin/bash

export KAFKA_HEAP_OPTS="-Xmx256M -Xms256M"

sudo /etc/init.d/postgresql start

cd kafka_2.11-0.10.0.0
bin/zookeeper-server-start.sh config/zookeeper.properties > /dev/null &

bin/kafka-server-start.sh ../server.properties