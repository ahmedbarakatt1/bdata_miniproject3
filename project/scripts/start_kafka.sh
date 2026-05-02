#!/bin/bash
echo "Starting Zookeeper..."
$KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties &
sleep 5

echo "Starting Kafka..."
$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties &
sleep 5

echo "Creating topic game-ratings..."
$KAFKA_HOME/bin/kafka-topics.sh --create --topic game-ratings --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1 2>/dev/null || echo "Topic may already exist."

echo "Kafka ready."
