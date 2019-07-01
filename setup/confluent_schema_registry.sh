#!/usr/bin/env bash

# set -Eeuo pipefail

echo "Start"



source  ./kafka-common.sh

start_kafka_cluster


# TEST_PROGRAM_JAR=${END_TO_END_DIR}/flink-confluent-schema-registry/target/TestAvroConsumerConfluent.jar

INPUT_MESSAGE_1='{"name":"Alyssa","favoriteNumber":"250","favoriteColor":"green","eventType":"meeting"}'
INPUT_MESSAGE_2='{"name":"Charlie","favoriteNumber":"10","favoriteColor":"blue","eventType":"meeting"}'
INPUT_MESSAGE_3='{"name":"Ben","favoriteNumber":"7","favoriteColor":"red","eventType":"meeting"}'
test_avro_input_SCHEMA='{"namespace":"example.avro","type":"record","name":"test_avro_input","fields":[{"name":"name","type":"string","default":""},{"name":"favoriteNumber","type":"string","default":""},{"name":"favoriteColor","type":"string","default":""},{"name":"eventType","type":{"name":"EventType","type":"enum","symbols":["meeting"]}}]}'

#curl -X POST \
#  ${SCHEMA_REGISTRY_URL}/subjects/users-value/versions \
#  -H 'cache-control: no-cache' \
#  -H 'content-type: application/vnd.schemaregistry.v1+json' \
#  -d '{"schema": "{\"namespace\": \"example.avro\",\"type\": \"record\",\"name\": \"User\",\"fields\": [{\"name\": \"name\", \"type\": \"string\", \"default\": \"\"},{\"name\": \"favoriteNumber\",  \"type\": \"string\", \"default\": \"\"},{\"name\": \"favoriteColor\", \"type\": \"string\", \"default\": \"\"},{\"name\": \"eventType\",\"type\": {\"name\": \"EventType\",\"type\": \"enum\", \"symbols\": [\"meeting\"] }}]}"}'

echo "Sending messages to Kafka topic [test-avro-input] ..."

send_messages_to_kafka_avro $INPUT_MESSAGE_1 test-avro-input $test_avro_input_SCHEMA
#send_messages_to_kafka_avro $INPUT_MESSAGE_2 test-avro-input $test_avro_input_SCHEMA
#send_messages_to_kafka_avro $INPUT_MESSAGE_3 test-avro-input $test_avro_input_SCHEMA


# stop_kafka_cluster

echo "End"