#!/usr/bin/env bash


SCHEMA_REGISTRY_PORT=8081
SCHEMA_REGISTRY_URL=http://localhost:${SCHEMA_REGISTRY_PORT}


function start_kafka_cluster {

    docker-compose up -d
}

function stop_kafka_cluster {
    docker-compose down
}

function create_kafka_topic {
  kafka-topics  --create --zookeeper localhost:2181 --replication-factor $1 --partitions $2 --topic $3
}

function send_messages_to_kafka {
  echo -e $1 | kafka-console-producer --broker-list localhost:9092 --topic $2
}

function read_messages_from_kafka {
  kafka-console-consumer --bootstrap-server localhost:9092 --from-beginning \
    --max-messages $1 \
    --topic $2 \
    --consumer-property group.id=$3 2> /dev/null
}

function send_messages_to_kafka_avro {
echo -e $1 | kafka-avro-console-producer --broker-list localhost:9092 --topic $2 --property value.schema=$3 --property schema.registry.url=${SCHEMA_REGISTRY_URL}
}

function modify_num_partitions {
  kafka-topics --alter --topic $1 --partitions $2 --zookeeper localhost:2181
}

function get_num_partitions {
  kafka-topics --describe --topic $1 --zookeeper localhost:2181 | grep -Eo "PartitionCount:[0-9]+" | cut -d ":" -f 2
}

function get_partition_end_offset {
  local topic=$1
  local partition=$2

  kafka-run-class  kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic $topic --partitions $partition --time -1 | cut -d ":" -f 3
}


function get_schema_subjects {
    curl "${SCHEMA_REGISTRY_URL}/subjects" 2> /dev/null || true
}

function get_and_verify_schema_subjects_exist {
    QUERY_RESULT=$(get_schema_subjects)

    [[ ${QUERY_RESULT} =~ \[.*\] ]]
}
