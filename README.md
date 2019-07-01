# flink_Kafka_cf
Transforming data in Apache Flink , sourced from Kafka topics.

Program Arguments:

--input-topic test-avro-input --output-topic test-avro-out
--bootstrap.servers localhost:9092
--zookeeper.connect localhost:2181
--group.id myconsumer --auto.offset.reset earliest
--schema-registry-url http://localhost:8081
