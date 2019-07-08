# flink_Kafka_cf
Transforming data in Apache Flink , sourced from Kafka topics.

Program Arguments:

--input-topic test-avro-input --output-topic test-avro-out
--bootstrap.servers localhost:9092
--zookeeper.connect localhost:2181
--group.id myconsumer --auto.offset.reset earliest
--schema-registry-url http://localhost:8081



Start Docker conatiner - docker-compose up -d

schema registry : http://localhost:8081/
Kafka restproxy : http://localhost:8082/
Kafka Connect: http://localhost:8083/

UI:
schema registry UI : http://localhost:8001/
Kafka Topics UI : http://localhost:8000/#/
Kafka Connect UI: http://localhost:8003/#/cluster/kafka-connect-1
