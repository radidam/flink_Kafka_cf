package com.example.flink_kafka.datastreams.example;

import org.apache.flink.api.common.functions.MapFunction;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

import example.avro.test_avro_input;
import org.apache.avro.specific.SpecificRecordBase;

import java.util.Properties;

/**
 * A simple example that shows how to read from and write to Kafka with Confluent Schema Registry.
 * This will read AVRO messages from the input topic, parse them into a POJO type via checking the Schema by calling Schema registry.
 * Then this example publish the POJO type to kafka by converting the POJO to AVRO and verifying the schema.
 * --input-topic test-input --output-topic test-output --bootstrap.servers localhost:9092 --zookeeper.connect localhost:2181 --schema-registry-url http://localhost:8081 --group.id myconsumer
 */
public class JobAvroConsumerConfluent {

    public static void main(String[] args) throws Exception {
        // parse input arguments
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        if (parameterTool.getNumberOfParameters() < 6) {
            System.out.println("Missing parameters!\n" +
                    "Usage: Kafka --input-topic <topic> --output-topic <topic> " +
                    "--bootstrap.servers <kafka brokers> " +
                    "--zookeeper.connect <zk quorum> " +
                    "--schema-registry-url <confluent schema registry> --group.id <some id>");
            return;
        }
        Properties config = new Properties();
        config.setProperty("bootstrap.servers", parameterTool.getRequired("bootstrap.servers"));
        config.setProperty("group.id", parameterTool.getRequired("group.id"));
        config.setProperty("zookeeper.connect", parameterTool.getRequired("zookeeper.connect"));
//      Kafka  Schema registry Configurations
        String schemaRegistryUrl = parameterTool.getRequired("schema-registry-url");

//      Kafka  Security Configurations
//        config.setProperty("security.protocol", KAFKA_SECURITY_PROTOCOL);
//        config.setProperty("ssl.truststore.location",KAFKA_SSL_TRUSTSTORE_LOCATION);
//        config.setProperty("ssl.truststore.password",KAFKA_SSL_TRUSTSTORE_PASSWORD);
//        config.setProperty("ssl.keystore.location",KAFKA_SSL_KEYSTORE_LOCATION);
//        config.setProperty("ssl.keystore.password",KAFKA_SSL_KEYSTORE_PASSWORD);
//        config.setProperty("ssl.key.password ",KAFKA_SSL_KEYSTORE_PASSWORD);



        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableSysoutLogging();

        DataStreamSource<test_avro_input> input = env
                .addSource(
                        new FlinkKafkaConsumer<>(
                                parameterTool.getRequired("input-topic"),
                                ConfluentRegistryAvroDeserializationSchema.forSpecific(test_avro_input.class, schemaRegistryUrl),
                                config).setStartFromEarliest());

        input.print();

        SingleOutputStreamOperator<String> mapToString = input.map((MapFunction<test_avro_input, String>) SpecificRecordBase::toString);

        mapToString.print();

        env.execute("Kafka 0.10 Confluent Schema Registry AVRO Example");
    }
}