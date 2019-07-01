package com.example.flink_kafka.datastreams.example;

import example.avro.test_avro_input;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import java.util.Properties;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
//import org.apache.flink.api.java.DataSet;
//import org.apache.flink.api.java.ExecutionEnvironment;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.avro.specific.SpecificRecordBase;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.table.api.TableEnvironment;
//import org.apache.flink.table.api.java.StreamTableEnvironment;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

/**
 * A simple example that shows how to read from and write to Kafka with Confluent Schema Registry.
 * This will read AVRO messages from the input topic, parse them into a POJO type via checking the Schema by calling Schema registry.
 * Then this example publish the POJO type to kafka by converting the POJO to AVRO and verifying the schema.
 * --input-topic test-input --output-topic test-output --bootstrap.servers localhost:9092 --zookeeper.connect localhost:2181 --schema-registry-url http://localhost:8081 --group.id myconsumer
 */
public class JobAvroOperations {

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
        Properties kafkaConfig = new Properties();
        kafkaConfig.setProperty("bootstrap.servers", parameterTool.getRequired("bootstrap.servers"));
        kafkaConfig.setProperty("group.id", parameterTool.getRequired("group.id"));
        kafkaConfig.setProperty("zookeeper.connect", parameterTool.getRequired("zookeeper.connect"));
        String schemaRegistryUrl = parameterTool.getRequired("schema-registry-url");

        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
//        sEnv.getConfig().disableSysoutLogging();
//        sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        String stateDir = "file:///Users/s101531/workspace/flink_datastreams/checkpoints";//parameterTool.get("stateDir");
        String socketHost = parameterTool.get("host");


        sEnv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        sEnv.setStateBackend(new RocksDBStateBackend(stateDir, false));
        sEnv.enableCheckpointing(60000);// start a checkpoint every 60seconds
        CheckpointConfig config = sEnv.getCheckpointConfig();
        config.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);// set mode to exactly-once (this is the default)
        config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);


        DataStreamSource<test_avro_input> sorStream1 = sEnv
                .addSource(
                        new FlinkKafkaConsumer<>(
                                parameterTool.getRequired("input-topic"),
                                ConfluentRegistryAvroDeserializationSchema.forSpecific(test_avro_input.class, schemaRegistryUrl),
                                kafkaConfig).setStartFromEarliest());//setStartFromLatest

//        sorStream1.print();

        DataStreamSource<test_avro_input> sorStream2 = sEnv
                .addSource(
                        new FlinkKafkaConsumer<>(
                                "test-avro-input2",
                                ConfluentRegistryAvroDeserializationSchema.forSpecific(test_avro_input.class, schemaRegistryUrl),
                                kafkaConfig).setStartFromEarliest()); //setStartFromEarliest


        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(sEnv);

        Table t1 = tableEnv.fromDataStream(sorStream1);
        Table t2 = tableEnv.fromDataStream(sorStream2);



        Table result1 = t1.select("name.lowerCase()") ;
        Table result2 = t2.select("name  as t2name, favoriteNumber as t2favoriteNumber ") ;



        Table result = t1.join(result2).where("name = t2name").select("name, t2name,  favoriteNumber ");
        tableEnv.toAppendStream(result, Row.class).print();


        System.out.println(sEnv.getExecutionPlan());


        sEnv.execute("Kafka 0.10 Confluent Schema Registry AVRO Example");
    }
}