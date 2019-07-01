package com.example.flink_kafka.datastreams.example;


import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import java.util.Properties;

//import org.apache.flink.api.common.state.ValueStateDescriptor;
//import org.apache.flink.api.common.typeinfo.Types;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.util.Collector;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.api.common.state.ValueState;
//import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;


public class JobStringPrint {



	private static final String CONSUMER_GROUP = "play-flink";
	private static final String KAFKA_CONSUMER_BROKER = "localhost:9092";
	private static final String KAFKA_TOPIC = "ranjit-topic-string-value";



	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();

		Properties consumerProperties = kafkaConsumer(KAFKA_CONSUMER_BROKER, CONSUMER_GROUP);
		FlinkKafkaConsumer<String> topicStream1 = createStringConsumerForTopic(KAFKA_TOPIC, consumerProperties);
		DataStream<String> first = sEnv.addSource(topicStream1);


		first.print();

		sEnv.execute("Flink AVRO KAFKA Test");
	}


	public static Properties kafkaConsumer(String brokers, String consumerGroup ) {
		Properties props = new Properties();
		props.setProperty("bootstrap.servers", brokers);
		props.setProperty("group.id",consumerGroup);
		return props;
	}


	public static FlinkKafkaConsumer<String> createStringConsumerForTopic( String topic, Properties props ) {
		FlinkKafkaConsumer<String>  stream =  new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), props);
//		stream.setStartFromEarliest();
		stream.setStartFromGroupOffsets();
		return stream;
	}


}
