package com.spark.streaming;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "com.spark.streaming")
public class StreamingApp/* implements CommandLineRunner */{
	public static void main(String[] args) throws InterruptedException {
		SpringApplication.run(StreamingApp.class, args);
		SparkConf sparkConf = new SparkConf().setAppName("kafka-sandbox").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(2000));

		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "localhost:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "test-consumer-group");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);
		
		Collection<String> topics = Arrays.asList("dell-kafka-topic");
		
		JavaInputDStream<ConsumerRecord<String, String>> directKafkaStream = KafkaUtils.createDirectStream(
				ssc,
				LocationStrategies.PreferConsistent(), 
				ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
		);
		
		// directKafkaStream.mapToPair(record -> new Tuple2<>(record.key(), record.value()));
		directKafkaStream.foreachRDD(rdd -> {
			rdd.foreach(record -> System.out.println(record.key() + " ==> " + record.value()));
		});
		
		
		ssc.start();
		ssc.awaitTermination();
	}

	/*@Override
	public void run(String... arg0) throws Exception {
		SparkSession sparkSession = SparkSession.builder().appName("SparkWithSpring").master("local").getOrCreate();
		System.out.println("Printing spark version => Spark Version : " + sparkSession.version());
	}*/

}
