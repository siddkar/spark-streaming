package com.spark.streaming;

import org.apache.spark.sql.SparkSession;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "com.spark.streaming")
public class StreamingApp implements CommandLineRunner {
	public static void main(String[] args) {
		SpringApplication.run(StreamingApp.class, args);
	}

	@Override
	public void run(String... arg0) throws Exception {
		SparkSession sparkSession = SparkSession
				.builder()
				.appName("SparkWithSpring")
				.master("local")
				.getOrCreate();
		System.out.println("Spark Version : " + sparkSession.version());
	}

}
