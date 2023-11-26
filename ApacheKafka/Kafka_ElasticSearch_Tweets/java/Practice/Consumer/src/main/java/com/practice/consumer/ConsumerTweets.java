package com.practice.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.practice.library.Constants;


public class ConsumerTweets {
	private static final Logger log = LoggerFactory.getLogger(ConsumerTweets.class);
	public static ObjectMapper objectMapper = new ObjectMapper();

	public static void main(String[] args) {
		log.info("-- Start Consumer");

		String topic = Constants.TOPIC_TWEETS_HASHTAGS_LANG;		// Configurar el topic

		// create consumer configs
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BOOTSTRAP_SERVER);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, Constants.GROUP_ID);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		// create consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

		// get a reference to the current thread
		final Thread mainThread = Thread.currentThread();

		// adding the shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
				consumer.wakeup();

				// join the main thread to allow the execution of the code in the main thread
				try {
					mainThread.join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});

		try {

			// subscribe consumer to our topic(s)
			consumer.subscribe(Arrays.asList(topic));
			
			// Buscar offsets para cada partición y asignarlos al inicio
	        consumer.poll(Duration.ofMillis(0)); // Necesario para asignar las particiones
	        for (TopicPartition partition : consumer.assignment()) {
	            consumer.seekToBeginning(Collections.singleton(partition));
	        }

			// poll for new data
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

				for (ConsumerRecord<String, String> record : records) {
//					log.info("Key: " + record.key() + ", Value: " + record.value());
//					log.info("Partition: " + record.partition() + ", Offset:" + record.offset());
					log.info(record.value());
				}
			}

		} catch (WakeupException e) {
			log.info("Wake up exception!");
			// we ignore this as this is an expected exception when closing a consumer
		} catch (Exception e) {
			log.error("Unexpected exception", e);
		} finally {
			consumer.close(); // this will also commit the offsets if need be.
			log.info("The consumer is now gracefully closed.");
		}
		
		log.info("-- End Consumer");
	}
}
