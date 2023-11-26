package com.practice.producer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.practice.library.Constants;
import com.practice.library.models.Hashtag;
import com.practice.library.models.HashtagSimple;
import com.practice.library.models.Tweet;

public class ProducerTweets {
	final static Logger log = LoggerFactory.getLogger(ProducerTweets.class);
	public static ObjectMapper objectMapper = new ObjectMapper();
	
	private static List<Tweet> tweets = new ArrayList<Tweet>();
	private static Set<String> hashtagsLang = new HashSet<String>();
	private static Map<String, Integer> hashtagsCounter = new HashMap<>();
	
	public static void main(String[] args) {

		log.info("-- Start Producer");

		Properties properties = new Properties();
		properties.put(ProducerConfig.ACKS_CONFIG, "1");
		properties.put(ProducerConfig.RETRIES_CONFIG, 3);
		properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
		properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BOOTSTRAP_SERVER);

		File fileData = new File(Constants.FILE_TWEETS);

		if (!fileData.exists()) {
			log.error(Constants.FILE_TWEETS + " not found.");
			return;
		}

		loadTweetsData(Constants.FILE_TWEETS);
		
		// Local
//		printHashtagLang();
//		printThrendingTopicsCounter();
//		printThrendingTopicsTweets();
		
		// Kafka
		sendTweets(properties);
		sendHashtagsLang(properties);
		sendTrendingTopics(properties);
	}
	
	
	//-------------------------------------------------
	// Process Data
	//-------------------------------------------------
	private static void loadTweetsData(String fileName) {
		try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
			String line;
			while ((line = br.readLine()) != null) {
				try {
					Tweet tweet = objectMapper.readValue(line, Tweet.class);

					if (tweet.entities.hashtags == null) {
						tweet.entities.hashtags = new ArrayList<Hashtag>();
					}
					tweets.add(tweet);
					updateHashtagsAndThrendingTopicsCounter(tweet.entities.hashtags, tweet.lang);

				} catch (Exception e) {
					e.printStackTrace();
					break;
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private static void updateHashtagsAndThrendingTopicsCounter(ArrayList<Hashtag> hashtags, String lang) {

		if (!hashtags.isEmpty()) {
			
			// Remove duplicates
			Set<String> hashtagsWithoutDuplicates = new HashSet<>(
					hashtags.stream().map(h -> h.tag).collect(Collectors.toList()));

			String[] arrayHashtags = hashtagsWithoutDuplicates.toArray(new String[0]);

			for (String hashtag : arrayHashtags) {
				
				// Trending topics
				if (hashtagsCounter.containsKey(hashtag)) {
					hashtagsCounter.put(hashtag, hashtagsCounter.get(hashtag) + 1);
				} else {
					hashtagsCounter.put(hashtag, 1);
				}
				
				// Hashtag lang
				if (Objects.equals(lang, Constants.HASHTAGS_LANG)) {
					hashtagsLang.add(hashtag);
				}
			}
		}
	}
	
	
	//-------------------------------------------------
	// Print Results
	//-------------------------------------------------
	
	private static void printHashtagLang() {
		
		for (String hashtag : hashtagsLang) {
			System.out.println("Hashtag: " + hashtag + ", Lang: " + Constants.HASHTAGS_LANG);
		}
	}


	private static void printThrendingTopicsCounter() {

		for (Map.Entry<String, Integer> trendingTopic : hashtagsCounter.entrySet()) {
			
			if (trendingTopic.getValue() >= Constants.TRENDING_TOPICS_THRESHOLD) {
				System.out.println("ThrendingTopic: " + trendingTopic.getKey() + ", Count: " + trendingTopic.getValue());
			}
		}
	}
	
	private static void printThrendingTopicsTweets() {

		List<String> threndingTopics = hashtagsCounter.entrySet().stream()
	            .filter(entry -> entry.getValue() >= Constants.TRENDING_TOPICS_THRESHOLD)
	            .map(Map.Entry::getKey)
	            .collect(Collectors.toList());

		for (Tweet tweet : tweets) {
			if (tweet.entities.hashtags.size() > 0 && tweet.entities.hashtags.stream().map(h -> h.tag).anyMatch(threndingTopics::contains)) {
				try {
					System.out.println(objectMapper.writeValueAsString(tweet));
				} catch (JsonProcessingException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	
	//-------------------------------------------------
	// Send to Kafka
	//-------------------------------------------------

	private static void sendTweets(Properties properties) {
		log.info("-- Start sendTweets");
		
		try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {

			for (Tweet tweet : tweets) {
				producer.send(new ProducerRecord<>(Constants.TOPIC_TWEETS, tweet.lang, objectMapper.writeValueAsString(tweet)));
			}
			
			producer.flush();
			producer.close();
			
		} catch (Exception e) {
			log.error("There was an error in sendTweets");
			e.printStackTrace();
		}

		log.info("-- End sendTweets");
	}
	
	
	private static void sendHashtagsLang(Properties properties) {
		log.info("-- Start sendTweetsHashtagsLang");
		
		try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
			
			for (String hashtag : hashtagsLang) {
				
				HashtagSimple sendHashtag = new HashtagSimple(hashtag);
				producer.send(new ProducerRecord<>(Constants.TOPIC_TWEETS_HASHTAGS_LANG, Constants.HASHTAGS_LANG, objectMapper.writeValueAsString(sendHashtag)));
			}


			producer.flush();
			producer.close();
			
		} catch (Exception e) {
			log.error("There was an error in sendTweetsHashtagsLang");
			e.printStackTrace();
		}

		log.info("-- End sendTweetsHashtagsLang");
	}
	
	
	private static void sendTrendingTopics(Properties properties) {
		log.info("-- Start sendTweetsTrendingTopics");
		
		try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {

			List<String> threndingTopics = hashtagsCounter.entrySet().stream()
											            .filter(entry -> entry.getValue() >= Constants.TRENDING_TOPICS_THRESHOLD)
											            .map(Map.Entry::getKey)
											            .collect(Collectors.toList());
			
			for (Tweet tweet : tweets) {
				if (tweet.entities.hashtags.size() > 0 && tweet.entities.hashtags.stream().map(h -> h.tag).anyMatch(threndingTopics::contains)) {
					producer.send(new ProducerRecord<>(Constants.TOPIC_TWEETS_TRENDING_TOPICS, tweet.lang, objectMapper.writeValueAsString(tweet)));
				}
			}
			
			producer.flush();
			producer.close();
			
		} catch (Exception e) {
			log.error("There was an error in sendTweetsTrendingTopics");
			e.printStackTrace();
		}

		log.info("-- End sendTweetsTrendingTopics");
	}
}