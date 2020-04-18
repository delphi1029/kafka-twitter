package com.deepak.kafka.elasticsearch.consumer;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonParser;

public class ElasticSearchConsumer {
	
	private static final Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class);
	
	private static JsonParser jsonParser = new JsonParser();
	
	public static RestHighLevelClient createClient() {
		

		
		final CredentialsProvider credentialProvider = new BasicCredentialsProvider();
		credentialProvider.setCredentials(AuthScope.ANY, 
				new UsernamePasswordCredentials(ElasticSearchCredentials.USERNAME,ElasticSearchCredentials.PASSWORD));
		
		RestClientBuilder builder = RestClient.builder(
				new HttpHost(ElasticSearchCredentials.HOST_NAME, 443, "https"))
				.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
					
					@Override
					public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
						
						return httpClientBuilder.setDefaultCredentialsProvider(credentialProvider);
					}
				});
		
		RestHighLevelClient client = new RestHighLevelClient(builder);
		
		return client;
	}
	
	public static KafkaConsumer<String,String> createConsumer(String topic) {
		Properties prop = new Properties();
		
		String groupId = "kafka-demo-elasticsearch";

		prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "ec2-13-235-54-190.ap-south-1.compute.amazonaws.com:9092");
		prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
		prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest"); //or "latest"
		
		//create consumer
		KafkaConsumer<String,String> consumer = new KafkaConsumer<>(prop);
		consumer.subscribe(Arrays.asList(topic));
		
		return consumer;
	}

	public static void main(String[] args) throws IOException {
		
		RestHighLevelClient client = createClient();
		
		KafkaConsumer<String,String> kafkaConsumer = createConsumer("twitter-tweets");
		
		
		while(true) {
			
			//kafka consumer consuming data from topic
			ConsumerRecords<String,String> records = kafkaConsumer.poll(Duration.ofMillis(100));
			
			for(ConsumerRecord<String,String> record : records) {
				
				//two stratergies to get unique id and make consumer indempotent
				// 1) Kafka generic id - id = record.topic() + record.partition() + record.offset()
				//2) Twitter feed specific id (unique id from tweet json)
				String id = extractIdFromTweet(record.value());
				
				//submitting the tweets to elastiscearch
				@SuppressWarnings("deprecation")
				IndexRequest indexRequest = new IndexRequest("twitter","tweets")
					.source(record.value(),XContentType.JSON, id); //id is to make consume idempotent
				
				IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
				
				String tweet_id = indexResponse.getId();
				
				logger.info(tweet_id);
			}
		}

		

	}

	private static String extractIdFromTweet(String value) {
		//gson parser
		return jsonParser.parse(value)
					.getAsJsonObject().get("id_str").getAsString();
	}

}
