package com.deepak.kafka.twitter;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterProducer {
	
	public static Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

	public static void main(String[] args) {
		
		TwitterProducer producer = new TwitterProducer();
		producer.run();
	}
	
	
	public void run(){
		
		/** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
		
		//create twitter client
		Client twitterClient = createTwitterClient(msgQueue);
		
		twitterClient.connect();
		
		//create kafka producer
		KafkaProducer<String,String> kafkaProducer = createKafkaProducer();
				
		//publish message to kafka
		
		while (!twitterClient.isDone()) {
			  String msg = null;
			try {
				msg = msgQueue.take();
			} catch (InterruptedException e) {
				e.printStackTrace();
				twitterClient.stop();
			}
			if(msg != null) {
				logger.info(msg);	
				kafkaProducer.send(new ProducerRecord<>("twitter-tweets-1",null,msg), new Callback() {

					@Override
					public void onCompletion(RecordMetadata metadata, Exception ex) {
						if(ex != null) {
							logger.error("Something bad happend",ex);
						}
						
					}
					
				});
			}
			  
		}
	
	}
	
	private KafkaProducer<String, String> createKafkaProducer() {
		// create producer properties
		Properties prop = new Properties();
				
		prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "ec2-13-235-54-190.ap-south-1.compute.amazonaws.com:9092");
		prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
				
		//safe producer properties
		prop.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		prop.setProperty(ProducerConfig.ACKS_CONFIG,"all");
		prop.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
		prop.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
		
		//high throughput producer at the expense of some CPU usage and latency
		prop.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"sanppy");
		prop.setProperty(ProducerConfig.LINGER_MS_CONFIG,"20");
		prop.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024)); //32 KB
		
		//create producer
		KafkaProducer<String,String> producer = new KafkaProducer<>(prop);
				
		return producer;
	}


	public Client createTwitterClient(BlockingQueue<String> msgQueue) {
		

		/** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		
		// Optional: set up some followings and track terms
		List<String> terms = Lists.newArrayList("bitcoin");
		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(TwitterCredentials.CONSUMER_KEY, TwitterCredentials.CONSUMER_SECRET, TwitterCredentials.TOKEN, TwitterCredentials.SECRET);
		
		ClientBuilder builder = new ClientBuilder()
				  .name("Hosebird-Client-01")                              // optional: mainly for the logs
				  .hosts(hosebirdHosts)
				  .authentication(hosebirdAuth)
				  .endpoint(hosebirdEndpoint)
				  .processor(new StringDelimitedProcessor(msgQueue));  

				Client hosebirdClient = builder.build();
				
				return hosebirdClient;
	}

}
