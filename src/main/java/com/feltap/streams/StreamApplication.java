package com.feltap.streams;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamApplication {
	
	private static final Logger logger = LoggerFactory.getLogger(StreamApplication.class);
	
	public static void main(String[] args) {
		Properties streamProperties = new Properties();
		
		streamProperties.put(StreamsConfig.APPLICATION_ID_CONFIG,"user-event-enricher-app");
		streamProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:51818");
		streamProperties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,"1000");
		streamProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		streamProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		streamProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		streamProperties.put(StreamsConfig.STATE_DIR_CONFIG, "C:/Users/Felipe/Desktop/state");
		
		
		
		StreamsBuilder builder = new StreamsBuilder();
		//We get a global table out of Kafka. This tabe will be replicated on each Kafka Streams Application
		//The key of our globalKTable is the user ID
		GlobalKTable<String, String>  usersGlobalTable = builder.globalTable("user-table");
		
		//We get a stream of user purchases
		KStream<String, String> userPurchases = builder.stream("user-purchaces");
		
		//We want to enrich that stream
		KStream<String,String> userPurcahesEnrichedJoin = 
				userPurchases.join( usersGlobalTable,
						//Map from the (key,  value) of this stream to the key of the GlobalKTable
						(key,value) -> key,  
						//Value of the userPuchases stream and value from GlobalKTable
						(userPurchase, userInfo) -> String.format("Purchase= %s, UserInfo=%s", userPurchase, userInfo) );
		
		userPurcahesEnrichedJoin.to("user-purchases-enriched-inner-join");
		
		KStream<String, String> userPurchaesEnrichedLeftJoin  =
				userPurchases.leftJoin(usersGlobalTable, (key,value) -> key, 
						//As this is a left join, user Info can be null
						(userPurchase, userInfo) -> userInfo != null? String.format("Purchase= %s, UserInfo= %s", userPurchase,userInfo) : String.format("Purchase= %s UserInfo=Null", userInfo));
		
		userPurchaesEnrichedLeftJoin.to("user-purchases-enriched-left-join");
		
		
		Topology topology = builder.build();
		logger.info(String.format("%s", topology.describe().toString()));
		
		KafkaStreams streams = new KafkaStreams(topology, streamProperties);
		streams.cleanUp();
		streams.start();
		
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
		
		
		
	
	}
}
