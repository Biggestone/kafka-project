package com.project.kafkaProject;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaProducerTest {
	
	private static final String TOPIC= "test";
	private static final String BOOTSTRAP_SERVERS="localhost:9092";
	
	private static Producer<Long, String> createProducer(){
		Properties props = new Properties();
		
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaProducerTest");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		
		
		return new KafkaProducer<>(props);
	}
	
	public static void runProducer(String message) {
		final Producer<Long, String> producer = createProducer();
		
		long time = System.currentTimeMillis();
		
		try {
			final ProducerRecord<Long, String> record = 
					new ProducerRecord<Long, String>(TOPIC,0,time,message );
			RecordMetadata metadata = producer.send(record).get();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally {
			producer.flush();
			producer.close();
		}
	}
}
