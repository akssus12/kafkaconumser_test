package com.dcclab.examples.streams.consumer;


import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.util.Collections;
import java.util.Properties;

public class Consumer {

    public static void main(String[] args) throws Exception{

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
	props.put("group.id", "karim-group-id-1"); 
	props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"); // value deserializer

	KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
	consumer.subscribe(Collections.singletonList("wordcount-output"));
	
	String message = null;
        try {
            while(true) {
                ConsumerRecords<String, String> records = consumer.poll(100);

                for (ConsumerRecord<String, String> record : records) {
                    message = record.value();
                    System.out.println(message);
                }
            }
        } catch(Exception e) {
        } finally {
            consumer.close();
        }
    }
}
