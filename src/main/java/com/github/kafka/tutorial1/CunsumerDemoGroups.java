package com.github.kafka.tutorial1;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class CunsumerDemoGroups {
  public static void main(String[] args) {
    // System.out.println("Hello World!!!");
    Logger logger = LoggerFactory.getLogger(CunsumerDemoGroups.class.getName());

    String bootstrapServers = "127.0.0.1:9092";
    String groupId = "my-forth-application";
    String topic = "first_topic";
    // create consumer configs
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");    // earliest/latest/none

    // create consumer
    KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);

    // subscribe consumer to our topic
    // consumer.subscribe(Collections.singleton(topic));
    consumer.subscribe(Arrays.asList(topic));
    while(true){
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
      for (ConsumerRecord<String, String> record : records) {
        logger.info("Key: " + record.key() + ", Value: " + record.value() + "\n");
        logger.info("Partition: " + record.partition() + ", Offset: "+ record.offset());
      }
    }

    // poll for new data

  }
}
