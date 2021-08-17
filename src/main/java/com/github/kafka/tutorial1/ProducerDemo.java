package com.github.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

  public static void main(String[] args) {
    String bootstrapServers = "127.0.0.1:9092";

    // create Producer PROPERTIES
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // create the Producer
    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

    // create a producer record
    for (int i = 0; i < 10; i++) {
      ProducerRecord<String, String> record =
              new ProducerRecord<String, String>("first_topic", "Hello, World!!!  " + Integer.toString(i));

      // SEND data -- asyncronous
      producer.send(record, new Callback() {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
          // executes every time a record is seccessfully  sent or ecxeption
          if (e == null) {
            //the record was seccessfully sent
            System.out.println("Received metadata. \n" +
                    "Topic: " + recordMetadata.topic() + "\n" +
                    "Partition: " + recordMetadata.partition() + "\n" +
                    "Offset: " + recordMetadata.offset() + "\n" +
                    "TimeStemp: " + recordMetadata.timestamp());
          } else {
            System.out.println("Error while producing " + e);
          }

        }
      });
    }

    // flush data
    producer.flush();
    // flush and close producer
    producer.close();

  }
}
