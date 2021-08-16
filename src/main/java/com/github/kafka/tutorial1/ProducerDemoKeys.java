package com.github.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

  public static void main(String[] args) throws ExecutionException, InterruptedException {
    String bootstrapServers = "127.0.0.1:9092";

    // create Producer PROPERTIES
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // create the Producer
    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

    for (int i = 0; i < 10; i++) {
      // create a producer record

      String topic = "first_topic";
      String value = "Hello World+ " + Integer.toString(i);
      String key   = "id_" + Integer.toString(i+1);

      ProducerRecord<String, String> record =
              new ProducerRecord<String, String>(topic, key, value);

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
                    "TimeStemp: " + recordMetadata.timestamp() + "\n" +
                    "Key = " + key);
          } else {
            System.out.println("Error while producing " + e);
          }

        }
      }).get(); // get() - block the .send() to make it synchronouse
    }

    // flush data
    producer.flush();
    // flush and close producer
    producer.close();

  }
}
