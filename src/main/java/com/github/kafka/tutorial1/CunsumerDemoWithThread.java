package com.github.kafka.tutorial1;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class CunsumerDemoWithThread {
  public static void main(String[] args) {
    new CunsumerDemoWithThread().run();
  }

  private CunsumerDemoWithThread() {
  }

  private void run() {
    // System.out.println("Hello World!!!");
    Logger logger = LoggerFactory.getLogger(CunsumerDemoWithThread.class.getName());

    String bootstrapServers = "127.0.0.1:9092";
    String groupId = "my-sixth-application";
    String topic = "first_topic";
    // latch for dealing multiple threads
    CountDownLatch latch = new CountDownLatch(1);
    // create the Consumer runnable
    logger.info("Creating a consumer thread");
    Runnable myConsumerRunnable = new ConsumerRunnable(
            bootstrapServers,
            groupId,
            topic,
            latch);
    // start the thread
    Thread myThread = new Thread(myConsumerRunnable);
    myThread.start();

    // add a shutdown hook
    Runtime.getRuntime().addShutdownHook(new Thread( () -> {
      logger.info("Caught shutdown hook");
      ((ConsumerRunnable) myConsumerRunnable).shutdown();
    }

    ));

    try {
      latch.await();
    } catch (InterruptedException e) {
      logger.error("Application interrupted", e);
    } finally {
      logger.info("Application is closing");
    }
  }


  public class ConsumerRunnable implements Runnable {
    private CountDownLatch latch;
    private KafkaConsumer<String, String> consumer;
    private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

    public ConsumerRunnable(String bootstrapServers,
                          String groupId,
                          String topic,
                          CountDownLatch latch) {
      this.latch = latch;
      // create consumer configs
      Properties properties = new Properties();
      properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
      properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
      properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
      properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
      properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");    // earliest/latest/none

      // create consumer
      consumer = new KafkaConsumer<String, String>(properties);

      // subscribe consumer to our topic
      // consumer.subscribe(Collections.singleton(topic));
      consumer.subscribe(Arrays.asList(topic));
    }

    @Override
    public void run() {
      // poll for new data
      try {
        while (true) {
          ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
          for (ConsumerRecord<String, String> record : records) {
            logger.info("Key: " + record.key() + ", Value: " + record.value() + "\n");
            logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
          }
        }
      } catch (WakeupException e) {
        logger.info("Received shutdoun signal!");
      } finally {
        consumer.close();
        latch.countDown();  // tell a main code we're done with the consumer
      }
    }

    public void shutdown() {
      // Run if WakeUp() Exception events
      consumer.wakeup(); // the wakeup() is the method to interrupt poll(). if Esception
    }
  }

}
