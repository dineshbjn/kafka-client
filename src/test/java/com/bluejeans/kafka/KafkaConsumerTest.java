/*
 * Copyright Blue Jeans Network.
 */
package com.bluejeans.kafka;

import com.bluejeans.kafka.SimpleKafkaConsumer;

/**
 * kafka consumer test
 *
 * @author Dinesh Ilindra
 */
public class KafkaConsumerTest {

    public static void main(final String[] args) throws Exception {
        final SimpleKafkaConsumer<String, String> consumer = new SimpleKafkaConsumer<>();
        consumer.setServer("10.5.7.246:9092");
        consumer.setTopic("indigo");
        consumer.setConsumerCount(4);
        consumer.init();
        Thread.sleep(2000);
        System.out.println(consumer.getTopicLag());
    }

}
