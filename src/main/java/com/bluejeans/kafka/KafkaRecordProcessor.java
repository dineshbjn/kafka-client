/*
 * Copyright Blue Jeans Network.
 */
package com.bluejeans.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Consumer record processor
 *
 * @author Dinesh Ilindra
 */
public interface KafkaRecordProcessor<K, V> {

    void processKafkaRecord(ConsumerRecord<K, V> record, KafkaProcessorContext<K, V> context);

}
