/*
 * Copyright Blue Jeans Network.
 */
package com.bluejeans.kafka;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.bluejeans.kafka.KafkaProcessorContext;
import com.bluejeans.kafka.KafkaRecordProcessor;

/**
 * Print kafka record processor
 *
 * @author Dinesh Ilindra
 */
public class PrintKafkaRecordProcessor implements KafkaRecordProcessor<String, String> {

    private final AtomicLong recordsPrinted = new AtomicLong();

    /*
     * (non-Javadoc)
     *
     * @see
     * com.bluejeans.common.utils.kafka.KafkaRecordProcessor#processKafkaRecord
     * (org.apache.kafka.clients.consumer.ConsumerRecord,
     * com.bluejeans.common.utils.kafka.KafkaProcessorContext)
     */
    @Override
    public void processKafkaRecord(final ConsumerRecord<String, String> record,
            final KafkaProcessorContext<String, String> context) {
        System.out.println(record);
        recordsPrinted.incrementAndGet();
    }

}
