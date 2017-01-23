/*
 * Copyright Blue Jeans Network.
 */
package com.bluejeans.kafka;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Logger kafka record processor
 *
 * @author Dinesh Ilindra
 */
public class LoggerKafkaRecordProcessor implements KafkaRecordProcessor<String, String> {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final AtomicLong recordsLogged = new AtomicLong();

    private boolean fullRecord = false;

    /*
     * (non-Javadoc)
     *
     * @see com.bluejeans.common.utils.kafka.KafkaRecordProcessor#processKafkaRecord
     * (org.apache.kafka.clients.consumer.ConsumerRecord,
     * com.bluejeans.common.utils.kafka.KafkaProcessorContext)
     */
    @Override
    public void processKafkaRecord(final ConsumerRecord<String, String> record,
            final KafkaProcessorContext<String, String> context) {
        if (fullRecord) {
            logger.info(record.toString());
        } else {
            logger.info(record.value());
        }
        recordsLogged.incrementAndGet();
    }

    /**
     * @return the fullRecord
     */
    public boolean isFullRecord() {
        return fullRecord;
    }

    /**
     * @param fullRecord
     *            the fullRecord to set
     */
    public void setFullRecord(final boolean fullRecord) {
        this.fullRecord = fullRecord;
    }

    /**
     * @return the recordsLogged
     */
    public AtomicLong getRecordsLogged() {
        return recordsLogged;
    }

}
