/*
 * Copyright Blue Jeans Network.
 */
package com.bluejeans.kafka;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Kafka processor context
 *
 * @author Dinesh Ilindra
 */
public class KafkaProcessorContext<K, V> {

    private final ConsumerRecord<K, V> record;
    private final Map<Object, Object> attributeMap = new HashMap<>();
    public KafkaProcessorContext(final ConsumerRecord<K, V> record) {
        this.record = record;
    }
    /**
     * @return the record
     */
    public ConsumerRecord<K, V> getRecord() {
        return record;
    }
    /**
     * @return the attributeMap
     */
    public Map<Object, Object> getAttributeMap() {
        return attributeMap;
    }

    public void setRecordData(final Object data) {
        attributeMap.put(record, data);
    }

    public Object getRecordDate() {
        return attributeMap.get(record);
    }

}
