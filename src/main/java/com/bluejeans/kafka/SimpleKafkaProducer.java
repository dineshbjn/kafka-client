/*
 * Copyright Blue Jeans Network.
 */
package com.bluejeans.kafka;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bluejeans.utils.EnumCounter;

/**
 * Kafka String based producer
 *
 * @author Dinesh Ilindra
 */
public class SimpleKafkaProducer<K, V> {

    public static enum Status {
        RECORDS_SENT, PROCESS_ERROR,
    }

    private static final Logger logger = LoggerFactory.getLogger(SimpleKafkaProducer.class);

    private String server = "localhost:9092";
    private String clientId = "local";
    private Serializer<K> keySerializer;
    private Serializer<V> valueSerializer;
    private String topic;
    private Map<String, Object> extraProps;
    private KafkaProducer<K, V> producer;
    private boolean async = false;
    private Callback callback;
    private final EnumCounter<Status> statusCounter = new EnumCounter<Status>(Status.class);

    @PostConstruct
    public void init() {
        if (keySerializer == null) {
            keySerializer = new ObjectSerializer<K>();
            keySerializer.configure(extraProps, true);
        }
        if (valueSerializer == null) {
            valueSerializer = new ObjectSerializer<V>();
            valueSerializer.configure(extraProps, false);
        }
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        if (extraProps != null) {
            props.putAll(extraProps);
        }
        producer = new KafkaProducer<K, V>(props, keySerializer, valueSerializer);
    }

    @PreDestroy
    public void shutdown() {
        producer.close();
    }

    public boolean send(final V value) {
        return send(null, value);
    }

    public boolean send(final K key, final V value) {
        return send(topic, key, value);
    }

    public boolean send(final ConsumerRecord<K, V> record) {
        return send(record.topic() + ":" + record.partition(), record.key(), record.value());
    }

    public boolean send(final String topic, final K key, final V value) {
        boolean status = true;
        if (async) {
            for (final String topicStr : topic.split(",")) {
                if (topicStr.indexOf(':') > 0) {
                    final String[] topicInfo = topicStr.split(":");
                    producer.send(new ProducerRecord<K, V>(topicInfo[0], Integer.valueOf(topicInfo[1]), key, value),
                            callback);
                } else {
                    producer.send(new ProducerRecord<K, V>(topicStr, key, value), callback);
                }
                statusCounter.incrementEventCount(Status.RECORDS_SENT);
            }
        } else {
            for (final String topicStr : topic.split(",")) {
                try {
                    if (topicStr.indexOf(':') > 0) {
                        final String[] topicInfo = topicStr.split(":");
                        producer.send(
                                new ProducerRecord<K, V>(topicInfo[0], Integer.parseInt(topicInfo[1]), key, value),
                                callback).get();
                    } else {
                        producer.send(new ProducerRecord<K, V>(topicStr, key, value), callback).get();
                    }
                    statusCounter.incrementEventCount(Status.RECORDS_SENT);
                    status &= true;
                } catch (InterruptedException | ExecutionException ex) {
                    status &= false;
                    statusCounter.incrementEventCount(Status.PROCESS_ERROR);
                    logger.warn("Problem in posting to topic - " + topicStr + "with data " + key + ":" + value, ex);
                }
            }
        }
        return status;
    }

    /**
     * @return the clientId
     */
    public String getClientId() {
        return clientId;
    }

    /**
     * @param clientId
     *            the clientId to set
     */
    public void setClientId(final String clientId) {
        this.clientId = clientId;
    }

    /**
     * @return the keySerializer
     */
    public Serializer<K> getKeySerializer() {
        return keySerializer;
    }

    /**
     * @param keySerializer
     *            the keySerializer to set
     */
    public void setKeySerializer(final Serializer<K> keySerializer) {
        this.keySerializer = keySerializer;
    }

    /**
     * @return the valueSerializer
     */
    public Serializer<V> getValueSerializer() {
        return valueSerializer;
    }

    /**
     * @param valueSerializer
     *            the valueSerializer to set
     */
    public void setValueSerializer(final Serializer<V> valueSerializer) {
        this.valueSerializer = valueSerializer;
    }

    /**
     * @return the async
     */
    public boolean isAsync() {
        return async;
    }

    /**
     * @param async
     *            the async to set
     */
    public void setAsync(final boolean async) {
        this.async = async;
    }

    /**
     * @return the callback
     */
    public Callback getCallback() {
        return callback;
    }

    /**
     * @param callback
     *            the callback to set
     */
    public void setCallback(final Callback callback) {
        this.callback = callback;
    }

    /**
     * @return the producer
     */
    public KafkaProducer<K, V> getProducer() {
        return producer;
    }

    /**
     * @return the server
     */
    public String getServer() {
        return server;
    }

    /**
     * @param server
     *            the server to set
     */
    public void setServer(final String server) {
        this.server = server;
    }

    /**
     * @return the topic
     */
    public String getTopic() {
        return topic;
    }

    /**
     * @param topic
     *            the topic to set
     */
    public void setTopic(final String topic) {
        this.topic = topic;
    }

    /**
     * @return the extraProps
     */
    public Map<String, Object> getExtraProps() {
        return extraProps;
    }

    /**
     * @param extraProps
     *            the extraProps to set
     */
    public void setExtraProps(final Map<String, Object> extraProps) {
        this.extraProps = extraProps;
    }

    /**
     * @return the statusCounter
     */
    public EnumCounter<Status> getStatusCounter() {
        return statusCounter;
    }

}
