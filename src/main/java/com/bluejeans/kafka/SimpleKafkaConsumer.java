/*
 * Copyright Blue Jeans Network.
 */
package com.bluejeans.kafka;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bluejeans.utils.EnumCounter;
import com.bluejeans.utils.MetaUtil;
import com.google.common.collect.Lists;

/**
 * Kafka String based consumer
 *
 * @author Dinesh Ilindra
 */
public class SimpleKafkaConsumer<K, V> {

    public static enum Status {
        RECORDS_POLLED, PROCESS_ERROR,
    }

    private static final Logger logger = LoggerFactory.getLogger(SimpleKafkaConsumer.class);
    private static final String MONITOR_GROUP_ID = "__kafkamonitor__";

    private String server = "localhost:9092";
    private String groupId = "local";
    private boolean enableAutoCommit = false;
    private boolean commitSyncEnabled = true;
    private boolean repostEnabled = false;
    private int autoCommitIntervalMillis = 1000;
    private int sessionTimeoutMillis = 30000;
    private Deserializer<K> keyDeserializer;
    private Deserializer<V> valueDeserializer;
    private String topic;
    private boolean specificPartitions = false;
    private int pollTimeout = 10000;
    private int monitorSleepMillis = 10000;
    private Map<String, Object> extraProps = new HashMap<>();
    private final List<KafkaConsumer<K, V>> consumers = new ArrayList<>();
    private SimpleKafkaProducer<K, V> simpleProducer = null;
    private KafkaConsumer<K, V> monitor;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private List<KafkaRecordProcessor<K, V>> recordProcessors;
    private final EnumCounter<Status> statusCounter = new EnumCounter<Status>(Status.class);
    private final List<KafkaConsumerThread> runThreads = new ArrayList<>();
    private Thread monitorThread;
    private boolean monitorEnabled = false;
    private boolean commitAfterProcess = false;
    private String name = "kafka-consumer";
    private int consumerCount = 1;
    private final Map<String, AtomicLong> topicQueueSizes = new HashMap<>();
    private final Map<String, Map<Integer, Long>> partitionQueueSizes = new HashMap<>();
    private final Map<String, Map<Integer, Long>> partitionConsumes = new HashMap<>();
    private final Map<String, Map<Integer, Long>> partitionLags = new HashMap<>();
    private final Map<String, Long> topicLags = new HashMap<>();
    private final List<TopicPartition> partitions = new ArrayList<TopicPartition>();
    private final Set<String> topics = new HashSet<>();

    @PostConstruct
    public void init() {
        preInit();
        // assign();
        start();
    }

    public void preInit() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
        if (enableAutoCommit) {
            props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, autoCommitIntervalMillis);
        }
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMillis);
        if (extraProps != null) {
            props.putAll(extraProps);
        }
        if (keyDeserializer == null) {
            keyDeserializer = new ObjectDeserializer<K>();
            keyDeserializer.configure(extraProps, true);
        }
        if (valueDeserializer == null) {
            valueDeserializer = new ObjectDeserializer<V>();
            valueDeserializer.configure(extraProps, false);
        }
        final Properties monitorProps = new Properties();
        monitorProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        monitorProps.put(ConsumerConfig.GROUP_ID_CONFIG, MONITOR_GROUP_ID);
        monitorProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMillis);
        monitor = new KafkaConsumer<K, V>(monitorProps, keyDeserializer, valueDeserializer);
        consumers.clear();
        for (int index = 0; index < consumerCount; index++) {
            consumers.add(new KafkaConsumer<K, V>(props, keyDeserializer, valueDeserializer));
        }
        if (repostEnabled) {
            simpleProducer = new SimpleKafkaProducer<>();
            simpleProducer.setAsync(false);
            simpleProducer.setClientId(groupId);
            simpleProducer.setServer(server);
            simpleProducer.init();
        }
    }

    private void ensureAssignment(final KafkaConsumer<K, V> consumer) {
        MetaUtil.runNestedMethodsSilently(consumer,
                "$coordinator..ensureCoordinatorKnown;;$coordinator..ensurePartitionAssignment");
    }

    public static void seek(final KafkaConsumer<?, ?> consumer, final TopicPartition partition, final boolean end) {
        try {
            if (end) {
                seek(consumer, partition, "seekToEnd");
            } else {
                seek(consumer, partition, "seekToBeginning");
            }
        } catch (final ReflectiveOperationException ex) {
            logger.error("could not seek", ex);
        }
    }

    private static void seek(final KafkaConsumer<?, ?> consumer, final TopicPartition partition,
            final String methodName) throws ReflectiveOperationException {
        try {
            consumer.getClass().getMethod(methodName, TopicPartition[].class);
            consumer.getClass().getMethod(methodName, TopicPartition[].class).invoke(consumer,
                    new Object[] { new TopicPartition[] { partition } });
        } catch (final NoSuchMethodException ex) {
            consumer.getClass().getMethod(methodName, Collection.class).invoke(consumer, Arrays.asList(partition));
        }
    }

    public class KafkaMonitorThread extends Thread {
        /*
         * (non-Javadoc)
         *
         * @see java.lang.Thread#run()
         */
        @Override
        public void run() {
            final Map<String, List<PartitionInfo>> allMap = monitor.listTopics();
            final List<TopicPartition> monPartitions = new ArrayList<>();
            for (final String topic : topics) {
                for (final PartitionInfo info : allMap.get(topic)) {
                    monPartitions.add(new TopicPartition(info.topic(), info.partition()));
                }
            }
            try {
                MetaUtil.findFirstMethod(monitor.getClass(), "assign", 1).invoke(monitor, monPartitions);
            } catch (final ReflectiveOperationException roe) {
                logger.error("could not assign", roe);
            }
            try {
                while (running.get()) {
                    for (final String topic : topics) {
                        topicQueueSizes.get(topic).set(0);
                    }
                    for (final TopicPartition tp : monPartitions) {
                        seek(monitor, tp, true);
                        final long position = monitor.position(tp);
                        topicQueueSizes.get(tp.topic()).addAndGet(position);
                        partitionQueueSizes.get(tp.topic()).put(tp.partition(), position);
                        final Map<Integer, Long> consumeMap = partitionConsumes.get(tp.topic());
                        if (consumeMap.containsKey(tp.partition())) {
                            partitionLags.get(tp.topic()).put(tp.partition(),
                                    partitionQueueSizes.get(tp.topic()).get(tp.partition()).longValue()
                                            - consumeMap.get(tp.partition()).longValue());
                        }
                        topicLags.put(tp.topic(), calculateTopicLag(tp.topic()));
                    }
                    Thread.sleep(monitorSleepMillis);
                }
            } catch (final WakeupException we) {
                if (running.get()) {
                    throw we;
                }
            } catch (final InterruptedException ie) {
                logger.warn("monitor thread interrupted", ie);
            } finally {
                monitor.close();
            }
        }
    }

    public class KafkaConsumerThread extends Thread {

        private final KafkaConsumer<K, V> consumer;

        private final List<TopicPartition> partitions;

        private Set<TopicPartition> currentAssignment = new HashSet<>();

        /**
         * @param consumer
         */
        public KafkaConsumerThread(final KafkaConsumer<K, V> consumer, final List<TopicPartition> partitions) {
            super();
            this.consumer = consumer;
            this.partitions = partitions;
        }

        /**
         * fix the consumer positions
         */
        public void fixPositions() {
            synchronized (partitions) {
                for (final TopicPartition partition : partitions) {
                    try {
                        final OffsetAndMetadata meta = consumer.committed(partition);
                        if (meta != null) {
                            logger.info("For partition - " + partition + " meta - " + meta);
                            consumer.seek(partition, meta.offset());
                        } else {
                            logger.info("For partition - " + partition + " no meta, seeking to beginning");
                            seek(consumer, partition, false);
                        }
                        logger.info("Partition - " + partition + " @ position - " + consumer.position(partition));
                    } catch (final RuntimeException re) {
                        logger.warn("could not fix positions", re);
                    }
                }
            }
        }

        /**
         * commit sync
         */
        public void commitSync() {
            if (commitSyncEnabled) {
                try {
                    consumer.commitSync();
                } catch (final RuntimeException re) {
                    logger.error(re.getMessage() + "\nCommit failed for " + currentAssignment);
                }
            }
        }

        /*
         * (non-Javadoc)
         *
         * @see java.lang.Thread#run()
         */
        @Override
        public void run() {
            if (specificPartitions) {
                try {
                    MetaUtil.findFirstMethod(consumer.getClass(), "assign", 1).invoke(consumer, partitions);
                    fixPositions();
                } catch (final ReflectiveOperationException roe) {
                    logger.error("could not assign", roe);
                }
            } else {
                try {
                    MetaUtil.findFirstMethod(consumer.getClass(), "subscribe", 1).invoke(consumer,
                            new ArrayList<>(topics));
                    ensureAssignment(consumer);
                } catch (final ReflectiveOperationException roe) {
                    logger.error("could not subscribe", roe);
                }
            }
            try {
                while (running.get()) {
                    currentAssignment = consumer.assignment();
                    if (monitorEnabled) {
                        for (final TopicPartition tp : currentAssignment) {
                            final OffsetAndMetadata meta = consumer.committed(tp);
                            final long consumedMessages = meta != null ? meta.offset() : 0;
                            partitionConsumes.get(tp.topic()).put(tp.partition(), consumedMessages);
                        }
                    }
                    final ConsumerRecords<K, V> records = consumer.poll(pollTimeout);
                    statusCounter.incrementEventCount(Status.RECORDS_POLLED, records.count());
                    if (!commitAfterProcess) {
                        commitSync();
                    }
                    // Handle records
                    if (recordProcessors != null) {
                        for (final ConsumerRecord<K, V> record : records) {
                            boolean failed = false;
                            final KafkaProcessorContext<K, V> context = new KafkaProcessorContext<K, V>(record);
                            for (final KafkaRecordProcessor<K, V> processor : recordProcessors) {
                                try {
                                    processor.processKafkaRecord(record, context);
                                } catch (final RuntimeException re) {
                                    failed = true;
                                    statusCounter.incrementEventCount(Status.PROCESS_ERROR);
                                    logger.warn(
                                            "Failed to process record - " + record + " using processor - " + processor,
                                            re);
                                }
                            }
                            if (repostEnabled && failed) {
                                simpleProducer.send(record);
                            }
                        }
                    }
                    if (commitAfterProcess) {
                        commitSync();
                    }
                }
            } catch (final WakeupException we) {
                if (running.get()) {
                    throw we;
                }
            } finally {
                consumer.close();
            }
        }

        public KafkaConsumer<K, V> getConsumer() {
            return consumer;
        }

        public Set<TopicPartition> getCurrentAssignment() {
            return currentAssignment;
        }
    }

    public synchronized void start() {
        if (!running.get()) {
            running.set(true);
            topicQueueSizes.clear();
            partitionQueueSizes.clear();
            partitionConsumes.clear();
            partitionLags.clear();
            topicLags.clear();
            partitions.clear();
            topics.clear();
            if (specificPartitions) {
                for (final String topic : topic.split(",")) {
                    final String[] t = topic.split(":");
                    topicQueueSizes.put(t[0], new AtomicLong());
                    topics.add(topic);
                    if (t.length > 1) {
                        final TopicPartition tp = new TopicPartition(t[0], Integer.parseInt(t[1]));
                        partitions.add(tp);
                    }
                }
            } else {
                for (final String topic : Arrays.asList(topic.split(","))) {
                    topics.add(topic);
                    topicQueueSizes.put(topic, new AtomicLong());
                }
            }
            for (final String topic : topics) {
                partitionQueueSizes.put(topic, new HashMap<Integer, Long>());
                partitionConsumes.put(topic, new ConcurrentHashMap<Integer, Long>());
                partitionLags.put(topic, new ConcurrentHashMap<Integer, Long>());
            }
            if (monitorEnabled) {
                monitorThread = new KafkaMonitorThread();
                monitorThread.setName(name + "-monitor");
                monitorThread.start();
            }
            runThreads.clear();
            if (!partitions.isEmpty()) {
                final List<List<TopicPartition>> consumerPartitions = Lists.partition(partitions,
                        (int) Math.ceil((double) partitions.size() / consumerCount));
                for (int index = 0; index < consumerPartitions.size(); index++) {
                    final KafkaConsumerThread runThread = new KafkaConsumerThread(
                            SimpleKafkaConsumer.this.consumers.get(index), consumerPartitions.get(index));
                    runThread.setName(name + "-" + index);
                    runThread.start();
                    runThreads.add(runThread);
                }
            }
        }

    }

    private void update() {
        logger.info("Updating the consumer...");
        shutdown();
        preInit();
        // assign();
        runThreads.clear();
        start();
    }

    @SuppressWarnings("unchecked")
    public void consumeFromPartition(final String server, final String partitionString,
            final KafkaRecordProcessor<K, V>... kafkaRecordProcessors) {
        if (!running.get()) {
            specificPartitions = true;
            this.server = server;
            this.topic = partitionString;
            this.setRecordProcessors(Arrays.asList(kafkaRecordProcessors));
            init();
        }
    }

    @PreDestroy
    public void shutdown() {
        running.set(false);
        for (final KafkaConsumer<K, V> consumer : consumers) {
            consumer.wakeup();
        }
        monitor.wakeup();
        if (simpleProducer != null) {
            simpleProducer.shutdown();
        }
    }

    /**
     * Add a topic-partition
     */
    public synchronized void addTopicPartition(final String topicPartition, final int maxPartitions) {
        final Set<String> topicSet = new HashSet<String>(Arrays.asList(this.topic.split(",")));
        topicSet.remove("");
        if (topicSet.size() < maxPartitions && topicSet.add(topicPartition)) {
            logger.warn("Adding topic-partition - " + topicPartition);
            this.topic = StringUtils.join(topicSet, ',');
            update();
        }
    }

    /**
     * Add a topic-partition
     */
    public synchronized void addTopicPartition(final String topicPartition) {
        final Set<String> topicSet = new HashSet<String>(Arrays.asList(this.topic.split(",")));
        topicSet.remove("");
        if (topicSet.add(topicPartition)) {
            logger.warn("Adding topic-partition - " + topicPartition);
            this.topic = StringUtils.join(topicSet, ',');
            update();
        }
    }

    /**
     * Add a topic-partition
     */
    public synchronized void removeTopicPartition(final String topicPartition) {
        final Set<String> topicSet = new HashSet<String>(Arrays.asList(this.topic.split(",")));
        if (topicSet.remove(topicPartition)) {
            logger.warn("Removing topic-partition - " + topicPartition);
            this.topic = StringUtils.join(topicSet, ',');
            update();
        }
    }

    /**
     * Re assign the topics
     */
    public synchronized void reassignTopic(final String topic) {
        logger.warn("Re-assigning topic " + this.topic + " -> " + topic);
        this.topic = topic;
        update();
    }

    /**
     * get the partition lag
     *
     * @return the partition lag
     */
    public Long getPartitionLag(final String topic, final Integer partition) {
        return partitionLags.get(topic).get(partition);
    }

    private Long calculateTopicLag(final String topic) {
        long lag = 0;
        for (final Integer partition : partitionLags.get(topic).keySet()) {
            lag += partitionLags.get(topic).get(partition);
        }
        return lag;
    }

    /**
     * get the topic lag
     *
     * @return the topic lag
     */
    public Long getTopicLag(final String topic) {
        return topicLags.get(topic);
    }

    /**
     * get the topic lag
     *
     * @return the topic lag
     */
    public Long getTopicLag() {
        return getTopicLag(topic);
    }

    /**
     * @return the monitorThread
     */
    public Thread getMonitorThread() {
        return monitorThread;
    }

    /**
     * @return the monitorEnabled
     */
    public boolean isMonitorEnabled() {
        return monitorEnabled;
    }

    /**
     * @param monitorEnabled
     *            the monitorEnabled to set
     */
    public void setMonitorEnabled(final boolean monitorEnabled) {
        this.monitorEnabled = monitorEnabled;
    }

    /**
     * @return the consumerCount
     */
    public int getConsumerCount() {
        return consumerCount;
    }

    /**
     * @param consumerCount
     *            the consumerCount to set
     */
    public void setConsumerCount(final int consumerCount) {
        this.consumerCount = consumerCount;
    }

    /**
     * @return the consumers
     */
    public List<KafkaConsumer<K, V>> getConsumers() {
        return consumers;
    }

    /**
     * @return the runThreads
     */
    public List<KafkaConsumerThread> getRunThreads() {
        return runThreads;
    }

    /**
     * Return the first consumer
     *
     * @return
     */
    public KafkaConsumer<K, V> getConsumer() {
        return consumers.get(0);
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
     * @return the groupId
     */
    public String getGroupId() {
        return groupId;
    }

    /**
     * @param groupId
     *            the groupId to set
     */
    public void setGroupId(final String groupId) {
        this.groupId = groupId;
    }

    /**
     * @return the enableAutoCommit
     */
    public boolean isEnableAutoCommit() {
        return enableAutoCommit;
    }

    /**
     * @param enableAutoCommit
     *            the enableAutoCommit to set
     */
    public void setEnableAutoCommit(final boolean enableAutoCommit) {
        this.enableAutoCommit = enableAutoCommit;
    }

    /**
     * @return the commitSyncEnabled
     */
    public boolean isCommitSyncEnabled() {
        return commitSyncEnabled;
    }

    /**
     * @param commitSyncEnabled
     *            the commitSyncEnabled to set
     */
    public void setCommitSyncEnabled(final boolean commitSyncEnabled) {
        this.commitSyncEnabled = commitSyncEnabled;
    }

    /**
     * @return the autoCommitIntervalMillis
     */
    public int getAutoCommitIntervalMillis() {
        return autoCommitIntervalMillis;
    }

    /**
     * @param autoCommitIntervalMillis
     *            the autoCommitIntervalMillis to set
     */
    public void setAutoCommitIntervalMillis(final int autoCommitIntervalMillis) {
        this.autoCommitIntervalMillis = autoCommitIntervalMillis;
    }

    /**
     * @return the sessionTimeoutMillis
     */
    public int getSessionTimeoutMillis() {
        return sessionTimeoutMillis;
    }

    /**
     * @param sessionTimeoutMillis
     *            the sessionTimeoutMillis to set
     */
    public void setSessionTimeoutMillis(final int sessionTimeoutMillis) {
        this.sessionTimeoutMillis = sessionTimeoutMillis;
    }

    /**
     * @return the keyDeserializer
     */
    public Deserializer<K> getKeyDeserializer() {
        return keyDeserializer;
    }

    /**
     * @param keyDeserializer
     *            the keyDeserializer to set
     */
    public void setKeyDeserializer(final Deserializer<K> keyDeserializer) {
        this.keyDeserializer = keyDeserializer;
    }

    /**
     * @return the valueDeserializer
     */
    public Deserializer<V> getValueDeserializer() {
        return valueDeserializer;
    }

    /**
     * @param valueDeserializer
     *            the valueDeserializer to set
     */
    public void setValueDeserializer(final Deserializer<V> valueDeserializer) {
        this.valueDeserializer = valueDeserializer;
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
     * @return the specificPartitions
     */
    public boolean isSpecificPartitions() {
        return specificPartitions;
    }

    /**
     * @param specificPartitions
     *            the specificPartitions to set
     */
    public void setSpecificPartitions(final boolean specificPartitions) {
        this.specificPartitions = specificPartitions;
    }

    /**
     * @return the pollTimeout
     */
    public int getPollTimeout() {
        return pollTimeout;
    }

    /**
     * @param pollTimeout
     *            the pollTimeout to set
     */
    public void setPollTimeout(final int pollTimeout) {
        this.pollTimeout = pollTimeout;
    }

    /**
     * @return the monitorSleepMillis
     */
    public int getMonitorSleepMillis() {
        return monitorSleepMillis;
    }

    /**
     * @param monitorSleepMillis
     *            the monitorSleepMillis to set
     */
    public void setMonitorSleepMillis(final int monitorSleepMillis) {
        this.monitorSleepMillis = monitorSleepMillis;
    }

    /**
     * @return the running
     */
    public AtomicBoolean getRunning() {
        return running;
    }

    /**
     * @return the recordProcessors
     */
    public List<KafkaRecordProcessor<K, V>> getRecordProcessors() {
        return recordProcessors;
    }

    /**
     * @param recordProcessors
     *            the recordProcessors to set
     */
    public void setRecordProcessors(final List<KafkaRecordProcessor<K, V>> recordProcessors) {
        this.recordProcessors = recordProcessors;
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
     * @return the monitor
     */
    public KafkaConsumer<K, V> getMonitor() {
        return monitor;
    }

    /**
     * @return the statusCounter
     */
    public EnumCounter<Status> getStatusCounter() {
        return statusCounter;
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @param name
     *            the name to set
     */
    public void setName(final String name) {
        this.name = name;
    }

    public boolean isRepostEnabled() {
        return repostEnabled;
    }

    public void setRepostEnabled(final boolean repostEnabled) {
        this.repostEnabled = repostEnabled;
    }

    /**
     * @return the commitAfterProcess
     */
    public boolean isCommitAfterProcess() {
        return commitAfterProcess;
    }

    /**
     * @param commitAfterProcess
     *            the commitAfterProcess to set
     */
    public void setCommitAfterProcess(final boolean commitAfterProcess) {
        this.commitAfterProcess = commitAfterProcess;
    }

}
