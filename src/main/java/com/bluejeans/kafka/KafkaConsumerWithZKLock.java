package com.bluejeans.kafka;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bluejeans.utils.zookeeper.ZKLock;
import com.bluejeans.utils.zookeeper.ZkHelper;
import com.bluejeans.utils.zookeeper.ZkHelper.LockListener;

public class KafkaConsumerWithZKLock<K, V> extends SimpleKafkaConsumer<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerWithZKLock.class);

    private ZkHelper zkHelper;

    private String lockPrefix;
    private int maxPartitions = 4;
    private final Map<String, ZKLock> locks = new ConcurrentHashMap<>();

    @Override
    @PostConstruct
    public void init() {
        preInit();
        setSpecificPartitions(true);
        final List<String> topics = Arrays.asList(getTopic().split(","));
        setTopic("");
        for (final String topic : topics) {
            int max = maxPartitions;
            final List<PartitionInfo> partitionInfos = getConsumers().get(0).partitionsFor(topic);
            if (partitionInfos == null) {
                throw new RuntimeException("Topic not found - " + topic);
            }
            if (max > partitionInfos.size()) {
                max = partitionInfos.size();
            }
            final List<String> items = partitionInfos.stream().map(pi -> String.valueOf(pi.partition()))
                    .collect(Collectors.toList());
            zkHelper.lockSomeAsync(lockPrefix + "/" + topic + "/", items, max, new LockListener() {
                @Override
                public void lockObtained(final String prefix, final String path, final ZKLock zkLock) {
                    locks.put(topic + ":" + path, zkLock);
                    try {
                        addTopicPartition(topic + ":" + path, maxPartitions);
                    } catch (final RuntimeException re) {
                        logger.error("Problem starting the consumer", re);
                    }
                }

                @Override
                public void lockReleased(final String prefix, final String path, final ZKLock zkLock) {
                    locks.remove(topic + ":" + path);
                    try {
                        removeTopicPartition(topic + ":" + path);
                    } catch (final RuntimeException re) {
                        logger.error("Problem starting the consumer", re);
                    }
                }
            });
        }
        // start();
    }

    @Override
    @PreDestroy
    public void shutdown() {
        super.shutdown();
        locks.values().forEach(l -> l.release());
    }

    // @RegistryAttribute(name = "queues", type = "array")
    public String getQueueName() {
        final Map<String, List<String>> assignMap = new HashMap<String, List<String>>();
        for (final String assigned : getTopic().split(",")) {
            final String[] data = assigned.split(":");
            assignMap.putIfAbsent(data[0], new ArrayList<String>());
            if (data.length > 1) {
                assignMap.get(data[0]).add(data[1]);
            }
        }
        ;
        final StringBuilder builder = new StringBuilder();
        assignMap.forEach((topic, partitions) -> {
            builder.append("," + topic + ":");
            Collections.sort(partitions);
            partitions.forEach(partition -> builder.append(partition + "_"));
        });
        return builder.substring(1);
    }

    /**
     * @return the zkHelper
     */
    public ZkHelper getZkHelper() {
        return zkHelper;
    }

    /**
     * @param zkHelper
     *            the zkHelper to set
     */
    public void setZkHelper(final ZkHelper zkHelper) {
        this.zkHelper = zkHelper;
    }

    /**
     * @return the lockPrefix
     */
    public String getLockPrefix() {
        return lockPrefix;
    }

    /**
     * @param lockPrefix
     *            the lockPrefix to set
     */
    public void setLockPrefix(final String lockPrefix) {
        this.lockPrefix = lockPrefix;
    }

    /**
     * @return the maxPartitions
     */
    public int getMaxPartitions() {
        return maxPartitions;
    }

    /**
     * @param maxPartitions
     *            the maxPartitions to set
     */
    public void setMaxPartitions(final int maxPartitions) {
        this.maxPartitions = maxPartitions;
    }

    /**
     * @return the locks
     */
    public Map<String, ZKLock> getLocks() {
        return locks;
    }

}
