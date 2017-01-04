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
    private boolean liveUpdateEnabled = true;
    private final Map<String, ZKLock> allLocks = new ConcurrentHashMap<>();
    private final Map<String, ZKLock> currentLocks = new ConcurrentHashMap<>();

    @Override
    @PostConstruct
    public void init() {
        preInit();
        setSpecificPartitions(true);
        final List<String> topics = Arrays.asList(getTopic().split(","));
        setTopic("");
        for (final String topicName : topics) {
            final List<PartitionInfo> partitionInfos = getConsumers().get(0).partitionsFor(topicName);
            if (partitionInfos == null) {
                throw new RuntimeException("Topic not found - " + topicName);
            }
            if (maxPartitions > partitionInfos.size()) {
                maxPartitions = partitionInfos.size();
            }
            final String pathPrefix = lockPrefix + "/" + topicName + "/" + getGroupId() + "/";
            final List<String> items = partitionInfos.stream().map(pi -> String.valueOf(pi.partition()))
                    .collect(Collectors.toList());
            items.forEach(i -> allLocks.put(pathPrefix + i, new ZKLock(zkHelper.getZkClient(), pathPrefix + i)));
            zkHelper.lockSomeAsync(allLocks, maxPartitions, new LockListener() {
                @Override
                public void lockObtained(final String path, final ZKLock zkLock) {
                    final String tp = topicName + ":" + path.substring(path.lastIndexOf('/') + 1);
                    synchronized (zkHelper) {
                        currentLocks.put(tp, zkLock);
                        try {
                            addTopicPartition(tp, liveUpdateEnabled && currentLocks.size() <= getConsumerCount());
                        } catch (final RuntimeException re) {
                            logger.error("Problem starting the consumer", re);
                        }
                    }
                }

                @Override
                public void lockReleased(final String path, final ZKLock zkLock) {
                    final String tp = topicName + ":" + path.substring(path.lastIndexOf('/') + 1);
                    synchronized (zkHelper) {
                        currentLocks.remove(tp, zkLock);
                        try {
                            removeTopicPartition(tp, liveUpdateEnabled && currentLocks.size() < getConsumerCount());
                        } catch (final RuntimeException re) {
                            logger.error("Problem starting the consumer", re);
                        }
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
        currentLocks.values().forEach(l -> l.release());
        zkHelper.cancelAll(allLocks.values());
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
     * @return the liveUpdateEnabled
     */
    public boolean isLiveUpdateEnabled() {
        return liveUpdateEnabled;
    }

    /**
     * @param liveUpdateEnabled
     *            the liveUpdateEnabled to set
     */
    public void setLiveUpdateEnabled(final boolean liveUpdateEnabled) {
        this.liveUpdateEnabled = liveUpdateEnabled;
    }

}
