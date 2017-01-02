/**
 *
 */
package com.bluejeans.kafka;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.Test;

import com.bluejeans.utils.zookeeper.ZkHelper;

/**
 * @author Dinesh Ilindra
 *
 */
public class KafkaConsumerWithLockTest {

    //test with locks
    @Test
    public void testWithLocks() throws Exception {
        final CuratorFramework client = CuratorFrameworkFactory.newClient("10.5.7.65:2181", new ExponentialBackoffRetry(1000, 3));
        final ZkHelper helper = new ZkHelper(client);
        final KafkaConsumerWithZKLock<String, String> consumer = new KafkaConsumerWithZKLock<>();
        consumer.setZkHelper(helper);
        consumer.setLockPrefix("/bjn/indigo/locks/testing");
        consumer.setGroupId("test-consumer");
        consumer.setServer("10.5.7.246:9092");
        consumer.setTopic("indigo-aggregator-test");
        consumer.init();
    }

}
