/**
 *
 */
package com.bluejeans.kafka;

import java.util.Random;

import org.junit.Test;

/**
 * @author Dinesh Ilindra
 *
 */
public class KafkaUtilTest {

    @Test
    public void testKafkaUtil() {
        final KafkaUtil util = new KafkaUtil("10.5.7.65:2181");
        final String topicName = "create-test-" + System.currentTimeMillis();
        util.init();
        util.createTopic(topicName, new Random().nextInt(10) + 1, 1);
        System.out.println(util.partitionSize(topicName));
        // KafkaUtil.createTopic("10.5.7.65:2181", "create-test-fail", 8, 2);
    }

}
