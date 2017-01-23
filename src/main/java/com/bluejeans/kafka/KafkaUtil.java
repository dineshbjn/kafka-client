package com.bluejeans.kafka;

import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;

import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;

/**
 * @author Dinesh Ilindra
 */
public class KafkaUtil {

    private String zkHost = null;
    private int sessionTimeOutInMs = 15 * 1000;
    private int connectionTimeOutInMs = 10 * 1000;
    private ZkClient zkClient = null;
    private ZkUtils zkUtils = null;
    private final Properties topicConfiguration = new Properties();

    /**
     * the default one
     */
    public KafkaUtil() {
    }

    /**
     * @param zkHost
     */
    public KafkaUtil(final String zkHost) {
        this.zkHost = zkHost;
    }

    /**
     * @param zkHost
     * @param sessionTimeOutInMs
     * @param connectionTimeOutInMs
     */
    public KafkaUtil(final String zkHost, final int sessionTimeOutInMs, final int connectionTimeOutInMs) {
        this.zkHost = zkHost;
        this.sessionTimeOutInMs = sessionTimeOutInMs;
        this.connectionTimeOutInMs = connectionTimeOutInMs;
    }

    /**
     * for chaining
     *
     * @return this
     */
    public KafkaUtil setup() {
        init();
        return this;
    }

    /**
     * initialize
     */
    public void init() {
        zkClient = new ZkClient(zkHost, sessionTimeOutInMs, connectionTimeOutInMs, ZKStringSerializer$.MODULE$);
        zkUtils = new ZkUtils(zkClient, new ZkConnection(zkHost), false);
    }

    /**
     * destroy
     */
    public void destroy() {
        if (zkClient != null) {
            zkClient.close();
        }
    }

    /**
     * create a topic
     *
     * @param topicName
     * @param partitionCount
     * @param replicationCount
     */
    public void createTopic(final String topicName, final int partitionCount, final int replicationCount) {
        AdminUtils.createTopic(zkUtils, topicName, partitionCount, replicationCount, topicConfiguration);
    }

    /**
     * delete topic
     *
     * @param topicName
     */
    public void deleteTopic(final String topicName) {
        AdminUtils.deleteTopic(zkUtils, topicName);
    }

    /**
     * @param topicName
     * @param count
     */
    public void addPartitions(final String topicName, final int count) {
        AdminUtils.addPartitions(zkUtils, topicName, count, "", true);
    }

    /**
     * @param topicName
     * @return
     */
    public boolean topicExists(final String topicName) {
        return AdminUtils.topicExists(zkUtils, topicName);
    }

    /**
     * @param topicName
     * @return
     */
    public int partitionSize(final String topicName) {
        return AdminUtils.fetchTopicMetadataFromZk(topicName, zkUtils).partitionsMetadata().size();
    }

    /**
     * @return the zkHost
     */
    public String getZkHost() {
        return zkHost;
    }

    /**
     * @param zkHost
     *            the zkHost to set
     */
    public void setZkHost(final String zkHost) {
        this.zkHost = zkHost;
    }

    /**
     * @return the sessionTimeOutInMs
     */
    public int getSessionTimeOutInMs() {
        return sessionTimeOutInMs;
    }

    /**
     * @param sessionTimeOutInMs
     *            the sessionTimeOutInMs to set
     */
    public void setSessionTimeOutInMs(final int sessionTimeOutInMs) {
        this.sessionTimeOutInMs = sessionTimeOutInMs;
    }

    /**
     * @return the connectionTimeOutInMs
     */
    public int getConnectionTimeOutInMs() {
        return connectionTimeOutInMs;
    }

    /**
     * @param connectionTimeOutInMs
     *            the connectionTimeOutInMs to set
     */
    public void setConnectionTimeOutInMs(final int connectionTimeOutInMs) {
        this.connectionTimeOutInMs = connectionTimeOutInMs;
    }

    /**
     * @return the zkClient
     */
    public ZkClient getZkClient() {
        return zkClient;
    }

    /**
     * @return the zkUtils
     */
    public ZkUtils getZkUtils() {
        return zkUtils;
    }

    /**
     * @return the topicConfiguration
     */
    public Properties getTopicConfiguration() {
        return topicConfiguration;
    }

}
