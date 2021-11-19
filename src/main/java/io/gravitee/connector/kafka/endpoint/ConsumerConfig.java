/**
 * Copyright (C) 2015 The Gravitee team (http://gravitee.io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.gravitee.connector.kafka.endpoint;

import io.gravitee.connector.kafka.configuration.AutoOffsetReset;
import io.gravitee.connector.kafka.configuration.IsolationLevel;
import io.gravitee.connector.kafka.configuration.SecurityProtocol;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.CommonClientConfigs;

/**
 * @author David BRASSELY (david.brassely at graviteesource.com)
 * @author GraviteeSource Team
 */
public class ConsumerConfig {

    private String groupId;

    private String groupInstanceId;

    private int sessionTimeoutMs = 10000;

    private int heartbeatIntervalMs = 3000;

    private String partitionAssignmentStrategy;

    private int metadataMaxAgeMs = 300000;

    private boolean enableAutoCommit = true;

    private int autoCommitIntervalMs = 5000;

    private String clientId = "";

    private String clientRack = "";

    private int maxPartitionFetchBytes = 1048576;

    private int sendBufferBytes = 131072;

    private int receiveBufferBytes = 65536;

    private int fetchMinBytes = 1;

    private int fetchMaxBytes = 52428800;

    private int fetchMaxWaitMs = 500;

    private int reconnectBackoffMs = 50;

    private int reconnectBackoffMaxMs = 1000;

    private int retryBackoffMs = 100;

    private String autoOffsetReset = "latest";

    private boolean checkCRCS = true;

    private int metricsSampleWindowMs = 30000;

    private int metricsNumSamples = 2;

    private String metricsRecordingLevel = "INFO";

    private List<String> metricReporters = Collections.emptyList();

    private String keyDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";

    private String valueDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";

    private int requestTimeoutMs = 30000;

    private int defaultApiTimeoutMs = 60000;

    private int connectionsMaxIdleMs = 540000;

    private List<String> interceptorClasses = Collections.emptyList();

    private int maxPollRecords = 500;

    private int maxPollIntervalMs = 300000;

    private boolean excludeInternalTopics = true;

    private String isolationLevel = "READ_UNCOMMITTED";

    private boolean allowAutoCreateTopics = true;

    private List<String> securityProviders = Collections.emptyList();

    private String securityProtocol = "PLAINTEXT";

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getGroupInstanceId() {
        return groupInstanceId;
    }

    public void setGroupInstanceId(String groupInstanceId) {
        this.groupInstanceId = groupInstanceId;
    }

    public int getSessionTimeoutMs() {
        return sessionTimeoutMs;
    }

    public void setSessionTimeoutMs(int sessionTimeoutMs) {
        this.sessionTimeoutMs = sessionTimeoutMs;
    }

    public int getHeartbeatIntervalMs() {
        return heartbeatIntervalMs;
    }

    public void setHeartbeatIntervalMs(int heartbeatIntervalMs) {
        this.heartbeatIntervalMs = heartbeatIntervalMs;
    }

    public String getPartitionAssignmentStrategy() {
        return partitionAssignmentStrategy;
    }

    public void setPartitionAssignmentStrategy(String partitionAssignmentStrategy) {
        this.partitionAssignmentStrategy = partitionAssignmentStrategy;
    }

    public int getMetadataMaxAgeMs() {
        return metadataMaxAgeMs;
    }

    public void setMetadataMaxAgeMs(int metadataMaxAgeMs) {
        this.metadataMaxAgeMs = metadataMaxAgeMs;
    }

    public boolean isEnableAutoCommit() {
        return enableAutoCommit;
    }

    public void setEnableAutoCommit(boolean enableAutoCommit) {
        this.enableAutoCommit = enableAutoCommit;
    }

    public int getAutoCommitIntervalMs() {
        return autoCommitIntervalMs;
    }

    public void setAutoCommitIntervalMs(int autoCommitIntervalMs) {
        this.autoCommitIntervalMs = autoCommitIntervalMs;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getClientRack() {
        return clientRack;
    }

    public void setClientRack(String clientRack) {
        this.clientRack = clientRack;
    }

    public int getMaxPartitionFetchBytes() {
        return maxPartitionFetchBytes;
    }

    public void setMaxPartitionFetchBytes(int maxPartitionFetchBytes) {
        this.maxPartitionFetchBytes = maxPartitionFetchBytes;
    }

    public int getSendBufferBytes() {
        return sendBufferBytes;
    }

    public void setSendBufferBytes(int sendBufferBytes) {
        this.sendBufferBytes = sendBufferBytes;
    }

    public int getReceiveBufferBytes() {
        return receiveBufferBytes;
    }

    public void setReceiveBufferBytes(int receiveBufferBytes) {
        this.receiveBufferBytes = receiveBufferBytes;
    }

    public int getFetchMinBytes() {
        return fetchMinBytes;
    }

    public void setFetchMinBytes(int fetchMinBytes) {
        this.fetchMinBytes = fetchMinBytes;
    }

    public int getFetchMaxBytes() {
        return fetchMaxBytes;
    }

    public void setFetchMaxBytes(int fetchMaxBytes) {
        this.fetchMaxBytes = fetchMaxBytes;
    }

    public int getFetchMaxWaitMs() {
        return fetchMaxWaitMs;
    }

    public void setFetchMaxWaitMs(int fetchMaxWaitMs) {
        this.fetchMaxWaitMs = fetchMaxWaitMs;
    }

    public int getReconnectBackoffMs() {
        return reconnectBackoffMs;
    }

    public void setReconnectBackoffMs(int reconnectBackoffMs) {
        this.reconnectBackoffMs = reconnectBackoffMs;
    }

    public int getReconnectBackoffMaxMs() {
        return reconnectBackoffMaxMs;
    }

    public void setReconnectBackoffMaxMs(int reconnectBackoffMaxMs) {
        this.reconnectBackoffMaxMs = reconnectBackoffMaxMs;
    }

    public int getRetryBackoffMs() {
        return retryBackoffMs;
    }

    public void setRetryBackoffMs(int retryBackoffMs) {
        this.retryBackoffMs = retryBackoffMs;
    }

    public String getAutoOffsetReset() {
        return autoOffsetReset;
    }

    public void setAutoOffsetReset(String autoOffsetReset) {
        this.autoOffsetReset = autoOffsetReset;
    }

    public boolean isCheckCRCS() {
        return checkCRCS;
    }

    public void setCheckCRCS(boolean checkCRCS) {
        this.checkCRCS = checkCRCS;
    }

    public int getMetricsSampleWindowMs() {
        return metricsSampleWindowMs;
    }

    public void setMetricsSampleWindowMs(int metricsSampleWindowMs) {
        this.metricsSampleWindowMs = metricsSampleWindowMs;
    }

    public int getMetricsNumSamples() {
        return metricsNumSamples;
    }

    public void setMetricsNumSamples(int metricsNumSamples) {
        this.metricsNumSamples = metricsNumSamples;
    }

    public String getMetricsRecordingLevel() {
        return metricsRecordingLevel;
    }

    public void setMetricsRecordingLevel(String metricsRecordingLevel) {
        this.metricsRecordingLevel = metricsRecordingLevel;
    }

    public List<String> getMetricReporters() {
        return metricReporters;
    }

    public void setMetricReporters(List<String> metricReporters) {
        this.metricReporters = metricReporters;
    }

    public String getKeyDeserializer() {
        return keyDeserializer;
    }

    public void setKeyDeserializer(String keyDeserializer) {
        this.keyDeserializer = keyDeserializer;
    }

    public String getValueDeserializer() {
        return valueDeserializer;
    }

    public void setValueDeserializer(String valueDeserializer) {
        this.valueDeserializer = valueDeserializer;
    }

    public int getRequestTimeoutMs() {
        return requestTimeoutMs;
    }

    public void setRequestTimeoutMs(int requestTimeoutMs) {
        this.requestTimeoutMs = requestTimeoutMs;
    }

    public int getDefaultApiTimeoutMs() {
        return defaultApiTimeoutMs;
    }

    public void setDefaultApiTimeoutMs(int defaultApiTimeoutMs) {
        this.defaultApiTimeoutMs = defaultApiTimeoutMs;
    }

    public int getConnectionsMaxIdleMs() {
        return connectionsMaxIdleMs;
    }

    public void setConnectionsMaxIdleMs(int connectionsMaxIdleMs) {
        this.connectionsMaxIdleMs = connectionsMaxIdleMs;
    }

    public List<String> getInterceptorClasses() {
        return interceptorClasses;
    }

    public void setInterceptorClasses(List<String> interceptorClasses) {
        this.interceptorClasses = interceptorClasses;
    }

    public int getMaxPollRecords() {
        return maxPollRecords;
    }

    public void setMaxPollRecords(int maxPollRecords) {
        this.maxPollRecords = maxPollRecords;
    }

    public int getMaxPollIntervalMs() {
        return maxPollIntervalMs;
    }

    public void setMaxPollIntervalMs(int maxPollIntervalMs) {
        this.maxPollIntervalMs = maxPollIntervalMs;
    }

    public boolean isExcludeInternalTopics() {
        return excludeInternalTopics;
    }

    public void setExcludeInternalTopics(boolean excludeInternalTopics) {
        this.excludeInternalTopics = excludeInternalTopics;
    }

    public String getIsolationLevel() {
        return isolationLevel;
    }

    public void setIsolationLevel(String isolationLevel) {
        this.isolationLevel = isolationLevel;
    }

    public boolean isAllowAutoCreateTopics() {
        return allowAutoCreateTopics;
    }

    public void setAllowAutoCreateTopics(boolean allowAutoCreateTopics) {
        this.allowAutoCreateTopics = allowAutoCreateTopics;
    }

    public List<String> getSecurityProviders() {
        return securityProviders;
    }

    public void setSecurityProviders(List<String> securityProviders) {
        this.securityProviders = securityProviders;
    }

    public String getSecurityProtocol() {
        return securityProtocol;
    }

    public void setSecurityProtocol(String securityProtocol) {
        this.securityProtocol = securityProtocol;
    }

    public Map<String, String> getKafkaConfig() {
        final Map<String, String> config = new HashMap<>();

        config.put(org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG, getGroupId());
        config.put(org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, getGroupInstanceId());
        config.put(org.apache.kafka.clients.consumer.ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, Integer.toString(getSessionTimeoutMs()));
        config.put(
            org.apache.kafka.clients.consumer.ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG,
            Integer.toString(getHeartbeatIntervalMs())
        );
        config.put(org.apache.kafka.clients.consumer.ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, getPartitionAssignmentStrategy());
        config.put(org.apache.kafka.clients.consumer.ConsumerConfig.METADATA_MAX_AGE_CONFIG, Integer.toString(getMetadataMaxAgeMs()));
        config.put(org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.toString(isEnableAutoCommit()));
        config.put(
            org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,
            Integer.toString(getAutoCommitIntervalMs())
        );
        config.put(org.apache.kafka.clients.consumer.ConsumerConfig.CLIENT_ID_CONFIG, getClientId());
        config.put(org.apache.kafka.clients.consumer.ConsumerConfig.CLIENT_RACK_CONFIG, getClientRack());
        config.put(
            org.apache.kafka.clients.consumer.ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG,
            Integer.toString(getMaxPartitionFetchBytes())
        );
        config.put(org.apache.kafka.clients.consumer.ConsumerConfig.SEND_BUFFER_CONFIG, Integer.toString(getSendBufferBytes()));
        config.put(org.apache.kafka.clients.consumer.ConsumerConfig.RECEIVE_BUFFER_CONFIG, Integer.toString(getReceiveBufferBytes()));
        config.put(org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MIN_BYTES_CONFIG, Integer.toString(getFetchMinBytes()));
        config.put(org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MAX_BYTES_CONFIG, Integer.toString(getFetchMaxBytes()));
        config.put(org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, Integer.toString(getFetchMaxWaitMs()));
        config.put(org.apache.kafka.clients.consumer.ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, Integer.toString(getReconnectBackoffMs()));
        config.put(
            org.apache.kafka.clients.consumer.ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG,
            Integer.toString(getReconnectBackoffMaxMs())
        );
        config.put(org.apache.kafka.clients.consumer.ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, Integer.toString(getRetryBackoffMs()));
        config.put(
            org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
            AutoOffsetReset.getOrDefault(getAutoOffsetReset()).toString()
        );
        config.put(org.apache.kafka.clients.consumer.ConsumerConfig.CHECK_CRCS_CONFIG, Boolean.toString(isCheckCRCS()));
        config.put(
            org.apache.kafka.clients.consumer.ConsumerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG,
            Integer.toString(getMetricsSampleWindowMs())
        );
        config.put(org.apache.kafka.clients.consumer.ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG, Integer.toString(getMetricsNumSamples()));
        config.put(org.apache.kafka.clients.consumer.ConsumerConfig.METRICS_RECORDING_LEVEL_CONFIG, getMetricsRecordingLevel());
        config.put(org.apache.kafka.clients.consumer.ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, String.join(",", getMetricReporters()));
        config.put(org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, getKeyDeserializer());
        config.put(org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, getValueDeserializer());
        config.put(org.apache.kafka.clients.consumer.ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, Integer.toString(getRequestTimeoutMs()));
        config.put(
            org.apache.kafka.clients.consumer.ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG,
            Integer.toString(getDefaultApiTimeoutMs())
        );
        config.put(
            org.apache.kafka.clients.consumer.ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG,
            Integer.toString(getConnectionsMaxIdleMs())
        );
        config.put(org.apache.kafka.clients.consumer.ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, String.join(",", getInterceptorClasses()));
        config.put(org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Integer.toString(getMaxPollRecords()));
        config.put(org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, Integer.toString(getMaxPollIntervalMs()));
        config.put(
            org.apache.kafka.clients.consumer.ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG,
            Boolean.toString(isExcludeInternalTopics())
        );
        config.put(
            org.apache.kafka.clients.consumer.ConsumerConfig.ISOLATION_LEVEL_CONFIG,
            IsolationLevel.getOrDefault(getIsolationLevel()).toString()
        );
        config.put(
            org.apache.kafka.clients.consumer.ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG,
            Boolean.toString(isAllowAutoCreateTopics())
        );
        config.put(org.apache.kafka.clients.consumer.ConsumerConfig.SECURITY_PROVIDERS_CONFIG, String.join(",", getSecurityProviders()));
        config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.getOrDefault(getSecurityProtocol()).toString());

        return config;
    }
}
