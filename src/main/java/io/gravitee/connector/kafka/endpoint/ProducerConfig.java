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

import io.gravitee.connector.kafka.configuration.CompressionType;
import io.gravitee.connector.kafka.configuration.MetricsRecordingLevel;
import io.gravitee.connector.kafka.configuration.SecurityProtocol;
import org.apache.kafka.clients.CommonClientConfigs;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author David BRASSELY (david.brassely at graviteesource.com)
 * @author GraviteeSource Team
 */
public class ProducerConfig {

    private int bufferMemory = 33554432;

    private int retries = 2147483647;

    private String acks = "1";

    private String compressionType = "none";

    private int batchSize = 16384;

    private int lingerMs = 0;

    private int deliveryTimeoutMs = 120000;

    private String clientId = "";

    private int sendBufferBytes = 131072;

    private int receiveBufferBytes = 32768;

    private int maxRequestSize = 1048576;

    private int reconnectBackoffMs = 50;

    private int reconnectBackoffMaxMs = 1000;

    private int retryBackoffMs = 100;

    private int maxBlockMs = 60000;

    private int requestTimeoutMs = 30000;

    private int metadataMaxAgeMs = 30000;

    private int metadataMaxIdleMs = 300000;

    private int metricsSampleWindowMs = 30000;

    private int metricsNumSamples = 2;

    private String metricsRecordingLevel = "INFO";

    private List<String> metricReporters = Collections.emptyList();

    private int maxInFlightRequestsPerConnection = 5;

    private String keySerializer = "org.apache.kafka.common.serialization.StringSerializer";

    private String valueSerializer = "org.apache.kafka.common.serialization.StringSerializer";

    private int connectionsMaxIdleMs = 540000;

    private String partitionerClass = "org.apache.kafka.clients.producer.internals.DefaultPartitioner";

    private List<String> interceptorClasses = Collections.emptyList();

    private String securityProtocol = "PLAINTEXT";

    private List<String> securityProviders = Collections.emptyList();

    private boolean enableIdempotence = false;

    private int transactionTimeoutMs = 60000;

    private String transactionalId = "";

    public int getBufferMemory() {
        return bufferMemory;
    }

    public void setBufferMemory(int bufferMemory) {
        this.bufferMemory = bufferMemory;
    }

    public int getRetries() {
        return retries;
    }

    public void setRetries(int retries) {
        this.retries = retries;
    }

    public String getAcks() {
        return acks;
    }

    public void setAcks(String acks) {
        this.acks = acks;
    }

    public String getCompressionType() {
        return compressionType;
    }

    public void setCompressionType(String compressionType) {
        this.compressionType = compressionType;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getLingerMs() {
        return lingerMs;
    }

    public void setLingerMs(int lingerMs) {
        this.lingerMs = lingerMs;
    }

    public int getDeliveryTimeoutMs() {
        return deliveryTimeoutMs;
    }

    public void setDeliveryTimeoutMs(int deliveryTimeoutMs) {
        this.deliveryTimeoutMs = deliveryTimeoutMs;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
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

    public int getMaxRequestSize() {
        return maxRequestSize;
    }

    public void setMaxRequestSize(int maxRequestSize) {
        this.maxRequestSize = maxRequestSize;
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

    public int getMaxBlockMs() {
        return maxBlockMs;
    }

    public void setMaxBlockMs(int maxBlockMs) {
        this.maxBlockMs = maxBlockMs;
    }

    public int getRequestTimeoutMs() {
        return requestTimeoutMs;
    }

    public void setRequestTimeoutMs(int requestTimeoutMs) {
        this.requestTimeoutMs = requestTimeoutMs;
    }

    public int getMetadataMaxAgeMs() {
        return metadataMaxAgeMs;
    }

    public void setMetadataMaxAgeMs(int metadataMaxAgeMs) {
        this.metadataMaxAgeMs = metadataMaxAgeMs;
    }

    public int getMetadataMaxIdleMs() {
        return metadataMaxIdleMs;
    }

    public void setMetadataMaxIdleMs(int metadataMaxIdleMs) {
        this.metadataMaxIdleMs = metadataMaxIdleMs;
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

    public int getMaxInFlightRequestsPerConnection() {
        return maxInFlightRequestsPerConnection;
    }

    public void setMaxInFlightRequestsPerConnection(int maxInFlightRequestsPerConnection) {
        this.maxInFlightRequestsPerConnection = maxInFlightRequestsPerConnection;
    }

    public String getKeySerializer() {
        return keySerializer;
    }

    public void setKeySerializer(String keySerializer) {
        this.keySerializer = keySerializer;
    }

    public String getValueSerializer() {
        return valueSerializer;
    }

    public void setValueSerializer(String valueSerializer) {
        this.valueSerializer = valueSerializer;
    }

    public int getConnectionsMaxIdleMs() {
        return connectionsMaxIdleMs;
    }

    public void setConnectionsMaxIdleMs(int connectionsMaxIdleMs) {
        this.connectionsMaxIdleMs = connectionsMaxIdleMs;
    }

    public String getPartitionerClass() {
        return partitionerClass;
    }

    public void setPartitionerClass(String partitionerClass) {
        this.partitionerClass = partitionerClass;
    }

    public List<String> getInterceptorClasses() {
        return interceptorClasses;
    }

    public void setInterceptorClasses(List<String> interceptorClasses) {
        this.interceptorClasses = interceptorClasses;
    }

    public String getSecurityProtocol() {
        return securityProtocol;
    }

    public void setSecurityProtocol(String securityProtocol) {
        this.securityProtocol = securityProtocol;
    }

    public List<String> getSecurityProviders() {
        return securityProviders;
    }

    public void setSecurityProviders(List<String> securityProviders) {
        this.securityProviders = securityProviders;
    }

    public boolean isEnableIdempotence() {
        return enableIdempotence;
    }

    public void setEnableIdempotence(boolean enableIdempotence) {
        this.enableIdempotence = enableIdempotence;
    }

    public int getTransactionTimeoutMs() {
        return transactionTimeoutMs;
    }

    public void setTransactionTimeoutMs(int transactionTimeoutMs) {
        this.transactionTimeoutMs = transactionTimeoutMs;
    }

    public String getTransactionalId() {
        return transactionalId;
    }

    public void setTransactionalId(String transactionalId) {
        this.transactionalId = transactionalId;
    }

    public Map<String, String> getKafkaConfig() {
        final Map<String, String> config = new HashMap<>();

        config.put(
                org.apache.kafka.clients.producer.ProducerConfig.BUFFER_MEMORY_CONFIG,
                Integer.toString(getBufferMemory())
        );

        config.put(
                org.apache.kafka.clients.producer.ProducerConfig.RETRIES_CONFIG,
                Integer.toString(getRetries())
        );

        config.put(
                org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG,
                getAcks()
        );

        config.put(
                org.apache.kafka.clients.producer.ProducerConfig.COMPRESSION_TYPE_CONFIG,
                CompressionType.getOrDefault(getCompressionType()).toString()
        );

        config.put(
                org.apache.kafka.clients.producer.ProducerConfig.BATCH_SIZE_CONFIG,
                Integer.toString(getBatchSize())
        );

        config.put(
                org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG,
                Integer.toString(getLingerMs())
        );

        config.put(
                org.apache.kafka.clients.producer.ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,
                Integer.toString(getDeliveryTimeoutMs())
        );

        config.put(
                org.apache.kafka.clients.producer.ProducerConfig.CLIENT_ID_CONFIG,
                getClientId()
        );

        config.put(
                org.apache.kafka.clients.producer.ProducerConfig.SEND_BUFFER_CONFIG,
                Integer.toString(getSendBufferBytes())
        );

        config.put(
                org.apache.kafka.clients.producer.ProducerConfig.RECEIVE_BUFFER_CONFIG,
                Integer.toString(getReceiveBufferBytes())
        );

        config.put(
                org.apache.kafka.clients.producer.ProducerConfig.MAX_REQUEST_SIZE_CONFIG,
                Integer.toString(getMaxRequestSize())
        );

        config.put(
                org.apache.kafka.clients.producer.ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG,
                Integer.toString(getReconnectBackoffMs())
        );

        config.put(
                org.apache.kafka.clients.producer.ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG,
                Integer.toString(getReconnectBackoffMaxMs())
        );

        config.put(
                org.apache.kafka.clients.producer.ProducerConfig.RETRY_BACKOFF_MS_CONFIG,
                Integer.toString(getRetryBackoffMs())
        );

        config.put(
                org.apache.kafka.clients.producer.ProducerConfig.MAX_BLOCK_MS_CONFIG,
                Integer.toString(getMaxBlockMs())
        );

        config.put(
                org.apache.kafka.clients.producer.ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,
                Integer.toString(getRequestTimeoutMs())
        );

        config.put(
                org.apache.kafka.clients.producer.ProducerConfig.METADATA_MAX_AGE_CONFIG,
                Integer.toString(getMetadataMaxAgeMs())
        );

        config.put(
                org.apache.kafka.clients.producer.ProducerConfig.METADATA_MAX_IDLE_CONFIG,
                Integer.toString(getMetadataMaxIdleMs())
        );

        config.put(
                org.apache.kafka.clients.producer.ProducerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG,
                Integer.toString(getMetricsSampleWindowMs())
        );

        config.put(
                org.apache.kafka.clients.producer.ProducerConfig.METRICS_NUM_SAMPLES_CONFIG,
                Integer.toString(getMetricsNumSamples())
        );

        config.put(
                org.apache.kafka.clients.producer.ProducerConfig.METRICS_RECORDING_LEVEL_CONFIG,
                MetricsRecordingLevel.getOrDefault(getMetricsRecordingLevel()).toString()
        );

        config.put(
                org.apache.kafka.clients.producer.ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG,
                String.join(",", getMetricReporters())
        );

        config.put(
                org.apache.kafka.clients.producer.ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
                Integer.toString(getMaxInFlightRequestsPerConnection())
        );

        config.put(
                org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                getKeySerializer()
        );

        config.put(
                org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                getValueSerializer()
        );

        config.put(
                org.apache.kafka.clients.producer.ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG,
                Integer.toString(getConnectionsMaxIdleMs())
        );

        config.put(
                org.apache.kafka.clients.producer.ProducerConfig.PARTITIONER_CLASS_CONFIG,
                getPartitionerClass()
        );

        config.put(
                org.apache.kafka.clients.producer.ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                String.join(",", getInterceptorClasses())
        );

        config.put(
                CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                SecurityProtocol.getOrDefault(getSecurityProtocol()).toString()
        );

        config.put(
                org.apache.kafka.clients.producer.ProducerConfig.SECURITY_PROVIDERS_CONFIG,
                String.join(",", getSecurityProviders())
        );

        config.put(
                org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
                Boolean.toString(isEnableIdempotence())
        );

        config.put(
                org.apache.kafka.clients.producer.ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,
                Integer.toString(getTransactionTimeoutMs())
        );

        config.put(
                org.apache.kafka.clients.producer.ProducerConfig.TRANSACTIONAL_ID_CONFIG,
                getTransactionalId()
        );

        return config;
    }
}
