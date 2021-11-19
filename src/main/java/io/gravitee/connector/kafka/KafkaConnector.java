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
package io.gravitee.connector.kafka;

import io.gravitee.common.http.HttpHeaders;
import io.gravitee.common.http.HttpMethod;
import io.gravitee.common.http.HttpStatusCode;
import io.gravitee.common.utils.UUID;
import io.gravitee.connector.api.AbstractConnector;
import io.gravitee.connector.api.Connection;
import io.gravitee.connector.api.response.StatusResponse;
import io.gravitee.connector.kafka.configuration.ClientDnsLookup;
import io.gravitee.connector.kafka.direct.DirectResponseConnection;
import io.gravitee.connector.kafka.endpoint.KafkaEndpoint;
import io.gravitee.connector.kafka.ws.PartitionBasedWebsocketConnection;
import io.gravitee.connector.kafka.ws.TopicBasedWebsocketConnection;
import io.gravitee.connector.kafka.ws.WebsocketConnection;
import io.gravitee.gateway.api.ExecutionContext;
import io.gravitee.gateway.api.handler.Handler;
import io.gravitee.gateway.api.proxy.ProxyRequest;
import io.gravitee.gateway.api.proxy.ws.WebSocketProxyRequest;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.vertx.core.AsyncResult;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.producer.KafkaProducer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author David BRASSELY (david.brassely at graviteesource.com)
 * @author GraviteeSource Team
 */
public class KafkaConnector extends AbstractConnector<Connection, ProxyRequest> {

    static final String KAFKA_CONTEXT_ATTRIBUTE = ExecutionContext.ATTR_PREFIX + "kafka.";

    static final String CONTEXT_ATTRIBUTE_KAFKA_OFFSET = KAFKA_CONTEXT_ATTRIBUTE + "offset";
    static final String CONTEXT_ATTRIBUTE_KAFKA_PARTITION = KAFKA_CONTEXT_ATTRIBUTE + "partition";
    static final String CONTEXT_ATTRIBUTE_KAFKA_TOPIC = KAFKA_CONTEXT_ATTRIBUTE + "topic";
    static final String CONTEXT_ATTRIBUTE_KAFKA_CLIENT_ID = KAFKA_CONTEXT_ATTRIBUTE + CommonClientConfigs.CLIENT_ID_CONFIG;
    static final String CONTEXT_ATTRIBUTE_KAFKA_GROUP_ID = KAFKA_CONTEXT_ATTRIBUTE + CommonClientConfigs.GROUP_ID_CONFIG;

    private final Logger LOGGER = LoggerFactory.getLogger(KafkaConnector.class);

    private static final String KAFKA_TOPIC_HEADER = "x-gravitee-kafka-topic";
    private static final String KAFKA_TOPIC_QUERY_PARAMETER = "topic";
    private static final String KAFKA_PARTITION_HEADER = "x-gravitee-kafka-partition";
    private static final String KAFKA_PARTITION_QUERY_PARAMETER = "partition";
    private static final String KAFKA_OFFSET_HEADER = "x-gravitee-kafka-offset";
    private static final String KAFKA_OFFSET_QUERY_PARAMETER = "offset";
    private static final String KAFKA_GROUP_HEADER = "x-gravitee-kafka-groupid";
    private static final String KAFKA_GROUP_QUERY_PARAMETER = "groupid";

    private final KafkaEndpoint endpoint;
    private final Map<Thread, KafkaConsumer> consumers = new ConcurrentHashMap<>();
    private final Map<Thread, KafkaProducer> producers = new ConcurrentHashMap<>();

    public KafkaConnector(final KafkaEndpoint endpoint) {
        this.endpoint = endpoint;
    }

    @Override
    public void request(ExecutionContext context, ProxyRequest request, Handler<Connection> connectionHandler) {
        final String topic = extractTopic(context, request);

        request.metrics().setEndpoint(topic);

        // Check the flow mode
        //  - Standard HTTP request
        //  - Streaming request (ie. websocket)
        if (isWebSocket(request)) {
            handleWebsocketRequest(context, request, topic, connectionHandler);
        } else {
            handleRequest(context, request, topic, connectionHandler);
        }
    }

    private void handleWebsocketRequest(
        ExecutionContext context,
        ProxyRequest request,
        String topic,
        Handler<Connection> connectionHandler
    ) {
        WebSocketProxyRequest wsRequest = (WebSocketProxyRequest) request;

        int partition = readIntValue(extractPartition(context, request));
        long offset = readLongValue(extractOffset(context, request));
        KafkaConsumer<String, String> consumer = consumers.computeIfAbsent(Thread.currentThread(), createConsumer(context));

        consumer
            .subscribe(topic)
            .onComplete(
                new io.vertx.core.Handler<AsyncResult<Void>>() {
                    @Override
                    public void handle(AsyncResult<Void> event) {
                        if (event.failed()) {
                            DirectResponseConnection connection = new DirectResponseConnection();
                            connectionHandler.handle(connection);

                            wsRequest.reject(HttpStatusCode.BAD_GATEWAY_502);
                        } else {
                            ((WebSocketProxyRequest) request).upgrade()
                                .thenAccept(
                                    new Consumer<WebSocketProxyRequest>() {
                                        @Override
                                        public void accept(WebSocketProxyRequest webSocketProxyRequest) {
                                            WebsocketConnection connection;

                                            if (partition == -1) {
                                                connection =
                                                    new TopicBasedWebsocketConnection(consumer, (WebSocketProxyRequest) request, topic);
                                            } else {
                                                connection =
                                                    new PartitionBasedWebsocketConnection(
                                                        consumer,
                                                        (WebSocketProxyRequest) request,
                                                        topic,
                                                        partition,
                                                        offset
                                                    );
                                            }

                                            connectionHandler.handle(connection);

                                            connection.listen();
                                        }
                                    }
                                );
                        }
                    }
                }
            );
    }

    private void handleRequest(ExecutionContext context, ProxyRequest request, String topic, Handler<Connection> connectionHandler) {
        final int partition = readIntValue(extractPartition(context, request));

        if (request.method() == HttpMethod.POST || request.method() == HttpMethod.PUT) {
            KafkaProducer<String, String> producer = producers.computeIfAbsent(Thread.currentThread(), createProducer(context));

            connectionHandler.handle(new InsertDataConnection(context, producer, topic, partition, request));
        } else if (request.method() == HttpMethod.GET) {
            KafkaConsumer<String, String> consumer = consumers.computeIfAbsent(Thread.currentThread(), createConsumer(context));

            connectionHandler.handle(new ReadDataConnection(consumer, topic, partition));
        } else {
            DirectResponseConnection connection = new DirectResponseConnection();
            connectionHandler.handle(connection);
            connection.sendResponse(new StatusResponse(HttpStatusCode.BAD_REQUEST_400));
        }
    }

    private static int readIntValue(String sValue) {
        try {
            return Integer.parseInt(sValue);
        } catch (Exception e) {
            return -1;
        }
    }

    private static long readLongValue(String sValue) {
        try {
            return Long.parseLong(sValue);
        } catch (Exception e) {
            return -1;
        }
    }

    private Function<Thread, KafkaProducer<String, String>> createProducer(ExecutionContext context) {
        return thread -> {
            final Map<String, String> config = endpoint.getProducerConfig().getKafkaConfig();

            config.put(org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, endpoint.target());
            config.put(
                CommonClientConfigs.CLIENT_DNS_LOOKUP_CONFIG,
                ClientDnsLookup.getOrDefault(endpoint.getCommonConfig().getClientDnsLookup()).toString()
            );

            // Override value from external attributes
            overrideWithContextAttributes(config, context);
            config.put(ProducerConfig.CLIENT_ID_CONFIG, getClientId(context));

            // Remove empty value
            config.entrySet().removeIf(entry -> entry.getValue() == null || entry.getValue().isEmpty());

            return create(
                (Function<Map<String, String>, KafkaProducer>) config1 -> KafkaProducer.create(Vertx.currentContext().owner(), config1),
                config
            );
        };
    }

    private void overrideWithContextAttributes(final Map<String, String> config, ExecutionContext context) {
        for (Map.Entry<String, String> entry : config.entrySet()) {
            Object attribute = context.getAttribute(KAFKA_CONTEXT_ATTRIBUTE + entry.getKey());
            if (attribute != null) {
                entry.setValue((String) attribute);
            }
        }
    }

    private Function<Thread, KafkaConsumer<String, String>> createConsumer(ExecutionContext context) {
        return thread -> {
            Map<String, String> config = endpoint.getConsumerConfig().getKafkaConfig();

            config.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, endpoint.target());
            config.put(
                CommonClientConfigs.CLIENT_DNS_LOOKUP_CONFIG,
                ClientDnsLookup.getOrDefault(endpoint.getCommonConfig().getClientDnsLookup()).toString()
            );

            // Override value from external attributes
            overrideWithContextAttributes(config, context);
            config.put(ConsumerConfig.GROUP_ID_CONFIG, getGroupId(context));
            config.put(ConsumerConfig.CLIENT_ID_CONFIG, getClientId(context));

            // Remove empty value
            config.entrySet().removeIf(entry -> entry.getValue() == null || entry.getValue().isEmpty());

            return create(
                (Function<Map<String, String>, KafkaConsumer>) config1 -> KafkaConsumer.create(Vertx.currentContext().owner(), config1),
                config
            );
        };
    }

    private <T> T create(Function<Map<String, String>, T> function, Map<String, String> config) {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        try {
            // Required to load classes from the kafka classloader
            Thread.currentThread().setContextClassLoader(null);
            return function.apply(config);
        } finally {
            Thread.currentThread().setContextClassLoader(classLoader);
        }
    }

    @Override
    protected void doStop() throws Exception {
        LOGGER.info("Graceful shutdown of Kafka Client for endpoint[{}] target[{}]", endpoint.name(), endpoint.target());

        consumers.values().forEach(kafkaConsumer -> kafkaConsumer.close());
        consumers.clear();
        producers.values().forEach(kafkaProducer -> kafkaProducer.close());
        producers.clear();
    }

    private boolean isWebSocket(ProxyRequest request) {
        String connectionHeader = request.headers().getFirst(HttpHeaders.CONNECTION);
        String upgradeHeader = request.headers().getFirst(HttpHeaders.UPGRADE);

        return (
            request.method() == HttpMethod.GET &&
            HttpHeaderValues.UPGRADE.contentEqualsIgnoreCase(connectionHeader) &&
            HttpHeaderValues.WEBSOCKET.contentEqualsIgnoreCase(upgradeHeader)
        );
    }

    private String getClientId(ExecutionContext context) {
        String clientId = (String) context.getAttribute(CONTEXT_ATTRIBUTE_KAFKA_CLIENT_ID);
        if (clientId == null) {
            clientId = UUID.random().toString();
        }

        return clientId;
    }

    private String getGroupId(ExecutionContext context) {
        String groupId = (String) context.getAttribute(CONTEXT_ATTRIBUTE_KAFKA_GROUP_ID);

        if (groupId == null || groupId.isEmpty()) {
            groupId = context.request().headers().getFirst(KAFKA_GROUP_HEADER);
            if (groupId == null || groupId.isEmpty()) {
                groupId = context.request().parameters().getFirst(KAFKA_GROUP_QUERY_PARAMETER);
            }

            if (groupId == null || groupId.isEmpty()) {
                groupId = UUID.random().toString();
            }
        }

        return groupId;
    }

    /**
     * Extracting topic from the incoming request
     * - Context attribute
     * - HTTP header
     * - query parameter
     * - last part of the path
     *
     * @param request
     * @return
     */
    private String extractTopic(ExecutionContext context, ProxyRequest request) {
        String topic = (String) context.getAttribute(CONTEXT_ATTRIBUTE_KAFKA_TOPIC);
        if (topic == null || topic.isEmpty()) {
            topic = request.headers().getFirst(KAFKA_TOPIC_HEADER);
            if (topic == null || topic.isEmpty()) {
                topic = request.parameters().getFirst(KAFKA_TOPIC_QUERY_PARAMETER);

                if (topic == null || topic.isEmpty()) {
                    final int idx = request.uri().lastIndexOf('/');

                    if (idx != request.uri().length()) {
                        return request.uri().substring(idx + 1);
                    } else {
                        String uri = request.uri().substring(0, idx - 1);
                        return uri.substring(uri.lastIndexOf('/') + 1);
                    }
                }
            }
        }

        return topic;
    }

    /**
     * Extracting partition from the incoming request
     * - Context attribute
     * - HTTP header
     * - query parameter
     *
     * @param request
     * @return
     */
    private String extractPartition(ExecutionContext context, ProxyRequest request) {
        String partition = (String) context.getAttribute(CONTEXT_ATTRIBUTE_KAFKA_PARTITION);
        if (partition == null || partition.isEmpty()) {
            partition = request.headers().getFirst(KAFKA_PARTITION_HEADER);
            if (partition == null || partition.isEmpty()) {
                partition = request.parameters().getFirst(KAFKA_PARTITION_QUERY_PARAMETER);
            }
        }

        return partition;
    }

    /**
     * Extracting offset from the incoming request
     * - Context attribute
     * - HTTP header
     * - query parameter
     *
     * @param request
     * @return
     */
    private String extractOffset(ExecutionContext context, ProxyRequest request) {
        String offset = (String) context.getAttribute(CONTEXT_ATTRIBUTE_KAFKA_OFFSET);
        if (offset == null || offset.isEmpty()) {
            offset = request.headers().getFirst(KAFKA_OFFSET_HEADER);
            if (offset == null || offset.isEmpty()) {
                offset = request.parameters().getFirst(KAFKA_OFFSET_QUERY_PARAMETER);
            }
        }

        return offset;
    }
}
