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

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.gravitee.common.http.HttpMethod;
import io.gravitee.common.http.HttpStatusCode;
import io.gravitee.common.util.LinkedMultiValueMap;
import io.gravitee.connector.api.Connection;
import io.gravitee.connector.api.Connector;
import io.gravitee.connector.api.ConnectorBuilder;
import io.gravitee.connector.api.Response;
import io.gravitee.connector.kafka.endpoint.CommonConfig;
import io.gravitee.connector.kafka.endpoint.ConsumerConfig;
import io.gravitee.connector.kafka.endpoint.KafkaEndpoint;
import io.gravitee.connector.kafka.endpoint.ProducerConfig;
import io.gravitee.connector.kafka.ws.MockWebSocket;
import io.gravitee.connector.kafka.ws.WebsocketConnection;
import io.gravitee.gateway.api.ExecutionContext;
import io.gravitee.gateway.api.Request;
import io.gravitee.gateway.api.buffer.Buffer;
import io.gravitee.gateway.api.handler.Handler;
import io.gravitee.gateway.api.http.HttpHeaders;
import io.gravitee.gateway.api.proxy.ProxyRequest;
import io.gravitee.gateway.api.proxy.builder.ProxyRequestBuilder;
import io.gravitee.gateway.api.ws.WebSocket;
import io.gravitee.gateway.api.ws.WebSocketFrame;
import io.gravitee.reporter.api.http.Metrics;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.BDDMockito;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testcontainers.utility.DockerImageName;

/**
 * @author David BRASSELY (david.brassely at graviteesource.com)
 * @author GraviteeSource Team
 */
@ExtendWith(MockitoExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class KafkaConnectorTest {

    private static final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"));

    private static final ObjectMapper mapper = new ObjectMapper();

    private static final String RECORD_PAYLOAD = "this-is-a-dummy-payload";
    private static final String TOPIC_NAME = "my-topic";
    private static final String TOPIC_PARTITION_NAME = "my-topic-partition";

    private static Connector<Connection, ProxyRequest> connector;

    @BeforeAll
    public static void startContainers() throws Exception {
        Startables.deepStart(kafka).join();

        KafkaEndpoint endpoint = new KafkaEndpoint("kafka", "my-endpoint", kafka.getBootstrapServers());
        endpoint.setCommonConfig(new CommonConfig());
        ConsumerConfig config = new ConsumerConfig();
        config.setAutoOffsetReset("earliest");
        endpoint.setConsumerConfig(config);
        endpoint.setProducerConfig(new ProducerConfig());

        String definition = mapper.writeValueAsString(endpoint);

        ConnectorBuilder builder = mock(ConnectorBuilder.class);
        when(builder.getMapper()).thenReturn(mapper);

        connector = new KafkaConnectorFactory().create(kafka.getBootstrapServers(), definition, builder);

        AdminClient adminClient = AdminClient.create(
            ImmutableMap.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers())
        );

        Collection<NewTopic> topics = Arrays.asList(
            new NewTopic(TOPIC_NAME, 1, (short) 1),
            new NewTopic(TOPIC_PARTITION_NAME, 2, (short) 1)
        );

        adminClient.createTopics(topics).all().get(30, TimeUnit.SECONDS);
    }

    @AfterAll
    public static void stopContainers() {
        kafka.stop();
    }

    @Test
    @Order(1)
    public void shouldPostRecordToTopic() throws Exception {
        HttpHeaders headers = mock(HttpHeaders.class);
        ExecutionContext context = mock(ExecutionContext.class);
        Request request = createMockRequest(context, HttpMethod.POST, TOPIC_NAME, headers);

        ProxyRequest proxyRequest = ProxyRequestBuilder.from(request).build();
        CountDownLatch latch = new CountDownLatch(1);

        Vertx
            .vertx()
            .runOnContext(
                new io.vertx.core.Handler<Void>() {
                    @Override
                    public void handle(Void event) {
                        connector.request(
                            context,
                            proxyRequest,
                            result -> {
                                result.responseHandler(
                                    new Handler<Response>() {
                                        @Override
                                        public void handle(Response response) {
                                            Assertions.assertEquals(HttpStatusCode.CREATED_201, response.status());
                                            Assertions.assertEquals(TOPIC_NAME, response.headers().get(KafkaConnector.KAFKA_TOPIC_HEADER));
                                            verify(context, times(1))
                                                .setAttribute(eq(InsertDataConnection.CONTEXT_ATTRIBUTE_KAFKA_RECORD_KEY), anyString());
                                            latch.countDown();
                                        }
                                    }
                                );

                                result.write(Buffer.buffer(RECORD_PAYLOAD));
                                result.end();
                            }
                        );
                    }
                }
            );

        Assertions.assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    @Test
    @Order(2)
    public void shouldGetRecordFromTopic() throws Exception {
        HttpHeaders headers = mock(HttpHeaders.class);
        lenient().when(headers.get(KafkaConnector.KAFKA_PARTITION_HEADER)).thenReturn("0");

        ExecutionContext context = mock(ExecutionContext.class);
        Request request = createMockRequest(context, HttpMethod.GET, TOPIC_NAME, headers);

        ProxyRequest proxyRequest = ProxyRequestBuilder.from(request).build();
        CountDownLatch latch = new CountDownLatch(1);

        Vertx
            .vertx()
            .runOnContext(
                new io.vertx.core.Handler<Void>() {
                    @Override
                    public void handle(Void event) {
                        connector.request(
                            context,
                            proxyRequest,
                            result -> {
                                result.responseHandler(
                                    new Handler<Response>() {
                                        @Override
                                        public void handle(Response response) {
                                            Assertions.assertEquals(HttpStatusCode.OK_200, response.status());

                                            response.bodyHandler(
                                                new Handler<Buffer>() {
                                                    @Override
                                                    public void handle(Buffer payload) {
                                                        JsonArray records = new JsonArray(payload.toString());
                                                        JsonObject record = records.getJsonObject(0);

                                                        Assertions.assertEquals(RECORD_PAYLOAD, record.getString("payload"));
                                                    }
                                                }
                                            );

                                            response.endHandler(result1 -> latch.countDown());
                                        }
                                    }
                                );

                                result.end();
                            }
                        );
                    }
                }
            );

        Assertions.assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    @Test
    @Order(3)
    public void shouldStreamRecordsFromTopic() throws Exception {
        CountDownLatch latch = new CountDownLatch(3);

        KafkaProducer<String, String> producer = new KafkaProducer<>(
            ImmutableMap.of(
                org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafka.getBootstrapServers(),
                org.apache.kafka.clients.producer.ProducerConfig.CLIENT_ID_CONFIG,
                UUID.randomUUID().toString()
            ),
            new StringSerializer(),
            new StringSerializer()
        );

        HttpHeaders headers = mock(HttpHeaders.class);

        ExecutionContext context = mock(ExecutionContext.class);
        Request request = createMockWebSocket(context, HttpMethod.GET, TOPIC_NAME, headers, frame -> latch.countDown());
        ProxyRequest proxyRequest = ProxyRequestBuilder.from(request).build();

        Vertx
            .vertx()
            .runOnContext(
                new io.vertx.core.Handler<Void>() {
                    @Override
                    public void handle(Void event) {
                        connector.request(
                            context,
                            proxyRequest,
                            result -> {
                                Assertions.assertTrue(result instanceof WebsocketConnection);

                                try {
                                    producer.send(new ProducerRecord<>(TOPIC_NAME, "payload1")).get();
                                    producer.send(new ProducerRecord<>(TOPIC_NAME, "payload2")).get();
                                    producer.send(new ProducerRecord<>(TOPIC_NAME, "payload3")).get();
                                } catch (Exception ex) {
                                    Assertions.fail(ex);
                                } finally {
                                    producer.close();
                                }

                                result.responseHandler(
                                    new Handler<Response>() {
                                        @Override
                                        public void handle(Response response) {
                                            Assertions.assertEquals(HttpStatusCode.SWITCHING_PROTOCOLS_101, response.status());
                                        }
                                    }
                                );

                                result.end();
                            }
                        );
                    }
                }
            );

        Assertions.assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    @Test
    @Order(4)
    public void shouldStreamRecordsFromGivenPartition() throws Exception {
        CountDownLatch latch = new CountDownLatch(2);

        KafkaProducer<String, String> producer = new KafkaProducer<>(
            ImmutableMap.of(
                org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafka.getBootstrapServers(),
                org.apache.kafka.clients.producer.ProducerConfig.CLIENT_ID_CONFIG,
                UUID.randomUUID().toString()
            ),
            new StringSerializer(),
            new StringSerializer()
        );

        HttpHeaders headers = mock(HttpHeaders.class);
        lenient().when(headers.get(KafkaConnector.KAFKA_PARTITION_HEADER)).thenReturn("0");

        ExecutionContext context = mock(ExecutionContext.class);
        Request request = createMockWebSocket(context, HttpMethod.GET, TOPIC_PARTITION_NAME, headers, frame -> latch.countDown());
        ProxyRequest proxyRequest = ProxyRequestBuilder.from(request).build();

        Vertx
            .vertx()
            .runOnContext(
                new io.vertx.core.Handler<Void>() {
                    @Override
                    public void handle(Void event) {
                        connector.request(
                            context,
                            proxyRequest,
                            result -> {
                                Assertions.assertTrue(result instanceof WebsocketConnection);

                                try {
                                    producer.send(new ProducerRecord<>(TOPIC_PARTITION_NAME, 0, "key1", "payload1")).get();
                                    producer.send(new ProducerRecord<>(TOPIC_PARTITION_NAME, 0, "key2", "payload2")).get();
                                    producer.send(new ProducerRecord<>(TOPIC_PARTITION_NAME, 1, "key3", "payload3")).get();
                                } catch (Exception ex) {
                                    Assertions.fail(ex);
                                } finally {
                                    producer.close();
                                }

                                result.responseHandler(
                                    new Handler<Response>() {
                                        @Override
                                        public void handle(Response response) {
                                            Assertions.assertEquals(HttpStatusCode.SWITCHING_PROTOCOLS_101, response.status());
                                        }
                                    }
                                );

                                result.end();
                            }
                        );
                    }
                }
            );

        Assertions.assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    @Test
    @Order(5)
    public void shouldStreamRecordsFromGivenPartition_multipleConsumers() throws Exception {
        KafkaProducer<String, String> producer = new KafkaProducer<>(
            ImmutableMap.of(
                org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafka.getBootstrapServers(),
                org.apache.kafka.clients.producer.ProducerConfig.CLIENT_ID_CONFIG,
                UUID.randomUUID().toString()
            ),
            new StringSerializer(),
            new StringSerializer()
        );

        CountDownLatch waitForConsumers = new CountDownLatch(2);
        CountDownLatch waitForRecords = new CountDownLatch(4);

        HttpHeaders headers = mock(HttpHeaders.class);
        lenient().when(headers.get(KafkaConnector.KAFKA_PARTITION_HEADER)).thenReturn("0");

        ExecutionContext context = mock(ExecutionContext.class);
        Request request = createMockWebSocket(context, HttpMethod.GET, TOPIC_PARTITION_NAME, headers, frame -> waitForRecords.countDown());
        ProxyRequest proxyRequest = ProxyRequestBuilder.from(request).build();

        Vertx
            .vertx()
            .runOnContext(
                new io.vertx.core.Handler<Void>() {
                    @Override
                    public void handle(Void event) {
                        connector.request(
                            context,
                            proxyRequest,
                            result -> {
                                result.responseHandler(response -> waitForConsumers.countDown());

                                result.end();
                            }
                        );

                        connector.request(
                            context,
                            proxyRequest,
                            result -> {
                                result.responseHandler(response -> waitForConsumers.countDown());

                                result.end();
                            }
                        );
                    }
                }
            );

        Assertions.assertTrue(waitForConsumers.await(10, TimeUnit.SECONDS));

        try {
            producer.send(new ProducerRecord<>(TOPIC_PARTITION_NAME, 0, "key1", "payload1")).get();
            producer.send(new ProducerRecord<>(TOPIC_PARTITION_NAME, 0, "key2", "payload2")).get();
        } catch (Exception ex) {
            Assertions.fail(ex);
        } finally {
            producer.close();
        }

        Assertions.assertTrue(waitForRecords.await(10, TimeUnit.SECONDS));
    }

    @Test
    @Order(4)
    public void shouldStreamRecordsFromGivenPartition_multipleConsumers_sameGroupId() throws Exception {
        KafkaProducer<String, String> producer = new KafkaProducer<>(
            ImmutableMap.of(
                org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafka.getBootstrapServers(),
                org.apache.kafka.clients.producer.ProducerConfig.CLIENT_ID_CONFIG,
                UUID.randomUUID().toString()
            ),
            new StringSerializer(),
            new StringSerializer()
        );

        CountDownLatch waitForConsumers = new CountDownLatch(2);
        CountDownLatch waitForRecords = new CountDownLatch(4);

        HttpHeaders headers = mock(HttpHeaders.class);
        lenient().when(headers.get(eq(KafkaConnector.KAFKA_PARTITION_HEADER))).thenReturn("0");

        lenient().when(headers.get(eq(KafkaConnector.KAFKA_GROUP_HEADER))).thenReturn("my-group-id");
        //        when(headers.get(KafkaConnector.KAFKA_TOPIC_HEADER)).thenReturn(TOPIC_PARTITION_NAME);

        ExecutionContext context = mock(ExecutionContext.class);
        Request request = createMockWebSocket(context, HttpMethod.GET, TOPIC_PARTITION_NAME, headers, frame -> waitForRecords.countDown());
        ProxyRequest proxyRequest = ProxyRequestBuilder.from(request).build();

        Vertx
            .vertx()
            .runOnContext(
                new io.vertx.core.Handler<Void>() {
                    @Override
                    public void handle(Void event) {
                        connector.request(
                            context,
                            proxyRequest,
                            result -> {
                                result.responseHandler(response -> waitForConsumers.countDown());

                                result.end();
                            }
                        );

                        connector.request(
                            context,
                            proxyRequest,
                            result -> {
                                result.responseHandler(response -> waitForConsumers.countDown());

                                result.end();
                            }
                        );
                    }
                }
            );

        Assertions.assertTrue(waitForConsumers.await(10, TimeUnit.SECONDS));

        try {
            producer.send(new ProducerRecord<>(TOPIC_PARTITION_NAME, 0, "key1", "payload1")).get();
            producer.send(new ProducerRecord<>(TOPIC_PARTITION_NAME, 0, "key2", "payload2")).get();
        } catch (Exception ex) {
            Assertions.fail(ex);
        } finally {
            producer.close();
        }

        Assertions.assertTrue(waitForRecords.await(10, TimeUnit.SECONDS));
    }

    private Request createMockRequest(ExecutionContext context, HttpMethod method, String topic, HttpHeaders headers) {
        final Request request = mock(Request.class);
        when(request.headers()).thenReturn(headers);
        when(request.metrics()).thenReturn(Metrics.on(System.currentTimeMillis()).build());

        when(request.parameters()).thenReturn(new LinkedMultiValueMap<>());
        when(request.uri()).thenReturn("http://backend/" + topic);
        when(request.method()).thenReturn(method);

        lenient().when(context.request()).thenReturn(request);

        return request;
    }

    private Request createMockWebSocket(
        ExecutionContext context,
        HttpMethod method,
        String topic,
        HttpHeaders headers,
        Handler<WebSocketFrame> writeHandler
    ) {
        final Request request = createMockRequest(context, method, topic, headers);

        lenient().when(headers.get(io.gravitee.common.http.HttpHeaders.CONNECTION)).thenReturn(HttpHeaderValues.UPGRADE.toString());
        lenient().when(headers.get(io.gravitee.common.http.HttpHeaders.UPGRADE)).thenReturn(HttpHeaderValues.WEBSOCKET.toString());

        when(request.isWebSocket()).thenReturn(true);
        when(request.websocket()).thenReturn(new MockWebSocket().writeHandler(writeHandler));

        return request;
    }
}
