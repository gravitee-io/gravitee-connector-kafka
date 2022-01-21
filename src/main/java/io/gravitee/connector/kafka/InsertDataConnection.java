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

import io.gravitee.common.http.HttpStatusCode;
import io.gravitee.common.utils.UUID;
import io.gravitee.connector.api.AbstractConnection;
import io.gravitee.connector.api.response.StatusResponse;
import io.gravitee.gateway.api.ExecutionContext;
import io.gravitee.gateway.api.buffer.Buffer;
import io.gravitee.gateway.api.proxy.ProxyRequest;
import io.gravitee.gateway.api.stream.WriteStream;
import io.vertx.core.Handler;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.RecordMetadata;
import java.util.Map;
import java.util.function.Consumer;

/**
 * @author David BRASSELY (david.brassely at graviteesource.com)
 * @author GraviteeSource Team
 */
public class InsertDataConnection extends AbstractConnection {

    public static final String CONTEXT_ATTRIBUTE_KAFKA_RECORD_KEY = KafkaConnector.KAFKA_CONTEXT_ATTRIBUTE + "key";

    private final ExecutionContext context;
    private final Buffer buffer = Buffer.buffer();
    private final KafkaProducer<String, String> producer;
    private final String topic;
    private final int partition;
    private final ProxyRequest request;

    public InsertDataConnection(
        final ExecutionContext context,
        final KafkaProducer<String, String> producer,
        final String topic,
        final int partition,
        final ProxyRequest request
    ) {
        this.context = context;
        this.producer = producer;
        this.topic = topic;
        this.partition = partition;
        this.request = request;
    }

    @Override
    public WriteStream<Buffer> write(Buffer content) {
        buffer.appendBuffer(content);
        return this;
    }

    @Override
    public void end() {
        KafkaProducerRecord<String, String> record = KafkaProducerRecord.create(
            topic,
            getKey(context),
            buffer.toString(),
            (partition != -1) ? partition : null
        );

        setHeaders(record);

        producer
            .send(record)
            .onFailure(
                new Handler<Throwable>() {
                    @Override
                    public void handle(Throwable event) {
                        responseHandler.handle(new StatusResponse(HttpStatusCode.INTERNAL_SERVER_ERROR_500));
                        producer.close();
                    }
                }
            )
            .onSuccess(
                new Handler<RecordMetadata>() {
                    @Override
                    public void handle(RecordMetadata event) {
                        StatusResponse response = new StatusResponse(HttpStatusCode.CREATED_201);

                        response.headers().set(KafkaConnector.KAFKA_TOPIC_HEADER, event.getTopic());
                        response.headers().set(KafkaConnector.KAFKA_PARTITION_HEADER, Integer.toString(event.getPartition()));
                        response.headers().set(KafkaConnector.KAFKA_OFFSET_HEADER, Long.toString(event.getOffset()));
                        responseHandler.handle(response);

                        producer.close();
                    }
                }
            );
    }

    private void setHeaders(KafkaProducerRecord<String, String> record) {
        request.headers().forEach(stringStringEntry -> record.addHeader(stringStringEntry.getKey(), stringStringEntry.getValue()));
    }

    private String getKey(ExecutionContext context) {
        String key = (String) context.getAttribute(CONTEXT_ATTRIBUTE_KAFKA_RECORD_KEY);
        if (key == null) {
            key = UUID.random().toString();
            context.setAttribute(CONTEXT_ATTRIBUTE_KAFKA_RECORD_KEY, key);
        }

        return key;
    }
}
