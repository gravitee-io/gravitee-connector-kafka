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
import io.gravitee.common.http.HttpStatusCode;
import io.gravitee.connector.api.AbstractConnection;
import io.gravitee.connector.api.response.StatusResponse;
import io.gravitee.connector.kafka.json.JsonRecordFormatter;
import io.gravitee.connector.kafka.response.RecordResponse;
import io.gravitee.gateway.api.buffer.Buffer;
import io.gravitee.gateway.api.stream.WriteStream;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author David BRASSELY (david.brassely at graviteesource.com)
 * @author GraviteeSource Team
 */
public class ReadDataConnection extends AbstractConnection {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReadDataConnection.class);

    private final KafkaConsumer<String, String> consumer;
    private final String topic;
    private final int partition, offset, timeout;

    private static final int DEFAULT_POLLING_TIMEOUT = 5000;

    public ReadDataConnection(
        final KafkaConsumer<String, String> consumer,
        final String topic,
        final int partition,
        final int offset,
        final int timeout
    ) {
        this.consumer = consumer;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.timeout = (timeout == -1) ? DEFAULT_POLLING_TIMEOUT : timeout;
    }

    @Override
    public WriteStream<Buffer> write(Buffer content) {
        return this;
    }

    @Override
    public void end() {
        Future<Void> subFut;

        if (partition != -1) {
            TopicPartition partition = new TopicPartition(topic, this.partition);
            subFut = consumer.assign(partition);
            if (offset != -1) {
                subFut = subFut.compose(v -> consumer.seek(partition, offset));
            } else {
                subFut = subFut.compose(v -> consumer.seekToBeginning(partition));
            }
        } else {
            subFut = consumer.subscribe(topic);
        }

        subFut
            .onSuccess(
                new Handler<Void>() {
                    @Override
                    public void handle(Void event) {
                        consumer
                            .poll(Duration.ofMillis(timeout))
                            .onSuccess(records -> {
                                if (records.isEmpty()) {
                                    responseHandler.handle(new StatusResponse(HttpStatusCode.NOT_FOUND_404));
                                } else {
                                    Buffer data = Buffer.buffer(JsonRecordFormatter.toString(records, true));
                                    RecordResponse response = new RecordResponse(HttpStatusCode.OK_200);
                                    response.headers().set(HttpHeaders.CONTENT_LENGTH, Integer.toString(data.length()));
                                    responseHandler.handle(response);
                                    response.bodyHandler().handle(data);
                                    response.endHandler().handle(null);
                                }

                                consumer.close();
                            })
                            .onFailure(event1 -> {
                                LOGGER.error("Kafka consume unable to poll a given partition", event1.getCause());
                                responseHandler.handle(new StatusResponse(HttpStatusCode.INTERNAL_SERVER_ERROR_500));
                                consumer.close();
                            });
                    }
                }
            )
            .onFailure(event1 -> {
                LOGGER.error("Kafka consume unable to seek for a given partition", event1.getCause());
                responseHandler.handle(new StatusResponse(HttpStatusCode.INTERNAL_SERVER_ERROR_500));
                consumer.close();
            });
    }
}
