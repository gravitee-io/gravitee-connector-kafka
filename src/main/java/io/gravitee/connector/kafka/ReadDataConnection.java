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
import io.gravitee.connector.api.AbstractConnection;
import io.gravitee.connector.api.response.StatusResponse;
import io.gravitee.connector.kafka.json.JsonRecordFormatter;
import io.gravitee.connector.kafka.response.RecordResponse;
import io.gravitee.gateway.api.buffer.Buffer;
import io.gravitee.gateway.api.stream.WriteStream;
import io.vertx.core.Handler;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;

import java.time.Duration;

/**
 * @author David BRASSELY (david.brassely at graviteesource.com)
 * @author GraviteeSource Team
 */
public class ReadDataConnection extends AbstractConnection {

    private final KafkaConsumer<String, String> consumer;
    private final String topic;
    private final int partition;

    public ReadDataConnection(
            final KafkaConsumer<String, String> consumer,
            final String topic,
            final int partition
    ) {
        this.consumer = consumer;
        this.topic = topic;
        this.partition = partition;
    }

    @Override
    public WriteStream<Buffer> write(Buffer content) {
        return this;
    }

    @Override
    public void end() {
        consumer
                .assign(new TopicPartition(topic, partition))
                .onSuccess(
                        new Handler<Void>() {
                            @Override
                            public void handle(Void event) {
                                consumer
                                        //TODO: How could we configure polling timeout?
                                        // Should it be managed by the consumer or from configuration?
                                        .poll(Duration.ofMillis(10000))
                                        .onSuccess(
                                                records -> {
                                                    if (records.isEmpty()) {
                                                        responseHandler.handle(
                                                                new StatusResponse(HttpStatusCode.NOT_FOUND_404)
                                                        );
                                                    } else {
                                                        // TODO: Only takes the first one for now, then manage bulk
                                                        KafkaConsumerRecord<String, String> record = records.recordAt(
                                                                0
                                                        );

                                                        RecordResponse response = new RecordResponse(
                                                                HttpStatusCode.OK_200
                                                        );
                                                        response.headers().set("x-kafka-record-id", record.key());
                                                        responseHandler.handle(response);
                                                        response
                                                                .bodyHandler()
                                                                .handle(
                                                                        Buffer.buffer(JsonRecordFormatter.format(record))
                                                                );
                                                        response.endHandler().handle(null);
                                                    }
                                                }
                                        )
                                        .onFailure(
                                                cause -> {
                                                    responseHandler.handle(
                                                            new StatusResponse(HttpStatusCode.INTERNAL_SERVER_ERROR_500)
                                                    );
                                                }
                                        );
                            }
                        }
                )
                .onFailure(
                        cause -> responseHandler.handle(
                                new StatusResponse(HttpStatusCode.INTERNAL_SERVER_ERROR_500)
                        )
                );
    }
}
