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
package io.gravitee.connector.kafka.ws;

import io.gravitee.gateway.api.proxy.ws.WebSocketProxyRequest;
import io.vertx.core.Future;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;

/**
 * @author David BRASSELY (david.brassely at graviteesource.com)
 * @author GraviteeSource Team
 */
public class PartitionBasedWebsocketConnection extends WebsocketConnection {

    private final String topic;
    private final int partition;
    private final long offset;

    public PartitionBasedWebsocketConnection(
        final KafkaConsumer<String, String> consumer,
        final WebSocketProxyRequest proxyRequest,
        final String topic,
        final int partition,
        final long offset
    ) {
        super(consumer, proxyRequest);
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
    }

    @Override
    public Future<Void> listen() {
        responseHandler.handle(new SwitchProtocolResponse());

        return (offset != -1)
            ? consumer.seek(new TopicPartition(topic, partition), offset)
            : consumer.assign(new TopicPartition(topic, partition));
    }
}
