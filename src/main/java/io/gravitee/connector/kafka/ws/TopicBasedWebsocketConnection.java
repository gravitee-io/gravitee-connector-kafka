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
import io.vertx.kafka.client.consumer.KafkaConsumer;

/**
 * @author David BRASSELY (david.brassely at graviteesource.com)
 * @author GraviteeSource Team
 */
public class TopicBasedWebsocketConnection extends WebsocketConnection {

    private final String topic;

    public TopicBasedWebsocketConnection(KafkaConsumer<String, String> consumer, WebSocketProxyRequest proxyRequest, String topic) {
        super(consumer, proxyRequest);
        this.topic = topic;
    }

    @Override
    public Future<Void> listen() {
        responseHandler.handle(new SwitchProtocolResponse());

        return consumer.subscribe(topic);
    }
}
