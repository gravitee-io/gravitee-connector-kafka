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

import io.gravitee.connector.api.AbstractConnection;
import io.gravitee.connector.kafka.json.JsonRecordFormatter;
import io.gravitee.gateway.api.buffer.Buffer;
import io.gravitee.gateway.api.proxy.ws.WebSocketProxyRequest;
import io.gravitee.gateway.api.stream.WriteStream;
import io.vertx.core.Future;
import io.vertx.kafka.client.consumer.KafkaConsumer;

/**
 * @author David BRASSELY (david.brassely at graviteesource.com)
 * @author GraviteeSource Team
 */
public abstract class WebsocketConnection extends AbstractConnection {

    protected final KafkaConsumer<String, String> consumer;
    protected final WebSocketProxyRequest proxyRequest;

    protected WebsocketConnection(final KafkaConsumer<String, String> consumer, final WebSocketProxyRequest proxyRequest) {
        this.consumer = consumer;
        this.proxyRequest = proxyRequest;

        consumer
            .handler(
                event ->
                    proxyRequest.write(
                        new WebSocketFrame(io.vertx.core.http.WebSocketFrame.textFrame(JsonRecordFormatter.toString(event, false), true))
                    )
            )
            .endHandler(event -> proxyRequest.close());

        proxyRequest.closeHandler(result -> consumer.unsubscribe().onComplete(event -> consumer.close()));
    }

    @Override
    public WriteStream<Buffer> write(Buffer content) {
        return this;
    }

    @Override
    public void end() {}

    public abstract Future<Void> listen();
}
