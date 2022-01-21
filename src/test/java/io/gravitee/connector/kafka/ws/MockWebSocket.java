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

import io.gravitee.gateway.api.handler.Handler;
import io.gravitee.gateway.api.ws.WebSocket;
import io.gravitee.gateway.api.ws.WebSocketFrame;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author David BRASSELY (david.brassely at graviteesource.com)
 * @author GraviteeSource Team
 */
public class MockWebSocket implements WebSocket {

    private final Logger logger = LoggerFactory.getLogger(MockWebSocket.class);

    private boolean upgraded = false;

    private Handler<WebSocketFrame> webSocketFrameHandler;
    private Handler<Void> closeHandler;
    private Handler<WebSocketFrame> writeHandler;

    @Override
    public CompletableFuture<WebSocket> upgrade() {
        logger.debug("Upgrade WS");
        upgraded = true;
        return CompletableFuture.completedFuture(this);
    }

    @Override
    public WebSocket reject(int statusCode) {
        logger.debug("Reject WS with status {}", statusCode);
        return this;
    }

    @Override
    public WebSocket write(WebSocketFrame frame) {
        logger.debug("Write frame to WS {}", frame);
        writeHandler.handle(frame);
        return this;
    }

    @Override
    public WebSocket close() {
        logger.debug("Close WS");
        return this;
    }

    public WebSocket writeHandler(Handler<WebSocketFrame> writeHandler) {
        logger.debug("Attach write handler to WS");

        this.writeHandler = writeHandler;
        return this;
    }

    @Override
    public WebSocket frameHandler(Handler<WebSocketFrame> frameHandler) {
        logger.debug("Attach frame handler to WS");

        this.webSocketFrameHandler = frameHandler;
        return this;
    }

    @Override
    public WebSocket closeHandler(Handler<Void> closeHandler) {
        logger.debug("Attach close handler to WS");
        this.closeHandler = closeHandler;
        return this;
    }

    @Override
    public boolean upgraded() {
        return upgraded;
    }
}
