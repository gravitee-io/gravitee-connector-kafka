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

import io.gravitee.connector.api.Connection;
import io.gravitee.connector.api.Connector;
import io.gravitee.connector.api.ConnectorBuilder;
import io.gravitee.connector.api.ConnectorFactory;
import io.gravitee.connector.kafka.endpoint.KafkaEndpoint;
import io.gravitee.connector.kafka.endpoint.factory.KafkaEndpointFactory;
import io.gravitee.gateway.api.proxy.ProxyRequest;
import java.util.Collection;
import java.util.Collections;

/**
 * @author David BRASSELY (david.brassely at graviteesource.com)
 * @author GraviteeSource Team
 */
public class KafkaConnectorFactory
  implements ConnectorFactory<Connector<Connection, ProxyRequest>> {

  private static final Collection<String> SUPPORTED_PROTOCOLS = Collections.singletonList(
    "kafka"
  );

  private final KafkaEndpointFactory endpointFactory = new KafkaEndpointFactory();

  @Override
  public Collection<String> supportedTypes() {
    return SUPPORTED_PROTOCOLS;
  }

  @Override
  public Connector<Connection, ProxyRequest> create(
    String target,
    String configuration,
    ConnectorBuilder builder
  ) {
    KafkaEndpoint kafkaEndpoint = endpointFactory.create(
      configuration,
      builder.getMapper()
    );

    return new KafkaConnector(kafkaEndpoint);
  }
}
