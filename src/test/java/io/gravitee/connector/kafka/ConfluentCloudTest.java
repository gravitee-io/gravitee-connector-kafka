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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;

public class ConfluentCloudTest {

    @Test
    public void test() {
        Map<String, Object> config = new HashMap<>();

        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put("bootstrap.servers", "pkc-ewzgj.europe-west4.gcp.confluent.cloud:9092");
        //    config.put("security.protocol", "SASL_SSL");
        config.put(
            "sasl.jaas.config",
            "org.apache.kafka.common.security.plain.PlainLoginModule   required username='MLEG2PDV4ZQ3DTMJ'   password='74xTGlUa7YxPT5XCTe/sRwrFsrCGY9Zq2Z672/nPTtTTIzrGVGk91LlNl/VfEup+';"
        );
        config.put("sasl.mechanism", "PLAIN");
        config.put("client.dns.lookup", "use_all_dns_ips");
        config.put("session.timeout.ms", "45000");
        config.put("acks", "all");
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "demo-consumer-1");

        final Consumer<String, Object> consumer = new KafkaConsumer<String, Object>(config);
        consumer.subscribe(Arrays.asList("test"));
        // schema.registry.url=https://{{ SR_ENDPOINT }}
        // basic.auth.credentials.source=USER_INFO
        // basic.auth.user.info={{ SR_API_KEY }}:{{ SR_API_SECRET }}

        try {
            while (true) {
                ConsumerRecords<String, Object> records = consumer.poll(100);
                for (ConsumerRecord<String, Object> record : records) {
                    String key = record.key();
                    Object value = record.value();
                    System.out.printf("Consumed record with key %s and value %s, and updated total count to %d%n", key, value);
                }
            }
        } finally {
            consumer.close();
        }
    }
}
