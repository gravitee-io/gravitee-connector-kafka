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
package io.gravitee.connector.kafka.endpoint;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.ssl.DefaultSslEngineFactory;

/**
 * @author David BRASSELY (david.brassely at graviteesource.com)
 * @author GraviteeSource Team
 */
public class SslConfig {

    private String protocol = SslConfigs.DEFAULT_SSL_PROTOCOL;

    private String provider;

    private List<String> cipherSuites;

    private String enabledProtocols = SslConfigs.DEFAULT_SSL_ENABLED_PROTOCOLS;

    private String keystoreType = SslConfigs.DEFAULT_SSL_KEYSTORE_TYPE;

    private String keystoreLocation;

    private String keystorePassword;

    private String keyPassword;

    private String truststoreLocation;

    private String truststorePassword;

    private String keymanagerAlgorithm = KeyManagerFactory.getDefaultAlgorithm();

    private String trustmanagerAlgorithm = TrustManagerFactory.getDefaultAlgorithm();

    private String truststoreType = SslConfigs.DEFAULT_SSL_TRUSTSTORE_TYPE;

    private String endpointIdentificationAlgorithm = SslConfigs.DEFAULT_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM;

    private String secureRandomImplementation;

    private String engineFactoryClass = DefaultSslEngineFactory.class.getName();

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public String getProvider() {
        return provider;
    }

    public void setProvider(String provider) {
        this.provider = provider;
    }

    public List<String> getCipherSuites() {
        return cipherSuites;
    }

    public void setCipherSuites(List<String> cipherSuites) {
        this.cipherSuites = cipherSuites;
    }

    public String getEnabledProtocols() {
        return enabledProtocols;
    }

    public void setEnabledProtocols(String enabledProtocols) {
        this.enabledProtocols = enabledProtocols;
    }

    public String getKeystoreType() {
        return keystoreType;
    }

    public void setKeystoreType(String keystoreType) {
        this.keystoreType = keystoreType;
    }

    public String getKeystoreLocation() {
        return keystoreLocation;
    }

    public void setKeystoreLocation(String keystoreLocation) {
        this.keystoreLocation = keystoreLocation;
    }

    public String getKeystorePassword() {
        return keystorePassword;
    }

    public void setKeystorePassword(String keystorePassword) {
        this.keystorePassword = keystorePassword;
    }

    public String getKeyPassword() {
        return keyPassword;
    }

    public void setKeyPassword(String keyPassword) {
        this.keyPassword = keyPassword;
    }

    public String getTruststoreLocation() {
        return truststoreLocation;
    }

    public void setTruststoreLocation(String truststoreLocation) {
        this.truststoreLocation = truststoreLocation;
    }

    public String getTruststorePassword() {
        return truststorePassword;
    }

    public void setTruststorePassword(String truststorePassword) {
        this.truststorePassword = truststorePassword;
    }

    public String getKeymanagerAlgorithm() {
        return keymanagerAlgorithm;
    }

    public void setKeymanagerAlgorithm(String keymanagerAlgorithm) {
        this.keymanagerAlgorithm = keymanagerAlgorithm;
    }

    public String getTrustmanagerAlgorithm() {
        return trustmanagerAlgorithm;
    }

    public void setTrustmanagerAlgorithm(String trustmanagerAlgorithm) {
        this.trustmanagerAlgorithm = trustmanagerAlgorithm;
    }

    public String getTruststoreType() {
        return truststoreType;
    }

    public void setTruststoreType(String truststoreType) {
        this.truststoreType = truststoreType;
    }

    public String getEndpointIdentificationAlgorithm() {
        return endpointIdentificationAlgorithm;
    }

    public void setEndpointIdentificationAlgorithm(String endpointIdentificationAlgorithm) {
        this.endpointIdentificationAlgorithm = endpointIdentificationAlgorithm;
    }

    public String getSecureRandomImplementation() {
        return secureRandomImplementation;
    }

    public void setSecureRandomImplementation(String secureRandomImplementation) {
        this.secureRandomImplementation = secureRandomImplementation;
    }

    public String getEngineFactoryClass() {
        return engineFactoryClass;
    }

    public void setEngineFactoryClass(String engineFactoryClass) {
        this.engineFactoryClass = engineFactoryClass;
    }

    public Map<String, String> getKafkaConfig() {
        final Map<String, String> config = new HashMap<>();

        config.put(SslConfigs.SSL_PROTOCOL_CONFIG, getProtocol());
        config.put(SslConfigs.SSL_PROVIDER_CONFIG, getProvider());
        if (cipherSuites != null) {
            config.put(SslConfigs.SSL_CIPHER_SUITES_CONFIG, String.join(",", getCipherSuites()));
        }
        config.put(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, getEnabledProtocols());
        config.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, getKeystoreType());
        config.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, getKeystoreLocation());
        config.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, getKeystorePassword());
        config.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, getKeyPassword());
        config.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, getTruststoreType());
        config.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, getTruststoreLocation());
        config.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, getTruststorePassword());
        config.put(SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG, getKeymanagerAlgorithm());
        config.put(SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG, getTrustmanagerAlgorithm());
        config.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, getEndpointIdentificationAlgorithm());
        config.put(SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG, getSecureRandomImplementation());
        config.put(SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG, getEngineFactoryClass());

        return config;
    }
}
