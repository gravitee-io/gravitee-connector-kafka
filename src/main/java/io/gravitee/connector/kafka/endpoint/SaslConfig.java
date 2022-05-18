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
import java.util.Map;
import org.apache.kafka.common.config.SaslConfigs;

/**
 * @author David BRASSELY (david.brassely at graviteesource.com)
 * @author GraviteeSource Team
 */
public class SaslConfig {

    private String mechanism = SaslConfigs.DEFAULT_SASL_MECHANISM;

    private String jaasConfig;

    private String clientCallbackHandlerClass;

    private String loginCallbackHandlerClass;

    private String loginClass;

    private double loginRefreshWindowFactor = SaslConfigs.DEFAULT_LOGIN_REFRESH_WINDOW_FACTOR;

    private double loginRefreshWindowJitter = SaslConfigs.DEFAULT_LOGIN_REFRESH_WINDOW_JITTER;

    private short loginRefreshMinPeriodSeconds = SaslConfigs.DEFAULT_LOGIN_REFRESH_MIN_PERIOD_SECONDS;

    private short loginRefreshBufferSeconds = SaslConfigs.DEFAULT_LOGIN_REFRESH_BUFFER_SECONDS;

    public String getMechanism() {
        return mechanism;
    }

    public void setMechanism(String mechanism) {
        this.mechanism = mechanism;
    }

    public String getJaasConfig() {
        return jaasConfig;
    }

    public void setJaasConfig(String jaasConfig) {
        this.jaasConfig = jaasConfig;
    }

    public String getClientCallbackHandlerClass() {
        return clientCallbackHandlerClass;
    }

    public void setClientCallbackHandlerClass(String clientCallbackHandlerClass) {
        this.clientCallbackHandlerClass = clientCallbackHandlerClass;
    }

    public String getLoginCallbackHandlerClass() {
        return loginCallbackHandlerClass;
    }

    public void setLoginCallbackHandlerClass(String loginCallbackHandlerClass) {
        this.loginCallbackHandlerClass = loginCallbackHandlerClass;
    }

    public String getLoginClass() {
        return loginClass;
    }

    public void setLoginClass(String loginClass) {
        this.loginClass = loginClass;
    }

    public double getLoginRefreshWindowFactor() {
        return loginRefreshWindowFactor;
    }

    public void setLoginRefreshWindowFactor(double loginRefreshWindowFactor) {
        this.loginRefreshWindowFactor = loginRefreshWindowFactor;
    }

    public double getLoginRefreshWindowJitter() {
        return loginRefreshWindowJitter;
    }

    public void setLoginRefreshWindowJitter(double loginRefreshWindowJitter) {
        this.loginRefreshWindowJitter = loginRefreshWindowJitter;
    }

    public short getLoginRefreshMinPeriodSeconds() {
        return loginRefreshMinPeriodSeconds;
    }

    public void setLoginRefreshMinPeriodSeconds(short loginRefreshMinPeriodSeconds) {
        this.loginRefreshMinPeriodSeconds = loginRefreshMinPeriodSeconds;
    }

    public short getLoginRefreshBufferSeconds() {
        return loginRefreshBufferSeconds;
    }

    public void setLoginRefreshBufferSeconds(short loginRefreshBufferSeconds) {
        this.loginRefreshBufferSeconds = loginRefreshBufferSeconds;
    }

    public Map<String, String> getKafkaConfig() {
        final Map<String, String> config = new HashMap<>();

        config.put(SaslConfigs.SASL_MECHANISM, getMechanism());
        config.put(SaslConfigs.SASL_JAAS_CONFIG, getJaasConfig());
        config.put(SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS, getClientCallbackHandlerClass());
        config.put(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, getLoginCallbackHandlerClass());
        config.put(SaslConfigs.SASL_LOGIN_CLASS, getLoginClass());
        config.put(SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR, Double.toString(getLoginRefreshWindowFactor()));
        config.put(SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER, Double.toString(getLoginRefreshWindowJitter()));
        config.put(SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS, Short.toString(getLoginRefreshMinPeriodSeconds()));
        config.put(SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS, Short.toString(getLoginRefreshBufferSeconds()));

        return config;
    }
}
