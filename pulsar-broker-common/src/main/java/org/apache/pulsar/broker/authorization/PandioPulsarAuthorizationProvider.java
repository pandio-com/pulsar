/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker.authorization;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.jsonwebtoken.*;
import io.jsonwebtoken.jackson.io.JacksonDeserializer;
import io.jsonwebtoken.security.SignatureException;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.val;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.broker.cache.ConfigurationCacheService;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.*;
import org.apache.pulsar.common.util.FutureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.AuthenticationException;
import java.io.IOException;
import java.security.Key;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Default authorization provider that stores authorization policies under local-zookeeper.
 */
public class PandioPulsarAuthorizationProvider implements AuthorizationProvider {
    private static final Logger log = LoggerFactory.getLogger(PandioPulsarAuthorizationProvider.class);

    public ServiceConfiguration conf;
    public ConfigurationCacheService configCache;
    private JwtParser parser;
    private JwtParser parserForDebug;
    private String clusterName;


    public PandioPulsarAuthorizationProvider() {
    }

    public PandioPulsarAuthorizationProvider(ServiceConfiguration conf, ConfigurationCacheService configCache)
            throws IOException {
        initialize(conf, configCache);
    }

    @Override
    public void initialize(ServiceConfiguration conf, ConfigurationCacheService configCache) throws IOException {
        checkNotNull(conf, "ServiceConfiguration can't be null");
        checkNotNull(configCache, "ConfigurationCacheService can't be null");
        this.conf = conf;
        this.clusterName = conf.getClusterName();
        this.configCache = configCache;
        this.parser = getJWTParser(conf);
        if (conf.isPandioAuthorizationLogEnabled()) {
            this.parserForDebug = getJWTParser(conf);
        }

    }


    /**
     * Check if the specified role has permission to send messages to the specified fully qualified topic name.
     *
     * @param topicName the fully qualified topic name associated with the topic.
     * @param role      the app id used to send messages to the topic.
     */
    @Override
    public CompletableFuture<Boolean> canProduceAsync(TopicName topicName, String role,
                                                      AuthenticationDataSource authenticationData) {
        try {
            val ret = getClusterClaim(authenticationData).checkProduce(topicName).get();
            logPayloadWithMessage("canProduceAsync", authenticationData, "Topic={}; Returned={}", topicName.getLookupName(), String.valueOf(ret));
            return CompletableFuture.completedFuture(ret);
        } catch (AuthenticationException | InterruptedException | ExecutionException e) {
            return FutureUtil.failedFuture(e);
        }
    }

    /**
     * Check if the specified role has permission to receive messages from the specified fully qualified topic
     * name.
     *
     * @param topicName    the fully qualified topic name associated with the topic.
     * @param role         the app id used to receive messages from the topic.
     * @param subscription the subscription name defined by the client
     */
    @Override
    public CompletableFuture<Boolean> canConsumeAsync(TopicName topicName, String role,
                                                      AuthenticationDataSource authenticationData, String subscription) {
        try {
            val ret = getClusterClaim(authenticationData).checkConsume(topicName).get();
            logPayloadWithMessage("canConsumeAsync", authenticationData, "Topic={}, Subscription={}, Returned={}", topicName.getLookupName(), subscription, String.valueOf(ret));
            return CompletableFuture.completedFuture(ret);
        } catch (AuthenticationException | InterruptedException | ExecutionException e) {
            return FutureUtil.failedFuture(e);
        }
    }

    /**
     * Check whether the specified role can perform a lookup for the specified topic.
     * <p>
     * For that the caller needs to have producer or consumer permission.
     *
     * @param topicName
     * @param role
     * @return
     * @throws Exception
     */
    @Override
    public CompletableFuture<Boolean> canLookupAsync(TopicName topicName, String role,
                                                     AuthenticationDataSource authenticationData) {
        try {
            val ret = getClusterClaim(authenticationData).checkLookup(topicName).get();
            logPayloadWithMessage("canLookupAsync", authenticationData, "Topic={}, Returned={}", topicName.getLookupName(), String.valueOf(ret));
            return CompletableFuture.completedFuture(ret);
        } catch (AuthenticationException | InterruptedException | ExecutionException e) {
            return FutureUtil.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<Boolean> allowFunctionOpsAsync(NamespaceName namespaceName, String role, AuthenticationDataSource authenticationData) {
        throw new UnsupportedOperationException("Authorization on Functions is not Supported");
    }

    @Override
    public CompletableFuture<Void> grantPermissionAsync(TopicName topicName, Set<AuthAction> actions,
                                                        String role, String authDataJson) {
        throw new UnsupportedOperationException("Grant Permissions is not Supported");
    }

    @Override
    public CompletableFuture<Void> grantPermissionAsync(NamespaceName namespaceName, Set<AuthAction> actions,
                                                        String role, String authDataJson) {
        throw new UnsupportedOperationException("Grant Permissions is not Supported");
    }

    @Override
    public CompletableFuture<Void> grantSubscriptionPermissionAsync(NamespaceName namespace, String subscriptionName,
                                                                    Set<String> roles, String authDataJson) {
        throw new UnsupportedOperationException("Grant Subscription Permissions is not Supported");
    }

    @Override
    public CompletableFuture<Void> revokeSubscriptionPermissionAsync(NamespaceName namespace, String subscriptionName,
                                                                     String role, String authDataJson) {
        throw new UnsupportedOperationException("Revoke Subscription Permissions is not Supported");
    }

    @Override
    public CompletableFuture<Boolean> isSuperUser(String role, ServiceConfiguration serviceConfiguration) {
        throw new UnsupportedOperationException("isSuperUser with role only is not Supported");
    }

    @Override
    public CompletableFuture<Boolean> isSuperUser(String role, AuthenticationDataSource authenticationData, ServiceConfiguration serviceConfiguration) {
        try {
            val ret = getClusterClaim(authenticationData).isSuperAdmin();
            logPayloadWithMessage("isSuperUser", authenticationData, "Returned={}", String.valueOf(ret));
            return CompletableFuture.completedFuture(ret);
        } catch (AuthenticationException e) {
            return FutureUtil.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<Boolean> isTenantAdmin(String tenant, String role, TenantInfo tenantInfo, AuthenticationDataSource authenticationData) {
        try {
            val ret = getClusterClaim(authenticationData).checkAdmin(tenant).get();
            logPayloadWithMessage("isTenantAdmin", authenticationData, "Tenant={}, Returned={}", tenant, String.valueOf(ret));
            return CompletableFuture.completedFuture(ret);
        } catch (AuthenticationException | InterruptedException | ExecutionException e) {
            return FutureUtil.failedFuture(e);
        }
    }

    @Override
    public void close() throws IOException {
        // No-op
    }

    @Override
    public CompletableFuture<Boolean> allowTenantOperationAsync(String tenantName, String originalRole, String role,
                                                                TenantOperation operation,
                                                                AuthenticationDataSource authData) {
        try {
            val ret = getClusterClaim(authData).checkAdmin(tenantName).get();
            logPayloadWithMessage("allowTenantOperationAsync", authData,
                    "Tenant={}, TenantOperation={}, Returned={}", tenantName, String.valueOf(operation), String.valueOf(ret));
            return CompletableFuture.completedFuture(ret);
        } catch (AuthenticationException | InterruptedException | ExecutionException e) {
            return FutureUtil.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<Boolean> allowNamespaceOperationAsync(NamespaceName namespaceName, String originalRole,
                                                                   String role, NamespaceOperation operation,
                                                                   AuthenticationDataSource authData) {
        try {
            val ret = getClusterClaim(authData).checkAdmin(namespaceName).get();
            logPayloadWithMessage("allowNamespaceOperationAsync", authData,
                    "Namespace={}, NamespaceOperation={}, Returned={}", String.valueOf(namespaceName), String.valueOf(operation), String.valueOf(ret));

            return CompletableFuture.completedFuture(ret);
        } catch (AuthenticationException | InterruptedException | ExecutionException e) {
            return FutureUtil.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<Boolean> allowNamespacePolicyOperationAsync(NamespaceName namespaceName, PolicyName policy,
                                                                         PolicyOperation operation, String originalRole,
                                                                         String role, AuthenticationDataSource authData) {
        try {
            val ret = getClusterClaim(authData).checkAdmin(namespaceName).get();
            logPayloadWithMessage("allowNamespacePolicyOperationAsync", authData,
                    "Namespace={}, PolicyOperation={}, Returned={}", String.valueOf(namespaceName), String.valueOf(operation), String.valueOf(ret));
            return CompletableFuture.completedFuture(ret);
        } catch (AuthenticationException | InterruptedException | ExecutionException e) {
            return FutureUtil.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<Boolean> allowTopicOperationAsync(TopicName topicName, String originalRole, String role,
                                                               TopicOperation operation,
                                                               AuthenticationDataSource authData) {
        CompletableFuture<Boolean> isAuthorizedFuture;
        switch (operation) {
            case LOOKUP:
                isAuthorizedFuture = canLookupAsync(topicName, role, authData);
                break;
            case PRODUCE:
                isAuthorizedFuture = canProduceAsync(topicName, role, authData);
                break;
            case CONSUME:
                isAuthorizedFuture = canConsumeAsync(topicName, role, authData, authData.getSubscription());
                break;
            default:
                isAuthorizedFuture = FutureUtil.failedFuture(
                        new IllegalStateException("TopicOperation is not supported."));
        }

        return isAuthorizedFuture;
    }

    private JwtParser getJWTParser(ServiceConfiguration conf) throws IOException {
        return Jwts.parserBuilder()
                .setSigningKey(getValidationKey(conf, getPublicKeyAlgType(conf)))
                .deserializeJsonWith(new JacksonDeserializer<>(new HashMap<String, Class>() {
                    {
                        put(PermissionsClaim.CLAIM_NAME, PermissionsClaim.class);
                    }
                }))
                .build();
    }

    private JwtParser getJWTParserForDebug(ServiceConfiguration conf) throws IOException {
        return Jwts.parserBuilder()
                .setSigningKey(getValidationKey(conf, getPublicKeyAlgType(conf)))
                .build();
    }

    // When symmetric key is configured
    final static String CONF_TOKEN_SECRET_KEY = "tokenSecretKey";

    // When public/private key pair is configured
    final static String CONF_TOKEN_PUBLIC_KEY = "tokenPublicKey";

    private Key getValidationKey(ServiceConfiguration conf, SignatureAlgorithm publicKeyAlg) throws IOException {
        if (conf.getProperty(CONF_TOKEN_SECRET_KEY) != null
                && StringUtils.isNotBlank((String) conf.getProperty(CONF_TOKEN_SECRET_KEY))) {
            final String validationKeyConfig = (String) conf.getProperty(CONF_TOKEN_SECRET_KEY);
            final byte[] validationKey = AuthTokenUtils.readKeyFromUrl(validationKeyConfig);
            return AuthTokenUtils.decodeSecretKey(validationKey);
        } else if (conf.getProperty(CONF_TOKEN_PUBLIC_KEY) != null
                && StringUtils.isNotBlank((String) conf.getProperty(CONF_TOKEN_PUBLIC_KEY))) {
            final String validationKeyConfig = (String) conf.getProperty(CONF_TOKEN_PUBLIC_KEY);
            final byte[] validationKey = AuthTokenUtils.readKeyFromUrl(validationKeyConfig);
            return AuthTokenUtils.decodePublicKey(validationKey, publicKeyAlg);
        } else {
            throw new IOException("No secret key was provided for token authentication");
        }
    }

    // When using public key's, the algorithm of the key
    final static String CONF_TOKEN_PUBLIC_ALG = "tokenPublicAlg";

    private SignatureAlgorithm getPublicKeyAlgType(ServiceConfiguration conf) throws IllegalArgumentException {
        if (conf.getProperty(CONF_TOKEN_PUBLIC_ALG) != null
                && StringUtils.isNotBlank((String) conf.getProperty(CONF_TOKEN_PUBLIC_ALG))) {
            String alg = (String) conf.getProperty(CONF_TOKEN_PUBLIC_ALG);
            try {
                return SignatureAlgorithm.forName(alg);
            } catch (SignatureException ex) {
                throw new IllegalArgumentException("invalid algorithm provided " + alg, ex);
            }
        } else {
            return SignatureAlgorithm.RS256;
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class PermissionsClaim {
        public static final String CLAIM_NAME = "permissions";

        @JsonDeserialize(contentAs = Permissions.class)
        Map<String, Permissions> clusterPermissions;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Permissions {
        @JsonProperty(value = "isSuperAdmin", defaultValue = "false")
        private boolean isSuperAdmin;
        private List<String> a;
        private List<String> c;
        private List<String> p;

        public CompletableFuture<Boolean> checkProduce(String tenantName) {
            return check(p, tenantName);
        }

        public CompletableFuture<Boolean> checkProduce(NamespaceName namespaceName) throws ExecutionException, InterruptedException {
            return check(p, namespaceName);
        }

        public CompletableFuture<Boolean> checkProduce(TopicName topicName) throws ExecutionException, InterruptedException {
            return check(p, topicName);
        }

        public CompletableFuture<Boolean> checkConsume(String tenantName) {
            return check(c, tenantName);
        }

        public CompletableFuture<Boolean> checkConsume(NamespaceName namespaceName) throws ExecutionException, InterruptedException {
            return check(c, namespaceName);
        }

        public CompletableFuture<Boolean> checkConsume(TopicName topicName) throws ExecutionException, InterruptedException {
            return check(c, topicName);
        }

        public CompletableFuture<Boolean> checkLookup(String tenantName) throws ExecutionException, InterruptedException {
            return CompletableFuture.completedFuture(check(c, tenantName).get() || check(p, tenantName).get());
        }

        public CompletableFuture<Boolean> checkLookup(NamespaceName namespaceName) throws ExecutionException, InterruptedException {
            return CompletableFuture.completedFuture(check(c, namespaceName).get() || check(p, namespaceName).get());
        }

        public CompletableFuture<Boolean> checkLookup(TopicName topicName) throws ExecutionException, InterruptedException {
            return CompletableFuture.completedFuture(check(c, topicName).get() || check(p, topicName).get());
        }

        public CompletableFuture<Boolean> checkAdmin(String tenantName) {
            return check(a, tenantName);
        }

        public CompletableFuture<Boolean> checkAdmin(NamespaceName namespaceName) throws ExecutionException, InterruptedException {
            return check(a, namespaceName);
        }

        public CompletableFuture<Boolean> checkAdmin(TopicName topicName) throws ExecutionException, InterruptedException {
            return check(a, topicName);
        }

        private CompletableFuture<Boolean> check(List<String> l, String tenantName) {
            if (isSuperAdmin) return CompletableFuture.completedFuture(true);
            if (StringUtils.isBlank(tenantName) || l == null) {
                return CompletableFuture.completedFuture(false);
            }
            return CompletableFuture.completedFuture(
                    isSuperAdmin ||
                            l.stream().anyMatch(tenantName::equals)
            );
        }

        private CompletableFuture<Boolean> check(List<String> l, NamespaceName namespaceName) throws ExecutionException, InterruptedException {
            if (isSuperAdmin) return CompletableFuture.completedFuture(true);
            if (namespaceName == null || l == null) {
                return CompletableFuture.completedFuture(false);
            }
            return CompletableFuture.completedFuture(
                    isSuperAdmin ||
                            check(l, namespaceName.getTenant()).get() ||
                            l.stream().anyMatch(s -> namespaceName.toString().equals(s))
            );
        }

        private CompletableFuture<Boolean> check(List<String> l, TopicName topicName) throws ExecutionException, InterruptedException {
            if (isSuperAdmin) return CompletableFuture.completedFuture(true);
            if (topicName == null || l == null) {
                return CompletableFuture.completedFuture(false);
            }
            return CompletableFuture.completedFuture(
                            check(l, topicName.getNamespaceObject()).get() ||
                            l.stream().anyMatch(s -> topicName.toString().equals(s))
            );
        }
    }

    private Permissions getClusterClaim(AuthenticationDataSource authenticationDataSource) throws AuthenticationException {
        return Optional.ofNullable(AuthenticationProviderToken.getToken(authenticationDataSource))
                .map(parser::parse)
                .map(jwt -> (Jwt<?, Claims>) jwt)
                .map(Jwt::getBody)
                .map(body -> body.get(PermissionsClaim.CLAIM_NAME, PermissionsClaim.class))
                .map(PermissionsClaim::getClusterPermissions)
                .map(m -> m.get(this.clusterName))
                .orElseThrow(() -> new AuthenticationException("Issue while fetching cluster permissions"));
    }


    private void logPayloadWithMessage(String message, AuthenticationDataSource authenticationDataSource, String format, Object... objects) throws AuthenticationException {
        if (conf.isPandioAuthorizationLogEnabled()) {
            log.info("Message={}, Payload={}, " + format, message, getClusterClaimAsRawJSON(authenticationDataSource), objects);
        }
    }


    public static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Used in logging.
     */
    private String getClusterClaimAsRawJSON(AuthenticationDataSource authenticationDataSource) throws AuthenticationException {
        return Optional.ofNullable(AuthenticationProviderToken.getToken(authenticationDataSource))
                .map(parserForDebug::parse)
                .map(jwt -> (Jwt<?, Claims>) jwt)
                .map(Jwt::getBody)
                .map(body -> {
                    try {
                        return objectMapper.writeValueAsString(body);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(new AuthenticationException("Invalid Payload, failed to serialize"));
                    }
                })
                .orElseThrow(() -> new AuthenticationException("Issue while fetching cluster permissions"));
    }
}
