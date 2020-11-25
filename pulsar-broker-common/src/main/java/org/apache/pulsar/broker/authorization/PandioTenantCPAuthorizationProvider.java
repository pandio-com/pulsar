package org.apache.pulsar.broker.authorization;

import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.util.RestException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;

import java.util.concurrent.CompletableFuture;

import static org.apache.pulsar.broker.cache.ConfigurationCacheService.POLICIES_ROOT;

public class PandioTenantCPAuthorizationProvider extends PulsarAuthorizationProvider
        implements AuthorizationProvider {

    private static final Logger log = LoggerFactory.getLogger(PandioTenantCPAuthorizationProvider.class);

    private TenantInfo tenantInfo(String tenantName) throws Exception {
        return configCache.propertiesCache()
                .get(POLICIES_ROOT + "/" + tenantName)
                .orElseThrow(() -> new RestException(Response.Status.NOT_FOUND, "Tenant does not exist"));
    }

    @Override
    public CompletableFuture<Boolean> canProduceAsync(TopicName topicName, String role, AuthenticationDataSource authenticationData) {
        String tenantName = topicName.getTenant();
        try {
            if (isTenantAdmin(tenantName, role, tenantInfo(tenantName), authenticationData).get()) {
                log.warn("{} is admin of {}. Can produce to any topic", role, tenantName);
                return CompletableFuture.completedFuture(true);
            }
        } catch (Exception e) {
            log.warn("Failed to get tenant info data for {}", tenantName);
        }
        return super.canProduceAsync(topicName, role, authenticationData);
    }

    @Override
    public CompletableFuture<Boolean> canConsumeAsync(TopicName topicName, String role, AuthenticationDataSource authenticationData, String subscription) {
        String tenantName = topicName.getTenant();
        try {
            if (isTenantAdmin(tenantName, role, tenantInfo(tenantName), authenticationData).get()) {
                log.warn("{} is admin of {}. Can consume any topic", role, tenantName);
                return CompletableFuture.completedFuture(true);
            }
        } catch (Exception e) {
            log.warn("Failed to get tenant info data for {}", tenantName);
        }
        return super.canConsumeAsync(topicName, role, authenticationData, subscription);
    }

}
