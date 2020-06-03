package org.apache.pulsar.broker.loadbalance.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.val;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.pulsar.broker.OverallBandwidthBrokerData;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.loadbalance.BillingData;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;

import java.util.Optional;

public class ModularLoadManagerWithBillingImpl extends ModularLoadManagerImpl {
    private Producer<byte[]> logProducer = null;

    public static final String BILLING_DATA_TOPIC = "non-persistent://public/default/billing_data";

    public ModularLoadManagerWithBillingImpl() {
        super();
    }

    private BillingData getBillingData() {
        val billingData = new BillingData();
        val bandwidthData = billingData.getBandwidthData();

        // Iterate over the broker data and update the bandwidth counters for the billing.
        loadData.getBrokerData().forEach((broker, value) -> {
            val overallBandWidthForBroker = bandwidthData.getOrDefault(broker, new OverallBandwidthBrokerData());
            overallBandWidthForBroker.update(value.getLocalData());
            bandwidthData.put(broker, overallBandWidthForBroker);
        });

        return billingData;
    }

    /**
     * Override to write additional data for billing
     */
    @Override
    public void writeBundleDataOnZooKeeper() {
        super.writeBundleDataOnZooKeeper();
        try {
            publishToBillingTopic(getBillingData());
        } catch (JsonProcessingException e) {
            log.error("failed to publish data on log topic", e);
        }
    }

    private void publishToBillingTopic(final BillingData billingData) throws JsonProcessingException {
        this.logProducer = Optional.ofNullable(logProducer)
                .orElseGet(() -> {
                    try {
                        return this.pulsar.getClient().newProducer()
                                .topic(BILLING_DATA_TOPIC)
                                .create();
                    } catch (PulsarClientException | PulsarServerException e) {
                        throw new RuntimeException(e);
                    }
                });
        this.logProducer.sendAsync(billingData.getJsonBytes());
    }
}
