package org.apache.pulsar.functions.runtime.kubernetes;

import io.kubernetes.client.models.V1Service;
import io.kubernetes.client.models.V1StatefulSet;
import org.apache.pulsar.functions.proto.Function;

import java.util.Map;

public class PandioKubernetesManifestCustomizer implements KubernetesManifestCustomizer {

    public PandioKubernetesManifestCustomizer() {

    }

    @Override
    public void initialize(Map<String, Object> runtimeCustomizerConfig) {

    }

    @Override
    public V1StatefulSet customizeStatefulSet(Function.FunctionDetails funcDetails, V1StatefulSet statefulSet) {
        return statefulSet;
    }

    @Override
    public V1Service customizeService(Function.FunctionDetails funcDetails, V1Service service) {
        return service;
    }

    @Override
    public String customizeNamespace(Function.FunctionDetails funcDetails, String currentNamespace) {
        return String.format("%s-%s", currentNamespace, funcDetails.getTenant());
    }
}
