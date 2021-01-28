package org.apache.pulsar.functions.runtime.kubernetes;

import org.apache.pulsar.functions.proto.Function;

import java.util.Map;

public class PandioKubernetesManifestCustomizer implements KubernetesManifestCustomizer {

    public PandioKubernetesManifestCustomizer() {

    }

    @Override
    public void initialize(Map<String, Object> runtimeCustomizerConfig) {

    }

    @Override
    public String customizeNamespace(Function.FunctionDetails funcDetails, String currentNamespace) {
        return String.format("%s-%s", currentNamespace, funcDetails.getTenant());
    }
}
