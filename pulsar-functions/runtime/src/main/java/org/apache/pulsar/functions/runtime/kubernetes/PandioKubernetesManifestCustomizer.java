package org.apache.pulsar.functions.runtime.kubernetes;

import io.kubernetes.client.openapi.models.V1Service;
import io.kubernetes.client.openapi.models.V1StatefulSet;
import org.apache.pulsar.functions.proto.Function;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
        Pattern nsPattern = Pattern.compile("(.+)--(.+)-fn", Pattern.CASE_INSENSITIVE);
        Matcher matcher = nsPattern.matcher(currentNamespace);
        if (matcher.find())
            return String.format("%s--%s--%s-fn", matcher.group(1), matcher.group(2), funcDetails.getTenant());
        else return currentNamespace;
    }
}
