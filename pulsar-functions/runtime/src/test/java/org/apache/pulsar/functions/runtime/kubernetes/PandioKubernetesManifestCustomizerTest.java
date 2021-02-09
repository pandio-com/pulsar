package org.apache.pulsar.functions.runtime.kubernetes;

import org.apache.pulsar.functions.proto.Function;
import org.junit.Test;

import static org.testng.Assert.assertEquals;

public class PandioKubernetesManifestCustomizerTest {

    @Test
    public void testPandioCustomizer() {
        KubernetesManifestCustomizer pmk = new PandioKubernetesManifestCustomizer();
        Function.FunctionDetails fd = Function.FunctionDetails
                .newBuilder()
                    .setTenant("rakshit-test-5").build();
        String pCustomized = pmk.customizeNamespace(fd, "pandio--starter-test-fn-3-fn");
        assertEquals(pCustomized, "pandio--starter-test-fn-3--rakshit-test-5-fn");
    }

}
