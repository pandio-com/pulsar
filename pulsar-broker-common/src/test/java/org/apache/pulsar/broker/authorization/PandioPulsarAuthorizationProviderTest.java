package org.apache.pulsar.broker.authorization;

import lombok.val;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class PandioPulsarAuthorizationProviderTest {

    @Test
    public void testSuperAdmin() throws ExecutionException, InterruptedException {
        val sut = new PandioPulsarAuthorizationProvider.Permissions(true, Collections.EMPTY_LIST, null, null);
        assertTrue(sut.checkAdmin("gib").get());
        assertTrue(sut.checkAdmin(NamespaceName.get("public", "default" )).get());
        assertTrue(sut.checkAdmin(TopicName.get("persistent", "public", "default", "test")).get());
        assertTrue(sut.checkProduce(TopicName.get("persistent", "public", "default", "test")).get());
        assertTrue(sut.checkConsume(TopicName.get("persistent", "public", "default", "test")).get());
    }

    @Test
    public void testProduceAccess() throws ExecutionException, InterruptedException {
        val sut = new PandioPulsarAuthorizationProvider.Permissions(false, null, null, Collections.singletonList("blah"));
        assertFalse(sut.checkAdmin("gib").get());
        assertFalse(sut.checkAdmin("blah").get());
        assertFalse(sut.checkConsume("blah").get());
        assertTrue(sut.checkProduce("blah").get());
        assertFalse(sut.checkProduce(NamespaceName.get("public", "default" )).get());
        assertFalse(sut.checkAdmin(TopicName.get("persistent", "public", "default", "test")).get());
    }

    @Test
    public void testProduceAccessNegative() throws ExecutionException, InterruptedException {
        val sut = new PandioPulsarAuthorizationProvider.Permissions(false, null, null, Collections.singletonList("public"));
        assertFalse(sut.checkAdmin("gib").get());
        assertFalse(sut.checkAdmin("blah").get());
        assertFalse(sut.checkConsume("blah").get());
        assertFalse(sut.checkProduce("blah").get());
        assertTrue(sut.checkProduce("public").get());
        assertTrue(sut.checkProduce(NamespaceName.get("public", "default" )).get());
        assertTrue(sut.checkProduce(TopicName.get("persistent", "public", "default", "test")).get());
    }

    @Test
    public void testConsumerAccess() throws ExecutionException, InterruptedException {
        val sut = new PandioPulsarAuthorizationProvider.Permissions(false, null, Collections.singletonList("public/default"),null);
        assertTrue(sut.checkConsume(TopicName.get("persistent://public/default/sample-test")).get());
        assertFalse(sut.checkProduce(TopicName.get("persistent://public/default/sample-test")).get());
    }
}