package org.apache.pulsar.proxy.server;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.val;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.policies.data.loadbalancer.JSONWritable;
import org.apache.pulsar.zookeeper.ZooKeeperClientFactory;
import org.apache.pulsar.zookeeper.ZookeeperClientFactoryImpl;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.time.LocalDateTime;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

public class PandioPulsarZookeeperPublisher {
    private static ZooKeeper zkClient;
    private static BandwidthStats bandwidthStats = new BandwidthStats();
    public static final String BANDWIDTH_DATA_PATH = "/pandio/bandwidth";

    private final ProxyConfiguration proxyConfiguration;

    private static ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);

    PandioPulsarZookeeperPublisher(final ProxyConfiguration proxyConfiguration) {
        this.proxyConfiguration = proxyConfiguration;
        initZookeeperClient();
        scheduledExecutorService.scheduleWithFixedDelay(zookeeperPublishTask(),
                0,
                proxyConfiguration.getPandioBandwidthPublisherZookeeperPublishIntervalMs(),
                TimeUnit.MILLISECONDS);
    }

    public void updateTenantBandwidthMap(TopicName topicName, ByteBuf buf) {
        if (topicName == null) {
            return;
        }
        final long size = buf.capacity();
        bandwidthStats.computeIfPresent(topicName.getTenant(), (t, acc) -> acc + size);
        bandwidthStats.computeIfAbsent(topicName.getTenant(), s -> size);
    }


    private void initZookeeperClient() {
        ZooKeeperClientFactory factory = new ZookeeperClientFactoryImpl();
        try {
            zkClient = factory.create(proxyConfiguration.getZookeeperServers(),
                    ZooKeeperClientFactory.SessionType.ReadWrite,
                    proxyConfiguration.getZookeeperSessionTimeoutMs()).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Runnable zookeeperPublishTask() {
        return () -> {
            try {
                val path = BANDWIDTH_DATA_PATH + "/" + System.currentTimeMillis();
                if (zkClient.exists(path, false) == null) {
                    try {
                        ZkUtils.createFullPathOptimistic(zkClient, path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                                CreateMode.PERSISTENT);
                    } catch (KeeperException.NodeExistsException e) {
                        // Ignore if already exists.
                    }
                }
                zkClient.setData(path, bandwidthStats.readBytesAndReset(), -1);
            } catch (Exception e) {
                e.printStackTrace();
            }
        };
    }

    // Attempt to create a ZooKeeper path if it does not exist.
    protected static void createZPathIfNotExists(final ZooKeeper zkClient, final String path) throws Exception {
        if (zkClient.exists(path, false) == null) {
            try {
                ZkUtils.createFullPathOptimistic(zkClient, path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException e) {
                // Ignore if already exists.
            }
        }
    }

    @Data
    public static class BandwidthStats extends JSONWritable {

        /**
         * Purpose of this lock
         * Read lock : for normal map operations, Thread safety will be taken care of by ConcurrentHashMap mechanism
         * Write lock : This is to stop all operations on the map.
         */
        @JsonIgnore
        private ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

        private ConcurrentHashMap<String, Long> data = new ConcurrentHashMap<>();

        public Long computeIfAbsent(String key, Function<String, Long> mappingFunction) {
            return executeWithReadLock(() -> data.computeIfAbsent(key, mappingFunction));
        }

        public Long computeIfPresent(String key, BiFunction<String, Long, Long> remappingFunction) {
            return executeWithReadLock(() -> data.computeIfPresent(key, remappingFunction));
        }

        public <T> T executeWithReadLock(Supplier<T> exec) {
            val readLock = readWriteLock.readLock();
            try {
                readLock.lock();
                return exec.get();
            } finally {
                readLock.unlock();
            }
        }

        public byte[] readBytesAndReset() throws JsonProcessingException {
            val writeLock = readWriteLock.writeLock();
            writeLock.lock();
            try {
                val ret = getJsonBytes();
                data.clear();
                return ret;
            } finally {
                System.out.println("Lock untook");
                writeLock.unlock();
            }
        }
    }
}
