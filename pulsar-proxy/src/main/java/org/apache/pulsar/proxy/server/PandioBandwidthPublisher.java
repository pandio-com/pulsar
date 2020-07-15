package org.apache.pulsar.proxy.server;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelId;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.Builder;
import lombok.Data;
import lombok.With;
import lombok.val;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.common.util.protobuf.ByteBufCodedInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.Map;

public class PandioBandwidthPublisher extends ChannelInboundHandlerAdapter {

    public static final String HANDLER_NAME = "pandioBandwidthPublisher";
    //producerid+channelid as key
    //or consumerid+channelid as key

    private static final Map<String, String> producerHashMap = new ConcurrentHashMap<>();
    private static final Map<String, String> consumerHashMap = new ConcurrentHashMap<>();

    private static final Map<String, List<ChannelInfo>> channelMap = new ConcurrentHashMap<>();

    public static Map<String, String> inboundOutboundChannelMap = new ConcurrentHashMap<>();

    private static ConcurrentHashMap<String, Long> tenantBandwidthMap = null;
    private static ExecutorService executorService = null;
    private static ScheduledExecutorService scheduledExecutorService = null;
    private final ProxyConfiguration config;

    public PandioBandwidthPublisher(final ProxyConfiguration config) {
        this.config = config;
        if (tenantBandwidthMap == null) {
            tenantBandwidthMap = new ConcurrentHashMap<>();
        }
        if (PandioBandwidthPublisher.executorService == null) {
            PandioBandwidthPublisher.executorService = Executors.newFixedThreadPool(config.getPandioBandwidthPublisherNumOfThreads());
        }
        if (PandioBandwidthPublisher.scheduledExecutorService == null) {
            scheduledExecutorService = Executors.newScheduledThreadPool(1);
            scheduledExecutorService.scheduleWithFixedDelay(zookeeperPublishTask(),
                    0,
                    config.getPandioBandwidthPublisherZookeeperPublishIntervalMs(),
                    TimeUnit.MILLISECONDS);
        }
    }

    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        ByteBuf buffer = (ByteBuf) (msg);
        writeMessageSize(buffer.copy(), String.valueOf(ctx.channel().id()));
        if (ProxyService.proxyLogLevel == 0) {
            // add totalSize to buffer Head
            ByteBuf totalSizeBuf = Unpooled.buffer(4);
            totalSizeBuf.writeInt(buffer.readableBytes());
            CompositeByteBuf compBuf = Unpooled.compositeBuffer();
            compBuf.addComponents(totalSizeBuf, buffer);
            compBuf.writerIndex(totalSizeBuf.capacity() + buffer.capacity());

            ctx.fireChannelRead(compBuf);
        } else {
            //if proxy level is greater than zero then pass the msg as it is for the {@link ParserProxyHandler}
            ctx.fireChannelRead(msg);
        }
    }

    private void writeMessageSize(final ByteBuf buffer, final String channelId) {
        PulsarApi.BaseCommand cmd = null;
        PulsarApi.BaseCommand.Builder cmdBuilder = null;
        TopicName topicName = null;

        try {
            buffer.markReaderIndex();
            buffer.markWriterIndex();
            int cmdSize = (int) buffer.readUnsignedInt();
            int writerIndex = buffer.writerIndex();
            buffer.writerIndex(buffer.readerIndex() + cmdSize);

            ByteBufCodedInputStream cmdInputStream = ByteBufCodedInputStream.get(buffer);
            cmdBuilder = PulsarApi.BaseCommand.newBuilder();
            cmd = cmdBuilder.mergeFrom(cmdInputStream, null).build();
            buffer.writerIndex(writerIndex);
            cmdInputStream.recycle();
            System.out.printf("CMD:%s, ID: %s\n", cmd.getType(), channelId);

            switch (cmd.getType()) {
                case PRODUCER: {
                    topicName = TopicName.get(cmd.getProducer().getTopic());
                    val producerMapKey = String.valueOf(cmd.getProducer().getProducerId()) + "," + channelId;
                    PandioBandwidthPublisher.producerHashMap.put(producerMapKey, cmd.getProducer().getTopic());
                    updateChannelInfoWithProducerMapKey(channelId, producerMapKey);
                }
                break;
                case SEND: {
                    val producerMapKey = String.valueOf(cmd.getSend().getProducerId()) + "," + channelId;
                    topicName = TopicName.get(PandioBandwidthPublisher.producerHashMap.get(producerMapKey));
                }
                break;
                case SUBSCRIBE: {
                    topicName = TopicName.get(cmd.getSubscribe().getTopic());
                    val consumerMapKey = String.valueOf(cmd.getSubscribe().getConsumerId()) + "," + channelId;
                    PandioBandwidthPublisher.consumerHashMap.put(consumerMapKey, cmd.getSubscribe().getTopic());
                    updateChannelInfoWithConsumerMapKey(channelId, consumerMapKey);
                }
                break;
                case MESSAGE: {
                    val consumerMapKey = String.valueOf(cmd.getMessage().getConsumerId()) + "," + PandioBandwidthPublisher.inboundOutboundChannelMap.get(channelId);
                    topicName = TopicName.get(PandioBandwidthPublisher.consumerHashMap.get(consumerMapKey));
                }
                break;
                default:
                    break;
            }
            updateTenantBandwidthMapAsync(topicName, buffer);
        } catch (Exception e) {
            log.error("{},{},{}", e.getMessage(), e.getStackTrace(), e.getCause());
        } finally {
            if (cmdBuilder != null) {
                cmdBuilder.recycle();
            }
            if (cmd != null) {
                cmd.recycle();
            }
            buffer.resetReaderIndex();
            buffer.resetWriterIndex();
        }
    }


    private void updateTenantBandwidthMapAsync(TopicName topicName, ByteBuf buf) {
        if (topicName == null) {
            return;
        }
        executorService.execute(
                () -> {
                    final long size = buf.capacity();
                    tenantBandwidthMap.computeIfPresent(topicName.getTenant(), (t, acc) -> acc + size);
                    tenantBandwidthMap.computeIfAbsent(topicName.getTenant(), s -> size);
                }
        );
    }

    private static Runnable zookeeperPublishTask() {
        return () -> {
            System.out.println("Publishing The Map for node");
            try {
                String map = ObjectMapperFactory.getThreadLocal().writeValueAsString(tenantBandwidthMap);
                System.out.println(map);

                String pm = ObjectMapperFactory.getThreadLocal().writeValueAsString(producerHashMap);
                System.out.println(pm);

                String cm = ObjectMapperFactory.getThreadLocal().writeValueAsString(consumerHashMap);
                System.out.println(cm);

                String cidm = ObjectMapperFactory.getThreadLocal().writeValueAsString(channelMap);
                System.out.println(cidm);

                String io = ObjectMapperFactory.getThreadLocal().writeValueAsString(inboundOutboundChannelMap);
                System.out.println(io);
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println();
            //tenantBandwidthMap.clear();
        };
    }

    @Data
    @Builder
    @With
    private static class ChannelInfo {
        private String key;
        private Map<String, String> relatedMap;
    }

    private void updateChannelInfo(final String channelId, final String relatedKey, final Map<String, String> relatedMap) {
        channelMap.computeIfAbsent(channelId, s -> new ArrayList<>());
        val newElem = ChannelInfo.builder()
                .key(relatedKey)
                .relatedMap(relatedMap)
                .build();
        channelMap.computeIfPresent(channelId, (s, channelInfos) -> {
            channelInfos.add(newElem);
            return channelInfos;
        });
    }

    private void updateChannelInfoWithProducerMapKey(final String channelId, final String producerMapKey) {
        updateChannelInfo(channelId, producerMapKey, producerHashMap);
    }

    private void updateChannelInfoWithConsumerMapKey(final String channelId, final String consumerMapKey) {
        updateChannelInfo(channelId, consumerMapKey, consumerHashMap);
    }

    private void removeChannelEntries(final String channelId) {
        channelMap.computeIfPresent(channelId, (s, channelInfos) -> {
            channelInfos.forEach( channelInfo -> channelInfo.getRelatedMap().remove(channelInfo.getKey()));
            return channelInfos;
        });
        channelMap.remove(channelId);
        inboundOutboundChannelMap.remove(channelId);
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        val channelId = String.valueOf(ctx.channel().id());
        executorService.execute(() -> {
            removeChannelEntries(channelId);
        });
        super.channelUnregistered(ctx);
    }

    private static final Logger log = LoggerFactory.getLogger(PandioBandwidthPublisher.class);
}
