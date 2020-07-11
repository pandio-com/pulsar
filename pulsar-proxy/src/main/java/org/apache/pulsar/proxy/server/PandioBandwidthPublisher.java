package org.apache.pulsar.proxy.server;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.common.util.protobuf.ByteBufCodedInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.*;
import java.util.Map;


public class PandioBandwidthPublisher extends ChannelInboundHandlerAdapter {

    public static final String HANDLER_NAME = "pandioBandwidthPublisher";
    //producerid+channelid as key
    //or consumerid+channelid as key
    private static Map<String, String> producerHashMap = new ConcurrentHashMap<>();
    private static Map<String, String> consumerHashMap = new ConcurrentHashMap<>();

    private static ConcurrentHashMap<String, Long> tenantBandwidthMap = null;
    private static ExecutorService executorService = null;
    private static ScheduledExecutorService scheduledExecutorService = null;

    private boolean isInbound;

    public PandioBandwidthPublisher(boolean isInbound) {
        this.isInbound = isInbound;
        if (tenantBandwidthMap == null) {
            tenantBandwidthMap = new ConcurrentHashMap<>();
        }
        if (PandioBandwidthPublisher.executorService == null) {
            PandioBandwidthPublisher.executorService = Executors.newFixedThreadPool(ProxyService.pandioBandwidthPublisherNumOfThreads);
        }
        if (PandioBandwidthPublisher.scheduledExecutorService == null) {
            scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
            scheduledExecutorService.scheduleWithFixedDelay(zookeeperPublishTask(),
                    0,
                    5000,
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
            switch (cmd.getType()) {
                case PRODUCER:
                    topicName = TopicName.get(cmd.getProducer().getTopic());
                    PandioBandwidthPublisher.producerHashMap.put(String.valueOf(cmd.getProducer().getProducerId()) + "," + channelId, cmd.getProducer().getTopic());
                    break;
                case SEND:
                    topicName = TopicName.get(PandioBandwidthPublisher.producerHashMap.get(String.valueOf(cmd.getSend().getProducerId()) + "," + channelId));
                    break;
                case SUBSCRIBE:
                    topicName = TopicName.get(cmd.getSubscribe().getTopic());
                    PandioBandwidthPublisher.consumerHashMap.put(String.valueOf(cmd.getSubscribe().getConsumerId()) + "," + channelId, cmd.getSubscribe().getTopic());
                    break;
                case MESSAGE:
                    topicName = TopicName.get(PandioBandwidthPublisher.consumerHashMap.get(String.valueOf(cmd.getMessage().getConsumerId()) + "," + DirectProxyHandler.inboundOutboundChannelMap.get(channelId)));
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
        if ( topicName == null ) {
            return;
        }
        executorService.execute(
                () -> {
                    long size = buf.capacity();
                    tenantBandwidthMap.computeIfPresent(topicName.getTenant(), (t, acc) -> acc + size);
                    tenantBandwidthMap.computeIfAbsent(topicName.getTenant(), s -> size);
                }
        );
    }

    private static final Logger log = LoggerFactory.getLogger(PandioBandwidthPublisher.class);

    private static Runnable zookeeperPublishTask() {
        return () -> {
            System.out.println("Publishing The Map");
            try {
                String map = ObjectMapperFactory.getThreadLocal().writeValueAsString(tenantBandwidthMap);
                System.out.println(map);
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println();
            //tenantBandwidthMap.clear();
        };
    }
}
