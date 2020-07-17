package org.apache.pulsar.proxy.server;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.Builder;
import lombok.Data;
import lombok.With;
import lombok.val;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.protobuf.ByteBufCodedInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.Map;

public class PandioBandwidthHandler extends ChannelInboundHandlerAdapter {

    public static final String HANDLER_NAME = "pandioBandwidthPublisher";
    //producerid+channelid as key
    //or consumerid+channelid as key

    private static final Map<String, String> producerHashMap = new ConcurrentHashMap<>();
    private static final Map<String, String> consumerHashMap = new ConcurrentHashMap<>();

    private static final Map<String, List<ChannelInfo>> channelMap = new ConcurrentHashMap<>();

    public static Map<String, String> inboundOutboundChannelMap = new ConcurrentHashMap<>();

    private static ExecutorService executorService = null;
    private final ProxyConfiguration config;

    public PandioBandwidthHandler(final ProxyConfiguration config) {
        this.config = config;
        if (PandioBandwidthHandler.executorService == null) {
            PandioBandwidthHandler.executorService = Executors.newFixedThreadPool(config.getPandioBandwidthPublisherNumOfThreads());
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
                case PRODUCER: {
                    topicName = TopicName.get(cmd.getProducer().getTopic());
                    val producerMapKey = String.valueOf(cmd.getProducer().getProducerId()) + "," + channelId;
                    PandioBandwidthHandler.producerHashMap.put(producerMapKey, cmd.getProducer().getTopic());
                    updateChannelInfoWithProducerMapKey(channelId, producerMapKey);
                }
                break;
                case SEND: {
                    val producerMapKey = String.valueOf(cmd.getSend().getProducerId()) + "," + channelId;
                    topicName = TopicName.get(PandioBandwidthHandler.producerHashMap.get(producerMapKey));
                }
                break;
                case SUBSCRIBE: {
                    topicName = TopicName.get(cmd.getSubscribe().getTopic());
                    val consumerMapKey = String.valueOf(cmd.getSubscribe().getConsumerId()) + "," + channelId;
                    PandioBandwidthHandler.consumerHashMap.put(consumerMapKey, cmd.getSubscribe().getTopic());
                    updateChannelInfoWithConsumerMapKey(channelId, consumerMapKey);
                }
                break;
                case MESSAGE: {
                    val consumerMapKey = String.valueOf(cmd.getMessage().getConsumerId()) + "," + PandioBandwidthHandler.inboundOutboundChannelMap.get(channelId);
                    topicName = TopicName.get(PandioBandwidthHandler.consumerHashMap.get(consumerMapKey));
                }
                break;
                case CLOSE_PRODUCER: {
                    val producerMapKey = String.valueOf(cmd.getCloseProducer().getProducerId()) + "," + channelId;
                    PandioBandwidthHandler.producerHashMap.remove(producerMapKey);
                }
                break;
                case CLOSE_CONSUMER: {
                    val consumerMapKey = String.valueOf(cmd.getMessage().getConsumerId()) + "," + PandioBandwidthHandler.inboundOutboundChannelMap.get(channelId);
                    PandioBandwidthHandler.consumerHashMap.remove(consumerMapKey);
                }
                break;
                default:
                    break;
            }
            updateBandwidthMapAsync(topicName, buffer);
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

    void updateBandwidthMapAsync(final TopicName topicName, final ByteBuf buffer) {
        executorService.execute(() -> ProxyService.pandioPulsarZookeeperPublisher.updateTenantBandwidthMap(topicName, buffer));
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
            channelInfos.forEach(channelInfo -> channelInfo.getRelatedMap().remove(channelInfo.getKey()));
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

    @Data
    @Builder
    @With
    private static class ChannelInfo {
        private String key;
        private Map<String, String> relatedMap;
    }

    private static final Logger log = LoggerFactory.getLogger(PandioBandwidthHandler.class);
}
