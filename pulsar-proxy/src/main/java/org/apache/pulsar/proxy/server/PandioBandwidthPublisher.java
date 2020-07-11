package org.apache.pulsar.proxy.server;

import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.api.raw.MessageParser;
import org.apache.pulsar.common.api.raw.RawMessage;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.protobuf.ByteBufCodedInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class PandioBandwidthPublisher extends ChannelInboundHandlerAdapter {

    public static final String HANDLER_NAME = "pandioBandwidthPublisher";
    //producerid+channelid as key
    //or consumerid+channelid as key
    private static Map<String, String> producerHashMap = new ConcurrentHashMap<>();
    private static Map<String, String> consumerHashMap = new ConcurrentHashMap<>();
    private static ExecutorService executorService = null;

    public PandioBandwidthPublisher() {
        if (PandioBandwidthPublisher.executorService == null) {
            PandioBandwidthPublisher.executorService = Executors.newFixedThreadPool(ProxyService.pandioBandwidthPublisherNumOfThreads);
        }
    }

    private void printTopicAndMessage(String topic, List<RawMessage> messages) {
        System.out.println("For the topic ------------------------>" + topic);

        for (int i = 0; i < messages.size(); i++) {
            System.out.printf("Message Index : %d, Message Data : %s", i, new String(ByteBufUtil.getBytes((messages.get(i)).getData())));
        }
        System.out.println();
    }

    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        PulsarApi.BaseCommand cmd = null;
        PulsarApi.BaseCommand.Builder cmdBuilder = null;
        TopicName topicName;
        List<RawMessage> messages = Lists.newArrayList();
        ByteBuf buffer = ((ByteBuf) (msg)).copy();

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
                    PandioBandwidthPublisher.producerHashMap.put(String.valueOf(cmd.getProducer().getProducerId()) + "," + String.valueOf(ctx.channel().id()), cmd.getProducer().getTopic());
                    break;
                case SEND:
                    topicName = TopicName.get(PandioBandwidthPublisher.producerHashMap.get(String.valueOf(cmd.getSend().getProducerId()) + "," + String.valueOf(ctx.channel().id())));
                    MessageParser.parseMessage(topicName,  -1L,
                            -1L,buffer,(message) -> {
                                messages.add(message);
                            }, Commands.DEFAULT_MAX_MESSAGE_SIZE);
                    printTopicAndMessage(topicName.getPersistenceNamingEncoding(), messages);
                    break;

                case SUBSCRIBE:
                    PandioBandwidthPublisher.consumerHashMap.put(String.valueOf(cmd.getSubscribe().getConsumerId()) + "," + String.valueOf(ctx.channel().id()), cmd.getSubscribe().getTopic());
                    break;
                case MESSAGE:
                    topicName = TopicName.get(PandioBandwidthPublisher.consumerHashMap.get(String.valueOf(cmd.getMessage().getConsumerId()) + "," + DirectProxyHandler.inboundOutboundChannelMap.get(ctx.channel().id())));
                    MessageParser.parseMessage(topicName,  -1L,
                            -1L,buffer,(message) -> {
                                messages.add(message);
                            }, Commands.DEFAULT_MAX_MESSAGE_SIZE);
                    printTopicAndMessage(topicName.getPersistenceNamingEncoding(), messages);
                    break;
                default:
                    break;
            }
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

            // add totalSize to buffer Head
            ByteBuf totalSizeBuf = Unpooled.buffer(4);
            totalSizeBuf.writeInt(buffer.readableBytes());
            CompositeByteBuf compBuf = Unpooled.compositeBuffer();
            compBuf.addComponents(totalSizeBuf, buffer);
            compBuf.writerIndex(totalSizeBuf.capacity() + buffer.capacity());

            //next handler
            /*
             * if proxy level is greater than zero then leave it pass the msg as it is for the {@link ParserProxyHandler}
             * unless pass the decoded buffer
             */
            ctx.fireChannelRead(ProxyService.proxyLogLevel == 0 ? compBuf : msg);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(PandioBandwidthPublisher.class);
}
