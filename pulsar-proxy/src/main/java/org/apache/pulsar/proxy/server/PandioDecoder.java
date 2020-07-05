/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
import org.apache.pulsar.common.util.protobuf.ByteBufCodedInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;


public class PandioDecoder extends ChannelInboundHandlerAdapter {


    //inbound
    protected static final String FRONTEND_CONN = "frontendconn";
    //outbound
    protected static final String BACKEND_CONN = "backendconn";

    private String connType;

    private int maxMessageSize;


    //producerid+channelid as key
    //or consumerid+channelid as key
    private static Map<String, String> producerHashMap = new ConcurrentHashMap<>();
    private static Map<String, String> consumerHashMap = new ConcurrentHashMap<>();

    public PandioDecoder(String type, int maxMessageSize) {
        this.connType = type;
        this.maxMessageSize = maxMessageSize;
    }

    private void printTopicAndMessage(String topic, List<RawMessage> messages) {
        System.out.println("For the topic ------------------------>" + topic);

        for (int i = 0; i < messages.size(); i++) {
            System.out.printf("Message Index : %d, Message Data : %s", i, new String(ByteBufUtil.getBytes((messages.get(i)).getData())));
        }
    }

    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        PulsarApi.BaseCommand cmd = null;
        PulsarApi.BaseCommand.Builder cmdBuilder = null;
        TopicName topicName;
        List<RawMessage> messages = Lists.newArrayList();
        ByteBuf buffer = (ByteBuf) (msg);

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
                    PandioDecoder.producerHashMap.put(String.valueOf(cmd.getProducer().getProducerId()) + "," + String.valueOf(ctx.channel().id()), cmd.getProducer().getTopic());

                    break;

                case SEND:
                    topicName = TopicName.get(PandioDecoder.producerHashMap.get(String.valueOf(cmd.getSend().getProducerId()) + "," + String.valueOf(ctx.channel().id())));
                    MessageParser.parseMessage(topicName, -1L,
                            -1L, buffer, (message) -> {
                                messages.add(message);
                            }, maxMessageSize);

                    printTopicAndMessage(topicName.getPersistenceNamingEncoding(), messages);
                    break;

                case SUBSCRIBE:
                    PandioDecoder.consumerHashMap.put(String.valueOf(cmd.getSubscribe().getConsumerId()) + "," + String.valueOf(ctx.channel().id()), cmd.getSubscribe().getTopic());

                    break;

                case MESSAGE:
                    topicName = TopicName.get(PandioDecoder.consumerHashMap.get(String.valueOf(cmd.getMessage().getConsumerId()) + "," + DirectProxyHandler.inboundOutboundChannelMap.get(ctx.channel().id())));
                    MessageParser.parseMessage(topicName, -1L,
                            -1L, buffer, (message) -> {
                                messages.add(message);
                            }, maxMessageSize);
                    printTopicAndMessage(topicName.getPersistenceNamingEncoding(), messages);


                    //logging(ctx.channel(), cmd.getType(), "", messages);
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
            ctx.fireChannelRead(compBuf);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(ParserProxyHandler.class);
}